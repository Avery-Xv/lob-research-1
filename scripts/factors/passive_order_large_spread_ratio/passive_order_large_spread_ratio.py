#!/usr/bin/env python3
"""
Compute the daily passive-order large-spread volume ratio from v3 LOB events.

For each trade:

    passive_order_spread =
        abs(passive_order_price - pre_trade_opposite_best) / pre_trade_mid

The daily continuous-auction factor is:

    sum(trade_volume where passive_order_spread > theta)
    / sum(all continuous-auction trade_volume)

`source_side` is the aggressive side. Therefore an aggressive buy links to the
passive sell order and compares its order price with the pre-trade best bid;
an aggressive sell links to the passive buy order and compares its order price
with the pre-trade best ask.
"""

from __future__ import annotations

import argparse
import csv
import glob
import os
import time
from pathlib import Path
from typing import Iterable, List, Sequence

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[3]

FIELDS = [
    "symbol",
    "date",
    "theta",
    "passive_order_large_spread_ratio",
    "large_spread_trade_volume",
    "total_trade_volume",
    "classified_trade_volume",
    "classified_volume_share",
    "large_spread_trade_count",
    "total_trade_count",
    "classified_trade_count",
]


def expand_inputs(patterns: Sequence[str], limit: int | None = None) -> List[str]:
    paths: List[str] = []
    for pattern in patterns:
        paths.extend(glob.glob(pattern) or [pattern])
    paths = sorted(dict.fromkeys(paths))
    if limit is not None:
        paths = paths[:limit]
    return paths


def chunks(items: Sequence[str], size: int) -> Iterable[Sequence[str]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def compute_batch(
    con: duckdb.DuckDBPyConnection,
    paths: Sequence[str],
    theta: float,
):
    query = """
WITH events AS (
    SELECT
        regexp_replace(regexp_extract(filename, '[^/]+$'), '\\.parquet$', '') AS symbol,
        date,
        row_id,
        CASE
            WHEN time >= 93000000 AND time < 113000000 THEN 'AM'
            WHEN time >= 130000000 AND time < 145700000 THEN 'PM'
            ELSE NULL
        END AS session,
        source_recid,
        source_action,
        source_side,
        source_price,
        source_volume,
        source_buy_order_recid,
        source_sell_order_recid,
        bid_px[1]::DOUBLE AS bid1,
        ask_px[1]::DOUBLE AS ask1
    FROM read_parquet(?, filename=true)
),
with_pre_trade_book AS (
    SELECT
        *,
        lag(bid1) OVER (
            PARTITION BY symbol, date, session
            ORDER BY row_id
        ) AS prev_bid1,
        lag(ask1) OVER (
            PARTITION BY symbol, date, session
            ORDER BY row_id
        ) AS prev_ask1
    FROM events
    WHERE session IS NOT NULL
),
order_records AS (
    SELECT
        symbol,
        date,
        source_recid,
        row_id AS order_row_id,
        source_side AS order_side,
        source_price AS order_price,
        row_number() OVER (
            PARTITION BY symbol, date, source_recid
            ORDER BY row_id
        ) AS occurrence
    FROM events
    WHERE source_action = 'ORDER_ADD'
      AND source_recid IS NOT NULL
      AND source_price IS NOT NULL
),
trades AS (
    SELECT
        e.symbol,
        e.date,
        e.row_id,
        e.source_side,
        e.source_volume,
        e.prev_bid1,
        e.prev_ask1,
        CASE
            WHEN e.prev_bid1 > 0 AND e.prev_ask1 > 0
            THEN (e.prev_bid1 + e.prev_ask1) / 2.0
            ELSE NULL
        END AS prev_mid,
        o.order_price AS passive_order_price,
        CASE
            WHEN e.source_side = 'B' THEN e.prev_bid1
            WHEN e.source_side = 'S' THEN e.prev_ask1
            ELSE NULL
        END AS pre_trade_opposite_best
    FROM with_pre_trade_book e
    LEFT JOIN order_records o
      ON o.symbol = e.symbol
     AND o.date = e.date
     AND o.source_recid = CASE
            WHEN e.source_side = 'B' THEN e.source_sell_order_recid
            WHEN e.source_side = 'S' THEN e.source_buy_order_recid
         END
     AND o.occurrence = 1
     AND o.order_row_id < e.row_id
     AND o.order_side = CASE
            WHEN e.source_side = 'B' THEN 'S'
            WHEN e.source_side = 'S' THEN 'B'
         END
    WHERE e.source_action = 'TRADE'
      AND e.source_volume > 0
),
classified AS (
    SELECT
        *,
        abs(passive_order_price - pre_trade_opposite_best) / prev_mid
            AS passive_order_spread
    FROM trades
    WHERE source_side IN ('B', 'S')
      AND passive_order_price IS NOT NULL
      AND pre_trade_opposite_best IS NOT NULL
      AND prev_mid > 0
)
SELECT
    t.symbol,
    t.date,
    ?::DOUBLE AS theta,
    CASE
        WHEN sum(t.source_volume) = 0 THEN 0.0
        ELSE coalesce(
            sum(c.source_volume) FILTER (WHERE c.passive_order_spread > ?),
            0
        )::DOUBLE / sum(t.source_volume)
    END AS passive_order_large_spread_ratio,
    coalesce(
        sum(c.source_volume) FILTER (WHERE c.passive_order_spread > ?),
        0
    )::BIGINT AS large_spread_trade_volume,
    sum(t.source_volume)::BIGINT AS total_trade_volume,
    coalesce(sum(c.source_volume), 0)::BIGINT AS classified_trade_volume,
    CASE
        WHEN sum(t.source_volume) = 0 THEN 0.0
        ELSE coalesce(sum(c.source_volume), 0)::DOUBLE / sum(t.source_volume)
    END AS classified_volume_share,
    count(c.row_id) FILTER (WHERE c.passive_order_spread > ?)
        AS large_spread_trade_count,
    count(*) AS total_trade_count,
    count(c.row_id) AS classified_trade_count
FROM trades t
LEFT JOIN classified c
  ON c.symbol = t.symbol
 AND c.date = t.date
 AND c.row_id = t.row_id
GROUP BY t.symbol, t.date
ORDER BY t.symbol, t.date
"""
    return con.execute(
        query,
        [list(paths), theta, theta, theta, theta],
    ).fetchall()


def write_rows(path: str, rows, write_header: bool) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w" if write_header else "a", newline="") as output:
        writer = csv.writer(output)
        if write_header:
            writer.writerow(FIELDS)
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Compute daily passive-order large-spread volume ratio."
    )
    parser.add_argument(
        "inputs",
        nargs="*",
        default=["/hdd_data/lob/event_full_depth_v3/202601/*.parquet"],
        help="Parquet path/glob. Defaults to v3 202601 full market.",
    )
    parser.add_argument(
        "--theta",
        type=float,
        default=0.001,
        help="Strict large-spread threshold as a ratio (default: 0.001 = 10 bp).",
    )
    parser.add_argument(
        "-o",
        "--output",
        default=str(
            PROJECT_ROOT
            / "data/processed/passive_order_large_spread_ratio_10bp_202601.csv"
        ),
    )
    parser.add_argument("--batch-size", type=int, default=1)
    parser.add_argument("--limit-files", type=int)
    parser.add_argument(
        "--threads",
        type=int,
        default=max(1, min(8, os.cpu_count() or 1)),
    )
    parser.add_argument("--memory-limit", default="16GB")
    args = parser.parse_args()

    if args.theta < 0:
        parser.error("--theta must be non-negative")
    if args.batch_size <= 0:
        parser.error("--batch-size must be positive")

    paths = expand_inputs(args.inputs, args.limit_files)
    if not paths:
        raise SystemExit("no parquet files matched")

    con = duckdb.connect()
    con.execute(f"PRAGMA threads={args.threads}")
    con.execute(f"PRAGMA memory_limit='{args.memory_limit}'")
    con.execute("PRAGMA preserve_insertion_order=false")

    started = time.perf_counter()
    write_header = True
    total_rows = 0
    for batch_no, batch in enumerate(chunks(paths, args.batch_size), start=1):
        t0 = time.perf_counter()
        rows = compute_batch(con, batch, args.theta)
        write_rows(args.output, rows, write_header)
        write_header = False
        total_rows += len(rows)
        print(
            f"batch={batch_no} files={len(batch)} result_rows={len(rows)} "
            f"elapsed_sec={time.perf_counter() - t0:.3f}",
            flush=True,
        )

    print(
        f"done files={len(paths)} result_rows={total_rows} "
        f"elapsed_sec={time.perf_counter() - started:.3f} output={args.output}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
