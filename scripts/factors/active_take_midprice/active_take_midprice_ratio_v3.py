#!/usr/bin/env python3
"""
Compute v3 active-take midprice-change ratio for LOB parquet files.

Definition, per symbol/date and continuous auction session only:

    ratio = sum(abs(delta_mid) for active-take TRADE mid moves)
            / sum(abs(delta_mid) for all event mid moves)

Rows are ordered by (date, row_id), and lag is computed inside each
symbol/date/session partition so the open, lunch break, and closing auction are
not mixed into the intraday continuous-auction denominator.
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
    "active_take_mid_gap_ratio",
    "active_take_mid_gap",
    "all_mid_gap",
    "active_take_mid_events",
    "all_mid_move_events",
    "trade_mid_move_events",
    "cancel_mid_move_events",
    "order_add_mid_move_events",
    "continuous_events",
    "valid_lag_events",
]


def expand_inputs(patterns: Sequence[str], limit: int | None = None) -> List[str]:
    paths: List[str] = []
    for pattern in patterns:
        matched = glob.glob(pattern)
        paths.extend(matched or [pattern])
    paths = sorted(dict.fromkeys(paths))
    if limit is not None:
        paths = paths[:limit]
    return paths


def chunks(items: Sequence[str], size: int) -> Iterable[Sequence[str]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def compute_batch(con: duckdb.DuckDBPyConnection, paths: Sequence[str]):
    query = """
WITH base AS (
    SELECT
        regexp_replace(regexp_extract(filename, '[^/]+$'), '\\.parquet$', '') AS symbol,
        date,
        row_id,
        CASE
            WHEN time >= 93000000 AND time < 113000000 THEN 'AM'
            WHEN time >= 130000000 AND time < 145700000 THEN 'PM'
            ELSE NULL
        END AS session,
        source_action,
        source_side,
        bid_px[1]::BIGINT AS bid1,
        ask_px[1]::BIGINT AS ask1,
        ((bid_px[1]::DOUBLE + ask_px[1]::DOUBLE) / 2.0) AS mid
    FROM read_parquet(?, filename=true)
    WHERE
        ((time >= 93000000 AND time < 113000000)
         OR (time >= 130000000 AND time < 145700000))
        AND array_length(bid_px) > 0
        AND array_length(ask_px) > 0
),
w AS (
    SELECT
        *,
        lag(mid) OVER (
            PARTITION BY symbol, date, session
            ORDER BY row_id
        ) AS prev_mid,
        lag(bid1) OVER (
            PARTITION BY symbol, date, session
            ORDER BY row_id
        ) AS prev_bid1,
        lag(ask1) OVER (
            PARTITION BY symbol, date, session
            ORDER BY row_id
        ) AS prev_ask1
    FROM base
),
e AS (
    SELECT
        *,
        mid - prev_mid AS delta_mid,
        abs(mid - prev_mid) AS abs_delta_mid,
        (
            source_action = 'TRADE'
            AND (
                (source_side = 'B' AND ask1 > prev_ask1 AND mid > prev_mid)
                OR
                (source_side = 'S' AND bid1 < prev_bid1 AND mid < prev_mid)
            )
        ) AS is_active_take_mid
    FROM w
    WHERE prev_mid IS NOT NULL
)
SELECT
    symbol,
    date,
    CASE
        WHEN sum(abs_delta_mid) = 0 THEN 0.0
        ELSE sum(CASE WHEN is_active_take_mid THEN abs_delta_mid ELSE 0.0 END)
             / sum(abs_delta_mid)
    END AS active_take_mid_gap_ratio,
    sum(CASE WHEN is_active_take_mid THEN abs_delta_mid ELSE 0.0 END) / 10000.0
        AS active_take_mid_gap,
    sum(abs_delta_mid) / 10000.0 AS all_mid_gap,
    count(*) FILTER (WHERE is_active_take_mid) AS active_take_mid_events,
    count(*) FILTER (WHERE abs_delta_mid > 0) AS all_mid_move_events,
    count(*) FILTER (WHERE source_action = 'TRADE' AND abs_delta_mid > 0)
        AS trade_mid_move_events,
    count(*) FILTER (WHERE source_action = 'CANCEL' AND abs_delta_mid > 0)
        AS cancel_mid_move_events,
    count(*) FILTER (WHERE source_action = 'ORDER_ADD' AND abs_delta_mid > 0)
        AS order_add_mid_move_events,
    count(*) + 2 AS continuous_events,
    count(*) AS valid_lag_events
FROM e
GROUP BY symbol, date
ORDER BY symbol, date
"""
    return con.execute(query, [list(paths)]).fetchall()


def write_rows(path: str, rows, write_header: bool) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    mode = "w" if write_header else "a"
    with open(path, mode, newline="") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(FIELDS)
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "inputs",
        nargs="*",
        default=["/hdd_data/lob/event_full_depth_v3/202601/*.parquet"],
        help="Parquet path/glob. Defaults to v3 202601 full market.",
    )
    parser.add_argument(
        "-o",
        "--output",
        default=str(PROJECT_ROOT / "data/processed/active_take_midprice_ratio_v3.csv"),
    )
    parser.add_argument("--batch-size", type=int, default=1)
    parser.add_argument("--limit-files", type=int)
    parser.add_argument("--threads", type=int, default=max(1, min(8, os.cpu_count() or 1)))
    parser.add_argument("--memory-limit", default="16GB")
    args = parser.parse_args()

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
        rows = compute_batch(con, batch)
        write_rows(args.output, rows, write_header)
        write_header = False
        total_rows += len(rows)
        elapsed = time.perf_counter() - t0
        print(
            f"batch={batch_no} files={len(batch)} result_rows={len(rows)} "
            f"elapsed_sec={elapsed:.3f}",
            flush=True,
        )

    total_elapsed = time.perf_counter() - started
    print(
        f"done files={len(paths)} result_rows={total_rows} "
        f"elapsed_sec={total_elapsed:.3f} output={args.output}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
