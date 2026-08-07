#!/usr/bin/env python3
"""Compute passive large-gap B/S ratios for a fixed intraday window."""

from __future__ import annotations

import argparse
import csv
import glob
import json
import math
import os
import time
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Sequence

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parents[3]
CONTINUOUS_SESSIONS = (
    (93000000, 113000000),
    (130000000, 145700000),
)
FIELDS = [
    "symbol",
    "date",
    "window_start",
    "window_end",
    "theta_5d_raw",
    "total_trade_volume",
    "matched_trade_volume",
    "match_rate",
    "large_gap_buy_volume",
    "large_gap_sell_volume",
    "large_gap_buy_ratio",
    "large_gap_sell_ratio",
    "valid_trade_count",
    "matched_trade_count",
    "large_gap_buy_trade_count",
    "large_gap_sell_trade_count",
    "passes_match_rate",
]


def validate_window(start_time: int, end_time: int) -> None:
    if start_time >= end_time:
        raise ValueError("start-time must be earlier than end-time")
    if not any(
        session_start <= start_time < end_time <= session_end
        for session_start, session_end in CONTINUOUS_SESSIONS
    ):
        raise ValueError("window must stay within one continuous-auction session")


def expand_inputs(patterns: Sequence[str]) -> list[str]:
    paths: list[str] = []
    for pattern in patterns:
        paths.extend(glob.glob(pattern) or [pattern])
    return sorted(dict.fromkeys(paths), key=lambda path: (os.path.getsize(path), path))


def task_key(path: str) -> tuple[str, str]:
    file_path = Path(path)
    return file_path.stem, file_path.parent.name


def load_theta(path: str) -> dict[tuple[str, int], float]:
    result = {}
    with open(path, newline="") as handle:
        for row in csv.DictReader(handle):
            value = row.get("theta_5d_raw", "")
            if not value:
                continue
            theta = float(value)
            if math.isfinite(theta) and theta > 0:
                result[(row["symbol"], int(row["date"]))] = theta
    return result


def read_completed_tasks(
    path: str,
    start_time: int,
    end_time: int,
) -> set[tuple[str, str]]:
    if not Path(path).exists():
        return set()
    completed = set()
    with open(path, newline="") as handle:
        for row in csv.DictReader(handle):
            if int(row["window_start"]) != start_time or int(row["window_end"]) != end_time:
                raise ValueError(f"existing output uses a different window: {path}")
            completed.add((row["symbol"], row["date"][:6]))
    return completed


def compute_one(
    path: str,
    theta_rows: list[tuple[int, float]],
    start_time: int,
    end_time: int,
    minimum_match_rate: float,
    memory_limit: str,
):
    if not theta_rows:
        return path, []
    con = duckdb.connect()
    con.execute("PRAGMA threads=1")
    con.execute(f"PRAGMA memory_limit='{memory_limit}'")
    con.execute("PRAGMA preserve_insertion_order=false")
    con.execute("CREATE TEMP TABLE theta(date INTEGER, theta_5d_raw DOUBLE)")
    con.executemany("INSERT INTO theta VALUES (?, ?)", theta_rows)
    query = """
WITH events AS (
    SELECT
        regexp_replace(regexp_extract(filename, '[^/]+$'), '\\.parquet$', '') AS symbol,
        date,
        time,
        row_id,
        CASE
            WHEN time >= 93000000 AND time < 113000000 THEN 'AM'
            WHEN time >= 130000000 AND time < 145700000 THEN 'PM'
            ELSE NULL
        END AS session,
        source_action,
        source_recid,
        source_side,
        source_price,
        source_volume,
        source_buy_order_id,
        source_sell_order_id,
        bid_px[1]::DOUBLE AS bid1,
        ask_px[1]::DOUBLE AS ask1
    FROM read_parquet(?, filename=true)
    WHERE time >= 93000000
      AND time < ?
),
continuous AS (
    SELECT
        *,
        last_value(
            CASE WHEN bid1 > 0 AND ask1 > bid1 THEN bid1 END IGNORE NULLS
        ) OVER (
            PARTITION BY symbol, date, session ORDER BY row_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS pre_bid1,
        last_value(
            CASE WHEN bid1 > 0 AND ask1 > bid1 THEN ask1 END IGNORE NULLS
        ) OVER (
            PARTITION BY symbol, date, session ORDER BY row_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS pre_ask1
    FROM events
    WHERE session IS NOT NULL
),
orders AS (
    SELECT
        symbol,
        date,
        CASE WHEN source_side = 'B' THEN source_buy_order_id
             WHEN source_side = 'S' THEN source_sell_order_id END AS order_id,
        row_id AS entry_row_id,
        source_side AS order_side,
        CASE
            WHEN source_side = 'B' AND pre_bid1 > 0
                THEN pre_bid1 - source_price
            WHEN source_side = 'S' AND pre_ask1 > 0
                THEN source_price - pre_ask1
            ELSE NULL
        END AS initial_gap,
        row_number() OVER (
            PARTITION BY symbol, date, source_side,
                CASE WHEN source_side = 'B' THEN source_buy_order_id
                     WHEN source_side = 'S' THEN source_sell_order_id END
            ORDER BY row_id
        ) AS occurrence
    FROM continuous
    WHERE source_action = 'ORDER_ADD'
      AND CASE WHEN source_side = 'B' THEN source_buy_order_id
               WHEN source_side = 'S' THEN source_sell_order_id END IS NOT NULL
      AND source_price > 0
),
deduplicated_trades AS (
    SELECT * EXCLUDE (trade_occurrence)
    FROM (
        SELECT
            *,
            row_number() OVER (
                PARTITION BY symbol, date,
                    coalesce(source_recid, -row_id)
                ORDER BY row_id
            ) AS trade_occurrence
        FROM continuous
        WHERE source_action = 'TRADE'
          AND source_side IN ('B', 'S')
          AND source_volume > 0
    )
    WHERE trade_occurrence = 1
),
matched AS (
    SELECT
        t.symbol,
        t.date,
        t.time AS trade_time,
        t.row_id,
        t.source_side AS active_side,
        t.source_volume,
        o.initial_gap,
        th.theta_5d_raw,
        o.order_id IS NOT NULL AS is_matched
    FROM deduplicated_trades t
    INNER JOIN theta th ON th.date = t.date
    LEFT JOIN orders o
      ON o.symbol = t.symbol
     AND o.date = t.date
     AND o.order_id = CASE
            WHEN t.source_side = 'B' THEN t.source_sell_order_id
            WHEN t.source_side = 'S' THEN t.source_buy_order_id
         END
     AND o.order_side = CASE
            WHEN t.source_side = 'B' THEN 'S'
            WHEN t.source_side = 'S' THEN 'B'
         END
     AND o.entry_row_id < t.row_id
     AND o.occurrence = 1
)
SELECT
    symbol,
    date,
    ?::BIGINT AS window_start,
    ?::BIGINT AS window_end,
    any_value(theta_5d_raw) AS theta_5d_raw,
    sum(source_volume)::BIGINT AS total_trade_volume,
    coalesce(sum(source_volume) FILTER (WHERE is_matched), 0)::BIGINT
        AS matched_trade_volume,
    coalesce(sum(source_volume) FILTER (WHERE is_matched), 0)::DOUBLE
        / sum(source_volume) AS match_rate,
    coalesce(sum(source_volume) FILTER (
        WHERE is_matched AND active_side = 'B' AND initial_gap > theta_5d_raw
    ), 0)::BIGINT AS large_gap_buy_volume,
    coalesce(sum(source_volume) FILTER (
        WHERE is_matched AND active_side = 'S' AND initial_gap > theta_5d_raw
    ), 0)::BIGINT AS large_gap_sell_volume,
    coalesce(sum(source_volume) FILTER (
        WHERE is_matched AND active_side = 'B' AND initial_gap > theta_5d_raw
    ), 0)::DOUBLE / sum(source_volume) AS large_gap_buy_ratio,
    coalesce(sum(source_volume) FILTER (
        WHERE is_matched AND active_side = 'S' AND initial_gap > theta_5d_raw
    ), 0)::DOUBLE / sum(source_volume) AS large_gap_sell_ratio,
    count(*) AS valid_trade_count,
    count(*) FILTER (WHERE is_matched) AS matched_trade_count,
    count(*) FILTER (
        WHERE is_matched AND active_side = 'B' AND initial_gap > theta_5d_raw
    ) AS large_gap_buy_trade_count,
    count(*) FILTER (
        WHERE is_matched AND active_side = 'S' AND initial_gap > theta_5d_raw
    ) AS large_gap_sell_trade_count,
    match_rate >= ? AS passes_match_rate
FROM matched
WHERE trade_time >= ?
  AND trade_time < ?
GROUP BY symbol, date
ORDER BY date
"""
    try:
        parameters = [
            path,
            end_time,
            start_time,
            end_time,
            minimum_match_rate,
            start_time,
            end_time,
        ]
        return path, con.execute(query, parameters).fetchall()
    finally:
        con.close()


def append_rows(path: str, rows, header: bool) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w" if header else "a", newline="") as handle:
        writer = csv.writer(handle)
        if header:
            writer.writerow(FIELDS)
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("inputs", nargs="*", help="Monthly parquet globs.")
    parser.add_argument("--file-list", help="Certified stock parquet manifest.")
    parser.add_argument("--universe-metadata", help="Certified stock-universe metadata JSON.")
    parser.add_argument("--exchange", choices=("ALL", "SH", "SZ"), default="ALL")
    parser.add_argument("--theta", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--start-time", type=int, default=100000000)
    parser.add_argument("--end-time", type=int, default=103000000)
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--memory-limit-per-worker", default="4GB")
    parser.add_argument("--minimum-match-rate", type=float, default=0.95)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--limit-files", type=int)
    args = parser.parse_args()

    validate_window(args.start_time, args.end_time)
    if not 0 <= args.minimum_match_rate <= 1:
        raise ValueError("minimum-match-rate must be in [0, 1]")

    theta = load_theta(args.theta)
    theta_by_task: dict[tuple[str, str], list[tuple[int, float]]] = defaultdict(list)
    for (symbol, date), value in theta.items():
        theta_by_task[(symbol, str(date)[:6])].append((date, value))
    for rows in theta_by_task.values():
        rows.sort()

    input_patterns = list(args.inputs)
    if args.file_list:
        input_patterns.extend(
            line.strip() for line in Path(args.file_list).read_text().splitlines()
            if line.strip()
        )
    if not input_patterns:
        raise ValueError("provide --file-list or parquet inputs")
    if args.universe_metadata:
        metadata = json.loads(Path(args.universe_metadata).read_text())
        whitelist = metadata.get("security_type_whitelist", {})
        if metadata.get("output_etf_symbols") != 0:
            raise ValueError("universe metadata does not certify ETF=0")
        if whitelist.get("SecuCategory") != [1] or whitelist.get("SecuMarket") != [83, 90]:
            raise ValueError("universe metadata is not a certified Shanghai/Shenzhen A-share universe")
    paths = expand_inputs(input_patterns)
    if args.exchange != "ALL":
        paths = [path for path in paths if Path(path).stem.startswith(args.exchange)]
    paths = [path for path in paths if task_key(path) in theta_by_task]
    if args.limit_files is not None:
        paths = paths[: args.limit_files]
    completed = (
        read_completed_tasks(args.output, args.start_time, args.end_time)
        if args.resume
        else set()
    )
    paths = [path for path in paths if task_key(path) not in completed]
    write_header = not (args.resume and Path(args.output).exists())
    started = time.perf_counter()

    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        futures = []
        for path in paths:
            symbol, month = task_key(path)
            futures.append(
                executor.submit(
                    compute_one,
                    path,
                    theta_by_task.get((symbol, month), []),
                    args.start_time,
                    args.end_time,
                    args.minimum_match_rate,
                    args.memory_limit_per_worker,
                )
            )

        factor_rows = 0
        for index, future in enumerate(as_completed(futures), start=1):
            path, rows = future.result()
            append_rows(args.output, rows, write_header)
            write_header = False
            factor_rows += len(rows)
            if index % 25 == 0 or index == len(paths):
                print(
                    f"done={index}/{len(paths)} rows={factor_rows} "
                    f"last={Path(path).name} elapsed_sec={time.perf_counter()-started:.1f}",
                    flush=True,
                )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
