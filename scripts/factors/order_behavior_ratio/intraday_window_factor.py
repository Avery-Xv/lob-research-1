#!/usr/bin/env python3
"""Compute intraday volume/count/order-size log ratios from v3 LOB events."""

from __future__ import annotations

import argparse
import csv
import glob
import math
import os
import time
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
    "trade_qty",
    "trade_count",
    "aggr_order_count",
    "passive_submit_qty",
    "passive_order_count",
    "vr_log",
    "cr_log",
    "single_size_ratio_log",
    "aggressive_order_add_qty_excluded",
    "aggressive_order_add_count_excluded",
    "unidentified_aggr_trade_qty",
    "unidentified_aggr_trade_count",
    "duplicate_trade_rows_excluded",
    "invalid_order_add_count",
    "is_valid",
    "invalid_reason",
]


def validate_window(start_time: int, end_time: int) -> None:
    if start_time >= end_time:
        raise ValueError("start-time must be earlier than end-time")
    if not any(
        session_start <= start_time < end_time <= session_end
        for session_start, session_end in CONTINUOUS_SESSIONS
    ):
        raise ValueError("window must stay within one continuous-auction session")


def calculate_log_factors(
    trade_qty: int,
    aggr_order_count: int,
    passive_submit_qty: int,
    passive_order_count: int,
) -> tuple[float | None, float | None, float | None]:
    """Return unsmoothed log ratios; zero denominators produce missing values."""
    vr_log = (
        math.log(trade_qty) - math.log(passive_submit_qty)
        if trade_qty > 0 and passive_submit_qty > 0
        else None
    )
    cr_log = (
        math.log(aggr_order_count) - math.log(passive_order_count)
        if aggr_order_count > 0 and passive_order_count > 0
        else None
    )
    single_size_ratio_log = (
        vr_log - cr_log if vr_log is not None and cr_log is not None else None
    )
    return vr_log, cr_log, single_size_ratio_log


def expand_inputs(patterns: Sequence[str], file_list: str | None = None) -> list[str]:
    paths: list[str] = []
    if file_list:
        with open(file_list) as handle:
            paths.extend(line.strip() for line in handle if line.strip())
    for pattern in patterns:
        paths.extend(glob.glob(pattern) or [pattern])
    return sorted(dict.fromkeys(paths), key=lambda path: (os.path.getsize(path), path))


def task_key(path: str) -> tuple[str, str]:
    file_path = Path(path)
    return file_path.stem, file_path.parent.name


def read_completed_tasks(
    output: str,
    start_time: int,
    end_time: int,
) -> set[tuple[str, str]]:
    if not Path(output).exists():
        return set()
    completed: set[tuple[str, str]] = set()
    with open(output, newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != FIELDS:
            raise ValueError(f"incompatible output header: {output}")
        for row in reader:
            if int(row["window_start"]) != start_time or int(row["window_end"]) != end_time:
                raise ValueError(f"existing output uses a different window: {output}")
            completed.add((row["symbol"], row["date"][:6]))
    return completed


def compute_one(
    path: str,
    start_time: int,
    end_time: int,
    memory_limit: str,
) -> tuple[str, list[tuple]]:
    """Compute all dates in one symbol-month parquet file.

    Events are read from the start of the continuous session through the signal
    time. This is required for Shanghai orders whose immediate TRADE triggers
    precede publication of an ORDER_ADD remainder. Shenzhen's ORDER_ADD then
    TRADE ordering is handled by the same active-order-ID exclusion.
    """
    session_start = next(
        session_start
        for session_start, session_end in CONTINUOUS_SESSIONS
        if session_start <= start_time < end_time <= session_end
    )
    con = duckdb.connect()
    con.execute("PRAGMA threads=1")
    con.execute(f"PRAGMA memory_limit='{memory_limit}'")
    temp_directory = Path("/tmp") / f"order_behavior_duckdb_{os.getpid()}"
    temp_directory.mkdir(parents=True, exist_ok=True)
    escaped_temp_directory = str(temp_directory).replace("'", "''")
    con.execute(f"PRAGMA temp_directory='{escaped_temp_directory}'")
    con.execute("PRAGMA preserve_insertion_order=false")
    query = """
WITH raw_events AS (
    SELECT
        regexp_replace(regexp_extract(filename, '[^/]+$'), '\\.parquet$', '') AS symbol,
        date,
        time,
        row_id,
        source_action,
        source_order_id,
        source_trade_id,
        source_buy_order_id,
        source_sell_order_id,
        source_side,
        source_volume
    FROM read_parquet(?, filename=true)
    WHERE time >= ?
      AND time < ?
),
trade_rows AS (
    SELECT
        *,
        CASE
            WHEN source_side = 'B' THEN source_buy_order_id
            WHEN source_side = 'S' THEN source_sell_order_id
            ELSE NULL
        END AS active_order_id,
        row_number() OVER (
            PARTITION BY symbol, date, coalesce(source_trade_id, -row_id)
            ORDER BY row_id
        ) AS trade_occurrence
    FROM raw_events
    WHERE source_action = 'TRADE'
      AND source_volume > 0
),
trades AS (
    SELECT *
    FROM trade_rows
    WHERE trade_occurrence = 1
),
active_order_ids AS (
    SELECT DISTINCT symbol, date, active_order_id
    FROM trades
    WHERE active_order_id IS NOT NULL
),
window_trades AS (
    SELECT *
    FROM trades
    WHERE time >= ?
      AND time < ?
),
trade_agg AS (
    SELECT
        symbol,
        date,
        sum(source_volume)::BIGINT AS trade_qty,
        count(*)::BIGINT AS trade_count,
        count(DISTINCT active_order_id)::BIGINT AS aggr_order_count,
        coalesce(sum(source_volume) FILTER (WHERE active_order_id IS NULL), 0)::BIGINT
            AS unidentified_aggr_trade_qty,
        count(*) FILTER (WHERE active_order_id IS NULL)::BIGINT
            AS unidentified_aggr_trade_count
    FROM window_trades
    GROUP BY symbol, date
),
window_order_rows AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY symbol, date, source_order_id
            ORDER BY row_id
        ) AS order_occurrence
    FROM raw_events
    WHERE time >= ?
      AND time < ?
      AND source_action = 'ORDER_ADD'
),
valid_order_adds AS (
    SELECT
        o.*,
        a.active_order_id IS NOT NULL AS is_aggressive_order
    FROM window_order_rows o
    LEFT JOIN active_order_ids a
      ON a.symbol = o.symbol
     AND a.date = o.date
     AND a.active_order_id = o.source_order_id
    WHERE o.source_order_id IS NOT NULL
      AND o.source_volume > 0
      AND o.order_occurrence = 1
),
order_agg AS (
    SELECT
        symbol,
        date,
        coalesce(sum(source_volume) FILTER (WHERE NOT is_aggressive_order), 0)::BIGINT
            AS passive_submit_qty,
        count(*) FILTER (WHERE NOT is_aggressive_order)::BIGINT
            AS passive_order_count,
        coalesce(sum(source_volume) FILTER (WHERE is_aggressive_order), 0)::BIGINT
            AS aggressive_order_add_qty_excluded,
        count(*) FILTER (WHERE is_aggressive_order)::BIGINT
            AS aggressive_order_add_count_excluded
    FROM valid_order_adds
    GROUP BY symbol, date
),
invalid_orders AS (
    SELECT
        symbol,
        date,
        count(*) FILTER (
            WHERE source_order_id IS NULL OR source_volume IS NULL OR source_volume <= 0
        )::BIGINT AS invalid_order_add_count
    FROM window_order_rows
    GROUP BY symbol, date
),
duplicate_trades AS (
    SELECT
        symbol,
        date,
        count(*) FILTER (
            WHERE time >= ? AND time < ? AND trade_occurrence > 1
        )::BIGINT AS duplicate_trade_rows_excluded
    FROM trade_rows
    GROUP BY symbol, date
),
dates AS (
    SELECT symbol, date FROM trade_agg
    UNION
    SELECT symbol, date FROM order_agg
    UNION
    SELECT symbol, date FROM invalid_orders
),
metrics AS (
    SELECT
        d.symbol,
        d.date,
        coalesce(t.trade_qty, 0)::BIGINT AS trade_qty,
        coalesce(t.trade_count, 0)::BIGINT AS trade_count,
        coalesce(t.aggr_order_count, 0)::BIGINT AS aggr_order_count,
        coalesce(o.passive_submit_qty, 0)::BIGINT AS passive_submit_qty,
        coalesce(o.passive_order_count, 0)::BIGINT AS passive_order_count,
        coalesce(o.aggressive_order_add_qty_excluded, 0)::BIGINT
            AS aggressive_order_add_qty_excluded,
        coalesce(o.aggressive_order_add_count_excluded, 0)::BIGINT
            AS aggressive_order_add_count_excluded,
        coalesce(t.unidentified_aggr_trade_qty, 0)::BIGINT
            AS unidentified_aggr_trade_qty,
        coalesce(t.unidentified_aggr_trade_count, 0)::BIGINT
            AS unidentified_aggr_trade_count,
        coalesce(dt.duplicate_trade_rows_excluded, 0)::BIGINT
            AS duplicate_trade_rows_excluded,
        coalesce(io.invalid_order_add_count, 0)::BIGINT AS invalid_order_add_count
    FROM dates d
    LEFT JOIN trade_agg t USING (symbol, date)
    LEFT JOIN order_agg o USING (symbol, date)
    LEFT JOIN invalid_orders io USING (symbol, date)
    LEFT JOIN duplicate_trades dt USING (symbol, date)
)
SELECT
    symbol,
    date,
    ?::BIGINT AS window_start,
    ?::BIGINT AS window_end,
    trade_qty,
    trade_count,
    aggr_order_count,
    passive_submit_qty,
    passive_order_count,
    CASE
        WHEN trade_qty > 0 AND passive_submit_qty > 0
            THEN ln(trade_qty::DOUBLE) - ln(passive_submit_qty::DOUBLE)
        ELSE NULL
    END AS vr_log,
    CASE
        WHEN aggr_order_count > 0 AND passive_order_count > 0
            THEN ln(aggr_order_count::DOUBLE) - ln(passive_order_count::DOUBLE)
        ELSE NULL
    END AS cr_log,
    CASE
        WHEN trade_qty > 0 AND passive_submit_qty > 0
         AND aggr_order_count > 0 AND passive_order_count > 0
            THEN (ln(trade_qty::DOUBLE) - ln(passive_submit_qty::DOUBLE))
               - (ln(aggr_order_count::DOUBLE) - ln(passive_order_count::DOUBLE))
        ELSE NULL
    END AS single_size_ratio_log,
    aggressive_order_add_qty_excluded,
    aggressive_order_add_count_excluded,
    unidentified_aggr_trade_qty,
    unidentified_aggr_trade_count,
    duplicate_trade_rows_excluded,
    invalid_order_add_count,
    trade_qty > 0
        AND aggr_order_count > 0
        AND passive_submit_qty > 0
        AND passive_order_count > 0
        AND unidentified_aggr_trade_count = 0
        AND invalid_order_add_count = 0 AS is_valid,
    concat_ws(';',
        CASE WHEN trade_qty = 0 THEN 'zero_trade_qty' END,
        CASE WHEN aggr_order_count = 0 THEN 'zero_aggr_order_count' END,
        CASE WHEN passive_submit_qty = 0 THEN 'zero_passive_submit_qty' END,
        CASE WHEN passive_order_count = 0 THEN 'zero_passive_order_count' END,
        CASE WHEN unidentified_aggr_trade_count > 0 THEN 'unidentified_aggressor' END,
        CASE WHEN invalid_order_add_count > 0 THEN 'invalid_order_adds_present' END
    ) AS invalid_reason
FROM metrics
ORDER BY date
"""
    parameters = [
        path,
        session_start,
        end_time,
        start_time,
        end_time,
        start_time,
        end_time,
        start_time,
        end_time,
        start_time,
        end_time,
    ]
    try:
        return path, con.execute(query, parameters).fetchall()
    finally:
        con.close()


def append_rows(output: str, rows: Sequence[tuple], write_header: bool) -> None:
    Path(output).parent.mkdir(parents=True, exist_ok=True)
    with open(output, "w" if write_header else "a", newline="") as handle:
        writer = csv.writer(handle)
        if write_header:
            writer.writerow(FIELDS)
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Compute vr_log, cr_log, and single_size_ratio_log from v3 events."
    )
    parser.add_argument(
        "inputs",
        nargs="*",
        default=["/hdd_data/lob/event_full_depth_v3/202601/*.parquet"],
        help="Monthly v3 parquet files or globs.",
    )
    parser.add_argument("--file-list", help="Text file with one parquet path per line.")
    parser.add_argument("--start-time", type=int, default=100000000)
    parser.add_argument("--end-time", type=int, default=103000000)
    parser.add_argument(
        "--output",
        default=str(
            PROJECT_ROOT
            / "data/processed/order_behavior_ratio_1000_1030_202601.csv"
        ),
    )
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--memory-limit-per-worker", default="4GB")
    parser.add_argument("--limit-files", type=int)
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()

    validate_window(args.start_time, args.end_time)
    if args.workers < 1:
        raise ValueError("workers must be at least 1")

    patterns = list(args.inputs)
    if args.file_list and patterns == ["/hdd_data/lob/event_full_depth_v3/202601/*.parquet"]:
        patterns = []
    paths = expand_inputs(patterns, args.file_list)
    if args.limit_files is not None:
        paths = paths[: args.limit_files]
    if not paths:
        raise SystemExit("no input parquet files matched")

    completed = (
        read_completed_tasks(args.output, args.start_time, args.end_time)
        if args.resume
        else set()
    )
    paths = [path for path in paths if task_key(path) not in completed]
    write_header = not (args.resume and Path(args.output).exists())
    started = time.perf_counter()
    factor_rows = 0

    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        futures = [
            executor.submit(
                compute_one,
                path,
                args.start_time,
                args.end_time,
                args.memory_limit_per_worker,
            )
            for path in paths
        ]
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
