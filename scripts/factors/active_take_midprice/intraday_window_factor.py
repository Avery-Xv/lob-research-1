#!/usr/bin/env python3
"""Compute intraday active-take midprice factors from LOB parquet snapshots."""

from __future__ import annotations

import argparse
import csv
import glob
import os
import random
import time
from pathlib import Path
from typing import Sequence

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[3]

FIELDS = [
    "symbol",
    "date",
    "window_start",
    "window_end",
    "start_mid",
    "active_take_mid_gap",
    "active_take_mid_gap_signed",
    "all_mid_gap",
    "active_take_mid_gap_ratio",
    "active_take_mid_gap_over_start_mid",
    "active_take_mid_gap_signed_over_start_mid",
    "active_take_mid_events",
    "all_mid_move_events",
    "valid_lag_events",
]


def chunks(items: Sequence[str], size: int):
    for i in range(0, len(items), size):
        yield items[i : i + size]


def compute_batch(con: duckdb.DuckDBPyConnection, paths: Sequence[str], start_time: int, end_time: int):
    query = """
WITH base AS (
    SELECT
        regexp_replace(regexp_extract(filename, '[^/]+$'), '\\.parquet$', '') AS symbol,
        date,
        row_id,
        time,
        source_action,
        source_side,
        bid_px[1]::BIGINT AS bid1,
        ask_px[1]::BIGINT AS ask1,
        ((bid_px[1]::DOUBLE + ask_px[1]::DOUBLE) / 2.0) AS mid
    FROM read_parquet(?, filename=true)
    WHERE time >= ?
      AND time < ?
      AND array_length(bid_px) > 0
      AND array_length(ask_px) > 0
),
w AS (
    SELECT
        *,
        first_value(mid) OVER (PARTITION BY symbol, date ORDER BY row_id) AS start_mid,
        lag(mid) OVER (PARTITION BY symbol, date ORDER BY row_id) AS prev_mid,
        lag(bid1) OVER (PARTITION BY symbol, date ORDER BY row_id) AS prev_bid1,
        lag(ask1) OVER (PARTITION BY symbol, date ORDER BY row_id) AS prev_ask1
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
    ? AS window_start,
    ? AS window_end,
    any_value(start_mid) / 10000.0 AS start_mid,
    sum(CASE WHEN is_active_take_mid THEN abs_delta_mid ELSE 0.0 END) / 10000.0 AS active_take_mid_gap,
    sum(CASE WHEN is_active_take_mid THEN delta_mid ELSE 0.0 END) / 10000.0
        AS active_take_mid_gap_signed,
    sum(abs_delta_mid) / 10000.0 AS all_mid_gap,
    CASE
        WHEN sum(abs_delta_mid) = 0 THEN 0.0
        ELSE sum(CASE WHEN is_active_take_mid THEN abs_delta_mid ELSE 0.0 END) / sum(abs_delta_mid)
    END AS active_take_mid_gap_ratio,
    CASE
        WHEN any_value(start_mid) = 0 THEN 0.0
        ELSE sum(CASE WHEN is_active_take_mid THEN abs_delta_mid ELSE 0.0 END) / any_value(start_mid)
    END AS active_take_mid_gap_over_start_mid,
    CASE
        WHEN any_value(start_mid) = 0 THEN 0.0
        ELSE sum(CASE WHEN is_active_take_mid THEN delta_mid ELSE 0.0 END) / any_value(start_mid)
    END AS active_take_mid_gap_signed_over_start_mid,
    count(*) FILTER (WHERE is_active_take_mid) AS active_take_mid_events,
    count(*) FILTER (WHERE abs_delta_mid > 0) AS all_mid_move_events,
    count(*) AS valid_lag_events
FROM e
GROUP BY symbol, date
ORDER BY symbol, date
"""
    return con.execute(query, [list(paths), start_time, end_time, start_time, end_time]).fetchall()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("inputs", nargs="*", default=["/hdd_data/lob/event_full_depth_v3/202601/*.parquet"])
    parser.add_argument("--file-list", help="Text file with one parquet path per line.")
    parser.add_argument("--start-time", type=int, default=93000000)
    parser.add_argument("--end-time", type=int, default=100000000)
    parser.add_argument(
        "--output",
        default=str(PROJECT_ROOT / "data/cache/intraday_factor_0930_1000_202601.csv"),
    )
    parser.add_argument("--batch-size", type=int, default=20)
    parser.add_argument("--limit-files", type=int)
    parser.add_argument("--sample-files", type=int, help="Randomly sample this many input files after glob expansion.")
    parser.add_argument("--append", action="store_true", help="Append rows to an existing compatible output.")
    parser.add_argument("--seed", type=int, default=20260716)
    parser.add_argument("--threads", type=int, default=max(1, min(8, os.cpu_count() or 1)))
    parser.add_argument("--memory-limit", default="16GB")
    args = parser.parse_args()

    paths = []
    if args.file_list:
        with open(args.file_list) as f:
            paths.extend(line.strip() for line in f if line.strip())
    for pattern in args.inputs:
        if args.file_list and pattern == "/hdd_data/lob/event_full_depth_v3/202601/*.parquet":
            continue
        paths.extend(glob.glob(pattern) or [pattern])
    paths = sorted(dict.fromkeys(paths))
    if args.sample_files:
        rng = random.Random(args.seed)
        if args.sample_files < len(paths):
            paths = sorted(rng.sample(paths, args.sample_files))
    if args.limit_files:
        paths = paths[: args.limit_files]
    if not paths:
        raise SystemExit("no input parquet files matched")

    con = duckdb.connect()
    con.execute(f"PRAGMA threads={args.threads}")
    con.execute(f"PRAGMA memory_limit='{args.memory_limit}'")
    con.execute("PRAGMA preserve_insertion_order=false")

    total = 0
    started = time.perf_counter()
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    mode = "a" if args.append else "w"
    if args.append:
        with open(args.output, newline="") as existing:
            header = next(csv.reader(existing), None)
        if header != FIELDS:
            raise ValueError(f"incompatible or missing output header: {args.output}")
    with open(args.output, mode, newline="") as f:
        writer = csv.writer(f)
        if not args.append:
            writer.writerow(FIELDS)
        for i, batch in enumerate(chunks(paths, args.batch_size), start=1):
            t0 = time.perf_counter()
            rows = compute_batch(con, batch, args.start_time, args.end_time)
            writer.writerows(rows)
            total += len(rows)
            print(f"batch={i} files={len(batch)} rows={len(rows)} elapsed={time.perf_counter()-t0:.2f}s", flush=True)
    print(f"done files={len(paths)} rows={total} elapsed={time.perf_counter()-started:.2f}s output={args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
