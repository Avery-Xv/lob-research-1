#!/usr/bin/env python3
"""Build daily typical quoted spread and strictly lagged five-day theta."""

from __future__ import annotations

import argparse
import csv
import glob
import math
import os
import time
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from statistics import median
from typing import Iterable, Sequence

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[3]
RAW_FIELDS = [
    "symbol",
    "date",
    "daily_typical_spread_raw",
    "valid_spread_snapshots",
]
THETA_FIELDS = [
    "symbol",
    "date",
    "daily_typical_spread_raw",
    "daily_typical_spread",
    "theta_5d_raw",
    "theta_5d",
    "history_days",
    "valid_spread_snapshots",
]


def expand_inputs(patterns: Sequence[str]) -> list[str]:
    paths: list[str] = []
    for pattern in patterns:
        paths.extend(glob.glob(pattern) or [pattern])
    return sorted(dict.fromkeys(paths), key=lambda path: (os.path.getsize(path), path))


def task_key(path: str) -> tuple[str, str]:
    file_path = Path(path)
    return file_path.stem, file_path.parent.name


def compute_one(path: str, memory_limit: str):
    con = duckdb.connect()
    con.execute("PRAGMA threads=1")
    con.execute(f"PRAGMA memory_limit='{memory_limit}'")
    con.execute("PRAGMA preserve_insertion_order=false")
    query = """
SELECT
    regexp_replace(regexp_extract(filename, '[^/]+$'), '\\.parquet$', '') AS symbol,
    date,
    median((ask_px[1] - bid_px[1])::DOUBLE) AS daily_typical_spread_raw,
    count(*) AS valid_spread_snapshots
FROM read_parquet(?, filename=true)
WHERE ((time >= 93000000 AND time < 113000000)
       OR (time >= 130000000 AND time < 145700000))
  AND array_length(bid_px) > 0
  AND array_length(ask_px) > 0
  AND bid_px[1] > 0
  AND ask_px[1] > bid_px[1]
GROUP BY symbol, date
ORDER BY date
"""
    try:
        return path, con.execute(query, [path]).fetchall()
    finally:
        con.close()


def read_completed_tasks(path: str) -> set[tuple[str, str]]:
    if not Path(path).exists():
        return set()
    completed = set()
    with open(path, newline="") as handle:
        for row in csv.DictReader(handle):
            completed.add((row["symbol"], row["date"][:6]))
    return completed


def append_rows(path: str, rows: Iterable[Sequence[object]], header: bool) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w" if header else "a", newline="") as handle:
        writer = csv.writer(handle)
        if header:
            writer.writerow(RAW_FIELDS)
        writer.writerows(rows)


def build_theta(raw_path: str, output_path: str, target_months: set[str]) -> int:
    observations: dict[tuple[str, str], tuple[float, int]] = {}
    calendar = set()
    with open(raw_path, newline="") as handle:
        for row in csv.DictReader(handle):
            spread = float(row["daily_typical_spread_raw"])
            if not math.isfinite(spread) or spread <= 0:
                continue
            key = row["symbol"], row["date"]
            observations[key] = (spread, int(row["valid_spread_snapshots"]))
            calendar.add(row["date"])

    dates = sorted(calendar)
    previous_dates = {date: dates[max(0, index - 5) : index] for index, date in enumerate(dates)}
    output_rows = []
    for (symbol, date), (daily_spread, snapshots) in sorted(observations.items()):
        if date[:6] not in target_months:
            continue
        history = [
            observations[(symbol, previous_date)][0]
            for previous_date in previous_dates[date]
            if (symbol, previous_date) in observations
        ]
        theta = median(history) if len(history) == 5 else None
        output_rows.append(
            [
                symbol,
                date,
                daily_spread,
                daily_spread / 10000.0,
                theta if theta is not None else "",
                theta / 10000.0 if theta is not None else "",
                len(history),
                snapshots,
            ]
        )

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(THETA_FIELDS)
        writer.writerows(output_rows)
    return len(output_rows)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("inputs", nargs="+", help="Monthly parquet globs.")
    parser.add_argument("--raw-output", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--target-months", nargs="+", required=True)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--memory-limit-per-worker", default="2GB")
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()

    paths = expand_inputs(args.inputs)
    completed = read_completed_tasks(args.raw_output) if args.resume else set()
    paths = [path for path in paths if task_key(path) not in completed]
    write_header = not (args.resume and Path(args.raw_output).exists())
    started = time.perf_counter()

    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        futures = [
            executor.submit(compute_one, path, args.memory_limit_per_worker)
            for path in paths
        ]
        for index, future in enumerate(as_completed(futures), start=1):
            path, rows = future.result()
            append_rows(args.raw_output, rows, write_header)
            write_header = False
            if index % 100 == 0 or index == len(paths):
                print(
                    f"done={index}/{len(paths)} last={Path(path).name} "
                    f"elapsed_sec={time.perf_counter() - started:.1f}",
                    flush=True,
                )

    output_rows = build_theta(
        args.raw_output,
        args.output,
        set(args.target_months),
    )
    print(f"theta_rows={output_rows} output={args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
