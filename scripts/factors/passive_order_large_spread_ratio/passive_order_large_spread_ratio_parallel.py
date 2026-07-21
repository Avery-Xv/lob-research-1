#!/usr/bin/env python3
"""Parallel file-level runner for the passive-order large-spread factor."""

from __future__ import annotations

import argparse
import csv
import os
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Sequence

import duckdb

from passive_order_large_spread_ratio import FIELDS, compute_batch, expand_inputs

PROJECT_ROOT = Path(__file__).resolve().parents[3]


def compute_one(path: str, theta: float, memory_limit: str):
    con = duckdb.connect()
    con.execute("PRAGMA threads=1")
    con.execute(f"PRAGMA memory_limit='{memory_limit}'")
    con.execute("PRAGMA preserve_insertion_order=false")
    try:
        return path, compute_batch(con, [path], theta)
    finally:
        con.close()


def append_rows(
    output: str,
    rows: Sequence[Sequence[object]],
    write_header: bool,
) -> None:
    Path(output).parent.mkdir(parents=True, exist_ok=True)
    with open(output, "w" if write_header else "a", newline="") as file:
        writer = csv.writer(file)
        if write_header:
            writer.writerow(FIELDS)
        writer.writerows(rows)


def completed_symbols(output: str) -> set[str]:
    path = Path(output)
    if not path.exists():
        return set()
    with path.open(newline="") as file:
        return {row["symbol"] for row in csv.DictReader(file)}


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Compute passive-order large-spread ratios in parallel."
    )
    parser.add_argument("inputs", nargs="+", help="Parquet path/glob.")
    parser.add_argument("--theta", type=float, default=0.001)
    parser.add_argument("-o", "--output", required=True)
    parser.add_argument("--limit-files", type=int)
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Skip symbols already present in output and append new rows.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=max(1, min(8, os.cpu_count() or 1)),
    )
    parser.add_argument("--memory-limit-per-worker", default="4GB")
    args = parser.parse_args()

    if args.theta < 0:
        parser.error("--theta must be non-negative")
    if args.workers <= 0:
        parser.error("--workers must be positive")

    paths = expand_inputs(args.inputs, args.limit_files)
    existing_symbols = completed_symbols(args.output) if args.resume else set()
    if existing_symbols:
        paths = [path for path in paths if Path(path).stem not in existing_symbols]
    paths.sort(key=os.path.getsize)
    if not paths:
        print("no pending parquet files")
        return 0

    started = time.perf_counter()
    completed = 0
    result_rows = 0
    write_header = not (args.resume and Path(args.output).exists())
    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        futures = [
            executor.submit(
                compute_one,
                path,
                args.theta,
                args.memory_limit_per_worker,
            )
            for path in paths
        ]
        for future in as_completed(futures):
            path, rows = future.result()
            append_rows(args.output, rows, write_header)
            write_header = False
            completed += 1
            result_rows += len(rows)
            if completed % 50 == 0 or completed == len(paths):
                print(
                    f"done={completed}/{len(paths)} "
                    f"last_file={os.path.basename(path)} "
                    f"rows={result_rows} "
                    f"elapsed_sec={time.perf_counter() - started:.1f}",
                    flush=True,
                )

    print(
        f"done files={len(paths)} result_rows={result_rows} "
        f"elapsed_sec={time.perf_counter() - started:.1f} output={args.output}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
