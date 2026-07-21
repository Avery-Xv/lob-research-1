#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import os
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Sequence

import duckdb

from active_take_midprice_ratio_v3 import FIELDS, compute_batch, expand_inputs

PROJECT_ROOT = Path(__file__).resolve().parents[3]


def compute_one(path: str, threads: int, memory_limit: str):
    con = duckdb.connect()
    con.execute(f"PRAGMA threads={threads}")
    con.execute(f"PRAGMA memory_limit='{memory_limit}'")
    con.execute("PRAGMA preserve_insertion_order=false")
    rows = compute_batch(con, [path])
    con.close()
    return path, rows


def append_rows(output: str, rows: Sequence[Sequence[object]], write_header: bool) -> None:
    Path(output).parent.mkdir(parents=True, exist_ok=True)
    mode = "w" if write_header else "a"
    with open(output, mode, newline="") as f:
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
    )
    parser.add_argument(
        "-o",
        "--output",
        default=str(PROJECT_ROOT / "data/processed/active_take_midprice_ratio_v3_full_parallel.csv"),
    )
    parser.add_argument("--limit-files", type=int)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--threads-per-worker", type=int, default=2)
    parser.add_argument("--memory-limit-per-worker", default="8GB")
    args = parser.parse_args()

    paths = expand_inputs(args.inputs, args.limit_files)
    if not paths:
        raise SystemExit("no parquet files matched")

    started = time.perf_counter()
    write_header = True
    done = 0
    result_rows = 0
    with ProcessPoolExecutor(max_workers=args.workers) as ex:
        futures = [
            ex.submit(compute_one, p, args.threads_per_worker, args.memory_limit_per_worker)
            for p in paths
        ]
        for fut in as_completed(futures):
            path, rows = fut.result()
            append_rows(args.output, rows, write_header)
            write_header = False
            done += 1
            result_rows += len(rows)
            elapsed = time.perf_counter() - started
            print(
                f"done={done}/{len(paths)} file={os.path.basename(path)} "
                f"rows={len(rows)} elapsed_sec={elapsed:.1f}",
                flush=True,
            )

    elapsed = time.perf_counter() - started
    print(
        f"done files={len(paths)} result_rows={result_rows} "
        f"elapsed_sec={elapsed:.1f} output={args.output}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
