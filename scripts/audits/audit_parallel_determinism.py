#!/usr/bin/env python3
"""Compare serial and spawned-process results on frozen real SH/SZ files."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timezone
from multiprocessing import get_context
from pathlib import Path
from typing import Any, Callable

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from scripts.factors.joint_large_gap_order_behavior.compute_v4 import compute_one as joint_compute
from scripts.factors.order_behavior_ratio.intraday_window_factor import compute_one as behavior_compute
from scripts.factors.passive_large_gap_ratio.intraday_window_factor import compute_one as passive_compute


IMPLEMENTATIONS = (
    "scripts/factors/order_behavior_ratio/intraday_window_factor.py",
    "scripts/factors/passive_large_gap_ratio/intraday_window_factor.py",
    "scripts/factors/joint_large_gap_order_behavior/compute_v4.py",
)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def canonical(result: tuple[str, list[tuple]]) -> str:
    return hashlib.sha256(
        json.dumps(sorted(result[1]), default=str, separators=(",", ":")).encode()
    ).hexdigest()


def compare(name: str, function: Callable[..., Any], tasks: list[tuple]) -> dict[str, Any]:
    serial = [function(*task) for task in tasks]
    with ProcessPoolExecutor(max_workers=2, mp_context=get_context("spawn")) as executor:
        parallel = [future.result() for future in [executor.submit(function, *task) for task in tasks]]
    serial_hashes = {Path(result[0]).stem if name != "joint" else result[0]: canonical(result) for result in serial}
    parallel_hashes = {Path(result[0]).stem if name != "joint" else result[0]: canonical(result) for result in parallel}
    return {
        "status": "PASS" if serial_hashes == parallel_hashes else "FAIL",
        "serial_hashes": serial_hashes, "parallel_hashes": parallel_hashes,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--file-list", type=Path, required=True)
    parser.add_argument("--calendar", type=Path, required=True)
    parser.add_argument("--month", default="202601")
    parser.add_argument("--symbols", nargs="+", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise SystemExit(f"Refusing to overwrite: {args.output}")
    paths: dict[str, dict[str, str]] = {}
    for line in args.file_list.read_text(encoding="utf-8").splitlines():
        path = Path(line)
        if path.stem in args.symbols:
            paths.setdefault(path.stem, {})[path.parent.name] = str(path)
    missing = set(args.symbols) - set(paths)
    if missing:
        raise SystemExit(f"Missing symbols: {sorted(missing)}")
    target_paths = [paths[symbol][args.month] for symbol in args.symbols]
    con = duckdb.connect()
    dates = sorted(row[0] for row in con.execute(
        "SELECT DISTINCT date FROM read_parquet(?) ORDER BY date", [target_paths[0]]
    ).fetchall())
    con.close()
    theta = [(date, 1.0) for date in dates]
    calendar_dates = [
        int(line.split(",")[0]) for line in args.calendar.read_text().splitlines()[1:]
        if line.strip()
    ]
    evidence = {
        "order_behavior": compare("behavior", behavior_compute, [
            (path, 100000000, 103000000, "1GB") for path in target_paths
        ]),
        "passive_large_gap": compare("passive", passive_compute, [
            (path, theta, 100000000, 103000000, 0.0, "1GB") for path in target_paths
        ]),
        "joint_large_gap": compare("joint", joint_compute, [
            (symbol, [paths[symbol][args.month]], calendar_dates, {args.month}, 0.0, "1GB")
            for symbol in args.symbols
        ]),
    }
    status = "PASS" if all(row["status"] == "PASS" for row in evidence.values()) else "FAIL"
    payload = {
        "kind": "real_sample_parallel_determinism", "status": status,
        "created_at": datetime.now(timezone.utc).isoformat(), "month": args.month,
        "symbols": args.symbols, "implementations": {
            path: sha256(REPO_ROOT / path) for path in IMPLEMENTATIONS
        }, "evidence": evidence,
    }
    args.output.parent.mkdir(parents=True, exist_ok=False)
    args.output.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({"status": status, "output": str(args.output)}))
    return 0 if status == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
