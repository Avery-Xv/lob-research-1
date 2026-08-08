#!/usr/bin/env python3
"""Certify byte-identical serial/process output for non-parent order state."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
IMPLEMENTATIONS = (
    "scripts/factors/order_shape_mechanism/non_parent_order_state_engine.py",
    "scripts/factors/order_shape_mechanism/compute_non_parent_order_state_v4.py",
)
OUTPUT_FILES = ("signals.csv", "quality.csv", "done.json")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def batch_evidence(root: Path) -> tuple[dict[str, dict[str, str]], list[str], int]:
    evidence: dict[str, dict[str, str]] = {}
    symbols: list[str] = []
    signal_rows = 0
    for batch in sorted(root.glob("batch_*")):
        hashes = {}
        for filename in OUTPUT_FILES:
            path = batch / filename
            if not path.is_file():
                raise ValueError(f"missing output: {path}")
            hashes[filename] = sha256(path)
        done = json.loads((batch / "done.json").read_text(encoding="utf-8"))
        symbols.extend(str(symbol) for symbol in done["symbols"])
        with (batch / "signals.csv").open(newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
        if any(int(row["signal_time"]) != 1030 for row in rows):
            raise ValueError(f"non-10:30 signal in {batch}")
        signal_rows += len(rows)
        evidence[batch.name] = hashes
    if not evidence:
        raise ValueError(f"no completed batches: {root}")
    return evidence, symbols, signal_rows


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--serial-dir", type=Path, required=True)
    parser.add_argument("--parallel-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise SystemExit(f"Refusing to overwrite: {args.output}")

    serial, serial_symbols, serial_rows = batch_evidence(args.serial_dir)
    parallel, parallel_symbols, parallel_rows = batch_evidence(args.parallel_dir)
    symbols = sorted(serial_symbols)
    checks = {
        "byte_identical": serial == parallel,
        "same_symbols": sorted(parallel_symbols) == symbols,
        "both_exchanges": any(s.startswith("SH") for s in symbols)
        and any(s.startswith("SZ") for s in symbols),
        "signal_rows_equal": serial_rows == parallel_rows,
    }
    status = "PASS" if all(checks.values()) else "FAIL"
    payload = {
        "kind": "non_parent_order_state_real_sample_determinism",
        "schema_version": 1,
        "status": status,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "symbols": symbols,
        "signal_time": 1030,
        "checks": checks,
        "implementations": {
            path: sha256(REPO_ROOT / path) for path in IMPLEMENTATIONS
        },
        "serial": {
            "root": str(args.serial_dir.resolve()),
            "signal_rows": serial_rows,
            "hashes": serial,
        },
        "parallel": {
            "root": str(args.parallel_dir.resolve()),
            "signal_rows": parallel_rows,
            "hashes": parallel,
        },
    }
    args.output.parent.mkdir(parents=True, exist_ok=False)
    args.output.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps({"status": status, "output": str(args.output)}))
    return 0 if status == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
