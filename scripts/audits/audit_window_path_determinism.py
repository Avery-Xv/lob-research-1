#!/usr/bin/env python3
"""Certify byte-identical serial/process output for the R016/R017 window cache."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
IMPLEMENTATIONS = (
    "scripts/factors/order_shape_non_parent/window_path_engine.py",
    "scripts/factors/order_shape_non_parent/compute_window_path_v4.py",
)
OUTPUT_FILES = ("window_paths.csv", "quality.csv", "done.json")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def evidence(root: Path) -> tuple[dict[str, dict[str, str]], list[str]]:
    hashes: dict[str, dict[str, str]] = {}
    symbols: list[str] = []
    for batch in sorted(root.glob("batch_*")):
        hashes[batch.name] = {name: sha256(batch / name) for name in OUTPUT_FILES}
        symbols.extend(json.loads((batch / "done.json").read_text())["symbols"])
    if not hashes:
        raise ValueError(f"no batches under {root}")
    return hashes, symbols


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--serial-dir", type=Path, required=True)
    parser.add_argument("--parallel-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise SystemExit(f"Refusing to overwrite: {args.output}")
    serial, serial_symbols = evidence(args.serial_dir)
    parallel, parallel_symbols = evidence(args.parallel_dir)
    checks = {
        "byte_identical": serial == parallel,
        "same_symbols": sorted(serial_symbols) == sorted(parallel_symbols),
        "both_exchanges": any(value.startswith("SH") for value in serial_symbols)
        and any(value.startswith("SZ") for value in serial_symbols),
    }
    status = "PASS" if all(checks.values()) else "FAIL"
    payload = {
        "kind": "window_path_real_sample_determinism", "schema_version": 1,
        "status": status, "created_at": datetime.now(timezone.utc).isoformat(),
        "symbols": sorted(serial_symbols), "checks": checks,
        "implementations": {path: sha256(REPO_ROOT / path) for path in IMPLEMENTATIONS},
        "serial": {"root": str(args.serial_dir.resolve()), "hashes": serial},
        "parallel": {"root": str(args.parallel_dir.resolve()), "hashes": parallel},
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({"status": status, "output": str(args.output)}))
    return 0 if status == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
