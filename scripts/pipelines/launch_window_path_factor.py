#!/usr/bin/env python3
"""Validate immutable F014 lineage, then exec the resumable window-path runner."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
PYTHON = REPO_ROOT / "conda_lob/bin/python"
RUNNER = REPO_ROOT / "scripts/factors/order_shape_non_parent/compute_window_path_v4.py"
ENGINE = REPO_ROOT / "scripts/factors/order_shape_non_parent/window_path_engine.py"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_json(path: Path, value: dict[str, object]) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")
    os.replace(temporary, path)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--factor-run-manifest", type=Path, required=True)
    parser.add_argument("--shard-dir", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--batch-size", type=int, default=4)
    parser.add_argument("--fetch-rows", type=int, default=100_000)
    parser.add_argument("--memory-limit", default="2GB")
    parser.add_argument("--validate-only", action="store_true")
    args = parser.parse_args()
    manifest_path = args.factor_run_manifest.resolve()
    manifest = json.loads(manifest_path.read_text())
    if manifest.get("kind") != "factor_run" or manifest.get("factor_id") != "F014":
        raise SystemExit("expected an F014 factor_run manifest")
    if manifest.get("status") != "ready_to_submit" or manifest.get("purpose") != "production":
        raise SystemExit("factor run is not ready_to_submit production")
    if manifest.get("months") != ["202601"] or manifest.get("exchange") != "ALL":
        raise SystemExit("unexpected month or exchange scope")
    input_manifest = Path(str(manifest["input_manifest"]))
    receipt_path = Path(str(manifest["audit_receipt"]))
    if sha256(input_manifest) != manifest.get("input_manifest_sha256"):
        raise SystemExit("input manifest changed after planning")
    if sha256(receipt_path) != manifest.get("audit_receipt_sha256"):
        raise SystemExit("preflight receipt changed after planning")
    receipt = json.loads(receipt_path.read_text())
    if receipt.get("status") != "PASS":
        raise SystemExit("preflight receipt is not PASS")
    expected_hashes = receipt.get("implementation_sha256", {})
    for path in (RUNNER, ENGINE):
        relative = str(path.relative_to(REPO_ROOT))
        if expected_hashes.get(relative) != sha256(path):
            raise SystemExit(f"implementation changed after preflight: {relative}")
    metadata = Path(str(receipt["certified_manifests"][0]["metadata"]))
    certified = next(
        row for row in receipt["certified_manifests"]
        if Path(str(row["path"])).resolve() == input_manifest.resolve()
    )
    if certified.get("output_etf_symbols") != 0:
        raise SystemExit("certified universe is not ETF-free")
    shard_dir = args.shard_dir.resolve()
    shard_dir.mkdir(parents=True, exist_ok=True)
    submission = {
        "kind": "window_path_factor_submission", "schema_version": 1,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "factor_run_manifest": str(manifest_path),
        "factor_run_manifest_sha256": sha256(manifest_path),
        "preflight_receipt": str(receipt_path), "preflight_receipt_sha256": sha256(receipt_path),
        "runner": str(RUNNER), "runner_sha256": sha256(RUNNER),
        "engine": str(ENGINE), "engine_sha256": sha256(ENGINE),
        "workers": args.workers, "batch_size": args.batch_size,
        "fetch_rows": args.fetch_rows, "memory_limit": args.memory_limit,
        "status": "submitted",
    }
    submission_path = shard_dir / "submission.json"
    if submission_path.exists():
        existing = json.loads(submission_path.read_text())
        stable = {key: value for key, value in submission.items() if key != "created_at"}
        old_stable = {key: value for key, value in existing.items() if key != "created_at"}
        if stable != old_stable:
            raise SystemExit("submission lineage changed; use a new shard directory")
    else:
        write_json(submission_path, submission)
    command = [
        str(PYTHON), str(RUNNER),
        "--file-list", str(input_manifest), "--universe-metadata", str(metadata),
        "--target-month", "202601", "--workers", str(args.workers),
        "--batch-size", str(args.batch_size), "--fetch-rows", str(args.fetch_rows),
        "--memory-limit", args.memory_limit, "--shard-dir", str(shard_dir),
    ]
    if args.validate_only:
        print(json.dumps({"status": "PASS", "submission": str(submission_path)}))
        return 0
    os.execv(str(PYTHON), command)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
