#!/usr/bin/env python3
"""Certify Q001-Q008 evidence before factor or research computation."""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from audit_receipt import sha256
from registry import REPO_ROOT, load_factors, validate_registries


TARGETED_TESTS = (
    "tests/factors/experiment_batch_1/test_engine.py",
    "tests/factors/order_shape_mechanism/test_batch_a_engine.py",
    "tests/factors/order_shape_mechanism/test_batch_determinism.py",
    "tests/factors/order_shape_mechanism/test_m1_quote_engine.py",
    "tests/factors/stylized_fact_4_6/test_reproduce_d01_d03.py",
    "tests/factors/test_order_behavior_intraday_window.py",
    "tests/factors/test_passive_large_gap_intraday_window.py",
    "tests/factors/test_joint_large_gap_order_behavior_v4.py",
    "tests/pipelines/test_registry.py",
    "tests/pipelines/test_audit_receipt.py",
    "tests/audits/test_order_remainder_audit.py",
)


def implementation_hashes() -> dict[str, str]:
    paths = {path for factor in load_factors().values() for path in factor.get("implementation", [])}
    paths.update({
        "scripts/pipelines/factor_pipeline.py",
        "scripts/pipelines/experiment_pipeline.py",
        "scripts/pipelines/audit_receipt.py",
        "scripts/pipelines/complete_factor_run.py",
        "scripts/pipelines/preflight.py",
    })
    return {path: sha256(REPO_ROOT / path) for path in sorted(paths)}


def trace_counts(path: Path) -> dict[str, int]:
    counts = {"SH": 0, "SZ": 0}
    seen: set[tuple[str, str, str, str]] = set()
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            key = (row["symbol"], row["date"], row["side"], row["order_id"])
            if key not in seen:
                counts[row["symbol"][:2]] += 1
                seen.add(key)
    return counts


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--q003-summary", type=Path, required=True)
    parser.add_argument("--trace-sample", type=Path, required=True)
    parser.add_argument("--determinism-summary", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, action="append", required=True)
    parser.add_argument("--metadata", type=Path, action="append", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if len(args.manifest) != len(args.metadata):
        raise SystemExit("Each --manifest needs one same-position --metadata")
    if args.output.exists():
        raise SystemExit(f"Refusing to overwrite receipt: {args.output}")

    summary = json.loads(args.q003_summary.read_text(encoding="utf-8"))
    if summary.get("status") != "PASS":
        raise SystemExit("Q003 audit is not PASS")
    for implementation, expected_hash in summary.get("implementation_sha256", {}).items():
        implementation_path = REPO_ROOT / implementation
        if not implementation_path.exists() or sha256(implementation_path) != expected_hash:
            raise SystemExit(f"Q003 implementation changed after audit: {implementation}")
        if "source_link_status" in implementation_path.read_text(encoding="utf-8"):
            raise SystemExit(f"Post-processed source_link_status is forbidden in audited implementation: {implementation}")
    determinism = json.loads(args.determinism_summary.read_text(encoding="utf-8"))
    if determinism.get("status") != "PASS":
        raise SystemExit("Real-sample serial/parallel audit is not PASS")
    for implementation, expected_hash in determinism.get("implementations", {}).items():
        if sha256(REPO_ROOT / implementation) != expected_hash:
            raise SystemExit(f"Determinism implementation changed after audit: {implementation}")
    traces = trace_counts(args.trace_sample)
    if traces["SH"] < 20 or traces["SZ"] < 20:
        raise SystemExit(f"Need at least 20 traced orders per exchange: {traces}")
    registry_errors = validate_registries()
    if registry_errors:
        raise SystemExit("Registry validation failed:\n" + "\n".join(registry_errors))

    certified = []
    for manifest, metadata_path in zip(args.manifest, args.metadata):
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        if metadata.get("output_etf_symbols") != 0:
            raise SystemExit(f"Metadata does not certify ETF=0: {metadata_path}")
        certified.append({
            "path": str(manifest.resolve()), "sha256": sha256(manifest),
            "metadata": str(metadata_path.resolve()), "metadata_sha256": sha256(metadata_path),
            "months": metadata.get("months", []), "output_etf_symbols": 0,
            "universe_rule": metadata.get("universe_rule"),
        })

    command = [str(REPO_ROOT / "conda_lob/bin/python"), "-m", "pytest", "-q", "--import-mode=importlib", *TARGETED_TESTS]
    completed = subprocess.run(command, cwd=REPO_ROOT, text=True, capture_output=True)
    if completed.returncode:
        print(completed.stdout)
        print(completed.stderr)
        raise SystemExit("Targeted preflight tests failed")
    receipt = {
        "kind": "lob_preflight_receipt", "schema_version": 1, "status": "PASS",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "passed_quality_gates": [f"Q{i:03d}" for i in range(1, 9)],
        "q003_summary": str(args.q003_summary.resolve()), "q003_summary_sha256": sha256(args.q003_summary),
        "trace_sample": str(args.trace_sample.resolve()), "trace_sample_sha256": sha256(args.trace_sample),
        "determinism_summary": str(args.determinism_summary.resolve()),
        "determinism_summary_sha256": sha256(args.determinism_summary),
        "trace_order_counts": traces, "certified_manifests": certified,
        "implementation_sha256": implementation_hashes(),
        "test_command": command, "test_stdout": completed.stdout.strip(),
        "controls": {
            "Q004": "No forward-linked FULL/PARTIAL fields are used by audited event-state implementations.",
            "Q006": "Targeted suite includes process-vs-serial byte equality.",
            "Q007": "Run manifests record immutable input and receipt fingerprints and refuse overwrite.",
            "Q008": "Registry dependency and implementation-path validation passed.",
        },
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(receipt, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(args.output.relative_to(REPO_ROOT))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
