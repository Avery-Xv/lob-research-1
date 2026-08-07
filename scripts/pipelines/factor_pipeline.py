#!/usr/bin/env python3
"""Inspect factor lineage and create immutable factor-run manifests."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from audit_receipt import sha256, validate_receipt
from registry import REPO_ROOT, load_factors, print_table, validate_registries


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description=__doc__)
    commands = root.add_subparsers(dest="command", required=True)
    commands.add_parser("status")
    show = commands.add_parser("show")
    show.add_argument("factor_id")
    plan = commands.add_parser("plan", help="Create a run manifest; does not submit computation")
    plan.add_argument("factor_id")
    plan.add_argument("--run-id", required=True)
    plan.add_argument("--months", nargs="+", required=True)
    plan.add_argument("--exchange", choices=("SH", "SZ", "ALL"), required=True)
    plan.add_argument("--window", default="1000_1030")
    plan.add_argument("--manifest", required=True)
    plan.add_argument("--audit-receipt", required=True)
    plan.add_argument("--purpose", choices=("production", "definition_audit"), default="production")
    plan.add_argument("--notes", default="")
    return root


def main() -> int:
    args = parser().parse_args()
    errors = validate_registries()
    if errors:
        raise SystemExit("Registry validation failed:\n" + "\n".join(errors))
    factors = load_factors()
    if args.command == "status":
        print_table(list(factors.values()), "definition_version")
        return 0
    if args.factor_id not in factors:
        raise SystemExit(f"Unknown factor: {args.factor_id}")
    factor = factors[args.factor_id]
    if args.command == "show":
        print(json.dumps(factor, ensure_ascii=False, indent=2))
        return 0
    if not factor["implementation"]:
        raise SystemExit(f"Factor {args.factor_id} has no implementation and cannot be submitted")
    production_statuses = {"ready", "recompute_required", "partial", "baseline"}
    if args.purpose == "production" and factor["status"] not in production_statuses:
        raise SystemExit(f"Factor {args.factor_id} status={factor['status']} is not production-submittable; use --purpose definition_audit only for a controlled definition audit")
    input_manifest = Path(args.manifest).expanduser().resolve()
    receipt_path = Path(args.audit_receipt).expanduser().resolve()
    try:
        receipt = validate_receipt(receipt_path, factor["required_quality_gates"], factor["implementation"], input_manifest)
    except ValueError as error:
        raise SystemExit(str(error)) from error
    certified = next(row for row in receipt["certified_manifests"] if row["sha256"] == sha256(input_manifest))
    invalid_months = sorted(set(args.months) - set(certified.get("months", [])))
    if invalid_months:
        raise SystemExit("Months absent from certified manifest: " + ", ".join(invalid_months))
    run_dir = REPO_ROOT / "runs" / "factors" / args.factor_id / args.run_id
    run_manifest = run_dir / "manifest.json"
    if run_manifest.exists():
        raise SystemExit(f"Refusing to overwrite existing run: {run_manifest}")
    payload = {
        "kind": "factor_run",
        "factor_id": args.factor_id,
        "definition_version": factor["definition_version"],
        "created_at": datetime.now(timezone.utc).isoformat(),
        "months": args.months,
        "exchange": args.exchange,
        "signal_window": args.window,
        "input_manifest": str(input_manifest),
        "input_manifest_sha256": sha256(input_manifest),
        "audit_receipt": str(receipt_path),
        "audit_receipt_sha256": sha256(receipt_path),
        "data_dependencies": factor["data_dependencies"],
        "required_quality_gates": factor["required_quality_gates"],
        "implementation": factor["implementation"],
        "purpose": args.purpose,
        "status": "ready_to_submit" if args.purpose == "production" else "ready_to_submit_audit",
        "notes": args.notes,
    }
    run_dir.mkdir(parents=True, exist_ok=False)
    run_manifest.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(run_manifest.relative_to(REPO_ROOT))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
