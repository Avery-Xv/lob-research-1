#!/usr/bin/env python3
"""Inspect factor lineage and create immutable factor-run manifests."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from registry import REPO_ROOT, load_factors, print_table, validate_registries


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description=__doc__)
    commands = root.add_subparsers(dest="command", required=True)
    commands.add_parser("status", help="List registered factors")
    show = commands.add_parser("show", help="Show one factor as JSON")
    show.add_argument("factor_id")
    plan = commands.add_parser("plan", help="Create a versioned run manifest; does not run computation")
    plan.add_argument("factor_id")
    plan.add_argument("--run-id", required=True)
    plan.add_argument("--months", nargs="+", required=True)
    plan.add_argument("--exchange", choices=("SH", "SZ", "ALL"), required=True)
    plan.add_argument("--window", default="1000_1030")
    plan.add_argument("--manifest", required=True, help="Point-in-time A-share input manifest")
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
        "input_manifest": str(Path(args.manifest).expanduser().resolve()),
        "universe_rule": "point-in-time A-share stocks; ETFs excluded before factor calculation",
        "prebook_impact": factor["affected_by_prebook_fix"],
        "implementation": factor["implementation"],
        "status": "planned",
        "notes": args.notes,
    }
    run_dir.mkdir(parents=True, exist_ok=False)
    run_manifest.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(run_manifest.relative_to(REPO_ROOT))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
