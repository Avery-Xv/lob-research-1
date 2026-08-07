#!/usr/bin/env python3
"""Inspect experiment lineage and create manifests tied to factor runs."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from registry import REPO_ROOT, load_experiments, load_factors, print_table, validate_registries


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description=__doc__)
    commands = root.add_subparsers(dest="command", required=True)
    commands.add_parser("status", help="List registered experiments")
    show = commands.add_parser("show", help="Show one experiment as JSON")
    show.add_argument("experiment_id")
    plan = commands.add_parser("plan", help="Create a versioned experiment manifest")
    plan.add_argument("experiment_id")
    plan.add_argument("--run-id", required=True)
    plan.add_argument("--factor-run", action="append", default=[], metavar="FACTOR_ID=MANIFEST")
    plan.add_argument("--notes", default="")
    return root


def parse_factor_runs(values: list[str]) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for value in values:
        if "=" not in value:
            raise SystemExit(f"Invalid --factor-run {value!r}; expected FACTOR_ID=MANIFEST")
        factor_id, manifest = value.split("=", 1)
        parsed[factor_id] = str(Path(manifest).expanduser().resolve())
    return parsed


def main() -> int:
    args = parser().parse_args()
    errors = validate_registries()
    if errors:
        raise SystemExit("Registry validation failed:\n" + "\n".join(errors))
    experiments = load_experiments()
    factors = load_factors()
    if args.command == "status":
        print_table(list(experiments.values()), "spec_version")
        return 0
    if args.experiment_id not in experiments:
        raise SystemExit(f"Unknown experiment: {args.experiment_id}")
    experiment = experiments[args.experiment_id]
    if args.command == "show":
        print(json.dumps(experiment, ensure_ascii=False, indent=2))
        return 0
    factor_runs = parse_factor_runs(args.factor_run)
    missing = sorted(set(experiment["factor_dependencies"]) - factor_runs.keys())
    unknown = sorted(set(factor_runs) - factors.keys())
    if missing or unknown:
        messages = []
        if missing:
            messages.append("missing factor runs: " + ", ".join(missing))
        if unknown:
            messages.append("unknown factors: " + ", ".join(unknown))
        raise SystemExit("; ".join(messages))
    for factor_id, manifest_path in factor_runs.items():
        payload = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
        if payload.get("factor_id") != factor_id:
            raise SystemExit(f"Factor manifest mismatch: expected {factor_id}, got {payload.get('factor_id')}")
    run_dir = REPO_ROOT / "runs" / "experiments" / args.experiment_id / args.run_id
    run_manifest = run_dir / "manifest.json"
    if run_manifest.exists():
        raise SystemExit(f"Refusing to overwrite existing run: {run_manifest}")
    payload = {
        "kind": "experiment_run",
        "experiment_id": args.experiment_id,
        "spec_version": experiment["spec_version"],
        "created_at": datetime.now(timezone.utc).isoformat(),
        "factor_runs": factor_runs,
        "experiment_dependencies": experiment["experiment_dependencies"],
        "primary_evaluation": "raw_non_neutralized",
        "result_root": experiment["result_root"],
        "status": "planned",
        "notes": args.notes,
    }
    run_dir.mkdir(parents=True, exist_ok=False)
    run_manifest.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(run_manifest.relative_to(REPO_ROOT))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
