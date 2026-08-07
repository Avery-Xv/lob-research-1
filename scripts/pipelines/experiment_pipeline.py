#!/usr/bin/env python3
"""Inspect research experiments and bind them to concrete factor runs."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from audit_receipt import sha256
from complete_factor_run import hash_output
from registry import REPO_ROOT, load_experiments, load_factors, print_table, required_gates, validate_registries


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description=__doc__)
    commands = root.add_subparsers(dest="command", required=True)
    commands.add_parser("status")
    show = commands.add_parser("show")
    show.add_argument("research_id")
    plan = commands.add_parser("plan", help="Create a research manifest; does not submit computation")
    plan.add_argument("research_id")
    plan.add_argument("--run-id", required=True)
    plan.add_argument("--factor-run", action="append", default=[], metavar="FACTOR_ID=MANIFEST")
    plan.add_argument("--notes", default="")
    return root


def parse_factor_runs(values: list[str]) -> dict[str, str]:
    parsed = {}
    for value in values:
        if "=" not in value:
            raise SystemExit(f"Invalid --factor-run {value!r}; expected FACTOR_ID=COMPLETION")
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
    if args.research_id not in experiments:
        raise SystemExit(f"Unknown research experiment: {args.research_id}")
    experiment = experiments[args.research_id]
    if args.command == "show":
        print(json.dumps(experiment, ensure_ascii=False, indent=2))
        return 0
    factor_runs = parse_factor_runs(args.factor_run)
    missing = sorted(set(experiment["factor_dependencies"]) - factor_runs.keys())
    unknown = sorted(set(factor_runs) - factors.keys())
    if missing or unknown:
        raise SystemExit("; ".join(filter(None, ["missing factor runs: " + ", ".join(missing) if missing else "", "unknown factors: " + ", ".join(unknown) if unknown else ""])))
    for factor_id, completion_path in factor_runs.items():
        payload = json.loads(Path(completion_path).read_text(encoding="utf-8"))
        if payload.get("factor_id") != factor_id or payload.get("status") != "completed_audited":
            raise SystemExit(f"Factor completion mismatch or not audited: expected {factor_id}")
        run_manifest = Path(payload["factor_run_manifest"])
        if sha256(run_manifest) != payload["factor_run_manifest_sha256"]:
            raise SystemExit(f"Factor run manifest changed after completion: {factor_id}")
        for output in payload.get("outputs", []):
            output_path = Path(output["path"])
            if not output_path.exists() or hash_output(output_path) != output["sha256"]:
                raise SystemExit(f"Factor output missing or changed: {factor_id}: {output_path}")
    run_dir = REPO_ROOT / "runs" / "research" / args.research_id / args.run_id
    run_manifest = run_dir / "manifest.json"
    if run_manifest.exists():
        raise SystemExit(f"Refusing to overwrite existing run: {run_manifest}")
    payload = {
        "kind": "research_run",
        "research_id": args.research_id,
        "spec_version": experiment["spec_version"],
        "created_at": datetime.now(timezone.utc).isoformat(),
        "research_question": experiment["research_question"],
        "decision_rule": experiment["decision_rule"],
        "factor_runs": factor_runs,
        "data_dependencies": experiment["data_dependencies"],
        "required_quality_gates": required_gates(experiment["factor_dependencies"], experiment["data_dependencies"]),
        "research_outputs": experiment["research_outputs"],
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
