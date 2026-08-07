#!/usr/bin/env python3
"""Load and validate factor/experiment registries without third-party packages."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
FACTOR_REGISTRY = REPO_ROOT / "research" / "factors.json"
EXPERIMENT_REGISTRY = REPO_ROOT / "research" / "experiments.json"


def _load(path: Path, collection: str) -> dict[str, dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = payload.get(collection)
    if not isinstance(rows, list):
        raise ValueError(f"{path}: {collection} must be a list")
    indexed: dict[str, dict[str, Any]] = {}
    for row in rows:
        item_id = row.get("id")
        if not isinstance(item_id, str) or not item_id:
            raise ValueError(f"{path}: every item needs a non-empty id")
        if item_id in indexed:
            raise ValueError(f"{path}: duplicate id {item_id}")
        indexed[item_id] = row
    return indexed


def load_factors() -> dict[str, dict[str, Any]]:
    return _load(FACTOR_REGISTRY, "factors")


def load_experiments() -> dict[str, dict[str, Any]]:
    return _load(EXPERIMENT_REGISTRY, "experiments")


def validate_registries() -> list[str]:
    factors = load_factors()
    experiments = load_experiments()
    errors: list[str] = []
    factor_required = {"id", "name", "status", "definition_version", "theory_sources", "implementation", "affected_by_prebook_fix", "next_action"}
    experiment_required = {"id", "name", "status", "spec_version", "theory_sources", "factor_dependencies", "experiment_dependencies", "result_root", "next_action"}
    for item_id, row in factors.items():
        missing = sorted(factor_required - row.keys())
        if missing:
            errors.append(f"factor {item_id}: missing {', '.join(missing)}")
        for source in row.get("implementation", []):
            if not (REPO_ROOT / source).exists():
                errors.append(f"factor {item_id}: implementation does not exist: {source}")
    for item_id, row in experiments.items():
        missing = sorted(experiment_required - row.keys())
        if missing:
            errors.append(f"experiment {item_id}: missing {', '.join(missing)}")
        for dependency in row.get("factor_dependencies", []):
            if dependency not in factors:
                errors.append(f"experiment {item_id}: unknown factor dependency {dependency}")
        for dependency in row.get("experiment_dependencies", []):
            if dependency not in experiments:
                errors.append(f"experiment {item_id}: unknown experiment dependency {dependency}")
    return errors


def print_table(rows: list[dict[str, Any]], version_key: str) -> None:
    headers = ("ID", "STATUS", "VERSION", "NEXT ACTION")
    widths = [len(value) for value in headers]
    values: list[tuple[str, str, str, str]] = []
    for row in rows:
        line = (row["id"], row["status"], row[version_key], row["next_action"])
        values.append(line)
        widths = [max(old, len(str(value))) for old, value in zip(widths, line)]
    print("  ".join(value.ljust(width) for value, width in zip(headers, widths)))
    print("  ".join("-" * width for width in widths))
    for line in values:
        print("  ".join(str(value).ljust(width) for value, width in zip(line, widths)))
