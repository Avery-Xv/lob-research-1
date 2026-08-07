#!/usr/bin/env python3
"""Load and validate factor, research, data-product, and quality registries."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
FACTOR_REGISTRY = REPO_ROOT / "research" / "factors.json"
EXPERIMENT_REGISTRY = REPO_ROOT / "research" / "experiments.json"
DATA_PRODUCT_REGISTRY = REPO_ROOT / "research" / "data_products.json"
QUALITY_GATE_REGISTRY = REPO_ROOT / "research" / "quality_gates.json"
IDEA_LINEAGE_REGISTRY = REPO_ROOT / "research" / "idea_lineage.json"
LEGACY_COMPUTE_INVENTORY = REPO_ROOT / "research" / "legacy_compute_inventory.json"


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


def load_data_products() -> dict[str, dict[str, Any]]:
    return _load(DATA_PRODUCT_REGISTRY, "products")


def load_quality_gates() -> dict[str, dict[str, Any]]:
    return _load(QUALITY_GATE_REGISTRY, "quality_gates")


def load_idea_lineage() -> dict[str, Any]:
    return json.loads(IDEA_LINEAGE_REGISTRY.read_text(encoding="utf-8"))


def load_legacy_compute_inventory() -> dict[str, Any]:
    return json.loads(LEGACY_COMPUTE_INVENTORY.read_text(encoding="utf-8"))


def _inventory_ids(groups: dict[str, list[dict[str, Any]]]) -> list[str]:
    ids: list[str] = []
    for rows in groups.values():
        for row in rows:
            ids.extend(row.get("ids", [row["id"]] if "id" in row else []))
    return ids


def validate_registries() -> list[str]:
    factors = load_factors()
    experiments = load_experiments()
    products = load_data_products()
    gates = load_quality_gates()
    lineage = load_idea_lineage()
    inventory = load_legacy_compute_inventory()
    errors: list[str] = []
    lineage_factor_ids = {row["id"] for row in lineage.get("factor_lineage", [])}
    lineage_research_ids = {item_id for row in lineage.get("research_lineage", []) for item_id in row.get("ids", [])}
    if lineage_factor_ids != set(factors):
        errors.append(f"idea lineage factor coverage mismatch: {sorted(set(factors) ^ lineage_factor_ids)}")
    if lineage_research_ids != set(experiments):
        errors.append(f"idea lineage research coverage mismatch: {sorted(set(experiments) ^ lineage_research_ids)}")
    for group in (lineage.get("factor_lineage", []), lineage.get("research_lineage", [])):
        for row in group:
            for source in row.get("source_sections", []):
                source_path = source.split("#", 1)[0]
                if not (REPO_ROOT / source_path).exists():
                    errors.append(f"idea lineage source does not exist: {source_path}")
    if lineage.get("unmapped_legacy_ideas"):
        errors.append("idea lineage contains unmapped legacy ideas")
    inventory_factor_ids = _inventory_ids(inventory.get("factors", {}))
    inventory_research_ids = _inventory_ids(inventory.get("research", {}))
    if len(inventory_factor_ids) != len(set(inventory_factor_ids)) or set(inventory_factor_ids) != set(factors):
        errors.append("legacy compute inventory must classify each factor exactly once")
    if len(inventory_research_ids) != len(set(inventory_research_ids)) or set(inventory_research_ids) != set(experiments):
        errors.append("legacy compute inventory must classify each research experiment exactly once")
    factor_required = {"id", "name", "status", "definition_version", "theory_sources", "implementation", "data_dependencies", "required_quality_gates", "next_action"}
    research_required = {"id", "name", "status", "spec_version", "research_question", "factor_dependencies", "data_dependencies", "research_outputs", "decision_rule", "theory_sources", "result_root"}
    for item_id, row in factors.items():
        missing = sorted(factor_required - row.keys())
        if missing:
            errors.append(f"factor {item_id}: missing {', '.join(missing)}")
        for source in row.get("implementation", []):
            if not (REPO_ROOT / source).exists():
                errors.append(f"factor {item_id}: implementation does not exist: {source}")
        for dependency in row.get("data_dependencies", []):
            if dependency not in products:
                errors.append(f"factor {item_id}: unknown data product {dependency}")
        for gate in row.get("required_quality_gates", []):
            if gate not in gates:
                errors.append(f"factor {item_id}: unknown quality gate {gate}")
    for item_id, row in experiments.items():
        missing = sorted(research_required - row.keys())
        if missing:
            errors.append(f"research experiment {item_id}: missing {', '.join(missing)}")
        for dependency in row.get("factor_dependencies", []):
            if dependency not in factors:
                errors.append(f"research experiment {item_id}: unknown factor {dependency}")
        for dependency in row.get("data_dependencies", []):
            if dependency not in products:
                errors.append(f"research experiment {item_id}: unknown data product {dependency}")
    for item_id, row in products.items():
        for source in row.get("implementation", []):
            if not (REPO_ROOT / source).exists():
                errors.append(f"data product {item_id}: implementation does not exist: {source}")
        for gate in row.get("required_quality_gates", []):
            if gate not in gates:
                errors.append(f"data product {item_id}: unknown quality gate {gate}")
    return errors


def required_gates(factor_ids: list[str], product_ids: list[str]) -> list[str]:
    factors = load_factors()
    products = load_data_products()
    gate_ids: set[str] = set()
    for factor_id in factor_ids:
        gate_ids.update(factors[factor_id]["required_quality_gates"])
    for product_id in product_ids:
        gate_ids.update(products[product_id]["required_quality_gates"])
    return sorted(gate_ids)


def print_table(rows: list[dict[str, Any]], version_key: str) -> None:
    headers = ("ID", "STATUS", "VERSION", "NAME")
    widths = [len(value) for value in headers]
    values = []
    for row in rows:
        line = (row["id"], row["status"], row[version_key], row["name"])
        values.append(line)
        widths = [max(old, len(str(value))) for old, value in zip(widths, line)]
    print("  ".join(value.ljust(width) for value, width in zip(headers, widths)))
    print("  ".join("-" * width for width in widths))
    for line in values:
        print("  ".join(str(value).ljust(width) for value, width in zip(line, widths)))
