#!/usr/bin/env python3
"""Create and validate immutable preflight receipts for LOB compute runs."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from registry import REPO_ROOT


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_receipt(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("kind") != "lob_preflight_receipt" or payload.get("status") != "PASS":
        raise ValueError(f"Preflight receipt is not PASS: {path}")
    return payload


def validate_receipt(
    receipt_path: Path,
    required_gates: list[str],
    implementations: list[str],
    input_manifest: Path,
) -> dict[str, Any]:
    receipt = load_receipt(receipt_path)
    missing_gates = sorted(set(required_gates) - set(receipt.get("passed_quality_gates", [])))
    if missing_gates:
        raise ValueError("Preflight receipt misses gates: " + ", ".join(missing_gates))
    certified = {row["sha256"] for row in receipt.get("certified_manifests", [])}
    manifest_hash = sha256(input_manifest)
    if manifest_hash not in certified:
        raise ValueError(f"Input manifest is not certified by receipt: {input_manifest}")
    recorded = receipt.get("implementation_sha256", {})
    stale = [
        path for path in implementations
        if not (REPO_ROOT / path).exists() or recorded.get(path) != sha256(REPO_ROOT / path)
    ]
    if stale:
        raise ValueError("Implementation changed after audit: " + ", ".join(stale))
    return receipt
