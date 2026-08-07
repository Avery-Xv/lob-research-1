from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest


PIPELINE_DIR = Path(__file__).resolve().parents[2] / "scripts" / "pipelines"
sys.path.insert(0, str(PIPELINE_DIR))

from audit_receipt import INFRASTRUCTURE, sha256, validate_receipt  # noqa: E402


def test_receipt_rejects_uncertified_manifest_and_stale_implementation(tmp_path: Path) -> None:
    manifest = tmp_path / "manifest.txt"
    receipt_path = tmp_path / "receipt.json"
    manifest.write_text("input\n")
    # validate_receipt resolves implementations below REPO_ROOT, so use a real repo file.
    repo_implementation = "scripts/pipelines/registry.py"
    repo_path = Path(__file__).resolve().parents[2] / repo_implementation
    receipt = {
        "kind": "lob_preflight_receipt", "status": "PASS",
        "passed_quality_gates": ["Q001", "Q003"],
        "certified_manifests": [{"sha256": sha256(manifest)}],
        "implementation_sha256": {
            repo_implementation: sha256(repo_path),
            **{path: sha256(Path(__file__).resolve().parents[2] / path) for path in INFRASTRUCTURE},
        },
    }
    receipt_path.write_text(json.dumps(receipt))
    validate_receipt(receipt_path, ["Q003"], [repo_implementation], manifest)
    manifest.write_text("changed\n")
    with pytest.raises(ValueError, match="not certified"):
        validate_receipt(receipt_path, ["Q003"], [repo_implementation], manifest)


def test_receipt_rejects_missing_gate(tmp_path: Path) -> None:
    manifest = tmp_path / "manifest.txt"
    receipt_path = tmp_path / "receipt.json"
    manifest.write_text("input\n")
    receipt_path.write_text(json.dumps({
        "kind": "lob_preflight_receipt", "status": "PASS",
        "passed_quality_gates": ["Q001"],
        "certified_manifests": [{"sha256": sha256(manifest)}],
        "implementation_sha256": {},
    }))
    with pytest.raises(ValueError, match="Q003"):
        validate_receipt(receipt_path, ["Q003"], [], manifest)
