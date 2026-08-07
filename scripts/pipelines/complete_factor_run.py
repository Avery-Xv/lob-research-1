#!/usr/bin/env python3
"""Seal completed factor outputs so research runs cannot consume planned jobs."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

from audit_receipt import sha256


def hash_output(path: Path) -> str:
    if path.is_file():
        return sha256(path)
    digest = hashlib.sha256()
    for child in sorted(item for item in path.rglob("*") if item.is_file()):
        digest.update(str(child.relative_to(path)).encode())
        digest.update(bytes.fromhex(sha256(child)))
    return digest.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--factor-run", type=Path, required=True)
    parser.add_argument("--output", type=Path, action="append", required=True)
    args = parser.parse_args()
    manifest = json.loads(args.factor_run.read_text(encoding="utf-8"))
    if manifest.get("kind") != "factor_run" or manifest.get("status") != "ready_to_submit":
        raise SystemExit("Factor run is not a ready_to_submit manifest")
    if sha256(Path(manifest["input_manifest"])) != manifest["input_manifest_sha256"]:
        raise SystemExit("Factor input manifest changed after planning")
    if sha256(Path(manifest["audit_receipt"])) != manifest["audit_receipt_sha256"]:
        raise SystemExit("Audit receipt changed after planning")
    outputs = []
    for path in args.output:
        resolved = path.resolve()
        if not resolved.exists():
            raise SystemExit(f"Missing factor output: {resolved}")
        outputs.append({"path": str(resolved), "sha256": hash_output(resolved)})
    completion_path = args.factor_run.parent / "completion.json"
    if completion_path.exists():
        raise SystemExit(f"Refusing to overwrite completion: {completion_path}")
    completion = {
        "kind": "factor_run_completion", "status": "completed_audited",
        "factor_id": manifest["factor_id"], "definition_version": manifest["definition_version"],
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "factor_run_manifest": str(args.factor_run.resolve()),
        "factor_run_manifest_sha256": sha256(args.factor_run), "outputs": outputs,
    }
    completion_path.write_text(json.dumps(completion, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(completion_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
