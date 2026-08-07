#!/usr/bin/env python3
"""Copy only audited legacy LOB assets into current F/R/P keyed locations."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

import duckdb


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_LEGACY_ROOT = Path("/home/avery/lob_test")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def tree_hash(path: Path) -> str:
    if path.is_file():
        return sha256(path)
    digest = hashlib.sha256()
    for child in sorted(item for item in path.rglob("*") if item.is_file()):
        digest.update(str(child.relative_to(path)).encode())
        digest.update(bytes.fromhex(sha256(child)))
    return digest.hexdigest()


def copy_new(source: Path, destination: Path) -> None:
    if destination.exists():
        raise FileExistsError(f"Refusing to overwrite migrated asset: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    if source.is_dir():
        shutil.copytree(source, destination, copy_function=shutil.copy2)
    else:
        shutil.copy2(source, destination)


def write_manifest(path: Path, payload: dict) -> None:
    if path.exists():
        raise FileExistsError(f"Refusing to overwrite import manifest: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def base_manifest(import_id: str, kind: str, item_id: str, source: Path, destination: Path) -> dict:
    return {
        "kind": kind,
        "import_id": import_id,
        "item_id": item_id,
        "status": "completed_audited_legacy_import",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "source": str(source.resolve()),
        "source_sha256": tree_hash(source),
        "destination": str(destination.resolve()),
        "destination_sha256": tree_hash(destination),
        "copy_semantics": "copied; legacy source preserved",
    }


def migrate_f001(legacy: Path) -> dict:
    source_dir = legacy / "data/processed/prebook_rerun"
    source_csv = source_dir / "pb01_a_active_take_midprice_sh_1000_1030_202601_202602.csv"
    source_meta = source_dir / "pb01_a_active_take_midprice_sh_1000_1030_202601_202602.metadata.json"
    metadata = json.loads(source_meta.read_text(encoding="utf-8"))
    if metadata["output_etf_symbols"] != 0 or metadata["input_files"] != 4607:
        raise ValueError("F001 legacy SH universe/inventory audit failed")
    if sha256(source_csv) != metadata["output_sha256"]:
        raise ValueError("F001 legacy output hash mismatch")
    with source_csv.open(encoding="utf-8") as handle:
        rows = sum(1 for _ in handle) - 1
    if rows != metadata["output_rows"]:
        raise ValueError("F001 legacy row count mismatch")
    old_code = legacy / "scripts/factors/active_take_midprice/intraday_window_factor.py"
    current_code = REPO_ROOT / "scripts/factors/active_take_midprice/intraday_window_factor.py"
    if sha256(old_code) != sha256(current_code):
        raise ValueError("F001 implementation differs from current code")

    destination = REPO_ROOT / "data/processed/imported/F001/legacy_sh_safe_202601_202602"
    destination.mkdir(parents=True, exist_ok=False)
    shutil.copy2(source_csv, destination / source_csv.name)
    shutil.copy2(source_meta, destination / source_meta.name)
    manifest = base_manifest("LI-F001-001", "factor_legacy_import", "F001", source_csv, destination)
    manifest.update({
        "scope": {"exchange": "SH", "months": ["202601", "202602"], "rows": rows},
        "definition": "active_take_midprice_intraday_safe_prebook_v1_20260807",
        "source_metadata": str(source_meta.resolve()),
        "source_metadata_sha256": sha256(source_meta),
        "current_code_sha256": sha256(current_code),
        "limitations": ["SZ counterpart was incomplete and was not imported", "R002 returns were not computed"],
    })
    write_manifest(REPO_ROOT / "runs/factors/F001/legacy_sh_safe_202601_202602/import_manifest.json", manifest)
    return manifest


def migrate_r001_f013(legacy: Path) -> list[dict]:
    old_cache = legacy / "data/cache/order_shape_mechanism/medium300_202601_v1"
    old_results = legacy / "results/intraday/order_shape_mechanism/medium300_202601_v1"
    old_manifest = json.loads((old_cache / "manifest.json").read_text(encoding="utf-8"))
    quality = json.loads((old_results / "quality_summary.json").read_text(encoding="utf-8"))
    done = len(list(old_cache.glob("batch_*/done.json")))
    if old_manifest["config"]["output_etf_symbols"] != 0 or done != 75:
        raise ValueError("F013/R001 completion or universe audit failed")
    if quality["totals"]["duplicate_trades"] or quality["totals"]["fill_over_submit"]:
        raise ValueError("F013/R001 conservation audit failed")
    old_code = legacy / "scripts/factors/order_shape_mechanism/engine.py"
    current_code = REPO_ROOT / "scripts/factors/order_shape_mechanism/engine.py"
    if sha256(old_code) != sha256(current_code):
        raise ValueError("F013 engine differs from current code")

    cache_dest = REPO_ROOT / "data/cache/imported/F013/medium300_202601_v1"
    result_dest = REPO_ROOT / "results/research/R001/legacy_medium300_202601_v1"
    copy_new(old_cache, cache_dest)
    copy_new(old_results, result_dest)
    legacy_manifest_dest = REPO_ROOT / "data/manifests/legacy/R001"
    legacy_manifest_dest.mkdir(parents=True, exist_ok=False)
    for name in (
        "order_shape_medium300_v4_paths_202510_202601.txt",
        "order_shape_medium300_v4_202601.metadata.json",
        "order_shape_medium300_domains_202601.csv",
    ):
        shutil.copy2(legacy / "data/manifests" / name, legacy_manifest_dest / name)

    factor_manifest = base_manifest("LI-F013-001", "factor_legacy_import", "F013", old_cache, cache_dest)
    factor_manifest.update({
        "scope": {"target_month": "202601", "symbols": 300, "stock_days": quality["stock_days"]},
        "definition": old_manifest["config"]["factor_version"],
        "input_manifest_sha256": old_manifest["config"]["file_list_sha256"],
        "current_code_sha256": sha256(current_code),
        "limitations": ["Core M1-M6 only", "M1-Q quote confirmation is excluded"],
    })
    factor_path = REPO_ROOT / "runs/factors/F013/legacy_medium300_202601_v1/import_manifest.json"
    write_manifest(factor_path, factor_manifest)

    research_manifest = base_manifest("LI-R001-001", "research_legacy_import", "R001", old_results, result_dest)
    research_manifest.update({
        "factor_import": str(factor_path.resolve()),
        "factor_import_sha256": sha256(factor_path),
        "decision": "M2/M3 stopped; M1/M6 retained as controls and candidate sources",
        "limitations": ["Mechanism/direct-target evidence only; no future-return claim"],
    })
    write_manifest(REPO_ROOT / "runs/research/R001/legacy_medium300_202601_v1/import_manifest.json", research_manifest)
    return [factor_manifest, research_manifest]


def migrate_p002_safe_columns(legacy: Path) -> dict:
    source = legacy / "data/cache/order_shape_mechanism/batch_a_medium300_202601_v1"
    source_manifest = json.loads((source / "manifest.json").read_text(encoding="utf-8"))
    if source_manifest["config"]["output_etf_symbols"] != 0:
        raise ValueError("P002 ETF audit failed")
    if len(list(source.glob("batch_*/done.json"))) != 150:
        raise ValueError("P002 source batches are incomplete")
    old_code = legacy / "scripts/factors/order_shape_mechanism/batch_a_engine.py"
    current_code = REPO_ROOT / "scripts/factors/order_shape_mechanism/batch_a_engine.py"
    if sha256(old_code) != sha256(current_code):
        raise ValueError("P002 engine differs from current code")

    destination = REPO_ROOT / "data/cache/imported/P002/batch_a_medium300_202601_safe_columns"
    destination.mkdir(parents=True, exist_ok=False)
    signals = destination / "signals.parquet"
    quality = destination / "quality.parquet"
    con = duckdb.connect()
    signal_glob = str(source / "batch_*/signals.csv").replace("'", "''")
    quality_glob = str(source / "batch_*/quality.csv").replace("'", "''")
    signal_target = str(signals).replace("'", "''")
    quality_target = str(quality).replace("'", "''")
    con.execute(
        f"COPY (SELECT * EXCLUDE (aggressive_add_buy, aggressive_add_sell, quote_aggressive_net) "
        f"FROM read_csv_auto('{signal_glob}', union_by_name=true)) TO '{signal_target}' "
        "(FORMAT PARQUET, COMPRESSION ZSTD)"
    )
    con.execute(
        f"COPY (SELECT * FROM read_csv_auto('{quality_glob}', union_by_name=true)) "
        f"TO '{quality_target}' (FORMAT PARQUET, COMPRESSION ZSTD)"
    )
    signal_rows = con.execute("SELECT count(*) FROM read_parquet(?)", [str(signals)]).fetchone()[0]
    con.close()
    metadata = {
        "kind": "sanitized_legacy_data_product", "product_id": "P002",
        "source_fingerprint": source_manifest["fingerprint"], "output_etf_symbols": 0,
        "target_month": "202601", "symbols": 300, "signal_rows": signal_rows,
        "dropped_affected_fields": ["aggressive_add_buy", "aggressive_add_sell", "quote_aggressive_net"],
        "allowed_use": "F014 NP01-NP05 definitions that do not reconstruct quote-arrival aggression",
        "source_manifest_sha256": sha256(source / "manifest.json"),
    }
    (destination / "metadata.json").write_text(json.dumps(metadata, ensure_ascii=False, indent=2) + "\n")
    manifest = base_manifest("LI-P002-001", "data_product_legacy_import", "P002", source, destination)
    manifest.update(metadata)
    write_manifest(REPO_ROOT / "runs/data_products/P002/legacy_medium300_202601_safe_columns/import_manifest.json", manifest)
    return manifest


def migrate_sz_partial(legacy: Path) -> list[dict]:
    source = legacy / "results/intraday/experiment_batch_1/mechanism_analysis_202601_202602_sz_v2_safe_prebook"
    metadata = json.loads((source / "metadata.json").read_text(encoding="utf-8"))
    if metadata["exchange"] != "SZ" or metadata["stock_month_files"] != 5764 or metadata["output_etf_symbols"] != 0:
        raise ValueError("P001 SZ fixed-result audit failed")
    cache = legacy / "data/cache/experiment_batch_1/intraday_1000_1030_202601_202602_sz_v2_safe_prebook"
    if len(list(cache.glob("batch_*/done.json"))) != 5764:
        raise ValueError("P001 SZ source batches are incomplete")
    old_code = legacy / "scripts/factors/experiment_batch_1/engine.py"
    current_code = REPO_ROOT / "scripts/factors/experiment_batch_1/engine.py"
    if sha256(old_code) != sha256(current_code):
        raise ValueError("P001 engine differs from current code")

    destination = REPO_ROOT / "data/cache/imported/P001/sz_safe_prebook_202601_202602_first_layer"
    copy_new(source, destination)
    product_manifest = base_manifest("LI-P001-001", "data_product_legacy_import", "P001", source, destination)
    product_manifest.update({
        "scope": {"exchange": "SZ", "months": metadata["months"], "stock_month_files": 5764},
        "definition": metadata["factor_version"], "current_code_sha256": sha256(current_code),
        "limitations": ["First-layer features only", "SH and future-return outputs are absent", "Raw 101GB lifecycle shards were verified but not duplicated"],
    })
    product_path = REPO_ROOT / "runs/data_products/P001/legacy_sz_safe_202601_202602/import_manifest.json"
    write_manifest(product_path, product_manifest)

    manifests = [product_manifest]
    mappings = {
        "F008": ("R005", ["d07_d09_d10_features.csv", "impact_retention_summary.csv", "impact_by_exchange.csv"]),
        "F010": ("R007", ["d07_d09_d10_features.csv", "d09_d10_feature_summary.csv", "quote_depth_bin_summary.csv"]),
        "F011": ("R008", ["d07_d09_d10_features.csv", "price_cap_grid.csv", "price_cap_grid_by_exchange.csv"]),
    }
    for factor_id, (research_id, files) in mappings.items():
        factor_manifest = {
            "kind": "factor_partial_legacy_import", "item_id": factor_id,
            "status": "partial_completed_audited", "exchange": "SZ",
            "months": metadata["months"], "data_product_import": str(product_path.resolve()),
            "data_product_import_sha256": sha256(product_path),
            "outputs": [{"path": str((destination / name).resolve()), "sha256": sha256(destination / name)} for name in files],
            "missing": ["SH", "raw future-return evaluation"],
        }
        factor_path = REPO_ROOT / f"runs/factors/{factor_id}/legacy_sz_safe_202601_202602/import_manifest.json"
        write_manifest(factor_path, factor_manifest)
        research_manifest = {
            "kind": "research_partial_legacy_import", "item_id": research_id,
            "status": "partial_completed_audited", "factor_import": str(factor_path.resolve()),
            "factor_import_sha256": sha256(factor_path), "exchange": "SZ",
            "months": metadata["months"], "missing": ["SH comparison", "registered raw-return outputs"],
        }
        write_manifest(REPO_ROOT / f"runs/research/{research_id}/legacy_sz_safe_202601_202602/import_manifest.json", research_manifest)
        manifests.extend([factor_manifest, research_manifest])
    return manifests


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--legacy-root", type=Path, default=DEFAULT_LEGACY_ROOT)
    args = parser.parse_args()
    manifests = []
    manifests.append(migrate_f001(args.legacy_root))
    manifests.extend(migrate_r001_f013(args.legacy_root))
    manifests.append(migrate_p002_safe_columns(args.legacy_root))
    manifests.extend(migrate_sz_partial(args.legacy_root))
    summary = {
        "kind": "legacy_migration_summary", "status": "PASS",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "imports": [{"import_id": row.get("import_id"), "item_id": row["item_id"], "status": row["status"]} for row in manifests],
        "not_imported": [
            "F001 SZ incomplete", "F002 definition not frozen", "F003 side-key/schema obsolete",
            "F004 depends on F002 and repaired run incomplete", "F005 repaired run incomplete",
            "F006 current intraday version never ran", "F007/F009/F012/F014-F017 unimplemented",
            "M1-Q and quote_aggressive_net repaired runs incomplete",
        ],
    }
    summary_path = REPO_ROOT / "runs/legacy_migration_summary.json"
    write_manifest(summary_path, summary)
    print(summary_path.relative_to(REPO_ROOT))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
