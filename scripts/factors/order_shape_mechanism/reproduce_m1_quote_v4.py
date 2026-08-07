#!/usr/bin/env python3
"""Run M1-Q with one projected read per target-month V4 file."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import shutil
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict
from multiprocessing import get_context
from pathlib import Path
from typing import Sequence

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.factors.order_shape_mechanism.m1_quote_engine import (
    FACTOR_VERSION,
    M1QuoteConfig,
    M1QuoteEngine,
    M1QuoteQuality,
)
from scripts.factors.order_shape_mechanism.reproduce_mechanisms_v4 import (
    STAT_FIELDS,
    TARGET_QUERY,
    atomic_write_csv,
    chunks,
    file_sha256,
    load_inputs,
    row_to_event,
    validate_month,
)

QUALITY_FIELDS = [
    "symbol",
    "date",
    *M1QuoteQuality.__dataclass_fields__,
    "factor_version",
]


def process_symbol(
    symbol: str,
    path: str,
    config: M1QuoteConfig,
    memory_limit: str,
    fetch_rows: int,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    connection = duckdb.connect()
    connection.execute("PRAGMA threads=1")
    connection.execute(f"PRAGMA memory_limit='{memory_limit}'")
    connection.execute("PRAGMA preserve_insertion_order=true")
    engine = M1QuoteEngine(symbol, config)
    try:
        cursor = connection.execute(TARGET_QUERY, [path])
        while True:
            rows = cursor.fetchmany(fetch_rows)
            if not rows:
                break
            for row in rows:
                engine.process(row_to_event(row))
        return engine.finish()
    finally:
        connection.close()


def write_json(path: Path, value: dict[str, object]) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")
    os.replace(temporary, path)


def validate_batch_dir(path: Path) -> None:
    required = {"stats.csv", "quality.csv", "done.json"}
    missing = sorted(name for name in required if not (path / name).is_file())
    if missing:
        raise ValueError(f"incomplete batch shard {path}: missing {missing}")
    for filename, fields in (("stats.csv", STAT_FIELDS), ("quality.csv", QUALITY_FIELDS)):
        with (path / filename).open(newline="") as handle:
            header = next(csv.reader(handle), None)
        if header != list(fields):
            raise ValueError(f"incompatible shard schema: {path / filename}")


def compute_batch_worker(
    batch_number: int,
    symbols: Sequence[str],
    inputs: dict[str, dict[str, str]],
    target_month: str,
    config: M1QuoteConfig,
    memory_limit: str,
    fetch_rows: int,
    shard_dir: str,
) -> tuple[int, int, int, int]:
    root = Path(shard_dir)
    final_dir = root / f"batch_{batch_number:06d}"
    temporary = root / f".batch_{batch_number:06d}.{os.getpid()}.tmp"
    if temporary.exists():
        shutil.rmtree(temporary)
    temporary.mkdir(parents=True)
    stats: list[dict[str, object]] = []
    quality: list[dict[str, object]] = []
    try:
        for symbol in symbols:
            symbol_stats, symbol_quality = process_symbol(
                symbol,
                inputs[symbol][target_month],
                config,
                memory_limit,
                fetch_rows,
            )
            stats.extend(symbol_stats)
            quality.extend(symbol_quality)
        atomic_write_csv(temporary / "stats.csv", STAT_FIELDS, stats)
        atomic_write_csv(temporary / "quality.csv", QUALITY_FIELDS, quality)
        write_json(
            temporary / "done.json",
            {
                "batch": batch_number,
                "symbols": list(symbols),
                "stat_rows": len(stats),
                "quality_rows": len(quality),
                "factor_version": FACTOR_VERSION,
            },
        )
        os.replace(temporary, final_dir)
        return batch_number, len(symbols), len(stats), len(quality)
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)


def build_manifest(
    file_list: Path,
    metadata_path: Path,
    metadata: dict[str, object],
    inputs: dict[str, dict[str, str]],
    target_month: str,
    config: M1QuoteConfig,
    batch_size: int,
    exchange: str | None,
) -> dict[str, object]:
    body = {
        "factor_version": FACTOR_VERSION,
        "file_list": str(file_list.resolve()),
        "file_list_sha256": file_sha256(file_list),
        "universe_metadata": str(metadata_path.resolve()),
        "universe_metadata_sha256": file_sha256(metadata_path),
        "universe_rule": metadata.get("universe_rule"),
        "domain_rule": metadata.get("domain_rule"),
        "output_etf_symbols": metadata.get("output_etf_symbols"),
        "target_month": target_month,
        "symbols": len(inputs),
        "target_files": len(inputs),
        "batch_size": batch_size,
        "exchange": exchange,
        "target_projection": [
            "date",
            "time",
            "row_id",
            "source_action",
            "source_recid",
            "source_buy_order_id",
            "source_sell_order_id",
            "source_side",
            "source_price",
            "source_volume",
            "bid_px",
            "ask_px",
            "bid_vol",
            "ask_vol",
        ],
        "excluded_columns": [
            "bid_ordvol",
            "ask_ordvol",
            "bid_cnt",
            "ask_cnt",
            "source_link_status",
        ],
        "warmup_files": 0,
        "config": asdict(config),
    }
    encoded = json.dumps(body, ensure_ascii=False, sort_keys=True).encode()
    return {"fingerprint": hashlib.sha256(encoded).hexdigest(), "config": body}


def prepare_shard_dir(path: Path, manifest: dict[str, object]) -> None:
    path.mkdir(parents=True, exist_ok=True)
    manifest_path = path / "manifest.json"
    if manifest_path.exists():
        existing = json.loads(manifest_path.read_text())
        if existing.get("fingerprint") != manifest["fingerprint"]:
            raise ValueError(
                f"run manifest mismatch: {manifest_path}; use a new shard directory"
            )
        return
    if list(path.glob("batch_*")):
        raise ValueError(f"batch shards exist without manifest: {path}")
    write_json(manifest_path, manifest)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run target-month-only M1-Q chain and quote-response experiment."
    )
    parser.add_argument("--file-list", type=Path, required=True)
    parser.add_argument("--universe-metadata", type=Path, required=True)
    parser.add_argument("--target-month", required=True)
    parser.add_argument("--exchange", choices=("SH", "SZ"))
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--batch-size", type=int, default=4)
    parser.add_argument("--fetch-rows", type=int, default=10_000)
    parser.add_argument("--memory-limit", default="1GB")
    parser.add_argument("--shard-dir", type=Path, required=True)
    parser.add_argument("--audit-symbols", nargs="*")
    parser.add_argument("--limit-symbols", type=int)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    target_month = validate_month(args.target_month)
    if args.workers <= 0 or args.batch_size <= 0 or args.fetch_rows <= 0:
        raise ValueError("workers, batch-size, and fetch-rows must be positive")
    inputs, metadata = load_inputs(
        args.file_list, args.universe_metadata, [], target_month
    )
    if args.exchange:
        inputs = {
            symbol: paths for symbol, paths in inputs.items()
            if symbol.startswith(args.exchange)
        }
    if args.audit_symbols:
        requested = set(args.audit_symbols)
        missing = requested - set(inputs)
        if missing:
            raise ValueError(f"audit symbols missing from target month: {sorted(missing)}")
        inputs = {symbol: inputs[symbol] for symbol in sorted(requested)}
    if args.limit_symbols is not None:
        if args.limit_symbols <= 0:
            raise ValueError("limit-symbols must be positive")
        inputs = dict(list(inputs.items())[: args.limit_symbols])
    config = M1QuoteConfig()
    manifest = build_manifest(
        args.file_list,
        args.universe_metadata,
        metadata,
        inputs,
        target_month,
        config,
        args.batch_size,
        args.exchange,
    )
    if args.dry_run:
        print(json.dumps(manifest, ensure_ascii=False, indent=2))
        return 0
    prepare_shard_dir(args.shard_dir, manifest)
    symbols = sorted(inputs)
    batches = list(enumerate(chunks(symbols, args.batch_size), start=1))
    pending: list[tuple[int, Sequence[str]]] = []
    resumed_symbols = 0
    for batch_number, batch_symbols in batches:
        path = args.shard_dir / f"batch_{batch_number:06d}"
        if path.exists():
            validate_batch_dir(path)
            resumed_symbols += len(batch_symbols)
        else:
            pending.append((batch_number, batch_symbols))
    print(
        f"resume_batches={len(batches)-len(pending)}/{len(batches)} "
        f"resume_symbols={resumed_symbols}/{len(symbols)}",
        flush=True,
    )
    completed_symbols = resumed_symbols
    if args.workers == 1:
        for completed, (batch_number, batch_symbols) in enumerate(pending, start=1):
            _, count, stat_rows, quality_rows = compute_batch_worker(
                batch_number,
                batch_symbols,
                inputs,
                target_month,
                config,
                args.memory_limit,
                args.fetch_rows,
                str(args.shard_dir),
            )
            completed_symbols += count
            print(
                f"new_batches={completed}/{len(pending)} "
                f"symbols={completed_symbols}/{len(symbols)} "
                f"stat_rows={stat_rows} quality_rows={quality_rows}",
                flush=True,
            )
    else:
        context = get_context("spawn")
        with ProcessPoolExecutor(
            max_workers=args.workers, mp_context=context
        ) as executor:
            futures = {
                executor.submit(
                    compute_batch_worker,
                    batch_number,
                    batch_symbols,
                    inputs,
                    target_month,
                    config,
                    args.memory_limit,
                    args.fetch_rows,
                    str(args.shard_dir),
                ): batch_number
                for batch_number, batch_symbols in pending
            }
            for completed, future in enumerate(as_completed(futures), start=1):
                batch_number, count, stat_rows, quality_rows = future.result()
                completed_symbols += count
                print(
                    f"new_batches={completed}/{len(pending)} batch={batch_number} "
                    f"symbols={completed_symbols}/{len(symbols)} "
                    f"stat_rows={stat_rows} quality_rows={quality_rows}",
                    flush=True,
                )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
