#!/usr/bin/env python3
"""Compute leakage-safe non-parent order-state signals with reusable history snapshots."""

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

from scripts.factors.order_shape_mechanism.non_parent_order_state_engine import (
    FACTOR_VERSION,
    NonParentOrderStateConfig,
    NonParentOrderStateEngine,
    NonParentOrderStateQuality,
)
from scripts.factors.order_shape_mechanism.reproduce_mechanisms_v4 import (
    atomic_write_csv,
    chunks,
    file_sha256,
    load_inputs,
    row_to_event,
    validate_month,
)


MONTH_QUERY = """
SELECT date::INTEGER, time::BIGINT, row_id::BIGINT, source_action,
       source_recid::BIGINT, source_buy_order_id::BIGINT,
       source_sell_order_id::BIGINT, source_side, source_price::BIGINT,
       source_volume::BIGINT,
       CASE WHEN array_length(bid_px)>0 THEN bid_px[1] END::BIGINT,
       CASE WHEN array_length(ask_px)>0 THEN ask_px[1] END::BIGINT,
       CASE WHEN array_length(bid_vol)>=1 THEN list_sum(list_slice(bid_vol,1,1)) END::BIGINT,
       CASE WHEN array_length(bid_vol)>=3 THEN list_sum(list_slice(bid_vol,1,3)) END::BIGINT,
       CASE WHEN array_length(bid_vol)>=10 THEN list_sum(list_slice(bid_vol,1,10)) END::BIGINT,
       CASE WHEN array_length(ask_vol)>=1 THEN list_sum(list_slice(ask_vol,1,1)) END::BIGINT,
       CASE WHEN array_length(ask_vol)>=3 THEN list_sum(list_slice(ask_vol,1,3)) END::BIGINT,
       CASE WHEN array_length(ask_vol)>=10 THEN list_sum(list_slice(ask_vol,1,10)) END::BIGINT
FROM read_parquet(?)
WHERE time>=95900000 AND time<104100000
"""

SIGNAL_FIELDS = [
    "symbol", "date", "signal_seconds", "signal_time", "state",
    "active_buy_volume", "active_sell_volume", "active_buy_count", "active_sell_count",
    "near_cancel_buy", "near_cancel_sell",
    "chain_buy_volume", "chain_sell_volume", "single_chain_count", "multi_chain_count", "chain_count",
    "active_net_share", "chain_net_share", "multi_chain_share",
    "quote_cancel_net", "spread_bps", "bid_depth3", "ask_depth3",
    "book_imbalance3", "pred_fill_buy", "pred_fill_sell", "fill_opportunity_diff",
    "fill_history_buy", "fill_history_sell",
    "future_buy_volume", "future_sell_volume", "future_net_flow", "future_total_active_volume",
    "future_buy_count", "future_sell_count", "future_event_count", "future_realized_vol_bps",
    "future_mid_moves", "end_spread_bps", "end_bid_depth3", "end_ask_depth3", "factor_version",
]
QUALITY_FIELDS = ["symbol", "date", *NonParentOrderStateQuality.__dataclass_fields__, "factor_version"]


def write_json(path: Path, value: dict[str, object]) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")
    os.replace(temporary, path)


def process_symbol(
    symbol: str,
    month_paths: dict[str, str],
    warmup_months: Sequence[str],
    target_month: str,
    config: NonParentOrderStateConfig,
    memory_limit: str,
    fetch_rows: int,
    history_state_dir: str,
    history_fingerprint: str,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    connection = duckdb.connect()
    connection.execute("PRAGMA threads=1")
    connection.execute(f"PRAGMA memory_limit='{memory_limit}'")
    connection.execute("PRAGMA preserve_insertion_order=true")
    connection.execute("PRAGMA enable_progress_bar=false")
    try:
        snapshot_path = Path(history_state_dir) / f"{symbol}.json"
        if snapshot_path.exists():
            snapshot = json.loads(snapshot_path.read_text())
            if snapshot.get("history_fingerprint") != history_fingerprint:
                raise ValueError(f"history snapshot fingerprint mismatch: {snapshot_path}")
        else:
            history_engine = NonParentOrderStateEngine(symbol, config)
            for month in warmup_months:
                cursor = connection.execute(MONTH_QUERY, [month_paths[month]])
                while True:
                    rows = cursor.fetchmany(fetch_rows)
                    if not rows:
                        break
                    for row in rows:
                        history_engine.process(row_to_event(row))
            snapshot = history_engine.export_history_snapshot()
            snapshot["history_months"] = list(warmup_months)
            snapshot["history_fingerprint"] = history_fingerprint
            write_json(snapshot_path, snapshot)

        engine = NonParentOrderStateEngine(symbol, config)
        engine.load_history_snapshot(snapshot)
        cursor = connection.execute(MONTH_QUERY, [month_paths[target_month]])
        while True:
            rows = cursor.fetchmany(fetch_rows)
            if not rows:
                break
            for row in rows:
                engine.process(row_to_event(row))
        signals, quality = engine.finish()
        safe_signals = [
            {field: row[field] for field in SIGNAL_FIELDS}
            for row in signals if int(row["signal_time"]) == 1030
        ]
        return safe_signals, quality
    finally:
        connection.close()


def validate_batch_dir(path: Path) -> None:
    required = {"signals.csv", "quality.csv", "done.json"}
    missing = sorted(name for name in required if not (path / name).is_file())
    if missing:
        raise ValueError(f"incomplete batch shard {path}: missing {missing}")
    for filename, fields in (("signals.csv", SIGNAL_FIELDS), ("quality.csv", QUALITY_FIELDS)):
        with (path / filename).open(newline="") as handle:
            header = next(csv.reader(handle), None)
        if header != list(fields):
            raise ValueError(f"incompatible shard schema: {path / filename}")


def compute_batch_worker(
    batch_number: int,
    symbols: Sequence[str],
    inputs: dict[str, dict[str, str]],
    warmup_months: Sequence[str],
    target_month: str,
    config: NonParentOrderStateConfig,
    memory_limit: str,
    fetch_rows: int,
    shard_dir: str,
    history_state_dir: str,
    history_fingerprint: str,
) -> tuple[int, int, int, int]:
    root = Path(shard_dir)
    final_dir = root / f"batch_{batch_number:06d}"
    temporary = root / f".batch_{batch_number:06d}.{os.getpid()}.tmp"
    if temporary.exists():
        shutil.rmtree(temporary)
    temporary.mkdir(parents=True)
    signals: list[dict[str, object]] = []
    quality: list[dict[str, object]] = []
    try:
        for symbol in symbols:
            symbol_signals, symbol_quality = process_symbol(
                symbol, inputs[symbol], warmup_months, target_month, config,
                memory_limit, fetch_rows, history_state_dir, history_fingerprint,
            )
            signals.extend(symbol_signals)
            quality.extend(symbol_quality)
        atomic_write_csv(temporary / "signals.csv", SIGNAL_FIELDS, signals)
        atomic_write_csv(temporary / "quality.csv", QUALITY_FIELDS, quality)
        write_json(temporary / "done.json", {
            "batch": batch_number, "symbols": list(symbols), "signal_rows": len(signals),
            "quality_rows": len(quality), "factor_version": FACTOR_VERSION,
        })
        os.replace(temporary, final_dir)
        return batch_number, len(symbols), len(signals), len(quality)
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)


def build_manifest(
    file_list: Path,
    metadata_path: Path,
    metadata: dict[str, object],
    inputs: dict[str, dict[str, str]],
    warmup_months: Sequence[str],
    config: NonParentOrderStateConfig,
    batch_size: int,
    exchange: str | None,
    history_state_dir: Path,
) -> dict[str, object]:
    history_body = {
        "factor_version": FACTOR_VERSION,
        "file_list_sha256": file_sha256(file_list),
        "universe_metadata_sha256": file_sha256(metadata_path),
        "history_months": list(warmup_months),
        "config": {
            "observation_seconds": config.observation_seconds,
            "intensity_reservoir_size": config.intensity_reservoir_size,
            "minimum_fill_history": config.minimum_fill_history,
            "near_bps": config.near_bps,
        },
    }
    history_fingerprint = hashlib.sha256(
        json.dumps(history_body, sort_keys=True).encode()
    ).hexdigest()
    body = {
        "factor_version": FACTOR_VERSION,
        "file_list": str(file_list.resolve()), "file_list_sha256": file_sha256(file_list),
        "universe_metadata": str(metadata_path.resolve()),
        "universe_metadata_sha256": file_sha256(metadata_path),
        "universe_rule": metadata.get("universe_rule"), "domain_rule": metadata.get("domain_rule"),
        "output_etf_symbols": metadata.get("output_etf_symbols"),
        "history_months": list(warmup_months), "target_month": config.target_month,
        "symbols": len(inputs),
        "history_stock_month_files": len(inputs) * len(warmup_months),
        "target_stock_month_files": len(inputs),
        "history_state_dir": str(history_state_dir.resolve()),
        "history_fingerprint": history_fingerprint,
        "batch_size": batch_size, "config": asdict(config),
        "exchange": exchange,
        "event_scan_window": "[09:59,10:41)",
        "model_order_window": "[09:59,10:40); final minute is label-only for 60s fill completion",
        "signal_rule": "fixed 10:30 only; features use (10:29,10:30); labels use [10:30,10:40)",
        "fill_model_rule": "time-local 10:30 history; reusable pre-target snapshot plus expanding prior-day-only empirical 60s passive fill probabilities",
        "label_policy": "direct targets only; no return label; each label handled independently",
        "target_projection": [
            "date", "time", "row_id", "source_action", "source_recid",
            "source_buy_order_id", "source_sell_order_id", "source_side", "source_price",
            "source_volume", "bid_px1", "ask_px1", "bid_depth1/3/10", "ask_depth1/3/10",
        ],
        "excluded_columns": ["bid_ordvol", "ask_ordvol", "bid_cnt", "ask_cnt"],
        "post_processed_link_fields_used": [],
        "dropped_output_fields": ["aggressive_add_buy", "aggressive_add_sell", "quote_aggressive_net"],
    }
    encoded = json.dumps(body, ensure_ascii=False, sort_keys=True).encode()
    return {"fingerprint": hashlib.sha256(encoded).hexdigest(), "config": body}


def prepare_shard_dir(path: Path, manifest: dict[str, object]) -> None:
    path.mkdir(parents=True, exist_ok=True)
    manifest_path = path / "manifest.json"
    if manifest_path.exists():
        existing = json.loads(manifest_path.read_text())
        if existing.get("fingerprint") != manifest["fingerprint"]:
            raise ValueError(f"run manifest mismatch: {manifest_path}; use a new shard directory")
        return
    if list(path.glob("batch_*")):
        raise ValueError(f"batch shards exist without manifest: {path}")
    write_json(manifest_path, manifest)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute non-parent order-state fixed-grid signals.")
    parser.add_argument("--file-list", type=Path, required=True)
    parser.add_argument("--universe-metadata", type=Path, required=True)
    parser.add_argument("--warmup-months", nargs="+", required=True)
    parser.add_argument("--target-month", required=True)
    parser.add_argument("--exchange", choices=("SH", "SZ"))
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--batch-size", type=int, default=4)
    parser.add_argument("--fetch-rows", type=int, default=10_000)
    parser.add_argument("--memory-limit", default="1GB")
    parser.add_argument("--shard-dir", type=Path, required=True)
    parser.add_argument("--history-state-dir", type=Path, required=True)
    parser.add_argument("--audit-symbols", nargs="*")
    parser.add_argument("--limit-symbols", type=int)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    target = validate_month(args.target_month)
    warmup = [validate_month(value) for value in args.warmup_months]
    months = sorted(set(warmup + [target]))
    if not all(month < target for month in warmup):
        raise ValueError("all warmup months must precede target month")
    inputs, metadata = load_inputs(args.file_list, args.universe_metadata, warmup, target)
    inputs = {symbol: paths for symbol, paths in inputs.items() if all(month in paths for month in months)}
    if args.exchange:
        inputs = {
            symbol: paths for symbol, paths in inputs.items()
            if symbol.startswith(args.exchange)
        }
    if args.audit_symbols:
        missing = set(args.audit_symbols) - set(inputs)
        if missing:
            raise ValueError(f"audit symbols missing complete months: {sorted(missing)}")
        inputs = {symbol: inputs[symbol] for symbol in sorted(set(args.audit_symbols))}
    if args.limit_symbols is not None:
        inputs = dict(list(inputs.items())[: args.limit_symbols])
    if not inputs:
        raise ValueError("no complete symbol histories")
    config = NonParentOrderStateConfig(target_month=target)
    manifest = build_manifest(
        args.file_list, args.universe_metadata, metadata, inputs, warmup, config,
        args.batch_size, args.exchange, args.history_state_dir,
    )
    if args.dry_run:
        print(json.dumps(manifest, ensure_ascii=False, indent=2))
        return 0
    prepare_shard_dir(args.shard_dir, manifest)
    args.history_state_dir.mkdir(parents=True, exist_ok=True)
    history_fingerprint = str(manifest["config"]["history_fingerprint"])
    symbols = sorted(inputs)
    batches = list(enumerate(chunks(symbols, args.batch_size), start=1))
    pending = []
    resumed = 0
    for batch_number, batch_symbols in batches:
        path = args.shard_dir / f"batch_{batch_number:06d}"
        if path.exists():
            validate_batch_dir(path); resumed += len(batch_symbols)
        else:
            pending.append((batch_number, batch_symbols))
    print(f"resume_batches={len(batches)-len(pending)}/{len(batches)} resume_symbols={resumed}/{len(symbols)}", flush=True)
    completed_symbols = resumed
    if args.workers == 1:
        for completed, (batch_number, batch_symbols) in enumerate(pending, 1):
            _, count, signal_rows, quality_rows = compute_batch_worker(
                batch_number, batch_symbols, inputs, warmup, target, config,
                args.memory_limit, args.fetch_rows, str(args.shard_dir),
                str(args.history_state_dir), history_fingerprint,
            )
            completed_symbols += count
            print(f"new_batches={completed}/{len(pending)} symbols={completed_symbols}/{len(symbols)} signals={signal_rows} quality={quality_rows}", flush=True)
    else:
        with ProcessPoolExecutor(max_workers=args.workers, mp_context=get_context("spawn")) as executor:
            futures = {
                executor.submit(
                    compute_batch_worker, batch_number, batch_symbols, inputs,
                    warmup, target, config, args.memory_limit, args.fetch_rows,
                    str(args.shard_dir), str(args.history_state_dir), history_fingerprint,
                ): batch_number
                for batch_number, batch_symbols in pending
            }
            for completed, future in enumerate(as_completed(futures), 1):
                batch_number, count, signal_rows, quality_rows = future.result()
                completed_symbols += count
                print(f"new_batches={completed}/{len(pending)} batch={batch_number} symbols={completed_symbols}/{len(symbols)} signals={signal_rows} quality={quality_rows}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
