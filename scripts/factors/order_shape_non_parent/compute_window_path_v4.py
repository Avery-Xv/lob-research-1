#!/usr/bin/env python3
"""Compute the full-market 10:00-10:30 window-path cache for R016/R017."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import shutil
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import get_context
from pathlib import Path
from typing import Sequence

import duckdb


REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.factors.order_shape_mechanism.reproduce_mechanisms_v4 import (
    atomic_write_csv, chunks, file_sha256, load_inputs, row_to_event, validate_month,
)
from scripts.factors.order_shape_non_parent.window_path_engine import (
    FACTOR_VERSION, WindowPathEngine, WindowPathQuality,
)


QUERY = """
SELECT date::INTEGER,time::BIGINT,row_id::BIGINT,source_action,
       source_recid::BIGINT,source_buy_order_id::BIGINT,source_sell_order_id::BIGINT,
       source_side,source_price::BIGINT,source_volume::BIGINT,
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


def output_fields() -> list[str]:
    book_metrics = [
        "coverage_seconds", "coverage_ratio", "bi1_twap", "bi3_twap", "bi3_time_std",
        "bi10_twap", "spread_bps_twap", "positive_time_share", "negative_time_share",
        "bid1_twap", "bid3_twap", "bid10_twap", "ask1_twap", "ask3_twap", "ask10_twap",
        "depth1_to_depth3", "depth3_to_depth10",
    ]
    flow_metrics = [
        "buy_volume", "sell_volume", "net_share", "total_volume",
        "buy_order_count", "sell_order_count", "order_count",
    ]
    fields = ["symbol", "date", "signal_time"]
    for prefix in ("book30m", "book5m"):
        fields.extend(f"{prefix}_{name}" for name in book_metrics)
    for prefix in ("flow30m", "flow5m", "flow1m"):
        fields.extend(f"{prefix}_{name}" for name in flow_metrics)
    fields.extend(["endpoint_bi3", "endpoint_spread_bps", "endpoint_bid_depth3", "endpoint_ask_depth3"])
    for index in range(1, 7):
        fields.extend(f"bin{index}_book_{name}" for name in book_metrics)
        fields.extend(f"bin{index}_flow_{name}" for name in flow_metrics)
    fields.extend([
        "book_shift_5m_minus_30m", "flow_shift_5m_minus_30m", "endpoint_minus_book5m",
        "invalid_chain_seconds", "book_sign_flips", "longest_positive_seconds", "longest_negative_seconds",
    ])
    for minutes in (1, 5, 10):
        fields.extend(f"future{minutes}m_{name}" for name in flow_metrics)
        fields.extend([
            f"future{minutes}m_event_count", f"future{minutes}m_realized_vol_bps",
            f"future{minutes}m_end_bi3", f"future{minutes}m_end_spread_bps",
        ])
    fields.append("factor_version")
    return fields


SIGNAL_FIELDS = output_fields()
QUALITY_FIELDS = ["symbol", "date", *WindowPathQuality.__dataclass_fields__, "factor_version"]


def write_json(path: Path, value: dict[str, object]) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def process_symbol(symbol: str, path: str, target_month: str, memory_limit: str, fetch_rows: int) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    connection = duckdb.connect()
    connection.execute("PRAGMA threads=1")
    connection.execute(f"PRAGMA memory_limit='{memory_limit}'")
    connection.execute("PRAGMA preserve_insertion_order=true")
    engine = WindowPathEngine(symbol, target_month)
    try:
        cursor = connection.execute(QUERY, [path])
        while True:
            rows = cursor.fetchmany(fetch_rows)
            if not rows:
                break
            for row in rows:
                engine.process(row_to_event(row))
        return engine.finish()
    finally:
        connection.close()


def compute_batch_worker(batch_number: int, symbols: Sequence[str], inputs: dict[str, dict[str, str]], target_month: str, memory_limit: str, fetch_rows: int, shard_dir: str) -> tuple[int, int, int]:
    root = Path(shard_dir)
    final = root / f"batch_{batch_number:06d}"
    temporary = root / f".batch_{batch_number:06d}.{os.getpid()}.tmp"
    if temporary.exists():
        shutil.rmtree(temporary)
    temporary.mkdir(parents=True)
    signals: list[dict[str, object]] = []
    quality: list[dict[str, object]] = []
    try:
        for symbol in symbols:
            symbol_signals, symbol_quality = process_symbol(symbol, inputs[symbol][target_month], target_month, memory_limit, fetch_rows)
            signals.extend(symbol_signals); quality.extend(symbol_quality)
        atomic_write_csv(temporary / "window_paths.csv", SIGNAL_FIELDS, signals)
        atomic_write_csv(temporary / "quality.csv", QUALITY_FIELDS, quality)
        write_json(temporary / "done.json", {
            "batch": batch_number, "symbols": list(symbols), "signal_rows": len(signals),
            "quality_rows": len(quality), "factor_version": FACTOR_VERSION,
        })
        os.replace(temporary, final)
        return batch_number, len(symbols), len(signals)
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)


def validate_batch(path: Path) -> None:
    for filename in ("window_paths.csv", "quality.csv", "done.json"):
        if not (path / filename).is_file():
            raise ValueError(f"incomplete batch: {path}")
    with (path / "window_paths.csv").open(newline="") as handle:
        if next(csv.reader(handle), None) != SIGNAL_FIELDS:
            raise ValueError(f"incompatible schema: {path}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--file-list", type=Path, required=True)
    parser.add_argument("--universe-metadata", type=Path, required=True)
    parser.add_argument("--target-month", required=True)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--batch-size", type=int, default=4)
    parser.add_argument("--fetch-rows", type=int, default=10_000)
    parser.add_argument("--memory-limit", default="1GB")
    parser.add_argument("--shard-dir", type=Path, required=True)
    parser.add_argument("--audit-symbols", nargs="*")
    parser.add_argument("--limit-symbols", type=int)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    target = validate_month(args.target_month)
    inputs, metadata = load_inputs(args.file_list, args.universe_metadata, [], target)
    if args.audit_symbols:
        missing = set(args.audit_symbols) - set(inputs)
        if missing:
            raise ValueError(f"audit symbols missing: {sorted(missing)}")
        inputs = {symbol: inputs[symbol] for symbol in sorted(set(args.audit_symbols))}
    if args.limit_symbols is not None:
        inputs = dict(list(inputs.items())[:args.limit_symbols])
    body = {
        "factor_version": FACTOR_VERSION, "target_month": target,
        "file_list": str(args.file_list.resolve()), "file_list_sha256": file_sha256(args.file_list),
        "universe_metadata": str(args.universe_metadata.resolve()),
        "universe_metadata_sha256": file_sha256(args.universe_metadata),
        "universe_rule": metadata.get("universe_rule"), "output_etf_symbols": metadata.get("output_etf_symbols"),
        "symbols": len(inputs), "workers": args.workers, "batch_size": args.batch_size,
        "event_scan_window": "[09:59,10:41)",
        "signal_rule": "10:30; primary current state [10:25,10:30), background [10:00,10:30), legacy [10:29,10:30)",
        "book_rule": "post-event valid uncrossed snapshots held until next event; duration weighted; invalid chains retain last valid book",
        "active_order_key": "(source_side,direction-specific active_order_id)",
        "direct_targets": ["[10:30,10:31)", "[10:30,10:35)", "[10:30,10:40)"],
        "post_processed_link_fields_used": [],
    }
    fingerprint = hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()
    manifest = {"fingerprint": fingerprint, "config": body}
    if args.dry_run:
        print(json.dumps(manifest, ensure_ascii=False, indent=2)); return 0
    args.shard_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = args.shard_dir / "manifest.json"
    if manifest_path.exists():
        if json.loads(manifest_path.read_text()).get("fingerprint") != fingerprint:
            raise ValueError("run manifest mismatch; use a new shard directory")
    else:
        if list(args.shard_dir.glob("batch_*")):
            raise ValueError("batch shards exist without manifest")
        write_json(manifest_path, manifest)
    batches = list(enumerate(chunks(sorted(inputs), args.batch_size), start=1))
    pending = []
    resumed = 0
    for number, symbols in batches:
        path = args.shard_dir / f"batch_{number:06d}"
        if path.exists():
            validate_batch(path); resumed += len(symbols)
        else:
            pending.append((number, symbols))
    print(f"resume_symbols={resumed}/{len(inputs)} pending_batches={len(pending)}", flush=True)
    completed = resumed
    if args.workers == 1:
        for index, (number, symbols) in enumerate(pending, start=1):
            _, count, rows = compute_batch_worker(number, symbols, inputs, target, args.memory_limit, args.fetch_rows, str(args.shard_dir))
            completed += count
            print(f"new_batches={index}/{len(pending)} symbols={completed}/{len(inputs)} rows={rows}", flush=True)
    else:
        with ProcessPoolExecutor(max_workers=args.workers, mp_context=get_context("spawn")) as executor:
            futures = {
                executor.submit(compute_batch_worker, number, symbols, inputs, target, args.memory_limit, args.fetch_rows, str(args.shard_dir)): number
                for number, symbols in pending
            }
            for index, future in enumerate(as_completed(futures), start=1):
                number, count, rows = future.result(); completed += count
                print(f"new_batches={index}/{len(pending)} batch={number} symbols={completed}/{len(inputs)} rows={rows}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
