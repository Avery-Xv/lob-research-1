#!/usr/bin/env python3
"""Run the unified 10:00-10:30 experiment batch on manifest-approved stocks."""

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
from typing import Iterable, Sequence

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.factors.experiment_batch_1.engine import (
    FACTOR_VERSION,
    BatchEngine,
    Event,
)


V4_ROOT = Path("/hdd_data/lob/event_depth10_v4")
QUERY = """
SELECT date::INTEGER, time::BIGINT, row_id::BIGINT, source_action,
       source_recid::BIGINT, source_buy_order_id::BIGINT,
       source_sell_order_id::BIGINT, source_side, source_price::BIGINT,
       source_volume::BIGINT,
       CASE WHEN array_length(bid_px)>0 THEN bid_px[1] END::BIGINT,
       CASE WHEN array_length(ask_px)>0 THEN ask_px[1] END::BIGINT,
       CASE WHEN array_length(bid_vol)>0 THEN bid_vol[1] END::BIGINT,
       CASE WHEN array_length(bid_vol)>=3 THEN list_sum(list_slice(bid_vol,1,3)) END::BIGINT,
       CASE WHEN array_length(ask_vol)>0 THEN ask_vol[1] END::BIGINT,
       CASE WHEN array_length(ask_vol)>=3 THEN list_sum(list_slice(ask_vol,1,3)) END::BIGINT,
       CASE WHEN array_length(bid_cnt)>0 THEN bid_cnt[1] END::BIGINT,
       CASE WHEN array_length(ask_cnt)>0 THEN ask_cnt[1] END::BIGINT
FROM read_parquet(?)
WHERE time>=100000000 AND time<103000000
"""

SIGNAL_FIELDS = [
    "symbol", "date", "signal_time", "window_start", "window_end_exclusive",
    "active_buy_volume", "active_sell_volume", "active_buy_count", "active_sell_count",
    "active_net_share", "chain_count", "multi_trade_chain_count",
    "multi_trade_chain_volume_share", "chain_volume_hhi", "chain_volume_entropy",
    "largest_chain_volume_share", "mean_spread_bps", "mean_bid_depth1", "mean_ask_depth1",
    "mean_bid_depth3", "mean_ask_depth3", "mean_bid_count1", "mean_ask_count1",
    "passive_improve_buy_count", "passive_improve_sell_count",
    "passive_improve_buy_volume", "passive_improve_sell_volume", "new_quote_count",
    "new_quote_relative_depth_mean", "new_quote_thin_share_lt_0_5",
    "new_quote_rehit_share", "new_quote_restored_share", "new_quote_censored_count",
    "impact_observations_5s", "directional_immediate_impact_mean_5s",
    "directional_retained_impact_mean_5s", "impact_reversal_share_5s",
    "impact_observations_30s", "directional_immediate_impact_mean_30s",
    "directional_retained_impact_mean_30s", "impact_reversal_share_30s",
    "impact_observations_60s", "directional_immediate_impact_mean_60s",
    "directional_retained_impact_mean_60s", "impact_reversal_share_60s", "factor_version",
]
CHAIN_FIELDS = [
    "symbol", "date", "side", "active_order_id", "first_seconds", "last_seconds",
    "duration_seconds", "trade_count", "volume", "notional",
    "directional_impact_bps_sum", "depth_loss", "acceleration_seconds", "factor_version",
]
QUOTE_FIELDS = [
    "symbol", "date", "side", "start_row_id", "end_row_id", "quote_price",
    "quote_quantity", "quote_count", "prior_side_quantity", "relative_depth",
    "lifetime_seconds", "rehit", "restored_pre_event_book", "censored_at_signal",
    "removal_action", "factor_version",
]
QUALITY_FIELDS = [
    "symbol", "date", "total_events", "valid_books", "invalid_books", "trade_events",
    "missing_books", "locked_books", "crossed_books",
    "missing_active_order_id", "impact_events", "impact_censored_5s",
    "impact_censored_30s", "impact_censored_60s", "quote_improvements",
    "quote_censored", "atomic_book_chains", "atomic_impact_events",
    "atomic_ambiguous_chains", "unresolved_atomic_chains", "factor_version",
]


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def atomic_write_csv(path: Path, fields: Sequence[str], rows: Sequence[dict[str, object]]) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def write_json(path: Path, value: dict[str, object]) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")
    os.replace(temporary, path)


def chunks(items: Sequence[tuple[str, str, str]], size: int) -> Iterable[Sequence[tuple[str, str, str]]]:
    for index in range(0, len(items), size):
        yield items[index:index + size]


def row_to_event(row: Sequence[object]) -> Event:
    integer = lambda value: int(value) if value is not None else None
    return Event(
        date=int(row[0]), time=int(row[1]), row_id=int(row[2]), action=str(row[3]),
        recid=integer(row[4]), buy_order_id=integer(row[5]), sell_order_id=integer(row[6]),
        side=str(row[7]) if row[7] is not None else None, price=integer(row[8]),
        volume=integer(row[9]), bid1=integer(row[10]), ask1=integer(row[11]),
        bid_depth1=integer(row[12]), bid_depth3=integer(row[13]),
        ask_depth1=integer(row[14]), ask_depth3=integer(row[15]),
        bid_count1=integer(row[16]), ask_count1=integer(row[17]),
    )


def process_file(path: str, symbol: str, memory_limit: str, fetch_rows: int):
    connection = duckdb.connect()
    connection.execute("PRAGMA threads=1")
    connection.execute(f"PRAGMA memory_limit='{memory_limit}'")
    connection.execute("PRAGMA preserve_insertion_order=true")
    connection.execute("PRAGMA enable_progress_bar=false")
    engine = BatchEngine(symbol)
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


def compute_worker(
    batch_number: int,
    tasks: Sequence[tuple[str, str, str]],
    shard_dir: str,
    memory_limit: str,
    fetch_rows: int,
) -> tuple[int, int, int, int, int]:
    root = Path(shard_dir)
    final = root / f"batch_{batch_number:06d}"
    temporary = root / f".batch_{batch_number:06d}.{os.getpid()}.tmp"
    if temporary.exists():
        shutil.rmtree(temporary)
    temporary.mkdir(parents=True)
    signals: list[dict[str, object]] = []
    chains: list[dict[str, object]] = []
    quotes: list[dict[str, object]] = []
    quality: list[dict[str, object]] = []
    try:
        for symbol, month, path in tasks:
            file_signals, file_chains, file_quotes, file_quality = process_file(
                path, symbol, memory_limit, fetch_rows
            )
            signals.extend(file_signals)
            chains.extend(file_chains)
            quotes.extend(file_quotes)
            quality.extend(file_quality)
        atomic_write_csv(temporary / "signals.csv", SIGNAL_FIELDS, signals)
        atomic_write_csv(temporary / "active_order_chains.csv", CHAIN_FIELDS, chains)
        atomic_write_csv(temporary / "quote_lifecycles.csv", QUOTE_FIELDS, quotes)
        atomic_write_csv(temporary / "quality.csv", QUALITY_FIELDS, quality)
        write_json(temporary / "done.json", {
            "batch": batch_number,
            "tasks": [{"symbol": symbol, "month": month, "path": path} for symbol, month, path in tasks],
            "signal_rows": len(signals),
            "chain_rows": len(chains),
            "quote_rows": len(quotes),
            "quality_rows": len(quality),
            "factor_version": FACTOR_VERSION,
        })
        os.replace(temporary, final)
        return batch_number, len(tasks), len(signals), len(chains), len(quotes)
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)


def load_tasks(file_list: Path, metadata_path: Path, months: Sequence[str]):
    metadata = json.loads(metadata_path.read_text())
    if metadata.get("output_etf_symbols") != 0:
        raise ValueError("universe metadata does not certify ETF=0")
    whitelist = metadata.get("security_type_whitelist", {})
    if whitelist.get("SecuCategory") != [1] or whitelist.get("SecuMarket") != [83, 90]:
        raise ValueError("manifest is not a certified Shanghai/Shenzhen A-share universe")
    if not set(months) <= set(metadata.get("months", [])):
        raise ValueError("requested month is absent from universe metadata")
    requested = set(months)
    tasks: list[tuple[str, str, str]] = []
    for raw in file_list.read_text().splitlines():
        if not raw.strip():
            continue
        path = Path(raw.strip()).resolve()
        relative = path.relative_to(V4_ROOT)
        if len(relative.parts) != 2 or path.suffix != ".parquet":
            raise ValueError(f"unexpected V4 path: {path}")
        month, filename = relative.parts
        if month not in requested:
            continue
        symbol = Path(filename).stem
        if len(symbol) != 8 or symbol[:2] not in {"SH", "SZ"} or not symbol[2:].isdigit():
            raise ValueError(f"invalid stock symbol: {symbol}")
        tasks.append((symbol, month, str(path)))
    tasks.sort(key=lambda item: (item[1], item[0]))
    if not tasks:
        raise ValueError("no manifest-approved tasks")
    if len(tasks) != len(set((symbol, month) for symbol, month, _ in tasks)):
        raise ValueError("duplicate symbol-month task")
    return tasks, metadata


def validate_shard(path: Path) -> None:
    for name in ("signals.csv", "active_order_chains.csv", "quote_lifecycles.csv", "quality.csv", "done.json"):
        if not (path / name).is_file():
            raise ValueError(f"incomplete shard {path}: {name}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--file-list", type=Path, required=True)
    parser.add_argument("--universe-metadata", type=Path, required=True)
    parser.add_argument("--months", nargs="+", required=True)
    parser.add_argument("--shard-dir", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--batch-size", type=int, default=1)
    parser.add_argument("--fetch-rows", type=int, default=20_000)
    parser.add_argument("--memory-limit", default="1GB")
    parser.add_argument("--limit-files", type=int)
    parser.add_argument("--symbols", nargs="*")
    parser.add_argument("--exchange", choices=("ALL", "SH", "SZ"), default="ALL")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    months = sorted(set(args.months))
    if any(len(month) != 6 or not month.isdigit() for month in months):
        raise ValueError("months must use YYYYMM")
    tasks, metadata = load_tasks(args.file_list, args.universe_metadata, months)
    if args.exchange != "ALL":
        tasks = [task for task in tasks if task[0].startswith(args.exchange)]
    if args.symbols:
        selected = set(args.symbols)
        tasks = [task for task in tasks if task[0] in selected]
        missing = selected - {task[0] for task in tasks}
        if missing:
            raise ValueError(f"requested symbols missing: {sorted(missing)}")
    if args.limit_files is not None:
        tasks = tasks[:args.limit_files]
    if not tasks:
        raise ValueError("no selected tasks")
    manifest_body = {
        "factor_version": FACTOR_VERSION,
        "file_list": str(args.file_list.resolve()),
        "file_list_sha256": file_sha256(args.file_list),
        "universe_metadata": str(args.universe_metadata.resolve()),
        "universe_metadata_sha256": file_sha256(args.universe_metadata),
        "universe_rule": metadata.get("universe_rule"),
        "output_etf_symbols": metadata.get("output_etf_symbols"),
        "months": months,
        "stock_month_files": len(tasks),
        "exchange": args.exchange,
        "window": "[10:00:00,10:30:00)",
        "signal_time": "10:30:00",
        "projection": [
            "event order/type", "source order IDs/side/price/volume",
            "top1 prices/depth/count", "top3 depth",
        ],
        "excluded": ["source_link_status", "source_*_order_recid", "bid_ordvol", "ask_ordvol"],
        "primary_evaluation": "raw; no evaluation neutralization",
        "batch_size": args.batch_size,
    }
    fingerprint = hashlib.sha256(
        json.dumps(manifest_body, ensure_ascii=False, sort_keys=True).encode()
    ).hexdigest()
    manifest = {"fingerprint": fingerprint, "config": manifest_body}
    if args.dry_run:
        print(json.dumps(manifest, ensure_ascii=False, indent=2))
        return 0
    args.shard_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = args.shard_dir / "manifest.json"
    if manifest_path.exists():
        if json.loads(manifest_path.read_text()).get("fingerprint") != fingerprint:
            raise ValueError("manifest mismatch; use a new shard directory")
    else:
        write_json(manifest_path, manifest)
    batches = list(enumerate(chunks(tasks, args.batch_size), 1))
    pending = []
    resumed = 0
    for number, batch_tasks in batches:
        path = args.shard_dir / f"batch_{number:06d}"
        if path.exists():
            validate_shard(path)
            resumed += len(batch_tasks)
        else:
            pending.append((number, batch_tasks))
    print(f"resume_batches={len(batches)-len(pending)}/{len(batches)} resume_files={resumed}/{len(tasks)}", flush=True)
    completed = resumed
    if args.workers == 1:
        for index, (number, batch_tasks) in enumerate(pending, 1):
            result = compute_worker(number, batch_tasks, str(args.shard_dir), args.memory_limit, args.fetch_rows)
            completed += result[1]
            print(f"new_batches={index}/{len(pending)} files={completed}/{len(tasks)} signals={result[2]} chains={result[3]} quotes={result[4]}", flush=True)
    else:
        with ProcessPoolExecutor(max_workers=args.workers, mp_context=get_context("spawn")) as executor:
            futures = {
                executor.submit(compute_worker, number, batch_tasks, str(args.shard_dir), args.memory_limit, args.fetch_rows): number
                for number, batch_tasks in pending
            }
            for index, future in enumerate(as_completed(futures), 1):
                result = future.result()
                completed += result[1]
                print(f"new_batches={index}/{len(pending)} batch={result[0]} files={completed}/{len(tasks)} signals={result[2]} chains={result[3]} quotes={result[4]}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
