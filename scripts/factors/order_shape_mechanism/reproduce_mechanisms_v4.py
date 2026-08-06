#!/usr/bin/env python3
"""Validate order-shape mechanisms with one projected read per V4 file."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import shutil
import sys
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict
from multiprocessing import get_context
from pathlib import Path
from typing import Iterable, Iterator, Sequence

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.factors.order_shape_mechanism.engine import (
    FACTOR_VERSION,
    DayQuality,
    Event,
    MechanismConfig,
    MechanismEngine,
)


V4_ROOT = Path("/hdd_data/lob/event_depth10_v4")
STAT_FIELDS = [
    "symbol", "date", "mechanism", "variant", "group_key",
    "observations", "value_sum", "value_sq_sum", "weight_sum", "factor_version",
]
QUALITY_FIELDS = ["symbol", "date", *DayQuality.__dataclass_fields__, "factor_version"]

# Warmup deliberately omits order IDs and all volume/queue arrays.
WARMUP_QUERY = """
SELECT date::INTEGER, time::BIGINT, row_id::BIGINT, source_action,
       source_recid::BIGINT, NULL::BIGINT, NULL::BIGINT, source_side,
       NULL::BIGINT, source_volume::BIGINT,
       CASE WHEN array_length(bid_px)>0 THEN bid_px[1] END::BIGINT,
       CASE WHEN array_length(ask_px)>0 THEN ask_px[1] END::BIGINT,
       NULL::BIGINT, NULL::BIGINT, NULL::BIGINT,
       NULL::BIGINT, NULL::BIGINT, NULL::BIGINT
FROM read_parquet(?)
WHERE (time>=93000000 AND time<113000000)
   OR (time>=130000000 AND time<145700000)
"""

# Target omits the especially large ordvol arrays, counts, and postprocessed link status.
TARGET_QUERY = """
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
WHERE (time>=93000000 AND time<113000000)
   OR (time>=130000000 AND time<145700000)
"""


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def validate_month(value: str) -> str:
    if len(value) != 6 or not value.isdigit():
        raise ValueError(f"invalid YYYYMM month: {value}")
    return value


def load_inputs(
    file_list: Path,
    metadata_path: Path,
    warmup_months: Sequence[str],
    target_month: str,
) -> tuple[dict[str, dict[str, str]], dict[str, object]]:
    metadata = json.loads(metadata_path.read_text())
    if metadata.get("output_etf_symbols") != 0:
        raise ValueError("universe metadata does not certify zero ETF symbols")
    whitelist = metadata.get("security_type_whitelist", {})
    if whitelist.get("SecuCategory") != [1] or whitelist.get("SecuMarket") != [83, 90]:
        raise ValueError("manifest is not certified as Shanghai/Shenzhen A shares")
    requested = set(warmup_months) | {target_month}
    registered = set(metadata.get("months", []))
    if not requested <= registered:
        raise ValueError(f"manifest months missing: {sorted(requested - registered)}")

    by_symbol: dict[str, dict[str, str]] = defaultdict(dict)
    for line in file_list.read_text().splitlines():
        raw_path = line.strip()
        if not raw_path:
            continue
        path = Path(raw_path).resolve()
        try:
            relative = path.relative_to(V4_ROOT)
        except ValueError as exc:
            raise ValueError(f"input is outside V4 root: {path}") from exc
        if len(relative.parts) != 2 or path.suffix != ".parquet":
            raise ValueError(f"unexpected V4 path layout: {path}")
        month, filename = relative.parts
        if month not in requested:
            continue
        symbol = Path(filename).stem
        if len(symbol) != 8 or symbol[:2] not in {"SH", "SZ"} or not symbol[2:].isdigit():
            raise ValueError(f"invalid stock symbol in manifest: {symbol}")
        if month in by_symbol[symbol]:
            raise ValueError(f"duplicate symbol-month path: {symbol} {month}")
        by_symbol[symbol][month] = str(path)
    selected = {
        symbol: months for symbol, months in by_symbol.items() if target_month in months
    }
    if not selected:
        raise ValueError("no target-month A-share V4 inputs")
    return dict(sorted(selected.items())), metadata


def chunks(items: Sequence[str], size: int) -> Iterable[Sequence[str]]:
    for index in range(0, len(items), size):
        yield items[index:index + size]


def row_to_event(row: Sequence[object]) -> Event:
    integer = lambda value: int(value) if value is not None else None
    return Event(
        date=int(row[0]), time=int(row[1]), row_id=int(row[2]), action=str(row[3]),
        recid=integer(row[4]), buy_order_id=integer(row[5]),
        sell_order_id=integer(row[6]), side=str(row[7]) if row[7] is not None else None,
        price=integer(row[8]), volume=integer(row[9]), bid1=integer(row[10]),
        ask1=integer(row[11]),
        bid_depths=tuple(integer(value) for value in row[12:15]),
        ask_depths=tuple(integer(value) for value in row[15:18]),
    )


def stream_events(
    connection: duckdb.DuckDBPyConnection, path: str, phase: str, fetch_rows: int
) -> Iterator[Event]:
    cursor = connection.execute(WARMUP_QUERY if phase == "warmup" else TARGET_QUERY, [path])
    while True:
        rows = cursor.fetchmany(fetch_rows)
        if not rows:
            return
        for row in rows:
            yield row_to_event(row)


def process_symbol(
    symbol: str,
    month_paths: dict[str, str],
    warmup_months: Sequence[str],
    target_month: str,
    config: MechanismConfig,
    memory_limit: str,
    fetch_rows: int,
) -> tuple[list[dict[str, object]], list[dict[str, object]], list[dict[str, object]], dict[str, object]]:
    connection = duckdb.connect()
    connection.execute("PRAGMA threads=1")
    connection.execute(f"PRAGMA memory_limit='{memory_limit}'")
    connection.execute("PRAGMA preserve_insertion_order=true")
    engine = MechanismEngine(symbol, config)
    try:
        for month in warmup_months:
            path = month_paths.get(month)
            if path:
                for event in stream_events(connection, path, "warmup", fetch_rows):
                    engine.process(event, "warmup")
        for event in stream_events(connection, month_paths[target_month], "target", fetch_rows):
            engine.process(event, "target")
        stats, quality, audit = engine.finish()
        return stats, quality, audit, engine.profile_summary()
    finally:
        connection.close()


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


def atomic_write_jsonl(path: Path, rows: Sequence[dict[str, object]]) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("w") as handle:
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def validate_batch_dir(path: Path) -> None:
    required = {"stats.csv", "quality.csv", "profiles.jsonl", "done.json"}
    missing = [name for name in required if not (path / name).is_file()]
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
    warmup_months: Sequence[str],
    target_month: str,
    config: MechanismConfig,
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
    audits: list[dict[str, object]] = []
    profiles: list[dict[str, object]] = []
    try:
        for symbol in symbols:
            symbol_stats, symbol_quality, symbol_audit, profile = process_symbol(
                symbol, inputs[symbol], warmup_months, target_month, config,
                memory_limit, fetch_rows,
            )
            stats.extend(symbol_stats)
            quality.extend(symbol_quality)
            audits.extend(symbol_audit)
            profiles.append(profile)
        atomic_write_csv(temporary / "stats.csv", STAT_FIELDS, stats)
        atomic_write_csv(temporary / "quality.csv", QUALITY_FIELDS, quality)
        atomic_write_jsonl(temporary / "profiles.jsonl", profiles)
        if audits:
            atomic_write_jsonl(temporary / "audit.jsonl", audits)
        done = {
            "batch": batch_number, "symbols": list(symbols), "stat_rows": len(stats),
            "quality_rows": len(quality), "factor_version": FACTOR_VERSION,
        }
        (temporary / "done.json").write_text(json.dumps(done, ensure_ascii=False, indent=2) + "\n")
        os.replace(temporary, final_dir)
        return batch_number, len(symbols), len(stats), len(quality)
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)


def build_run_manifest(
    file_list: Path,
    metadata_path: Path,
    metadata: dict[str, object],
    inputs: dict[str, dict[str, str]],
    warmup_months: Sequence[str],
    target_month: str,
    config: MechanismConfig,
    batch_size: int,
) -> dict[str, object]:
    month_counts: dict[str, int] = defaultdict(int)
    for months in inputs.values():
        for month in months:
            month_counts[month] += 1
    body = {
        "factor_version": FACTOR_VERSION,
        "file_list": str(file_list.resolve()),
        "file_list_sha256": file_sha256(file_list),
        "universe_metadata": str(metadata_path.resolve()),
        "universe_metadata_sha256": file_sha256(metadata_path),
        "universe_rule": metadata.get("universe_rule"),
        "output_etf_symbols": metadata.get("output_etf_symbols"),
        "warmup_months": list(warmup_months), "target_month": target_month,
        "symbols": len(inputs), "month_file_counts": dict(sorted(month_counts.items())),
        "batch_size": batch_size,
        "warmup_projection": [
            "date", "time", "row_id", "source_action", "source_recid",
            "source_side", "source_volume", "bid_px", "ask_px",
        ],
        "target_projection": [
            "date", "time", "row_id", "source_action", "source_recid",
            "source_buy_order_id", "source_sell_order_id", "source_side",
            "source_price", "source_volume", "bid_px", "ask_px", "bid_vol", "ask_vol",
        ],
        "excluded_columns": [
            "bid_ordvol", "ask_ordvol", "bid_cnt", "ask_cnt", "source_link_status",
        ],
        "config": asdict(config),
    }
    encoded = json.dumps(body, ensure_ascii=False, sort_keys=True, default=list).encode()
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
    temporary = path / f".manifest.{os.getpid()}.tmp"
    temporary.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, default=list) + "\n")
    os.replace(temporary, manifest_path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Validate M1-M6 order-shape mechanisms from V4 with one projected "
            "read per stock-month and stock-day sufficient-stat outputs."
        )
    )
    parser.add_argument("--file-list", type=Path, required=True)
    parser.add_argument("--universe-metadata", type=Path, required=True)
    parser.add_argument("--warmup-months", nargs="+", required=True)
    parser.add_argument("--target-month", required=True)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--batch-size", type=int, default=8)
    parser.add_argument("--fetch-rows", type=int, default=10_000)
    parser.add_argument("--memory-limit", default="1GB")
    parser.add_argument(
        "--shard-dir", type=Path,
        default=PROJECT_ROOT / "data/cache/order_shape_mechanism/shards",
    )
    parser.add_argument("--audit-symbols", nargs="*")
    parser.add_argument("--audit-dates", nargs="*", type=int)
    parser.add_argument("--audit-max-events", type=int, default=200)
    parser.add_argument("--audit-max-orders", type=int, default=60)
    parser.add_argument("--limit-symbols", type=int)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    warmup_months = sorted(dict.fromkeys(validate_month(value) for value in args.warmup_months))
    target_month = validate_month(args.target_month)
    if target_month in warmup_months or any(month >= target_month for month in warmup_months):
        raise ValueError("warmup months must be strictly earlier than target month")
    if args.workers <= 0 or args.batch_size <= 0 or args.fetch_rows <= 0:
        raise ValueError("workers, batch-size, and fetch-rows must be positive")
    inputs, metadata = load_inputs(
        args.file_list, args.universe_metadata, warmup_months, target_month,
    )
    if args.audit_symbols:
        requested = set(args.audit_symbols)
        missing = requested - set(inputs)
        if missing:
            raise ValueError(f"audit symbols missing from target month: {sorted(missing)}")
        inputs = {symbol: inputs[symbol] for symbol in sorted(requested)}
    if args.limit_symbols is not None:
        if args.limit_symbols <= 0:
            raise ValueError("limit-symbols must be positive")
        inputs = dict(list(inputs.items())[:args.limit_symbols])
    config = MechanismConfig(
        target_month=target_month,
        audit_dates=frozenset(args.audit_dates or []),
        audit_max_events=args.audit_max_events,
        audit_max_orders=args.audit_max_orders,
    )
    manifest = build_run_manifest(
        args.file_list, args.universe_metadata, metadata, inputs, warmup_months,
        target_month, config, args.batch_size,
    )
    if args.dry_run:
        print(json.dumps(manifest, ensure_ascii=False, indent=2, default=list))
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
        f"resume_symbols={resumed_symbols}/{len(symbols)}", flush=True,
    )
    completed_symbols = resumed_symbols
    if args.workers == 1:
        for completed, (batch_number, batch_symbols) in enumerate(pending, start=1):
            _, count, stat_rows, quality_rows = compute_batch_worker(
                batch_number, batch_symbols, inputs, warmup_months, target_month,
                config, args.memory_limit, args.fetch_rows, str(args.shard_dir),
            )
            completed_symbols += count
            print(
                f"new_batches={completed}/{len(pending)} symbols={completed_symbols}/{len(symbols)} "
                f"stat_rows={stat_rows} quality_rows={quality_rows}", flush=True,
            )
    else:
        with ProcessPoolExecutor(max_workers=args.workers, mp_context=get_context("spawn")) as executor:
            futures = {
                executor.submit(
                    compute_batch_worker, batch_number, batch_symbols, inputs,
                    warmup_months, target_month, config, args.memory_limit,
                    args.fetch_rows, str(args.shard_dir),
                ): batch_number
                for batch_number, batch_symbols in pending
            }
            for completed, future in enumerate(as_completed(futures), start=1):
                _batch, count, stat_rows, quality_rows = future.result()
                completed_symbols += count
                print(
                    f"new_batches={completed}/{len(pending)} symbols={completed_symbols}/{len(symbols)} "
                    f"stat_rows={stat_rows} quality_rows={quality_rows}", flush=True,
                )
    print(f"complete symbols={len(symbols)} batches={len(batches)} shard_dir={args.shard_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
