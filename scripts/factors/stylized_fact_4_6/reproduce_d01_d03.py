#!/usr/bin/env python3
"""Reproduce the first Stylized Fact 4-6 factor group from v4 LOB events.

Each input parquet is read once.  The same event stream produces daily
09:30-close and 10:00-close primitives plus the 10:00-10:30 intraday window.
Cross-sectional D01-D02 values and strictly lagged per-stock historical D03
values are finalized after all input shards finish.
"""

from __future__ import annotations

import argparse
import csv
import glob
import hashlib
import json
import math
import os
import random
import shutil
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Iterable, Sequence

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parents[3]
FACTOR_VERSION = "stylized_fact_4_6_d01_d03_safe_prebook_v1_20260807"
WINDOWS = (
    ("daily_0930_close", 93_000_000, 145_700_000, "daily"),
    ("daily_1000_close", 100_000_000, 145_700_000, "daily"),
    ("intraday_1000_1030", 100_000_000, 103_000_000, "intraday"),
)

PRIMITIVE_FIELDS = [
    "symbol",
    "date",
    "frequency",
    "window_name",
    "window_start",
    "window_end",
    "normalizer_name",
    "normalizer_price",
    "start_mid",
    "end_mid",
    "order_impact_signed",
    "cancel_impact_signed",
    "trade_impact_signed",
    "order_impact_abs",
    "cancel_impact_abs",
    "trade_impact_abs",
    "order_impact_over_normalizer",
    "cancel_impact_over_normalizer",
    "trade_impact_over_normalizer",
    "trade_buy_impact_signed",
    "trade_sell_impact_signed",
    "legacy_active_take_impact_signed",
    "total_mid_impact_signed",
    "unclassified_mid_impact_signed",
    "order_events",
    "cancel_events",
    "trade_events",
    "order_mid_move_events",
    "cancel_mid_move_events",
    "trade_mid_move_events",
    "positive_mid_move_events",
    "negative_mid_move_events",
    "window_events",
    "valid_impact_events",
    "invalid_book_events",
    "invalid_lag_events",
    "genuine_passive_add_impact_signed",
    "aggressive_remainder_impact_signed",
    "unclassified_add_impact_signed",
    "genuine_passive_add_events",
    "aggressive_remainder_events",
    "unclassified_add_events",
    "factor_version",
]

FACTOR_FIELDS = [
    "symbol",
    "date",
    "frequency",
    "window_name",
    "normalizer_name",
    "normalizer_price",
    "order_impact_over_normalizer",
    "trade_impact_over_normalizer",
    "order_impact_winsorized",
    "trade_impact_winsorized",
    "order_impact_z",
    "trade_impact_z",
    "order_impact_history_z",
    "order_impact_history_rank_pct",
    "d03_history_observations",
    "d01_trade_reversal",
    "d02_trade_momentum",
    "d03_positive_order_ts_extreme90",
    "d03_positive_order_ts_extreme95",
    "cross_section_n",
    "factor_version",
]


def chunks(items: Sequence[str], size: int) -> Iterable[Sequence[str]]:
    for index in range(0, len(items), size):
        yield items[index : index + size]


def load_stock_symbols(reference_file: str) -> set[str]:
    """Load an explicit stock-only universe from the 10:00 reference file."""
    symbols: set[str] = set()
    with open(reference_file, newline="") as handle:
        reader = csv.DictReader(handle)
        required = {"symbol", "date", "close_1000", "security_category"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"reference file missing columns: {sorted(missing)}")
        for row in reader:
            if row["security_category"] != "1":
                raise ValueError(
                    "reference file contains a non-stock security: "
                    f"{row['symbol']} category={row['security_category']}"
                )
            if float(row["close_1000"]) <= 0:
                raise ValueError(
                    f"non-positive close_1000: {row['symbol']} {row['date']}"
                )
            symbols.add(row["symbol"])
    return symbols


def expand_stock_inputs(patterns: Sequence[str], stock_symbols: set[str]) -> list[str]:
    paths: list[str] = []
    for pattern in patterns:
        paths.extend(glob.glob(pattern) or [pattern])
    unique_paths = sorted(dict.fromkeys(paths))
    return [path for path in unique_paths if Path(path).stem in stock_symbols]


def compute_batch(
    con: duckdb.DuckDBPyConnection,
    paths: Sequence[str],
    reference_file: str,
    date_from: int,
    date_to: int,
) -> list[tuple]:
    query = """
WITH refs AS (
    SELECT
        symbol,
        date::INTEGER AS date,
        close_1000::DOUBLE AS normalizer_price
    FROM read_csv_auto(?, header=true)
    WHERE security_category::INTEGER = 1
      AND close_1000::DOUBLE > 0
      AND date::INTEGER BETWEEN ? AND ?
),
base_unwindowed AS (
    SELECT
        regexp_replace(regexp_extract(filename, '[^/]+$'), '\\.parquet$', '')
            AS symbol,
        e.date::INTEGER AS date,
        e.time::BIGINT AS time,
        e.row_id::BIGINT AS row_id,
        CASE
            WHEN e.time >= 93000000 AND e.time < 113000000 THEN 'AM'
            WHEN e.time >= 130000000 AND e.time < 145700000 THEN 'PM'
        END AS session,
        e.source_action,
        e.source_side,
        CASE WHEN e.source_side = 'B' THEN e.source_buy_order_id
             WHEN e.source_side = 'S' THEN e.source_sell_order_id END AS event_order_id,
        CASE
            WHEN array_length(e.bid_px) > 0
             AND array_length(e.ask_px) > 0
             AND e.bid_px[1] IS NOT NULL
             AND e.ask_px[1] IS NOT NULL
             AND e.bid_px[1] > 0
             AND e.ask_px[1] > e.bid_px[1]
            THEN true ELSE false
        END AS valid_book,
        CASE
            WHEN array_length(e.bid_px) > 0
             AND array_length(e.ask_px) > 0
             AND e.bid_px[1] IS NOT NULL
             AND e.ask_px[1] IS NOT NULL
             AND e.bid_px[1] > 0
             AND e.ask_px[1] > e.bid_px[1]
            THEN e.bid_px[1]::DOUBLE
        END AS bid1,
        CASE
            WHEN array_length(e.bid_px) > 0
             AND array_length(e.ask_px) > 0
             AND e.bid_px[1] IS NOT NULL
             AND e.ask_px[1] IS NOT NULL
             AND e.bid_px[1] > 0
             AND e.ask_px[1] > e.bid_px[1]
            THEN e.ask_px[1]::DOUBLE
        END AS ask1,
        CASE
            WHEN array_length(e.bid_px) > 0
             AND array_length(e.ask_px) > 0
             AND e.bid_px[1] IS NOT NULL
             AND e.ask_px[1] IS NOT NULL
             AND e.bid_px[1] > 0
             AND e.ask_px[1] > e.bid_px[1]
            THEN (e.bid_px[1]::DOUBLE + e.ask_px[1]::DOUBLE) / 2.0
        END AS mid,
        r.normalizer_price
    FROM read_parquet(?, filename=true) e
    INNER JOIN refs r
      ON r.symbol = regexp_replace(
          regexp_extract(filename, '[^/]+$'), '\\.parquet$', ''
      )
     AND r.date = e.date
    WHERE e.date BETWEEN ? AND ?
      AND (
            (e.time >= 93000000 AND e.time < 113000000)
         OR (e.time >= 130000000 AND e.time < 145700000)
      )
),
base_state AS (
    SELECT
        *,
        last_value(mid IGNORE NULLS) OVER (
            PARTITION BY symbol, date, session ORDER BY time, row_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS prev_mid,
        last_value(bid1 IGNORE NULLS) OVER (
            PARTITION BY symbol, date, session ORDER BY time, row_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS prev_bid1,
        last_value(ask1 IGNORE NULLS) OVER (
            PARTITION BY symbol, date, session ORDER BY time, row_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS prev_ask1
    FROM base_unwindowed
),
first_active_trade AS (
    SELECT symbol, date, source_side, event_order_id, min(row_id) AS first_trade_row
    FROM base_unwindowed
    WHERE source_action = 'TRADE' AND source_side IN ('B', 'S')
      AND event_order_id IS NOT NULL
    GROUP BY symbol, date, source_side, event_order_id
),
base AS (
    SELECT b.*,
        CASE WHEN b.source_action <> 'ORDER_ADD' THEN NULL
             WHEN starts_with(b.symbol, 'SZ') THEN 'unclassified'
             WHEN b.source_side NOT IN ('B', 'S') OR b.event_order_id IS NULL
               THEN 'unclassified'
             WHEN t.first_trade_row < b.row_id THEN 'aggressive_remainder'
             ELSE 'genuine_passive' END AS add_class
    FROM base_state b
    LEFT JOIN first_active_trade t
      ON t.symbol = b.symbol AND t.date = b.date
     AND t.source_side = b.source_side AND t.event_order_id = b.event_order_id
),
events AS (
    SELECT
        *,
        mid - prev_mid AS delta_mid,
        abs(mid - prev_mid) AS abs_delta_mid,
        (
            source_action = 'TRADE'
            AND (
                (source_side = 'B' AND ask1 > prev_ask1 AND mid > prev_mid)
                OR
                (source_side = 'S' AND bid1 < prev_bid1 AND mid < prev_mid)
            )
        ) AS is_legacy_active_take
    FROM base
),
window_defs(window_name, window_start, window_end, frequency) AS (
    VALUES
        ('daily_0930_close', 93000000::BIGINT, 145700000::BIGINT, 'daily'),
        ('daily_1000_close', 100000000::BIGINT, 145700000::BIGINT, 'daily'),
        ('intraday_1000_1030', 100000000::BIGINT, 103000000::BIGINT, 'intraday')
),
expanded AS (
    SELECT e.*, w.window_name, w.window_start, w.window_end, w.frequency
    FROM events e
    INNER JOIN window_defs w
      ON e.time >= w.window_start
     AND e.time < w.window_end
)
SELECT
    symbol,
    date,
    frequency,
    window_name,
    window_start,
    window_end,
    'close_1000' AS normalizer_name,
    any_value(normalizer_price) AS normalizer_price,
    arg_min(mid, row_id) FILTER (WHERE mid IS NOT NULL) / 10000.0 AS start_mid,
    arg_max(mid, row_id) FILTER (WHERE mid IS NOT NULL) / 10000.0 AS end_mid,
    coalesce(sum(delta_mid) FILTER (WHERE source_action = 'ORDER_ADD'), 0.0)
        / 10000.0 AS order_impact_signed,
    coalesce(sum(delta_mid) FILTER (WHERE source_action = 'CANCEL'), 0.0)
        / 10000.0 AS cancel_impact_signed,
    coalesce(sum(delta_mid) FILTER (WHERE source_action = 'TRADE'), 0.0)
        / 10000.0 AS trade_impact_signed,
    coalesce(sum(abs_delta_mid) FILTER (WHERE source_action = 'ORDER_ADD'), 0.0)
        / 10000.0 AS order_impact_abs,
    coalesce(sum(abs_delta_mid) FILTER (WHERE source_action = 'CANCEL'), 0.0)
        / 10000.0 AS cancel_impact_abs,
    coalesce(sum(abs_delta_mid) FILTER (WHERE source_action = 'TRADE'), 0.0)
        / 10000.0 AS trade_impact_abs,
    (
        coalesce(sum(delta_mid) FILTER (WHERE source_action = 'ORDER_ADD'), 0.0)
        / 10000.0
    ) / any_value(normalizer_price) AS order_impact_over_normalizer,
    (
        coalesce(sum(delta_mid) FILTER (WHERE source_action = 'CANCEL'), 0.0)
        / 10000.0
    ) / any_value(normalizer_price) AS cancel_impact_over_normalizer,
    (
        coalesce(sum(delta_mid) FILTER (WHERE source_action = 'TRADE'), 0.0)
        / 10000.0
    ) / any_value(normalizer_price) AS trade_impact_over_normalizer,
    coalesce(sum(delta_mid) FILTER (
        WHERE source_action = 'TRADE' AND source_side = 'B'
    ), 0.0) / 10000.0 AS trade_buy_impact_signed,
    coalesce(sum(delta_mid) FILTER (
        WHERE source_action = 'TRADE' AND source_side = 'S'
    ), 0.0) / 10000.0 AS trade_sell_impact_signed,
    coalesce(sum(delta_mid) FILTER (WHERE is_legacy_active_take), 0.0)
        / 10000.0 AS legacy_active_take_impact_signed,
    coalesce(sum(delta_mid), 0.0) / 10000.0 AS total_mid_impact_signed,
    coalesce(sum(delta_mid) FILTER (
        WHERE source_action NOT IN ('ORDER_ADD', 'CANCEL', 'TRADE')
           OR source_action IS NULL
    ), 0.0) / 10000.0 AS unclassified_mid_impact_signed,
    count(*) FILTER (WHERE source_action = 'ORDER_ADD') AS order_events,
    count(*) FILTER (WHERE source_action = 'CANCEL') AS cancel_events,
    count(*) FILTER (WHERE source_action = 'TRADE') AS trade_events,
    count(*) FILTER (
        WHERE source_action = 'ORDER_ADD' AND abs_delta_mid > 0
    ) AS order_mid_move_events,
    count(*) FILTER (
        WHERE source_action = 'CANCEL' AND abs_delta_mid > 0
    ) AS cancel_mid_move_events,
    count(*) FILTER (
        WHERE source_action = 'TRADE' AND abs_delta_mid > 0
    ) AS trade_mid_move_events,
    count(*) FILTER (WHERE delta_mid > 0) AS positive_mid_move_events,
    count(*) FILTER (WHERE delta_mid < 0) AS negative_mid_move_events,
    count(*) AS window_events,
    count(delta_mid) AS valid_impact_events,
    count(*) FILTER (WHERE NOT valid_book) AS invalid_book_events,
    count(*) FILTER (WHERE delta_mid IS NULL) AS invalid_lag_events,
    coalesce(sum(delta_mid) FILTER (
        WHERE source_action = 'ORDER_ADD' AND add_class = 'genuine_passive'
    ), 0.0) / 10000.0 AS genuine_passive_add_impact_signed,
    coalesce(sum(delta_mid) FILTER (
        WHERE source_action = 'ORDER_ADD' AND add_class = 'aggressive_remainder'
    ), 0.0) / 10000.0 AS aggressive_remainder_impact_signed,
    coalesce(sum(delta_mid) FILTER (
        WHERE source_action = 'ORDER_ADD' AND add_class = 'unclassified'
    ), 0.0) / 10000.0 AS unclassified_add_impact_signed,
    count(*) FILTER (
        WHERE source_action = 'ORDER_ADD' AND add_class = 'genuine_passive'
    ) AS genuine_passive_add_events,
    count(*) FILTER (
        WHERE source_action = 'ORDER_ADD' AND add_class = 'aggressive_remainder'
    ) AS aggressive_remainder_events,
    count(*) FILTER (
        WHERE source_action = 'ORDER_ADD' AND add_class = 'unclassified'
    ) AS unclassified_add_events,
    ? AS factor_version
FROM expanded
GROUP BY symbol, date, frequency, window_name, window_start, window_end
ORDER BY symbol, date, window_start, window_end
"""
    return con.execute(
        query,
        [
            reference_file,
            date_from,
            date_to,
            list(paths),
            date_from,
            date_to,
            FACTOR_VERSION,
        ],
    ).fetchall()


def compute_worker(
    paths: Sequence[str],
    reference_file: str,
    date_from: int,
    date_to: int,
    memory_limit: str,
) -> list[tuple]:
    con = duckdb.connect()
    con.execute("PRAGMA threads=1")
    con.execute(f"PRAGMA memory_limit='{memory_limit}'")
    con.execute("PRAGMA preserve_insertion_order=false")
    try:
        return compute_batch(con, paths, reference_file, date_from, date_to)
    finally:
        con.close()



def compute_shard_worker(
    batch_number: int,
    paths: Sequence[str],
    reference_file: str,
    date_from: int,
    date_to: int,
    memory_limit: str,
    shard_path: str,
    temp_root: str,
) -> tuple[int, int, int]:
    """Compute one stable batch and atomically persist its primitive rows."""
    temp_directory = Path(temp_root) / f"batch_{batch_number:06d}_{os.getpid()}"
    temp_directory.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("PRAGMA threads=1")
    con.execute(f"PRAGMA memory_limit='{memory_limit}'")
    escaped_temp_directory = str(temp_directory).replace("'", "''")
    con.execute(f"PRAGMA temp_directory='{escaped_temp_directory}'")
    con.execute("PRAGMA preserve_insertion_order=false")
    try:
        rows = compute_batch(con, paths, reference_file, date_from, date_to)
        write_tuple_rows(shard_path, PRIMITIVE_FIELDS, rows)
        return batch_number, len(paths), len(rows)
    finally:
        con.close()
        shutil.rmtree(temp_directory, ignore_errors=True)


def quantile(values: Sequence[float], probability: float) -> float:
    ordered = sorted(values)
    if not ordered:
        raise ValueError("cannot compute a quantile of an empty sequence")
    position = probability * (len(ordered) - 1)
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def winsorized_z(values: Sequence[float]) -> tuple[list[float], list[float]]:
    lower = quantile(values, 0.01)
    upper = quantile(values, 0.99)
    clipped = [min(max(value, lower), upper) for value in values]
    mean = sum(clipped) / len(clipped)
    variance = sum((value - mean) ** 2 for value in clipped) / len(clipped)
    standard_deviation = math.sqrt(variance)
    if standard_deviation == 0:
        return clipped, [0.0] * len(clipped)
    return clipped, [(value - mean) / standard_deviation for value in clipped]


def historical_order_statistics(
    rows: Sequence[dict[str, object]],
    lookback_days: int,
    min_history: int,
) -> dict[tuple[str, int, str], tuple[float | None, float | None, int]]:
    """Compute strictly lagged per-stock order-impact z-scores and ranks."""
    series: dict[tuple[str, str], list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        series[(str(row["symbol"]), str(row["window_name"]))].append(row)

    result: dict[tuple[str, int, str], tuple[float | None, float | None, int]] = {}
    for (symbol, window_name), observations in series.items():
        observations.sort(key=lambda row: int(row["date"]))
        values: list[float] = []
        for row in observations:
            current = float(row["order_impact_over_normalizer"])
            history = values[-lookback_days:]
            history_count = len(history)
            history_z: float | None = None
            history_rank: float | None = None
            if history_count >= min_history:
                mean = sum(history) / history_count
                variance = sum((value - mean) ** 2 for value in history) / history_count
                standard_deviation = math.sqrt(variance)
                history_z = (
                    (current - mean) / standard_deviation
                    if standard_deviation > 0
                    else 0.0
                )
                less = sum(value < current for value in history)
                equal = sum(value == current for value in history)
                history_rank = (less + 0.5 * equal) / history_count
            result[(symbol, int(row["date"]), window_name)] = (
                history_z,
                history_rank,
                history_count,
            )
            values.append(current)
    return result


def finalize_factors(
    primitive_rows: Sequence[tuple],
    d03_history_days: int,
    d03_min_history: int,
) -> list[dict[str, object]]:
    dict_rows = [dict(zip(PRIMITIVE_FIELDS, row, strict=True)) for row in primitive_rows]
    history_statistics = historical_order_statistics(
        dict_rows,
        lookback_days=d03_history_days,
        min_history=d03_min_history,
    )
    groups: dict[tuple[int, str], list[dict[str, object]]] = defaultdict(list)
    for row in dict_rows:
        groups[(int(row["date"]), str(row["window_name"]))].append(row)

    factors: list[dict[str, object]] = []
    for rows in groups.values():
        rows.sort(key=lambda row: str(row["symbol"]))
        order_values = [float(row["order_impact_over_normalizer"]) for row in rows]
        trade_values = [float(row["trade_impact_over_normalizer"]) for row in rows]
        order_clipped, order_z = winsorized_z(order_values)
        trade_clipped, trade_z = winsorized_z(trade_values)
        cross_section_n = len(rows)
        for index, row in enumerate(rows):
            history_z, history_rank, history_count = history_statistics[
                (str(row["symbol"]), int(row["date"]), str(row["window_name"]))
            ]
            positive_order_impact = order_values[index] > 0
            factors.append(
                {
                    "symbol": row["symbol"],
                    "date": row["date"],
                    "frequency": row["frequency"],
                    "window_name": row["window_name"],
                    "normalizer_name": row["normalizer_name"],
                    "normalizer_price": row["normalizer_price"],
                    "order_impact_over_normalizer": order_values[index],
                    "trade_impact_over_normalizer": trade_values[index],
                    "order_impact_winsorized": order_clipped[index],
                    "trade_impact_winsorized": trade_clipped[index],
                    "order_impact_z": order_z[index],
                    "trade_impact_z": trade_z[index],
                    "order_impact_history_z": history_z,
                    "order_impact_history_rank_pct": history_rank,
                    "d03_history_observations": history_count,
                    "d01_trade_reversal": -trade_z[index],
                    "d02_trade_momentum": trade_z[index],
                    "d03_positive_order_ts_extreme90": (
                        -history_z
                        if positive_order_impact
                        and history_rank is not None
                        and history_rank > 0.90
                        and history_z is not None
                        else 0.0
                    ),
                    "d03_positive_order_ts_extreme95": (
                        -history_z
                        if positive_order_impact
                        and history_rank is not None
                        and history_rank > 0.95
                        and history_z is not None
                        else 0.0
                    ),
                    "cross_section_n": cross_section_n,
                    "factor_version": FACTOR_VERSION,
                }
            )
    factors.sort(key=lambda row: (row["date"], row["window_name"], row["symbol"]))
    return factors


def write_tuple_rows(path: str, fields: Sequence[str], rows: Sequence[tuple]) -> None:
    """Atomically write tuple rows so an existing shard is always complete."""
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(f".{output.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("w", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(fields)
            writer.writerows(rows)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, output)
    finally:
        if temporary.exists():
            temporary.unlink()


def write_dict_rows(
    path: str, fields: Sequence[str], rows: Sequence[dict[str, object]]
) -> None:
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(f".{output.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, output)
    finally:
        if temporary.exists():
            temporary.unlink()


def read_tuple_rows(path: Path, expected_fields: Sequence[str]) -> list[tuple]:
    with path.open(newline="") as handle:
        reader = csv.reader(handle)
        header = next(reader, None)
        if header != list(expected_fields):
            raise ValueError(f"invalid or incompatible shard header: {path}")
        return [tuple(row) for row in reader]


def validate_shard(path: Path) -> None:
    with path.open(newline="") as handle:
        header = next(csv.reader(handle), None)
    if header != PRIMITIVE_FIELDS:
        raise ValueError(f"invalid or incompatible shard header: {path}")


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def build_run_manifest(
    paths: Sequence[str],
    reference_file: str,
    date_from: int,
    date_to: int,
    batch_size: int,
) -> dict[str, object]:
    reference_path = Path(reference_file).resolve()
    config: dict[str, object] = {
        "factor_version": FACTOR_VERSION,
        "date_from": date_from,
        "date_to": date_to,
        "batch_size": batch_size,
        "windows": WINDOWS,
        "reference_file": str(reference_path),
        "reference_sha256": file_sha256(reference_path),
        "inputs": [str(Path(item).resolve()) for item in paths],
    }
    encoded = json.dumps(config, ensure_ascii=False, sort_keys=True).encode()
    return {"fingerprint": hashlib.sha256(encoded).hexdigest(), "config": config}


def prepare_run_manifest(shard_dir: Path, manifest: dict[str, object]) -> None:
    shard_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = shard_dir / "manifest.json"
    if manifest_path.exists():
        existing = json.loads(manifest_path.read_text())
        if existing.get("fingerprint") != manifest["fingerprint"]:
            raise ValueError(
                f"shard manifest mismatch: {manifest_path}; use a new shard directory"
            )
        return
    existing_shards = sorted(shard_dir.glob("batch_*.csv"))
    if existing_shards:
        raise ValueError(
            f"shards exist without a manifest in {shard_dir}; use a new directory"
        )
    temporary = shard_dir / f".manifest.{os.getpid()}.tmp"
    temporary.write_text(json.dumps(manifest, ensure_ascii=False, indent=2))
    os.replace(temporary, manifest_path)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Compute v4 ORDER_ADD/CANCEL/TRADE mid-price shocks and finalize "
            "D01-D03 for daily and 10:00-10:30 windows."
        )
    )
    parser.add_argument(
        "inputs",
        nargs="*",
        default=["/hdd_data/lob/event_depth10_v4/202601/*.parquet"],
    )
    parser.add_argument(
        "--reference-file",
        default=str(PROJECT_ROOT / "data/cache/min1_close_1000_stock_202601.csv"),
        help="Stock-only CSV containing symbol,date,close_1000,security_category.",
    )
    parser.add_argument("--exchange", choices=("ALL", "SH", "SZ"), default="ALL")
    parser.add_argument("--date-from", type=int, default=20260105)
    parser.add_argument("--date-to", type=int, default=20260107)
    parser.add_argument("--batch-size", type=int, default=4)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--limit-files", type=int)
    parser.add_argument("--sample-files", type=int)
    parser.add_argument("--seed", type=int, default=20260801)
    parser.add_argument(
        "--memory-limit",
        default="2GB",
        help="DuckDB memory limit applied independently to each worker.",
    )
    parser.add_argument("--d03-history-days", type=int, default=60)
    parser.add_argument("--d03-min-history", type=int, default=20)
    parser.add_argument(
        "--shard-dir",
        default=str(
            PROJECT_ROOT
            / "data/cache/stylized_fact_4_6/g1_d01_d03_shards"
        ),
        help="Stable batch shard directory used for automatic resume.",
    )
    parser.add_argument(
        "--temp-root",
        default="/tmp/stylized_fact_4_6_d01_d03",
        help="DuckDB spill directory root; each worker gets a private child.",
    )
    parser.add_argument(
        "--primitive-output",
        default=str(
            PROJECT_ROOT
            / "data/cache/stylized_fact_4_6/g1_d01_d03_primitives_sample.csv"
        ),
    )
    parser.add_argument(
        "--factor-output",
        default=str(
            PROJECT_ROOT
            / "data/processed/stylized_fact_4_6/g1_d01_d03_factors_sample.csv"
        ),
    )
    args = parser.parse_args()

    if args.date_from > args.date_to:
        raise ValueError("date-from must not be after date-to")
    if args.batch_size <= 0:
        raise ValueError("batch-size must be positive")
    if args.workers <= 0:
        raise ValueError("workers must be positive")
    if args.d03_history_days <= 0:
        raise ValueError("d03-history-days must be positive")
    if not 1 <= args.d03_min_history <= args.d03_history_days:
        raise ValueError("d03-min-history must be in [1, d03-history-days]")
    if args.limit_files is not None and args.limit_files <= 0:
        raise ValueError("limit-files must be positive")
    if args.sample_files is not None and args.sample_files <= 0:
        raise ValueError("sample-files must be positive")

    stock_symbols = load_stock_symbols(args.reference_file)
    paths = expand_stock_inputs(args.inputs, stock_symbols)
    if args.exchange != "ALL":
        paths = [path for path in paths if Path(path).stem.startswith(args.exchange)]
    if args.sample_files is not None and args.sample_files < len(paths):
        paths = sorted(random.Random(args.seed).sample(paths, args.sample_files))
    if args.limit_files is not None:
        paths = paths[: args.limit_files]
    if not paths:
        raise SystemExit("no v4 stock parquet inputs matched the explicit universe")

    batches = list(enumerate(chunks(paths, args.batch_size), start=1))
    shard_dir = Path(args.shard_dir)
    manifest = build_run_manifest(
        paths,
        reference_file=args.reference_file,
        date_from=args.date_from,
        date_to=args.date_to,
        batch_size=args.batch_size,
    )
    prepare_run_manifest(shard_dir, manifest)

    pending_batches: list[tuple[int, Sequence[str], Path]] = []
    resumed_files = 0
    for batch_number, batch in batches:
        shard_path = shard_dir / f"batch_{batch_number:06d}.csv"
        if shard_path.exists():
            validate_shard(shard_path)
            resumed_files += len(batch)
        else:
            pending_batches.append((batch_number, batch, shard_path))
    print(
        f"resume_batches={len(batches) - len(pending_batches)}/{len(batches)} "
        f"resume_files={resumed_files}/{len(paths)}",
        flush=True,
    )

    completed_files = resumed_files
    if args.workers == 1:
        for completed_batches, (batch_number, batch, shard_path) in enumerate(
            pending_batches, start=1
        ):
            _, batch_files, row_count = compute_shard_worker(
                batch_number,
                batch,
                args.reference_file,
                args.date_from,
                args.date_to,
                args.memory_limit,
                str(shard_path),
                args.temp_root,
            )
            completed_files += batch_files
            print(
                f"new_batches={completed_batches}/{len(pending_batches)} "
                f"total_files={completed_files}/{len(paths)} "
                f"batch_rows={row_count}",
                flush=True,
            )
    else:
        with ProcessPoolExecutor(max_workers=args.workers) as executor:
            futures = {
                executor.submit(
                    compute_shard_worker,
                    batch_number,
                    batch,
                    args.reference_file,
                    args.date_from,
                    args.date_to,
                    args.memory_limit,
                    str(shard_path),
                    args.temp_root,
                ): batch_number
                for batch_number, batch, shard_path in pending_batches
            }
            for completed_batches, future in enumerate(
                as_completed(futures), start=1
            ):
                _, batch_files, row_count = future.result()
                completed_files += batch_files
                print(
                    f"new_batches={completed_batches}/{len(pending_batches)} "
                    f"total_files={completed_files}/{len(paths)} "
                    f"batch_rows={row_count}",
                    flush=True,
                )

    primitive_rows: list[tuple] = []
    for batch_number, _batch in batches:
        shard_path = shard_dir / f"batch_{batch_number:06d}.csv"
        if not shard_path.exists():
            raise RuntimeError(f"missing completed shard: {shard_path}")
        primitive_rows.extend(read_tuple_rows(shard_path, PRIMITIVE_FIELDS))
    primitive_rows.sort(key=lambda row: (int(row[1]), row[3], row[0]))
    factor_rows = finalize_factors(
        primitive_rows,
        d03_history_days=args.d03_history_days,
        d03_min_history=args.d03_min_history,
    )
    write_tuple_rows(args.primitive_output, PRIMITIVE_FIELDS, primitive_rows)
    write_dict_rows(args.factor_output, FACTOR_FIELDS, factor_rows)
    print(
        f"files={len(paths)} workers={args.workers} "
        f"dates={args.date_from}:{args.date_to} "
        f"primitive_rows={len(primitive_rows)} factor_rows={len(factor_rows)} "
        f"primitive_output={args.primitive_output} factor_output={args.factor_output}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
