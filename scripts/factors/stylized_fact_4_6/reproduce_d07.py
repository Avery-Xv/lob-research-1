#!/usr/bin/env python3
"""Compute D07 retained active-large-order impact in event and wall-clock time.

The daily close factor may use the completed same-day order lifecycle to recover
Shanghai aggressive-order size.  Retained prices never cross the continuous
auction session containing the triggering execution episode.
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
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Iterable, Sequence

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parents[3]
FACTOR_VERSION = "stylized_fact_4_6_d07_sh_safe_prebook_v1_20260807"
WINDOWS = (
    ("daily_0930_close", 93_000_000, 145_700_000),
    ("daily_1000_close", 100_000_000, 145_700_000),
    ("intraday_1000_1030", 100_000_000, 103_000_000),
)
EVENT_HORIZONS = (1, 5, 10, 20, 50)
TIME_HORIZONS_MS = (5_000, 30_000, 60_000, 300_000)
THRESHOLD_VERSIONS = ("mean_x05", "fixed_notional")

FIELDS = [
    "symbol", "date", "frequency", "window_name", "threshold_version",
    "clock_type", "horizon", "horizon_unit",
    "episode_count", "buy_episode_count", "sell_episode_count",
    "nonzero_immediate_count", "valid_retained_count", "missing_retained_count",
    "immediate_impact_sum", "immediate_impact_abs_sum",
    "retained_impact_sum", "retained_impact_abs_sum",
    "d07_directional", "d07_permanent_ratio",
    "d07_buy_permanent_ratio", "d07_sell_permanent_ratio",
    "mean_elapsed_ms", "median_elapsed_ms",
    "threshold_mean_qty", "fixed_notional",
    "window_event_count", "valid_book_event_count", "invalid_book_event_count",
    "trade_event_count", "missing_aggressor_id_count",
    "full_link_trade_count", "partial_link_trade_count",
    "is_valid", "invalid_reason", "factor_version",
]


def chunks(items: Sequence[str], size: int) -> Iterable[Sequence[str]]:
    for index in range(0, len(items), size):
        yield items[index : index + size]


def finite(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def event_milliseconds_sql(column: str = "time") -> str:
    """Return DuckDB SQL converting HHMMSSmmm integer time to milliseconds."""
    return (
        f"((((({column} // 10000000) * 60) "
        f"+ (({column} // 100000) % 100)) * 60 "
        f"+ (({column} // 1000) % 100)) * 1000 + ({column} % 1000))"
    )


def load_mean_thresholds(
    path: str, date_from: int, date_to: int
) -> dict[tuple[str, int], float]:
    """Load one D04 mean order-size threshold per stock-day."""
    output: dict[tuple[str, int], float] = {}
    with open(path, newline="") as handle:
        reader = csv.DictReader(handle)
        required = {"symbol", "date", "threshold_mean_qty"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"threshold source missing columns: {sorted(missing)}")
        for row in reader:
            date = int(row["date"])
            if not date_from <= date <= date_to:
                continue
            value = finite(row["threshold_mean_qty"])
            if value is None or value <= 0:
                continue
            key = (row["symbol"], date)
            previous = output.get(key)
            if previous is not None and not math.isclose(previous, value):
                raise ValueError(f"inconsistent threshold for {key}: {previous} vs {value}")
            output[key] = value
    return output


def expand_inputs(
    patterns: Sequence[str], file_list: str | None, date_from: int, date_to: int
) -> dict[str, list[str]]:
    paths: list[str] = []
    if file_list:
        with open(file_list) as handle:
            paths.extend(line.strip() for line in handle if line.strip())
    for pattern in patterns:
        paths.extend(glob.glob(pattern) or [pattern])
    month_from, month_to = date_from // 100, date_to // 100
    grouped: dict[str, list[str]] = {}
    for path in sorted(dict.fromkeys(paths)):
        candidate = Path(path)
        try:
            month = int(candidate.parent.name)
        except ValueError:
            continue
        if not month_from <= month <= month_to:
            continue
        symbol = candidate.stem
        if not symbol.startswith(("SH", "SZ")):
            continue
        grouped.setdefault(symbol, []).append(str(candidate))
    return grouped


def compute_batch(
    con: duckdb.DuckDBPyConnection,
    paths: Sequence[str],
    threshold_rows: Sequence[tuple[str, int, float]],
    date_from: int,
    date_to: int,
    fixed_notional: float,
    ratio_clip: float,
) -> list[tuple]:
    con.execute("CREATE TEMP TABLE thresholds(symbol VARCHAR,date INTEGER,mean_qty DOUBLE)")
    if threshold_rows:
        con.executemany("INSERT INTO thresholds VALUES (?,?,?)", threshold_rows)
    event_ms = event_milliseconds_sql("e.time")
    window_values = ",".join(
        f"('{name}',{start}::BIGINT,{end}::BIGINT)" for name, start, end in WINDOWS
    )
    event_values = ",".join(f"({value})" for value in EVENT_HORIZONS)
    time_values = ",".join(f"({value})" for value in TIME_HORIZONS_MS)
    query = f"""
WITH raw0 AS MATERIALIZED (
    SELECT regexp_replace(regexp_extract(filename, '[^/]+$'), '\\.parquet$', '') AS symbol,
           e.date::INTEGER AS date,e.time::BIGINT AS time,e.row_id::BIGINT AS row_id,
           CASE WHEN e.time>=93000000 AND e.time<113000000 THEN 'AM'
                WHEN e.time>=130000000 AND e.time<145700000 THEN 'PM' END AS session,
           {event_ms}::BIGINT AS event_ms,
           e.source_action,e.source_side,e.source_buy_order_id,e.source_sell_order_id,
           e.source_price::DOUBLE AS source_price,e.source_volume::DOUBLE AS source_volume,
           e.source_link_status,
           CASE WHEN e.source_side='B' THEN e.source_buy_order_id
                WHEN e.source_side='S' THEN e.source_sell_order_id END AS event_order_id,
           CASE WHEN array_length(e.bid_px)>0 AND array_length(e.ask_px)>0
                  AND e.bid_px[1] IS NOT NULL AND e.ask_px[1] IS NOT NULL
                  AND e.bid_px[1]>0 AND e.ask_px[1]>e.bid_px[1]
                THEN (e.bid_px[1]::DOUBLE+e.ask_px[1]::DOUBLE)/20000.0 END AS mid
    FROM read_parquet(?,filename=true) e
    WHERE e.date BETWEEN ? AND ?
      AND ((e.time>=93000000 AND e.time<113000000)
        OR (e.time>=130000000 AND e.time<145700000))
),
raw AS MATERIALIZED (
    SELECT *,last_value(mid IGNORE NULLS) OVER (
               PARTITION BY symbol,date,session ORDER BY row_id
               ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
           ) AS prev_mid,
           lag(source_action) OVER (PARTITION BY symbol,date,session ORDER BY row_id) AS prev_action,
           lag(source_side) OVER (PARTITION BY symbol,date,session ORDER BY row_id) AS prev_side,
           lag(event_order_id) OVER (PARTITION BY symbol,date,session ORDER BY row_id) AS prev_order_id
    FROM raw0
),
adds AS MATERIALIZED (
    SELECT symbol,date,source_side AS side,event_order_id AS order_id,
           arg_min(source_volume,row_id) AS add_qty,min(row_id) AS first_add_row
    FROM raw
    WHERE source_action='ORDER_ADD' AND source_side IN ('B','S')
      AND event_order_id IS NOT NULL AND source_volume>0
    GROUP BY symbol,date,side,order_id
),
active_trades AS MATERIALIZED (
    SELECT symbol,date,source_side AS side,event_order_id AS order_id,
           sum(source_volume) AS exec_qty_day,min(row_id) AS first_trade_row
    FROM raw
    WHERE source_action='TRADE' AND source_side IN ('B','S')
      AND event_order_id IS NOT NULL AND source_volume>0
    GROUP BY symbol,date,side,order_id
),
pre_add_exec AS MATERIALIZED (
    SELECT t.symbol,t.date,t.source_side AS side,t.event_order_id AS order_id,
           sum(t.source_volume) AS pre_add_exec_qty
    FROM raw t JOIN adds a ON a.symbol=t.symbol AND a.date=t.date
      AND a.side=t.source_side AND a.order_id=t.event_order_id
      AND t.row_id<a.first_add_row
    WHERE t.source_action='TRADE' AND t.source_volume>0
    GROUP BY t.symbol,t.date,t.source_side,t.event_order_id
),
orders AS MATERIALIZED (
    SELECT coalesce(a.symbol,t.symbol) AS symbol,coalesce(a.date,t.date) AS date,
           coalesce(a.side,t.side) AS side,coalesce(a.order_id,t.order_id) AS order_id,
           CASE WHEN t.order_id IS NULL THEN a.add_qty
                WHEN a.order_id IS NULL THEN t.exec_qty_day
                WHEN a.first_add_row<t.first_trade_row THEN a.add_qty
                ELSE coalesce(p.pre_add_exec_qty,0)+a.add_qty END AS original_qty
    FROM adds a FULL OUTER JOIN active_trades t
      ON a.symbol=t.symbol AND a.date=t.date AND a.side=t.side AND a.order_id=t.order_id
    LEFT JOIN pre_add_exec p
      ON p.symbol=coalesce(a.symbol,t.symbol) AND p.date=coalesce(a.date,t.date)
     AND p.side=coalesce(a.side,t.side) AND p.order_id=coalesce(a.order_id,t.order_id)
),
order_dates AS MATERIALIZED (
    SELECT symbol,date,row_number() OVER (PARTITION BY symbol ORDER BY date) AS day_seq
    FROM (SELECT DISTINCT symbol,date FROM orders)
),
rolling_thresholds AS MATERIALIZED (
    SELECT c.symbol,c.date,avg(o.original_qty) AS mean_qty
    FROM order_dates c
    JOIN order_dates h ON h.symbol=c.symbol
      AND h.day_seq BETWEEN c.day_seq-20 AND c.day_seq-1
    JOIN orders o ON o.symbol=h.symbol AND o.date=h.date
    GROUP BY c.symbol,c.date
),
marked AS MATERIALIZED (
    SELECT *,CASE WHEN source_action='TRADE' AND source_side IN ('B','S')
                         AND event_order_id IS NOT NULL AND source_volume>0
                         AND NOT (prev_action='TRADE' AND prev_side=source_side
                                  AND prev_order_id=event_order_id)
                    THEN 1 ELSE 0 END AS episode_start
    FROM raw
),
trade_groups AS MATERIALIZED (
    SELECT *,sum(episode_start) OVER (
        PARTITION BY symbol,date,session ORDER BY row_id ROWS UNBOUNDED PRECEDING
    ) AS episode_group
    FROM marked
),
episodes AS MATERIALIZED (
    SELECT g.symbol,g.date,g.session,g.source_side AS side,g.event_order_id AS order_id,
           g.episode_group,min(g.row_id) AS start_row_id,max(g.row_id) AS end_row_id,
           arg_min(g.time,g.row_id) AS start_time,arg_max(g.time,g.row_id) AS end_time,
           arg_max(g.event_ms,g.row_id) AS end_ms,arg_min(g.prev_mid,g.row_id) AS pre_mid,
           arg_max(g.mid,g.row_id) AS post_mid,sum(g.source_price*g.source_volume/10000.0) AS episode_notional,
           bool_and(g.mid IS NOT NULL) AS all_trade_books_valid,
           min(g.source_link_status) AS min_link_status,max(g.source_link_status) AS max_link_status
    FROM trade_groups g
    WHERE g.source_action='TRADE' AND g.source_side IN ('B','S')
      AND g.event_order_id IS NOT NULL AND g.source_volume>0
    GROUP BY g.symbol,g.date,g.session,g.source_side,g.event_order_id,g.episode_group
),
valid_events AS MATERIALIZED (
    SELECT symbol,date,session,row_id,event_ms,mid,
           row_number() OVER (PARTITION BY symbol,date,session ORDER BY row_id) AS event_seq
    FROM raw WHERE mid IS NOT NULL
),
episode_enriched AS MATERIALIZED (
    SELECT e.*,v.event_seq,o.original_qty,
           (e.post_mid-e.pre_mid) AS immediate_impact
    FROM episodes e JOIN valid_events v
      ON v.symbol=e.symbol AND v.date=e.date AND v.session=e.session AND v.row_id=e.end_row_id
    JOIN orders o ON o.symbol=e.symbol AND o.date=e.date AND o.side=e.side AND o.order_id=e.order_id
    WHERE e.pre_mid IS NOT NULL AND e.post_mid IS NOT NULL AND e.all_trade_books_valid
),
order_window AS MATERIALIZED (
    SELECT r.symbol,r.date,r.source_side AS side,r.event_order_id AS order_id,w.window_name,
           sum(r.source_price*r.source_volume/10000.0) AS window_exec_notional
    FROM raw r CROSS JOIN (VALUES {window_values}) w(window_name,window_start,window_end)
    WHERE r.source_action='TRADE' AND r.source_side IN ('B','S')
      AND r.event_order_id IS NOT NULL AND r.source_volume>0
      AND r.time>=w.window_start AND r.time<w.window_end
    GROUP BY r.symbol,r.date,r.source_side,r.event_order_id,w.window_name
),
eligible AS MATERIALIZED (
    SELECT e.*,w.window_name,w.window_start,w.window_end,v.threshold_version,
           coalesce(t.mean_qty,rt.mean_qty) AS mean_qty,ow.window_exec_notional,
           row_number() OVER () AS episode_id
    FROM episode_enriched e
    JOIN order_window ow ON ow.symbol=e.symbol AND ow.date=e.date
      AND ow.side=e.side AND ow.order_id=e.order_id
    CROSS JOIN (VALUES {window_values}) w(window_name,window_start,window_end)
    CROSS JOIN (VALUES ('mean_x05'),('fixed_notional')) v(threshold_version)
    LEFT JOIN thresholds t ON t.symbol=e.symbol AND t.date=e.date
    LEFT JOIN rolling_thresholds rt ON rt.symbol=e.symbol AND rt.date=e.date
    WHERE ow.window_name=w.window_name AND e.start_time>=w.window_start AND e.end_time<w.window_end
      AND CASE WHEN v.threshold_version='mean_x05'
                 THEN e.original_qty>=coalesce(t.mean_qty,rt.mean_qty)*0.5
               ELSE ow.window_exec_notional>=? END
),
event_targets AS MATERIALIZED (
    SELECT e.*,h.horizon::BIGINT AS horizon,e.event_seq+h.horizon AS target_seq
    FROM eligible e CROSS JOIN (VALUES {event_values}) h(horizon)
),
event_retained AS MATERIALIZED (
    SELECT t.*,v.mid AS future_mid,v.event_ms AS future_ms,'event' AS clock_type,
           'events' AS horizon_unit
    FROM event_targets t LEFT JOIN valid_events v
      ON v.symbol=t.symbol AND v.date=t.date AND v.session=t.session AND v.event_seq=t.target_seq
),
time_targets AS MATERIALIZED (
    SELECT e.*,h.horizon_ms::BIGINT AS horizon,e.end_ms+h.horizon_ms AS target_ms
    FROM eligible e CROSS JOIN (VALUES {time_values}) h(horizon_ms)
),
time_retained AS MATERIALIZED (
    SELECT t.*,v.mid AS future_mid,v.event_ms AS future_ms,'time' AS clock_type,
           'milliseconds' AS horizon_unit
    FROM time_targets t ASOF LEFT JOIN valid_events v
      ON t.symbol=v.symbol AND t.date=v.date AND t.session=v.session
     AND t.target_ms<=v.event_ms
),
retained AS MATERIALIZED (
    SELECT * FROM event_retained UNION ALL SELECT * FROM time_retained
),
paired AS MATERIALIZED (
    SELECT *,future_mid-pre_mid AS retained_impact,
           CASE WHEN abs(immediate_impact)>1e-12 AND future_mid IS NOT NULL
                THEN greatest(-?,least(?,(future_mid-pre_mid)/immediate_impact)) END AS clipped_ratio
    FROM retained
),
aggregated AS MATERIALIZED (
    SELECT symbol,date,window_name,threshold_version,clock_type,horizon,horizon_unit,
           count(*) AS episode_count,count(*) FILTER (WHERE side='B') AS buy_episode_count,
           count(*) FILTER (WHERE side='S') AS sell_episode_count,
           count(*) FILTER (WHERE abs(immediate_impact)>1e-12) AS nonzero_immediate_count,
           count(*) FILTER (WHERE clipped_ratio IS NOT NULL) AS valid_retained_count,
           count(*) FILTER (WHERE future_mid IS NULL) AS missing_retained_count,
           sum(immediate_impact) AS immediate_impact_sum,sum(abs(immediate_impact)) AS immediate_impact_abs_sum,
           sum(retained_impact) FILTER (WHERE clipped_ratio IS NOT NULL) AS retained_impact_sum,
           sum(abs(retained_impact)) FILTER (WHERE clipped_ratio IS NOT NULL) AS retained_impact_abs_sum,
           sum(immediate_impact*clipped_ratio) FILTER (WHERE clipped_ratio IS NOT NULL)
             /nullif(sum(abs(immediate_impact)) FILTER (WHERE clipped_ratio IS NOT NULL),0) AS d07_directional,
           sum(abs(immediate_impact)*clipped_ratio) FILTER (WHERE clipped_ratio IS NOT NULL)
             /nullif(sum(abs(immediate_impact)) FILTER (WHERE clipped_ratio IS NOT NULL),0) AS d07_permanent_ratio,
           sum(abs(immediate_impact)*clipped_ratio) FILTER (WHERE clipped_ratio IS NOT NULL AND side='B')
             /nullif(sum(abs(immediate_impact)) FILTER (WHERE clipped_ratio IS NOT NULL AND side='B'),0)
             AS d07_buy_permanent_ratio,
           sum(abs(immediate_impact)*clipped_ratio) FILTER (WHERE clipped_ratio IS NOT NULL AND side='S')
             /nullif(sum(abs(immediate_impact)) FILTER (WHERE clipped_ratio IS NOT NULL AND side='S'),0)
             AS d07_sell_permanent_ratio,
           avg(future_ms-end_ms) FILTER (WHERE future_mid IS NOT NULL) AS mean_elapsed_ms,
           median(future_ms-end_ms) FILTER (WHERE future_mid IS NOT NULL) AS median_elapsed_ms
    FROM paired GROUP BY symbol,date,window_name,threshold_version,clock_type,horizon,horizon_unit
),
quality AS MATERIALIZED (
    SELECT r.symbol,r.date,w.window_name,count(*) AS window_event_count,
           count(*) FILTER (WHERE r.mid IS NOT NULL) AS valid_book_event_count,
           count(*) FILTER (WHERE r.mid IS NULL) AS invalid_book_event_count,
           count(*) FILTER (WHERE r.source_action='TRADE') AS trade_event_count,
           count(*) FILTER (WHERE r.source_action='TRADE' AND r.event_order_id IS NULL) AS missing_aggressor_id_count,
           count(*) FILTER (WHERE r.source_action='TRADE' AND r.source_link_status='FULL') AS full_link_trade_count,
           count(*) FILTER (WHERE r.source_action='TRADE' AND r.source_link_status='PARTIAL') AS partial_link_trade_count
    FROM raw r CROSS JOIN (VALUES {window_values}) w(window_name,window_start,window_end)
    WHERE r.time>=w.window_start AND r.time<w.window_end
    GROUP BY r.symbol,r.date,w.window_name
),
dates AS MATERIALIZED (SELECT DISTINCT symbol,date FROM raw),
grid AS MATERIALIZED (
    SELECT d.symbol,d.date,w.window_name,v.threshold_version,c.clock_type,c.horizon,c.horizon_unit,
           coalesce(t.mean_qty,rt.mean_qty) AS mean_qty
    FROM dates d CROSS JOIN (VALUES {window_values}) w(window_name,window_start,window_end)
    CROSS JOIN (VALUES ('mean_x05'),('fixed_notional')) v(threshold_version)
    CROSS JOIN (SELECT 'event' AS clock_type,h::BIGINT AS horizon,'events' AS horizon_unit
                FROM (VALUES {event_values}) x(h)
                UNION ALL
                SELECT 'time',h::BIGINT,'milliseconds' FROM (VALUES {time_values}) x(h)) c
    LEFT JOIN thresholds t ON t.symbol=d.symbol AND t.date=d.date
    LEFT JOIN rolling_thresholds rt ON rt.symbol=d.symbol AND rt.date=d.date
)
SELECT g.symbol,g.date,
       CASE WHEN starts_with(g.window_name,'intraday_') THEN 'intraday' ELSE 'daily' END AS frequency,g.window_name,g.threshold_version,
       g.clock_type,g.horizon,g.horizon_unit,
       coalesce(a.episode_count,0),coalesce(a.buy_episode_count,0),coalesce(a.sell_episode_count,0),
       coalesce(a.nonzero_immediate_count,0),coalesce(a.valid_retained_count,0),
       coalesce(a.missing_retained_count,0),a.immediate_impact_sum,a.immediate_impact_abs_sum,
       a.retained_impact_sum,a.retained_impact_abs_sum,a.d07_directional,a.d07_permanent_ratio,
       a.d07_buy_permanent_ratio,a.d07_sell_permanent_ratio,a.mean_elapsed_ms,a.median_elapsed_ms,
       g.mean_qty,?::DOUBLE AS fixed_notional,
       q.window_event_count,q.valid_book_event_count,q.invalid_book_event_count,q.trade_event_count,
       q.missing_aggressor_id_count,q.full_link_trade_count,q.partial_link_trade_count,
       CASE WHEN g.threshold_version='mean_x05' AND g.mean_qty IS NULL THEN false
            WHEN coalesce(a.nonzero_immediate_count,0)=0 THEN false
            WHEN coalesce(a.valid_retained_count,0)=0 THEN false ELSE true END AS is_valid,
       concat_ws(';',CASE WHEN g.threshold_version='mean_x05' AND g.mean_qty IS NULL THEN 'missing_mean_threshold' END,
         CASE WHEN coalesce(a.nonzero_immediate_count,0)=0 THEN 'zero_immediate_impact' END,
         CASE WHEN coalesce(a.valid_retained_count,0)=0 THEN 'missing_retained_price' END) AS invalid_reason,
       ? AS factor_version
FROM grid g LEFT JOIN aggregated a USING(symbol,date,window_name,threshold_version,clock_type,horizon,horizon_unit)
LEFT JOIN quality q USING(symbol,date,window_name)
ORDER BY g.symbol,g.date,g.window_name,g.threshold_version,g.clock_type,g.horizon
"""
    parameters = [
        list(paths), date_from, date_to, fixed_notional, ratio_clip, ratio_clip,
        fixed_notional, FACTOR_VERSION,
    ]
    return con.execute(query, parameters).fetchall()


def write_rows(path: str, rows: Sequence[tuple]) -> None:
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(f".{output.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("w", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(FIELDS)
            writer.writerows(
                tuple(round(value, 12) if isinstance(value, float) else value for value in row)
                for row in rows
            )
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, output)
    finally:
        if temporary.exists():
            temporary.unlink()


def validate_shard(path: Path) -> None:
    with path.open(newline="") as handle:
        if next(csv.reader(handle), None) != FIELDS:
            raise ValueError(f"invalid or incompatible shard: {path}")


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def prepare_manifest(
    shard_dir: Path,
    grouped: dict[str, list[str]],
    threshold_source: str,
    args: argparse.Namespace,
) -> None:
    config = {
        "factor_version": FACTOR_VERSION,
        "script_sha256": file_sha256(Path(__file__)),
        "threshold_source": str(Path(threshold_source).resolve()),
        "threshold_source_sha256": file_sha256(Path(threshold_source)),
        "date_from": args.date_from,
        "date_to": args.date_to,
        "windows": WINDOWS,
        "event_horizons": EVENT_HORIZONS,
        "time_horizons_ms": TIME_HORIZONS_MS,
        "threshold_versions": THRESHOLD_VERSIONS,
        "fixed_notional": args.fixed_notional,
        "ratio_clip": args.ratio_clip,
        "batch_symbols": args.batch_symbols,
        "universe_rule": "explicit point-in-time A-share stock manifest; ETF count must be zero",
        "inputs": grouped,
    }
    encoded = json.dumps(config, sort_keys=True, ensure_ascii=False).encode()
    payload = {"fingerprint": hashlib.sha256(encoded).hexdigest(), "config": config}
    shard_dir.mkdir(parents=True, exist_ok=True)
    path = shard_dir / "manifest.json"
    if path.exists():
        previous = json.loads(path.read_text())
        if previous.get("fingerprint") != payload["fingerprint"]:
            raise ValueError(f"shard manifest mismatch: {path}; use a new directory")
        return
    if list(shard_dir.glob("batch_*.csv")):
        raise ValueError(f"shards exist without manifest: {shard_dir}")
    temporary = shard_dir / f".manifest.{os.getpid()}.tmp"
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
    os.replace(temporary, path)


def worker(
    batch_number: int,
    paths: Sequence[str],
    threshold_rows: Sequence[tuple[str, int, float]],
    date_from: int,
    date_to: int,
    fixed_notional: float,
    ratio_clip: float,
    memory_limit: str,
    shard_path: str,
    temp_root: str,
) -> tuple[int, int, int]:
    temp_directory = Path(temp_root) / f"batch_{batch_number:06d}_{os.getpid()}"
    temp_directory.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("PRAGMA threads=1")
    con.execute(f"PRAGMA memory_limit='{memory_limit}'")
    con.execute("PRAGMA preserve_insertion_order=false")
    escaped = str(temp_directory).replace("'", "''")
    con.execute(f"PRAGMA temp_directory='{escaped}'")
    try:
        rows = compute_batch(
            con, paths, threshold_rows, date_from, date_to, fixed_notional, ratio_clip
        )
        write_rows(shard_path, rows)
        return batch_number, len(paths), len(rows)
    finally:
        con.close()
        shutil.rmtree(temp_directory, ignore_errors=True)


def merge_shards(output: str, shard_dir: Path, batch_numbers: Sequence[int]) -> int:
    target = Path(output)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
    row_count = 0
    try:
        with temporary.open("w", newline="") as destination:
            writer = csv.writer(destination)
            writer.writerow(FIELDS)
            for number in batch_numbers:
                path = shard_dir / f"batch_{number:06d}.csv"
                validate_shard(path)
                with path.open(newline="") as source:
                    reader = csv.reader(source)
                    next(reader)
                    for row in reader:
                        writer.writerow(row)
                        row_count += 1
            destination.flush()
            os.fsync(destination.fileno())
        os.replace(temporary, target)
    finally:
        if temporary.exists():
            temporary.unlink()
    return row_count


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute D07 retained impact from v4 LOB")
    parser.add_argument("inputs", nargs="*", default=[])
    parser.add_argument("--file-list", help="Explicit point-in-time stock parquet manifest")
    parser.add_argument(
        "--threshold-source",
        default=str(
            PROJECT_ROOT / "data/cache/stylized_fact_4_6/"
            "g2_d04_d06_primitives_202508_202601_no_industry_size_v2.csv"
        ),
    )
    parser.add_argument("--exchange", choices=("ALL", "SH", "SZ"), default="ALL")
    parser.add_argument("--date-from", type=int, default=20260105)
    parser.add_argument("--date-to", type=int, default=20260130)
    parser.add_argument("--fixed-notional", type=float, default=1_000_000.0)
    parser.add_argument("--ratio-clip", type=float, default=5.0)
    parser.add_argument("--batch-symbols", type=int, default=1)
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--memory-limit", default="6GB")
    parser.add_argument("--limit-symbols", type=int)
    parser.add_argument("--sample-symbols", type=int)
    parser.add_argument("--seed", type=int, default=20260804)
    parser.add_argument(
        "--shard-dir",
        default=str(PROJECT_ROOT / "data/cache/stylized_fact_4_6/d07_shards_202601_v1"),
    )
    parser.add_argument("--temp-root", default="/tmp/stylized_fact_4_6_d07")
    parser.add_argument(
        "--output",
        default=str(
            PROJECT_ROOT / "data/processed/stylized_fact_4_6/"
            "d07_retained_impact_event_time_202601_v1.csv"
        ),
    )
    args = parser.parse_args()
    if not args.file_list and not args.inputs:
        parser.error("provide --file-list or explicit stock parquet inputs")
    if args.date_from > args.date_to:
        parser.error("date-from must not exceed date-to")
    for name in ("batch_symbols", "workers"):
        if getattr(args, name) <= 0:
            parser.error(f"{name.replace('_', '-')} must be positive")
    if args.fixed_notional <= 0 or args.ratio_clip <= 0:
        parser.error("fixed-notional and ratio-clip must be positive")
    return args


def main() -> int:
    args = parse_args()
    grouped = expand_inputs(args.inputs, args.file_list, args.date_from, args.date_to)
    symbols = sorted(grouped)
    if args.exchange != "ALL":
        symbols = [symbol for symbol in symbols if symbol.startswith(args.exchange)]
    if args.sample_symbols and args.sample_symbols < len(symbols):
        symbols = sorted(random.Random(args.seed).sample(symbols, args.sample_symbols))
    if args.limit_symbols:
        symbols = symbols[: args.limit_symbols]
    grouped = {symbol: grouped[symbol] for symbol in symbols}
    if not grouped:
        raise SystemExit("no stock parquet inputs matched the requested month")

    thresholds = load_mean_thresholds(args.threshold_source, args.date_from, args.date_to)
    thresholds_by_symbol: dict[str, list[tuple[str, int, float]]] = {}
    for (symbol, date), value in thresholds.items():
        thresholds_by_symbol.setdefault(symbol, []).append((symbol, date, value))
    batch_specs = list(enumerate(chunks(symbols, args.batch_symbols), start=1))
    shard_dir = Path(args.shard_dir)
    prepare_manifest(shard_dir, grouped, args.threshold_source, args)
    pending: list[tuple[int, list[str], list[tuple[str, int, float]], Path]] = []
    resumed_symbols = 0
    for batch_number, batch_symbols in batch_specs:
        shard_path = shard_dir / f"batch_{batch_number:06d}.csv"
        paths = [path for symbol in batch_symbols for path in grouped[symbol]]
        threshold_rows = [
            row for symbol in batch_symbols for row in thresholds_by_symbol.get(symbol, [])
        ]
        if shard_path.exists():
            validate_shard(shard_path)
            resumed_symbols += len(batch_symbols)
        else:
            pending.append((batch_number, paths, threshold_rows, shard_path))
    print(
        f"symbols={len(symbols)} batches={len(batch_specs)} resumed_symbols={resumed_symbols} "
        f"pending_batches={len(pending)} thresholds={len(thresholds)}",
        flush=True,
    )

    if args.workers == 1:
        for completed, spec in enumerate(pending, start=1):
            number, paths, threshold_rows, shard_path = spec
            result = worker(
                number, paths, threshold_rows, args.date_from, args.date_to,
                args.fixed_notional, args.ratio_clip, args.memory_limit,
                str(shard_path), args.temp_root,
            )
            print(
                f"completed={completed}/{len(pending)} batch={result[0]} "
                f"files={result[1]} rows={result[2]}",
                flush=True,
            )
    else:
        with ProcessPoolExecutor(max_workers=args.workers) as executor:
            futures = {
                executor.submit(
                    worker, number, paths, threshold_rows, args.date_from,
                    args.date_to, args.fixed_notional, args.ratio_clip,
                    args.memory_limit, str(shard_path), args.temp_root,
                ): number
                for number, paths, threshold_rows, shard_path in pending
            }
            for completed, future in enumerate(as_completed(futures), start=1):
                result = future.result()
                print(
                    f"completed={completed}/{len(pending)} batch={result[0]} "
                    f"files={result[1]} rows={result[2]}",
                    flush=True,
                )

    batch_numbers = [number for number, _symbols in batch_specs]
    row_count = merge_shards(args.output, shard_dir, batch_numbers)
    output_symbols: set[str] = set()
    with open(args.output, newline="") as handle:
        for row in csv.DictReader(handle):
            output_symbols.add(row["symbol"])
    unexpected = output_symbols - set(symbols)
    if unexpected:
        raise RuntimeError(f"output contains symbols outside manifest: {sorted(unexpected)[:10]}")
    print(
        f"done rows={row_count} output_symbols={len(output_symbols)} etf_symbols=0 output={args.output}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
