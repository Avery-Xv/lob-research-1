#!/usr/bin/env python3
"""Compute the point-in-time 10:00--10:30 intraday D05 factor.

The current-day order classifier only uses events observable by 10:30.  In
particular, a Shanghai aggressive order whose remainder is published later is
classified from its cumulative executions known at the cutoff; a later
``ORDER_ADD`` and the post-processed linkage fields are never used.  Large-order
thresholds use complete order lifecycles, but strictly from the preceding
``history_days`` eligible trading observations.

The factor stage forms D05 surprises from 60 preceding observations of this
same intraday window and residualizes ALF on previous-trading-day LOB5-ex-size
controls.  This is a separate pipeline and does not alter daily D04--D06 output.
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
from collections import defaultdict, deque
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from statistics import mean
from typing import Iterable, Sequence

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parents[3]
FACTOR_VERSION = "stylized_fact_4_6_intraday_d05_pit_v1"
WINDOW_NAME = "intraday_1000_1030"
WINDOW_START = 100_000_000
WINDOW_END = 103_000_000
THRESHOLD_VERSIONS = ("mean_x05", "mean_x10", "p80", "p90", "fixed_notional")
STYLE_COLUMNS = (
    "non_linear_size", "momentum", "liquidity", "beta", "residual_volatility",
)

PRIMITIVE_BASE_FIELDS = [
    "symbol", "date", "frequency", "window_name", "window_start", "window_end",
    "threshold_history_days", "threshold_history_order_count",
    "threshold_mean_qty", "threshold_p80_qty", "threshold_p90_qty",
]
PRIMITIVE_FLOW_FIELDS = [
    field
    for version in THRESHOLD_VERSIONS
    for field in (
        f"{version}_buy_exec_qty", f"{version}_sell_exec_qty",
        f"{version}_buy_order_count", f"{version}_sell_order_count",
        f"{version}_alf",
    )
]
PRIMITIVE_QUALITY_FIELDS = [
    "window_events", "trade_events", "order_add_events", "cancel_events",
    "active_order_count", "trade_before_add_observed_count",
    "no_add_observed_count", "missing_aggressor_id_count",
    "invalid_volume_count", "fixed_notional", "is_valid", "invalid_reason",
    "factor_version",
]
PRIMITIVE_FIELDS = (
    PRIMITIVE_BASE_FIELDS + PRIMITIVE_FLOW_FIELDS + PRIMITIVE_QUALITY_FIELDS
)

FACTOR_FIELDS = [
    "symbol", "date", "frequency", "window_name", "threshold_version",
    "alf_raw", "alf_winsorized", "d04_residual", "d04_z", "d04_rank_pct",
    "d04_cross_section_n", "d04_regression_r2",
    "d05_surprise_60", "d05_buy_surprise_60", "d05_sell_surprise_60",
    "d05_history_observations", "active_large_buy_exec_qty",
    "active_large_sell_exec_qty", "active_large_buy_order_count",
    "active_large_sell_order_count", "threshold_history_days",
    "threshold_history_order_count", "control_date", "exposure_timing",
    "style_specification", "is_valid", "invalid_reason", "factor_version",
]


def chunks(items: Sequence[str], size: int) -> Iterable[Sequence[str]]:
    for index in range(0, len(items), size):
        yield items[index:index + size]


def finite(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def quantile(values: Sequence[float], probability: float) -> float:
    ordered = sorted(values)
    if not ordered:
        raise ValueError("cannot calculate a quantile of an empty sequence")
    position = probability * (len(ordered) - 1)
    lower, upper = math.floor(position), math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def winsorize(values: Sequence[float]) -> list[float]:
    lower, upper = quantile(values, 0.01), quantile(values, 0.99)
    return [min(max(value, lower), upper) for value in values]


def zscores(values: Sequence[float]) -> list[float]:
    center = mean(values)
    variance = sum((value - center) ** 2 for value in values) / len(values)
    scale = math.sqrt(variance)
    return [0.0] * len(values) if scale == 0 else [
        (value - center) / scale for value in values
    ]


def percentile_ranks(values: Sequence[float]) -> list[float]:
    order = sorted(range(len(values)), key=lambda index: (values[index], index))
    output = [0.0] * len(values)
    cursor = 0
    while cursor < len(order):
        end = cursor + 1
        while end < len(order) and values[order[end]] == values[order[cursor]]:
            end += 1
        rank = (cursor + end - 1) / 2.0
        percentile = rank / (len(values) - 1) if len(values) > 1 else 0.5
        for position in range(cursor, end):
            output[order[position]] = percentile
        cursor = end
    return output


def build_orthonormal_basis(exposures: Sequence[Sequence[float]]) -> list[list[float]]:
    if not exposures:
        return []
    basis_columns: list[list[float]] = []
    for column_index in range(len(exposures[0])):
        column = [row[column_index] for row in exposures]
        center = mean(column)
        vector = [value - center for value in column]
        for basis in basis_columns:
            projection = sum(value * base for value, base in zip(vector, basis))
            vector = [value - projection * base for value, base in zip(vector, basis)]
        norm = math.sqrt(sum(value * value for value in vector))
        if norm > 1e-10:
            basis_columns.append([value / norm for value in vector])
    return basis_columns


def residualize(values: Sequence[float], basis: Sequence[Sequence[float]]) -> list[float]:
    center = mean(values)
    residuals = [value - center for value in values]
    for column in basis:
        projection = sum(value * base for value, base in zip(residuals, column))
        residuals = [value - projection * base for value, base in zip(residuals, column)]
    return residuals


def r_squared(values: Sequence[float], residuals: Sequence[float]) -> float | None:
    center = mean(values)
    total = sum((value - center) ** 2 for value in values)
    if total == 0:
        return None
    unexplained = sum(value * value for value in residuals)
    return max(0.0, min(1.0, 1.0 - unexplained / total))


def ewma(values: Sequence[float], span: int) -> float:
    alpha = 2.0 / (span + 1.0)
    result = values[0]
    for value in values[1:]:
        result = alpha * value + (1.0 - alpha) * result
    return result


class RollingSurprise:
    """Strictly-lagged fixed-window surprise."""

    def __init__(self, length: int = 60) -> None:
        self.length = length
        self.values: deque[float] = deque(maxlen=length)

    def score(self, current: float) -> float | None:
        if len(self.values) < self.length:
            return None
        sample = list(self.values)
        center = ewma(sample, self.length)
        sample_mean = mean(sample)
        variance = sum((value - sample_mean) ** 2 for value in sample) / len(sample)
        scale = math.sqrt(variance)
        return (current - center) / scale if scale > 0 else 0.0

    def append(self, value: float) -> None:
        self.values.append(value)


class D05State:
    """History for one symbol/window/threshold series."""

    def __init__(self, length: int = 60) -> None:
        self.alf = RollingSurprise(length)
        self.buy = RollingSurprise(length)
        self.sell = RollingSurprise(length)
        self.observations = 0

    def update(self, current: float, buy_current: float, sell_current: float) -> dict[str, object]:
        result = {
            "d05_surprise_60": self.alf.score(current),
            "d05_buy_surprise_60": self.buy.score(buy_current),
            "d05_sell_surprise_60": self.sell.score(sell_current),
            "d05_history_observations": self.observations,
        }
        self.alf.append(current)
        self.buy.append(buy_current)
        self.sell.append(sell_current)
        self.observations += 1
        return result


def load_control_rows(path: str) -> tuple[dict[tuple[str, int], dict[str, str]], set[str]]:
    rows: dict[tuple[str, int], dict[str, str]] = {}
    symbols: set[str] = set()
    required = {
        "symbol", "date", "security_category", "board", "is_st",
        "is_suspended", "listing_days", "liquidity_history_days", *STYLE_COLUMNS,
    }
    with open(path, newline="") as handle:
        reader = csv.DictReader(handle)
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"control file missing columns: {sorted(missing)}")
        for row in reader:
            if int(row["security_category"]) != 1:
                raise ValueError(f"non-stock control row: {row['symbol']} {row['date']}")
            symbol = row["symbol"]
            if not (symbol.startswith("SH") or symbol.startswith("SZ")):
                continue
            key = (symbol, int(row["date"]))
            if key in rows:
                raise ValueError(f"duplicate control row: {key}")
            rows[key] = row
            symbols.add(symbol)
    return rows, symbols


def previous_market_dates(controls: dict[tuple[str, int], dict[str, str]]) -> dict[int, int]:
    dates = sorted({date for _symbol, date in controls})
    return {date: dates[index - 1] for index, date in enumerate(dates) if index > 0}


def expand_inputs(patterns: Sequence[str], stock_symbols: set[str]) -> dict[str, list[str]]:
    paths: list[str] = []
    for pattern in patterns:
        paths.extend(glob.glob(pattern) or [pattern])
    grouped: dict[str, list[str]] = defaultdict(list)
    for path in sorted(dict.fromkeys(paths)):
        symbol = Path(path).stem
        if symbol in stock_symbols:
            grouped[symbol].append(path)
    return dict(grouped)


def compute_primitives_batch(
    con: duckdb.DuckDBPyConnection,
    paths: Sequence[str],
    controls_file: str,
    history_date_from: int,
    date_from: int,
    date_to: int,
    history_days: int,
    fixed_notional: float,
) -> list[tuple]:
    """Return primitives; current-day classification is cutoff-local by construction."""
    query = f"""
WITH refs_unranked AS MATERIALIZED (
    SELECT symbol,date::INTEGER AS date
    FROM read_csv_auto(?,header=true)
    WHERE security_category::INTEGER=1 AND is_st::INTEGER=0
      AND is_suspended::INTEGER=0 AND listing_days::INTEGER>=10
      AND liquidity_history_days::INTEGER>=20 AND date::INTEGER BETWEEN ? AND ?
),
refs AS MATERIALIZED (
    SELECT *,row_number() OVER (PARTITION BY symbol ORDER BY date) AS day_seq
    FROM refs_unranked
),
raw_history AS MATERIALIZED (
    SELECT regexp_replace(regexp_extract(filename,'[^/]+$'),'\\.parquet$','') AS symbol,
      e.date::INTEGER AS date,e.time::BIGINT AS time,e.row_id::BIGINT AS row_id,
      e.source_action,e.source_side,e.source_buy_order_id,e.source_sell_order_id,
      e.source_price::DOUBLE AS source_price,e.source_volume::DOUBLE AS source_volume,
      CASE WHEN e.source_side='B' THEN e.source_buy_order_id
           WHEN e.source_side='S' THEN e.source_sell_order_id END AS event_order_id
    FROM read_parquet(?,filename=true) e INNER JOIN refs r
      ON r.symbol=regexp_replace(regexp_extract(filename,'[^/]+$'),'\\.parquet$','')
     AND r.date=e.date
    WHERE e.date BETWEEN ? AND ?
      AND ((e.time>=93000000 AND e.time<113000000)
        OR (e.time>=130000000 AND e.time<145700000))
),
history_adds AS MATERIALIZED (
    SELECT symbol,date,source_side AS side,event_order_id AS order_id,
      arg_min(source_volume,row_id) AS add_qty,min(row_id) AS first_add_row
    FROM raw_history WHERE source_action='ORDER_ADD' AND event_order_id IS NOT NULL
      AND source_volume>0 AND source_side IN ('B','S')
    GROUP BY symbol,date,side,order_id
),
history_trades AS MATERIALIZED (
    SELECT symbol,date,source_side AS side,event_order_id AS order_id,
      sum(source_volume) AS exec_qty,min(row_id) AS first_trade_row
    FROM raw_history WHERE source_action='TRADE' AND event_order_id IS NOT NULL
      AND source_volume>0 AND source_side IN ('B','S')
    GROUP BY symbol,date,side,order_id
),
history_pre_add AS MATERIALIZED (
    SELECT t.symbol,t.date,t.source_side AS side,t.event_order_id AS order_id,
      sum(t.source_volume) AS exec_qty
    FROM raw_history t JOIN history_adds a ON a.symbol=t.symbol AND a.date=t.date
      AND a.side=t.source_side AND a.order_id=t.event_order_id AND t.row_id<a.first_add_row
    WHERE t.source_action='TRADE' AND t.source_volume>0
    GROUP BY t.symbol,t.date,t.source_side,t.event_order_id
),
history_orders AS MATERIALIZED (
    SELECT coalesce(a.symbol,t.symbol) AS symbol,coalesce(a.date,t.date) AS date,
      CASE WHEN t.order_id IS NULL THEN a.add_qty WHEN a.order_id IS NULL THEN t.exec_qty
           WHEN a.first_add_row<t.first_trade_row THEN a.add_qty
           ELSE coalesce(p.exec_qty,0)+a.add_qty END AS original_qty
    FROM history_adds a FULL OUTER JOIN history_trades t
      ON a.symbol=t.symbol AND a.date=t.date AND a.side=t.side AND a.order_id=t.order_id
    LEFT JOIN history_pre_add p ON p.symbol=coalesce(a.symbol,t.symbol)
      AND p.date=coalesce(a.date,t.date) AND p.side=coalesce(a.side,t.side)
      AND p.order_id=coalesce(a.order_id,t.order_id)
),
order_hist AS MATERIALIZED (
    SELECT o.symbol,o.date,r.day_seq,o.original_qty,count(*) AS order_count
    FROM history_orders o JOIN refs r USING(symbol,date) WHERE o.original_qty>0
    GROUP BY o.symbol,o.date,r.day_seq,o.original_qty
),
current_dates AS MATERIALIZED (SELECT * FROM refs WHERE date BETWEEN ? AND ?),
rolling_hist AS MATERIALIZED (
    SELECT c.symbol,c.date,h.original_qty,sum(h.order_count) AS size_count
    FROM current_dates c JOIN order_hist h ON h.symbol=c.symbol
      AND h.day_seq BETWEEN c.day_seq-? AND c.day_seq-1
    GROUP BY c.symbol,c.date,h.original_qty
),
rolling_cum AS MATERIALIZED (
    SELECT *,sum(size_count) OVER (PARTITION BY symbol,date) AS total_count,
      sum(size_count) OVER (PARTITION BY symbol,date ORDER BY original_qty
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_count
    FROM rolling_hist
),
thresholds AS MATERIALIZED (
    SELECT symbol,date,sum(original_qty*size_count)/max(total_count) AS mean_qty,
      min(original_qty) FILTER (WHERE cumulative_count>=total_count*0.80) AS p80_qty,
      min(original_qty) FILTER (WHERE cumulative_count>=total_count*0.90) AS p90_qty,
      max(total_count) AS history_order_count
    FROM rolling_cum GROUP BY symbol,date
),
history_day_counts AS MATERIALIZED (
    SELECT c.symbol,c.date,count(DISTINCT h.date) AS history_days
    FROM current_dates c LEFT JOIN order_hist h ON h.symbol=c.symbol
      AND h.day_seq BETWEEN c.day_seq-? AND c.day_seq-1
    GROUP BY c.symbol,c.date
),
signal_known AS MATERIALIZED (
    SELECT * FROM raw_history WHERE date BETWEEN ? AND ? AND time<{WINDOW_END}
),
signal_adds AS MATERIALIZED (
    SELECT symbol,date,source_side AS side,event_order_id AS order_id,
      arg_min(source_volume,row_id) AS add_qty,min(row_id) AS first_add_row
    FROM signal_known WHERE source_action='ORDER_ADD' AND event_order_id IS NOT NULL
      AND source_volume>0 AND source_side IN ('B','S')
    GROUP BY symbol,date,side,order_id
),
signal_trades AS MATERIALIZED (
    SELECT symbol,date,source_side AS side,event_order_id AS order_id,
      sum(source_volume) AS known_exec_qty,min(row_id) AS first_trade_row,
      sum(source_volume) FILTER (WHERE time>={WINDOW_START} AND time<{WINDOW_END}) AS window_exec_qty,
      sum(source_price*source_volume/10000.0)
        FILTER (WHERE time>={WINDOW_START} AND time<{WINDOW_END}) AS window_exec_notional
    FROM signal_known WHERE source_action='TRADE' AND event_order_id IS NOT NULL
      AND source_volume>0 AND source_side IN ('B','S')
    GROUP BY symbol,date,side,order_id
),
signal_pre_add AS MATERIALIZED (
    SELECT t.symbol,t.date,t.source_side AS side,t.event_order_id AS order_id,
      sum(t.source_volume) AS exec_qty
    FROM signal_known t JOIN signal_adds a ON a.symbol=t.symbol AND a.date=t.date
      AND a.side=t.source_side AND a.order_id=t.event_order_id AND t.row_id<a.first_add_row
    WHERE t.source_action='TRADE' AND t.source_volume>0
    GROUP BY t.symbol,t.date,t.source_side,t.event_order_id
),
observable_active AS MATERIALIZED (
    SELECT t.symbol,t.date,t.side,t.order_id,t.window_exec_qty,t.window_exec_notional,
      CASE WHEN a.order_id IS NULL THEN t.known_exec_qty
           WHEN a.first_add_row<t.first_trade_row THEN a.add_qty
           ELSE coalesce(p.exec_qty,0)+a.add_qty END AS observable_qty,
      a.order_id IS NULL AS no_add_observed,
      a.first_add_row>t.first_trade_row AS trade_before_add_observed
    FROM signal_trades t LEFT JOIN signal_adds a USING(symbol,date,side,order_id)
    LEFT JOIN signal_pre_add p USING(symbol,date,side,order_id)
    WHERE coalesce(t.window_exec_qty,0)>0
),
classified AS MATERIALIZED (
    SELECT a.*,v.threshold_version,
      CASE WHEN v.threshold_version='fixed_notional' THEN a.window_exec_notional>=?
           ELSE a.observable_qty>=CASE v.threshold_version
             WHEN 'mean_x05' THEN t.mean_qty*0.5 WHEN 'mean_x10' THEN t.mean_qty
             WHEN 'p80' THEN t.p80_qty WHEN 'p90' THEN t.p90_qty END END AS is_large
    FROM observable_active a JOIN thresholds t USING(symbol,date)
    CROSS JOIN (VALUES ('mean_x05'),('mean_x10'),('p80'),('p90'),('fixed_notional'))
      v(threshold_version)
),
flow_long AS MATERIALIZED (
    SELECT symbol,date,threshold_version,
      coalesce(sum(window_exec_qty) FILTER (WHERE is_large AND side='B'),0) AS buy_qty,
      coalesce(sum(window_exec_qty) FILTER (WHERE is_large AND side='S'),0) AS sell_qty,
      count(*) FILTER (WHERE is_large AND side='B') AS buy_count,
      count(*) FILTER (WHERE is_large AND side='S') AS sell_count
    FROM classified GROUP BY symbol,date,threshold_version
),
flow_wide AS MATERIALIZED (
    SELECT symbol,date,
      max(buy_qty) FILTER (WHERE threshold_version='mean_x05') AS mean_x05_buy_qty,max(sell_qty) FILTER (WHERE threshold_version='mean_x05') AS mean_x05_sell_qty,
      max(buy_count) FILTER (WHERE threshold_version='mean_x05') AS mean_x05_buy_count,max(sell_count) FILTER (WHERE threshold_version='mean_x05') AS mean_x05_sell_count,
      max(buy_qty) FILTER (WHERE threshold_version='mean_x10') AS mean_x10_buy_qty,max(sell_qty) FILTER (WHERE threshold_version='mean_x10') AS mean_x10_sell_qty,
      max(buy_count) FILTER (WHERE threshold_version='mean_x10') AS mean_x10_buy_count,max(sell_count) FILTER (WHERE threshold_version='mean_x10') AS mean_x10_sell_count,
      max(buy_qty) FILTER (WHERE threshold_version='p80') AS p80_buy_qty,max(sell_qty) FILTER (WHERE threshold_version='p80') AS p80_sell_qty,
      max(buy_count) FILTER (WHERE threshold_version='p80') AS p80_buy_count,max(sell_count) FILTER (WHERE threshold_version='p80') AS p80_sell_count,
      max(buy_qty) FILTER (WHERE threshold_version='p90') AS p90_buy_qty,max(sell_qty) FILTER (WHERE threshold_version='p90') AS p90_sell_qty,
      max(buy_count) FILTER (WHERE threshold_version='p90') AS p90_buy_count,max(sell_count) FILTER (WHERE threshold_version='p90') AS p90_sell_count,
      max(buy_qty) FILTER (WHERE threshold_version='fixed_notional') AS fixed_notional_buy_qty,max(sell_qty) FILTER (WHERE threshold_version='fixed_notional') AS fixed_notional_sell_qty,
      max(buy_count) FILTER (WHERE threshold_version='fixed_notional') AS fixed_notional_buy_count,max(sell_count) FILTER (WHERE threshold_version='fixed_notional') AS fixed_notional_sell_count
    FROM flow_long GROUP BY symbol,date
),
window_stats AS MATERIALIZED (
    SELECT symbol,date,count(*) AS window_events,
      count(*) FILTER (WHERE source_action='TRADE') AS trade_events,
      count(*) FILTER (WHERE source_action='ORDER_ADD') AS order_add_events,
      count(*) FILTER (WHERE source_action='CANCEL') AS cancel_events,
      count(*) FILTER (WHERE source_action='TRADE' AND event_order_id IS NULL) AS missing_id,
      count(*) FILTER (WHERE source_volume IS NULL OR source_volume<=0) AS invalid_volume
    FROM signal_known WHERE time>={WINDOW_START} AND time<{WINDOW_END}
    GROUP BY symbol,date
),
order_quality AS MATERIALIZED (
    SELECT symbol,date,count(*) AS active_count,
      count(*) FILTER (WHERE trade_before_add_observed) AS trade_before_add_count,
      count(*) FILTER (WHERE no_add_observed) AS no_add_count
    FROM observable_active GROUP BY symbol,date
)
SELECT ws.symbol,ws.date,'intraday','{WINDOW_NAME}',{WINDOW_START},{WINDOW_END},
  h.history_days,t.history_order_count,t.mean_qty,t.p80_qty,t.p90_qty,
  coalesce(f.mean_x05_buy_qty,0),coalesce(f.mean_x05_sell_qty,0),coalesce(f.mean_x05_buy_count,0),coalesce(f.mean_x05_sell_count,0),
  CASE WHEN coalesce(f.mean_x05_buy_qty,0)+coalesce(f.mean_x05_sell_qty,0)>0 THEN (f.mean_x05_buy_qty-f.mean_x05_sell_qty)::DOUBLE/(f.mean_x05_buy_qty+f.mean_x05_sell_qty) END,
  coalesce(f.mean_x10_buy_qty,0),coalesce(f.mean_x10_sell_qty,0),coalesce(f.mean_x10_buy_count,0),coalesce(f.mean_x10_sell_count,0),
  CASE WHEN coalesce(f.mean_x10_buy_qty,0)+coalesce(f.mean_x10_sell_qty,0)>0 THEN (f.mean_x10_buy_qty-f.mean_x10_sell_qty)::DOUBLE/(f.mean_x10_buy_qty+f.mean_x10_sell_qty) END,
  coalesce(f.p80_buy_qty,0),coalesce(f.p80_sell_qty,0),coalesce(f.p80_buy_count,0),coalesce(f.p80_sell_count,0),
  CASE WHEN coalesce(f.p80_buy_qty,0)+coalesce(f.p80_sell_qty,0)>0 THEN (f.p80_buy_qty-f.p80_sell_qty)::DOUBLE/(f.p80_buy_qty+f.p80_sell_qty) END,
  coalesce(f.p90_buy_qty,0),coalesce(f.p90_sell_qty,0),coalesce(f.p90_buy_count,0),coalesce(f.p90_sell_count,0),
  CASE WHEN coalesce(f.p90_buy_qty,0)+coalesce(f.p90_sell_qty,0)>0 THEN (f.p90_buy_qty-f.p90_sell_qty)::DOUBLE/(f.p90_buy_qty+f.p90_sell_qty) END,
  coalesce(f.fixed_notional_buy_qty,0),coalesce(f.fixed_notional_sell_qty,0),coalesce(f.fixed_notional_buy_count,0),coalesce(f.fixed_notional_sell_count,0),
  CASE WHEN coalesce(f.fixed_notional_buy_qty,0)+coalesce(f.fixed_notional_sell_qty,0)>0 THEN (f.fixed_notional_buy_qty-f.fixed_notional_sell_qty)::DOUBLE/(f.fixed_notional_buy_qty+f.fixed_notional_sell_qty) END,
  ws.window_events,ws.trade_events,ws.order_add_events,ws.cancel_events,
  coalesce(q.active_count,0),coalesce(q.trade_before_add_count,0),coalesce(q.no_add_count,0),
  ws.missing_id,ws.invalid_volume,?,
  h.history_days>=? AND t.history_order_count>0 AND ws.missing_id=0,
  concat_ws(';',CASE WHEN h.history_days<? THEN 'insufficient_threshold_history' END,
    CASE WHEN t.history_order_count=0 THEN 'zero_threshold_orders' END,
    CASE WHEN ws.missing_id>0 THEN 'missing_aggressor_id' END),?
FROM window_stats ws JOIN thresholds t USING(symbol,date)
JOIN history_day_counts h USING(symbol,date) LEFT JOIN flow_wide f USING(symbol,date)
LEFT JOIN order_quality q USING(symbol,date)
ORDER BY ws.date,ws.symbol
"""
    parameters = [
        controls_file, history_date_from, date_to, list(paths), history_date_from,
        date_to, date_from, date_to, history_days, history_days, date_from, date_to,
        fixed_notional, fixed_notional, history_days, history_days, FACTOR_VERSION,
    ]
    return con.execute(query, parameters).fetchall()


def write_tuple_rows(path: str, fields: Sequence[str], rows: Sequence[tuple]) -> None:
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


def write_dict_rows(path: str, fields: Sequence[str], rows: Sequence[dict[str, object]]) -> None:
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


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def validate_shard(path: Path) -> None:
    with path.open(newline="") as handle:
        header = next(csv.reader(handle), None)
    if header != PRIMITIVE_FIELDS:
        raise ValueError(f"invalid or incompatible shard header: {path}")


def read_tuple_rows(path: Path) -> list[tuple]:
    with path.open(newline="") as handle:
        reader = csv.reader(handle)
        header = next(reader, None)
        if header != PRIMITIVE_FIELDS:
            raise ValueError(f"invalid or incompatible shard header: {path}")
        return [tuple(row) for row in reader]


def build_manifest(grouped: dict[str, list[str]], controls_file: str, args: argparse.Namespace) -> dict[str, object]:
    config = {
        "factor_version": FACTOR_VERSION,
        "script_sha256": file_sha256(Path(__file__).resolve()),
        "controls_file": str(Path(controls_file).resolve()),
        "controls_sha256": file_sha256(Path(controls_file)),
        "history_date_from": args.history_date_from,
        "date_from": args.date_from,
        "date_to": args.date_to,
        "history_days": args.history_days,
        "fixed_notional": args.fixed_notional,
        "batch_symbols": args.batch_symbols,
        "window": (WINDOW_NAME, WINDOW_START, WINDOW_END),
        "threshold_versions": THRESHOLD_VERSIONS,
        "point_in_time_rule": "current order size uses events through 10:30 only; no linkage fields",
        "inputs": {symbol: [str(Path(path).resolve()) for path in paths] for symbol, paths in sorted(grouped.items())},
    }
    encoded = json.dumps(config, sort_keys=True, ensure_ascii=False).encode()
    return {"fingerprint": hashlib.sha256(encoded).hexdigest(), "config": config}


def prepare_manifest(shard_dir: Path, manifest: dict[str, object]) -> None:
    shard_dir.mkdir(parents=True, exist_ok=True)
    path = shard_dir / "manifest.json"
    if path.exists():
        if json.loads(path.read_text()).get("fingerprint") != manifest["fingerprint"]:
            raise ValueError(f"shard manifest mismatch: {path}; use a new directory")
        return
    if list(shard_dir.glob("batch_*.csv")):
        raise ValueError(f"shards exist without manifest: {shard_dir}")
    temporary = shard_dir / f".manifest.{os.getpid()}.tmp"
    temporary.write_text(json.dumps(manifest, ensure_ascii=False, indent=2))
    os.replace(temporary, path)


def primitive_worker(
    batch_number: int, paths: Sequence[str], controls_file: str,
    history_date_from: int, date_from: int, date_to: int, history_days: int,
    fixed_notional: float, memory_limit: str, shard_path: str, temp_root: str,
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
        rows = compute_primitives_batch(
            con, paths, controls_file, history_date_from, date_from, date_to,
            history_days, fixed_notional,
        )
        write_tuple_rows(shard_path, PRIMITIVE_FIELDS, rows)
        return batch_number, len(paths), len(rows)
    finally:
        con.close()
        shutil.rmtree(temp_directory, ignore_errors=True)


def finalize_factors(
    primitive_path: str, controls_file: str, factor_date_from: int,
    factor_date_to: int, min_cross_section: int, surprise_observations: int = 60,
) -> list[dict[str, object]]:
    controls, _symbols = load_control_rows(controls_file)
    prior_dates = previous_market_dates(controls)
    states: dict[tuple[str, str, str], D05State] = {}
    output: list[dict[str, object]] = []

    def process_group(primitives: Sequence[dict[str, str]]) -> None:
        common_rows: list[dict[str, object]] = []
        for primitive in primitives:
            if primitive["is_valid"].lower() not in ("true", "1"):
                continue
            symbol, date = primitive["symbol"], int(primitive["date"])
            current_control = controls.get((symbol, date))
            control_date = prior_dates.get(date)
            prior_control = controls.get((symbol, control_date)) if control_date else None
            if current_control is None or prior_control is None:
                continue
            if any(int(current_control[name]) != expected for name, expected in (
                ("security_category", 1), ("is_st", 0), ("is_suspended", 0)
            )):
                continue
            styles = [finite(prior_control[name]) for name in STYLE_COLUMNS]
            if any(value is None for value in styles):
                continue
            common_rows.append({
                "primitive": primitive, "symbol": symbol, "date": date,
                "control_date": control_date,
                "styles": [float(value) for value in styles if value is not None],
                "threshold_history_days": int(primitive["threshold_history_days"]),
                "threshold_history_order_count": int(float(primitive["threshold_history_order_count"])),
            })

        for version in THRESHOLD_VERSIONS:
            rows: list[dict[str, object]] = []
            for common in common_rows:
                primitive = common["primitive"]
                assert isinstance(primitive, dict)
                alf = finite(primitive[f"{version}_alf"])
                if alf is None:
                    continue
                rows.append({
                    **{key: value for key, value in common.items() if key != "primitive"},
                    "alf_raw": alf,
                    "buy_qty": float(primitive[f"{version}_buy_exec_qty"]),
                    "sell_qty": float(primitive[f"{version}_sell_exec_qty"]),
                    "buy_count": int(float(primitive[f"{version}_buy_order_count"])),
                    "sell_count": int(float(primitive[f"{version}_sell_order_count"])),
                })
            rows.sort(key=lambda row: str(row["symbol"]))
            if len(rows) < min_cross_section:
                continue
            clipped = winsorize([float(row["alf_raw"]) for row in rows])
            style_matrix = [list(row["styles"]) for row in rows]
            basis = build_orthonormal_basis(style_matrix)
            residuals = residualize(clipped, basis)
            residual_z = zscores(residuals)
            residual_rank = percentile_ranks(residuals)
            model_r2 = r_squared(clipped, residuals)
            for index, row in enumerate(rows):
                key = (str(row["symbol"]), WINDOW_NAME, version)
                state = states.setdefault(key, D05State(surprise_observations))
                d05 = state.update(
                    residuals[index], math.log1p(float(row["buy_qty"])),
                    math.log1p(float(row["sell_qty"])),
                )
                date = int(row["date"])
                if factor_date_from <= date <= factor_date_to:
                    output.append({
                        "symbol": row["symbol"], "date": date,
                        "frequency": "intraday", "window_name": WINDOW_NAME,
                        "threshold_version": version, "alf_raw": row["alf_raw"],
                        "alf_winsorized": clipped[index], "d04_residual": residuals[index],
                        "d04_z": residual_z[index], "d04_rank_pct": residual_rank[index],
                        "d04_cross_section_n": len(rows), "d04_regression_r2": model_r2,
                        **d05,
                        "active_large_buy_exec_qty": row["buy_qty"],
                        "active_large_sell_exec_qty": row["sell_qty"],
                        "active_large_buy_order_count": row["buy_count"],
                        "active_large_sell_order_count": row["sell_count"],
                        "threshold_history_days": row["threshold_history_days"],
                        "threshold_history_order_count": row["threshold_history_order_count"],
                        "control_date": row["control_date"],
                        "exposure_timing": "previous_trading_day",
                        "style_specification": "LOB5-ex-size",
                        "is_valid": True, "invalid_reason": "",
                        "factor_version": FACTOR_VERSION,
                    })

    current_date: int | None = None
    group: list[dict[str, str]] = []
    with open(primitive_path, newline="") as handle:
        for primitive in csv.DictReader(handle):
            if primitive["window_name"] != WINDOW_NAME or primitive["frequency"] != "intraday":
                raise ValueError("intraday D05 primitive has an incompatible window/frequency")
            date = int(primitive["date"])
            if current_date is not None and date < current_date:
                raise ValueError("primitive input must be sorted by date")
            if current_date is not None and date != current_date:
                process_group(group)
                group = []
            current_date = date
            group.append(primitive)
    if group:
        process_group(group)
    output.sort(key=lambda row: (int(row["date"]), str(row["threshold_version"]), str(row["symbol"])))
    return output


def run_primitives(args: argparse.Namespace) -> None:
    _controls, stock_symbols = load_control_rows(args.controls_file)
    grouped = expand_inputs(args.inputs, stock_symbols)
    symbols = sorted(grouped)
    if args.sample_symbols and args.sample_symbols < len(symbols):
        symbols = sorted(random.Random(args.seed).sample(symbols, args.sample_symbols))
    if args.limit_symbols:
        symbols = symbols[:args.limit_symbols]
    grouped = {symbol: grouped[symbol] for symbol in symbols}
    if not grouped:
        raise SystemExit("no stock parquet inputs matched the explicit A-share universe")
    batches = list(enumerate(chunks(symbols, args.batch_symbols), start=1))
    shard_dir = Path(args.shard_dir)
    prepare_manifest(shard_dir, build_manifest(grouped, args.controls_file, args))
    pending: list[tuple[int, list[str], Path]] = []
    for number, batch_symbols in batches:
        shard_path = shard_dir / f"batch_{number:06d}.csv"
        paths = [path for symbol in batch_symbols for path in grouped[symbol]]
        if shard_path.exists():
            validate_shard(shard_path)
        else:
            pending.append((number, paths, shard_path))
    if args.workers == 1:
        for number, paths, shard_path in pending:
            print(primitive_worker(
                number, paths, args.controls_file, args.history_date_from,
                args.date_from, args.date_to, args.history_days, args.fixed_notional,
                args.memory_limit, str(shard_path), args.temp_root,
            ), flush=True)
    else:
        with ProcessPoolExecutor(max_workers=args.workers) as executor:
            futures = [executor.submit(
                primitive_worker, number, paths, args.controls_file,
                args.history_date_from, args.date_from, args.date_to,
                args.history_days, args.fixed_notional, args.memory_limit,
                str(shard_path), args.temp_root,
            ) for number, paths, shard_path in pending]
            for future in as_completed(futures):
                print(future.result(), flush=True)
    rows: list[tuple] = []
    for number, _symbols in batches:
        rows.extend(read_tuple_rows(shard_dir / f"batch_{number:06d}.csv"))
    rows.sort(key=lambda row: (int(row[1]), row[0]))
    write_tuple_rows(args.primitive_output, PRIMITIVE_FIELDS, rows)
    print(f"primitive_rows={len(rows)} primitive_output={args.primitive_output}", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute point-in-time 10:00--10:30 intraday D05.")
    parser.add_argument("stage", choices=("primitives", "factors", "all"), nargs="?", default="all")
    parser.add_argument("inputs", nargs="*", default=[
        str(Path("/hdd_data/lob/event_depth10_v4") / month / "*.parquet")
        for month in ("202507", "202508", "202509", "202510", "202511", "202512", "202601")
    ])
    parser.add_argument("--controls-file", default=str(PROJECT_ROOT / "data/cache/stylized_fact_4_6/d04_d06_controls_202507_202601.csv"))
    parser.add_argument("--history-date-from", type=int, default=20250701)
    parser.add_argument("--date-from", type=int, default=20250801)
    parser.add_argument("--date-to", type=int, default=20260130)
    parser.add_argument("--factor-date-from", type=int, default=20260105)
    parser.add_argument("--factor-date-to", type=int, default=20260130)
    parser.add_argument("--history-days", type=int, default=20)
    parser.add_argument("--surprise-observations", type=int, default=60)
    parser.add_argument("--fixed-notional", type=float, default=1_000_000.0)
    parser.add_argument("--batch-symbols", type=int, default=1)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--memory-limit", default="8GB")
    parser.add_argument("--limit-symbols", type=int)
    parser.add_argument("--sample-symbols", type=int)
    parser.add_argument("--seed", type=int, default=20260806)
    parser.add_argument("--min-cross-section", type=int, default=100)
    parser.add_argument("--shard-dir", default=str(PROJECT_ROOT / "data/cache/stylized_fact_4_6/intraday_d05_pit_shards_202508_202601_v1"))
    parser.add_argument("--temp-root", default="/tmp/stylized_fact_4_6_intraday_d05")
    parser.add_argument("--primitive-output", default=str(PROJECT_ROOT / "data/cache/stylized_fact_4_6/intraday_d05_pit_primitives_202508_202601_v1.csv"))
    parser.add_argument("--factor-output", default=str(PROJECT_ROOT / "data/processed/stylized_fact_4_6/intraday_d05_pit_factors_202601_v1.csv"))
    args = parser.parse_args()
    if args.history_date_from > args.date_from or args.date_from > args.date_to:
        parser.error("require history-date-from <= date-from <= date-to")
    if args.factor_date_from > args.factor_date_to:
        parser.error("factor-date-from must not exceed factor-date-to")
    for name in ("history_days", "surprise_observations", "batch_symbols", "workers", "min_cross_section"):
        if getattr(args, name) <= 0:
            parser.error(f"{name.replace('_', '-')} must be positive")
    return args


def main() -> int:
    args = parse_args()
    if args.stage in ("primitives", "all"):
        run_primitives(args)
    if args.stage in ("factors", "all"):
        rows = finalize_factors(
            args.primitive_output, args.controls_file, args.factor_date_from,
            args.factor_date_to, args.min_cross_section, args.surprise_observations,
        )
        if not rows:
            raise RuntimeError("factor finalization produced no rows")
        write_dict_rows(args.factor_output, FACTOR_FIELDS, rows)
        print(f"factor_rows={len(rows)} factor_output={args.factor_output}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
