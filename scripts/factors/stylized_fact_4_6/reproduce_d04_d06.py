#!/usr/bin/env python3
"""Compute daily D04--D06 active-large-order-flow factors from v4 LOB.

The primitive stage groups all requested month files for the same symbols so
that strictly lagged 20-day order-size thresholds cross month boundaries.  A
single materialized event scan produces both daily windows and every threshold
variant.  The factor stage reads only primitive/control CSV files.
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
FACTOR_VERSION = "stylized_fact_4_6_d04_d06_no_industry_size_v2"
WINDOWS = (
    ("daily_0930_close", 93_000_000, 145_700_000),
    ("daily_1000_close", 100_000_000, 145_700_000),
)
THRESHOLD_VERSIONS = (
    "mean_x05",
    "mean_x10",
    "p80",
    "p90",
    "fixed_notional",
)

PRIMITIVE_BASE_FIELDS = [
    "symbol", "date", "frequency", "window_name", "window_start", "window_end",
    "start_mid", "end_mid", "window_return",
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
    "active_order_count", "split_active_order_count", "trade_before_add_count",
    "fully_immediate_order_count", "full_link_trade_count",
    "partial_link_trade_count", "missing_aggressor_id_count",
    "invalid_volume_count", "valid_book_events", "invalid_book_events",
    "fixed_notional", "is_valid", "invalid_reason", "factor_version",
]
PRIMITIVE_FIELDS = (
    PRIMITIVE_BASE_FIELDS + PRIMITIVE_FLOW_FIELDS + PRIMITIVE_QUALITY_FIELDS
)

FACTOR_FIELDS = [
    "symbol", "date", "frequency", "window_name", "threshold_version",
    "alf_raw", "alf_winsorized", "d04_residual", "d04_z", "d04_rank_pct",
    "d04_cross_section_n", "d04_regression_r2",
    "d05_surprise_60", "d05_acceleration_3_20", "d05_persistence_5",
    "d05_same_sign_count_5", "d05_same_sign_run_length",
    "d05_buy_surprise_60", "d05_sell_surprise_60",
    "d05_history_observations",
    "price_response_residual", "d06_flow_bucket", "d06_response_bucket",
    "d06_underreaction_event", "d06_diff", "d06_expected_response",
    "d06_response_gap", "d06_daily_beta",
    "active_large_buy_exec_qty", "active_large_sell_exec_qty",
    "threshold_history_days", "threshold_history_order_count",
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


def quantile(values: Sequence[float], probability: float) -> float:
    ordered = sorted(values)
    if not ordered:
        raise ValueError("cannot calculate a quantile of an empty sequence")
    position = probability * (len(ordered) - 1)
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def winsorize(values: Sequence[float]) -> list[float]:
    lower = quantile(values, 0.01)
    upper = quantile(values, 0.99)
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
        average_rank = (cursor + end - 1) / 2.0
        percentile = average_rank / (len(values) - 1) if len(values) > 1 else 0.5
        for position in range(cursor, end):
            output[order[position]] = percentile
        cursor = end
    return output


def quintile(percentile: float) -> int:
    return min(5, int(percentile * 5.0) + 1)


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
    if not values:
        raise ValueError("EWMA requires at least one value")
    alpha = 2.0 / (span + 1.0)
    result = values[0]
    for value in values[1:]:
        result = alpha * value + (1.0 - alpha) * result
    return result


def lagged_surprise(current: float, history: Sequence[float], length: int = 60) -> float | None:
    if len(history) < length:
        return None
    sample = list(history[-length:])
    center = ewma(sample, length)
    sample_mean = mean(sample)
    variance = sum((value - sample_mean) ** 2 for value in sample) / len(sample)
    scale = math.sqrt(variance)
    return (current - center) / scale if scale > 0 else 0.0


class RollingSurprise:
    """Strictly-lagged fixed-window surprise with O(1) updates."""

    def __init__(self, length: int = 60) -> None:
        self.length = length
        self.values: deque[float] = deque()
        self.total = 0.0
        self.total_squared = 0.0
        self.alpha = 2.0 / (length + 1.0)
        self.decay = 1.0 - self.alpha
        self.decay_power = self.decay ** length
        self.center: float | None = None

    def score(self, current: float) -> float | None:
        if len(self.values) < self.length:
            return None
        sample_mean = self.total / self.length
        variance = max(0.0, self.total_squared / self.length - sample_mean ** 2)
        scale = math.sqrt(variance)
        assert self.center is not None
        return (current - self.center) / scale if scale > 0 else 0.0

    def append(self, value: float) -> None:
        if len(self.values) < self.length:
            self.values.append(value)
            self.total += value
            self.total_squared += value * value
            if len(self.values) == self.length:
                self.center = ewma(list(self.values), self.length)
            return

        oldest = self.values.popleft()
        new_first = self.values[0]
        assert self.center is not None
        self.center = (
            self.decay * self.center
            - self.decay_power * oldest
            + self.decay_power * new_first
            + self.alpha * value
        )
        self.values.append(value)
        self.total += value - oldest
        self.total_squared += value * value - oldest * oldest


class D05State:
    """Incremental D05 history for one symbol/window/threshold series."""

    def __init__(self) -> None:
        self.d04_surprise = RollingSurprise()
        self.buy_surprise = RollingSurprise()
        self.sell_surprise = RollingSurprise()
        self.ewma3: float | None = None
        self.ewma20: float | None = None
        self.recent: deque[float] = deque(maxlen=5)
        self.last_sign = 0
        self.run_length = 0
        self.observations = 0

    def update(
        self, current: float, buy_current: float, sell_current: float
    ) -> dict[str, object]:
        surprise = self.d04_surprise.score(current)
        buy_surprise = self.buy_surprise.score(buy_current)
        sell_surprise = self.sell_surprise.score(sell_current)

        self.ewma3 = current if self.ewma3 is None else (
            0.5 * current + 0.5 * self.ewma3
        )
        alpha20 = 2.0 / 21.0
        self.ewma20 = current if self.ewma20 is None else (
            alpha20 * current + (1.0 - alpha20) * self.ewma20
        )
        self.recent.append(current)
        persistence = sum(self.recent) if len(self.recent) == 5 else None
        sign = 1 if current > 0 else (-1 if current < 0 else 0)
        same_sign_count = sum(
            (1 if value > 0 else (-1 if value < 0 else 0)) == sign
            for value in self.recent
        ) if len(self.recent) == 5 and sign else None
        if sign:
            self.run_length = self.run_length + 1 if sign == self.last_sign else 1
        else:
            self.run_length = 0
        self.last_sign = sign

        result = {
            "d05_surprise_60": surprise,
            "d05_buy_surprise_60": buy_surprise,
            "d05_sell_surprise_60": sell_surprise,
            "d05_acceleration_3_20": self.ewma3 - self.ewma20,
            "d05_persistence_5": persistence,
            "d05_same_sign_count_5": same_sign_count,
            "d05_same_sign_run_length": self.run_length if sign else None,
            "d05_history_observations": self.observations,
        }
        self.d04_surprise.append(current)
        self.buy_surprise.append(buy_current)
        self.sell_surprise.append(sell_current)
        self.observations += 1
        return result


def load_control_rows(path: str) -> tuple[dict[tuple[str, int], dict[str, str]], set[str]]:
    rows: dict[tuple[str, int], dict[str, str]] = {}
    symbols: set[str] = set()
    with open(path, newline="") as handle:
        reader = csv.DictReader(handle)
        required = {
            "symbol", "date", "security_category", "board", "industry",
            "is_st", "is_suspended", "listing_days", "close", "ret5_lagged",
            "turnover20", "amihud20", "liquidity_history_days", "log_circ_mv",
            "residual_volatility",
        }
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"control file missing columns: {sorted(missing)}")
        for row in reader:
            if int(row["security_category"]) != 1:
                raise ValueError(
                    f"non-stock control row: {row['symbol']} {row['date']}"
                )
            symbol = row["symbol"]
            if not (symbol.startswith("SH") or symbol.startswith("SZ")):
                continue
            key = (symbol, int(row["date"]))
            if key in rows:
                raise ValueError(f"duplicate control row: {key}")
            rows[key] = row
            symbols.add(symbol)
    return rows, symbols


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
    query = """
WITH refs_unranked AS MATERIALIZED (
    SELECT symbol, date::INTEGER AS date
    FROM read_csv_auto(?, header=true)
    WHERE security_category::INTEGER = 1
      AND is_st::INTEGER = 0
      AND is_suspended::INTEGER = 0
      AND listing_days::INTEGER >= 10
      AND liquidity_history_days::INTEGER >= 20
      AND date::INTEGER BETWEEN ? AND ?
),
refs AS MATERIALIZED (
    SELECT *, row_number() OVER (PARTITION BY symbol ORDER BY date) AS day_seq
    FROM refs_unranked
),
raw_events AS MATERIALIZED (
    SELECT
        regexp_replace(regexp_extract(filename, '[^/]+$'), '\\.parquet$', '') AS symbol,
        e.date::INTEGER AS date, e.time::BIGINT AS time, e.row_id::BIGINT AS row_id,
        e.source_action, e.source_side, e.source_buy_order_id,
        e.source_sell_order_id, e.source_price::DOUBLE AS source_price,
        e.source_volume::DOUBLE AS source_volume, e.source_link_status,
        CASE WHEN e.source_side = 'B' THEN e.source_buy_order_id
             WHEN e.source_side = 'S' THEN e.source_sell_order_id END AS event_order_id,
        CASE WHEN array_length(e.bid_px) > 0 AND array_length(e.ask_px) > 0
                  AND e.bid_px[1] IS NOT NULL AND e.ask_px[1] IS NOT NULL
                  AND e.bid_px[1] > 0 AND e.ask_px[1] >= e.bid_px[1]
             THEN (e.bid_px[1]::DOUBLE + e.ask_px[1]::DOUBLE) / 20000.0 END AS mid,
        CASE WHEN array_length(e.bid_px) > 0 AND array_length(e.ask_px) > 0
                  AND e.bid_px[1] IS NOT NULL AND e.ask_px[1] IS NOT NULL
                  AND e.bid_px[1] > 0 AND e.ask_px[1] >= e.bid_px[1]
             THEN true ELSE false END AS valid_book
    FROM read_parquet(?, filename=true) e
    INNER JOIN refs r
      ON r.symbol = regexp_replace(regexp_extract(filename, '[^/]+$'), '\\.parquet$', '')
     AND r.date = e.date
    WHERE e.date BETWEEN ? AND ?
      AND ((e.time >= 93000000 AND e.time < 113000000)
        OR (e.time >= 130000000 AND e.time < 145700000))
),
adds AS MATERIALIZED (
    SELECT symbol, date, source_side AS side, event_order_id AS order_id,
           arg_min(source_volume, row_id) AS add_qty,
           min(row_id) AS first_add_row, count(*) AS add_rows
    FROM raw_events
    WHERE source_action = 'ORDER_ADD' AND event_order_id IS NOT NULL
      AND source_volume > 0 AND source_side IN ('B','S')
    GROUP BY symbol, date, side, order_id
),
active_trades AS MATERIALIZED (
    SELECT symbol, date, source_side AS side, event_order_id AS order_id,
           sum(source_volume) AS exec_qty_day, count(*) AS trade_rows,
           min(row_id) AS first_trade_row,
           sum(source_volume) FILTER (WHERE time >= 93000000 AND time < 145700000)
               AS exec_qty_0930,
           sum(source_volume) FILTER (WHERE time >= 100000000 AND time < 145700000)
               AS exec_qty_1000,
           sum(source_price * source_volume / 10000.0)
               FILTER (WHERE time >= 93000000 AND time < 145700000)
               AS exec_notional_0930,
           sum(source_price * source_volume / 10000.0)
               FILTER (WHERE time >= 100000000 AND time < 145700000)
               AS exec_notional_1000
    FROM raw_events
    WHERE source_action = 'TRADE' AND event_order_id IS NOT NULL
      AND source_volume > 0 AND source_side IN ('B','S')
    GROUP BY symbol, date, side, order_id
),
pre_add_exec AS MATERIALIZED (
    SELECT t.symbol, t.date, t.source_side AS side, t.event_order_id AS order_id,
           sum(t.source_volume) AS pre_add_exec_qty
    FROM raw_events t
    INNER JOIN adds a ON a.symbol=t.symbol AND a.date=t.date
      AND a.side=t.source_side AND a.order_id=t.event_order_id
      AND t.row_id < a.first_add_row
    WHERE t.source_action='TRADE' AND t.source_volume>0
    GROUP BY t.symbol, t.date, t.source_side, t.event_order_id
),
orders AS MATERIALIZED (
    SELECT coalesce(a.symbol,t.symbol) AS symbol, coalesce(a.date,t.date) AS date,
           coalesce(a.side,t.side) AS side, coalesce(a.order_id,t.order_id) AS order_id,
           CASE
             WHEN t.order_id IS NULL THEN a.add_qty
             WHEN a.order_id IS NULL THEN t.exec_qty_day
             WHEN a.first_add_row < t.first_trade_row THEN a.add_qty
             ELSE coalesce(p.pre_add_exec_qty,0) + a.add_qty
           END AS original_qty,
           t.order_id IS NOT NULL AS is_active,
           t.first_trade_row < a.first_add_row AS trade_before_add,
           a.add_rows
    FROM adds a FULL OUTER JOIN active_trades t
      ON a.symbol=t.symbol AND a.date=t.date AND a.side=t.side AND a.order_id=t.order_id
    LEFT JOIN pre_add_exec p
      ON p.symbol=coalesce(a.symbol,t.symbol) AND p.date=coalesce(a.date,t.date)
     AND p.side=coalesce(a.side,t.side) AND p.order_id=coalesce(a.order_id,t.order_id)
    WHERE CASE
             WHEN t.order_id IS NULL THEN a.add_qty
             WHEN a.order_id IS NULL THEN t.exec_qty_day
             WHEN a.first_add_row < t.first_trade_row THEN a.add_qty
             ELSE coalesce(p.pre_add_exec_qty,0) + a.add_qty
          END > 0
),
order_hist AS MATERIALIZED (
    SELECT o.symbol,o.date,r.day_seq,o.original_qty,count(*) AS order_count
    FROM orders o JOIN refs r USING(symbol,date)
    GROUP BY o.symbol,o.date,r.day_seq,o.original_qty
),
current_dates AS MATERIALIZED (
    SELECT * FROM refs WHERE date BETWEEN ? AND ?
),
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
    SELECT symbol,date,count(DISTINCT CASE WHEN size_count>0 THEN original_qty END) AS size_bins,
           sum(original_qty*size_count)/max(total_count) AS mean_qty,
           min(original_qty) FILTER (WHERE cumulative_count >= total_count*0.80) AS p80_qty,
           min(original_qty) FILTER (WHERE cumulative_count >= total_count*0.90) AS p90_qty,
           max(total_count) AS history_order_count
    FROM rolling_cum GROUP BY symbol,date
),
history_day_counts AS MATERIALIZED (
    SELECT c.symbol,c.date,count(DISTINCT h.date) AS history_days
    FROM current_dates c LEFT JOIN order_hist h ON h.symbol=c.symbol
      AND h.day_seq BETWEEN c.day_seq-? AND c.day_seq-1
    GROUP BY c.symbol,c.date
),
active_expanded AS MATERIALIZED (
    SELECT a.symbol,a.date,a.side,a.order_id,o.original_qty,a.trade_rows,
           w.window_name,w.window_start,w.window_end,
           CASE WHEN w.window_name='daily_0930_close' THEN a.exec_qty_0930
                ELSE a.exec_qty_1000 END AS window_exec_qty,
           CASE WHEN w.window_name='daily_0930_close' THEN a.exec_notional_0930
                ELSE a.exec_notional_1000 END AS window_exec_notional
    FROM active_trades a JOIN orders o ON o.symbol=a.symbol AND o.date=a.date
      AND o.side=a.side AND o.order_id=a.order_id
    CROSS JOIN (VALUES ('daily_0930_close',93000000::BIGINT,145700000::BIGINT),
                       ('daily_1000_close',100000000::BIGINT,145700000::BIGINT))
      w(window_name,window_start,window_end)
    WHERE a.date BETWEEN ? AND ?
),
classified AS MATERIALIZED (
    SELECT a.*,v.threshold_version,
           CASE v.threshold_version
             WHEN 'mean_x05' THEN t.mean_qty*0.5
             WHEN 'mean_x10' THEN t.mean_qty
             WHEN 'p80' THEN t.p80_qty
             WHEN 'p90' THEN t.p90_qty
             ELSE NULL END AS size_threshold,
           CASE WHEN v.threshold_version='fixed_notional'
                THEN a.window_exec_notional >= ?
                ELSE a.original_qty >= CASE v.threshold_version
                  WHEN 'mean_x05' THEN t.mean_qty*0.5
                  WHEN 'mean_x10' THEN t.mean_qty
                  WHEN 'p80' THEN t.p80_qty
                  WHEN 'p90' THEN t.p90_qty END END AS is_large
    FROM active_expanded a JOIN thresholds t USING(symbol,date)
    CROSS JOIN (VALUES ('mean_x05'),('mean_x10'),('p80'),('p90'),('fixed_notional'))
      v(threshold_version)
),
flow_long AS MATERIALIZED (
    SELECT symbol,date,window_name,threshold_version,
      coalesce(sum(window_exec_qty) FILTER (WHERE is_large AND side='B'),0) AS buy_qty,
      coalesce(sum(window_exec_qty) FILTER (WHERE is_large AND side='S'),0) AS sell_qty,
      count(*) FILTER (WHERE is_large AND side='B' AND window_exec_qty>0) AS buy_count,
      count(*) FILTER (WHERE is_large AND side='S' AND window_exec_qty>0) AS sell_count
    FROM classified GROUP BY symbol,date,window_name,threshold_version
),
flow_wide AS MATERIALIZED (
    SELECT symbol,date,window_name,
      max(buy_qty) FILTER (WHERE threshold_version='mean_x05') AS mean_x05_buy_exec_qty,
      max(sell_qty) FILTER (WHERE threshold_version='mean_x05') AS mean_x05_sell_exec_qty,
      max(buy_count) FILTER (WHERE threshold_version='mean_x05') AS mean_x05_buy_order_count,
      max(sell_count) FILTER (WHERE threshold_version='mean_x05') AS mean_x05_sell_order_count,
      max(buy_qty) FILTER (WHERE threshold_version='mean_x10') AS mean_x10_buy_exec_qty,
      max(sell_qty) FILTER (WHERE threshold_version='mean_x10') AS mean_x10_sell_exec_qty,
      max(buy_count) FILTER (WHERE threshold_version='mean_x10') AS mean_x10_buy_order_count,
      max(sell_count) FILTER (WHERE threshold_version='mean_x10') AS mean_x10_sell_order_count,
      max(buy_qty) FILTER (WHERE threshold_version='p80') AS p80_buy_exec_qty,
      max(sell_qty) FILTER (WHERE threshold_version='p80') AS p80_sell_exec_qty,
      max(buy_count) FILTER (WHERE threshold_version='p80') AS p80_buy_order_count,
      max(sell_count) FILTER (WHERE threshold_version='p80') AS p80_sell_order_count,
      max(buy_qty) FILTER (WHERE threshold_version='p90') AS p90_buy_exec_qty,
      max(sell_qty) FILTER (WHERE threshold_version='p90') AS p90_sell_exec_qty,
      max(buy_count) FILTER (WHERE threshold_version='p90') AS p90_buy_order_count,
      max(sell_count) FILTER (WHERE threshold_version='p90') AS p90_sell_order_count,
      max(buy_qty) FILTER (WHERE threshold_version='fixed_notional') AS fixed_notional_buy_exec_qty,
      max(sell_qty) FILTER (WHERE threshold_version='fixed_notional') AS fixed_notional_sell_exec_qty,
      max(buy_count) FILTER (WHERE threshold_version='fixed_notional') AS fixed_notional_buy_order_count,
      max(sell_count) FILTER (WHERE threshold_version='fixed_notional') AS fixed_notional_sell_order_count
    FROM flow_long GROUP BY symbol,date,window_name
),
window_stats AS MATERIALIZED (
    SELECT r.symbol,r.date,w.window_name,w.window_start,w.window_end,
      arg_min(r.mid,r.row_id) FILTER (WHERE r.mid IS NOT NULL) AS start_mid,
      arg_max(r.mid,r.row_id) FILTER (WHERE r.mid IS NOT NULL) AS end_mid,
      count(*) AS window_events,
      count(*) FILTER (WHERE source_action='TRADE') AS trade_events,
      count(*) FILTER (WHERE source_action='ORDER_ADD') AS order_add_events,
      count(*) FILTER (WHERE source_action='CANCEL') AS cancel_events,
      count(*) FILTER (WHERE valid_book) AS valid_book_events,
      count(*) FILTER (WHERE NOT valid_book) AS invalid_book_events,
      count(*) FILTER (WHERE source_volume IS NULL OR source_volume<=0) AS invalid_volume_count,
      count(*) FILTER (WHERE source_action='TRADE' AND event_order_id IS NULL)
        AS missing_aggressor_id_count,
      count(*) FILTER (WHERE source_action='TRADE' AND source_link_status='FULL')
        AS full_link_trade_count,
      count(*) FILTER (WHERE source_action='TRADE' AND source_link_status='PARTIAL')
        AS partial_link_trade_count
    FROM raw_events r CROSS JOIN (VALUES
      ('daily_0930_close',93000000::BIGINT,145700000::BIGINT),
      ('daily_1000_close',100000000::BIGINT,145700000::BIGINT))
      w(window_name,window_start,window_end)
    WHERE r.date BETWEEN ? AND ? AND r.time>=w.window_start AND r.time<w.window_end
    GROUP BY r.symbol,r.date,w.window_name,w.window_start,w.window_end
),
order_quality AS MATERIALIZED (
    SELECT symbol,date,count(*) FILTER (WHERE is_active) AS active_order_count,
      count(*) FILTER (WHERE is_active AND order_id IN (
        SELECT order_id FROM active_trades t WHERE t.symbol=orders.symbol
          AND t.date=orders.date AND t.side=orders.side AND t.trade_rows>1))
        AS split_active_order_count,
      count(*) FILTER (WHERE trade_before_add) AS trade_before_add_count,
      count(*) FILTER (WHERE is_active AND add_rows IS NULL) AS fully_immediate_order_count
    FROM orders GROUP BY symbol,date
)
SELECT ws.symbol,ws.date,'daily' AS frequency,ws.window_name,ws.window_start,ws.window_end,
  ws.start_mid,ws.end_mid,
  CASE WHEN ws.start_mid>0 THEN ws.end_mid/ws.start_mid-1 END AS window_return,
  h.history_days,t.history_order_count,t.mean_qty,t.p80_qty,t.p90_qty,
  coalesce(f.mean_x05_buy_exec_qty,0),coalesce(f.mean_x05_sell_exec_qty,0),
  coalesce(f.mean_x05_buy_order_count,0),coalesce(f.mean_x05_sell_order_count,0),
  CASE WHEN coalesce(f.mean_x05_buy_exec_qty,0)+coalesce(f.mean_x05_sell_exec_qty,0)>0
       THEN (f.mean_x05_buy_exec_qty-f.mean_x05_sell_exec_qty)::DOUBLE/
            (f.mean_x05_buy_exec_qty+f.mean_x05_sell_exec_qty) END,
  coalesce(f.mean_x10_buy_exec_qty,0),coalesce(f.mean_x10_sell_exec_qty,0),
  coalesce(f.mean_x10_buy_order_count,0),coalesce(f.mean_x10_sell_order_count,0),
  CASE WHEN coalesce(f.mean_x10_buy_exec_qty,0)+coalesce(f.mean_x10_sell_exec_qty,0)>0
       THEN (f.mean_x10_buy_exec_qty-f.mean_x10_sell_exec_qty)::DOUBLE/
            (f.mean_x10_buy_exec_qty+f.mean_x10_sell_exec_qty) END,
  coalesce(f.p80_buy_exec_qty,0),coalesce(f.p80_sell_exec_qty,0),
  coalesce(f.p80_buy_order_count,0),coalesce(f.p80_sell_order_count,0),
  CASE WHEN coalesce(f.p80_buy_exec_qty,0)+coalesce(f.p80_sell_exec_qty,0)>0
       THEN (f.p80_buy_exec_qty-f.p80_sell_exec_qty)::DOUBLE/
            (f.p80_buy_exec_qty+f.p80_sell_exec_qty) END,
  coalesce(f.p90_buy_exec_qty,0),coalesce(f.p90_sell_exec_qty,0),
  coalesce(f.p90_buy_order_count,0),coalesce(f.p90_sell_order_count,0),
  CASE WHEN coalesce(f.p90_buy_exec_qty,0)+coalesce(f.p90_sell_exec_qty,0)>0
       THEN (f.p90_buy_exec_qty-f.p90_sell_exec_qty)::DOUBLE/
            (f.p90_buy_exec_qty+f.p90_sell_exec_qty) END,
  coalesce(f.fixed_notional_buy_exec_qty,0),coalesce(f.fixed_notional_sell_exec_qty,0),
  coalesce(f.fixed_notional_buy_order_count,0),coalesce(f.fixed_notional_sell_order_count,0),
  CASE WHEN coalesce(f.fixed_notional_buy_exec_qty,0)+coalesce(f.fixed_notional_sell_exec_qty,0)>0
       THEN (f.fixed_notional_buy_exec_qty-f.fixed_notional_sell_exec_qty)::DOUBLE/
            (f.fixed_notional_buy_exec_qty+f.fixed_notional_sell_exec_qty) END,
  ws.window_events,ws.trade_events,ws.order_add_events,ws.cancel_events,
  coalesce(oq.active_order_count,0),coalesce(oq.split_active_order_count,0),
  coalesce(oq.trade_before_add_count,0),coalesce(oq.fully_immediate_order_count,0),
  ws.full_link_trade_count,ws.partial_link_trade_count,ws.missing_aggressor_id_count,
  ws.invalid_volume_count,ws.valid_book_events,ws.invalid_book_events,?,
  h.history_days>=? AND t.history_order_count>0 AND ws.start_mid>0 AND ws.end_mid>0
    AND ws.missing_aggressor_id_count=0 AS is_valid,
  concat_ws(';',CASE WHEN h.history_days<? THEN 'insufficient_threshold_history' END,
    CASE WHEN t.history_order_count=0 THEN 'zero_threshold_orders' END,
    CASE WHEN ws.start_mid IS NULL OR ws.end_mid IS NULL THEN 'invalid_window_price' END,
    CASE WHEN ws.missing_aggressor_id_count>0 THEN 'missing_aggressor_id' END) AS invalid_reason,
  ? AS factor_version
FROM window_stats ws JOIN thresholds t USING(symbol,date)
JOIN history_day_counts h USING(symbol,date)
LEFT JOIN flow_wide f USING(symbol,date,window_name)
LEFT JOIN order_quality oq USING(symbol,date)
ORDER BY ws.date,ws.window_name,ws.symbol
"""
    parameters = [
        controls_file, history_date_from, date_to, list(paths), history_date_from,
        date_to, date_from, date_to, history_days, history_days, date_from, date_to,
        fixed_notional, date_from, date_to, fixed_notional, history_days,
        history_days, FACTOR_VERSION,
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


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def build_manifest(
    grouped_paths: dict[str, list[str]], controls_file: str, args: argparse.Namespace
) -> dict[str, object]:
    script_path = Path(__file__).resolve()
    config = {
        "factor_version": FACTOR_VERSION,
        "script_sha256": file_sha256(script_path),
        "controls_file": str(Path(controls_file).resolve()),
        "controls_sha256": file_sha256(Path(controls_file)),
        "history_date_from": args.history_date_from,
        "date_from": args.date_from,
        "date_to": args.date_to,
        "history_days": args.history_days,
        "fixed_notional": args.fixed_notional,
        "batch_symbols": args.batch_symbols,
        "windows": WINDOWS,
        "threshold_versions": THRESHOLD_VERSIONS,
        "inputs": {
            symbol: [str(Path(path).resolve()) for path in paths]
            for symbol, paths in sorted(grouped_paths.items())
        },
    }
    encoded = json.dumps(config, sort_keys=True, ensure_ascii=False).encode()
    return {"fingerprint": hashlib.sha256(encoded).hexdigest(), "config": config}


def prepare_manifest(shard_dir: Path, manifest: dict[str, object]) -> None:
    shard_dir.mkdir(parents=True, exist_ok=True)
    path = shard_dir / "manifest.json"
    if path.exists():
        existing = json.loads(path.read_text())
        if existing.get("fingerprint") != manifest["fingerprint"]:
            raise ValueError(f"shard manifest mismatch: {path}; use a new directory")
        return
    if list(shard_dir.glob("batch_*.csv")):
        raise ValueError(f"shards exist without manifest: {shard_dir}")
    temporary = shard_dir / f".manifest.{os.getpid()}.tmp"
    temporary.write_text(json.dumps(manifest, ensure_ascii=False, indent=2))
    os.replace(temporary, path)


def primitive_worker(
    batch_number: int,
    paths: Sequence[str],
    controls_file: str,
    history_date_from: int,
    date_from: int,
    date_to: int,
    history_days: int,
    fixed_notional: float,
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
        rows = compute_primitives_batch(
            con, paths, controls_file, history_date_from, date_from, date_to,
            history_days, fixed_notional,
        )
        write_tuple_rows(shard_path, PRIMITIVE_FIELDS, rows)
        return batch_number, len(paths), len(rows)
    finally:
        con.close()
        shutil.rmtree(temp_directory, ignore_errors=True)


def build_exposures(
    rows: Sequence[dict[str, object]], *, include_window_return: bool
) -> list[list[float]]:
    continuous_names = [
        "ret5_lagged", "turnover20", "amihud20", "residual_volatility",
        "log_price",
    ]
    if include_window_return:
        continuous_names.insert(0, "window_return")
    continuous_columns: list[list[float]] = []
    for name in continuous_names:
        continuous_columns.append(zscores([float(row[name]) for row in rows]))
    boards = sorted({str(row["board"]) for row in rows})[1:]
    output: list[list[float]] = []
    for index, row in enumerate(rows):
        features = [column[index] for column in continuous_columns]
        features.extend(float(row["board"] == value) for value in boards)
        output.append(features)
    return output


def finalize_factors(
    primitive_path: str,
    controls_file: str,
    factor_date_from: int,
    factor_date_to: int,
    min_cross_section: int,
) -> list[dict[str, object]]:
    controls, _ = load_control_rows(controls_file)
    states: dict[tuple[str, str, str], D05State] = {}
    output: list[dict[str, object]] = []

    def process_group(primitives: Sequence[dict[str, str]]) -> None:
        common_rows: list[dict[str, object]] = []
        for primitive in primitives:
            if primitive["is_valid"].lower() not in ("true", "1"):
                continue
            symbol, date = primitive["symbol"], int(primitive["date"])
            control = controls.get((symbol, date))
            if control is None:
                continue
            values = {
                "ret5_lagged": finite(control["ret5_lagged"]),
                "turnover20": finite(control["turnover20"]),
                "amihud20": finite(control["amihud20"]),
                "residual_volatility": finite(control["residual_volatility"]),
                "log_price": math.log(float(control["close"]))
                    if finite(control["close"]) and float(control["close"]) > 0 else None,
                "window_return": finite(primitive["window_return"]),
            }
            if any(value is None for value in values.values()):
                continue
            common_rows.append({
                "primitive": primitive,
                "symbol": symbol,
                "date": date,
                "frequency": primitive["frequency"],
                "window_name": primitive["window_name"],
                "board": control["board"],
                "threshold_history_days": int(primitive["threshold_history_days"]),
                "threshold_history_order_count": int(
                    float(primitive["threshold_history_order_count"])
                ),
                **{name: float(value) for name, value in values.items()},
            })

        for version in sorted(THRESHOLD_VERSIONS):
            rows: list[dict[str, object]] = []
            for common in common_rows:
                primitive = common["primitive"]
                assert isinstance(primitive, dict)
                alf = finite(primitive[f"{version}_alf"])
                if alf is None:
                    continue
                rows.append({
                    **{name: value for name, value in common.items() if name != "primitive"},
                    "threshold_version": version,
                    "alf_raw": alf,
                    "buy_qty": float(primitive[f"{version}_buy_exec_qty"]),
                    "sell_qty": float(primitive[f"{version}_sell_exec_qty"]),
                })
            rows.sort(key=lambda row: str(row["symbol"]))
            if len(rows) < min_cross_section:
                continue

            clipped = winsorize([float(row["alf_raw"]) for row in rows])
            d04_basis = build_orthonormal_basis(
                build_exposures(rows, include_window_return=True)
            )
            response_basis = build_orthonormal_basis(
                build_exposures(rows, include_window_return=False)
            )
            d04 = residualize(clipped, d04_basis)
            response = residualize(
                [float(row["window_return"]) for row in rows], response_basis
            )
            d04_z = zscores(d04)
            d04_rank = percentile_ranks(d04)
            response_z = zscores(response)
            response_rank = percentile_ranks(response)
            covariance = sum(x * y for x, y in zip(d04, response))
            variance = sum(x * x for x in d04)
            beta = covariance / variance if variance > 0 else 0.0
            model_r2 = r_squared(clipped, d04)

            for index, row in enumerate(rows):
                flow_bucket = quintile(d04_rank[index])
                response_bucket = quintile(response_rank[index])
                event = 1 if flow_bucket == 5 and response_bucket <= 2 else (
                    -1 if flow_bucket == 1 and response_bucket >= 4 else 0
                )
                expected = beta * d04[index]
                key = (
                    str(row["symbol"]),
                    str(row["window_name"]),
                    str(row["threshold_version"]),
                )
                state = states.get(key)
                if state is None:
                    state = D05State()
                    states[key] = state
                d05 = state.update(
                    d04[index],
                    math.log1p(float(row["buy_qty"])),
                    math.log1p(float(row["sell_qty"])),
                )
                date = int(row["date"])
                if factor_date_from <= date <= factor_date_to:
                    output.append({
                        "symbol": row["symbol"], "date": date,
                        "frequency": row["frequency"],
                        "window_name": row["window_name"],
                        "threshold_version": row["threshold_version"],
                        "alf_raw": row["alf_raw"],
                        "alf_winsorized": clipped[index],
                        "d04_residual": d04[index],
                        "d04_z": d04_z[index],
                        "d04_rank_pct": d04_rank[index],
                        "d04_cross_section_n": len(rows),
                        "d04_regression_r2": model_r2,
                        **d05,
                        "price_response_residual": response[index],
                        "d06_flow_bucket": flow_bucket,
                        "d06_response_bucket": response_bucket,
                        "d06_underreaction_event": event,
                        "d06_diff": d04_z[index] - response_z[index],
                        "d06_expected_response": expected,
                        "d06_response_gap": expected - response[index],
                        "d06_daily_beta": beta,
                        "active_large_buy_exec_qty": row["buy_qty"],
                        "active_large_sell_exec_qty": row["sell_qty"],
                        "threshold_history_days": row["threshold_history_days"],
                        "threshold_history_order_count": row[
                            "threshold_history_order_count"
                        ],
                        "is_valid": True,
                        "invalid_reason": "",
                        "factor_version": FACTOR_VERSION,
                    })

    current_key: tuple[int, str] | None = None
    group: list[dict[str, str]] = []
    completed_groups = 0
    with open(primitive_path, newline="") as handle:
        for primitive in csv.DictReader(handle):
            key = (int(primitive["date"]), primitive["window_name"])
            if current_key is not None and key < current_key:
                raise ValueError(
                    "primitive input must be sorted by date and window_name"
                )
            if current_key is not None and key != current_key:
                process_group(group)
                completed_groups += 1
                if completed_groups % 10 == 0:
                    print(
                        f"factor_groups={completed_groups} last_group={current_key}",
                        flush=True,
                    )
                group = []
            current_key = key
            group.append(primitive)
    if group:
        process_group(group)
        completed_groups += 1
        print(
            f"factor_groups={completed_groups} last_group={current_key}",
            flush=True,
        )

    output.sort(key=lambda row: (
        int(row["date"]), str(row["window_name"]),
        str(row["threshold_version"]), str(row["symbol"]),
    ))
    return output


def run_primitives(args: argparse.Namespace) -> None:
    _controls, stock_symbols = load_control_rows(args.controls_file)
    grouped = expand_inputs(args.inputs, stock_symbols)
    symbols = sorted(grouped)
    if args.sample_symbols and args.sample_symbols < len(symbols):
        symbols = sorted(random.Random(args.seed).sample(symbols, args.sample_symbols))
    if args.limit_symbols:
        symbols = symbols[: args.limit_symbols]
    grouped = {symbol: grouped[symbol] for symbol in symbols}
    if not grouped:
        raise SystemExit("no stock parquet inputs matched the explicit A-share universe")
    batches = list(enumerate(chunks(symbols, args.batch_symbols), start=1))
    manifest = build_manifest(grouped, args.controls_file, args)
    shard_dir = Path(args.shard_dir)
    prepare_manifest(shard_dir, manifest)
    pending: list[tuple[int, list[str], Path]] = []
    resumed_symbols = 0
    for batch_number, batch_symbols in batches:
        shard_path = shard_dir / f"batch_{batch_number:06d}.csv"
        paths = [path for symbol in batch_symbols for path in grouped[symbol]]
        if shard_path.exists():
            validate_shard(shard_path)
            resumed_symbols += len(batch_symbols)
        else:
            pending.append((batch_number, paths, shard_path))
    print(
        f"symbols={len(symbols)} batches={len(batches)} "
        f"resumed_symbols={resumed_symbols} pending_batches={len(pending)}",
        flush=True,
    )
    if args.workers == 1:
        for completed, (number, paths, shard_path) in enumerate(pending, start=1):
            result = primitive_worker(
                number, paths, args.controls_file, args.history_date_from,
                args.date_from, args.date_to, args.history_days,
                args.fixed_notional, args.memory_limit, str(shard_path), args.temp_root,
            )
            print(f"completed={completed}/{len(pending)} batch={result[0]} files={result[1]} rows={result[2]}", flush=True)
    else:
        with ProcessPoolExecutor(max_workers=args.workers) as executor:
            futures = {
                executor.submit(
                    primitive_worker, number, paths, args.controls_file,
                    args.history_date_from, args.date_from, args.date_to,
                    args.history_days, args.fixed_notional, args.memory_limit,
                    str(shard_path), args.temp_root,
                ): number
                for number, paths, shard_path in pending
            }
            for completed, future in enumerate(as_completed(futures), start=1):
                result = future.result()
                print(f"completed={completed}/{len(pending)} batch={result[0]} files={result[1]} rows={result[2]}", flush=True)
    rows: list[tuple] = []
    for number, _batch_symbols in batches:
        shard_path = shard_dir / f"batch_{number:06d}.csv"
        if not shard_path.exists():
            raise RuntimeError(f"missing shard: {shard_path}")
        rows.extend(read_tuple_rows(shard_path))
    rows.sort(key=lambda row: (int(row[1]), row[3], row[0]))
    write_tuple_rows(args.primitive_output, PRIMITIVE_FIELDS, rows)
    print(f"primitive_rows={len(rows)} primitive_output={args.primitive_output}", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute daily D04--D06 factors from v4 LOB events.")
    parser.add_argument("stage", choices=("primitives", "factors", "all"), nargs="?", default="all")
    parser.add_argument("inputs", nargs="*", default=[
        "/hdd_data/lob/event_depth10_v4/202507/*.parquet",
        "/hdd_data/lob/event_depth10_v4/202508/*.parquet",
        "/hdd_data/lob/event_depth10_v4/202509/*.parquet",
        "/hdd_data/lob/event_depth10_v4/202510/*.parquet",
        "/hdd_data/lob/event_depth10_v4/202511/*.parquet",
        "/hdd_data/lob/event_depth10_v4/202512/*.parquet",
        "/hdd_data/lob/event_depth10_v4/202601/*.parquet",
    ])
    parser.add_argument("--controls-file", default=str(PROJECT_ROOT / "data/cache/stylized_fact_4_6/d04_d06_controls_202507_202601.csv"))
    parser.add_argument("--history-date-from", type=int, default=20250701)
    parser.add_argument("--date-from", type=int, default=20250801)
    parser.add_argument("--date-to", type=int, default=20260130)
    parser.add_argument("--factor-date-from", type=int, default=20260105)
    parser.add_argument("--factor-date-to", type=int, default=20260130)
    parser.add_argument("--history-days", type=int, default=20)
    parser.add_argument("--fixed-notional", type=float, default=1_000_000.0)
    parser.add_argument("--batch-symbols", type=int, default=1)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--memory-limit", default="8GB")
    parser.add_argument("--limit-symbols", type=int)
    parser.add_argument("--sample-symbols", type=int)
    parser.add_argument("--seed", type=int, default=20260802)
    parser.add_argument("--min-cross-section", type=int, default=100)
    parser.add_argument("--shard-dir", default=str(PROJECT_ROOT / "data/cache/stylized_fact_4_6/g2_d04_d06_shards_202508_202601_no_industry_size_v2"))
    parser.add_argument("--temp-root", default="/tmp/stylized_fact_4_6_d04_d06")
    parser.add_argument("--primitive-output", default=str(PROJECT_ROOT / "data/cache/stylized_fact_4_6/g2_d04_d06_primitives_202508_202601_no_industry_size_v2.csv"))
    parser.add_argument("--factor-output", default=str(PROJECT_ROOT / "data/processed/stylized_fact_4_6/g2_d04_d06_factors_202601_no_industry_size_v2.csv"))
    args = parser.parse_args()
    if args.history_date_from > args.date_from or args.date_from > args.date_to:
        parser.error("require history-date-from <= date-from <= date-to")
    if args.factor_date_from > args.factor_date_to:
        parser.error("factor-date-from must not exceed factor-date-to")
    for name in ("history_days", "batch_symbols", "workers", "min_cross_section"):
        if getattr(args, name) <= 0:
            parser.error(f"{name.replace('_','-')} must be positive")
    return args


def main() -> int:
    args = parse_args()
    if args.stage in ("primitives", "all"):
        run_primitives(args)
    if args.stage in ("factors", "all"):
        rows = finalize_factors(
            args.primitive_output, args.controls_file, args.factor_date_from,
            args.factor_date_to, args.min_cross_section,
        )
        if not rows:
            raise RuntimeError("factor finalization produced no rows")
        write_dict_rows(args.factor_output, FACTOR_FIELDS, rows)
        print(f"factor_rows={len(rows)} factor_output={args.factor_output}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
