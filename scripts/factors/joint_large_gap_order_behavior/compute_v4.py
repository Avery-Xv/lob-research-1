#!/usr/bin/env python3
"""Compute joint V4 large-gap and order-behavior factors with one LOB scan."""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from statistics import median
from typing import Iterable, Sequence

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parents[3]
V4_ROOT = Path("/hdd_data/lob/event_depth10_v4")
CONTINUOUS_SESSIONS = (
    (93000000, 113000000),
    (130000000, 145700000),
)
INTRADAY_START = 100000000
INTRADAY_END = 103000000
FACTOR_VERSION = "joint_large_gap_order_behavior_v4_sh_safe_prebook_20260807"
UNIVERSE_RULE = (
    "point-in-time Shanghai/Shenzhen A shares only; SecuCategory=1; "
    "SecuMarket in (83,90); ETF excluded before factor calculation"
)

FIELDS = [
    "symbol",
    "date",
    "daily_typical_spread_raw",
    "theta_5d_raw",
    "theta_history_days",
    "valid_spread_snapshots",
    "has_theta",
    "daily_total_trade_volume",
    "daily_matched_trade_volume",
    "daily_match_rate",
    "daily_large_gap_buy_volume",
    "daily_large_gap_sell_volume",
    "daily_large_gap_buy_ratio",
    "daily_large_gap_sell_ratio",
    "daily_valid_trade_count",
    "daily_matched_trade_count",
    "daily_large_gap_buy_trade_count",
    "daily_large_gap_sell_trade_count",
    "daily_passes_match_rate",
    "intraday_window_start",
    "intraday_window_end",
    "intraday_total_trade_volume",
    "intraday_matched_trade_volume",
    "intraday_match_rate",
    "intraday_large_gap_buy_volume",
    "intraday_large_gap_sell_volume",
    "intraday_large_gap_buy_ratio",
    "intraday_large_gap_sell_ratio",
    "intraday_valid_trade_count",
    "intraday_matched_trade_count",
    "intraday_large_gap_buy_trade_count",
    "intraday_large_gap_sell_trade_count",
    "intraday_passes_match_rate",
    "ob_trade_qty",
    "ob_trade_count",
    "ob_aggr_order_count",
    "ob_passive_submit_qty",
    "ob_passive_order_count",
    "vr_log",
    "cr_log",
    "single_size_ratio_log",
    "ob_aggressive_order_add_qty_excluded",
    "ob_aggressive_order_add_count_excluded",
    "ob_unidentified_aggr_trade_qty",
    "ob_unidentified_aggr_trade_count",
    "ob_duplicate_trade_rows_excluded",
    "ob_invalid_order_add_count",
    "ob_is_valid",
    "ob_invalid_reason",
    "daily_ob_trade_qty",
    "daily_ob_trade_count",
    "daily_ob_aggr_order_count",
    "daily_ob_passive_submit_qty",
    "daily_ob_passive_order_count",
    "daily_ob_aggressive_order_add_qty_excluded",
    "daily_ob_aggressive_order_add_count_excluded",
    "daily_ob_unidentified_aggr_trade_qty",
    "daily_ob_unidentified_aggr_trade_count",
    "daily_ob_duplicate_trade_rows_excluded",
    "daily_ob_invalid_order_add_count",
    "daily_ob_is_valid",
    "daily_ob_invalid_reason",
    "source_version",
    "universe_rule",
    "factor_version",
]


def calculate_log_factors(
    trade_qty: int,
    aggr_order_count: int,
    passive_submit_qty: int,
    passive_order_count: int,
) -> tuple[float | None, float | None, float | None]:
    vr_log = (
        math.log(trade_qty) - math.log(passive_submit_qty)
        if trade_qty > 0 and passive_submit_qty > 0
        else None
    )
    cr_log = (
        math.log(aggr_order_count) - math.log(passive_order_count)
        if aggr_order_count > 0 and passive_order_count > 0
        else None
    )
    single_size_ratio_log = (
        vr_log - cr_log if vr_log is not None and cr_log is not None else None
    )
    return vr_log, cr_log, single_size_ratio_log


def calculate_strict_theta(
    observations: dict[int, tuple[float, int]],
    calendar_dates: Sequence[int],
    target_months: set[str],
) -> dict[int, tuple[float, float | None, int, int]]:
    """Return spread, strictly lagged five-market-day theta, history count, rows."""
    result: dict[int, tuple[float, float | None, int, int]] = {}
    date_index = {trade_date: index for index, trade_date in enumerate(calendar_dates)}
    for trade_date, (spread, snapshots) in observations.items():
        if str(trade_date)[:6] not in target_months or trade_date not in date_index:
            continue
        index = date_index[trade_date]
        previous_dates = calendar_dates[max(0, index - 5) : index]
        history = [
            observations[previous_date][0]
            for previous_date in previous_dates
            if previous_date in observations
        ]
        theta = median(history) if len(history) == 5 else None
        result[trade_date] = (spread, theta, len(history), snapshots)
    return result


def validate_v4_path(path: str, allowed_months: set[str]) -> tuple[str, str]:
    resolved = Path(path).resolve()
    try:
        relative = resolved.relative_to(V4_ROOT)
    except ValueError as exc:
        raise ValueError(f"input is not under V4 root {V4_ROOT}: {resolved}") from exc
    if len(relative.parts) != 2 or relative.suffix != ".parquet":
        raise ValueError(f"unexpected V4 path layout: {resolved}")
    month, filename = relative.parts
    symbol = Path(filename).stem
    if month not in allowed_months:
        raise ValueError(f"input month {month} is not registered: {resolved}")
    if not (
        len(symbol) == 8
        and symbol[:2] in {"SH", "SZ"}
        and symbol[2:].isdigit()
    ):
        raise ValueError(f"unexpected stock symbol in manifest: {symbol}")
    return symbol, month


def load_inputs(
    file_list: Path,
    universe_metadata: Path,
) -> tuple[dict[str, list[str]], dict[str, object]]:
    metadata = json.loads(universe_metadata.read_text())
    if metadata.get("output_etf_symbols") != 0:
        raise ValueError("universe metadata does not certify zero ETF symbols")
    whitelist = metadata.get("security_type_whitelist", {})
    if whitelist.get("SecuCategory") != [1]:
        raise ValueError("manifest is not certified as SecuCategory=1 A shares")
    allowed_months = set(metadata.get("months", []))
    if not allowed_months:
        raise ValueError("universe metadata has no months")
    by_symbol: dict[str, list[str]] = {}
    for line in file_list.read_text().splitlines():
        path = line.strip()
        if not path:
            continue
        symbol, _ = validate_v4_path(path, allowed_months)
        by_symbol.setdefault(symbol, []).append(str(Path(path).resolve()))
    if not by_symbol:
        raise ValueError("empty V4 stock manifest")
    for paths in by_symbol.values():
        paths.sort(key=lambda value: Path(value).parent.name)
    return by_symbol, metadata


def load_calendar(path: Path) -> list[int]:
    dates = []
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        if "date" not in (reader.fieldnames or []):
            raise ValueError(f"calendar must contain a date column: {path}")
        for row in reader:
            value = row["date"].replace("-", "")
            dates.append(int(value))
    dates = sorted(dict.fromkeys(dates))
    if not dates:
        raise ValueError("empty trading calendar")
    return dates


def sql_list_literal(paths: Sequence[str]) -> str:
    return "[" + ",".join("'" + path.replace("'", "''") + "'" for path in paths) + "]"


def configure_connection(memory_limit: str) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("PRAGMA threads=1")
    con.execute(f"PRAGMA memory_limit='{memory_limit}'")
    temp_directory = Path("/tmp") / f"joint_v4_duckdb_{os.getpid()}"
    temp_directory.mkdir(parents=True, exist_ok=True)
    escaped = str(temp_directory).replace("'", "''")
    con.execute(f"PRAGMA temp_directory='{escaped}'")
    con.execute("PRAGMA preserve_insertion_order=false")
    return con


def compute_one(
    symbol: str,
    paths: Sequence[str],
    calendar_dates: Sequence[int],
    target_months: set[str],
    minimum_match_rate: float,
    memory_limit: str,
) -> tuple[str, list[tuple]]:
    con = configure_connection(memory_limit)
    path_literal = sql_list_literal(paths)
    try:
        # This is the only raw Parquet scan. All downstream branches read the
        # materialized scalar event table.
        con.execute(
            f"""
            CREATE TEMP TABLE raw_events AS
            SELECT
                '{symbol}'::VARCHAR AS symbol,
                date,
                time,
                row_id,
                source_action,
                source_recid,
                source_buy_order_id,
                source_sell_order_id,
                source_side,
                source_price,
                source_volume,
                CASE WHEN array_length(bid_px) > 0 THEN bid_px[1] END::DOUBLE AS bid1,
                CASE WHEN array_length(ask_px) > 0 THEN ask_px[1] END::DOUBLE AS ask1
            FROM read_parquet({path_literal})
            WHERE (time >= 93000000 AND time < 113000000)
               OR (time >= 130000000 AND time < 145700000)
            """
        )
        spread_rows = con.execute(
            """
            SELECT date,
                   median((ask1 - bid1)::DOUBLE) AS daily_typical_spread_raw,
                   count(*)::BIGINT AS valid_spread_snapshots
            FROM raw_events
            WHERE bid1 > 0 AND ask1 > bid1
            GROUP BY date
            ORDER BY date
            """
        ).fetchall()
        observations = {
            int(row[0]): (float(row[1]), int(row[2])) for row in spread_rows
        }
        theta = calculate_strict_theta(observations, calendar_dates, target_months)
        if not theta:
            return symbol, []
        con.execute(
            """
            CREATE TEMP TABLE theta(
                date INTEGER,
                daily_typical_spread_raw DOUBLE,
                theta_5d_raw DOUBLE,
                theta_history_days INTEGER,
                valid_spread_snapshots BIGINT
            )
            """
        )
        con.executemany(
            "INSERT INTO theta VALUES (?, ?, ?, ?, ?)",
            [
                (trade_date, spread, value, history_days, snapshots)
                for trade_date, (spread, value, history_days, snapshots) in theta.items()
            ],
        )

        con.execute(
            """
            CREATE TEMP TABLE continuous AS
            SELECT *,
                   last_value(
                       CASE WHEN bid1 > 0 AND ask1 > bid1 THEN bid1 END IGNORE NULLS
                   ) OVER (
                       PARTITION BY date,
                           CASE WHEN time < 120000000 THEN 'AM' ELSE 'PM' END
                       ORDER BY row_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                   ) AS pre_bid1,
                   last_value(
                       CASE WHEN bid1 > 0 AND ask1 > bid1 THEN ask1 END IGNORE NULLS
                   ) OVER (
                       PARTITION BY date,
                           CASE WHEN time < 120000000 THEN 'AM' ELSE 'PM' END
                       ORDER BY row_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                   ) AS pre_ask1
            FROM raw_events
            """
        )
        con.execute(
            """
            CREATE TEMP TABLE resting_orders AS
            SELECT * EXCLUDE (occurrence)
            FROM (
                SELECT
                    date,
                    row_id AS entry_row_id,
                    source_side AS order_side,
                    CASE
                        WHEN source_side = 'B' THEN source_buy_order_id
                        WHEN source_side = 'S' THEN source_sell_order_id
                    END AS order_id,
                    CASE
                        WHEN source_side = 'B' AND pre_bid1 > 0
                            THEN pre_bid1 - source_price
                        WHEN source_side = 'S' AND pre_ask1 > 0
                            THEN source_price - pre_ask1
                    END AS initial_gap,
                    row_number() OVER (
                        PARTITION BY date, source_side,
                            CASE
                                WHEN source_side = 'B' THEN source_buy_order_id
                                WHEN source_side = 'S' THEN source_sell_order_id
                            END
                        ORDER BY row_id
                    ) AS occurrence
                FROM continuous
                WHERE source_action = 'ORDER_ADD'
                  AND source_side IN ('B', 'S')
                  AND source_price > 0
                  AND CASE
                        WHEN source_side = 'B' THEN source_buy_order_id
                        WHEN source_side = 'S' THEN source_sell_order_id
                      END IS NOT NULL
            )
            WHERE occurrence = 1
            """
        )
        con.execute(
            """
            CREATE TEMP TABLE deduplicated_trades AS
            SELECT * EXCLUDE (trade_occurrence)
            FROM (
                SELECT
                    *,
                    row_number() OVER (
                        PARTITION BY date, coalesce(source_recid, -row_id)
                        ORDER BY row_id
                    ) AS trade_occurrence
                FROM continuous
                WHERE source_action = 'TRADE'
                  AND source_volume > 0
            )
            WHERE trade_occurrence = 1
            """
        )
        con.execute(
            """
            CREATE TEMP TABLE matched_trades AS
            SELECT
                t.date,
                t.time AS trade_time,
                t.row_id,
                t.source_recid,
                t.source_side AS active_side,
                t.source_volume,
                CASE
                    WHEN t.source_side = 'B' THEN t.source_buy_order_id
                    WHEN t.source_side = 'S' THEN t.source_sell_order_id
                END AS active_order_id,
                o.initial_gap,
                th.theta_5d_raw,
                o.order_id IS NOT NULL AS is_matched
            FROM deduplicated_trades t
            LEFT JOIN theta th USING (date)
            LEFT JOIN resting_orders o
              ON o.date = t.date
             AND o.order_id = CASE
                    WHEN t.source_side = 'B' THEN t.source_sell_order_id
                    WHEN t.source_side = 'S' THEN t.source_buy_order_id
                 END
             AND o.order_side = CASE
                    WHEN t.source_side = 'B' THEN 'S'
                    WHEN t.source_side = 'S' THEN 'B'
                 END
             AND o.entry_row_id < t.row_id
            WHERE t.source_side IN ('B', 'S')
            """
        )
        con.execute(
            f"""
            CREATE TEMP TABLE large_gap_agg AS
            SELECT
                date,
                sum(source_volume)::BIGINT AS daily_total_trade_volume,
                coalesce(sum(source_volume) FILTER (WHERE is_matched), 0)::BIGINT
                    AS daily_matched_trade_volume,
                coalesce(sum(source_volume) FILTER (WHERE is_matched), 0)::DOUBLE
                    / nullif(sum(source_volume), 0) AS daily_match_rate,
                coalesce(sum(source_volume) FILTER (
                    WHERE is_matched AND active_side = 'B'
                      AND initial_gap > theta_5d_raw
                ), 0)::BIGINT AS daily_large_gap_buy_volume,
                coalesce(sum(source_volume) FILTER (
                    WHERE is_matched AND active_side = 'S'
                      AND initial_gap > theta_5d_raw
                ), 0)::BIGINT AS daily_large_gap_sell_volume,
                CASE WHEN any_value(theta_5d_raw) IS NOT NULL THEN
                    coalesce(sum(source_volume) FILTER (
                        WHERE is_matched AND active_side = 'B'
                          AND initial_gap > theta_5d_raw
                    ), 0)::DOUBLE / nullif(sum(source_volume), 0)
                END AS daily_large_gap_buy_ratio,
                CASE WHEN any_value(theta_5d_raw) IS NOT NULL THEN
                    coalesce(sum(source_volume) FILTER (
                        WHERE is_matched AND active_side = 'S'
                          AND initial_gap > theta_5d_raw
                    ), 0)::DOUBLE / nullif(sum(source_volume), 0)
                END AS daily_large_gap_sell_ratio,
                count(*)::BIGINT AS daily_valid_trade_count,
                count(*) FILTER (WHERE is_matched)::BIGINT
                    AS daily_matched_trade_count,
                CASE WHEN any_value(theta_5d_raw) IS NOT NULL THEN count(*) FILTER (
                    WHERE is_matched AND active_side = 'B'
                      AND initial_gap > theta_5d_raw
                ) END::BIGINT AS daily_large_gap_buy_trade_count,
                CASE WHEN any_value(theta_5d_raw) IS NOT NULL THEN count(*) FILTER (
                    WHERE is_matched AND active_side = 'S'
                      AND initial_gap > theta_5d_raw
                ) END::BIGINT AS daily_large_gap_sell_trade_count,
                daily_match_rate >= {minimum_match_rate} AS daily_passes_match_rate,
                sum(source_volume) FILTER (
                    WHERE trade_time >= {INTRADAY_START} AND trade_time < {INTRADAY_END}
                )::BIGINT AS intraday_total_trade_volume,
                coalesce(sum(source_volume) FILTER (
                    WHERE trade_time >= {INTRADAY_START} AND trade_time < {INTRADAY_END}
                      AND is_matched
                ), 0)::BIGINT AS intraday_matched_trade_volume,
                coalesce(sum(source_volume) FILTER (
                    WHERE trade_time >= {INTRADAY_START} AND trade_time < {INTRADAY_END}
                      AND is_matched
                ), 0)::DOUBLE / nullif(sum(source_volume) FILTER (
                    WHERE trade_time >= {INTRADAY_START} AND trade_time < {INTRADAY_END}
                ), 0) AS intraday_match_rate,
                coalesce(sum(source_volume) FILTER (
                    WHERE trade_time >= {INTRADAY_START} AND trade_time < {INTRADAY_END}
                      AND is_matched AND active_side = 'B'
                      AND initial_gap > theta_5d_raw
                ), 0)::BIGINT AS intraday_large_gap_buy_volume,
                coalesce(sum(source_volume) FILTER (
                    WHERE trade_time >= {INTRADAY_START} AND trade_time < {INTRADAY_END}
                      AND is_matched AND active_side = 'S'
                      AND initial_gap > theta_5d_raw
                ), 0)::BIGINT AS intraday_large_gap_sell_volume,
                CASE WHEN any_value(theta_5d_raw) IS NOT NULL THEN
                    intraday_large_gap_buy_volume::DOUBLE
                        / nullif(intraday_total_trade_volume, 0)
                END AS intraday_large_gap_buy_ratio,
                CASE WHEN any_value(theta_5d_raw) IS NOT NULL THEN
                    intraday_large_gap_sell_volume::DOUBLE
                        / nullif(intraday_total_trade_volume, 0)
                END AS intraday_large_gap_sell_ratio,
                count(*) FILTER (
                    WHERE trade_time >= {INTRADAY_START} AND trade_time < {INTRADAY_END}
                )::BIGINT AS intraday_valid_trade_count,
                count(*) FILTER (
                    WHERE trade_time >= {INTRADAY_START} AND trade_time < {INTRADAY_END}
                      AND is_matched
                )::BIGINT AS intraday_matched_trade_count,
                CASE WHEN any_value(theta_5d_raw) IS NOT NULL THEN count(*) FILTER (
                    WHERE trade_time >= {INTRADAY_START} AND trade_time < {INTRADAY_END}
                      AND is_matched AND active_side = 'B'
                      AND initial_gap > theta_5d_raw
                ) END::BIGINT AS intraday_large_gap_buy_trade_count,
                CASE WHEN any_value(theta_5d_raw) IS NOT NULL THEN count(*) FILTER (
                    WHERE trade_time >= {INTRADAY_START} AND trade_time < {INTRADAY_END}
                      AND is_matched AND active_side = 'S'
                      AND initial_gap > theta_5d_raw
                ) END::BIGINT AS intraday_large_gap_sell_trade_count,
                intraday_match_rate >= {minimum_match_rate}
                    AS intraday_passes_match_rate
            FROM matched_trades
            GROUP BY date
            """
        )

        con.execute(
            f"""
            CREATE TEMP TABLE ob_windows AS
            SELECT * FROM (VALUES
                ('intraday', 93000000::BIGINT, {INTRADAY_END}::BIGINT,
                 {INTRADAY_START}::BIGINT, {INTRADAY_END}::BIGINT),
                ('daily', 93000000::BIGINT, 145700000::BIGINT,
                 93000000::BIGINT, 145700000::BIGINT)
            ) AS t(window_name, knowledge_start, knowledge_end, agg_start, agg_end)
            """
        )
        con.execute(
            """
            CREATE TEMP TABLE ob_active_ids AS
            SELECT DISTINCT
                w.window_name,
                t.date,
                t.source_side AS active_side,
                CASE
                    WHEN t.source_side = 'B' THEN t.source_buy_order_id
                    WHEN t.source_side = 'S' THEN t.source_sell_order_id
                END AS active_order_id
            FROM ob_windows w
            JOIN deduplicated_trades t
              ON t.time >= w.knowledge_start AND t.time < w.knowledge_end
            WHERE CASE
                    WHEN t.source_side = 'B' THEN t.source_buy_order_id
                    WHEN t.source_side = 'S' THEN t.source_sell_order_id
                  END IS NOT NULL
            """
        )
        con.execute(
            """
            CREATE TEMP TABLE ob_order_rows AS
            SELECT * EXCLUDE (order_occurrence)
            FROM (
                SELECT
                    w.window_name,
                    e.date,
                    e.source_side AS order_side,
                    e.source_volume,
                    CASE
                        WHEN e.source_side = 'B' THEN e.source_buy_order_id
                        WHEN e.source_side = 'S' THEN e.source_sell_order_id
                    END AS order_id,
                    row_number() OVER (
                        PARTITION BY w.window_name, e.date, e.source_side,
                            CASE
                                WHEN e.source_side = 'B' THEN e.source_buy_order_id
                                WHEN e.source_side = 'S' THEN e.source_sell_order_id
                            END
                        ORDER BY e.row_id
                    ) AS order_occurrence
                FROM ob_windows w
                JOIN raw_events e
                  ON e.time >= w.agg_start AND e.time < w.agg_end
                WHERE e.source_action = 'ORDER_ADD'
            )
            WHERE order_occurrence = 1
            """
        )
        con.execute(
            """
            CREATE TEMP TABLE ob_metrics AS
            WITH window_trades AS (
                SELECT
                    w.window_name,
                    t.*,
                    CASE
                        WHEN t.source_side = 'B' THEN t.source_buy_order_id
                        WHEN t.source_side = 'S' THEN t.source_sell_order_id
                    END AS active_order_id
                FROM ob_windows w
                JOIN deduplicated_trades t
                  ON t.time >= w.agg_start AND t.time < w.agg_end
            ),
            trade_agg AS (
                SELECT
                    window_name,
                    date,
                    sum(source_volume)::BIGINT AS trade_qty,
                    count(*)::BIGINT AS trade_count,
                    count(DISTINCT (source_side, active_order_id))::BIGINT AS aggr_order_count,
                    coalesce(sum(source_volume) FILTER (
                        WHERE active_order_id IS NULL
                    ), 0)::BIGINT AS unidentified_aggr_trade_qty,
                    count(*) FILTER (
                        WHERE active_order_id IS NULL
                    )::BIGINT AS unidentified_aggr_trade_count
                FROM window_trades
                GROUP BY window_name, date
            ),
            order_agg AS (
                SELECT
                    o.window_name,
                    o.date,
                    coalesce(sum(o.source_volume) FILTER (
                        WHERE a.active_order_id IS NULL
                          AND o.order_id IS NOT NULL AND o.source_volume > 0
                    ), 0)::BIGINT AS passive_submit_qty,
                    count(*) FILTER (
                        WHERE a.active_order_id IS NULL
                          AND o.order_id IS NOT NULL AND o.source_volume > 0
                    )::BIGINT AS passive_order_count,
                    coalesce(sum(o.source_volume) FILTER (
                        WHERE a.active_order_id IS NOT NULL
                          AND o.order_id IS NOT NULL AND o.source_volume > 0
                    ), 0)::BIGINT AS aggressive_order_add_qty_excluded,
                    count(*) FILTER (
                        WHERE a.active_order_id IS NOT NULL
                          AND o.order_id IS NOT NULL AND o.source_volume > 0
                    )::BIGINT AS aggressive_order_add_count_excluded,
                    count(*) FILTER (
                        WHERE o.order_id IS NULL OR o.source_volume IS NULL
                           OR o.source_volume <= 0
                    )::BIGINT AS invalid_order_add_count
                FROM ob_order_rows o
                LEFT JOIN ob_active_ids a
                  ON a.window_name = o.window_name
                 AND a.date = o.date
                 AND a.active_side = o.order_side
                 AND a.active_order_id = o.order_id
                GROUP BY o.window_name, o.date
            ),
            dates AS (
                SELECT window_name, date FROM trade_agg
                UNION
                SELECT window_name, date FROM order_agg
            )
            SELECT
                d.window_name,
                d.date,
                coalesce(t.trade_qty, 0)::BIGINT AS trade_qty,
                coalesce(t.trade_count, 0)::BIGINT AS trade_count,
                coalesce(t.aggr_order_count, 0)::BIGINT AS aggr_order_count,
                coalesce(o.passive_submit_qty, 0)::BIGINT AS passive_submit_qty,
                coalesce(o.passive_order_count, 0)::BIGINT AS passive_order_count,
                coalesce(o.aggressive_order_add_qty_excluded, 0)::BIGINT
                    AS aggressive_order_add_qty_excluded,
                coalesce(o.aggressive_order_add_count_excluded, 0)::BIGINT
                    AS aggressive_order_add_count_excluded,
                coalesce(t.unidentified_aggr_trade_qty, 0)::BIGINT
                    AS unidentified_aggr_trade_qty,
                coalesce(t.unidentified_aggr_trade_count, 0)::BIGINT
                    AS unidentified_aggr_trade_count,
                0::BIGINT AS duplicate_trade_rows_excluded,
                coalesce(o.invalid_order_add_count, 0)::BIGINT
                    AS invalid_order_add_count
            FROM dates d
            LEFT JOIN trade_agg t USING (window_name, date)
            LEFT JOIN order_agg o USING (window_name, date)
            """
        )

        rows = con.execute(
            f"""
            WITH target_dates AS (
                SELECT * FROM theta
            ),
            ob AS (
                SELECT
                    date,
                    max(trade_qty) FILTER (WHERE window_name = 'intraday') AS ob_trade_qty,
                    max(trade_count) FILTER (WHERE window_name = 'intraday') AS ob_trade_count,
                    max(aggr_order_count) FILTER (WHERE window_name = 'intraday') AS ob_aggr_order_count,
                    max(passive_submit_qty) FILTER (WHERE window_name = 'intraday') AS ob_passive_submit_qty,
                    max(passive_order_count) FILTER (WHERE window_name = 'intraday') AS ob_passive_order_count,
                    max(aggressive_order_add_qty_excluded) FILTER (WHERE window_name = 'intraday') AS ob_aggressive_order_add_qty_excluded,
                    max(aggressive_order_add_count_excluded) FILTER (WHERE window_name = 'intraday') AS ob_aggressive_order_add_count_excluded,
                    max(unidentified_aggr_trade_qty) FILTER (WHERE window_name = 'intraday') AS ob_unidentified_aggr_trade_qty,
                    max(unidentified_aggr_trade_count) FILTER (WHERE window_name = 'intraday') AS ob_unidentified_aggr_trade_count,
                    max(duplicate_trade_rows_excluded) FILTER (WHERE window_name = 'intraday') AS ob_duplicate_trade_rows_excluded,
                    max(invalid_order_add_count) FILTER (WHERE window_name = 'intraday') AS ob_invalid_order_add_count,
                    max(trade_qty) FILTER (WHERE window_name = 'daily') AS daily_ob_trade_qty,
                    max(trade_count) FILTER (WHERE window_name = 'daily') AS daily_ob_trade_count,
                    max(aggr_order_count) FILTER (WHERE window_name = 'daily') AS daily_ob_aggr_order_count,
                    max(passive_submit_qty) FILTER (WHERE window_name = 'daily') AS daily_ob_passive_submit_qty,
                    max(passive_order_count) FILTER (WHERE window_name = 'daily') AS daily_ob_passive_order_count,
                    max(aggressive_order_add_qty_excluded) FILTER (WHERE window_name = 'daily') AS daily_ob_aggressive_order_add_qty_excluded,
                    max(aggressive_order_add_count_excluded) FILTER (WHERE window_name = 'daily') AS daily_ob_aggressive_order_add_count_excluded,
                    max(unidentified_aggr_trade_qty) FILTER (WHERE window_name = 'daily') AS daily_ob_unidentified_aggr_trade_qty,
                    max(unidentified_aggr_trade_count) FILTER (WHERE window_name = 'daily') AS daily_ob_unidentified_aggr_trade_count,
                    max(duplicate_trade_rows_excluded) FILTER (WHERE window_name = 'daily') AS daily_ob_duplicate_trade_rows_excluded,
                    max(invalid_order_add_count) FILTER (WHERE window_name = 'daily') AS daily_ob_invalid_order_add_count
                FROM ob_metrics
                GROUP BY date
            )
            SELECT
                '{symbol}' AS symbol,
                d.date,
                d.daily_typical_spread_raw,
                d.theta_5d_raw,
                d.theta_history_days,
                d.valid_spread_snapshots,
                d.theta_5d_raw IS NOT NULL AS has_theta,
                g.daily_total_trade_volume,
                g.daily_matched_trade_volume,
                g.daily_match_rate,
                g.daily_large_gap_buy_volume,
                g.daily_large_gap_sell_volume,
                g.daily_large_gap_buy_ratio,
                g.daily_large_gap_sell_ratio,
                g.daily_valid_trade_count,
                g.daily_matched_trade_count,
                g.daily_large_gap_buy_trade_count,
                g.daily_large_gap_sell_trade_count,
                g.daily_passes_match_rate,
                {INTRADAY_START}::BIGINT AS intraday_window_start,
                {INTRADAY_END}::BIGINT AS intraday_window_end,
                g.intraday_total_trade_volume,
                g.intraday_matched_trade_volume,
                g.intraday_match_rate,
                g.intraday_large_gap_buy_volume,
                g.intraday_large_gap_sell_volume,
                g.intraday_large_gap_buy_ratio,
                g.intraday_large_gap_sell_ratio,
                g.intraday_valid_trade_count,
                g.intraday_matched_trade_count,
                g.intraday_large_gap_buy_trade_count,
                g.intraday_large_gap_sell_trade_count,
                g.intraday_passes_match_rate,
                o.ob_trade_qty,
                o.ob_trade_count,
                o.ob_aggr_order_count,
                o.ob_passive_submit_qty,
                o.ob_passive_order_count,
                CASE WHEN o.ob_trade_qty > 0 AND o.ob_passive_submit_qty > 0
                    THEN ln(o.ob_trade_qty::DOUBLE) - ln(o.ob_passive_submit_qty::DOUBLE)
                END AS vr_log,
                CASE WHEN o.ob_aggr_order_count > 0 AND o.ob_passive_order_count > 0
                    THEN ln(o.ob_aggr_order_count::DOUBLE) - ln(o.ob_passive_order_count::DOUBLE)
                END AS cr_log,
                CASE WHEN o.ob_trade_qty > 0 AND o.ob_passive_submit_qty > 0
                       AND o.ob_aggr_order_count > 0 AND o.ob_passive_order_count > 0
                    THEN (ln(o.ob_trade_qty::DOUBLE) - ln(o.ob_passive_submit_qty::DOUBLE))
                       - (ln(o.ob_aggr_order_count::DOUBLE) - ln(o.ob_passive_order_count::DOUBLE))
                END AS single_size_ratio_log,
                o.ob_aggressive_order_add_qty_excluded,
                o.ob_aggressive_order_add_count_excluded,
                o.ob_unidentified_aggr_trade_qty,
                o.ob_unidentified_aggr_trade_count,
                o.ob_duplicate_trade_rows_excluded,
                o.ob_invalid_order_add_count,
                o.ob_trade_qty > 0 AND o.ob_aggr_order_count > 0
                    AND o.ob_passive_submit_qty > 0 AND o.ob_passive_order_count > 0
                    AND o.ob_unidentified_aggr_trade_count = 0
                    AND o.ob_invalid_order_add_count = 0 AS ob_is_valid,
                concat_ws(';' ,
                    CASE WHEN o.ob_trade_qty = 0 THEN 'zero_trade_qty' END,
                    CASE WHEN o.ob_aggr_order_count = 0 THEN 'zero_aggr_order_count' END,
                    CASE WHEN o.ob_passive_submit_qty = 0 THEN 'zero_passive_submit_qty' END,
                    CASE WHEN o.ob_passive_order_count = 0 THEN 'zero_passive_order_count' END,
                    CASE WHEN o.ob_unidentified_aggr_trade_count > 0 THEN 'unidentified_aggressor' END,
                    CASE WHEN o.ob_invalid_order_add_count > 0 THEN 'invalid_order_adds_present' END
                ) AS ob_invalid_reason,
                o.daily_ob_trade_qty,
                o.daily_ob_trade_count,
                o.daily_ob_aggr_order_count,
                o.daily_ob_passive_submit_qty,
                o.daily_ob_passive_order_count,
                o.daily_ob_aggressive_order_add_qty_excluded,
                o.daily_ob_aggressive_order_add_count_excluded,
                o.daily_ob_unidentified_aggr_trade_qty,
                o.daily_ob_unidentified_aggr_trade_count,
                o.daily_ob_duplicate_trade_rows_excluded,
                o.daily_ob_invalid_order_add_count,
                o.daily_ob_trade_qty > 0 AND o.daily_ob_aggr_order_count > 0
                    AND o.daily_ob_passive_submit_qty > 0 AND o.daily_ob_passive_order_count > 0
                    AND o.daily_ob_unidentified_aggr_trade_count = 0
                    AND o.daily_ob_invalid_order_add_count = 0 AS daily_ob_is_valid,
                concat_ws(';' ,
                    CASE WHEN o.daily_ob_trade_qty = 0 THEN 'zero_trade_qty' END,
                    CASE WHEN o.daily_ob_aggr_order_count = 0 THEN 'zero_aggr_order_count' END,
                    CASE WHEN o.daily_ob_passive_submit_qty = 0 THEN 'zero_passive_submit_qty' END,
                    CASE WHEN o.daily_ob_passive_order_count = 0 THEN 'zero_passive_order_count' END,
                    CASE WHEN o.daily_ob_unidentified_aggr_trade_count > 0 THEN 'unidentified_aggressor' END,
                    CASE WHEN o.daily_ob_invalid_order_add_count > 0 THEN 'invalid_order_adds_present' END
                ) AS daily_ob_invalid_reason,
                'event_depth10_v4' AS source_version,
                '{UNIVERSE_RULE}' AS universe_rule,
                '{FACTOR_VERSION}' AS factor_version
            FROM target_dates d
            LEFT JOIN large_gap_agg g USING (date)
            LEFT JOIN ob o USING (date)
            ORDER BY d.date
            """
        ).fetchall()
        return symbol, rows
    finally:
        con.close()


def completed_symbols(output: Path) -> set[str]:
    if not output.exists():
        return set()
    completed = set()
    with output.open(newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != FIELDS:
            raise ValueError(f"incompatible output header: {output}")
        for row in reader:
            completed.add(row["symbol"])
    return completed


def append_rows(output: Path, rows: Iterable[Sequence[object]], write_header: bool) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w" if write_header else "a", newline="") as handle:
        writer = csv.writer(handle)
        if write_header:
            writer.writerow(FIELDS)
        writer.writerows(rows)
        handle.flush()
        os.fsync(handle.fileno())


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Compute daily/intraday B/S large-gap ratios and intraday "
            "VR/CR/single-size ratios from one V4 scan per stock."
        )
    )
    parser.add_argument("--file-list", type=Path, required=True)
    parser.add_argument("--universe-metadata", type=Path, required=True)
    parser.add_argument("--calendar", type=Path, required=True)
    parser.add_argument("--target-months", nargs="+", required=True)
    parser.add_argument("--exchange", choices=("ALL", "SH", "SZ"), default="ALL")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--metadata-output", type=Path)
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--memory-limit-per-worker", default="4GB")
    parser.add_argument("--minimum-match-rate", type=float, default=0.95)
    parser.add_argument("--symbols", nargs="+")
    parser.add_argument("--limit-symbols", type=int)
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()

    if args.workers < 1:
        raise ValueError("workers must be at least 1")
    if not 0 <= args.minimum_match_rate <= 1:
        raise ValueError("minimum-match-rate must be in [0, 1]")
    target_months = set(args.target_months)
    by_symbol, universe_metadata = load_inputs(
        args.file_list, args.universe_metadata
    )
    manifest_months = set(universe_metadata["months"])
    if not target_months <= manifest_months:
        raise ValueError("target months are not all present in the manifest")
    if args.exchange != "ALL":
        by_symbol = {
            symbol: paths for symbol, paths in by_symbol.items()
            if symbol.startswith(args.exchange)
        }
    if args.symbols:
        selected = set(args.symbols)
        by_symbol = {
            symbol: paths for symbol, paths in by_symbol.items() if symbol in selected
        }
        missing = selected - set(by_symbol)
        if missing:
            raise ValueError(f"symbols missing from manifest: {sorted(missing)}")
    tasks = sorted(
        by_symbol.items(),
        key=lambda item: (sum(Path(path).stat().st_size for path in item[1]), item[0]),
    )
    if args.limit_symbols is not None:
        tasks = tasks[: args.limit_symbols]
    done = completed_symbols(args.output) if args.resume else set()
    tasks = [(symbol, paths) for symbol, paths in tasks if symbol not in done]
    calendar_dates = load_calendar(args.calendar)
    write_header = not (args.resume and args.output.exists())
    started = time.perf_counter()
    factor_rows = 0

    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        futures = [
            executor.submit(
                compute_one,
                symbol,
                paths,
                calendar_dates,
                target_months,
                args.minimum_match_rate,
                args.memory_limit_per_worker,
            )
            for symbol, paths in tasks
        ]
        for index, future in enumerate(as_completed(futures), start=1):
            symbol, rows = future.result()
            append_rows(args.output, rows, write_header)
            write_header = False
            factor_rows += len(rows)
            if index % 25 == 0 or index == len(tasks):
                print(
                    f"done={index}/{len(tasks)} rows={factor_rows} symbol={symbol} "
                    f"elapsed_sec={time.perf_counter()-started:.1f}",
                    flush=True,
                )

    metadata_output = args.metadata_output or args.output.with_suffix(".metadata.json")
    metadata = {
        "created_at": datetime.now().astimezone().isoformat(),
        "factor_version": FACTOR_VERSION,
        "source_version": "event_depth10_v4",
        "target_months": sorted(target_months),
        "exchange": args.exchange,
        "input_months": universe_metadata["months"],
        "input_manifest": str(args.file_list.resolve()),
        "universe_metadata": str(args.universe_metadata.resolve()),
        "universe_rule": UNIVERSE_RULE,
        "output_etf_symbols": 0,
        "theta_rule": "median daily quoted spread over exactly prior 5 market dates",
        "intraday_window": [INTRADAY_START, INTRADAY_END],
        "daily_window": "09:30-11:30 plus 13:00-14:57 continuous auction",
        "minimum_match_rate": args.minimum_match_rate,
        "workers": args.workers,
        "output": str(args.output.resolve()),
        "rows_written_this_run": factor_rows,
        "symbols_completed_this_run": len(tasks),
    }
    metadata_output.parent.mkdir(parents=True, exist_ok=True)
    metadata_output.write_text(json.dumps(metadata, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps(metadata, ensure_ascii=False), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
