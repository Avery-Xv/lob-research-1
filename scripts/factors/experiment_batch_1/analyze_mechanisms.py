#!/usr/bin/env python3
"""Aggregate first-layer mechanism evidence from experiment_batch_1.

This script deliberately does not join post-signal returns.  It consolidates
stock-day signals, active-order chain structure, quote-improvement lifecycles,
and quality diagnostics.  Price is observed at 10:30; market capitalisation is
strictly lagged by one trading observation within symbol.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parents[3]


def sql_path(path: Path | str) -> str:
    return str(path).replace("'", "''")


def copy_csv(connection: duckdb.DuckDBPyConnection, query: str, output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    connection.execute(
        f"COPY ({query}) TO '{sql_path(output)}' "
        "(FORMAT CSV, HEADER TRUE, DELIMITER ',')"
    )


def copy_parquet(connection: duckdb.DuckDBPyConnection, query: str, output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    connection.execute(
        f"COPY ({query}) TO '{sql_path(output)}' "
        "(FORMAT PARQUET, COMPRESSION ZSTD)"
    )


def build_tables(
    connection: duckdb.DuckDBPyConnection,
    shard_root: Path,
    cache_dir: Path,
) -> None:
    signal_glob = sql_path(shard_root / "batch_*" / "signals.csv")
    quality_glob = sql_path(shard_root / "batch_*" / "quality.csv")
    chain_glob = sql_path(shard_root / "batch_*" / "active_order_chains.csv")
    quote_glob = sql_path(shard_root / "batch_*" / "quote_lifecycles.csv")

    connection.execute(f"""
        CREATE OR REPLACE TABLE signals AS
        SELECT * FROM read_csv_auto(
            '{signal_glob}', header=true, union_by_name=true, sample_size=100000
        )
    """)
    connection.execute(f"""
        CREATE OR REPLACE TABLE quality AS
        SELECT * FROM read_csv_auto(
            '{quality_glob}', header=true, union_by_name=true, sample_size=100000
        )
    """)

    january_price = sql_path(PROJECT_ROOT / "data/cache/min1_close_1030_202601.csv")
    later_price = sql_path(PROJECT_ROOT / "data/cache/min1_ret_1000_1030_202602_202604.csv")
    connection.execute(f"""
        CREATE OR REPLACE TABLE prices AS
        SELECT symbol, date::BIGINT AS date, start_mid::DOUBLE AS price_1030
        FROM read_csv_auto('{january_price}', header=true)
        WHERE date BETWEEN 20260101 AND 20260131
        UNION ALL
        SELECT symbol, date::BIGINT AS date, close_1030::DOUBLE AS price_1030
        FROM read_csv_auto('{later_price}', header=true)
        WHERE date BETWEEN 20260201 AND 20260228
    """)

    cap_early = sql_path(PROJECT_ROOT / "data/cache/daily_market_cap_202512_202601.csv")
    cap_later = sql_path(PROJECT_ROOT / "data/cache/daily_market_cap_202601_202604.csv")
    connection.execute(f"""
        CREATE OR REPLACE TABLE caps AS
        WITH raw AS (
            SELECT symbol, date::BIGINT AS date, total_mv::DOUBLE AS total_mv, 1 AS source_order
            FROM read_csv_auto('{cap_early}', header=true)
            WHERE date BETWEEN 20251201 AND 20260131
            UNION ALL
            SELECT symbol, date::BIGINT AS date, total_mv::DOUBLE AS total_mv, 2 AS source_order
            FROM read_csv_auto('{cap_later}', header=true)
            WHERE date BETWEEN 20260101 AND 20260228
        ), deduplicated AS (
            SELECT symbol, date, total_mv
            FROM raw
            QUALIFY row_number() OVER (
                PARTITION BY symbol, date ORDER BY source_order DESC
            ) = 1
        )
        SELECT symbol, date, total_mv,
               lag(total_mv) OVER (PARTITION BY symbol ORDER BY date) AS lag_total_mv
        FROM deduplicated
    """)

    connection.execute("""
        CREATE OR REPLACE TABLE enriched_signals AS
        WITH joined AS (
            SELECT s.*, p.price_1030, c.lag_total_mv
            FROM signals s
            LEFT JOIN prices p USING (symbol, date)
            LEFT JOIN caps c USING (symbol, date)
        )
        SELECT *,
               ntile(3) OVER (PARTITION BY date ORDER BY price_1030) AS price_tercile,
               ntile(3) OVER (PARTITION BY date ORDER BY lag_total_mv) AS cap_tercile
        FROM joined
        WHERE price_1030 > 0 AND lag_total_mv > 0
    """)

    print("building chain_day", flush=True)
    connection.execute(f"""
        CREATE OR REPLACE TABLE chain_day AS
        SELECT symbol, date::BIGINT AS date,
               count(*)::BIGINT AS chain_rows,
               sum((trade_count::BIGINT > 1)::INTEGER)::BIGINT AS multi_chain_rows,
               sum(volume::DOUBLE) AS chain_volume,
               sum(volume::DOUBLE) FILTER (WHERE trade_count::BIGINT > 1)
                   / nullif(sum(volume::DOUBLE), 0) AS multi_chain_volume_share,
               avg(trade_count::DOUBLE) AS mean_fragments,
               quantile_cont(trade_count::DOUBLE, 0.5) AS median_fragments,
               quantile_cont(trade_count::DOUBLE, 0.9) AS p90_fragments,
               avg(duration_seconds::DOUBLE) FILTER (WHERE trade_count::BIGINT > 1)
                   AS mean_multi_chain_duration_seconds,
               quantile_cont(duration_seconds::DOUBLE, 0.5)
                   FILTER (WHERE trade_count::BIGINT > 1) AS median_multi_chain_duration_seconds,
               quantile_cont(duration_seconds::DOUBLE, 0.9)
                   FILTER (WHERE trade_count::BIGINT > 1) AS p90_multi_chain_duration_seconds,
               avg(acceleration_seconds::DOUBLE)
                   FILTER (WHERE acceleration_seconds IS NOT NULL) AS mean_acceleration_seconds,
               count(acceleration_seconds)::BIGINT AS acceleration_chain_rows
        FROM read_csv_auto(
            '{chain_glob}', header=true, union_by_name=true, sample_size=100000,
            types={{'acceleration_seconds': 'DOUBLE'}}
        )
        GROUP BY symbol, date
    """)

    print("building quote_day_bin", flush=True)
    connection.execute(f"""
        CREATE OR REPLACE TABLE quote_day_bin AS
        WITH quotes AS (
            SELECT symbol, date::BIGINT AS date,
                   relative_depth::DOUBLE AS relative_depth,
                   lifetime_seconds::DOUBLE AS lifetime_seconds,
                   rehit::INTEGER AS rehit,
                   restored_pre_event_book::INTEGER AS restored,
                   censored_at_signal::INTEGER AS censored,
                   removal_action,
                   CASE
                     WHEN relative_depth::DOUBLE <= 0.10 THEN '01_<=0.10'
                     WHEN relative_depth::DOUBLE <= 0.25 THEN '02_0.10-0.25'
                     WHEN relative_depth::DOUBLE <= 0.50 THEN '03_0.25-0.50'
                     WHEN relative_depth::DOUBLE <= 1.00 THEN '04_0.50-1.00'
                     ELSE '05_>1.00'
                   END AS depth_bin
            FROM read_csv_auto(
                '{quote_glob}', header=true, union_by_name=true, sample_size=100000,
                types={{'relative_depth': 'DOUBLE'}}
            )
            WHERE relative_depth IS NOT NULL AND relative_depth::DOUBLE >= 0
        )
        SELECT symbol, date, depth_bin,
               count(*)::BIGINT AS quote_events,
               avg(relative_depth) AS mean_relative_depth,
               avg(rehit) AS rehit_share,
               avg(restored) FILTER (WHERE censored=0) AS restored_share_closed,
               avg((removal_action='TRADE')::INTEGER) FILTER (WHERE censored=0)
                   AS trade_removal_share_closed,
               avg(censored) AS censored_share,
               avg(lifetime_seconds) FILTER (WHERE censored=0) AS mean_lifetime_closed,
               quantile_cont(lifetime_seconds, 0.5) FILTER (WHERE censored=0)
                   AS median_lifetime_closed
        FROM quotes
        GROUP BY symbol, date, depth_bin
    """)

    connection.execute(f"""
        CREATE OR REPLACE TABLE quote_day_features AS
        WITH quotes AS (
            SELECT symbol, date::BIGINT AS date,
                   relative_depth::DOUBLE AS relative_depth,
                   lifetime_seconds::DOUBLE AS lifetime_seconds,
                   rehit::INTEGER AS rehit,
                   restored_pre_event_book::INTEGER AS restored,
                   censored_at_signal::INTEGER AS censored,
                   removal_action
            FROM read_csv_auto(
                '{quote_glob}', header=true, union_by_name=true, sample_size=100000,
                types={{'relative_depth': 'DOUBLE'}}
            )
            WHERE relative_depth IS NOT NULL AND relative_depth::DOUBLE >= 0
        )
        SELECT symbol, date,
               count(*)::BIGINT AS quote_events,
               count(*) FILTER (WHERE relative_depth < 0.50)::BIGINT
                   AS thin_quote_events,
               count(*) FILTER (WHERE relative_depth <= 0.10)::BIGINT
                   AS ultra_thin_quote_events,
               count(*) FILTER (WHERE relative_depth < 0.50)::DOUBLE
                   / nullif(count(*), 0) AS thin_quote_share,
               avg(relative_depth) AS relative_depth_mean,
               avg(rehit) FILTER (WHERE relative_depth < 0.50)
                   AS thin_rehit_share,
               avg(restored) FILTER (
                   WHERE relative_depth < 0.50 AND censored=0
               ) AS thin_restored_share_closed,
               avg((removal_action='TRADE')::INTEGER) FILTER (
                   WHERE relative_depth < 0.50 AND censored=0
               ) AS thin_trade_removal_share_closed,
               avg(censored) FILTER (WHERE relative_depth < 0.50)
                   AS thin_censored_share,
               quantile_cont(lifetime_seconds, 0.5) FILTER (
                   WHERE relative_depth < 0.50 AND censored=0
               ) AS thin_median_lifetime_seconds
        FROM quotes
        GROUP BY symbol, date
    """)

    connection.execute("""
        CREATE OR REPLACE TABLE d07_d09_d10_features AS
        SELECT e.symbol, e.date, e.signal_time, e.price_1030, e.lag_total_mv,
               e.price_tercile, e.cap_tercile,
               (e.price_tercile=1 AND e.cap_tercile=3)::INTEGER
                   AS d10_low_price_large_cap,
               e.impact_observations_5s AS d07_observations_5s,
               e.directional_immediate_impact_mean_5s/e.price_1030
                   AS d07_immediate_bps_5s,
               e.directional_retained_impact_mean_5s/e.price_1030
                   AS d07_retained_bps_5s,
               (e.directional_retained_impact_mean_5s
                 -e.directional_immediate_impact_mean_5s)/e.price_1030
                   AS d07_post_impact_drift_bps_5s,
               e.impact_reversal_share_5s AS d07_reversal_share_5s,
               e.impact_observations_30s AS d07_observations_30s,
               e.directional_immediate_impact_mean_30s/e.price_1030
                   AS d07_immediate_bps_30s,
               e.directional_retained_impact_mean_30s/e.price_1030
                   AS d07_retained_bps_30s,
               (e.directional_retained_impact_mean_30s
                 -e.directional_immediate_impact_mean_30s)/e.price_1030
                   AS d07_post_impact_drift_bps_30s,
               e.impact_reversal_share_30s AS d07_reversal_share_30s,
               e.impact_observations_60s AS d07_observations_60s,
               e.directional_immediate_impact_mean_60s/e.price_1030
                   AS d07_immediate_bps_60s,
               e.directional_retained_impact_mean_60s/e.price_1030
                   AS d07_retained_bps_60s,
               (e.directional_retained_impact_mean_60s
                 -e.directional_immediate_impact_mean_60s)/e.price_1030
                   AS d07_post_impact_drift_bps_60s,
               e.impact_reversal_share_60s AS d07_reversal_share_60s,
               q.quote_events AS d09_quote_events,
               q.thin_quote_events AS d09_thin_quote_events,
               q.ultra_thin_quote_events AS d09_ultra_thin_quote_events,
               q.thin_quote_share AS d09_thin_quote_share,
               q.relative_depth_mean AS d09_relative_depth_mean,
               q.thin_rehit_share AS d09_thin_rehit_share,
               q.thin_restored_share_closed AS d09_thin_restored_share_closed,
               q.thin_trade_removal_share_closed
                   AS d09_thin_trade_removal_share_closed,
               q.thin_censored_share AS d09_thin_censored_share,
               q.thin_median_lifetime_seconds
                   AS d09_thin_median_lifetime_seconds,
               q.thin_quote_share AS d10_thin_quote_share,
               q.relative_depth_mean AS d10_relative_depth_mean,
               e.mean_spread_bps AS d10_spread_bps,
               e.mean_bid_depth1 AS d10_bid_depth1,
               e.mean_ask_depth1 AS d10_ask_depth1,
               e.mean_bid_count1 AS d10_bid_count1,
               e.mean_ask_count1 AS d10_ask_count1,
               e.factor_version
        FROM enriched_signals e
        LEFT JOIN quote_day_features q USING (symbol, date)
    """)

    copy_parquet(connection, "SELECT * FROM signals", cache_dir / "signals.parquet")
    copy_parquet(connection, "SELECT * FROM quality", cache_dir / "quality.parquet")
    copy_parquet(connection, "SELECT * FROM enriched_signals", cache_dir / "enriched_signals.parquet")
    copy_parquet(connection, "SELECT * FROM chain_day", cache_dir / "chain_day.parquet")
    copy_parquet(connection, "SELECT * FROM quote_day_bin", cache_dir / "quote_day_bin.parquet")
    copy_parquet(connection, "SELECT * FROM quote_day_features", cache_dir / "quote_day_features.parquet")
    copy_parquet(
        connection,
        "SELECT * FROM d07_d09_d10_features",
        cache_dir / "d07_d09_d10_features.parquet",
    )


def export_summaries(
    connection: duckdb.DuckDBPyConnection,
    output_dir: Path,
) -> None:
    copy_csv(connection, """
        WITH base AS (
            SELECT CASE WHEN grouping(date//100)=1 THEN 'ALL' ELSE cast(date//100 AS VARCHAR) END AS month,
                   count(*) AS stock_days, count(DISTINCT symbol) AS symbols,
                   count(DISTINCT date) AS dates, sum(total_events) AS total_events,
                   sum(trade_events) AS trade_events,
                   sum(missing_active_order_id) AS missing_active_order_id,
                   sum(invalid_books) AS invalid_books,
                   sum(valid_books) AS valid_books,
                   sum(quote_improvements) AS quote_improvements,
                   sum(quote_censored) AS quote_censored,
                   sum(impact_censored_5s) AS impact_censored_5s,
                   sum(impact_censored_30s) AS impact_censored_30s,
                   sum(impact_censored_60s) AS impact_censored_60s
            FROM quality
            GROUP BY GROUPING SETS ((date//100), ())
        )
        SELECT * FROM base ORDER BY month
    """, output_dir / "quality_summary.csv")

    copy_csv(connection, """
        SELECT left(symbol,2) AS exchange,
               count(*) AS stock_days, count(DISTINCT symbol) AS symbols,
               count(DISTINCT date) AS dates, sum(total_events) AS total_events,
               sum(invalid_books)::DOUBLE/nullif(sum(total_events),0) AS invalid_book_share,
               sum(missing_active_order_id) AS missing_active_order_id,
               sum(quote_improvements) AS quote_improvements,
               sum(quote_censored)::DOUBLE/nullif(sum(quote_improvements),0)
                   AS quote_censored_share
        FROM quality GROUP BY exchange ORDER BY exchange
    """, output_dir / "quality_by_exchange.csv")

    copy_csv(connection, """
        WITH combined AS (
            SELECT s.symbol, s.date, s.chain_count, s.multi_trade_chain_count,
                   s.multi_trade_chain_volume_share, s.chain_volume_hhi,
                   s.chain_volume_entropy, s.largest_chain_volume_share,
                   d.mean_fragments, d.p90_fragments,
                   d.mean_multi_chain_duration_seconds,
                   d.p90_multi_chain_duration_seconds,
                   d.mean_acceleration_seconds
            FROM signals s LEFT JOIN chain_day d USING (symbol, date)
        ), labelled AS (
            SELECT cast(date//100 AS VARCHAR) AS month, * FROM combined
            UNION ALL SELECT 'ALL' AS month, * FROM combined
        )
        SELECT month, count(*) AS stock_days, count(DISTINCT date) AS dates,
               avg(chain_count) AS mean_chain_count,
               avg(multi_trade_chain_count) AS mean_multi_chain_count,
               avg(multi_trade_chain_volume_share) AS mean_multi_chain_volume_share,
               avg(chain_volume_hhi) AS mean_chain_volume_hhi,
               avg(chain_volume_entropy) AS mean_chain_volume_entropy,
               avg(largest_chain_volume_share) AS mean_largest_chain_volume_share,
               avg(mean_fragments) AS mean_chain_fragments,
               avg(p90_fragments) AS mean_stock_day_p90_fragments,
               avg(mean_multi_chain_duration_seconds) AS mean_multi_chain_duration_seconds,
               avg(p90_multi_chain_duration_seconds) AS mean_stock_day_p90_duration_seconds,
               avg(mean_acceleration_seconds) AS mean_acceleration_seconds
        FROM labelled GROUP BY month ORDER BY month
    """, output_dir / "chain_structure_summary.csv")

    copy_csv(connection, """
        WITH long AS (
            SELECT symbol,date,price_1030,5 AS horizon,impact_observations_5s AS observations,
                   directional_immediate_impact_mean_5s/price_1030 AS immediate_bps,
                   directional_retained_impact_mean_5s/price_1030 AS retained_bps,
                   impact_reversal_share_5s AS reversal_share FROM enriched_signals
            UNION ALL
            SELECT symbol,date,price_1030,30,impact_observations_30s,
                   directional_immediate_impact_mean_30s/price_1030,
                   directional_retained_impact_mean_30s/price_1030,
                   impact_reversal_share_30s FROM enriched_signals
            UNION ALL
            SELECT symbol,date,price_1030,60,impact_observations_60s,
                   directional_immediate_impact_mean_60s/price_1030,
                   directional_retained_impact_mean_60s/price_1030,
                   impact_reversal_share_60s FROM enriched_signals
        ), by_date AS (
            SELECT date,horizon,count(*) AS stocks,sum(observations) AS observations,
                   avg(immediate_bps) AS immediate_bps,
                   avg(retained_bps) AS retained_bps,
                   avg(reversal_share) AS reversal_share
            FROM long WHERE observations>0 GROUP BY date,horizon
        ), labelled AS (
            SELECT cast(date//100 AS VARCHAR) AS month,* FROM by_date
            UNION ALL SELECT 'ALL' AS month,* FROM by_date
        )
        SELECT month,horizon,count(*) AS dates,sum(stocks) AS stock_days,
               sum(observations) AS observations,
               avg(immediate_bps) AS date_equal_immediate_bps,
               avg(retained_bps) AS date_equal_retained_bps,
               avg(retained_bps-immediate_bps) AS date_equal_post_impact_drift_bps,
               avg(reversal_share) AS date_equal_reversal_share,
               stddev_samp(retained_bps-immediate_bps)/sqrt(count(*)) AS drift_se,
               avg(retained_bps-immediate_bps)
                 / nullif(stddev_samp(retained_bps-immediate_bps)/sqrt(count(*)),0) AS drift_t
        FROM labelled GROUP BY month,horizon ORDER BY month,horizon
    """, output_dir / "impact_retention_summary.csv")

    copy_csv(connection, """
        WITH long AS (
            SELECT symbol,date,price_1030,5 AS horizon,impact_observations_5s AS observations,
                   directional_immediate_impact_mean_5s/price_1030 AS immediate_bps,
                   directional_retained_impact_mean_5s/price_1030 AS retained_bps,
                   impact_reversal_share_5s AS reversal_share FROM enriched_signals
            UNION ALL
            SELECT symbol,date,price_1030,30,impact_observations_30s,
                   directional_immediate_impact_mean_30s/price_1030,
                   directional_retained_impact_mean_30s/price_1030,
                   impact_reversal_share_30s FROM enriched_signals
            UNION ALL
            SELECT symbol,date,price_1030,60,impact_observations_60s,
                   directional_immediate_impact_mean_60s/price_1030,
                   directional_retained_impact_mean_60s/price_1030,
                   impact_reversal_share_60s FROM enriched_signals
        ), by_date AS (
            SELECT date,left(symbol,2) AS exchange,horizon,count(*) AS stocks,
                   sum(observations) AS observations,
                   avg(immediate_bps) AS stock_day_equal_immediate_bps,
                   avg(retained_bps) AS stock_day_equal_retained_bps,
                   avg(reversal_share) AS stock_day_equal_reversal_share,
                   sum(immediate_bps*observations)/nullif(sum(observations),0)
                       AS event_weighted_immediate_bps,
                   sum(retained_bps*observations)/nullif(sum(observations),0)
                       AS event_weighted_retained_bps
            FROM long WHERE observations>0 GROUP BY date,exchange,horizon
        )
        SELECT exchange,horizon,count(*) AS dates,sum(stocks) AS stock_days,
               sum(observations) AS observations,
               avg(stock_day_equal_immediate_bps) AS stock_day_equal_immediate_bps,
               avg(stock_day_equal_retained_bps) AS stock_day_equal_retained_bps,
               avg(stock_day_equal_reversal_share) AS stock_day_equal_reversal_share,
               sum(event_weighted_immediate_bps*observations)/sum(observations)
                   AS event_weighted_immediate_bps,
               sum(event_weighted_retained_bps*observations)/sum(observations)
                   AS event_weighted_retained_bps
        FROM by_date GROUP BY exchange,horizon ORDER BY exchange,horizon
    """, output_dir / "impact_by_exchange.csv")

    copy_csv(connection, """
        WITH by_date AS (
            SELECT date,depth_bin,sum(quote_events) AS quote_events,count(*) AS stock_days,
                   avg(rehit_share) AS stock_day_equal_rehit_share,
                   avg(restored_share_closed) AS stock_day_equal_restored_share,
                   avg(trade_removal_share_closed) AS stock_day_equal_trade_removal_share,
                   avg(censored_share) AS stock_day_equal_censored_share,
                   avg(median_lifetime_closed) AS stock_day_equal_median_lifetime
            FROM quote_day_bin GROUP BY date,depth_bin
        ), labelled AS (
            SELECT cast(date//100 AS VARCHAR) AS month,* FROM by_date
            UNION ALL SELECT 'ALL' AS month,* FROM by_date
        )
        SELECT month,depth_bin,count(*) AS dates,sum(stock_days) AS stock_days,
               sum(quote_events) AS quote_events,
               avg(stock_day_equal_rehit_share) AS rehit_share,
               avg(stock_day_equal_restored_share) AS restored_share_closed,
               avg(stock_day_equal_trade_removal_share) AS trade_removal_share_closed,
               avg(stock_day_equal_censored_share) AS censored_share,
               avg(stock_day_equal_median_lifetime) AS median_lifetime_seconds,
               stddev_samp(stock_day_equal_rehit_share)/sqrt(count(*)) AS rehit_se
        FROM labelled GROUP BY month,depth_bin ORDER BY month,depth_bin
    """, output_dir / "quote_depth_bin_summary.csv")

    copy_csv(connection, """
        WITH by_date AS (
            SELECT date,left(symbol,2) AS exchange,depth_bin,
                   sum(quote_events) AS quote_events,count(*) AS stock_days,
                   avg(rehit_share) AS rehit_share,
                   avg(restored_share_closed) AS restored_share,
                   avg(trade_removal_share_closed) AS trade_removal_share,
                   avg(censored_share) AS censored_share,
                   avg(median_lifetime_closed) AS median_lifetime_seconds
            FROM quote_day_bin GROUP BY date,exchange,depth_bin
        )
        SELECT exchange,depth_bin,count(*) AS dates,sum(stock_days) AS stock_days,
               sum(quote_events) AS quote_events,avg(rehit_share) AS rehit_share,
               avg(restored_share) AS restored_share_closed,
               avg(trade_removal_share) AS trade_removal_share_closed,
               avg(censored_share) AS censored_share,
               avg(median_lifetime_seconds) AS median_lifetime_seconds
        FROM by_date GROUP BY exchange,depth_bin ORDER BY exchange,depth_bin
    """, output_dir / "quote_depth_bin_by_exchange.csv")

    copy_csv(connection, """
        WITH by_date AS (
            SELECT date,price_tercile,cap_tercile,count(*) AS stocks,
                   avg(new_quote_thin_share_lt_0_5) AS thin_share,
                   avg(new_quote_rehit_share) AS rehit_share,
                   avg(new_quote_restored_share) AS restored_share,
                   avg(impact_reversal_share_30s) AS reversal_30s,
                   avg(impact_reversal_share_60s) AS reversal_60s,
                   avg(multi_trade_chain_volume_share) AS multi_chain_volume_share,
                   avg(chain_volume_hhi) AS chain_hhi
            FROM enriched_signals GROUP BY date,price_tercile,cap_tercile
        ), labelled AS (
            SELECT cast(date//100 AS VARCHAR) AS month,* FROM by_date
            UNION ALL SELECT 'ALL' AS month,* FROM by_date
        )
        SELECT month,price_tercile,cap_tercile,count(*) AS dates,sum(stocks) AS stock_days,
               avg(thin_share) AS thin_share,
               avg(rehit_share) AS rehit_share,
               avg(restored_share) AS restored_share,
               avg(reversal_30s) AS reversal_30s,
               avg(reversal_60s) AS reversal_60s,
               avg(multi_chain_volume_share) AS multi_chain_volume_share,
               avg(chain_hhi) AS chain_hhi
        FROM labelled
        GROUP BY month,price_tercile,cap_tercile
        ORDER BY month,price_tercile,cap_tercile
    """, output_dir / "price_cap_grid.csv")

    copy_csv(connection, """
        WITH by_date AS (
            SELECT date,left(symbol,2) AS exchange,price_tercile,cap_tercile,
                   count(*) AS stocks,
                   avg(new_quote_thin_share_lt_0_5) AS thin_share,
                   avg(new_quote_rehit_share) AS rehit_share,
                   avg(new_quote_restored_share) AS restored_share,
                   avg(impact_reversal_share_30s) AS reversal_30s,
                   avg(impact_reversal_share_60s) AS reversal_60s,
                   avg(multi_trade_chain_volume_share) AS multi_chain_volume_share,
                   avg(chain_volume_hhi) AS chain_hhi
            FROM enriched_signals
            GROUP BY date,exchange,price_tercile,cap_tercile
        )
        SELECT exchange,price_tercile,cap_tercile,count(*) AS dates,
               sum(stocks) AS stock_days,avg(thin_share) AS thin_share,
               avg(rehit_share) AS rehit_share,avg(restored_share) AS restored_share,
               avg(reversal_30s) AS reversal_30s,avg(reversal_60s) AS reversal_60s,
               avg(multi_chain_volume_share) AS multi_chain_volume_share,
               avg(chain_hhi) AS chain_hhi
        FROM by_date GROUP BY exchange,price_tercile,cap_tercile
        ORDER BY exchange,price_tercile,cap_tercile
    """, output_dir / "price_cap_grid_by_exchange.csv")

    copy_csv(connection, """
        SELECT count(*) AS signal_rows, count(DISTINCT symbol) AS symbols,
               count(DISTINCT date) AS dates,
               count(*)-count(DISTINCT symbol || ':' || cast(date AS VARCHAR)) AS duplicate_keys,
               count(price_1030) AS price_covered,
               count(lag_total_mv) AS lag_cap_covered,
               (SELECT count(*) FROM enriched_signals) AS fully_enriched_rows
        FROM signals s
        LEFT JOIN prices p USING(symbol,date)
        LEFT JOIN caps c USING(symbol,date)
    """, output_dir / "join_coverage.csv")

    copy_csv(
        connection,
        "SELECT * FROM d07_d09_d10_features ORDER BY date, symbol",
        output_dir / "d07_d09_d10_features.csv",
    )

    copy_csv(connection, """
        WITH by_date AS (
            SELECT date, price_tercile, cap_tercile,
                   count(*) AS stock_days,
                   count(d09_quote_events) AS quote_covered_stock_days,
                   sum(d09_quote_events) AS quote_events,
                   avg(d09_thin_quote_share) AS thin_quote_share,
                   avg(d09_thin_rehit_share) AS thin_rehit_share,
                   avg(d09_thin_restored_share_closed)
                       AS thin_restored_share_closed,
                   avg(d09_thin_trade_removal_share_closed)
                       AS thin_trade_removal_share_closed,
                   avg(d09_thin_censored_share) AS thin_censored_share,
                   avg(d09_thin_median_lifetime_seconds)
                       AS thin_median_lifetime_seconds
            FROM d07_d09_d10_features
            GROUP BY date, price_tercile, cap_tercile
        ), labelled AS (
            SELECT cast(date//100 AS VARCHAR) AS month, * FROM by_date
            UNION ALL SELECT 'ALL' AS month, * FROM by_date
        )
        SELECT month, price_tercile, cap_tercile, count(*) AS dates,
               sum(stock_days) AS stock_days,
               sum(quote_covered_stock_days) AS quote_covered_stock_days,
               sum(quote_events) AS quote_events,
               avg(thin_quote_share) AS thin_quote_share,
               avg(thin_rehit_share) AS thin_rehit_share,
               avg(thin_restored_share_closed) AS thin_restored_share_closed,
               avg(thin_trade_removal_share_closed)
                   AS thin_trade_removal_share_closed,
               avg(thin_censored_share) AS thin_censored_share,
               avg(thin_median_lifetime_seconds)
                   AS thin_median_lifetime_seconds
        FROM labelled
        GROUP BY month, price_tercile, cap_tercile
        ORDER BY month, price_tercile, cap_tercile
    """, output_dir / "d09_d10_feature_summary.csv")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--shard-root",
        type=Path,
        default=PROJECT_ROOT / "data/cache/experiment_batch_1/intraday_1000_1030_202601_202602_v1",
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=PROJECT_ROOT / "data/cache/experiment_batch_1/mechanism_analysis_202601_202602_v1",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=PROJECT_ROOT / "results/intraday/experiment_batch_1/mechanism_analysis_202601_202602_v1",
    )
    parser.add_argument("--threads", type=int, default=16)
    parser.add_argument("--memory-limit", default="32GB")
    parser.add_argument(
        "--reuse-tables",
        action="store_true",
        help="Reuse compact DuckDB tables and only regenerate summary files.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    source_manifest_path = args.shard_root / "manifest.json"
    if not source_manifest_path.is_file():
        raise FileNotFoundError(f"missing source manifest: {source_manifest_path}")
    source_manifest = json.loads(source_manifest_path.read_text())
    source_config = source_manifest.get("config", {})
    args.cache_dir.mkdir(parents=True, exist_ok=True)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    temporary = args.cache_dir / "duckdb_tmp"
    temporary.mkdir(exist_ok=True)
    database = args.cache_dir / "mechanism_analysis.duckdb"
    connection = duckdb.connect(str(database))
    connection.execute(f"PRAGMA threads={args.threads}")
    connection.execute(f"PRAGMA memory_limit='{args.memory_limit}'")
    connection.execute(f"PRAGMA temp_directory='{sql_path(temporary)}'")
    connection.execute("PRAGMA enable_progress_bar=true")
    try:
        if not args.reuse_tables:
            build_tables(connection, args.shard_root, args.cache_dir)
        export_summaries(connection, args.output_dir)
        metadata = {
            "analysis": "experiment_batch_1 first-layer mechanisms",
            "analysis_version": "experiment_batch_1_first_layer_v2_20260807",
            "factor_version": source_config.get("factor_version"),
            "source_fingerprint": source_manifest.get("fingerprint"),
            "exchange": source_config.get("exchange", "ALL"),
            "months": source_config.get("months"),
            "stock_month_files": source_config.get("stock_month_files"),
            "universe_rule": source_config.get("universe_rule"),
            "output_etf_symbols": source_config.get("output_etf_symbols"),
            "window": source_config.get("window"),
            "signal_time": source_config.get("signal_time"),
            "post_signal_returns_joined": False,
            "evaluation_neutralization": "none",
            "market_cap_rule": "previous trading observation total_mv within symbol",
            "price_rule": "10:30 price",
            "d07_rule": "atomic safe-prebook immediate and retained impact at 5/30/60 seconds",
            "d09_rule": "inside-spread quote lifecycle; thin means relative_depth < 0.50",
            "d10_rule": "D09 state crossed with exchange-local signal-date price/cap terciles",
            "primary_weighting": "stock-day equal, then date equal",
            "pooled_event_results": "diagnostic only",
        }
        (args.output_dir / "metadata.json").write_text(
            json.dumps(metadata, ensure_ascii=False, indent=2) + "\n"
        )
    finally:
        connection.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
