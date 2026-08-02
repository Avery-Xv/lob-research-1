#!/usr/bin/env python3
"""CNE5-neutral open-to-open diagnostics for existing daily LOB factors."""

from __future__ import annotations

import argparse
import csv
import math
from collections import defaultdict
from pathlib import Path
from statistics import mean, stdev

import duckdb

from backtest_daily_domains import domain, pearson, ranks


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CNE5_COLS = (
    "size",
    "non_linear_size",
    "momentum",
    "liquidity",
    "book_to_price",
    "leverage",
    "growth",
    "earnings_yield",
    "beta",
    "residual_volatility",
)


def finite(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def mean_t(values: list[float]) -> tuple[float | None, float | None]:
    if not values:
        return None, None
    average = mean(values)
    if len(values) < 2:
        return average, None
    volatility = stdev(values)
    return average, average / (volatility / math.sqrt(len(values))) if volatility else None


def spread(ordered_returns: list[float]) -> float:
    bucket = max(1, len(ordered_returns) // 10)
    return mean(ordered_returns[-bucket:]) - mean(ordered_returns[:bucket])


def event_gap(returns: list[float], events: list[bool]) -> float | None:
    event_returns = [value for value, event in zip(returns, events) if event]
    control_returns = [value for value, event in zip(returns, events) if not event]
    if not event_returns or not control_returns:
        return None
    return mean(event_returns) - mean(control_returns)


def build_orthonormal_basis(exposures: list[list[float]]) -> list[list[float]]:
    """Build the centered CNE5 projection basis once for a stock universe."""
    orthonormal_columns: list[list[float]] = []
    for column_index in range(len(exposures[0])):
        column = [row[column_index] for row in exposures]
        column_mean = mean(column)
        vector = [value - column_mean for value in column]
        for basis in orthonormal_columns:
            projection = sum(value * base for value, base in zip(vector, basis))
            vector = [value - projection * base for value, base in zip(vector, basis)]
        norm = math.sqrt(sum(value * value for value in vector))
        if norm > 1e-10:
            orthonormal_columns.append([value / norm for value in vector])
    return orthonormal_columns


def residualize(values: list[float], basis_columns: list[list[float]]) -> list[float]:
    """Residualize values on an already-built centered exposure basis."""
    value_mean = mean(values)
    residuals = [value - value_mean for value in values]
    for basis in basis_columns:
        projection = sum(value * base for value, base in zip(residuals, basis))
        residuals = [value - projection * base for value, base in zip(residuals, basis)]
    return residuals


ProjectionCacheValue = tuple[
    list[list[float]], list[float], list[float], list[float]
]


def daily_stat(
    rows: list[tuple[str, float, float, list[float], bool]],
    cache_key_prefix: tuple[str, str, str],
    projection_cache: dict[tuple[object, ...], ProjectionCacheValue],
) -> dict[str, object] | None:
    if len(rows) < 20:
        return None
    rows = sorted(rows, key=lambda row: row[0])
    symbols = [row[0] for row in rows]
    raw_factors = [row[1] for row in rows]
    returns = [row[2] for row in rows]
    exposures = [row[3] for row in rows]
    events = [row[4] for row in rows]
    cache_key = (*cache_key_prefix, tuple(symbols))
    cached = projection_cache.get(cache_key)
    if cached is None:
        basis_columns = build_orthonormal_basis(exposures)
        neutral_returns = residualize(returns, basis_columns)
        raw_return_ranks = ranks(returns)
        neutral_return_ranks = ranks(neutral_returns)
        cached = (
            basis_columns,
            neutral_returns,
            raw_return_ranks,
            neutral_return_ranks,
        )
        projection_cache[cache_key] = cached
    basis_columns, neutral_returns, raw_return_ranks, neutral_return_ranks = cached
    neutral_factors = residualize(raw_factors, basis_columns)
    raw_factor_ranks = ranks(raw_factors)
    neutral_factor_ranks = ranks(neutral_factors)
    raw_order = sorted(range(len(rows)), key=lambda index: (raw_factors[index], symbols[index]))
    neutral_order = sorted(
        range(len(rows)), key=lambda index: (neutral_factors[index], symbols[index])
    )
    return {
        "n": len(rows),
        "event_n": sum(events),
        "raw_rank_ic": pearson(raw_factor_ranks, raw_return_ranks),
        "cne5_rank_ic": pearson(neutral_factor_ranks, raw_return_ranks),
        "cne5_residual_return_rank_ic": pearson(
            neutral_factor_ranks, neutral_return_ranks
        ),
        "raw_d10_d1": spread([returns[index] for index in raw_order]),
        "cne5_d10_d1": spread([returns[index] for index in neutral_order]),
        "raw_event_gap": event_gap(returns, events),
        "cne5_residual_return_event_gap": event_gap(neutral_returns, events),
    }


def summarize(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    keys = ("factor", "window_name", "cap_group", "price_group")
    grouped: dict[tuple[str, ...], list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        grouped[tuple(str(row[key]) for key in keys)].append(row)
    metrics = (
        "raw_rank_ic",
        "cne5_rank_ic",
        "cne5_residual_return_rank_ic",
        "raw_d10_d1",
        "cne5_d10_d1",
        "raw_event_gap",
        "cne5_residual_return_event_gap",
    )
    output: list[dict[str, object]] = []
    for group, observations in sorted(grouped.items()):
        result: dict[str, object] = dict(zip(keys, group))
        result.update(
            {
                "n_days": len(observations),
                "n_obs": sum(int(row["n"]) for row in observations),
                "avg_names": mean(int(row["n"]) for row in observations),
                "event_n": sum(int(row["event_n"]) for row in observations),
            }
        )
        for metric in metrics:
            values = [
                float(row[metric]) for row in observations if row[metric] is not None
            ]
            average, t_stat = mean_t(values)
            result[metric] = average
            result[f"{metric}_t"] = t_stat
        output.append(result)
    return output


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    if not rows:
        raise ValueError(f"no output rows for {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    with temporary.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    temporary.replace(path)


def load_common(
    returns_path: str,
    market_caps_path: str,
    cne5_path: str,
    date_from: int,
    date_to: int,
) -> dict[tuple[str, int], tuple[float, float, list[float], float]]:
    connection = duckdb.connect()
    connection.read_csv(returns_path).create_view("returns_raw")
    connection.read_csv(market_caps_path).create_view("caps_raw")
    connection.read_csv(cne5_path).create_view("cne5_raw")
    exposure_sql = ", ".join(f"x.{column}::DOUBLE" for column in CNE5_COLS)
    rows = connection.execute(
        f"""
        WITH returns AS (
            SELECT DISTINCT symbol, date::INTEGER AS date,
                   next_date::INTEGER AS next_date, open::DOUBLE AS open,
                   o2o_ret::DOUBLE AS o2o_ret
            FROM returns_raw
            WHERE next_date::INTEGER > date::INTEGER
        ),
        caps AS (
            SELECT DISTINCT symbol, date::INTEGER AS date, total_mv::DOUBLE AS total_mv
            FROM caps_raw
        ),
        previous_caps AS (
            SELECT symbol, date,
                   lag(total_mv) OVER (PARTITION BY symbol ORDER BY date) AS previous_market_cap
            FROM caps
        ),
        exposures AS (
            SELECT symbol, replace(date::VARCHAR, '-', '')::INTEGER AS date,
                   {', '.join(column + '::DOUBLE AS ' + column for column in CNE5_COLS)}
            FROM cne5_raw
        )
        SELECT r0.symbol, r0.date, r0.open, r1.o2o_ret, c.previous_market_cap,
               {exposure_sql}
        FROM returns r0
        JOIN returns r1 ON r1.symbol = r0.symbol AND r1.date = r0.next_date
        JOIN previous_caps c ON c.symbol = r0.symbol AND c.date = r0.date
        JOIN exposures x ON x.symbol = r0.symbol AND x.date = r0.date
        WHERE r0.date BETWEEN ? AND ?
        """,
        [date_from, date_to],
    ).fetchall()
    connection.close()
    common: dict[tuple[str, int], tuple[float, float, list[float], float]] = {}
    for row in rows:
        symbol, date = str(row[0]), int(row[1])
        signal_open, target_return, previous_cap = map(finite, row[2:5])
        exposures = [finite(value) for value in row[5:]]
        if (
            signal_open is None
            or target_return is None
            or previous_cap is None
            or any(value is None for value in exposures)
        ):
            continue
        common[(symbol, date)] = (
            float(signal_open),
            float(target_return),
            [float(value) for value in exposures if value is not None],
            float(previous_cap),
        )
    return common


def add_factor(
    grouped: dict[
        tuple[str, str, str, str, str],
        list[tuple[str, float, float, list[float], bool]],
    ],
    common: dict[tuple[str, int], tuple[float, float, list[float], float]],
    *,
    factor_name: str,
    window_name: str,
    symbol: str,
    date: int,
    factor: float | None,
    event: bool = False,
) -> None:
    observation = common.get((symbol, date))
    if observation is None or factor is None:
        return
    signal_open, target_return, exposures, previous_cap = observation
    groups = domain(previous_cap, signal_open, symbol)
    if groups is None:
        return
    value = (symbol, factor, target_return, exposures, event)
    for cap_group, price_group in (("all", "all"), groups):
        grouped[(factor_name, window_name, cap_group, price_group, str(date))].append(value)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--old-active",
        default=str(PROJECT_ROOT / "data/processed/active_take_mid_gap_daily_close1000_v4_202601.csv"),
    )
    parser.add_argument(
        "--all-trade",
        default=str(PROJECT_ROOT / "data/processed/stylized_fact_4_6/g1_d01_d03_factors_202512_202601_history20_v3.csv"),
    )
    parser.add_argument(
        "--strict-active",
        default=str(PROJECT_ROOT / "data/cache/stylized_fact_4_6/g1_d01_d03_active_take_diagnostic_202601.csv"),
    )
    parser.add_argument(
        "--returns",
        default=str(PROJECT_ROOT / "data/cache/daily_open_to_open_market_calendar_202512_20260206.csv"),
    )
    parser.add_argument(
        "--market-caps",
        default=str(PROJECT_ROOT / "data/cache/daily_market_cap_202512_202601.csv"),
    )
    parser.add_argument(
        "--cne5",
        default=str(PROJECT_ROOT / "data/cache/cne5_style_full_202512_202601.csv"),
    )
    parser.add_argument("--date-from", type=int, default=20260101)
    parser.add_argument("--date-to", type=int, default=20260130)
    parser.add_argument(
        "--output-dir",
        default=str(PROJECT_ROOT / "results/daily/stylized_fact_4_6/o2o_cne5_202601"),
    )
    args = parser.parse_args()

    common = load_common(
        args.returns, args.market_caps, args.cne5, args.date_from, args.date_to
    )
    grouped: dict[
        tuple[str, str, str, str, str],
        list[tuple[str, float, float, list[float], bool]],
    ] = defaultdict(list)

    with Path(args.old_active).open(newline="") as handle:
        for row in csv.DictReader(handle):
            date, symbol = int(row["date"]), row["symbol"]
            if not args.date_from <= date <= args.date_to:
                continue
            for name, column in (
                ("old_active_abs", "active_take_mid_gap_over_1000_close"),
                ("old_active_signed", "active_take_mid_gap_signed_over_1000_close"),
                ("old_active_ratio", "active_take_mid_gap_ratio"),
            ):
                add_factor(
                    grouped,
                    common,
                    factor_name=name,
                    window_name="daily_0930_close",
                    symbol=symbol,
                    date=date,
                    factor=finite(row.get(column)),
                )

    with Path(args.all_trade).open(newline="") as handle:
        for row in csv.DictReader(handle):
            if row["frequency"] != "daily":
                continue
            date, symbol = int(row["date"]), row["symbol"]
            if not args.date_from <= date <= args.date_to:
                continue
            for name, column in (
                ("all_trade_d01", "d01_trade_reversal"),
                ("all_trade_d02", "d02_trade_momentum"),
            ):
                add_factor(
                    grouped,
                    common,
                    factor_name=name,
                    window_name=row["window_name"],
                    symbol=symbol,
                    date=date,
                    factor=finite(row.get(column)),
                )
            history_count = int(row["d03_history_observations"])
            if history_count < 20:
                continue
            order_impact = finite(row["order_impact_over_normalizer"])
            history_rank = finite(row["order_impact_history_rank_pct"])
            for name, column, threshold in (
                ("d03_90", "d03_positive_order_ts_extreme90", 0.90),
                ("d03_95", "d03_positive_order_ts_extreme95", 0.95),
            ):
                event = bool(
                    order_impact is not None
                    and order_impact > 0
                    and history_rank is not None
                    and history_rank > threshold
                )
                add_factor(
                    grouped,
                    common,
                    factor_name=name,
                    window_name=row["window_name"],
                    symbol=symbol,
                    date=date,
                    factor=finite(row.get(column)),
                    event=event,
                )

    with Path(args.strict_active).open(newline="") as handle:
        for row in csv.DictReader(handle):
            if row["frequency"] != "daily":
                continue
            date, symbol = int(row["date"]), row["symbol"]
            if not args.date_from <= date <= args.date_to:
                continue
            for name, column in (
                ("strict_active_d01", "d01_trade_reversal"),
                ("strict_active_d02", "d02_trade_momentum"),
            ):
                add_factor(
                    grouped,
                    common,
                    factor_name=name,
                    window_name=row["window_name"],
                    symbol=symbol,
                    date=date,
                    factor=finite(row.get(column)),
                )

    daily_rows: list[dict[str, object]] = []
    projection_cache: dict[tuple[object, ...], ProjectionCacheValue] = {}
    for key, observations in sorted(grouped.items()):
        factor_name, window_name, cap_group, price_group, date = key
        stats = daily_stat(
            observations,
            (date, cap_group, price_group),
            projection_cache,
        )
        if stats is None:
            continue
        daily_rows.append(
            {
                "factor": factor_name,
                "window_name": window_name,
                "cap_group": cap_group,
                "price_group": price_group,
                "date": date,
                **stats,
            }
        )

    output_dir = Path(args.output_dir)
    write_csv(output_dir / "daily.csv", daily_rows)
    summary_rows = summarize(daily_rows)
    write_csv(output_dir / "summary.csv", summary_rows)
    print(
        f"common_rows={len(common)} daily_rows={len(daily_rows)} "
        f"summary_rows={len(summary_rows)} cached_universes={len(projection_cache)} output_dir={output_dir}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
