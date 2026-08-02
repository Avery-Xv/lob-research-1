#!/usr/bin/env python3
"""LOB-five-ex-size style exposure and neutralized backtests for existing factors."""

from __future__ import annotations

import argparse
import csv
import math
from collections import defaultdict
from pathlib import Path
from statistics import mean, stdev

import duckdb

from backtest_daily_domains import domain, pearson, ranks
from backtest_existing_daily_o2o_cne5 import (
    build_orthonormal_basis,
    finite,
    residualize,
    spread,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOB5_EX_SIZE_COLS = (
    "non_linear_size",
    "momentum",
    "liquidity",
    "beta",
    "residual_volatility",
)
INTRADAY_TARGETS = (
    "ret_1031_1035",
    "ret_1031_1040",
    "ret_1031_1045",
    "ret_1031_1100",
    "ret_1031_1457",
)

CommonValue = tuple[float, tuple[float | None, ...], list[float], float]
FactorRow = tuple[str, float, tuple[float | None, ...], list[float]]


def mean_t(values: list[float]) -> tuple[float | None, float | None]:
    if not values:
        return None, None
    average = mean(values)
    if len(values) < 2:
        return average, None
    volatility = stdev(values)
    return average, average / (volatility / math.sqrt(len(values))) if volatility else None


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    if not rows:
        raise ValueError(f"no rows for {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    with temporary.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    temporary.replace(path)


def load_common(
    *,
    returns_path: str,
    intraday_returns_path: str,
    market_caps_path: str,
    styles_path: str,
    date_from: int,
    date_to: int,
) -> tuple[dict[tuple[str, int], CommonValue], dict[tuple[str, int], CommonValue]]:
    connection = duckdb.connect()
    connection.read_csv(returns_path).create_view("returns_raw")
    connection.read_csv(intraday_returns_path).create_view("intraday_raw")
    connection.read_csv(market_caps_path).create_view("caps_raw")
    connection.read_csv(styles_path).create_view("styles_raw")
    style_select = ", ".join(f"{column}::DOUBLE AS {column}" for column in LOB5_EX_SIZE_COLS)
    style_current = ", ".join(f"s.{column}::DOUBLE" for column in LOB5_EX_SIZE_COLS)
    style_previous = ", ".join(f"s.previous_{column}::DOUBLE" for column in LOB5_EX_SIZE_COLS)
    previous_style_select = ", ".join(
        f"lag({column}) OVER (PARTITION BY symbol ORDER BY date) AS previous_{column}"
        for column in LOB5_EX_SIZE_COLS
    )
    connection.execute(
        f"""
        CREATE VIEW returns AS
        SELECT DISTINCT symbol, date::INTEGER AS date, next_date::INTEGER AS next_date,
               open::DOUBLE AS open, o2o_ret::DOUBLE AS o2o_ret
        FROM returns_raw WHERE next_date::INTEGER > date::INTEGER;
        CREATE VIEW caps AS
        SELECT DISTINCT symbol, date::INTEGER AS date, total_mv::DOUBLE AS total_mv
        FROM caps_raw;
        CREATE VIEW previous_caps AS
        SELECT symbol, date,
               lag(total_mv) OVER (PARTITION BY symbol ORDER BY date) AS previous_market_cap
        FROM caps;
        CREATE VIEW styles AS
        SELECT symbol, replace(date::VARCHAR, '-', '')::INTEGER AS date, {style_select}
        FROM styles_raw;
        CREATE VIEW previous_styles AS
        SELECT symbol, date, {previous_style_select}
        FROM styles;
        """
    )
    daily_rows = connection.execute(
        f"""
        SELECT r0.symbol, r0.date, r0.open, r1.o2o_ret, c.previous_market_cap,
               {style_current}
        FROM returns r0
        LEFT JOIN returns r1 ON r1.symbol = r0.symbol AND r1.date = r0.next_date
        JOIN previous_caps c ON c.symbol = r0.symbol AND c.date = r0.date
        JOIN styles s ON s.symbol = r0.symbol AND s.date = r0.date
        WHERE r0.date BETWEEN ? AND ?
        """,
        [date_from, date_to],
    ).fetchall()
    intraday_rows = connection.execute(
        f"""
        SELECT r.symbol, r.date::INTEGER, c.previous_market_cap,
               r.ret_1031_1035::DOUBLE, r.ret_1031_1040::DOUBLE,
               r.ret_1031_1045::DOUBLE, r.ret_1031_1100::DOUBLE,
               r.ret_1031_1457::DOUBLE, {style_previous}
        FROM intraday_raw r
        JOIN previous_caps c ON c.symbol = r.symbol AND c.date = r.date::INTEGER
        JOIN previous_styles s ON s.symbol = r.symbol AND s.date = r.date::INTEGER
        WHERE r.date::INTEGER BETWEEN ? AND ?
          AND r.is_st = 0 AND r.is_suspended = 0
        """,
        [date_from, date_to],
    ).fetchall()
    connection.close()

    daily: dict[tuple[str, int], CommonValue] = {}
    for row in daily_rows:
        signal_open = finite(row[2])
        target = finite(row[3])
        previous_cap = finite(row[4])
        styles = [finite(value) for value in row[5:]]
        if signal_open is None or previous_cap is None or any(value is None for value in styles):
            continue
        daily[(str(row[0]), int(row[1]))] = (
            float(signal_open),
            (target,),
            [float(value) for value in styles if value is not None],
            float(previous_cap),
        )

    intraday: dict[tuple[str, int], CommonValue] = {}
    for row in intraday_rows:
        previous_cap = finite(row[2])
        targets = tuple(finite(value) for value in row[3:8])
        styles = [finite(value) for value in row[8:]]
        if previous_cap is None or any(value is None for value in styles):
            continue
        intraday[(str(row[0]), int(row[1]))] = (
            math.nan,
            targets,
            [float(value) for value in styles if value is not None],
            float(previous_cap),
        )
    return daily, intraday


def add_observation(
    performance: dict[tuple[str, ...], list[FactorRow]],
    exposure: dict[tuple[str, ...], list[FactorRow]],
    common: dict[tuple[str, int], CommonValue],
    *,
    frequency: str,
    factor_name: str,
    window_name: str,
    symbol: str,
    date: int,
    factor: float | None,
    signal_price: float | None,
) -> None:
    observation = common.get((symbol, date))
    if observation is None or factor is None:
        return
    common_price, targets, styles, previous_cap = observation
    price = signal_price if frequency == "intraday" else common_price
    if price is None or not math.isfinite(price) or price <= 0:
        return
    groups = domain(previous_cap, price, symbol)
    if groups is None:
        return
    value = (symbol, factor, targets, styles)
    exposure[(frequency, factor_name, window_name, str(date))].append(value)
    for cap_group, price_group in (("all", "all"), groups):
        performance[
            (frequency, factor_name, window_name, cap_group, price_group, str(date))
        ].append(value)


def load_factors(
    *,
    old_daily_path: str,
    old_intraday_path: str,
    all_trade_path: str,
    strict_active_path: str,
    daily_common: dict[tuple[str, int], CommonValue],
    intraday_common: dict[tuple[str, int], CommonValue],
    date_from: int,
    date_to: int,
) -> tuple[
    dict[tuple[str, ...], list[FactorRow]],
    dict[tuple[str, ...], list[FactorRow]],
]:
    performance: dict[tuple[str, ...], list[FactorRow]] = defaultdict(list)
    exposure: dict[tuple[str, ...], list[FactorRow]] = defaultdict(list)

    with Path(old_daily_path).open(newline="") as handle:
        for row in csv.DictReader(handle):
            date, symbol = int(row["date"]), row["symbol"]
            if not date_from <= date <= date_to:
                continue
            for name, column in (
                ("old_active_abs", "active_take_mid_gap_over_1000_close"),
                ("old_active_signed", "active_take_mid_gap_signed_over_1000_close"),
                ("old_active_ratio", "active_take_mid_gap_ratio"),
            ):
                add_observation(
                    performance, exposure, daily_common,
                    frequency="daily", factor_name=name, window_name="daily_0930_close",
                    symbol=symbol, date=date, factor=finite(row.get(column)), signal_price=None,
                )

    with Path(old_intraday_path).open(newline="") as handle:
        for row in csv.DictReader(handle):
            date, symbol = int(row["date"]), row["symbol"]
            if not date_from <= date <= date_to:
                continue
            signal_price = finite(row.get("start_mid"))
            for name, column in (
                ("old_active_abs", "active_take_mid_gap_over_start_mid"),
                ("old_active_signed", "active_take_mid_gap_signed_over_start_mid"),
                ("old_active_ratio", "active_take_mid_gap_ratio"),
            ):
                add_observation(
                    performance, exposure, intraday_common,
                    frequency="intraday", factor_name=name,
                    window_name="intraday_1000_1030", symbol=symbol, date=date,
                    factor=finite(row.get(column)), signal_price=signal_price,
                )

    with Path(all_trade_path).open(newline="") as handle:
        for row in csv.DictReader(handle):
            frequency = row["frequency"]
            if frequency not in {"daily", "intraday"}:
                continue
            date, symbol = int(row["date"]), row["symbol"]
            if not date_from <= date <= date_to:
                continue
            common = daily_common if frequency == "daily" else intraday_common
            signal_price = finite(row.get("normalizer_price"))
            factor_specs = [
                ("all_trade_d01", "d01_trade_reversal"),
                ("all_trade_d02", "d02_trade_momentum"),
            ]
            history_count = int(row["d03_history_observations"])
            if history_count >= 20:
                factor_specs.extend(
                    [
                        ("d03_90", "d03_positive_order_ts_extreme90"),
                        ("d03_95", "d03_positive_order_ts_extreme95"),
                    ]
                )
            for name, column in factor_specs:
                add_observation(
                    performance, exposure, common,
                    frequency=frequency, factor_name=name, window_name=row["window_name"],
                    symbol=symbol, date=date, factor=finite(row.get(column)),
                    signal_price=signal_price,
                )

    with Path(strict_active_path).open(newline="") as handle:
        for row in csv.DictReader(handle):
            frequency = row["frequency"]
            if frequency not in {"daily", "intraday"}:
                continue
            date, symbol = int(row["date"]), row["symbol"]
            if not date_from <= date <= date_to:
                continue
            common = daily_common if frequency == "daily" else intraday_common
            signal_price = finite(row.get("normalizer_price"))
            for name, column in (
                ("strict_active_d01", "d01_trade_reversal"),
                ("strict_active_d02", "d02_trade_momentum"),
            ):
                add_observation(
                    performance, exposure, common,
                    frequency=frequency, factor_name=name, window_name=row["window_name"],
                    symbol=symbol, date=date, factor=finite(row.get(column)),
                    signal_price=signal_price,
                )
    return performance, exposure


def exposure_by_date(
    grouped: dict[tuple[str, ...], list[FactorRow]],
) -> list[dict[str, object]]:
    output: list[dict[str, object]] = []
    for key, rows in sorted(grouped.items()):
        if len(rows) < 20:
            continue
        frequency, factor_name, window_name, date = key
        rows = sorted(rows, key=lambda row: row[0])
        factors = [row[1] for row in rows]
        styles = [row[3] for row in rows]
        basis = build_orthonormal_basis(styles)
        residual = residualize(factors, basis)
        factor_mean = mean(factors)
        centered = [value - factor_mean for value in factors]
        total_variance = sum(value * value for value in centered)
        residual_variance = sum(value * value for value in residual)
        result: dict[str, object] = {
            "frequency": frequency,
            "factor": factor_name,
            "window_name": window_name,
            "date": date,
            "n": len(rows),
            "joint_r2": (
                max(0.0, min(1.0, 1.0 - residual_variance / total_variance))
                if total_variance > 0 else None
            ),
        }
        factor_ranks = ranks(factors)
        for index, style_name in enumerate(LOB5_EX_SIZE_COLS):
            result[f"{style_name}_rank_exposure"] = pearson(
                factor_ranks, ranks([row[index] for row in styles])
            )
        output.append(result)
    return output


def summarize_exposure(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    grouped: dict[tuple[str, str, str], list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        grouped[(str(row["frequency"]), str(row["factor"]), str(row["window_name"]))].append(row)
    output: list[dict[str, object]] = []
    for key, observations in sorted(grouped.items()):
        result: dict[str, object] = {
            "frequency": key[0], "factor": key[1], "window_name": key[2],
            "n_days": len(observations),
            "n_obs": sum(int(row["n"]) for row in observations),
        }
        r2_values = [float(row["joint_r2"]) for row in observations if row["joint_r2"] is not None]
        result["joint_r2"], result["joint_r2_t"] = mean_t(r2_values)
        for style_name in LOB5_EX_SIZE_COLS:
            column = f"{style_name}_rank_exposure"
            values = [float(row[column]) for row in observations if row[column] is not None]
            result[column], result[f"{column}_t"] = mean_t(values)
            result[f"{style_name}_mean_abs_rank_exposure"] = mean(map(abs, values)) if values else None
        output.append(result)
    return output


def performance_by_date(
    grouped: dict[tuple[str, ...], list[FactorRow]],
) -> list[dict[str, object]]:
    output: list[dict[str, object]] = []
    projection_cache: dict[tuple[object, ...], list[list[float]]] = {}
    for key, rows in sorted(grouped.items()):
        if len(rows) < 20:
            continue
        frequency, factor_name, window_name, cap_group, price_group, date = key
        rows = sorted(rows, key=lambda row: row[0])
        symbols = [row[0] for row in rows]
        factors = [row[1] for row in rows]
        styles = [row[3] for row in rows]
        cache_key = (frequency, date, cap_group, price_group, tuple(symbols))
        basis = projection_cache.get(cache_key)
        if basis is None:
            basis = build_orthonormal_basis(styles)
            projection_cache[cache_key] = basis
        neutral_factors = residualize(factors, basis)
        target_names = ("open_to_open_d1",) if frequency == "daily" else INTRADAY_TARGETS
        for target_index, target_name in enumerate(target_names):
            eligible = [
                index for index, row in enumerate(rows)
                if row[2][target_index] is not None
            ]
            if len(eligible) < 20:
                continue
            eligible_factors = [factors[index] for index in eligible]
            eligible_neutral_factors = [neutral_factors[index] for index in eligible]
            eligible_symbols = [symbols[index] for index in eligible]
            returns = [float(rows[index][2][target_index]) for index in eligible]
            raw_ranks = ranks(eligible_factors)
            neutral_ranks = ranks(eligible_neutral_factors)
            return_ranks = ranks(returns)
            raw_order = sorted(
                range(len(eligible)),
                key=lambda index: (eligible_factors[index], eligible_symbols[index]),
            )
            neutral_order = sorted(
                range(len(eligible)),
                key=lambda index: (eligible_neutral_factors[index], eligible_symbols[index]),
            )
            output.append(
                {
                    "frequency": frequency, "factor": factor_name,
                    "window_name": window_name, "target": target_name,
                    "cap_group": cap_group, "price_group": price_group,
                    "date": date, "n": len(eligible),
                    "raw_rank_ic": pearson(raw_ranks, return_ranks),
                    "lob5_ex_size_rank_ic": pearson(neutral_ranks, return_ranks),
                    "raw_d10_d1": spread([returns[index] for index in raw_order]),
                    "lob5_ex_size_d10_d1": spread([returns[index] for index in neutral_order]),
                }
            )
    return output


def summarize_performance(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    keys = ("frequency", "factor", "window_name", "target", "cap_group", "price_group")
    grouped: dict[tuple[str, ...], list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        grouped[tuple(str(row[key]) for key in keys)].append(row)
    output: list[dict[str, object]] = []
    for key, observations in sorted(grouped.items()):
        result: dict[str, object] = dict(zip(keys, key))
        result.update(
            n_days=len(observations), n_obs=sum(int(row["n"]) for row in observations),
            avg_names=mean(int(row["n"]) for row in observations),
        )
        for metric in ("raw_rank_ic", "lob5_ex_size_rank_ic", "raw_d10_d1", "lob5_ex_size_d10_d1"):
            values = [float(row[metric]) for row in observations if row[metric] is not None]
            result[metric], result[f"{metric}_t"] = mean_t(values)
        output.append(result)
    return output


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--old-daily", default=str(PROJECT_ROOT / "data/processed/active_take_mid_gap_daily_close1000_v4_202601.csv"))
    parser.add_argument("--old-intraday", default=str(PROJECT_ROOT / "data/cache/intraday_factor_1000_1030_202601_full_signed.csv"))
    parser.add_argument("--all-trade", default=str(PROJECT_ROOT / "data/processed/stylized_fact_4_6/g1_d01_d03_factors_202512_202601_history20_v3.csv"))
    parser.add_argument("--strict-active", default=str(PROJECT_ROOT / "data/cache/stylized_fact_4_6/g1_d01_d03_active_take_diagnostic_202601.csv"))
    parser.add_argument("--returns", default=str(PROJECT_ROOT / "data/cache/daily_open_to_open_market_calendar_202512_20260206.csv"))
    parser.add_argument("--intraday-returns", default=str(PROJECT_ROOT / "data/cache/min1_ret_1031_decay_horizons_202601_clean_with_status.csv"))
    parser.add_argument("--market-caps", default=str(PROJECT_ROOT / "data/cache/daily_market_cap_202512_202601.csv"))
    parser.add_argument("--styles", default=str(PROJECT_ROOT / "data/cache/cne5_style_full_202512_202601.csv"))
    parser.add_argument("--date-from", type=int, default=20260101)
    parser.add_argument("--date-to", type=int, default=20260130)
    parser.add_argument("--output-dir", default=str(PROJECT_ROOT / "results/lob5_ex_size/existing_factors_202601"))
    args = parser.parse_args()

    daily_common, intraday_common = load_common(
        returns_path=args.returns, intraday_returns_path=args.intraday_returns,
        market_caps_path=args.market_caps, styles_path=args.styles,
        date_from=args.date_from, date_to=args.date_to,
    )
    performance_grouped, exposure_grouped = load_factors(
        old_daily_path=args.old_daily, old_intraday_path=args.old_intraday,
        all_trade_path=args.all_trade, strict_active_path=args.strict_active,
        daily_common=daily_common, intraday_common=intraday_common,
        date_from=args.date_from, date_to=args.date_to,
    )
    exposure_daily = exposure_by_date(exposure_grouped)
    performance_daily = performance_by_date(performance_grouped)
    exposure_summary = summarize_exposure(exposure_daily)
    performance_summary = summarize_performance(performance_daily)
    output_dir = Path(args.output_dir)
    write_csv(output_dir / "exposure_by_date.csv", exposure_daily)
    write_csv(output_dir / "exposure_summary.csv", exposure_summary)
    write_csv(output_dir / "performance_by_date.csv", performance_daily)
    write_csv(output_dir / "performance_summary.csv", performance_summary)
    print(
        f"daily_common={len(daily_common)} intraday_common={len(intraday_common)} "
        f"exposure_rows={len(exposure_daily)} performance_rows={len(performance_daily)} "
        f"output_dir={output_dir}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
