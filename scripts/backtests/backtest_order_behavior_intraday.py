#!/usr/bin/env python3
"""Cross-sectional intraday backtest for order-behavior log-ratio factors."""

from __future__ import annotations

import argparse
import csv
import math
from collections import defaultdict
from pathlib import Path
from statistics import mean, stdev


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_FACTORS = ("vr_log", "cr_log", "single_size_ratio_log")
DEFAULT_RETURNS = ("ret_1031_1040", "ret_1031_1045")


def parse_float(value: str | None) -> float | None:
    try:
        result = float(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None
    return result if result is not None and math.isfinite(result) else None


def ranks(values: list[float]) -> list[float]:
    order = sorted(range(len(values)), key=values.__getitem__)
    result = [0.0] * len(values)
    start = 0
    while start < len(order):
        end = start + 1
        while end < len(order) and values[order[end]] == values[order[start]]:
            end += 1
        rank = (start + end - 1) / 2.0 + 1.0
        for position in range(start, end):
            result[order[position]] = rank
        start = end
    return result


def pearson(xs: list[float], ys: list[float]) -> float | None:
    if len(xs) < 3:
        return None
    mean_x, mean_y = mean(xs), mean(ys)
    var_x = sum((value - mean_x) ** 2 for value in xs)
    var_y = sum((value - mean_y) ** 2 for value in ys)
    if var_x <= 0 or var_y <= 0:
        return None
    covariance = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    return covariance / math.sqrt(var_x * var_y)


def mean_t(values: list[float]) -> tuple[float, float]:
    if not values:
        return 0.0, 0.0
    average = mean(values)
    if len(values) < 2:
        return average, 0.0
    volatility = stdev(values)
    t_stat = average / (volatility / math.sqrt(len(values))) if volatility > 0 else 0.0
    return average, t_stat


def neutralize(values: list[float], exposures: list[list[float]]) -> list[float]:
    mean_value = mean(values)
    residual = [value - mean_value for value in values]
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
    for basis in orthonormal_columns:
        projection = sum(value * base for value, base in zip(residual, basis))
        residual = [value - projection * base for value, base in zip(residual, basis)]
    return residual


def decile_means(
    factors: list[float],
    targets: list[float],
    symbols: list[str],
) -> list[float]:
    """Return ten equal-count portfolio means, ordered from low to high factor."""
    order = sorted(range(len(factors)), key=lambda index: (factors[index], symbols[index]))
    result = []
    for bucket in range(10):
        start = math.floor(bucket * len(order) / 10)
        end = math.floor((bucket + 1) * len(order) / 10)
        result.append(mean(targets[index] for index in order[start:end]))
    return result


def load_factor_rows(
    path: str,
    factor_columns: list[str],
) -> dict[str, list[dict[str, object]]]:
    by_date: dict[str, list[dict[str, object]]] = defaultdict(list)
    seen: set[tuple[str, str]] = set()
    with open(path, newline="") as handle:
        reader = csv.DictReader(handle)
        missing = set(factor_columns + ["symbol", "date", "is_valid"]) - set(
            reader.fieldnames or []
        )
        if missing:
            raise ValueError(f"factor input missing columns: {sorted(missing)}")
        for row in reader:
            if row.get("is_valid", "").lower() != "true":
                continue
            key = (row["symbol"], row["date"].replace("-", ""))
            if key in seen:
                raise ValueError(f"duplicate factor row: {key}")
            values = {column: parse_float(row.get(column)) for column in factor_columns}
            if any(value is None for value in values.values()):
                continue
            seen.add(key)
            by_date[key[1]].append(
                {
                    "symbol": key[0],
                    **{column: float(value) for column, value in values.items() if value is not None},
                }
            )
    return by_date


def load_returns(
    path: str,
    return_columns: list[str],
) -> dict[tuple[str, str], dict[str, float]]:
    result: dict[tuple[str, str], dict[str, float]] = {}
    with open(path, newline="") as handle:
        reader = csv.DictReader(handle)
        required = set(return_columns + ["symbol", "date", "is_st", "is_suspended"])
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"return input missing columns: {sorted(missing)}")
        for row in reader:
            if row.get("is_st") != "0" or row.get("is_suspended") != "0":
                continue
            values = {column: parse_float(row.get(column)) for column in return_columns}
            if any(value is None for value in values.values()):
                continue
            key = (row["symbol"], row["date"].replace("-", ""))
            result[key] = {
                column: float(value) for column, value in values.items() if value is not None
            }
    return result


def load_previous_vectors(
    path: str,
    value_columns: list[str],
) -> dict[tuple[str, str], list[float]]:
    by_symbol: dict[str, list[tuple[str, list[float]]]] = defaultdict(list)
    with open(path, newline="") as handle:
        reader = csv.DictReader(handle)
        missing = set(value_columns + ["symbol", "date"]) - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"risk input missing columns: {sorted(missing)}")
        for row in reader:
            values = [parse_float(row.get(column)) for column in value_columns]
            if all(value is not None for value in values):
                by_symbol[row["symbol"]].append(
                    (
                        row["date"].replace("-", ""),
                        [float(value) for value in values if value is not None],
                    )
                )
    previous: dict[tuple[str, str], list[float]] = {}
    for symbol, observations in by_symbol.items():
        observations.sort(key=lambda item: item[0])
        for index in range(1, len(observations)):
            date, _ = observations[index]
            previous[(symbol, date)] = observations[index - 1][1]
    return previous


def build_daily_rows(
    factors_by_date: dict[str, list[dict[str, object]]],
    returns: dict[tuple[str, str], dict[str, float]],
    factor_columns: list[str],
    return_columns: list[str],
    minimum_names: int,
    previous_risks: dict[tuple[str, str], list[float]] | None = None,
) -> list[dict[str, object]]:
    daily_rows: list[dict[str, object]] = []
    for date, factor_rows in sorted(factors_by_date.items()):
        joined = []
        for factor_row in factor_rows:
            symbol = str(factor_row["symbol"])
            future_returns = returns.get((symbol, date))
            risk = previous_risks.get((symbol, date)) if previous_risks is not None else []
            if future_returns is not None and risk is not None:
                joined.append((symbol, factor_row, future_returns, risk))
        if len(joined) < minimum_names:
            continue

        symbols = [row[0] for row in joined]
        for factor_column in factor_columns:
            raw_factor_values = [float(row[1][factor_column]) for row in joined]
            factor_values = (
                neutralize(raw_factor_values, [row[3] for row in joined])
                if previous_risks is not None
                else raw_factor_values
            )
            factor_ranks = ranks(factor_values)
            for return_column in return_columns:
                future_returns = [float(row[2][return_column]) for row in joined]
                absolute_returns = [abs(value) for value in future_returns]
                return_deciles = decile_means(factor_values, future_returns, symbols)
                absolute_deciles = decile_means(factor_values, absolute_returns, symbols)
                daily_row: dict[str, object] = {
                    "factor_name": factor_column,
                    "factor_variant": "cne5_neutral" if previous_risks is not None else "raw",
                    "return_horizon": return_column,
                    "date": date,
                    "n": len(joined),
                    "rank_ic": pearson(factor_ranks, ranks(future_returns)),
                    "abs_rank_ic": pearson(factor_ranks, ranks(absolute_returns)),
                    "universe_ret": mean(future_returns),
                    "universe_abs_ret": mean(absolute_returns),
                    "q10_minus_q1": return_deciles[-1] - return_deciles[0],
                    "abs_q10_minus_q1": absolute_deciles[-1] - absolute_deciles[0],
                }
                for index, value in enumerate(return_deciles, start=1):
                    daily_row[f"q{index}_ret"] = value
                for index, value in enumerate(absolute_deciles, start=1):
                    daily_row[f"q{index}_abs_ret"] = value
                daily_rows.append(daily_row)
    return daily_rows


def build_summary_rows(
    daily_rows: list[dict[str, object]],
    factor_columns: list[str],
    return_columns: list[str],
) -> list[dict[str, object]]:
    summary_rows: list[dict[str, object]] = []
    for factor_column in factor_columns:
        for return_column in return_columns:
            rows = [
                row
                for row in daily_rows
                if row["factor_name"] == factor_column
                and row["return_horizon"] == return_column
            ]
            rank_ics = [float(row["rank_ic"]) for row in rows]
            abs_rank_ics = [float(row["abs_rank_ic"]) for row in rows]
            spreads = [float(row["q10_minus_q1"]) for row in rows]
            abs_spreads = [float(row["abs_q10_minus_q1"]) for row in rows]
            rank_ic, rank_ic_t = mean_t(rank_ics)
            abs_rank_ic, abs_rank_ic_t = mean_t(abs_rank_ics)
            spread, spread_t = mean_t(spreads)
            abs_spread, abs_spread_t = mean_t(abs_spreads)
            row: dict[str, object] = {
                "factor_name": factor_column,
                "factor_variant": str(rows[0]["factor_variant"]) if rows else "",
                "return_horizon": return_column,
                "n_days": len(rows),
                "n_obs": sum(int(item["n"]) for item in rows),
                "avg_names": mean(int(item["n"]) for item in rows) if rows else 0,
                "rank_ic": rank_ic,
                "rank_ic_t": rank_ic_t,
                "rank_ic_ir": rank_ic / stdev(rank_ics) if len(rank_ics) > 1 and stdev(rank_ics) > 0 else 0,
                "rank_ic_positive_share": sum(value > 0 for value in rank_ics) / len(rank_ics) if rank_ics else 0,
                "abs_rank_ic": abs_rank_ic,
                "abs_rank_ic_t": abs_rank_ic_t,
                "abs_rank_ic_positive_share": sum(value > 0 for value in abs_rank_ics) / len(abs_rank_ics) if abs_rank_ics else 0,
                "q10_minus_q1": spread,
                "q10_minus_q1_t": spread_t,
                "q10_minus_q1_bps": spread * 10_000,
                "q10_minus_q1_net_5bps": spread - 4 * 5 / 10_000,
                "q10_minus_q1_net_10bps": spread - 4 * 10 / 10_000,
                "q10_minus_q1_net_20bps": spread - 4 * 20 / 10_000,
                "abs_q10_minus_q1": abs_spread,
                "abs_q10_minus_q1_t": abs_spread_t,
                "abs_q10_minus_q1_bps": abs_spread * 10_000,
            }
            for bucket in range(1, 11):
                row[f"q{bucket}_ret_bps"] = mean(
                    float(item[f"q{bucket}_ret"]) for item in rows
                ) * 10_000 if rows else 0
                row[f"q{bucket}_abs_ret_bps"] = mean(
                    float(item[f"q{bucket}_abs_ret"]) for item in rows
                ) * 10_000 if rows else 0
            summary_rows.append(row)
    return summary_rows


def write_rows(path: str, rows: list[dict[str, object]]) -> None:
    if not rows:
        raise ValueError(f"no rows produced for {path}")
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--factors",
        default=str(PROJECT_ROOT / "data/processed/order_behavior_ratio_1000_1030_202601.csv"),
    )
    parser.add_argument(
        "--returns",
        default=str(PROJECT_ROOT / "data/cache/min1_ret_1031_1040_1045_202601_clean_with_status.csv"),
    )
    parser.add_argument("--factor-cols", nargs="+", default=list(DEFAULT_FACTORS))
    parser.add_argument("--return-cols", nargs="+", default=list(DEFAULT_RETURNS))
    parser.add_argument("--risk-exposures")
    parser.add_argument("--risk-cols", nargs="+")
    parser.add_argument("--minimum-names", type=int, default=100)
    parser.add_argument(
        "--daily-out",
        default=str(PROJECT_ROOT / "results/intraday/order_behavior_1000_1030_daily.csv"),
    )
    parser.add_argument(
        "--summary-out",
        default=str(PROJECT_ROOT / "results/intraday/order_behavior_1000_1030_summary.csv"),
    )
    args = parser.parse_args()
    if args.minimum_names < 20:
        raise ValueError("minimum-names must be at least 20 for decile portfolios")
    if bool(args.risk_exposures) != bool(args.risk_cols):
        raise ValueError("--risk-exposures and --risk-cols must be provided together")

    factors_by_date = load_factor_rows(args.factors, args.factor_cols)
    returns = load_returns(args.returns, args.return_cols)
    previous_risks = (
        load_previous_vectors(args.risk_exposures, args.risk_cols)
        if args.risk_exposures
        else None
    )
    daily_rows = build_daily_rows(
        factors_by_date,
        returns,
        args.factor_cols,
        args.return_cols,
        args.minimum_names,
        previous_risks,
    )
    summary_rows = build_summary_rows(daily_rows, args.factor_cols, args.return_cols)
    write_rows(args.daily_out, daily_rows)
    write_rows(args.summary_out, summary_rows)
    print(
        f"done daily_rows={len(daily_rows)} summary_rows={len(summary_rows)} "
        f"daily_out={args.daily_out} summary_out={args.summary_out}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
