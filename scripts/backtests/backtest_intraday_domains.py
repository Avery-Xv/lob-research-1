#!/usr/bin/env python3
"""Backtest an intraday LOB factor in point-in-time market-cap/price domains."""

from __future__ import annotations

import argparse
import csv
import math
from collections import defaultdict
from pathlib import Path
from statistics import mean, stdev


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CAP_GROUPS = ("cap_lt_50yi", "cap_50_500yi", "cap_ge_500yi")
PRICE_GROUPS = ("non_star_lt_10", "non_star_ge_10", "star_ge_10")


def parse_float(value: str | None) -> float | None:
    try:
        result = float(value) if value is not None else None
    except (TypeError, ValueError):
        return None
    return result if result is not None and math.isfinite(result) else None


def normalize_date(value: str) -> str:
    return value.replace("-", "")


def pearson(xs: list[float], ys: list[float]) -> float | None:
    if len(xs) < 3:
        return None
    mx, my = mean(xs), mean(ys)
    vx = sum((x - mx) ** 2 for x in xs)
    vy = sum((y - my) ** 2 for y in ys)
    if vx <= 0 or vy <= 0:
        return None
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / math.sqrt(vx * vy)


def ranks(values: list[float]) -> list[float]:
    order = sorted(range(len(values)), key=values.__getitem__)
    result = [0.0] * len(values)
    start = 0
    while start < len(order):
        end = start + 1
        while end < len(order) and values[order[end]] == values[order[start]]:
            end += 1
        rank = (start + end - 1) / 2.0 + 1.0
        for pos in range(start, end):
            result[order[pos]] = rank
        start = end
    return result


def mean_t(values: list[float]) -> tuple[float, float]:
    if not values:
        return 0.0, 0.0
    avg = mean(values)
    if len(values) < 2:
        return avg, 0.0
    vol = stdev(values)
    return avg, avg / (vol / math.sqrt(len(values))) if vol > 0 else 0.0


def load_previous_values(path: str, value_col: str) -> dict[tuple[str, str], float]:
    by_symbol: dict[str, list[tuple[str, float]]] = defaultdict(list)
    with open(path, newline="") as handle:
        for row in csv.DictReader(handle):
            value = parse_float(row.get(value_col))
            if value is not None:
                by_symbol[row["symbol"]].append((row["date"], value))

    previous = {}
    for symbol, observations in by_symbol.items():
        observations.sort()
        for index in range(1, len(observations)):
            date, _ = observations[index]
            previous[(symbol, date)] = observations[index - 1][1]
    return previous


def load_previous_vectors(path: str, value_cols: list[str]) -> dict[tuple[str, str], list[float]]:
    by_symbol: dict[str, list[tuple[str, list[float]]]] = defaultdict(list)
    with open(path, newline="") as handle:
        for row in csv.DictReader(handle):
            values = [parse_float(row.get(column)) for column in value_cols]
            if all(value is not None for value in values):
                by_symbol[row["symbol"]].append(
                    (
                        normalize_date(row["date"]),
                        [float(value) for value in values if value is not None],
                    )
                )

    previous = {}
    for symbol, observations in by_symbol.items():
        observations.sort(key=lambda item: item[0])
        for index in range(1, len(observations)):
            date, _ = observations[index]
            previous[(symbol, date)] = observations[index - 1][1]
    return previous


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


def order_returns(
    factors: list[float],
    returns: list[float],
    symbols: list[str],
) -> list[float]:
    order = sorted(range(len(factors)), key=lambda index: (factors[index], symbols[index]))
    return [returns[index] for index in order]


def load_factors(paths: list[str], factor_col: str) -> dict[str, list[dict[str, object]]]:
    unique: dict[tuple[str, str], dict[str, object]] = {}
    for path in paths:
        with open(path, newline="") as handle:
            for row in csv.DictReader(handle):
                factor = parse_float(row.get(factor_col))
                start_mid = parse_float(row.get("start_mid"))
                events = parse_float(row.get("valid_lag_events"))
                if factor is None or start_mid is None or events is None or start_mid <= 0:
                    continue
                key = (row["symbol"], row["date"])
                candidate = {
                    "symbol": row["symbol"],
                    "date": row["date"],
                    "factor": factor,
                    "start_mid": start_mid,
                    "events": events,
                }
                existing = unique.get(key)
                if existing is not None and any(
                    not math.isclose(float(existing[name]), float(candidate[name]), rel_tol=1e-12)
                    for name in ("factor", "start_mid", "events")
                ):
                    raise ValueError(f"conflicting duplicate factor row: {key}")
                unique[key] = candidate

    by_date: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in unique.values():
        by_date[str(row["date"])].append(row)
    return by_date


def load_returns(path: str, return_col: str) -> dict[tuple[str, str], float]:
    result = {}
    with open(path, newline="") as handle:
        for row in csv.DictReader(handle):
            value = parse_float(row.get(return_col))
            if value is None or row.get("is_st") != "0" or row.get("is_suspended") != "0":
                continue
            result[(row["symbol"], row["date"])] = value
    return result


def is_star(symbol: str) -> bool:
    return symbol.startswith(("SH688", "SH689"))


def domain(prev_market_cap: float, start_mid: float, symbol: str) -> tuple[str, str] | None:
    if prev_market_cap < 500_000:
        cap_group = CAP_GROUPS[0]
    elif prev_market_cap < 5_000_000:
        cap_group = CAP_GROUPS[1]
    else:
        cap_group = CAP_GROUPS[2]

    star = is_star(symbol)
    if not star and start_mid < 10:
        price_group = PRICE_GROUPS[0]
    elif not star and start_mid >= 10:
        price_group = PRICE_GROUPS[1]
    elif star and start_mid >= 10:
        price_group = PRICE_GROUPS[2]
    else:
        return None
    return cap_group, price_group


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--factors",
        nargs="+",
        default=[
            str(PROJECT_ROOT / "data/cache/intraday_factor_1000_1030_202601_size_ge_p20.csv"),
            str(PROJECT_ROOT / "data/cache/intraday_factor_1000_1030_202601_size_le_p80.csv"),
        ],
    )
    parser.add_argument(
        "--returns",
        default=str(PROJECT_ROOT / "data/cache/min1_ret_1031_horizons_202601_filtered_base.csv"),
    )
    parser.add_argument(
        "--market-caps",
        default=str(PROJECT_ROOT / "data/cache/daily_market_cap_202512_202601.csv"),
    )
    parser.add_argument("--risk-exposures")
    parser.add_argument("--risk-col", default="residual_volatility")
    parser.add_argument("--risk-cols", nargs="+")
    parser.add_argument("--factor-col", default="active_take_mid_gap_over_start_mid")
    parser.add_argument("--return-col", default="ret_1031_1045")
    parser.add_argument("--active-quantile", type=float, default=0.8)
    parser.add_argument(
        "--daily-out",
        default=str(PROJECT_ROOT / "results/intraday/domain_9way_1000_1030_to_1031_1045_daily.csv"),
    )
    parser.add_argument(
        "--summary-out",
        default=str(PROJECT_ROOT / "results/intraday/domain_9way_1000_1030_to_1031_1045_summary.csv"),
    )
    args = parser.parse_args()
    if not 0 < args.active_quantile <= 1:
        raise ValueError("active-quantile must be in (0, 1]")

    factors_by_date = load_factors(args.factors, args.factor_col)
    returns = load_returns(args.returns, args.return_col)
    previous_caps = load_previous_values(args.market_caps, "total_mv")
    risk_cols = args.risk_cols or [args.risk_col]
    previous_risks = (
        load_previous_vectors(args.risk_exposures, risk_cols) if args.risk_exposures else None
    )
    grouped: dict[tuple[str, str, str], list[dict[str, float]]] = defaultdict(list)
    excluded_star_below_10 = 0
    active_observations = 0

    for date, rows in sorted(factors_by_date.items()):
        rows.sort(key=lambda row: (-float(row["events"]), str(row["symbol"])))
        keep = max(1, math.floor(len(rows) * args.active_quantile))
        for row in rows[:keep]:
            symbol = str(row["symbol"])
            ret = returns.get((symbol, date))
            prev_cap = previous_caps.get((symbol, date))
            risk = previous_risks.get((symbol, date)) if previous_risks is not None else [0.0]
            if ret is None or prev_cap is None or risk is None:
                continue
            active_observations += 1
            group = domain(prev_cap, float(row["start_mid"]), symbol)
            if group is None:
                excluded_star_below_10 += 1
                continue
            grouped[(group[0], group[1], date)].append(
                {
                    "symbol": symbol,
                    "factor": float(row["factor"]),
                    "ret": ret,
                    "prev_cap": prev_cap,
                    "start_mid": float(row["start_mid"]),
                    "risk": risk,
                }
            )

    daily_rows = []
    for cap_group in CAP_GROUPS:
        for price_group in PRICE_GROUPS:
            dates = sorted(date for c, p, date in grouped if c == cap_group and p == price_group)
            for date in dates:
                rows = sorted(grouped[(cap_group, price_group, date)], key=lambda row: row["factor"])
                n = len(rows)
                if n < 20:
                    continue
                raw_factors = [row["factor"] for row in rows]
                factors = (
                    neutralize(raw_factors, [row["risk"] for row in rows])
                    if previous_risks is not None
                    else raw_factors
                )
                rets = [row["ret"] for row in rows]
                ordered_rets = order_returns(
                    factors,
                    rets,
                    [str(row["symbol"]) for row in rows],
                )
                bucket = max(1, n // 10)
                top_count = max(1, math.ceil(n * 0.2))
                universe_ret = mean(rets)
                daily_rows.append(
                    {
                        "cap_group": cap_group,
                        "price_group": price_group,
                        "date": date,
                        "n": n,
                        "raw_rank_ic": pearson(ranks(raw_factors), ranks(rets)),
                        "rank_ic": pearson(ranks(factors), ranks(rets)),
                        "pearson_ic": pearson(factors, rets),
                        "d10_d1": mean(ordered_rets[-bucket:]) - mean(ordered_rets[:bucket]),
                        "bottom20_ret": mean(ordered_rets[:top_count]),
                        "top20_ret": mean(ordered_rets[-top_count:]),
                        "universe_ret": universe_ret,
                        "bottom20_excess": mean(ordered_rets[:top_count]) - universe_ret,
                        "top20_excess": mean(ordered_rets[-top_count:]) - universe_ret,
                        "avg_prev_cap_yi": mean(row["prev_cap"] for row in rows) / 10_000,
                        "avg_start_mid": mean(row["start_mid"] for row in rows),
                    }
                )

    daily_fields = [
        "cap_group", "price_group", "date", "n", "raw_rank_ic", "rank_ic", "pearson_ic",
        "d10_d1", "bottom20_ret", "top20_ret", "universe_ret",
        "bottom20_excess", "top20_excess",
        "avg_prev_cap_yi", "avg_start_mid",
    ]
    Path(args.daily_out).parent.mkdir(parents=True, exist_ok=True)
    with open(args.daily_out, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=daily_fields)
        writer.writeheader()
        writer.writerows(daily_rows)

    summary_rows = []
    for cap_group in CAP_GROUPS:
        for price_group in PRICE_GROUPS:
            rows = [
                row for row in daily_rows
                if row["cap_group"] == cap_group and row["price_group"] == price_group
            ]
            raw_rank_ic, raw_rank_ic_t = mean_t([float(row["raw_rank_ic"]) for row in rows])
            rank_ic, rank_ic_t = mean_t([float(row["rank_ic"]) for row in rows])
            pearson_ic, pearson_ic_t = mean_t([float(row["pearson_ic"]) for row in rows])
            d10_d1, d10_d1_t = mean_t([float(row["d10_d1"]) for row in rows])
            bottom20, bottom20_t = mean_t([float(row["bottom20_excess"]) for row in rows])
            top20_ret, top20_ret_t = mean_t([float(row["top20_ret"]) for row in rows])
            universe_ret, universe_ret_t = mean_t([float(row["universe_ret"]) for row in rows])
            top20, top20_t = mean_t([float(row["top20_excess"]) for row in rows])
            summary_rows.append(
                {
                    "cap_group": cap_group,
                    "price_group": price_group,
                    "n_obs": sum(int(row["n"]) for row in rows),
                    "n_days": len(rows),
                    "avg_names": mean(int(row["n"]) for row in rows) if rows else 0,
                    "raw_rank_ic": raw_rank_ic,
                    "raw_rank_ic_t": raw_rank_ic_t,
                    "rank_ic": rank_ic,
                    "rank_ic_t": rank_ic_t,
                    "pearson_ic": pearson_ic,
                    "pearson_ic_t": pearson_ic_t,
                    "d10_d1": d10_d1,
                    "d10_d1_t": d10_d1_t,
                    "bottom20_excess": bottom20,
                    "bottom20_excess_t": bottom20_t,
                    "top20_ret": top20_ret,
                    "top20_ret_t": top20_ret_t,
                    "universe_ret": universe_ret,
                    "universe_ret_t": universe_ret_t,
                    "top20_excess": top20,
                    "top20_excess_t": top20_t,
                }
            )

    summary_fields = list(summary_rows[0])
    Path(args.summary_out).parent.mkdir(parents=True, exist_ok=True)
    with open(args.summary_out, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=summary_fields)
        writer.writeheader()
        writer.writerows(summary_rows)

    print(
        f"dates={len(factors_by_date)} active_obs={active_observations} "
        f"excluded_star_below_10={excluded_star_below_10} daily_rows={len(daily_rows)}"
    )
    print(f"wrote {args.daily_out} and {args.summary_out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
