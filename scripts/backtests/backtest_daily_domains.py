#!/usr/bin/env python3
"""Backtest a daily factor in point-in-time market-cap/price domains."""

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


def ranks(values: list[float]) -> list[float]:
    order = sorted(range(len(values)), key=values.__getitem__)
    output = [0.0] * len(values)
    start = 0
    while start < len(order):
        end = start + 1
        while end < len(order) and values[order[end]] == values[order[start]]:
            end += 1
        rank = (start + end - 1) / 2.0 + 1.0
        for position in range(start, end):
            output[order[position]] = rank
        start = end
    return output


def pearson(xs: list[float], ys: list[float]) -> float | None:
    if len(xs) < 3:
        return None
    mx, my = mean(xs), mean(ys)
    vx = sum((x - mx) ** 2 for x in xs)
    vy = sum((y - my) ** 2 for y in ys)
    if vx <= 0 or vy <= 0:
        return None
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / math.sqrt(vx * vy)


def mean_t(values: list[float]) -> tuple[float, float]:
    if not values:
        return 0.0, 0.0
    average = mean(values)
    volatility = stdev(values) if len(values) > 1 else 0.0
    t_stat = average / (volatility / math.sqrt(len(values))) if volatility > 0 else 0.0
    return average, t_stat


def neutralize(values: list[float], exposures: list[list[float]]) -> list[float]:
    """Return OLS residuals after an intercept and cross-sectional exposures."""
    residuals = [value - mean(values) for value in values]
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
        projection = sum(value * base for value, base in zip(residuals, basis))
        residuals = [value - projection * base for value, base in zip(residuals, basis)]
    return residuals


def normalize_date(value: str) -> str:
    return value.replace("-", "")


def domain(previous_market_cap: float, signal_open: float, symbol: str) -> tuple[str, str] | None:
    if previous_market_cap < 500_000:
        cap_group = CAP_GROUPS[0]
    elif previous_market_cap < 5_000_000:
        cap_group = CAP_GROUPS[1]
    else:
        cap_group = CAP_GROUPS[2]

    is_star = symbol.startswith(("SH688", "SH689"))
    if not is_star:
        price_group = PRICE_GROUPS[0] if signal_open < 10 else PRICE_GROUPS[1]
    elif signal_open >= 10:
        price_group = PRICE_GROUPS[2]
    else:
        return None
    return cap_group, price_group


def load_return_rows(
    path: Path,
    return_col: str = "o2o_ret",
    exit_date_col: str = "next_date",
) -> tuple[dict[tuple[str, str], dict[str, object]], dict[str, str]]:
    raw: dict[tuple[str, str], dict[str, object]] = {}
    dates: set[str] = set()
    with path.open(newline="") as handle:
        for row in csv.DictReader(handle):
            signal_open = parse_float(row.get("open"))
            ret = parse_float(row.get(return_col))
            if signal_open is None or ret is None:
                continue
            date = row["date"]
            dates.add(date)
            raw[(row["symbol"], date)] = {
                "open": signal_open,
                "ret": ret,
                "next_date": row["next_date"],
                "exit_date": row[exit_date_col],
            }
    ordered_dates = sorted(dates)
    previous_dates = {date: ordered_dates[index - 1] for index, date in enumerate(ordered_dates) if index}
    return raw, previous_dates


def select_return_row(
    returns: dict[tuple[str, str], dict[str, object]],
    symbol: str,
    signal_date: str,
    lag_trading_days: int,
) -> tuple[str, dict[str, object]] | None:
    target_date = signal_date
    target_row = returns.get((symbol, target_date))
    for _ in range(lag_trading_days):
        if target_row is None:
            return None
        target_date = str(target_row["next_date"])
        target_row = returns.get((symbol, target_date))
    return (target_date, target_row) if target_row is not None else None


def summarize(values: list[float]) -> dict[str, float]:
    average, t_stat = mean_t(values)
    volatility = stdev(values) if len(values) > 1 else 0.0
    return {
        "avg": average,
        "t": t_stat,
        "cum": math.prod(1.0 + value for value in values) - 1.0 if values else 0.0,
        "sharpe": average / volatility * math.sqrt(252.0) if volatility > 0 else 0.0,
        "win_rate": mean(value > 0 for value in values) if values else 0.0,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--factor", required=True)
    parser.add_argument("--factor-col", required=True)
    parser.add_argument("--returns", required=True)
    parser.add_argument("--return-col", default="o2o_ret")
    parser.add_argument("--return-exit-date-col", default="next_date")
    parser.add_argument(
        "--return-lag-trading-days",
        type=int,
        choices=(0, 1),
        default=1,
        help="0 for a return beginning on the signal date; 1 for T+1 returns.",
    )
    parser.add_argument("--market-caps", required=True)
    parser.add_argument("--risk-exposures")
    parser.add_argument("--risk-cols", nargs="+")
    parser.add_argument("--daily-out", required=True)
    parser.add_argument("--summary-out", required=True)
    args = parser.parse_args()

    returns, previous_dates = load_return_rows(
        Path(args.returns),
        args.return_col,
        args.return_exit_date_col,
    )
    market_caps: dict[tuple[str, str], float] = {}
    with Path(args.market_caps).open(newline="") as handle:
        for row in csv.DictReader(handle):
            value = parse_float(row.get("total_mv"))
            if value is not None:
                market_caps[(row["symbol"], row["date"])] = value

    risk_exposures: dict[tuple[str, str], list[float]] | None = None
    if args.risk_exposures:
        if not args.risk_cols:
            raise ValueError("--risk-cols is required with --risk-exposures")
        risk_exposures = {}
        with Path(args.risk_exposures).open(newline="") as handle:
            for row in csv.DictReader(handle):
                values = [parse_float(row.get(column)) for column in args.risk_cols]
                if all(value is not None for value in values):
                    risk_exposures[(row["symbol"], normalize_date(row["date"]))] = [
                        float(value) for value in values if value is not None
                    ]

    grouped: dict[tuple[str, str, str], list[dict[str, object]]] = defaultdict(list)
    excluded_star_below_10 = 0
    with Path(args.factor).open(newline="") as handle:
        for row in csv.DictReader(handle):
            factor = parse_float(row.get(args.factor_col))
            date, symbol = row["date"], row["symbol"]
            signal_row = returns.get((symbol, date))
            previous_date = previous_dates.get(date)
            if factor is None or signal_row is None or previous_date is None:
                continue
            selected_return = select_return_row(
                returns,
                symbol,
                date,
                args.return_lag_trading_days,
            )
            previous_cap = market_caps.get((symbol, previous_date))
            risk = risk_exposures.get((symbol, date)) if risk_exposures is not None else []
            if selected_return is None or previous_cap is None or risk is None:
                continue
            entry_date, entry_row = selected_return
            group = domain(previous_cap, float(signal_row["open"]), symbol)
            if group is None:
                excluded_star_below_10 += 1
                continue
            grouped[(group[0], group[1], date)].append(
                {
                    "symbol": symbol,
                    "factor": factor,
                    "ret": float(entry_row["ret"]),
                    "entry_date": entry_date,
                    "exit_date": str(entry_row["exit_date"]),
                    "previous_cap": previous_cap,
                    "signal_open": float(signal_row["open"]),
                    "risk": risk,
                }
            )

    daily_rows: list[dict[str, object]] = []
    for cap_group in CAP_GROUPS:
        for price_group in PRICE_GROUPS:
            dates = sorted(date for cap, price, date in grouped if (cap, price) == (cap_group, price_group))
            for date in dates:
                rows = sorted(grouped[(cap_group, price_group, date)], key=lambda item: (item["factor"], item["symbol"]))
                n = len(rows)
                if n < 20:
                    continue
                raw_factors = [float(row["factor"]) for row in rows]
                factors = (
                    neutralize(raw_factors, [list(row["risk"]) for row in rows])
                    if risk_exposures is not None
                    else raw_factors
                )
                returns_for_day = [float(row["ret"]) for row in rows]
                residual_order = sorted(
                    range(n), key=lambda index: (factors[index], rows[index]["symbol"])
                )
                ordered_returns = [returns_for_day[index] for index in residual_order]
                bucket = max(1, n // 10)
                bottom_return = mean(ordered_returns[:bucket])
                top_return = mean(ordered_returns[-bucket:])
                universe_return = mean(returns_for_day)
                top_bottom = top_return - bottom_return
                daily_rows.append(
                    {
                        "cap_group": cap_group,
                        "price_group": price_group,
                        "date": date,
                        "entry_date": rows[0]["entry_date"],
                        "exit_date": rows[0]["exit_date"],
                        "n": n,
                        "raw_rank_ic": pearson(ranks(raw_factors), ranks(returns_for_day)),
                        "rank_ic": pearson(ranks(factors), ranks(returns_for_day)),
                        "pearson_ic": pearson(factors, returns_for_day),
                        "d10_d1": top_bottom,
                        "d1_ret": bottom_return,
                        "d10_ret": top_return,
                        "universe_ret": universe_return,
                        "d1_excess": bottom_return - universe_return,
                        "d10_excess": top_return - universe_return,
                        "avg_prev_cap_yi": mean(float(row["previous_cap"]) for row in rows) / 10_000,
                        "avg_signal_open": mean(float(row["signal_open"]) for row in rows),
                    }
                )

    daily_fields = list(daily_rows[0]) if daily_rows else [
        "cap_group", "price_group", "date", "entry_date", "exit_date", "n",
        "raw_rank_ic", "rank_ic", "pearson_ic", "d10_d1", "d1_ret", "d10_ret",
        "universe_ret", "d1_excess", "d10_excess", "avg_prev_cap_yi", "avg_signal_open",
    ]
    Path(args.daily_out).parent.mkdir(parents=True, exist_ok=True)
    with Path(args.daily_out).open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=daily_fields)
        writer.writeheader()
        writer.writerows(daily_rows)

    summary_rows: list[dict[str, object]] = []
    for cap_group in CAP_GROUPS:
        for price_group in PRICE_GROUPS:
            rows = [row for row in daily_rows if (row["cap_group"], row["price_group"]) == (cap_group, price_group)]
            spreads = [float(row["d10_d1"]) for row in rows]
            d1_returns = [float(row["d1_ret"]) for row in rows]
            d1_excess_returns = [float(row["d1_excess"]) for row in rows]
            raw_rank_ics = [float(row["raw_rank_ic"]) for row in rows if row["raw_rank_ic"] is not None]
            rank_ics = [float(row["rank_ic"]) for row in rows if row["rank_ic"] is not None]
            pearson_ics = [float(row["pearson_ic"]) for row in rows if row["pearson_ic"] is not None]
            spread_stats = summarize(spreads)
            d1_stats = summarize(d1_returns)
            d1_excess_stats = summarize(d1_excess_returns)
            raw_rank_ic, raw_rank_ic_t = mean_t(raw_rank_ics)
            rank_ic, rank_ic_t = mean_t(rank_ics)
            pearson_ic, pearson_ic_t = mean_t(pearson_ics)
            summary_rows.append(
                {
                    "cap_group": cap_group,
                    "price_group": price_group,
                    "n_obs": sum(int(row["n"]) for row in rows),
                    "n_days": len(rows),
                    "avg_names": mean(int(row["n"]) for row in rows) if rows else 0.0,
                    "d10_d1": spread_stats["avg"],
                    "d10_d1_t": spread_stats["t"],
                    "d10_d1_cum": spread_stats["cum"],
                    "d10_d1_sharpe": spread_stats["sharpe"],
                    "d10_d1_win_rate": spread_stats["win_rate"],
                    "d1_ret": d1_stats["avg"],
                    "d1_ret_t": d1_stats["t"],
                    "d1_ret_cum": d1_stats["cum"],
                    "d1_ret_sharpe": d1_stats["sharpe"],
                    "d1_ret_win_rate": d1_stats["win_rate"],
                    "d1_excess": d1_excess_stats["avg"],
                    "d1_excess_t": d1_excess_stats["t"],
                    "d1_excess_cum": d1_excess_stats["cum"],
                    "d1_excess_sharpe": d1_excess_stats["sharpe"],
                    "d1_excess_win_rate": d1_excess_stats["win_rate"],
                    "raw_rank_ic": raw_rank_ic,
                    "raw_rank_ic_t": raw_rank_ic_t,
                    "rank_ic": rank_ic,
                    "rank_ic_t": rank_ic_t,
                    "pearson_ic": pearson_ic,
                    "pearson_ic_t": pearson_ic_t,
                }
            )
    Path(args.summary_out).parent.mkdir(parents=True, exist_ok=True)
    with Path(args.summary_out).open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(summary_rows[0]))
        writer.writeheader()
        writer.writerows(summary_rows)

    print(f"daily_rows={len(daily_rows)} excluded_star_below_10={excluded_star_below_10}")
    print(f"wrote {args.daily_out} and {args.summary_out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
