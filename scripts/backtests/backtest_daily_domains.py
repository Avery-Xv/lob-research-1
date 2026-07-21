#!/usr/bin/env python3
"""Point-in-time 3x3 market-cap/price domain backtest for daily factors."""

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
    mx, my = mean(xs), mean(ys)
    vx = sum((value - mx) ** 2 for value in xs)
    vy = sum((value - my) ** 2 for value in ys)
    if vx <= 0 or vy <= 0:
        return None
    covariance = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    return covariance / math.sqrt(vx * vy)


def mean_t(values: list[float]) -> tuple[float, float]:
    if not values:
        return 0.0, 0.0
    average = mean(values)
    if len(values) < 2:
        return average, 0.0
    volatility = stdev(values)
    t_stat = average / (volatility / math.sqrt(len(values))) if volatility > 0 else 0.0
    return average, t_stat


def load_previous_caps(path: str) -> dict[tuple[str, str], float]:
    by_symbol: dict[str, list[tuple[str, float]]] = defaultdict(list)
    with open(path, newline="") as handle:
        for row in csv.DictReader(handle):
            value = parse_float(row.get("total_mv"))
            if value is not None:
                by_symbol[row["symbol"]].append((row["date"], value))

    previous = {}
    for symbol, observations in by_symbol.items():
        observations.sort()
        for index in range(1, len(observations)):
            date, _ = observations[index]
            previous[(symbol, date)] = observations[index - 1][1]
    return previous


def load_forward_returns(
    path: str,
) -> dict[tuple[str, str], dict[str, object]]:
    one_day = {}
    with open(path, newline="") as handle:
        for row in csv.DictReader(handle):
            value = parse_float(row.get("o2o_ret"))
            open_price = parse_float(row.get("open"))
            if value is None or open_price is None or open_price <= 0:
                continue
            one_day[(row["symbol"], row["date"])] = {
                "return": value,
                "open": open_price,
                "next_date": row["next_date"],
            }

    forward = {}
    for (symbol, signal_date), signal_row in one_day.items():
        entry_date = str(signal_row["next_date"])
        entry_row = one_day.get((symbol, entry_date))
        if entry_row is None:
            continue
        forward[(symbol, signal_date)] = {
            "return": float(entry_row["return"]),
            "signal_open": float(signal_row["open"]),
            "entry_date": entry_date,
            "exit_date": str(entry_row["next_date"]),
        }
    return forward


def is_star(symbol: str) -> bool:
    return symbol.startswith(("SH688", "SH689"))


def domain(previous_cap: float, signal_open: float, symbol: str) -> tuple[str, str] | None:
    if previous_cap < 500_000:
        cap_group = CAP_GROUPS[0]
    elif previous_cap < 5_000_000:
        cap_group = CAP_GROUPS[1]
    else:
        cap_group = CAP_GROUPS[2]

    if is_star(symbol):
        return (cap_group, PRICE_GROUPS[2]) if signal_open >= 10 else None
    price_group = PRICE_GROUPS[0] if signal_open < 10 else PRICE_GROUPS[1]
    return cap_group, price_group


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--factor", required=True)
    parser.add_argument("--returns", required=True)
    parser.add_argument("--market-caps", required=True)
    parser.add_argument("--factor-col", required=True)
    parser.add_argument("--daily-out", required=True)
    parser.add_argument("--summary-out", required=True)
    args = parser.parse_args()

    forward_returns = load_forward_returns(args.returns)
    previous_caps = load_previous_caps(args.market_caps)
    grouped: dict[tuple[str, str, str], list[dict[str, object]]] = defaultdict(list)

    with open(args.factor, newline="") as handle:
        for row in csv.DictReader(handle):
            factor = parse_float(row.get(args.factor_col))
            symbol, date = row["symbol"], row["date"]
            return_row = forward_returns.get((symbol, date))
            previous_cap = previous_caps.get((symbol, date))
            if factor is None or return_row is None or previous_cap is None:
                continue
            group = domain(previous_cap, float(return_row["signal_open"]), symbol)
            if group is None:
                continue
            grouped[(group[0], group[1], date)].append(
                {
                    "symbol": symbol,
                    "factor": factor,
                    "return": float(return_row["return"]),
                    "entry_date": return_row["entry_date"],
                    "exit_date": return_row["exit_date"],
                }
            )

    daily_rows = []
    for cap_group in CAP_GROUPS:
        for price_group in PRICE_GROUPS:
            dates = sorted(
                date
                for cap, price, date in grouped
                if cap == cap_group and price == price_group
            )
            for date in dates:
                rows = sorted(
                    grouped[(cap_group, price_group, date)],
                    key=lambda row: (float(row["factor"]), str(row["symbol"])),
                )
                if len(rows) < 20:
                    continue
                factors = [float(row["factor"]) for row in rows]
                returns = [float(row["return"]) for row in rows]
                decile_size = max(1, len(rows) // 10)
                tail_size = max(1, math.ceil(len(rows) * 0.2))
                universe_return = mean(returns)
                daily_rows.append(
                    {
                        "cap_group": cap_group,
                        "price_group": price_group,
                        "date": date,
                        "entry_date": rows[0]["entry_date"],
                        "exit_date": rows[0]["exit_date"],
                        "n": len(rows),
                        "rank_ic": pearson(ranks(factors), ranks(returns)),
                        "pearson_ic": pearson(factors, returns),
                        "d10_d1": mean(returns[-decile_size:]) - mean(returns[:decile_size]),
                        "bottom20_excess": mean(returns[:tail_size]) - universe_return,
                        "top20_excess": mean(returns[-tail_size:]) - universe_return,
                    }
                )

    daily_fields = [
        "cap_group",
        "price_group",
        "date",
        "entry_date",
        "exit_date",
        "n",
        "rank_ic",
        "pearson_ic",
        "d10_d1",
        "bottom20_excess",
        "top20_excess",
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
                row
                for row in daily_rows
                if row["cap_group"] == cap_group and row["price_group"] == price_group
            ]
            metrics = {}
            for column in ("rank_ic", "pearson_ic", "d10_d1", "bottom20_excess", "top20_excess"):
                metrics[column], metrics[f"{column}_t"] = mean_t(
                    [float(row[column]) for row in rows if row[column] is not None]
                )
            summary_rows.append(
                {
                    "cap_group": cap_group,
                    "price_group": price_group,
                    "n_days": len(rows),
                    "n_obs": sum(int(row["n"]) for row in rows),
                    "avg_names": mean(int(row["n"]) for row in rows) if rows else 0.0,
                    **metrics,
                }
            )

    Path(args.summary_out).parent.mkdir(parents=True, exist_ok=True)
    with open(args.summary_out, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(summary_rows[0]))
        writer.writeheader()
        writer.writerows(summary_rows)

    print(
        f"daily_rows={len(daily_rows)} "
        f"observations={sum(row['n_obs'] for row in summary_rows)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
