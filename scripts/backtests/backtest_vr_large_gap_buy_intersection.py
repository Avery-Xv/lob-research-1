#!/usr/bin/env python3
"""Backtest a long-only intersection of intraday VR and buy-side large-gap ratio."""

from __future__ import annotations

import argparse
import csv
import math
from collections import defaultdict
from pathlib import Path
from statistics import mean

from backtest_intraday_domains import (
    load_previous_values,
    load_previous_vectors,
    load_returns,
    mean_t,
    neutralize,
    parse_float,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CNE5_COLS = [
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
]
STRATEGIES = ("buy_gap_top30", "vr_top30", "both_top30")


def load_factor(
    path: str,
    factor_col: str,
    *,
    valid_col: str | None = None,
) -> dict[tuple[str, str], tuple[float, float]]:
    result: dict[tuple[str, str], tuple[float, float]] = {}
    with open(path, newline="") as handle:
        for row in csv.DictReader(handle):
            if valid_col is not None and row.get(valid_col, "").lower() not in {"1", "true"}:
                continue
            factor = parse_float(row.get(factor_col))
            start_mid = parse_float(row.get("start_mid"))
            if factor is None or start_mid is None or start_mid <= 0:
                continue
            key = (row["symbol"], row["date"].replace("-", ""))
            value = (factor, start_mid)
            previous = result.get(key)
            if previous is not None and previous != value:
                raise ValueError(f"conflicting duplicate factor row: {key}")
            result[key] = value
    return result


def top_indices(values: list[float], symbols: list[str], fraction: float) -> set[int]:
    count = max(1, math.ceil(len(values) * fraction))
    order = sorted(
        range(len(values)),
        key=lambda index: (values[index], symbols[index]),
        reverse=True,
    )
    return set(order[:count])


def write_csv(path: str, rows: list[dict[str, object]]) -> None:
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--buy-gap",
        default=str(
            PROJECT_ROOT
            / "data/cache/passive_large_gap_B_1000_1030_match95_backtest_input_202601.csv"
        ),
    )
    parser.add_argument(
        "--vr",
        default=str(
            PROJECT_ROOT / "data/cache/order_behavior_vr_log_1000_1030_domain_input_202601.csv"
        ),
    )
    parser.add_argument(
        "--returns",
        default=str(
            PROJECT_ROOT
            / "data/cache/min1_ret_1031_1040_1045_202601_clean_with_status.csv"
        ),
    )
    parser.add_argument(
        "--market-caps",
        default=str(PROJECT_ROOT / "data/cache/daily_market_cap_202512_202601.csv"),
    )
    parser.add_argument(
        "--risk-exposures",
        default=str(PROJECT_ROOT / "data/cache/cne5_style_full_202512_202601.csv"),
    )
    parser.add_argument("--return-cols", nargs="+", default=["ret_1031_1040", "ret_1031_1045"])
    parser.add_argument("--top-fraction", type=float, default=0.30)
    parser.add_argument(
        "--daily-out",
        default=str(
            PROJECT_ROOT
            / "results/intraday/vr_large_gap_buy_top30_largecap_highprice_daily.csv"
        ),
    )
    parser.add_argument(
        "--summary-out",
        default=str(
            PROJECT_ROOT
            / "results/intraday/vr_large_gap_buy_top30_largecap_highprice_summary.csv"
        ),
    )
    args = parser.parse_args()
    if not 0 < args.top_fraction <= 1:
        raise ValueError("--top-fraction must be in (0, 1]")

    buy_gap = load_factor(args.buy_gap, "large_gap_buy_ratio")
    vr = load_factor(args.vr, "vr_log")
    previous_caps = load_previous_values(args.market_caps, "total_mv")
    previous_risks = load_previous_vectors(args.risk_exposures, CNE5_COLS)
    return_maps = {
        return_col: load_returns(args.returns, return_col)
        for return_col in args.return_cols
    }

    by_date: dict[str, list[dict[str, object]]] = defaultdict(list)
    common_keys = buy_gap.keys() & vr.keys()
    for symbol, date in sorted(common_keys):
        if symbol.startswith(("SH688", "SH689")):
            continue
        cap = previous_caps.get((symbol, date))
        risk = previous_risks.get((symbol, date))
        if cap is None or cap < 5_000_000 or risk is None:
            continue
        buy_value, buy_mid = buy_gap[(symbol, date)]
        vr_value, vr_mid = vr[(symbol, date)]
        start_mid = vr_mid
        if start_mid < 10:
            continue
        if not math.isclose(buy_mid, vr_mid, rel_tol=0.0, abs_tol=0.02):
            continue
        if any((symbol, date) not in values for values in return_maps.values()):
            continue
        by_date[date].append(
            {
                "symbol": symbol,
                "buy_gap": buy_value,
                "vr": vr_value,
                "risk": risk,
            }
        )

    daily_rows: list[dict[str, object]] = []
    for date, rows in sorted(by_date.items()):
        if len(rows) < 20:
            continue
        symbols = [str(row["symbol"]) for row in rows]
        exposures = [list(row["risk"]) for row in rows]
        buy_residual = neutralize([float(row["buy_gap"]) for row in rows], exposures)
        vr_residual = neutralize([float(row["vr"]) for row in rows], exposures)
        buy_top = top_indices(buy_residual, symbols, args.top_fraction)
        vr_top = top_indices(vr_residual, symbols, args.top_fraction)
        selections = {
            "buy_gap_top30": buy_top,
            "vr_top30": vr_top,
            "both_top30": buy_top & vr_top,
        }

        for return_col, returns in return_maps.items():
            universe_values = [returns[(symbol, date)] for symbol in symbols]
            universe_return = mean(universe_values)
            for strategy in STRATEGIES:
                selected = sorted(selections[strategy])
                if not selected:
                    continue
                selected_return = mean(universe_values[index] for index in selected)
                daily_rows.append(
                    {
                        "date": date,
                        "return_horizon": return_col,
                        "strategy": strategy,
                        "universe_n": len(rows),
                        "selected_n": len(selected),
                        "selected_share": len(selected) / len(rows),
                        "selected_return": selected_return,
                        "universe_return": universe_return,
                        "excess_return": selected_return - universe_return,
                    }
                )

    summary_rows: list[dict[str, object]] = []
    for return_col in args.return_cols:
        for strategy in STRATEGIES:
            rows = [
                row
                for row in daily_rows
                if row["return_horizon"] == return_col and row["strategy"] == strategy
            ]
            selected_return, selected_return_t = mean_t(
                [float(row["selected_return"]) for row in rows]
            )
            excess_return, excess_return_t = mean_t(
                [float(row["excess_return"]) for row in rows]
            )
            summary_rows.append(
                {
                    "return_horizon": return_col,
                    "strategy": strategy,
                    "n_days": len(rows),
                    "n_obs": sum(int(row["selected_n"]) for row in rows),
                    "avg_universe_n": mean(int(row["universe_n"]) for row in rows),
                    "avg_selected_n": mean(int(row["selected_n"]) for row in rows),
                    "avg_selected_share": mean(float(row["selected_share"]) for row in rows),
                    "selected_return": selected_return,
                    "selected_return_t": selected_return_t,
                    "selected_return_bps": selected_return * 10_000,
                    "excess_return": excess_return,
                    "excess_return_t": excess_return_t,
                    "excess_return_bps": excess_return * 10_000,
                    "excess_positive_share": mean(
                        float(row["excess_return"]) > 0 for row in rows
                    ),
                }
            )

    if not daily_rows or not summary_rows:
        raise RuntimeError("no backtest rows produced")
    write_csv(args.daily_out, daily_rows)
    write_csv(args.summary_out, summary_rows)
    print(
        f"common_factor_rows={len(common_keys)} dates={len(by_date)} "
        f"daily_rows={len(daily_rows)}"
    )
    print(f"wrote {args.daily_out} and {args.summary_out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
