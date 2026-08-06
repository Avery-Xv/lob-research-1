#!/usr/bin/env python3
"""Evaluate an intraday active overlay on an existing long-only bottom inventory."""

from __future__ import annotations

import argparse
import csv
import json
import math
from collections import defaultdict
from pathlib import Path
from statistics import mean
from typing import Sequence

import backtest_large_gap_by_raw_vr_state as base


def overlay_day(
    long_return: float,
    base_return: float,
    overlay_share: float,
    buy_cost_bp: float,
    sell_cost_bp: float,
) -> dict[str, float]:
    """Return P&L contributions for an overlay opened and reversed intraday."""
    long_leg = overlay_share * long_return
    underweight_leg = -overlay_share * base_return
    gross_active = long_leg + underweight_leg
    cost = 2.0 * overlay_share * (buy_cost_bp + sell_cost_bp) / 10_000.0
    net_active = gross_active - cost
    return {
        "long_leg_contribution": long_leg,
        "underweight_leg_contribution": underweight_leg,
        "gross_active_return": gross_active,
        "trading_cost": cost,
        "net_active_return": net_active,
        "gross_total_return": base_return + gross_active,
        "net_total_return": base_return + net_active,
    }


def compound(values: Sequence[float]) -> float:
    wealth = 1.0
    for value in values:
        wealth *= 1.0 + value
    return wealth - 1.0


def relative_compound(total: Sequence[float], base_values: Sequence[float]) -> float:
    total_wealth = 1.0
    base_wealth = 1.0
    for total_return, base_return in zip(total, base_values):
        total_wealth *= 1.0 + total_return
        base_wealth *= 1.0 + base_return
    return total_wealth / base_wealth - 1.0


def load_rows(
    path: str,
    target: str,
    s_cut: str,
    b_filter: str,
    vr_scope: str,
) -> list[dict[str, object]]:
    output: list[dict[str, object]] = []
    seen: set[int] = set()
    with Path(path).open(newline="") as handle:
        for row in csv.DictReader(handle):
            if (
                row["scope"] != "domain_aggregate"
                or row["target"] != target
                or row["s_cut"] != s_cut
                or row["b_filter"] != b_filter
                or row["vr_scope"] != vr_scope
            ):
                continue
            date = int(row["date"])
            if date in seen:
                raise ValueError(f"duplicate selected daily portfolio: {date}")
            seen.add(date)
            output.append({
                "date": date,
                "long_return": float(row["long_return"]),
                "base_return": float(row["benchmark_return"]),
            })
    if not output:
        raise ValueError("no matching daily portfolio rows")
    return sorted(output, key=lambda row: int(row["date"]))


def evaluate(
    rows: Sequence[dict[str, object]],
    overlay_shares: Sequence[float],
    buy_cost_bp: float,
    sell_cost_bp: float,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    by_date: list[dict[str, object]] = []
    for overlay_share in overlay_shares:
        for row in rows:
            metrics = overlay_day(
                float(row["long_return"]),
                float(row["base_return"]),
                overlay_share,
                buy_cost_bp,
                sell_cost_bp,
            )
            by_date.append({
                "overlay_share": overlay_share,
                "date": int(row["date"]),
                "long_return": float(row["long_return"]),
                "base_return": float(row["base_return"]),
                **metrics,
            })
    grouped: dict[float, list[dict[str, object]]] = defaultdict(list)
    for row in by_date:
        grouped[float(row["overlay_share"])].append(row)
    summary: list[dict[str, object]] = []
    for overlay_share, values in sorted(grouped.items()):
        values.sort(key=lambda row: int(row["date"]))
        result: dict[str, object] = {
            "overlay_share": overlay_share,
            "n_days": len(values),
            "buy_cost_bp": buy_cost_bp,
            "sell_cost_bp": sell_cost_bp,
            "roundtrip_overlay_cost_bp": 2.0 * (buy_cost_bp + sell_cost_bp),
            "daily_total_nav_cost_bp": (
                2.0 * overlay_share * (buy_cost_bp + sell_cost_bp)
            ),
        }
        for metric in (
            "long_leg_contribution", "underweight_leg_contribution",
            "gross_active_return", "trading_cost", "net_active_return",
            "gross_total_return", "net_total_return",
        ):
            metric_values = [float(row[metric]) for row in values]
            result[f"mean_{metric}"], result[f"{metric}_t"] = base.mean_t(metric_values)
        base_values = [float(row["base_return"]) for row in values]
        gross_total = [float(row["gross_total_return"]) for row in values]
        net_total = [float(row["net_total_return"]) for row in values]
        result["cumulative_base_return"] = compound(base_values)
        result["cumulative_gross_total_return"] = compound(gross_total)
        result["cumulative_net_total_return"] = compound(net_total)
        result["cumulative_gross_active_excess"] = relative_compound(
            gross_total, base_values
        )
        result["cumulative_net_active_excess"] = relative_compound(
            net_total, base_values
        )
        result["gross_active_hit_rate"] = mean(
            float(row["gross_active_return"]) > 0 for row in values
        )
        result["net_active_hit_rate"] = mean(
            float(row["net_active_return"]) > 0 for row in values
        )
        summary.append(result)
    return by_date, summary


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--daily-portfolios", required=True)
    parser.add_argument("--target", default="ret_1031_1100")
    parser.add_argument("--s-cut", default="top10")
    parser.add_argument("--b-filter", default="not_bottom20")
    parser.add_argument("--vr-scope", default="high")
    parser.add_argument("--overlay-shares", nargs="+", type=float, default=[0.1, 0.2, 0.3])
    parser.add_argument("--buy-cost-bp", type=float, default=3.0)
    parser.add_argument("--sell-cost-bp", type=float, default=8.0)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()
    if any(not 0 < value <= 1 for value in args.overlay_shares):
        raise ValueError("overlay shares must lie in (0, 1]")
    if args.buy_cost_bp < 0 or args.sell_cost_bp < 0:
        raise ValueError("costs must be non-negative")

    rows = load_rows(
        args.daily_portfolios, args.target, args.s_cut, args.b_filter, args.vr_scope
    )
    by_date, summary = evaluate(
        rows, args.overlay_shares, args.buy_cost_bp, args.sell_cost_bp
    )
    output_dir = Path(args.output_dir)
    base.write_csv(output_dir / "overlay_by_date.csv", by_date)
    base.write_csv(output_dir / "overlay_summary.csv", summary)
    output_dir.mkdir(parents=True, exist_ok=True)
    metadata = {
        "input": args.daily_portfolios,
        "target": args.target,
        "s_cut": args.s_cut,
        "b_filter": args.b_filter,
        "vr_scope": args.vr_scope,
        "overlay_shares": args.overlay_shares,
        "buy_cost_bp": args.buy_cost_bp,
        "sell_cost_bp": args.sell_cost_bp,
        "base_inventory": (
            "synthetic equal-weight benchmark in the same date, structural domains, "
            "and VR scope as the long candidates"
        ),
        "overlay": (
            "at 10:30 reduce base inventory by overlay_share and allocate the same notional "
            "to the signal long basket; reverse at 11:00"
        ),
        "underweight_leg": (
            "-overlay_share * base_return; positive when reduced base inventory loses value"
        ),
        "cost_formula": (
            "2 * overlay_share * (buy_cost_bp + sell_cost_bp), covering entry and reversal"
        ),
        "neutralization": "none",
        "execution_caveat": (
            "synthetic bottom inventory; exact realized P&L requires the user's actual holdings, "
            "weights, trade constraints, and fills"
        ),
    }
    (output_dir / "metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2) + "\n"
    )
    print(f"days={len(rows)} summaries={len(summary)} output_dir={output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
