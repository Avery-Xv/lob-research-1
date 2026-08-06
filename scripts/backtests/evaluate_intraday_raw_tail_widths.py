#!/usr/bin/env python3
"""Evaluate wider raw-factor tail portfolios for the joint V4 intraday sample."""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path
from statistics import mean
from typing import Sequence

from backtest_daily_domains import domain
from backtest_joint_v4_domains import (
    Observation,
    load_common,
    load_factor_groups,
    mean_t,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_WIDTHS = (0.10, 0.20, 0.30)


def tail_spread(
    scores: Sequence[float],
    returns: Sequence[float],
    symbols: Sequence[str],
    width: float,
) -> tuple[float, float, float, int]:
    """Return top, bottom, and top-minus-bottom means for a tail width."""
    if not 0.0 < width <= 0.5:
        raise ValueError(f"width must be in (0, 0.5], got {width}")
    if not (len(scores) == len(returns) == len(symbols)):
        raise ValueError("scores, returns, and symbols must have equal lengths")
    if not scores:
        raise ValueError("cannot form tails from an empty cross-section")
    order = sorted(range(len(scores)), key=lambda index: (scores[index], symbols[index]))
    bucket = max(1, int(len(order) * width))
    bottom = mean(returns[index] for index in order[:bucket])
    top = mean(returns[index] for index in order[-bucket:])
    return top, bottom, top - bottom, bucket


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


def evaluate(
    grouped: dict[tuple[str, str, int], list[Observation]],
    target_index: int,
    target_name: str,
    widths: Sequence[float],
    minimum_cross_section: int,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    by_date: list[dict[str, object]] = []
    for (frequency, factor, date), rows in sorted(grouped.items()):
        if frequency != "intraday":
            continue
        signal_rows = [row for row in rows if domain(row[4], row[5], row[0]) is not None]
        eligible = [row for row in signal_rows if row[2][target_index] is not None]
        if len(eligible) < minimum_cross_section:
            continue
        scores = [row[1] for row in eligible]
        returns = [float(row[2][target_index]) for row in eligible]
        symbols = [row[0] for row in eligible]
        for width in widths:
            top, bottom, spread, bucket = tail_spread(scores, returns, symbols, width)
            by_date.append({
                "factor": factor,
                "window_name": "intraday_1000_1030",
                "target": target_name,
                "scope": "raw_all_market",
                "date": date,
                "tail_width": width,
                "n": len(eligible),
                "tail_names": bucket,
                "top_return": top,
                "bottom_return": bottom,
                "top_minus_bottom": spread,
            })

    grouped_metrics: dict[tuple[str, float], list[dict[str, object]]] = defaultdict(list)
    for row in by_date:
        grouped_metrics[(str(row["factor"]), float(row["tail_width"]))].append(row)

    summary: list[dict[str, object]] = []
    for (factor, width), observations in sorted(grouped_metrics.items()):
        result: dict[str, object] = {
            "factor": factor,
            "window_name": "intraday_1000_1030",
            "target": target_name,
            "scope": "raw_all_market",
            "tail_width": width,
            "n_days": len(observations),
            "n_obs": sum(int(row["n"]) for row in observations),
            "avg_names": mean(int(row["n"]) for row in observations),
            "avg_tail_names": mean(int(row["tail_names"]) for row in observations),
        }
        for metric in ("top_return", "bottom_return", "top_minus_bottom"):
            values = [float(row[metric]) for row in observations]
            result[metric], result[f"{metric}_t"] = mean_t(values)
            result[f"{metric}_bp"] = 10_000.0 * float(result[metric])
        summary.append(result)
    return by_date, summary


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--factors", required=True)
    parser.add_argument("--returns", required=True)
    parser.add_argument("--intraday-returns", required=True)
    parser.add_argument("--market-caps", required=True)
    parser.add_argument("--styles", required=True)
    parser.add_argument("--date-from", type=int, default=20260201)
    parser.add_argument("--date-to", type=int, default=20260430)
    parser.add_argument("--target", choices=("ret_1031_1040", "ret_1031_1045"), default="ret_1031_1040")
    parser.add_argument("--widths", nargs="+", type=float, default=DEFAULT_WIDTHS)
    parser.add_argument("--minimum-cross-section", type=int, default=20)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()

    daily_common, intraday_common = load_common(
        args.returns,
        args.intraday_returns,
        args.market_caps,
        args.styles,
        args.date_from,
        args.date_to,
    )
    groups = load_factor_groups(
        args.factors, daily_common, intraday_common, args.date_from, args.date_to
    )
    target_index = 0 if args.target == "ret_1031_1040" else 1
    by_date, summary = evaluate(
        groups,
        target_index,
        args.target,
        args.widths,
        args.minimum_cross_section,
    )

    output_dir = Path(args.output_dir)
    write_csv(output_dir / "tail_widths_by_date.csv", by_date)
    write_csv(output_dir / "tail_widths_summary.csv", summary)
    metadata = {
        "date_from": args.date_from,
        "date_to": args.date_to,
        "signal": "raw factors formed from 10:00-10:30",
        "target": args.target,
        "scope": "raw all-market diagnostic over the same nine-domain-eligible stock universe",
        "tail_rule": "daily deterministic factor sort; compare equal-width top and bottom tails",
        "tail_widths": args.widths,
        "label_policy": "establish signal-time universe first; filter the requested target independently",
        "universe_rule": "point-in-time Shanghai/Shenzhen A shares; ETF excluded before factor calculation",
        "input_paths": {
            "factors": args.factors,
            "returns": args.returns,
            "intraday_returns": args.intraday_returns,
            "market_caps": args.market_caps,
            "styles": args.styles,
        },
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2) + "\n"
    )
    print(f"by_date_rows={len(by_date)} summary_rows={len(summary)} output_dir={output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
