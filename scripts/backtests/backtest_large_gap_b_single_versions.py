#!/usr/bin/env python3
"""Backtest pre-specified one-sided B large-gap reversal composites."""

from __future__ import annotations

import argparse
import csv
import json
import math
from collections import defaultdict
from pathlib import Path
from statistics import mean, median
from typing import NamedTuple, Sequence

import backtest_large_gap_by_raw_vr_state as base
import backtest_large_gap_by_raw_vr_state_all_market_neutral as neutral
from backtest_daily_domains import domain, pearson, ranks
from backtest_existing_daily_o2o_cne5 import build_orthonormal_basis, finite, residualize


TARGETS = ("ret_1031_1040", "ret_1031_1045", "ret_1031_1100")
VARIANTS = (
    "b_reversal",
    "b_reversal_vr",
    "b_reversal_vr_reliability",
)


class Observation(NamedTuple):
    symbol: str
    buy_gap: float
    vr_log: float
    matched_trade_count: int
    targets: tuple[float | None, ...]
    styles: list[float]
    previous_market_cap: float
    signal_price: float


def build_scores(
    buy_residuals: Sequence[float],
    vr_values: Sequence[float],
    matched_trade_counts: Sequence[int],
    symbols: Sequence[str],
) -> dict[str, list[float]]:
    lengths = {len(buy_residuals), len(vr_values), len(matched_trade_counts), len(symbols)}
    if len(lengths) != 1 or not buy_residuals:
        raise ValueError("score inputs must have one equal, positive length")
    buy_percentiles = base.percentile_ranks(buy_residuals)
    vr_percentiles = base.percentile_ranks(vr_values)
    typical_count = median(matched_trade_counts)
    reliability = [
        math.sqrt(count / (count + typical_count)) if count + typical_count > 0 else 0.0
        for count in matched_trade_counts
    ]
    centered_reversal = [-(value - 0.5) for value in buy_percentiles]
    return {
        "b_reversal": centered_reversal,
        "b_reversal_vr": [
            reversal * vr for reversal, vr in zip(centered_reversal, vr_percentiles)
        ],
        "b_reversal_vr_reliability": [
            reversal * vr * weight
            for reversal, vr, weight in zip(centered_reversal, vr_percentiles, reliability)
        ],
    }


def load_groups(
    factor_path: str,
    common: dict[tuple[str, int], neutral.CommonValue],
    date_from: int,
    date_to: int,
) -> dict[int, list[Observation]]:
    grouped: dict[int, list[Observation]] = defaultdict(list)
    seen: set[tuple[str, int]] = set()
    with Path(factor_path).open(newline="") as handle:
        for row in csv.DictReader(handle):
            symbol, date = row["symbol"], int(row["date"])
            if not date_from <= date <= date_to:
                continue
            key = (symbol, date)
            if key in seen:
                raise ValueError(f"duplicate factor row: {key}")
            seen.add(key)
            common_value = common.get(key)
            buy_gap = finite(row.get("intraday_large_gap_buy_ratio"))
            vr_log = finite(row.get("vr_log"))
            trade_count = finite(row.get("intraday_matched_trade_count"))
            if (
                common_value is None
                or buy_gap is None
                or vr_log is None
                or trade_count is None
                or trade_count <= 0
                or not base.parse_bool(row.get("intraday_passes_match_rate"))
                or not base.parse_bool(row.get("ob_is_valid"))
            ):
                continue
            signal_price, targets, styles, previous_cap = common_value
            grouped[date].append(Observation(
                symbol=symbol,
                buy_gap=float(buy_gap),
                vr_log=float(vr_log),
                matched_trade_count=int(trade_count),
                targets=targets,
                styles=styles,
                previous_market_cap=float(previous_cap),
                signal_price=float(signal_price),
            ))
    return grouped


def decile_means(
    scores: Sequence[float], returns: Sequence[float], symbols: Sequence[str]
) -> list[float]:
    order = sorted(range(len(scores)), key=lambda index: (scores[index], symbols[index]))
    buckets: list[list[float]] = [[] for _ in range(10)]
    for position, index in enumerate(order):
        bucket = min(9, position * 10 // len(order))
        buckets[bucket].append(returns[index])
    return [mean(bucket) for bucket in buckets]


def append_metrics(
    output: list[dict[str, object]],
    date: int,
    scope: str,
    cap_group: str,
    price_group: str,
    variant: str,
    rows: Sequence[Observation],
    scores: Sequence[float],
    minimum_cross_section: int,
) -> None:
    for target_index, target in enumerate(TARGETS):
        eligible = [index for index, row in enumerate(rows) if row.targets[target_index] is not None]
        if len(eligible) < minimum_cross_section:
            continue
        eligible_scores = [scores[index] for index in eligible]
        returns = [float(rows[index].targets[target_index]) for index in eligible]
        symbols = [rows[index].symbol for index in eligible]
        buckets = decile_means(eligible_scores, returns, symbols)
        result: dict[str, object] = {
            "variant": variant,
            "target": target,
            "scope": scope,
            "cap_group": cap_group,
            "price_group": price_group,
            "date": date,
            "n": len(eligible),
            "rank_ic": pearson(ranks(eligible_scores), ranks(returns)),
            "d10_d1": buckets[-1] - buckets[0],
        }
        for bucket, value in enumerate(buckets, 1):
            result[f"q{bucket}_return"] = value
        output.append(result)


def process_groups(
    grouped: dict[int, list[Observation]], minimum_cross_section: int
) -> list[dict[str, object]]:
    performance: list[dict[str, object]] = []
    for date, rows in sorted(grouped.items()):
        ordered = sorted(rows, key=lambda row: row.symbol)
        all_market_residuals = residualize(
            [row.buy_gap for row in ordered],
            build_orthonormal_basis([row.styles for row in ordered]),
        )
        residual_by_symbol = {
            row.symbol: value for row, value in zip(ordered, all_market_residuals)
        }
        domains: dict[tuple[str, str], list[Observation]] = defaultdict(list)
        for row in rows:
            group = domain(row.previous_market_cap, row.signal_price, row.symbol)
            if group is not None:
                domains[group].append(row)
        pooled: dict[str, list[tuple[Observation, float]]] = defaultdict(list)
        for (cap_group, price_group), domain_rows in sorted(domains.items()):
            if len(domain_rows) < minimum_cross_section:
                continue
            domain_rows.sort(key=lambda row: row.symbol)
            scores_by_variant = build_scores(
                [residual_by_symbol[row.symbol] for row in domain_rows],
                [row.vr_log for row in domain_rows],
                [row.matched_trade_count for row in domain_rows],
                [row.symbol for row in domain_rows],
            )
            for variant in VARIANTS:
                scores = scores_by_variant[variant]
                append_metrics(
                    performance, date, "domain", cap_group, price_group,
                    variant, domain_rows, scores, minimum_cross_section,
                )
                pooled[variant].extend(zip(domain_rows, scores))
        for variant in VARIANTS:
            pairs = sorted(pooled[variant], key=lambda pair: pair[0].symbol)
            append_metrics(
                performance, date, "domain_neutral_aggregate", "domain_neutral",
                "aggregate", variant, [pair[0] for pair in pairs],
                [pair[1] for pair in pairs], minimum_cross_section,
            )
    return performance


def summarize(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    keys = ("variant", "target", "scope", "cap_group", "price_group")
    grouped: dict[tuple[str, ...], list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        grouped[tuple(str(row[key]) for key in keys)].append(row)
    output: list[dict[str, object]] = []
    for key, observations in sorted(grouped.items()):
        result: dict[str, object] = dict(zip(keys, key))
        result.update(
            n_days=len(observations),
            n_obs=sum(int(row["n"]) for row in observations),
            avg_names=mean(int(row["n"]) for row in observations),
        )
        for metric in ("rank_ic", "d10_d1"):
            values = [float(row[metric]) for row in observations]
            result[metric], result[f"{metric}_t"] = base.mean_t(values)
        decile_values: list[float] = []
        for bucket in range(1, 11):
            values = [float(row[f"q{bucket}_return"]) for row in observations]
            bucket_mean, bucket_t = base.mean_t(values)
            result[f"q{bucket}_return"] = bucket_mean
            result[f"q{bucket}_return_t"] = bucket_t
            decile_values.append(float(bucket_mean))
        result["adjacent_increases"] = sum(
            decile_values[index + 1] > decile_values[index] for index in range(9)
        )
        result["decile_rank_correlation"] = pearson(
            ranks(list(range(1, 11))), ranks(decile_values)
        )
        output.append(result)
    return output


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--factors", required=True)
    parser.add_argument("--returns", required=True)
    parser.add_argument("--market-caps", required=True)
    parser.add_argument("--styles", required=True)
    parser.add_argument("--date-from", type=int, default=20260201)
    parser.add_argument("--date-to", type=int, default=20260430)
    parser.add_argument("--minimum-cross-section", type=int, default=20)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()

    common = neutral.load_common(
        args.returns, args.market_caps, args.styles, TARGETS,
        args.date_from, args.date_to, neutral.LOB4_NO_SIZE_COLS,
    )
    groups = load_groups(args.factors, common, args.date_from, args.date_to)
    performance = process_groups(groups, args.minimum_cross_section)
    summary = summarize(performance)
    output_dir = Path(args.output_dir)
    base.write_csv(output_dir / "performance_by_date.csv", performance)
    base.write_csv(output_dir / "performance_summary.csv", summary)
    output_dir.mkdir(parents=True, exist_ok=True)
    metadata = {
        "date_from": args.date_from,
        "date_to": args.date_to,
        "targets": list(TARGETS),
        "variants": {
            "b_reversal": "-(domain percentile of all-market LOB4 residual B - 0.5)",
            "b_reversal_vr": "b_reversal * domain percentile of raw vr_log",
            "b_reversal_vr_reliability": (
                "b_reversal_vr * sqrt(N/(N+k)); N=matched trade count; "
                "k=date-domain median matched trade count"
            ),
        },
        "neutralization_order": (
            "per date full-market raw B residualized on momentum, liquidity, beta, "
            "residual_volatility; then structural domains"
        ),
        "excluded_size_styles": ["size", "non_linear_size"],
        "vr_treatment": "raw vr_log is not neutralized; percentile-ranked within date-domain",
        "exposure_timing": "previous trading-day CNE5 exposures and market cap",
        "universe_rule": "point-in-time Shanghai/Shenzhen A shares; ETF count zero",
        "validity": "large-gap match_rate>=0.95 and ob_is_valid=true; ST/suspended excluded",
        "missing_label_policy": "scores formed before independent target filtering",
        "common_rows": len(common),
        "signal_rows": sum(len(rows) for rows in groups.values()),
    }
    (output_dir / "metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2) + "\n"
    )
    print(
        f"common={len(common)} dates={len(groups)} performance_rows={len(performance)} "
        f"output_dir={output_dir}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
