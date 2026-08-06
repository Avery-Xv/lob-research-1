#!/usr/bin/env python3
"""Intraday VR and large-gap B/S intersections using the daily strategy rules."""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path
from statistics import mean
from typing import Sequence

from analyze_existing_factors_lob5_ex_size import LOB5_EX_SIZE_COLS
from backtest_daily_domains import domain, pearson, ranks
from backtest_daily_vr_large_gap_intersection import percentile_ranks
from backtest_existing_daily_o2o_cne5 import build_orthonormal_basis, residualize
from backtest_large_gap_by_raw_vr_state import (
    Observation,
    load_common,
    load_groups,
    mean_t,
)
from backtest_order_behavior_daily_o2o_domains import winsorize


PROJECT_ROOT = Path(__file__).resolve().parents[2]
TARGETS = ("ret_1031_1040", "ret_1031_1045")
STRATEGIES = (
    "gap_baseline_30",
    "strict_both_30",
    "vr_median_filter",
    "short_vr_confirmed",
)


def selection_indices(
    factor: str,
    gap_percentiles: Sequence[float],
    vr_percentiles: Sequence[float],
    strategy: str,
) -> tuple[list[int], list[int]]:
    """Select daily-style long/short legs using factor-specific directions."""
    if len(gap_percentiles) != len(vr_percentiles):
        raise ValueError("gap and VR percentile vectors must have equal length")
    if factor not in {"large_gap_B", "large_gap_S"}:
        raise ValueError(f"unknown factor: {factor}")

    if factor == "large_gap_B":
        gap_long = lambda value: value <= 0.30
        gap_short = lambda value: value >= 0.70
    else:
        gap_long = lambda value: value >= 0.70
        gap_short = lambda value: value <= 0.30

    if strategy == "gap_baseline_30":
        long = [index for index, value in enumerate(gap_percentiles) if gap_long(value)]
        short = [index for index, value in enumerate(gap_percentiles) if gap_short(value)]
    elif strategy == "strict_both_30":
        long = [
            index
            for index, (gap, vr) in enumerate(zip(gap_percentiles, vr_percentiles))
            if gap_long(gap) and vr <= 0.30
        ]
        short = [
            index
            for index, (gap, vr) in enumerate(zip(gap_percentiles, vr_percentiles))
            if gap_short(gap) and vr >= 0.70
        ]
    elif strategy == "vr_median_filter":
        long = [
            index
            for index, (gap, vr) in enumerate(zip(gap_percentiles, vr_percentiles))
            if gap_long(gap) and vr <= 0.50
        ]
        short = [
            index
            for index, (gap, vr) in enumerate(zip(gap_percentiles, vr_percentiles))
            if gap_short(gap) and vr >= 0.50
        ]
    elif strategy == "short_vr_confirmed":
        long = [index for index, value in enumerate(gap_percentiles) if gap_long(value)]
        short = [
            index
            for index, (gap, vr) in enumerate(zip(gap_percentiles, vr_percentiles))
            if gap_short(gap) and vr >= 0.50
        ]
    else:
        raise ValueError(f"unknown strategy: {strategy}")
    return long, short


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


def performance_row(
    *,
    rows: Sequence[Observation],
    long_indices: Sequence[int],
    short_indices: Sequence[int],
    target_index: int,
    factor: str,
    signal_variant: str,
    strategy: str,
    scope: str,
    cap_group: str,
    price_group: str,
    date: int,
    minimum_leg_names: int,
) -> dict[str, object] | None:
    eligible = [index for index, row in enumerate(rows) if row[3][target_index] is not None]
    long = [index for index in long_indices if rows[index][3][target_index] is not None]
    short = [index for index in short_indices if rows[index][3][target_index] is not None]
    if len(eligible) < 20 or len(long) < minimum_leg_names or len(short) < minimum_leg_names:
        return None
    universe_return = mean(float(rows[index][3][target_index]) for index in eligible)
    long_return = mean(float(rows[index][3][target_index]) for index in long)
    short_return = mean(float(rows[index][3][target_index]) for index in short)
    return {
        "factor": factor,
        "signal_variant": signal_variant,
        "strategy": strategy,
        "target": TARGETS[target_index],
        "scope": scope,
        "cap_group": cap_group,
        "price_group": price_group,
        "date": date,
        "signal_n": len(rows),
        "label_n": len(eligible),
        "long_n": len(long),
        "short_n": len(short),
        "long_share": len(long_indices) / len(rows),
        "short_share": len(short_indices) / len(rows),
        "long_return": long_return,
        "short_return": short_return,
        "long_short": long_return - short_return,
        "universe_return": universe_return,
        "long_excess": long_return - universe_return,
        "short_alpha": universe_return - short_return,
    }


def process(
    grouped: dict[tuple[str, int], list[Observation]],
    winsor_lower: float,
    winsor_upper: float,
    minimum_leg_names: int,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    output: list[dict[str, object]] = []
    correlations: list[dict[str, object]] = []
    aggregate: dict[
        tuple[str, int, str, str], dict[str, list[Observation]]
    ] = defaultdict(lambda: {"universe": [], "long": [], "short": []})

    for (factor, date), factor_rows in sorted(grouped.items()):
        domains: dict[tuple[str, str], list[Observation]] = defaultdict(list)
        for row in factor_rows:
            group = domain(row[5], row[6], row[0])
            if group is not None:
                domains[group].append(row)
        for (cap_group, price_group), rows in sorted(domains.items()):
            if len(rows) < 20:
                continue
            rows.sort(key=lambda row: row[0])
            styles = [row[4] for row in rows]
            basis = build_orthonormal_basis(styles)
            raw_vr = winsorize([row[1] for row in rows], winsor_lower, winsor_upper)
            raw_gap = winsorize([row[2] for row in rows], winsor_lower, winsor_upper)
            neutral_vr = residualize(raw_vr, basis)
            neutral_gap = residualize(raw_gap, basis)
            correlations.append({
                "factor": factor,
                "date": date,
                "cap_group": cap_group,
                "price_group": price_group,
                "n": len(rows),
                "raw_rank_correlation": pearson(ranks(raw_gap), ranks(raw_vr)),
                "lob5_rank_correlation": pearson(ranks(neutral_gap), ranks(neutral_vr)),
            })
            for variant, gap_values, vr_values in (
                ("raw_domain", raw_gap, raw_vr),
                ("lob5_ex_size_domain", neutral_gap, neutral_vr),
            ):
                gap_percentiles = percentile_ranks(gap_values)
                vr_percentiles = percentile_ranks(vr_values)
                for strategy in STRATEGIES:
                    long_indices, short_indices = selection_indices(
                        factor, gap_percentiles, vr_percentiles, strategy
                    )
                    for target_index in range(len(TARGETS)):
                        result = performance_row(
                            rows=rows,
                            long_indices=long_indices,
                            short_indices=short_indices,
                            target_index=target_index,
                            factor=factor,
                            signal_variant=variant,
                            strategy=strategy,
                            scope="domain",
                            cap_group=cap_group,
                            price_group=price_group,
                            date=date,
                            minimum_leg_names=minimum_leg_names,
                        )
                        if result is not None:
                            output.append(result)
                    selections = aggregate[(factor, date, variant, strategy)]
                    selections["universe"].extend(rows)
                    selections["long"].extend(rows[index] for index in long_indices)
                    selections["short"].extend(rows[index] for index in short_indices)

    for (factor, date, variant, strategy), selections in sorted(aggregate.items()):
        rows = selections["universe"]
        row_index = {id(row): index for index, row in enumerate(rows)}
        long_indices = [row_index[id(row)] for row in selections["long"]]
        short_indices = [row_index[id(row)] for row in selections["short"]]
        for target_index in range(len(TARGETS)):
            result = performance_row(
                rows=rows,
                long_indices=long_indices,
                short_indices=short_indices,
                target_index=target_index,
                factor=factor,
                signal_variant=variant.replace("_domain", "_domain_aggregate"),
                strategy=strategy,
                scope="domain_aggregate",
                cap_group="all",
                price_group="domain_ranked",
                date=date,
                minimum_leg_names=minimum_leg_names,
            )
            if result is not None:
                output.append(result)
    return output, correlations


def summarize(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    keys = (
        "factor", "signal_variant", "strategy", "target", "scope",
        "cap_group", "price_group",
    )
    grouped: dict[tuple[str, ...], list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        grouped[tuple(str(row[key]) for key in keys)].append(row)
    output: list[dict[str, object]] = []
    for key, observations in sorted(grouped.items()):
        result: dict[str, object] = dict(zip(keys, key))
        result.update(
            n_days=len(observations),
            n_obs=sum(int(row["label_n"]) for row in observations),
            avg_universe_names=mean(int(row["label_n"]) for row in observations),
            avg_long_names=mean(int(row["long_n"]) for row in observations),
            avg_short_names=mean(int(row["short_n"]) for row in observations),
            avg_long_share=mean(float(row["long_share"]) for row in observations),
            avg_short_share=mean(float(row["short_share"]) for row in observations),
        )
        for metric in (
            "long_return", "short_return", "long_short", "long_excess", "short_alpha"
        ):
            values = [float(row[metric]) for row in observations]
            result[metric], result[f"{metric}_t"] = mean_t(values)
            result[f"{metric}_bp"] = 10_000.0 * float(result[metric])
        result["long_short_positive_share"] = mean(
            float(row["long_short"]) > 0 for row in observations
        )
        output.append(result)
    return output


def summarize_correlations(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    grouped: dict[tuple[str, str, str], list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        grouped[(str(row["factor"]), str(row["cap_group"]), str(row["price_group"]))].append(row)
    output: list[dict[str, object]] = []
    for (factor, cap_group, price_group), observations in sorted(grouped.items()):
        result: dict[str, object] = {
            "factor": factor,
            "cap_group": cap_group,
            "price_group": price_group,
            "n_days": len(observations),
            "n_obs": sum(int(row["n"]) for row in observations),
            "avg_names": mean(int(row["n"]) for row in observations),
        }
        for metric in ("raw_rank_correlation", "lob5_rank_correlation"):
            values = [float(row[metric]) for row in observations if row[metric] is not None]
            result[metric], result[f"{metric}_t"] = mean_t(values)
        output.append(result)
    return output


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--factors", required=True)
    parser.add_argument("--intraday-returns", required=True)
    parser.add_argument("--market-caps", required=True)
    parser.add_argument("--styles", required=True)
    parser.add_argument("--date-from", type=int, default=20260201)
    parser.add_argument("--date-to", type=int, default=20260430)
    parser.add_argument("--winsor-lower", type=float, default=0.01)
    parser.add_argument("--winsor-upper", type=float, default=0.99)
    parser.add_argument("--minimum-leg-names", type=int, default=5)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()

    common = load_common(
        args.intraday_returns, args.market_caps, args.styles, args.date_from, args.date_to
    )
    groups = load_groups(args.factors, common, args.date_from, args.date_to)
    performance, correlations = process(
        groups, args.winsor_lower, args.winsor_upper, args.minimum_leg_names
    )
    summary = summarize(performance)
    correlation_summary = summarize_correlations(correlations)
    output_dir = Path(args.output_dir)
    write_csv(output_dir / "performance_by_date.csv", performance)
    write_csv(output_dir / "performance_summary.csv", summary)
    write_csv(output_dir / "correlation_by_date.csv", correlations)
    write_csv(output_dir / "correlation_summary.csv", correlation_summary)
    metadata = {
        "date_from": args.date_from,
        "date_to": args.date_to,
        "signal": "large-gap B/S and vr_log from 10:00-10:30",
        "targets": list(TARGETS),
        "entry": "10:31 close",
        "factor_directions": {
            "large_gap_B": "long low B, short high B",
            "large_gap_S": "long high S, short low S",
            "vr_log": "long low VR, short high VR",
        },
        "strategies": {
            "gap_baseline_30": "factor-oriented bottom/top 30% legs",
            "strict_both_30": "factor and VR both in confirming 30% tails",
            "vr_median_filter": "factor 30% legs confirmed by the matching VR half",
            "short_vr_confirmed": "unfiltered factor long leg; short leg also requires high VR",
        },
        "primary": "within-date/domain 1%-99% winsorization, LOB5-ex-size residualization, percentile ranks, then domain aggregation",
        "style_columns": list(LOB5_EX_SIZE_COLS),
        "style_timing": "previous trading day",
        "missing_label_policy": "select on signal-time universe, filter each target independently afterward",
        "universe_rule": "point-in-time Shanghai/Shenzhen A shares; ETF count zero",
        "costs": "not deducted",
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2) + "\n"
    )
    print(
        f"common={len(common)} group_rows={sum(len(rows) for rows in groups.values())} "
        f"performance_rows={len(performance)} summary_rows={len(summary)} output_dir={output_dir}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
