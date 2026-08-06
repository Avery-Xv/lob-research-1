#!/usr/bin/env python3
"""Layer-one mechanism tests for the intraday B large-gap ratio."""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path
from statistics import mean
from typing import NamedTuple, Sequence

import backtest_large_gap_by_raw_vr_state as base
import backtest_large_gap_by_raw_vr_state_all_market_neutral as neutral
from backtest_daily_domains import domain, pearson, ranks
from backtest_existing_daily_o2o_cne5 import (
    build_orthonormal_basis,
    finite,
    residualize,
)


TARGETS = ("ret_1031_1040", "ret_1031_1045", "ret_1031_1100")
STYLE_NAMES = neutral.LOB4_NO_SIZE_COLS
MODEL_CONTROLS = {
    "m0_b_only": (),
    "m1_pre_return": ("pre_return",),
    "m2_microstructure": (
        "pre_return", "vr_log", "cr_log", "single_size_ratio_log", "sell_gap",
    ),
    "m3_microstructure_lob4": (
        "pre_return", "vr_log", "cr_log", "single_size_ratio_log", "sell_gap",
        *STYLE_NAMES,
    ),
}


class Observation(NamedTuple):
    symbol: str
    buy_gap: float
    sell_gap: float
    vr_log: float
    cr_log: float
    single_size_ratio_log: float
    pre_return: float
    targets: tuple[float | None, ...]
    styles: tuple[float, ...]
    previous_market_cap: float
    signal_price: float


def centered_percentiles(values: Sequence[float]) -> list[float]:
    return [value - 0.5 for value in base.percentile_ranks(values)]


def build_model_residuals(rows: Sequence[Observation]) -> dict[str, list[float]]:
    """Residualize ranked B once across the full date cross-section."""
    if not rows:
        raise ValueError("rows must not be empty")
    columns: dict[str, list[float]] = {
        "buy_gap": centered_percentiles([row.buy_gap for row in rows]),
        "sell_gap": centered_percentiles([row.sell_gap for row in rows]),
        "vr_log": centered_percentiles([row.vr_log for row in rows]),
        "cr_log": centered_percentiles([row.cr_log for row in rows]),
        "single_size_ratio_log": centered_percentiles(
            [row.single_size_ratio_log for row in rows]
        ),
        "pre_return": centered_percentiles([row.pre_return for row in rows]),
    }
    for style_index, style_name in enumerate(STYLE_NAMES):
        columns[style_name] = centered_percentiles(
            [row.styles[style_index] for row in rows]
        )
    output: dict[str, list[float]] = {}
    for model, controls in MODEL_CONTROLS.items():
        if not controls:
            output[model] = residualize(columns["buy_gap"], [])
            continue
        exposures = [[columns[name][index] for name in controls] for index in range(len(rows))]
        output[model] = residualize(
            columns["buy_gap"], build_orthonormal_basis(exposures)
        )
    return output


def load_pre_returns(path: str) -> dict[tuple[str, int], float]:
    output: dict[tuple[str, int], float] = {}
    with Path(path).open(newline="") as handle:
        for row in csv.DictReader(handle):
            key = (row["symbol"], int(row["date"]))
            value = finite(row.get("ret_1000_1030"))
            if key in output:
                raise ValueError(f"duplicate pre-return row: {key}")
            if value is not None:
                output[key] = float(value)
    return output


def load_groups(
    factor_path: str,
    common: dict[tuple[str, int], neutral.CommonValue],
    pre_returns: dict[tuple[str, int], float],
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
            if not symbol.startswith(("SH", "SZ")):
                raise ValueError(f"non-SH/SZ symbol in factor universe: {symbol}")
            if "ETF excluded" not in row.get("universe_rule", ""):
                raise ValueError("factor artifact does not document ETF exclusion")
            common_value = common.get(key)
            pre_return = pre_returns.get(key)
            values = [
                finite(row.get("intraday_large_gap_buy_ratio")),
                finite(row.get("intraday_large_gap_sell_ratio")),
                finite(row.get("vr_log")),
                finite(row.get("cr_log")),
                finite(row.get("single_size_ratio_log")),
            ]
            if (
                common_value is None
                or pre_return is None
                or any(value is None for value in values)
                or not base.parse_bool(row.get("intraday_passes_match_rate"))
                or not base.parse_bool(row.get("ob_is_valid"))
            ):
                continue
            signal_price, targets, styles, previous_cap = common_value
            grouped[date].append(Observation(
                symbol=symbol,
                buy_gap=float(values[0]),
                sell_gap=float(values[1]),
                vr_log=float(values[2]),
                cr_log=float(values[3]),
                single_size_ratio_log=float(values[4]),
                pre_return=pre_return,
                targets=targets,
                styles=tuple(styles),
                previous_market_cap=previous_cap,
                signal_price=signal_price,
            ))
    return grouped


def score_spread(
    scores: Sequence[float], returns: Sequence[float], symbols: Sequence[str]
) -> float:
    return base.score_spread(scores, returns, symbols)


def append_metrics(
    output: list[dict[str, object]],
    date: int,
    scope: str,
    cap_group: str,
    price_group: str,
    vr_state: str,
    model: str,
    rows: Sequence[Observation],
    scores: Sequence[float],
    minimum_cross_section: int,
) -> None:
    for target_index, target in enumerate(TARGETS):
        eligible = [
            index for index, row in enumerate(rows)
            if row.targets[target_index] is not None
        ]
        if len(eligible) < minimum_cross_section:
            continue
        model_scores = [scores[index] for index in eligible]
        returns = [float(rows[index].targets[target_index]) for index in eligible]
        symbols = [rows[index].symbol for index in eligible]
        output.append({
            "model": model,
            "target": target,
            "scope": scope,
            "cap_group": cap_group,
            "price_group": price_group,
            "vr_state": vr_state,
            "date": date,
            "n": len(eligible),
            "rank_ic": pearson(ranks(model_scores), ranks(returns)),
            "d10_d1": score_spread(model_scores, returns, symbols),
        })


def exact_bucket_labels(
    values: Sequence[float], symbols: Sequence[str], bucket_count: int, prefix: str
) -> list[str]:
    order = sorted(range(len(values)), key=lambda index: (values[index], symbols[index]))
    labels = [""] * len(values)
    for position, index in enumerate(order):
        bucket = min(bucket_count - 1, position * bucket_count // len(order)) + 1
        labels[index] = f"{prefix}{bucket}"
    return labels


def process_groups(
    grouped: dict[int, list[Observation]], minimum_cross_section: int
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    performance: list[dict[str, object]] = []
    tail_cells: dict[tuple[int, str, str, str], list[float]] = defaultdict(list)
    for date, date_rows in sorted(grouped.items()):
        ordered = sorted(date_rows, key=lambda row: row.symbol)
        residuals = build_model_residuals(ordered)
        residual_by_model = {
            model: {row.symbol: value for row, value in zip(ordered, values)}
            for model, values in residuals.items()
        }
        domains: dict[tuple[str, str], list[Observation]] = defaultdict(list)
        for row in ordered:
            group = domain(row.previous_market_cap, row.signal_price, row.symbol)
            if group is not None:
                domains[group].append(row)
        pooled: dict[tuple[str, str], list[tuple[Observation, float]]] = defaultdict(list)
        for (cap_group, price_group), domain_rows in sorted(domains.items()):
            if len(domain_rows) < minimum_cross_section:
                continue
            domain_rows.sort(key=lambda row: row.symbol)
            states = base.assign_raw_vr_states(
                [row.vr_log for row in domain_rows],
                [row.symbol for row in domain_rows],
            )
            for model in MODEL_CONTROLS:
                domain_scores = base.percentile_ranks(
                    [residual_by_model[model][row.symbol] for row in domain_rows]
                )
                for vr_state in base.VR_STATES:
                    indices = [index for index, state in enumerate(states) if state == vr_state]
                    state_rows = [domain_rows[index] for index in indices]
                    state_scores = [domain_scores[index] for index in indices]
                    append_metrics(
                        performance, date, "domain", cap_group, price_group,
                        vr_state, model, state_rows, state_scores,
                        minimum_cross_section,
                    )
                    pooled[(model, vr_state)].extend(zip(state_rows, state_scores))

            high_indices = [index for index, state in enumerate(states) if state == "high"]
            if len(high_indices) < 30:
                continue
            high_rows = [domain_rows[index] for index in high_indices]
            b_labels = exact_bucket_labels(
                [row.buy_gap for row in high_rows],
                [row.symbol for row in high_rows], 10, "b",
            )
            pre_labels = exact_bucket_labels(
                [row.pre_return for row in high_rows],
                [row.symbol for row in high_rows], 3, "pre",
            )
            for row, b_label, pre_label in zip(high_rows, b_labels, pre_labels):
                for target_index, target in enumerate(TARGETS):
                    value = row.targets[target_index]
                    if value is not None:
                        tail_cells[(date, target, pre_label, b_label)].append(float(value))

        for (model, vr_state), pairs in sorted(pooled.items()):
            pairs.sort(key=lambda pair: pair[0].symbol)
            append_metrics(
                performance, date, "domain_neutral_aggregate", "domain_neutral",
                "aggregate", vr_state, model,
                [pair[0] for pair in pairs], [pair[1] for pair in pairs],
                minimum_cross_section,
            )
    tail_by_date = [
        {
            "target": target,
            "vr_state": "high",
            "pre_return_state": pre_state,
            "b_decile": b_decile,
            "date": date,
            "n": len(values),
            "mean_return": mean(values),
        }
        for (date, target, pre_state, b_decile), values in sorted(tail_cells.items())
    ]
    return performance, tail_by_date


def summarize_performance(rows: Sequence[dict[str, object]]) -> list[dict[str, object]]:
    keys = ("model", "target", "scope", "cap_group", "price_group", "vr_state")
    grouped: dict[tuple[str, ...], list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        grouped[tuple(str(row[key]) for key in keys)].append(row)
    output: list[dict[str, object]] = []
    for key, values in sorted(grouped.items()):
        result: dict[str, object] = dict(zip(keys, key))
        result.update(
            n_days=len(values),
            n_obs=sum(int(row["n"]) for row in values),
            avg_names=mean(int(row["n"]) for row in values),
        )
        for metric in ("rank_ic", "d10_d1"):
            result[metric], result[f"{metric}_t"] = base.mean_t(
                [float(row[metric]) for row in values]
            )
        output.append(result)
    return output


def summarize_tail(rows: Sequence[dict[str, object]]) -> list[dict[str, object]]:
    keys = ("target", "vr_state", "pre_return_state", "b_decile")
    grouped: dict[tuple[str, ...], list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        grouped[tuple(str(row[key]) for key in keys)].append(row)
    output: list[dict[str, object]] = []
    for key, values in sorted(grouped.items()):
        average, t_value = base.mean_t([float(row["mean_return"]) for row in values])
        output.append({
            **dict(zip(keys, key)),
            "n_days": len(values),
            "n_obs": sum(int(row["n"]) for row in values),
            "mean_return": average,
            "mean_return_t": t_value,
        })
    return output


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--factors", required=True)
    parser.add_argument("--returns", required=True)
    parser.add_argument("--pre-returns", required=True)
    parser.add_argument("--market-caps", required=True)
    parser.add_argument("--styles", required=True)
    parser.add_argument("--date-from", type=int, default=20260201)
    parser.add_argument("--date-to", type=int, default=20260430)
    parser.add_argument("--minimum-cross-section", type=int, default=20)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()

    common = neutral.load_common(
        args.returns, args.market_caps, args.styles, TARGETS,
        args.date_from, args.date_to, STYLE_NAMES,
    )
    pre_returns = load_pre_returns(args.pre_returns)
    grouped = load_groups(
        args.factors, common, pre_returns, args.date_from, args.date_to
    )
    performance, tail_by_date = process_groups(grouped, args.minimum_cross_section)
    performance_summary = summarize_performance(performance)
    tail_summary = summarize_tail(tail_by_date)
    output_dir = Path(args.output_dir)
    base.write_csv(output_dir / "performance_by_date.csv", performance)
    base.write_csv(output_dir / "performance_summary.csv", performance_summary)
    base.write_csv(output_dir / "high_vr_b_pre_return_grid_by_date.csv", tail_by_date)
    base.write_csv(output_dir / "high_vr_b_pre_return_grid_summary.csv", tail_summary)
    output_dir.mkdir(parents=True, exist_ok=True)
    metadata = {
        "date_from": args.date_from,
        "date_to": args.date_to,
        "signal_window": "10:00:00 inclusive to 10:30:00 exclusive",
        "entry_and_targets": "10:30 minute close to 10:40/10:45/11:00 minute close",
        "pre_return": (
            "latest available 1-minute close at or before 10:30 divided by latest "
            "available 1-minute close at or before 10:00, minus one"
        ),
        "models": {name: list(controls) for name, controls in MODEL_CONTROLS.items()},
        "transform": (
            "per-date all-market percentile ranks; residualize ranked B on ranked controls; "
            "then rank residual within structural domain and split raw vr_log terciles"
        ),
        "winsorization": "none; percentile ranks bound outlier leverage",
        "collinearity": (
            "single_size_ratio_log equals vr_log-cr_log algebraically; orthonormalization "
            "drops the redundant direction"
        ),
        "style_specification": "LOB4-no-size",
        "style_columns": list(STYLE_NAMES),
        "exposure_timing": "previous trading-day CNE5 styles and market cap",
        "universe_rule": "point-in-time Shanghai/Shenzhen A shares; ETF excluded in V4 factor artifact",
        "validity": "match_rate>=0.95, ob_is_valid, non-ST, non-suspended",
        "missing_label_policy": "scores and states formed before target-specific filtering",
        "tail_grid": (
            "within each date-domain high-vr tercile, cross raw B exact deciles with "
            "pre-return exact terciles"
        ),
        "common_rows": len(common),
        "pre_return_rows": len(pre_returns),
        "analysis_rows": sum(len(rows) for rows in grouped.values()),
    }
    (output_dir / "metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2) + "\n"
    )
    print(
        f"dates={len(grouped)} analysis_rows={metadata['analysis_rows']} "
        f"performance_rows={len(performance)} tail_rows={len(tail_by_date)} "
        f"output_dir={output_dir}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
