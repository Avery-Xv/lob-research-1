#!/usr/bin/env python3
"""Backtest intraday large-gap B/S factors conditional on raw VR states."""

from __future__ import annotations

import argparse
import csv
import json
import math
from collections import defaultdict
from pathlib import Path
from statistics import mean, stdev
from typing import Sequence

import duckdb

from analyze_existing_factors_lob5_ex_size import LOB5_EX_SIZE_COLS
from backtest_daily_domains import domain, pearson, ranks
from backtest_existing_daily_o2o_cne5 import (
    build_orthonormal_basis,
    finite,
    residualize,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
TARGETS = ("ret_1031_1040", "ret_1031_1045")
FACTOR_SPECS = (
    ("large_gap_B", "intraday_large_gap_buy_ratio"),
    ("large_gap_S", "intraday_large_gap_sell_ratio"),
)
VR_STATES = ("low", "mid", "high")

CommonValue = tuple[float, tuple[float | None, float | None], list[float], float]
Observation = tuple[
    str, float, float, tuple[float | None, float | None], list[float], float, float
]


def parse_bool(value: str | None) -> bool:
    return str(value).strip().lower() in {"1", "true", "t", "yes"}


def mean_t(values: Sequence[float]) -> tuple[float | None, float | None]:
    if not values:
        return None, None
    average = mean(values)
    if len(values) < 2:
        return average, None
    volatility = stdev(values)
    return average, average / (volatility / math.sqrt(len(values))) if volatility else None


def percentile_ranks(values: Sequence[float]) -> list[float]:
    ranked = ranks(list(values))
    denominator = max(1, len(values) - 1)
    return [(value - 1.0) / denominator for value in ranked]


def assign_raw_vr_states(values: Sequence[float], symbols: Sequence[str]) -> list[str]:
    """Assign exact-count terciles using raw VR, with symbol-only tie breaking."""
    if len(values) != len(symbols):
        raise ValueError("VR values and symbols must have equal length")
    order = sorted(range(len(values)), key=lambda index: (values[index], symbols[index]))
    states = [""] * len(values)
    for position, index in enumerate(order):
        bucket = min(2, position * 3 // len(order))
        states[index] = VR_STATES[bucket]
    return states


def score_spread(
    scores: Sequence[float], returns: Sequence[float], symbols: Sequence[str]
) -> float:
    order = sorted(range(len(scores)), key=lambda index: (scores[index], symbols[index]))
    ordered_returns = [returns[index] for index in order]
    bucket = max(1, len(ordered_returns) // 10)
    return mean(ordered_returns[-bucket:]) - mean(ordered_returns[:bucket])


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


def load_common(
    returns_path: str,
    caps_path: str,
    styles_path: str,
    date_from: int,
    date_to: int,
) -> dict[tuple[str, int], CommonValue]:
    connection = duckdb.connect()
    connection.read_csv(returns_path).create_view("returns_raw")
    connection.read_csv(caps_path).create_view("caps_raw")
    connection.read_csv(styles_path).create_view("styles_raw")
    style_select = ", ".join(f"{name}::DOUBLE AS {name}" for name in LOB5_EX_SIZE_COLS)
    previous_style_select = ", ".join(
        f"lag({name}) OVER (PARTITION BY symbol ORDER BY date) AS previous_{name}"
        for name in LOB5_EX_SIZE_COLS
    )
    previous_styles = ", ".join(
        f"s.previous_{name}::DOUBLE" for name in LOB5_EX_SIZE_COLS
    )
    connection.execute(
        f"""
        CREATE VIEW caps AS
        SELECT DISTINCT symbol, date::INTEGER AS date, total_mv::DOUBLE AS total_mv
        FROM caps_raw;
        CREATE VIEW previous_caps AS
        SELECT symbol, date,
               lag(total_mv) OVER (PARTITION BY symbol ORDER BY date) AS previous_market_cap
        FROM caps;
        CREATE VIEW styles AS
        SELECT symbol, replace(date::VARCHAR, '-', '')::INTEGER AS date, {style_select}
        FROM styles_raw;
        CREATE VIEW previous_styles AS
        SELECT symbol, date, {previous_style_select}
        FROM styles;
        """
    )
    rows = connection.execute(
        f"""
        SELECT r.symbol, r.date::INTEGER, r.signal_price::DOUBLE,
               r.ret_1031_1040::DOUBLE, r.ret_1031_1045::DOUBLE,
               c.previous_market_cap, {previous_styles}
        FROM returns_raw r
        JOIN previous_caps c ON c.symbol=r.symbol AND c.date=r.date::INTEGER
        JOIN previous_styles s ON s.symbol=r.symbol AND s.date=r.date::INTEGER
        WHERE r.date::INTEGER BETWEEN ? AND ?
          AND r.is_st::INTEGER=0 AND r.is_suspended::INTEGER=0
        """,
        [date_from, date_to],
    ).fetchall()
    connection.close()

    result: dict[tuple[str, int], CommonValue] = {}
    for row in rows:
        price, previous_cap = finite(row[2]), finite(row[5])
        targets = (finite(row[3]), finite(row[4]))
        styles = [finite(value) for value in row[6:]]
        if price is None or price <= 0 or previous_cap is None or any(v is None for v in styles):
            continue
        result[(str(row[0]), int(row[1]))] = (
            float(price), targets, [float(v) for v in styles if v is not None], float(previous_cap)
        )
    return result


def load_groups(
    factor_path: str,
    common: dict[tuple[str, int], CommonValue],
    date_from: int,
    date_to: int,
) -> dict[tuple[str, int], list[Observation]]:
    grouped: dict[tuple[str, int], list[Observation]] = defaultdict(list)
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
            vr = finite(row.get("vr_log"))
            if common_value is None or vr is None or not parse_bool(row.get("ob_is_valid")):
                continue
            if not parse_bool(row.get("intraday_passes_match_rate")):
                continue
            price, targets, styles, previous_cap = common_value
            for factor_name, factor_column in FACTOR_SPECS:
                factor = finite(row.get(factor_column))
                if factor is not None:
                    grouped[(factor_name, date)].append(
                        (symbol, float(vr), float(factor), targets, styles, previous_cap, price)
                    )
    return grouped


def append_metrics(
    output: list[dict[str, object]],
    factor_name: str,
    date: int,
    scope: str,
    cap_group: str,
    price_group: str,
    vr_state: str,
    rows: Sequence[Observation],
    neutral_scores: Sequence[float],
    minimum_cross_section: int,
) -> None:
    symbols = [row[0] for row in rows]
    raw_scores = [row[2] for row in rows]
    vr_values = [row[1] for row in rows]
    for target_index, target_name in enumerate(TARGETS):
        eligible = [index for index, row in enumerate(rows) if row[3][target_index] is not None]
        if len(eligible) < minimum_cross_section:
            continue
        returns = [float(rows[index][3][target_index]) for index in eligible]
        raw = [raw_scores[index] for index in eligible]
        neutral = [neutral_scores[index] for index in eligible]
        names = [symbols[index] for index in eligible]
        output.append({
            "factor": factor_name,
            "window_name": "intraday_1000_1030",
            "target": target_name,
            "scope": scope,
            "cap_group": cap_group,
            "price_group": price_group,
            "vr_state": vr_state,
            "date": date,
            "n": len(eligible),
            "mean_raw_vr_log": mean(vr_values[index] for index in eligible),
            "min_raw_vr_log": min(vr_values[index] for index in eligible),
            "max_raw_vr_log": max(vr_values[index] for index in eligible),
            "raw_rank_ic": pearson(ranks(raw), ranks(returns)),
            "lob5_ex_size_rank_ic": pearson(ranks(neutral), ranks(returns)),
            "raw_d10_d1": score_spread(raw, returns, names),
            "lob5_ex_size_d10_d1": score_spread(neutral, returns, names),
        })


def process_groups(
    grouped: dict[tuple[str, int], list[Observation]],
    minimum_cross_section: int,
) -> list[dict[str, object]]:
    performance: list[dict[str, object]] = []
    for (factor_name, date), rows in sorted(grouped.items()):
        domains: dict[tuple[str, str], list[Observation]] = defaultdict(list)
        for row in rows:
            group = domain(row[5], row[6], row[0])
            if group is not None:
                domains[group].append(row)
        pooled: dict[str, list[tuple[Observation, float]]] = defaultdict(list)
        for (cap_group, price_group), domain_rows in sorted(domains.items()):
            if len(domain_rows) < minimum_cross_section:
                continue
            domain_rows.sort(key=lambda row: row[0])
            residual = residualize(
                [row[2] for row in domain_rows],
                build_orthonormal_basis([row[4] for row in domain_rows]),
            )
            scores = percentile_ranks(residual)
            states = assign_raw_vr_states(
                [row[1] for row in domain_rows], [row[0] for row in domain_rows]
            )
            for vr_state in VR_STATES:
                indices = [index for index, state in enumerate(states) if state == vr_state]
                state_rows = [domain_rows[index] for index in indices]
                state_scores = [scores[index] for index in indices]
                append_metrics(
                    performance, factor_name, date, "domain", cap_group, price_group,
                    vr_state, state_rows, state_scores, minimum_cross_section,
                )
                pooled[vr_state].extend(zip(state_rows, state_scores))
        for vr_state in VR_STATES:
            state_pairs = pooled.get(vr_state, [])
            if not state_pairs:
                continue
            state_pairs.sort(key=lambda item: item[0][0])
            state_rows = [item[0] for item in state_pairs]
            state_scores = [item[1] for item in state_pairs]
            append_metrics(
                performance, factor_name, date, "domain_neutral_aggregate",
                "domain_neutral", "aggregate", vr_state, state_rows, state_scores,
                minimum_cross_section,
            )
    return performance


def summarize_performance(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    keys = ("factor", "window_name", "target", "scope", "cap_group", "price_group", "vr_state")
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
            mean_raw_vr_log=mean(float(row["mean_raw_vr_log"]) for row in observations),
            avg_min_raw_vr_log=mean(float(row["min_raw_vr_log"]) for row in observations),
            avg_max_raw_vr_log=mean(float(row["max_raw_vr_log"]) for row in observations),
        )
        for metric in ("raw_rank_ic", "lob5_ex_size_rank_ic", "raw_d10_d1", "lob5_ex_size_d10_d1"):
            values = [float(row[metric]) for row in observations if row[metric] is not None]
            result[metric], result[f"{metric}_t"] = mean_t(values)
        output.append(result)
    return output


def build_contrasts(rows: list[dict[str, object]]) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    aggregate = [row for row in rows if row["scope"] == "domain_neutral_aggregate"]
    indexed = {
        (str(row["factor"]), str(row["target"]), int(row["date"]), str(row["vr_state"])): row
        for row in aggregate
    }
    by_date: list[dict[str, object]] = []
    keys = sorted({(key[0], key[1], key[2]) for key in indexed})
    for factor, target, date in keys:
        low = indexed.get((factor, target, date, "low"))
        high = indexed.get((factor, target, date, "high"))
        if low is None or high is None:
            continue
        by_date.append({
            "factor": factor,
            "target": target,
            "date": date,
            "low_n": low["n"],
            "high_n": high["n"],
            "raw_rank_ic_high_minus_low": float(high["raw_rank_ic"]) - float(low["raw_rank_ic"]),
            "lob5_ex_size_rank_ic_high_minus_low": (
                float(high["lob5_ex_size_rank_ic"]) - float(low["lob5_ex_size_rank_ic"])
            ),
            "raw_d10_d1_high_minus_low": float(high["raw_d10_d1"]) - float(low["raw_d10_d1"]),
            "lob5_ex_size_d10_d1_high_minus_low": (
                float(high["lob5_ex_size_d10_d1"]) - float(low["lob5_ex_size_d10_d1"])
            ),
        })
    grouped: dict[tuple[str, str], list[dict[str, object]]] = defaultdict(list)
    for row in by_date:
        grouped[(str(row["factor"]), str(row["target"]))].append(row)
    summary: list[dict[str, object]] = []
    metrics = (
        "raw_rank_ic_high_minus_low",
        "lob5_ex_size_rank_ic_high_minus_low",
        "raw_d10_d1_high_minus_low",
        "lob5_ex_size_d10_d1_high_minus_low",
    )
    for (factor, target), observations in sorted(grouped.items()):
        result: dict[str, object] = {
            "factor": factor,
            "target": target,
            "n_days": len(observations),
            "avg_low_names": mean(int(row["low_n"]) for row in observations),
            "avg_high_names": mean(int(row["high_n"]) for row in observations),
        }
        for metric in metrics:
            result[metric], result[f"{metric}_t"] = mean_t(
                [float(row[metric]) for row in observations]
            )
        summary.append(result)
    return by_date, summary


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--factors", required=True)
    parser.add_argument("--intraday-returns", required=True)
    parser.add_argument("--market-caps", required=True)
    parser.add_argument("--styles", required=True)
    parser.add_argument("--date-from", type=int, default=20260201)
    parser.add_argument("--date-to", type=int, default=20260430)
    parser.add_argument("--minimum-cross-section", type=int, default=20)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()

    common = load_common(
        args.intraday_returns, args.market_caps, args.styles, args.date_from, args.date_to
    )
    groups = load_groups(args.factors, common, args.date_from, args.date_to)
    performance = process_groups(groups, args.minimum_cross_section)
    summary = summarize_performance(performance)
    contrast_by_date, contrast_summary = build_contrasts(performance)
    output_dir = Path(args.output_dir)
    write_csv(output_dir / "performance_by_date.csv", performance)
    write_csv(output_dir / "performance_summary.csv", summary)
    write_csv(output_dir / "state_contrast_by_date.csv", contrast_by_date)
    write_csv(output_dir / "state_contrast_summary.csv", contrast_summary)
    metadata = {
        "date_from": args.date_from,
        "date_to": args.date_to,
        "signal": "large-gap B/S and raw vr_log from 10:00-10:30",
        "entry_and_targets": "10:31 close to 10:40/10:45 close signed returns",
        "vr_state_rule": (
            "raw vr_log exact-count terciles within each date and each of nine structural domains; "
            "vr_log is not neutralized; symbol breaks ties"
        ),
        "factor_rule": (
            "large-gap B/S residualized within date/domain on LOB5-ex-size, then percentile-ranked"
        ),
        "style_columns": list(LOB5_EX_SIZE_COLS),
        "exposure_timing": "previous trading-day CNE5 exposures and market cap",
        "universe_rule": "point-in-time Shanghai/Shenzhen A shares; ETF count zero",
        "validity": "large-gap match_rate>=0.95 and ob_is_valid=true; ST/suspended excluded",
        "missing_label_policy": "state assignment and factor neutralization precede target-specific filtering",
        "common_rows": len(common),
        "group_rows": sum(len(rows) for rows in groups.values()),
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2) + "\n"
    )
    print(
        f"common={len(common)} groups={len(groups)} performance_rows={len(performance)} "
        f"output_dir={output_dir}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
