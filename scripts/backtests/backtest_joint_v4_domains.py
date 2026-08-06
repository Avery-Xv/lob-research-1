#!/usr/bin/env python3
"""Domain-neutralized backtest for joint V4 large-gap/order factors."""

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
DAILY_TARGETS = ("open_to_open_d1",)
INTRADAY_TARGETS = (
    "ret_1031_1040",
    "ret_1031_1045",
    "abs_ret_1031_1040",
    "abs_ret_1031_1045",
)
FACTOR_SPECS = (
    ("daily", "large_gap_B", "daily_large_gap_buy_ratio", "daily_passes_match_rate"),
    ("daily", "large_gap_S", "daily_large_gap_sell_ratio", "daily_passes_match_rate"),
    ("intraday", "large_gap_B", "intraday_large_gap_buy_ratio", "intraday_passes_match_rate"),
    ("intraday", "large_gap_S", "intraday_large_gap_sell_ratio", "intraday_passes_match_rate"),
    ("intraday", "vr_log", "vr_log", "ob_is_valid"),
    ("intraday", "cr_log", "cr_log", "ob_is_valid"),
    ("intraday", "single_size_ratio_log", "single_size_ratio_log", "ob_is_valid"),
)

CommonValue = tuple[float, tuple[float | None, ...], list[float], float]
Observation = tuple[str, float, tuple[float | None, ...], list[float], float, float]


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


def score_spread(
    scores: Sequence[float], returns: Sequence[float], symbols: Sequence[str]
) -> float:
    """Return top-minus-bottom decile return with deterministic tie ordering."""
    order = sorted(range(len(scores)), key=lambda i: (scores[i], symbols[i]))
    ordered_returns = [returns[i] for i in order]
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
    intraday_returns_path: str,
    caps_path: str,
    styles_path: str,
    date_from: int,
    date_to: int,
) -> tuple[dict[tuple[str, int], CommonValue], dict[tuple[str, int], CommonValue]]:
    connection = duckdb.connect()
    connection.read_csv(returns_path).create_view("returns_raw")
    connection.read_csv(intraday_returns_path).create_view("intraday_raw")
    connection.read_csv(caps_path).create_view("caps_raw")
    connection.read_csv(styles_path).create_view("styles_raw")
    style_select = ", ".join(f"{name}::DOUBLE AS {name}" for name in LOB5_EX_SIZE_COLS)
    current_styles = ", ".join(f"s.{name}::DOUBLE" for name in LOB5_EX_SIZE_COLS)
    previous_styles = ", ".join(f"s.previous_{name}::DOUBLE" for name in LOB5_EX_SIZE_COLS)
    previous_style_select = ", ".join(
        f"lag({name}) OVER (PARTITION BY symbol ORDER BY date) AS previous_{name}"
        for name in LOB5_EX_SIZE_COLS
    )
    connection.execute(
        f"""
        CREATE VIEW returns AS
        SELECT DISTINCT symbol, date::INTEGER AS date, next_date::INTEGER AS next_date,
               open::DOUBLE AS open, o2o_ret::DOUBLE AS o2o_ret
        FROM returns_raw WHERE next_date::INTEGER > date::INTEGER;
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
    daily_rows = connection.execute(
        f"""
        SELECT r0.symbol, r0.date, r0.open, r1.o2o_ret, c.previous_market_cap,
               {current_styles}
        FROM returns r0
        LEFT JOIN returns r1 ON r1.symbol=r0.symbol AND r1.date=r0.next_date
        JOIN previous_caps c ON c.symbol=r0.symbol AND c.date=r0.date
        JOIN styles s ON s.symbol=r0.symbol AND s.date=r0.date
        WHERE r0.date BETWEEN ? AND ?
        """,
        [date_from, date_to],
    ).fetchall()
    intraday_rows = connection.execute(
        f"""
        SELECT r.symbol, r.date::INTEGER, r.signal_price::DOUBLE,
               r.ret_1031_1040::DOUBLE, r.ret_1031_1045::DOUBLE,
               r.abs_ret_1031_1040::DOUBLE, r.abs_ret_1031_1045::DOUBLE,
               c.previous_market_cap, {previous_styles}
        FROM intraday_raw r
        JOIN previous_caps c ON c.symbol=r.symbol AND c.date=r.date::INTEGER
        JOIN previous_styles s ON s.symbol=r.symbol AND s.date=r.date::INTEGER
        WHERE r.date::INTEGER BETWEEN ? AND ?
          AND r.is_st::INTEGER=0 AND r.is_suspended::INTEGER=0
        """,
        [date_from, date_to],
    ).fetchall()
    connection.close()

    daily: dict[tuple[str, int], CommonValue] = {}
    for row in daily_rows:
        price, previous_cap = finite(row[2]), finite(row[4])
        target = finite(row[3])
        styles = [finite(value) for value in row[5:]]
        if price is None or price <= 0 or previous_cap is None or any(v is None for v in styles):
            continue
        daily[(str(row[0]), int(row[1]))] = (
            float(price), (target,), [float(v) for v in styles if v is not None], float(previous_cap)
        )

    intraday: dict[tuple[str, int], CommonValue] = {}
    for row in intraday_rows:
        price, previous_cap = finite(row[2]), finite(row[7])
        targets = tuple(finite(value) for value in row[3:7])
        styles = [finite(value) for value in row[8:]]
        if price is None or price <= 0 or previous_cap is None or any(v is None for v in styles):
            continue
        intraday[(str(row[0]), int(row[1]))] = (
            float(price), targets, [float(v) for v in styles if v is not None], float(previous_cap)
        )
    return daily, intraday


def load_factor_groups(
    factor_path: str,
    daily_common: dict[tuple[str, int], CommonValue],
    intraday_common: dict[tuple[str, int], CommonValue],
    date_from: int,
    date_to: int,
) -> dict[tuple[str, str, int], list[Observation]]:
    grouped: dict[tuple[str, str, int], list[Observation]] = defaultdict(list)
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
            for frequency, factor_name, column, valid_column in FACTOR_SPECS:
                if not parse_bool(row.get(valid_column)):
                    continue
                value = finite(row.get(column))
                common = daily_common if frequency == "daily" else intraday_common
                common_value = common.get(key)
                if value is None or common_value is None:
                    continue
                price, targets, styles, previous_cap = common_value
                grouped[(frequency, factor_name, date)].append(
                    (symbol, float(value), targets, styles, previous_cap, price)
                )
    return grouped


def append_metrics(
    output: list[dict[str, object]],
    frequency: str,
    factor_name: str,
    date: int,
    scope: str,
    cap_group: str,
    price_group: str,
    rows: Sequence[Observation],
    neutral_scores: Sequence[float],
) -> None:
    target_names = DAILY_TARGETS if frequency == "daily" else INTRADAY_TARGETS
    raw_scores = [row[1] for row in rows]
    symbols = [row[0] for row in rows]
    for target_index, target_name in enumerate(target_names):
        eligible = [i for i, row in enumerate(rows) if row[2][target_index] is not None]
        if len(eligible) < 20:
            continue
        returns = [float(rows[i][2][target_index]) for i in eligible]
        raw = [raw_scores[i] for i in eligible]
        neutral = [neutral_scores[i] for i in eligible]
        names = [symbols[i] for i in eligible]
        output.append({
            "frequency": frequency,
            "factor": factor_name,
            "window_name": "daily_continuous" if frequency == "daily" else "intraday_1000_1030",
            "target": target_name,
            "scope": scope,
            "cap_group": cap_group,
            "price_group": price_group,
            "date": date,
            "n": len(eligible),
            "raw_rank_ic": pearson(ranks(raw), ranks(returns)),
            "lob5_ex_size_rank_ic": pearson(ranks(neutral), ranks(returns)),
            "raw_d10_d1": score_spread(raw, returns, names),
            "lob5_ex_size_d10_d1": score_spread(neutral, returns, names),
        })


def append_exposure(
    output: list[dict[str, object]],
    frequency: str,
    factor_name: str,
    date: int,
    rows: Sequence[Observation],
) -> None:
    factors = [row[1] for row in rows]
    styles = [row[3] for row in rows]
    basis = build_orthonormal_basis(styles)
    residual = residualize(factors, basis)
    center = mean(factors)
    total = sum((value - center) ** 2 for value in factors)
    unexplained = sum(value * value for value in residual)
    result: dict[str, object] = {
        "frequency": frequency,
        "factor": factor_name,
        "window_name": "daily_continuous" if frequency == "daily" else "intraday_1000_1030",
        "date": date,
        "n": len(rows),
        "joint_r2": max(0.0, min(1.0, 1.0 - unexplained / total)) if total > 0 else None,
    }
    factor_ranks = ranks(factors)
    for index, style_name in enumerate(LOB5_EX_SIZE_COLS):
        result[f"{style_name}_rank_exposure"] = pearson(
            factor_ranks, ranks([row[3][index] for row in rows])
        )
    output.append(result)


def process_groups(
    grouped: dict[tuple[str, str, int], list[Observation]],
    minimum_cross_section: int,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    performance: list[dict[str, object]] = []
    exposures: list[dict[str, object]] = []
    for (frequency, factor_name, date), rows in sorted(grouped.items()):
        domains: dict[tuple[str, str], list[Observation]] = defaultdict(list)
        for row in rows:
            group = domain(row[4], row[5], row[0])
            if group is not None:
                domains[group].append(row)
        all_rows = sorted(
            (row for domain_rows in domains.values() for row in domain_rows),
            key=lambda row: row[0],
        )
        if len(all_rows) < minimum_cross_section:
            continue
        append_exposure(exposures, frequency, factor_name, date, all_rows)
        pooled_rows: list[Observation] = []
        pooled_scores: list[float] = []
        for (cap_group, price_group), domain_rows in sorted(domains.items()):
            if len(domain_rows) < minimum_cross_section:
                continue
            domain_rows.sort(key=lambda row: row[0])
            residual = residualize(
                [row[1] for row in domain_rows],
                build_orthonormal_basis([row[3] for row in domain_rows]),
            )
            append_metrics(
                performance, frequency, factor_name, date, "domain",
                cap_group, price_group, domain_rows, residual,
            )
            pooled_rows.extend(domain_rows)
            pooled_scores.extend(percentile_ranks(residual))
        if len(pooled_rows) >= minimum_cross_section:
            append_metrics(
                performance, frequency, factor_name, date,
                "domain_neutral_aggregate", "domain_neutral", "aggregate",
                pooled_rows, pooled_scores,
            )
        all_residual = residualize(
            [row[1] for row in all_rows],
            build_orthonormal_basis([row[3] for row in all_rows]),
        )
        append_metrics(
            performance, frequency, factor_name, date, "all_market",
            "all", "all", all_rows, all_residual,
        )
    return performance, exposures


def summarize_performance(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    keys = ("frequency", "factor", "window_name", "target", "scope", "cap_group", "price_group")
    grouped: dict[tuple[str, ...], list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        grouped[tuple(str(row[key]) for key in keys)].append(row)
    output = []
    for key, observations in sorted(grouped.items()):
        result: dict[str, object] = dict(zip(keys, key))
        result.update(
            n_days=len(observations), n_obs=sum(int(row["n"]) for row in observations),
            avg_names=mean(int(row["n"]) for row in observations),
        )
        for metric in ("raw_rank_ic", "lob5_ex_size_rank_ic", "raw_d10_d1", "lob5_ex_size_d10_d1"):
            values = [float(row[metric]) for row in observations if row[metric] is not None]
            result[metric], result[f"{metric}_t"] = mean_t(values)
        output.append(result)
    return output


def summarize_exposure(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    grouped: dict[tuple[str, str, str], list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        grouped[(str(row["frequency"]), str(row["factor"]), str(row["window_name"]))].append(row)
    output = []
    for key, observations in sorted(grouped.items()):
        result: dict[str, object] = {
            "frequency": key[0], "factor": key[1], "window_name": key[2],
            "n_days": len(observations), "n_obs": sum(int(row["n"]) for row in observations),
        }
        r2 = [float(row["joint_r2"]) for row in observations if row["joint_r2"] is not None]
        result["joint_r2"], result["joint_r2_t"] = mean_t(r2)
        for style in LOB5_EX_SIZE_COLS:
            column = f"{style}_rank_exposure"
            values = [float(row[column]) for row in observations if row[column] is not None]
            result[column], result[f"{column}_t"] = mean_t(values)
            result[f"{style}_mean_abs_rank_exposure"] = mean(map(abs, values)) if values else None
        output.append(result)
    return output


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--factors", required=True)
    parser.add_argument("--returns", required=True)
    parser.add_argument("--intraday-returns", required=True)
    parser.add_argument("--market-caps", required=True)
    parser.add_argument("--styles", required=True)
    parser.add_argument("--date-from", type=int, default=20260201)
    parser.add_argument("--date-to", type=int, default=20260430)
    parser.add_argument("--minimum-cross-section", type=int, default=20)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()

    daily_common, intraday_common = load_common(
        args.returns, args.intraday_returns, args.market_caps, args.styles,
        args.date_from, args.date_to,
    )
    groups = load_factor_groups(
        args.factors, daily_common, intraday_common, args.date_from, args.date_to
    )
    performance, exposures = process_groups(groups, args.minimum_cross_section)
    performance_summary = summarize_performance(performance)
    exposure_summary = summarize_exposure(exposures)
    output_dir = Path(args.output_dir)
    write_csv(output_dir / "performance_by_date.csv", performance)
    write_csv(output_dir / "performance_summary.csv", performance_summary)
    write_csv(output_dir / "exposure_by_date.csv", exposures)
    write_csv(output_dir / "exposure_summary.csv", exposure_summary)
    metadata = {
        "date_from": args.date_from,
        "date_to": args.date_to,
        "daily_timing": "factor on T continuous-auction close; target T+1 open to T+2 open",
        "intraday_timing": "factor 10:00-10:30; entry 10:31 close; exits 10:40/10:45 close",
        "intraday_exposure_timing": "previous trading-day CNE5 exposure and market cap",
        "daily_exposure_timing": "same-day close CNE5 exposure; previous trading-day market cap",
        "style_specification": "LOB5-ex-size",
        "style_columns": list(LOB5_EX_SIZE_COLS),
        "universe_rule": "point-in-time Shanghai/Shenzhen A shares; ETF count zero",
        "validity": "large-gap match_rate>=0.95; order behavior ob_is_valid=true",
        "missing_label_policy": "each target filtered independently after signal neutralization",
        "factor_rows": sum(len(rows) for rows in groups.values()),
        "daily_common": len(daily_common),
        "intraday_common": len(intraday_common),
    }
    (output_dir / "metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2) + "\n"
    )
    print(
        f"daily_common={len(daily_common)} intraday_common={len(intraday_common)} "
        f"performance_rows={len(performance)} exposure_rows={len(exposures)} "
        f"output_dir={output_dir}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
