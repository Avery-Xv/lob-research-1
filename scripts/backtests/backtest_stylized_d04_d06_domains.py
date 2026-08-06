#!/usr/bin/env python3
"""Leakage-safe LOB5-ex-size domain backtest for daily D04--D06 factors."""

from __future__ import annotations

import argparse
import csv
import json
import math
from collections import Counter, defaultdict
from pathlib import Path
from statistics import mean, stdev
from typing import Sequence

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CAP_GROUPS = ("cap_lt_50yi", "cap_50_500yi", "cap_ge_500yi")
PRICE_GROUPS = ("non_star_lt_10", "non_star_ge_10", "star_ge_10")
LOB5_EX_SIZE_COLS = (
    "non_linear_size",
    "momentum",
    "liquidity",
    "beta",
    "residual_volatility",
)
TARGETS = ("open_to_open_d1", "open_to_open_d2", "open_to_open_d3", "open_to_open_d5")
TARGET_HAC_LAGS = {
    "open_to_open_d1": 0,
    "open_to_open_d2": 1,
    "open_to_open_d3": 2,
    "open_to_open_d5": 4,
}
FACTOR_SPECS = (
    ("D04_residual", "d04_residual", False),
    ("D05_surprise_60", "d05_surprise_60", False),
    ("D05_acceleration_3_20", "d05_acceleration_3_20", False),
    ("D05_persistence_5", "d05_persistence_5", False),
    ("D05_same_sign_count_5", "d05_same_sign_count_5", False),
    ("D05_same_sign_run_length", "d05_same_sign_run_length", False),
    ("D05_buy_surprise_60", "d05_buy_surprise_60", False),
    ("D05_sell_surprise_60", "d05_sell_surprise_60", False),
    ("D06_underreaction_event", "d06_underreaction_event", True),
    ("D06_diff", "d06_diff", False),
    ("D06_response_gap", "d06_response_gap", False),
)

CommonValue = tuple[float, tuple[float | None, ...], list[float], float]
FactorRow = tuple[str, float, tuple[float | None, ...], list[float], int]


def finite(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def domain(previous_market_cap: float, price: float, symbol: str) -> tuple[str, str] | None:
    if previous_market_cap < 500_000:
        cap_group = CAP_GROUPS[0]
    elif previous_market_cap < 5_000_000:
        cap_group = CAP_GROUPS[1]
    else:
        cap_group = CAP_GROUPS[2]
    star = symbol.startswith(("SH688", "SH689"))
    if not star:
        price_group = PRICE_GROUPS[0] if price < 10 else PRICE_GROUPS[1]
    elif price >= 10:
        price_group = PRICE_GROUPS[2]
    else:
        return None
    return cap_group, price_group


def ranks(values: Sequence[float]) -> list[float]:
    order = sorted(range(len(values)), key=lambda index: (values[index], index))
    output = [0.0] * len(values)
    start = 0
    while start < len(order):
        end = start + 1
        while end < len(order) and values[order[end]] == values[order[start]]:
            end += 1
        average_rank = (start + end - 1) / 2.0 + 1.0
        for position in range(start, end):
            output[order[position]] = average_rank
        start = end
    return output


def percentile_ranks(values: Sequence[float]) -> list[float]:
    ranked = ranks(values)
    return [0.5] * len(values) if len(values) < 2 else [
        (value - 1.0) / (len(values) - 1.0) for value in ranked
    ]


def pearson(xs: Sequence[float], ys: Sequence[float]) -> float | None:
    if len(xs) < 3:
        return None
    mx, my = mean(xs), mean(ys)
    vx = sum((value - mx) ** 2 for value in xs)
    vy = sum((value - my) ** 2 for value in ys)
    if vx <= 0 or vy <= 0:
        return None
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / math.sqrt(vx * vy)


def mean_t(values: Sequence[float]) -> tuple[float | None, float | None]:
    if not values:
        return None, None
    average = mean(values)
    if len(values) < 2:
        return average, None
    volatility = stdev(values)
    return average, average / (volatility / math.sqrt(len(values))) if volatility else None


def newey_west_mean_t(
    values: Sequence[float], max_lag: int
) -> tuple[float | None, float | None]:
    if not values:
        return None, None
    average = mean(values)
    if len(values) < 2:
        return average, None
    if max_lag == 0:
        return mean_t(values)
    centered = [value - average for value in values]
    n = len(centered)
    long_run_variance = sum(value * value for value in centered) / n
    for lag in range(1, min(max_lag, n - 1) + 1):
        covariance = sum(
            centered[index] * centered[index - lag]
            for index in range(lag, n)
        ) / n
        weight = 1.0 - lag / (max_lag + 1.0)
        long_run_variance += 2.0 * weight * covariance
    variance_of_mean = max(0.0, long_run_variance) / n
    return (
        average,
        average / math.sqrt(variance_of_mean) if variance_of_mean > 0 else None,
    )


def build_orthonormal_basis(exposures: Sequence[Sequence[float]]) -> list[list[float]]:
    if not exposures:
        return []
    basis_columns: list[list[float]] = []
    for column_index in range(len(exposures[0])):
        column = [row[column_index] for row in exposures]
        center = mean(column)
        vector = [value - center for value in column]
        for basis in basis_columns:
            projection = sum(value * base for value, base in zip(vector, basis))
            vector = [value - projection * base for value, base in zip(vector, basis)]
        norm = math.sqrt(sum(value * value for value in vector))
        if norm > 1e-10:
            basis_columns.append([value / norm for value in vector])
    return basis_columns


def residualize(values: Sequence[float], basis: Sequence[Sequence[float]]) -> list[float]:
    center = mean(values)
    residuals = [value - center for value in values]
    for column in basis:
        projection = sum(value * base for value, base in zip(residuals, column))
        residuals = [value - projection * base for value, base in zip(residuals, column)]
    return residuals


def compounded(values: Sequence[float | None]) -> float | None:
    if any(value is None for value in values):
        return None
    return math.prod(1.0 + float(value) for value in values) - 1.0


def spread(scores: Sequence[float], returns: Sequence[float], symbols: Sequence[str]) -> float:
    order = sorted(range(len(scores)), key=lambda index: (scores[index], symbols[index]))
    bucket = max(1, len(order) // 10)
    return mean(returns[index] for index in order[-bucket:]) - mean(
        returns[index] for index in order[:bucket]
    )


def event_gap(returns: Sequence[float], events: Sequence[int], sign: int) -> float | None:
    event_returns = [value for value, event in zip(returns, events) if event == sign]
    controls = [value for value, event in zip(returns, events) if event == 0]
    return mean(event_returns) - mean(controls) if event_returns and controls else None


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


def write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
    temporary.replace(path)


def load_common(
    *,
    returns_path: str,
    market_caps_path: str,
    styles_path: str,
    controls_path: str,
    date_from: int,
    date_to: int,
) -> dict[tuple[str, int], CommonValue]:
    connection = duckdb.connect()
    connection.read_csv(returns_path).create_view("returns_raw")
    connection.read_csv(market_caps_path).create_view("caps_raw")
    connection.read_csv(styles_path).create_view("styles_raw")
    connection.read_csv(controls_path).create_view("controls_raw")
    style_select = ", ".join(
        f"{column}::DOUBLE AS {column}" for column in LOB5_EX_SIZE_COLS
    )
    style_output = ", ".join(f"s.{column}::DOUBLE" for column in LOB5_EX_SIZE_COLS)
    rows = connection.execute(
        f"""
        WITH returns AS (
            SELECT DISTINCT symbol, date::INTEGER AS date,
                   next_date::INTEGER AS next_date, o2o_ret::DOUBLE AS o2o_ret
            FROM returns_raw
            WHERE next_date::INTEGER > date::INTEGER
        ),
        caps AS (
            SELECT DISTINCT symbol, date::INTEGER AS date, total_mv::DOUBLE AS total_mv
            FROM caps_raw
        ),
        previous_caps AS (
            SELECT symbol, date,
                   lag(total_mv) OVER (PARTITION BY symbol ORDER BY date)
                       AS previous_market_cap
            FROM caps
        ),
        styles AS (
            SELECT symbol, replace(date::VARCHAR, '-', '')::INTEGER AS date,
                   {style_select}
            FROM styles_raw
        ),
        controls AS (
            SELECT DISTINCT symbol, date::INTEGER AS date, close::DOUBLE AS close,
                   security_category::INTEGER AS security_category,
                   is_st::INTEGER AS is_st, is_suspended::INTEGER AS is_suspended
            FROM controls_raw
        )
        SELECT c.symbol, c.date, c.close, p.previous_market_cap,
               r1.o2o_ret, r2.o2o_ret, r3.o2o_ret, r4.o2o_ret, r5.o2o_ret,
               {style_output}
        FROM controls c
        JOIN previous_caps p USING (symbol, date)
        JOIN styles s USING (symbol, date)
        LEFT JOIN returns r0 USING (symbol, date)
        LEFT JOIN returns r1 ON r1.symbol=c.symbol AND r1.date=r0.next_date
        LEFT JOIN returns r2 ON r2.symbol=c.symbol AND r2.date=r1.next_date
        LEFT JOIN returns r3 ON r3.symbol=c.symbol AND r3.date=r2.next_date
        LEFT JOIN returns r4 ON r4.symbol=c.symbol AND r4.date=r3.next_date
        LEFT JOIN returns r5 ON r5.symbol=c.symbol AND r5.date=r4.next_date
        WHERE c.date BETWEEN ? AND ?
          AND c.security_category=1 AND c.is_st=0 AND c.is_suspended=0
        """,
        [date_from, date_to],
    ).fetchall()
    connection.close()

    common: dict[tuple[str, int], CommonValue] = {}
    for row in rows:
        symbol, date = str(row[0]), int(row[1])
        close, previous_cap = finite(row[2]), finite(row[3])
        one_day = [finite(value) for value in row[4:9]]
        styles = [finite(value) for value in row[9:]]
        if (
            close is None
            or close <= 0
            or previous_cap is None
            or any(value is None for value in styles)
        ):
            continue
        targets = (
            compounded(one_day[:1]),
            compounded(one_day[:2]),
            compounded(one_day[:3]),
            compounded(one_day[:5]),
        )
        common[(symbol, date)] = (
            close,
            targets,
            [float(value) for value in styles if value is not None],
            previous_cap,
        )
    return common


def append_scope_metrics(
    output: list[dict[str, object]],
    *,
    factor_name: str,
    window_name: str,
    threshold_version: str,
    date: int,
    scope: str,
    cap_group: str,
    price_group: str,
    rows: Sequence[FactorRow],
    neutral_scores: Sequence[float],
) -> None:
    symbols = [row[0] for row in rows]
    raw_scores = [row[1] for row in rows]
    events = [row[4] for row in rows]
    for target_index, target_name in enumerate(TARGETS):
        eligible = [
            index for index, row in enumerate(rows) if row[2][target_index] is not None
        ]
        if len(eligible) < 20:
            continue
        target_returns = [float(rows[index][2][target_index]) for index in eligible]
        raw = [raw_scores[index] for index in eligible]
        neutral = [neutral_scores[index] for index in eligible]
        eligible_symbols = [symbols[index] for index in eligible]
        eligible_events = [events[index] for index in eligible]
        output.append({
            "factor": factor_name,
            "window_name": window_name,
            "threshold_version": threshold_version,
            "target": target_name,
            "scope": scope,
            "cap_group": cap_group,
            "price_group": price_group,
            "date": date,
            "n": len(eligible),
            "raw_rank_ic": pearson(ranks(raw), ranks(target_returns)),
            "lob5_ex_size_rank_ic": pearson(ranks(neutral), ranks(target_returns)),
            "raw_d10_d1": spread(raw, target_returns, eligible_symbols),
            "lob5_ex_size_d10_d1": spread(
                neutral, target_returns, eligible_symbols
            ),
            "positive_event_n": sum(event == 1 for event in eligible_events),
            "negative_event_n": sum(event == -1 for event in eligible_events),
            "control_n": sum(event == 0 for event in eligible_events),
            "positive_event_minus_control": event_gap(
                target_returns, eligible_events, 1
            ),
            "negative_event_minus_control": event_gap(
                target_returns, eligible_events, -1
            ),
        })


def append_exposure(
    output: list[dict[str, object]],
    *,
    factor_name: str,
    window_name: str,
    threshold_version: str,
    date: int,
    rows: Sequence[FactorRow],
    basis: Sequence[Sequence[float]],
) -> None:
    factors = [row[1] for row in rows]
    residual = residualize(factors, basis)
    center = mean(factors)
    total = sum((value - center) ** 2 for value in factors)
    unexplained = sum(value * value for value in residual)
    result: dict[str, object] = {
        "factor": factor_name,
        "window_name": window_name,
        "threshold_version": threshold_version,
        "date": date,
        "n": len(rows),
        "joint_r2": (
            max(0.0, min(1.0, 1.0 - unexplained / total)) if total > 0 else None
        ),
    }
    factor_ranks = ranks(factors)
    for index, style_name in enumerate(LOB5_EX_SIZE_COLS):
        result[f"{style_name}_rank_exposure"] = pearson(
            factor_ranks, ranks([row[3][index] for row in rows])
        )
    output.append(result)


def process_factor(
    performance: list[dict[str, object]],
    exposures: list[dict[str, object]],
    *,
    factor_name: str,
    window_name: str,
    threshold_version: str,
    date: int,
    rows: Sequence[FactorRow],
    min_cross_section: int,
) -> None:
    domain_rows: dict[tuple[str, str], list[FactorRow]] = defaultdict(list)
    for row in rows:
        observation_domain = domain(row[5], row[6], row[0])  # type: ignore[index]
        if observation_domain is not None:
            domain_rows[observation_domain].append(row[:5])

    pooled_rows: list[FactorRow] = []
    pooled_scores: list[float] = []
    all_rows = sorted(
        (row for observations in domain_rows.values() for row in observations),
        key=lambda row: row[0],
    )
    for (cap_group, price_group), observations in sorted(domain_rows.items()):
        if len(observations) < min_cross_section:
            continue
        observations.sort(key=lambda row: row[0])
        styles = [row[3] for row in observations]
        basis = build_orthonormal_basis(styles)
        residual = residualize([row[1] for row in observations], basis)
        append_scope_metrics(
            performance,
            factor_name=factor_name,
            window_name=window_name,
            threshold_version=threshold_version,
            date=date,
            scope="domain",
            cap_group=cap_group,
            price_group=price_group,
            rows=observations,
            neutral_scores=residual,
        )
        pooled_rows.extend(observations)
        pooled_scores.extend(percentile_ranks(residual))

    if len(pooled_rows) >= min_cross_section:
        append_scope_metrics(
            performance,
            factor_name=factor_name,
            window_name=window_name,
            threshold_version=threshold_version,
            date=date,
            scope="domain_neutral_aggregate",
            cap_group="domain_neutral",
            price_group="aggregate",
            rows=pooled_rows,
            neutral_scores=pooled_scores,
        )
    if len(all_rows) >= min_cross_section:
        all_rows.sort(key=lambda row: row[0])
        basis = build_orthonormal_basis([row[3] for row in all_rows])
        residual = residualize([row[1] for row in all_rows], basis)
        append_scope_metrics(
            performance,
            factor_name=factor_name,
            window_name=window_name,
            threshold_version=threshold_version,
            date=date,
            scope="all_market",
            cap_group="all",
            price_group="all",
            rows=all_rows,
            neutral_scores=residual,
        )
        append_exposure(
            exposures,
            factor_name=factor_name,
            window_name=window_name,
            threshold_version=threshold_version,
            date=date,
            rows=all_rows,
            basis=basis,
        )


def summarize_performance(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    keys = (
        "factor", "window_name", "threshold_version", "target",
        "scope", "cap_group", "price_group",
    )
    metrics = (
        "raw_rank_ic", "lob5_ex_size_rank_ic",
        "raw_d10_d1", "lob5_ex_size_d10_d1",
        "positive_event_minus_control", "negative_event_minus_control",
    )
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
            positive_event_n=sum(int(row["positive_event_n"]) for row in observations),
            negative_event_n=sum(int(row["negative_event_n"]) for row in observations),
            control_n=sum(int(row["control_n"]) for row in observations),
        )
        for metric in metrics:
            values = [
                float(row[metric])
                for row in observations
                if row[metric] not in (None, "")
            ]
            result[metric], result[f"{metric}_t"] = newey_west_mean_t(
                values, TARGET_HAC_LAGS[key[3]]
            )
        output.append(result)
    return output


def summarize_exposure(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    keys = ("factor", "window_name", "threshold_version")
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
        r2 = [float(row["joint_r2"]) for row in observations if row["joint_r2"] is not None]
        result["joint_r2"], result["joint_r2_t"] = mean_t(r2)
        for style_name in LOB5_EX_SIZE_COLS:
            column = f"{style_name}_rank_exposure"
            values = [
                float(row[column]) for row in observations if row[column] is not None
            ]
            result[column], result[f"{column}_t"] = mean_t(values)
            result[f"{style_name}_mean_abs_rank_exposure"] = (
                mean(map(abs, values)) if values else None
            )
        output.append(result)
    return output


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--factors",
        default=str(
            PROJECT_ROOT
            / "data/processed/stylized_fact_4_6/"
            "g2_d04_d06_factors_202601_no_industry_size_v2.csv"
        ),
    )
    parser.add_argument(
        "--returns",
        default=str(
            PROJECT_ROOT / "data/cache/daily_open_to_open_market_calendar_202512_20260206.csv"
        ),
    )
    parser.add_argument(
        "--market-caps",
        default=str(PROJECT_ROOT / "data/cache/daily_market_cap_202512_202601.csv"),
    )
    parser.add_argument(
        "--styles",
        default=str(PROJECT_ROOT / "data/cache/cne5_style_full_202512_202601.csv"),
    )
    parser.add_argument(
        "--controls",
        default=str(
            PROJECT_ROOT / "data/cache/stylized_fact_4_6/d04_d06_controls_202507_202601.csv"
        ),
    )
    parser.add_argument("--date-from", type=int, default=20260105)
    parser.add_argument("--date-to", type=int, default=20260130)
    parser.add_argument("--min-cross-section", type=int, default=20)
    parser.add_argument(
        "--output-dir",
        default=str(
            PROJECT_ROOT
            / "results/daily/stylized_fact_4_6/d04_d06_domains_lob5_ex_size_202601"
        ),
    )
    args = parser.parse_args()

    common = load_common(
        returns_path=args.returns,
        market_caps_path=args.market_caps,
        styles_path=args.styles,
        controls_path=args.controls,
        date_from=args.date_from,
        date_to=args.date_to,
    )
    performance: list[dict[str, object]] = []
    exposures: list[dict[str, object]] = []
    stock_symbols = {symbol for symbol, _date in common}
    excluded_non_stock = 0
    excluded_missing_common = 0
    processed_groups = 0

    current_key: tuple[int, str, str] | None = None
    group: list[dict[str, str]] = []

    def process_group(rows: Sequence[dict[str, str]]) -> None:
        nonlocal excluded_non_stock, excluded_missing_common, processed_groups
        if not rows:
            return
        date = int(rows[0]["date"])
        window_name = rows[0]["window_name"]
        threshold_version = rows[0]["threshold_version"]
        prepared: list[tuple[dict[str, str], CommonValue]] = []
        for row in rows:
            symbol = row["symbol"]
            if symbol not in stock_symbols:
                excluded_non_stock += 1
                continue
            observation = common.get((symbol, date))
            if observation is None:
                excluded_missing_common += 1
                continue
            prepared.append((row, observation))

        for factor_name, column, is_event in FACTOR_SPECS:
            factor_rows = []
            for row, observation in prepared:
                value = finite(row.get(column))
                if value is None:
                    continue
                close, targets, styles, previous_cap = observation
                event = int(value) if is_event else 0
                factor_rows.append(
                    (row["symbol"], value, targets, styles, event, previous_cap, close)
                )
            if len(factor_rows) < args.min_cross_section:
                continue
            process_factor(
                performance,
                exposures,
                factor_name=factor_name,
                window_name=window_name,
                threshold_version=threshold_version,
                date=date,
                rows=factor_rows,  # type: ignore[arg-type]
                min_cross_section=args.min_cross_section,
            )

        processed_groups += 1
        if processed_groups % 10 == 0:
            print(
                f"processed_groups={processed_groups} last_group="
                f"{(date, window_name, threshold_version)}",
                flush=True,
            )

    with Path(args.factors).open(newline="") as handle:
        for row in csv.DictReader(handle):
            if row["frequency"] != "daily":
                continue
            date = int(row["date"])
            if not args.date_from <= date <= args.date_to:
                continue
            key = (date, row["window_name"], row["threshold_version"])
            if current_key is not None and key < current_key:
                raise ValueError("factor input must be sorted by date/window/threshold")
            if current_key is not None and key != current_key:
                process_group(group)
                group = []
            current_key = key
            group.append(row)
    process_group(group)

    performance_summary = summarize_performance(performance)
    exposure_summary = summarize_exposure(exposures)
    output_dir = Path(args.output_dir)
    write_csv(output_dir / "performance_by_date.csv", performance)
    write_csv(output_dir / "performance_summary.csv", performance_summary)
    write_csv(output_dir / "exposure_by_date.csv", exposures)
    write_csv(output_dir / "exposure_summary.csv", exposure_summary)
    write_json(
        output_dir / "metadata.json",
        {
            "factor_file": str(Path(args.factors).resolve()),
            "date_from": args.date_from,
            "date_to": args.date_to,
            "signal_time": "daily close",
            "entry_time": "next trading day open",
            "targets": list(TARGETS),
            "missing_label_policy": "neutralize on signal-time universe; filter labels independently by horizon",
            "t_statistic": "Newey-West; lags 0/1/2/4 for D1/D2/D3/D5",
            "universe": "security_category=1, signal-date is_st=0, is_suspended=0",
            "domain_rule": {
                "market_cap": "previous trading day: <50yi, 50-500yi, >=500yi",
                "price_board": "signal-date close: non-STAR <10, non-STAR >=10, STAR >=10",
                "excluded": "STAR below 10",
            },
            "style_specification": "LOB5-ex-size",
            "style_columns": list(LOB5_EX_SIZE_COLS),
            "factor_standardization": "within-date/domain percentile rank for aggregate",
            "winsorization": "none; D04 input already carries its construction-stage winsorization",
            "factor_specs": [
                {"name": name, "column": column, "event": event}
                for name, column, event in FACTOR_SPECS
            ],
            "excluded_non_stock_rows": excluded_non_stock,
            "excluded_missing_common_rows": excluded_missing_common,
            "output_rows": {
                "performance_by_date": len(performance),
                "performance_summary": len(performance_summary),
                "exposure_by_date": len(exposures),
                "exposure_summary": len(exposure_summary),
            },
        },
    )
    print(
        f"common={len(common)} performance_by_date={len(performance)} "
        f"performance_summary={len(performance_summary)} "
        f"exposure_by_date={len(exposures)} exposure_summary={len(exposure_summary)} "
        f"excluded_non_stock={excluded_non_stock} "
        f"excluded_missing_common={excluded_missing_common} output_dir={output_dir}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
