#!/usr/bin/env python3
"""Leakage-safe domain backtest for daily D07 retained-impact variants."""

from __future__ import annotations

import argparse
import importlib.util
from pathlib import Path
from typing import Iterable, Sequence

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parents[2]
COMMON_PATH = Path(__file__).with_name("backtest_stylized_d04_d06_domains.py")
COMMON_SPEC = importlib.util.spec_from_file_location("d04_d06_domains_common", COMMON_PATH)
if COMMON_SPEC is None or COMMON_SPEC.loader is None:
    raise ImportError(f"cannot load shared backtest helpers from {COMMON_PATH}")
COMMON = importlib.util.module_from_spec(COMMON_SPEC)
COMMON_SPEC.loader.exec_module(COMMON)

FACTOR_COLUMNS = (
    "d07_directional",
    "d07_permanent_ratio",
    "d07_buy_permanent_ratio",
    "d07_sell_permanent_ratio",
)
PRIMARY_CONFIGURATION = {
    "factor": "d07_directional",
    "window_name": "daily_0930_close",
    "threshold_version": "mean_x05",
    "horizons": "all pre-specified event/time horizons as a decay family",
}


def encode_factor(factor: str, clock_type: str, horizon: int, horizon_unit: str) -> str:
    """Keep clock dimensions unique while reusing the established summarizer."""
    return "|".join((factor, clock_type, str(horizon), horizon_unit))


def decode_factor(value: str) -> tuple[str, str, int, str]:
    factor, clock_type, horizon, horizon_unit = value.split("|", 3)
    return factor, clock_type, int(horizon), horizon_unit


def add_factor_dimensions(rows: Iterable[dict[str, object]]) -> None:
    for row in rows:
        factor, clock_type, horizon, horizon_unit = decode_factor(str(row["factor"]))
        row["factor"] = factor
        row["clock_type"] = clock_type
        row["horizon"] = horizon
        row["horizon_unit"] = horizon_unit


def selected_factor_columns(value: str) -> tuple[str, ...]:
    columns = tuple(item.strip() for item in value.split(",") if item.strip())
    unknown = sorted(set(columns) - set(FACTOR_COLUMNS))
    if unknown:
        raise ValueError(f"unknown factor columns: {unknown}")
    if not columns:
        raise ValueError("select at least one factor column")
    return columns


def finite(value: object) -> float | None:
    return COMMON.finite(value)


def build_query(args: argparse.Namespace, factor_columns: Sequence[str]) -> tuple[str, list[object]]:
    conditions = ["frequency='daily'", "date::INTEGER BETWEEN ? AND ?"]
    parameters: list[object] = [args.factors, args.date_from, args.date_to]
    optional_filters = (
        ("window_name", args.window_name),
        ("threshold_version", args.threshold_version),
        ("clock_type", args.clock_type),
        ("horizon::BIGINT", args.horizon),
    )
    for column, value in optional_filters:
        if value is not None:
            conditions.append(f"{column}=?")
            parameters.append(value)
    factor_select = ",".join(f"{column}::DOUBLE" for column in factor_columns)
    query = f"""
        SELECT symbol::VARCHAR,date::INTEGER,window_name::VARCHAR,
               threshold_version::VARCHAR,clock_type::VARCHAR,horizon::BIGINT,
               horizon_unit::VARCHAR,is_valid::BOOLEAN,{factor_select}
        FROM read_csv_auto(?,header=true)
        WHERE {' AND '.join(conditions)}
        ORDER BY date,window_name,threshold_version,clock_type,horizon,symbol
    """
    return query, parameters


def process_factor_global_then_domain(
    performance: list[dict[str, object]],
    exposures: list[dict[str, object]],
    *,
    factor_name: str,
    window_name: str,
    threshold_version: str,
    date: int,
    rows: Sequence[tuple],
    min_cross_section: int,
) -> None:
    # Neutralize once on the domain-eligible universe, then split domains.
    domain_rows: dict[tuple[str, str], list[tuple]] = {}
    for row in rows:
        observation_domain = COMMON.domain(row[5], row[6], row[0])
        if observation_domain is not None:
            domain_rows.setdefault(observation_domain, []).append(row[:5])

    all_rows = sorted(
        (row for observations in domain_rows.values() for row in observations),
        key=lambda row: row[0],
    )
    if len(all_rows) < min_cross_section:
        return
    global_basis = COMMON.build_orthonormal_basis([row[3] for row in all_rows])
    global_residual = COMMON.residualize(
        [row[1] for row in all_rows], global_basis
    )
    residual_by_symbol = {
        row[0]: residual for row, residual in zip(all_rows, global_residual)
    }

    pooled_rows: list[tuple] = []
    pooled_scores: list[float] = []
    for (cap_group, price_group), observations in sorted(domain_rows.items()):
        if len(observations) < min_cross_section:
            continue
        observations.sort(key=lambda row: row[0])
        neutral_scores = [residual_by_symbol[row[0]] for row in observations]
        COMMON.append_scope_metrics(
            performance,
            factor_name=factor_name,
            window_name=window_name,
            threshold_version=threshold_version,
            date=date,
            scope="domain",
            cap_group=cap_group,
            price_group=price_group,
            rows=observations,
            neutral_scores=neutral_scores,
        )
        pooled_rows.extend(observations)
        pooled_scores.extend(COMMON.percentile_ranks(neutral_scores))

    if len(pooled_rows) >= min_cross_section:
        COMMON.append_scope_metrics(
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
    COMMON.append_scope_metrics(
        performance,
        factor_name=factor_name,
        window_name=window_name,
        threshold_version=threshold_version,
        date=date,
        scope="all_market",
        cap_group="all",
        price_group="all",
        rows=all_rows,
        neutral_scores=global_residual,
    )
    COMMON.append_exposure(
        exposures,
        factor_name=factor_name,
        window_name=window_name,
        threshold_version=threshold_version,
        date=date,
        rows=all_rows,
        basis=global_basis,
    )

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Backtest daily D07 with point-in-time domains and LOB5-ex-size"
    )
    parser.add_argument(
        "--factors",
        default=str(
            PROJECT_ROOT / "data/processed/stylized_fact_4_6/"
            "d07_retained_impact_event_time_202601_v1.csv"
        ),
    )
    parser.add_argument(
        "--returns",
        default=str(PROJECT_ROOT / "data/cache/daily_open_to_open_202601_20260213.csv"),
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
            PROJECT_ROOT / "data/cache/stylized_fact_4_6/"
            "d04_d06_controls_202507_202601.csv"
        ),
    )
    parser.add_argument("--date-from", type=int, default=20260105)
    parser.add_argument("--date-to", type=int, default=20260130)
    parser.add_argument("--min-cross-section", type=int, default=20)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--memory-limit", default="16GB")
    parser.add_argument("--temp-root", default="/tmp/stylized_fact_4_6_d07_backtest")
    parser.add_argument("--window-name")
    parser.add_argument("--threshold-version")
    parser.add_argument("--clock-type", choices=("event", "time"))
    parser.add_argument("--horizon", type=int)
    parser.add_argument("--factor-columns", default=",".join(FACTOR_COLUMNS))
    parser.add_argument(
        "--neutralization-order",
        choices=("within_domain", "global_then_domain"),
        default="within_domain",
    )
    parser.add_argument(
        "--output-dir",
        default=str(
            PROJECT_ROOT / "results/daily/stylized_fact_4_6/"
            "d07_domains_lob5_ex_size_202601"
        ),
    )
    args = parser.parse_args()
    if args.date_from > args.date_to:
        parser.error("date-from must not exceed date-to")
    if args.threads <= 0 or args.min_cross_section <= 0:
        parser.error("threads and min-cross-section must be positive")
    try:
        factor_columns = selected_factor_columns(args.factor_columns)
    except ValueError as error:
        parser.error(str(error))

    common = COMMON.load_common(
        returns_path=args.returns,
        market_caps_path=args.market_caps,
        styles_path=args.styles,
        controls_path=args.controls,
        date_from=args.date_from,
        date_to=args.date_to,
    )
    stock_symbols = {symbol for symbol, _date in common}
    performance: list[dict[str, object]] = []
    exposures: list[dict[str, object]] = []
    excluded_non_stock = 0
    excluded_missing_common = 0
    excluded_invalid_factor = 0
    processed_groups = 0

    query, parameters = build_query(args, factor_columns)
    temp_root = Path(args.temp_root)
    temp_root.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect()
    connection.execute(f"PRAGMA threads={args.threads}")
    connection.execute(f"PRAGMA memory_limit='{args.memory_limit}'")
    connection.execute("PRAGMA preserve_insertion_order=false")
    escaped_temp = str(temp_root).replace("'", "''")
    connection.execute(f"PRAGMA temp_directory='{escaped_temp}'")
    cursor = connection.execute(query, parameters)

    current_key: tuple[int, str, str, str, int, str] | None = None
    group: list[tuple[object, ...]] = []

    def process_group(rows: Sequence[tuple[object, ...]]) -> None:
        nonlocal excluded_non_stock, excluded_missing_common
        nonlocal excluded_invalid_factor, processed_groups
        if not rows:
            return
        date = int(rows[0][1])
        window_name = str(rows[0][2])
        threshold_version = str(rows[0][3])
        clock_type = str(rows[0][4])
        horizon = int(rows[0][5])
        horizon_unit = str(rows[0][6])
        prepared: list[tuple[tuple[object, ...], object]] = []
        for row in rows:
            symbol = str(row[0])
            if symbol not in stock_symbols:
                excluded_non_stock += 1
                continue
            observation = common.get((symbol, date))
            if observation is None:
                excluded_missing_common += 1
                continue
            if not bool(row[7]):
                excluded_invalid_factor += 1
                continue
            prepared.append((row, observation))

        for factor_offset, factor_column in enumerate(factor_columns, start=8):
            factor_rows = []
            for row, observation in prepared:
                value = finite(row[factor_offset])
                if value is None:
                    continue
                close, targets, styles, previous_cap = observation
                factor_rows.append(
                    (str(row[0]), value, targets, styles, 0, previous_cap, close)
                )
            if len(factor_rows) < args.min_cross_section:
                continue
            process_function = (
                process_factor_global_then_domain
                if args.neutralization_order == "global_then_domain"
                else COMMON.process_factor
            )
            process_function(
                performance,
                exposures,
                factor_name=encode_factor(
                    factor_column, clock_type, horizon, horizon_unit
                ),
                window_name=window_name,
                threshold_version=threshold_version,
                date=date,
                rows=factor_rows,
                min_cross_section=args.min_cross_section,
            )

        processed_groups += 1
        if processed_groups % 20 == 0:
            print(f"processed_groups={processed_groups} last_group={current_key}", flush=True)

    while True:
        rows = cursor.fetchmany(50_000)
        if not rows:
            break
        for row in rows:
            key = (
                int(row[1]), str(row[2]), str(row[3]), str(row[4]),
                int(row[5]), str(row[6]),
            )
            if current_key is not None and key != current_key:
                process_group(group)
                group = []
            current_key = key
            group.append(row)
    process_group(group)
    connection.close()

    if not performance:
        raise RuntimeError("no eligible D07 performance rows were produced")
    performance_summary = COMMON.summarize_performance(performance)
    exposure_summary = COMMON.summarize_exposure(exposures)
    for rows in (performance, performance_summary, exposures, exposure_summary):
        add_factor_dimensions(rows)

    output_dir = Path(args.output_dir)
    COMMON.write_csv(output_dir / "performance_by_date.csv", performance)
    COMMON.write_csv(output_dir / "performance_summary.csv", performance_summary)
    COMMON.write_csv(output_dir / "exposure_by_date.csv", exposures)
    COMMON.write_csv(output_dir / "exposure_summary.csv", exposure_summary)
    COMMON.write_json(
        output_dir / "metadata.json",
        {
            "factor_file": str(Path(args.factors).resolve()),
            "date_from": args.date_from,
            "date_to": args.date_to,
            "signal_time": "daily continuous-auction close (14:57 cutoff)",
            "entry_time": "next trading day open",
            "targets": list(COMMON.TARGETS),
            "missing_label_policy": (
                "neutralize on signal-time universe; filter labels independently by horizon"
            ),
            "universe": (
                "point-in-time A-share factor manifest; security_category=1, "
                "signal-date is_st=0, is_suspended=0; ETF count=0"
            ),
            "primary_configuration": PRIMARY_CONFIGURATION,
            "factor_columns": list(factor_columns),
            "variant_filters": {
                "window_name": args.window_name,
                "threshold_version": args.threshold_version,
                "clock_type": args.clock_type,
                "horizon": args.horizon,
            },
            "domain_rule": {
                "market_cap": "previous trading day: <50yi, 50-500yi, >=500yi",
                "price_board": (
                    "signal-date close: non-STAR <10, non-STAR >=10, STAR >=10"
                ),
                "excluded": "STAR below 10",
            },
            "style_specification": "LOB5-ex-size",
            "style_columns": list(COMMON.LOB5_EX_SIZE_COLS),
            "neutralization_order": args.neutralization_order,
            "linear_size_control": (
                "excluded; global-first mode intentionally retains linear size exposure"
                if args.neutralization_order == "global_then_domain"
                else "excluded because prior-day market-cap domains are the primary size control"
            ),
            "factor_standardization": (
                "global residual first; within-date/domain percentile rank only for aggregate"
                if args.neutralization_order == "global_then_domain"
                else "within-date/domain residual then percentile rank for aggregate"
            ),
            "winsorization": (
                "none at daily stage; episode retained ratios were clipped to [-5,5]"
            ),
            "t_statistic": "Newey-West; lags 0/1/2/4 for D1/D2/D3/D5",
            "processed_groups": processed_groups,
            "excluded_non_stock_rows": excluded_non_stock,
            "excluded_missing_common_rows": excluded_missing_common,
            "excluded_invalid_factor_rows": excluded_invalid_factor,
            "output_rows": {
                "performance_by_date": len(performance),
                "performance_summary": len(performance_summary),
                "exposure_by_date": len(exposures),
                "exposure_summary": len(exposure_summary),
            },
        },
    )
    print(
        f"common={len(common)} processed_groups={processed_groups} "
        f"performance_by_date={len(performance)} "
        f"performance_summary={len(performance_summary)} "
        f"exposure_by_date={len(exposures)} "
        f"exposure_summary={len(exposure_summary)} "
        f"excluded_non_stock={excluded_non_stock} "
        f"excluded_missing_common={excluded_missing_common} "
        f"excluded_invalid_factor={excluded_invalid_factor} output_dir={output_dir}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
