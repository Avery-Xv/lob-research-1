#!/usr/bin/env python3
"""Backtest B/S large-gap factors neutralized before structural domains."""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Sequence

import duckdb

import backtest_large_gap_by_raw_vr_state as base
from analyze_existing_factors_lob5_ex_size import LOB5_EX_SIZE_COLS
from backtest_daily_domains import domain
from backtest_existing_daily_o2o_cne5 import (
    build_orthonormal_basis,
    finite,
    residualize,
)


CommonValue = tuple[float, tuple[float | None, ...], list[float], float]
Observation = base.Observation
LOB4_NO_SIZE_COLS = ("momentum", "liquidity", "beta", "residual_volatility")
STYLE_SPECS = {
    "lob5_ex_size": tuple(LOB5_EX_SIZE_COLS),
    "lob4_no_size": LOB4_NO_SIZE_COLS,
}


def load_common(
    returns_path: str,
    caps_path: str,
    styles_path: str,
    targets: Sequence[str],
    date_from: int,
    date_to: int,
    style_columns: Sequence[str] = LOB5_EX_SIZE_COLS,
) -> dict[tuple[str, int], CommonValue]:
    if not targets or any(re.fullmatch(r"[a-z][a-z0-9_]*", target) is None for target in targets):
        raise ValueError("target columns must be non-empty snake_case identifiers")
    connection = duckdb.connect()
    connection.read_csv(returns_path).create_view("returns_raw")
    connection.read_csv(caps_path).create_view("caps_raw")
    connection.read_csv(styles_path).create_view("styles_raw")
    style_select = ", ".join(f"{name}::DOUBLE AS {name}" for name in style_columns)
    previous_style_select = ", ".join(
        f"lag({name}) OVER (PARTITION BY symbol ORDER BY date) AS previous_{name}"
        for name in style_columns
    )
    previous_styles = ", ".join(
        f"s.previous_{name}::DOUBLE" for name in style_columns
    )
    target_select = ", ".join(f"r.{target}::DOUBLE" for target in targets)
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
               {target_select}, c.previous_market_cap, {previous_styles}
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
    target_end = 3 + len(targets)
    for row in rows:
        price, previous_cap = finite(row[2]), finite(row[target_end])
        target_values = tuple(finite(value) for value in row[3:target_end])
        styles = [finite(value) for value in row[target_end + 1:]]
        if price is None or price <= 0 or previous_cap is None or any(v is None for v in styles):
            continue
        result[(str(row[0]), int(row[1]))] = (
            float(price), target_values, [float(v) for v in styles if v is not None],
            float(previous_cap),
        )
    return result


def all_market_residuals(rows: Sequence[Observation]) -> dict[str, float]:
    """Residualize once per date across all valid stocks, before any domain split."""
    ordered = sorted(rows, key=lambda row: row[0])
    residuals = residualize(
        [row[2] for row in ordered],
        build_orthonormal_basis([row[4] for row in ordered]),
    )
    return {row[0]: value for row, value in zip(ordered, residuals)}


def process_groups(
    grouped: dict[tuple[str, int], list[Observation]],
    minimum_cross_section: int,
    targets: Sequence[str],
) -> list[dict[str, object]]:
    performance: list[dict[str, object]] = []
    base.TARGETS = tuple(targets)
    for (factor_name, date), rows in sorted(grouped.items()):
        if len(rows) < minimum_cross_section:
            continue
        residual_by_symbol = all_market_residuals(rows)
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
            scores = base.percentile_ranks(
                [residual_by_symbol[row[0]] for row in domain_rows]
            )
            states = base.assign_raw_vr_states(
                [row[1] for row in domain_rows], [row[0] for row in domain_rows]
            )
            for vr_state in base.VR_STATES:
                indices = [index for index, state in enumerate(states) if state == vr_state]
                state_rows = [domain_rows[index] for index in indices]
                state_scores = [scores[index] for index in indices]
                base.append_metrics(
                    performance, factor_name, date, "domain", cap_group, price_group,
                    vr_state, state_rows, state_scores, minimum_cross_section,
                )
                pooled[vr_state].extend(zip(state_rows, state_scores))
        for vr_state in base.VR_STATES:
            state_pairs = pooled.get(vr_state, [])
            if not state_pairs:
                continue
            state_pairs.sort(key=lambda item: item[0][0])
            base.append_metrics(
                performance, factor_name, date, "domain_neutral_aggregate",
                "domain_neutral", "aggregate", vr_state,
                [item[0] for item in state_pairs], [item[1] for item in state_pairs],
                minimum_cross_section,
            )
    return performance


def rename_neutral_metrics(
    rows: Sequence[dict[str, object]], style_specification: str
) -> None:
    """Give non-default style outputs an unambiguous metric prefix."""
    if style_specification == "lob5_ex_size":
        return
    old_prefix = "lob5_ex_size_"
    new_prefix = f"{style_specification}_"
    for row in rows:
        for key in list(row):
            if key.startswith(old_prefix):
                row[new_prefix + key[len(old_prefix):]] = row.pop(key)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--factors", required=True)
    parser.add_argument("--intraday-returns", required=True)
    parser.add_argument("--market-caps", required=True)
    parser.add_argument("--styles", required=True)
    parser.add_argument("--target-cols", nargs="+", required=True)
    parser.add_argument(
        "--style-specification",
        choices=tuple(STYLE_SPECS),
        default="lob5_ex_size",
    )
    parser.add_argument("--date-from", type=int, default=20260201)
    parser.add_argument("--date-to", type=int, default=20260430)
    parser.add_argument("--minimum-cross-section", type=int, default=20)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()
    style_columns = STYLE_SPECS[args.style_specification]

    common = load_common(
        args.intraday_returns, args.market_caps, args.styles, args.target_cols,
        args.date_from, args.date_to, style_columns,
    )
    groups = base.load_groups(args.factors, common, args.date_from, args.date_to)
    performance = process_groups(groups, args.minimum_cross_section, args.target_cols)
    summary = base.summarize_performance(performance)
    contrast_by_date, contrast_summary = base.build_contrasts(performance)
    for rows in (performance, summary, contrast_by_date, contrast_summary):
        rename_neutral_metrics(rows, args.style_specification)
    output_dir = Path(args.output_dir)
    base.write_csv(output_dir / "performance_by_date.csv", performance)
    base.write_csv(output_dir / "performance_summary.csv", summary)
    base.write_csv(output_dir / "state_contrast_by_date.csv", contrast_by_date)
    base.write_csv(output_dir / "state_contrast_summary.csv", contrast_summary)
    metadata = {
        "date_from": args.date_from,
        "date_to": args.date_to,
        "target_columns": args.target_cols,
        "signal": "large-gap B/S and raw vr_log from 10:00-10:30",
        "neutralization_order": (
            "per date: residualize raw B/S across the full valid A-share cross-section on "
            f"{args.style_specification}; then split structural domains; then split raw vr_log terciles"
        ),
        "neutralization_scope": "all-market before domains",
        "style_specification": args.style_specification,
        "excluded_size_styles": (
            ["size", "non_linear_size"]
            if args.style_specification == "lob4_no_size" else ["size"]
        ),
        "style_columns": list(style_columns),
        "vr_state_rule": (
            "raw vr_log exact-count terciles within each date and structural domain; "
            "vr_log is not neutralized"
        ),
        "exposure_timing": "previous trading-day CNE5 exposures and market cap",
        "universe_rule": "point-in-time Shanghai/Shenzhen A shares; ETF count zero",
        "validity": "large-gap match_rate>=0.95 and ob_is_valid=true; ST/suspended excluded",
        "missing_label_policy": "neutralization and states precede target-specific filtering",
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
