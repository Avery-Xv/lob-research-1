#!/usr/bin/env python3
"""Domain-neutralized open-to-open backtest for the daily order-behavior VR.

The end-of-day factor on T predicts the open-to-open return from T+1 to T+2.
The signal universe is established before the future label is selected.  The
primary specification winsorizes within each date/domain, residualizes on
LOB5-ex-size, and ranks the residual within the same date/domain.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
from collections import defaultdict
from pathlib import Path
from statistics import mean, stdev

import duckdb

from backtest_daily_domains import CAP_GROUPS, PRICE_GROUPS, domain, pearson, ranks
from backtest_existing_daily_o2o_cne5 import (
    build_orthonormal_basis,
    finite,
    residualize,
    spread,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOB5_EX_SIZE_COLS = (
    "non_linear_size",
    "momentum",
    "liquidity",
    "beta",
    "residual_volatility",
)
EXPOSURE_DIAGNOSTIC_COLS = ("size",) + LOB5_EX_SIZE_COLS


def mean_t(values: list[float]) -> tuple[float | None, float | None]:
    if not values:
        return None, None
    average = mean(values)
    if len(values) < 2:
        return average, None
    volatility = stdev(values)
    return average, average / (volatility / math.sqrt(len(values))) if volatility else None


def quantile(values: list[float], probability: float) -> float:
    ordered = sorted(values)
    position = probability * (len(ordered) - 1)
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def winsorize(values: list[float], lower: float, upper: float) -> list[float]:
    if not values:
        return []
    lower_bound = quantile(values, lower)
    upper_bound = quantile(values, upper)
    return [min(upper_bound, max(lower_bound, value)) for value in values]


def percentile_scores(values: list[float]) -> list[float]:
    value_ranks = ranks(values)
    denominator = len(values) + 1.0
    return [rank / denominator - 0.5 for rank in value_ranks]


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


def load_rows(
    *,
    factor_path: str,
    returns_path: str,
    market_caps_path: str,
    styles_path: str,
    factor_col: str,
    date_from: int,
    date_to: int,
    excluded_symbols: set[str],
) -> tuple[list[dict[str, object]], dict[str, int]]:
    connection = duckdb.connect()
    connection.read_csv(factor_path, all_varchar=True).create_view("factor_raw")
    connection.read_csv(returns_path).create_view("returns_raw")
    connection.read_csv(market_caps_path).create_view("caps_raw")
    connection.read_csv(styles_path).create_view("styles_raw")
    style_select = ", ".join(
        f"s.{column}::DOUBLE AS {column}" for column in EXPOSURE_DIAGNOSTIC_COLS
    )
    rows = connection.execute(
        f"""
        WITH factors AS (
            SELECT symbol, date::INTEGER AS date, {factor_col}::DOUBLE AS factor
            FROM factor_raw
            WHERE lower(is_valid) = 'true'
        ),
        returns AS (
            SELECT DISTINCT symbol, date::INTEGER AS date,
                   next_date::INTEGER AS next_date, open::DOUBLE AS open,
                   o2o_ret::DOUBLE AS o2o_ret
            FROM returns_raw
            WHERE next_date::INTEGER > date::INTEGER
        ),
        caps AS (
            SELECT DISTINCT symbol, date::INTEGER AS date, total_mv::DOUBLE AS total_mv
            FROM caps_raw
        ),
        previous_caps AS (
            SELECT symbol, date,
                   lag(total_mv) OVER (PARTITION BY symbol ORDER BY date) AS previous_market_cap
            FROM caps
        ),
        styles AS (
            SELECT symbol, replace(date::VARCHAR, '-', '')::INTEGER AS date,
                   {', '.join(column + '::DOUBLE AS ' + column for column in EXPOSURE_DIAGNOSTIC_COLS)}
            FROM styles_raw
        )
        SELECT f.symbol, f.date, f.factor, r0.open, c.previous_market_cap,
               r0.next_date AS entry_date, r1.next_date AS exit_date,
               r1.o2o_ret AS target_return, {style_select}
        FROM factors f
        JOIN returns r0 ON r0.symbol = f.symbol AND r0.date = f.date
        JOIN previous_caps c ON c.symbol = f.symbol AND c.date = f.date
        JOIN styles s ON s.symbol = f.symbol AND s.date = f.date
        LEFT JOIN returns r1 ON r1.symbol = r0.symbol AND r1.date = r0.next_date
        WHERE f.date BETWEEN ? AND ?
        ORDER BY f.date, f.symbol
        """,
        [date_from, date_to],
    ).fetchall()
    connection.close()

    output: list[dict[str, object]] = []
    counts = {
        "joined_before_security_filter": len(rows),
        "excluded_non_a_share": 0,
        "excluded_invalid_numeric": 0,
        "excluded_star_below_10": 0,
    }
    for raw in rows:
        symbol = str(raw[0])
        if symbol in excluded_symbols:
            counts["excluded_non_a_share"] += 1
            continue
        factor = finite(raw[2])
        signal_open = finite(raw[3])
        previous_cap = finite(raw[4])
        diagnostics = [finite(value) for value in raw[8:]]
        if (
            factor is None
            or signal_open is None
            or signal_open <= 0
            or previous_cap is None
            or any(value is None for value in diagnostics)
        ):
            counts["excluded_invalid_numeric"] += 1
            continue
        groups = domain(float(previous_cap), float(signal_open), symbol)
        if groups is None:
            counts["excluded_star_below_10"] += 1
            continue
        output.append(
            {
                "symbol": symbol,
                "date": int(raw[1]),
                "factor": float(factor),
                "signal_open": float(signal_open),
                "previous_cap": float(previous_cap),
                "entry_date": int(raw[5]) if raw[5] is not None else None,
                "exit_date": int(raw[6]) if raw[6] is not None else None,
                "target": finite(raw[7]),
                "diagnostics": [float(value) for value in diagnostics if value is not None],
                "styles": [float(value) for value in diagnostics[1:] if value is not None],
                "cap_group": groups[0],
                "price_group": groups[1],
            }
        )
    counts["signal_rows"] = len(output)
    counts["signal_symbols"] = len({str(row["symbol"]) for row in output})
    counts["label_rows"] = sum(row["target"] is not None for row in output)
    return output, counts


def performance_row(
    *,
    values: list[float],
    rows: list[dict[str, object]],
    variant: str,
    cap_group: str,
    price_group: str,
) -> dict[str, object] | None:
    eligible = [index for index, row in enumerate(rows) if row["target"] is not None]
    if len(eligible) < 20:
        return None
    factors = [values[index] for index in eligible]
    targets = [float(rows[index]["target"]) for index in eligible]
    symbols = [str(rows[index]["symbol"]) for index in eligible]
    order = sorted(range(len(eligible)), key=lambda index: (factors[index], symbols[index]))
    ordered_targets = [targets[index] for index in order]
    bucket = max(1, len(order) // 10)
    d1_return = mean(ordered_targets[:bucket])
    d10_return = mean(ordered_targets[-bucket:])
    entry_dates = {rows[index]["entry_date"] for index in eligible}
    exit_dates = {rows[index]["exit_date"] for index in eligible}
    return {
        "variant": variant,
        "cap_group": cap_group,
        "price_group": price_group,
        "date": rows[0]["date"],
        "entry_date": next(iter(entry_dates)) if len(entry_dates) == 1 else "mixed",
        "exit_date": next(iter(exit_dates)) if len(exit_dates) == 1 else "mixed",
        "signal_n": len(rows),
        "n": len(eligible),
        "rank_ic": pearson(ranks(factors), ranks(targets)),
        "d10_d1": d10_return - d1_return,
        "d1_ret": d1_return,
        "d10_ret": d10_return,
    }


def build_outputs(
    rows: list[dict[str, object]],
    winsor_lower: float,
    winsor_upper: float,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    by_date: dict[int, list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        by_date[int(row["date"])].append(row)

    performance: list[dict[str, object]] = []
    exposures: list[dict[str, object]] = []
    for date, date_rows in sorted(by_date.items()):
        date_rows.sort(key=lambda row: str(row["symbol"]))
        raw = [float(row["factor"]) for row in date_rows]
        raw_winsor = winsorize(raw, winsor_lower, winsor_upper)
        diagnostic_matrix = [list(row["diagnostics"]) for row in date_rows]
        lob5_matrix = [list(row["styles"]) for row in date_rows]
        basis = build_orthonormal_basis(lob5_matrix)
        all_market_residual = residualize(raw_winsor, basis)
        centered = [value - mean(raw_winsor) for value in raw_winsor]
        total_variance = sum(value * value for value in centered)
        residual_variance = sum(value * value for value in all_market_residual)
        exposure_row: dict[str, object] = {
            "date": date,
            "n": len(date_rows),
            "lob5_joint_r2": (
                max(0.0, min(1.0, 1.0 - residual_variance / total_variance))
                if total_variance > 0 else None
            ),
        }
        factor_ranks = ranks(raw_winsor)
        for column_index, style_name in enumerate(EXPOSURE_DIAGNOSTIC_COLS):
            exposure_row[f"{style_name}_rank_exposure"] = pearson(
                factor_ranks,
                ranks([style[column_index] for style in diagnostic_matrix]),
            )
        exposures.append(exposure_row)

        for variant, values in (
            ("raw_all_market", raw_winsor),
            ("lob5_ex_size_all_market", all_market_residual),
        ):
            result = performance_row(
                values=values,
                rows=date_rows,
                variant=variant,
                cap_group="all",
                price_group="all",
            )
            if result is not None:
                performance.append(result)

        domain_ranked: list[tuple[dict[str, object], float]] = []
        for cap_group in CAP_GROUPS:
            for price_group in PRICE_GROUPS:
                group_rows = [
                    row for row in date_rows
                    if row["cap_group"] == cap_group and row["price_group"] == price_group
                ]
                if len(group_rows) < 20:
                    continue
                group_raw = [float(row["factor"]) for row in group_rows]
                group_winsor = winsorize(group_raw, winsor_lower, winsor_upper)
                group_basis = build_orthonormal_basis(
                    [list(row["styles"]) for row in group_rows]
                )
                group_residual = residualize(group_winsor, group_basis)
                for variant, values in (
                    ("raw_domain", group_winsor),
                    ("lob5_ex_size_domain", group_residual),
                ):
                    result = performance_row(
                        values=values,
                        rows=group_rows,
                        variant=variant,
                        cap_group=cap_group,
                        price_group=price_group,
                    )
                    if result is not None:
                        performance.append(result)
                for row, score in zip(group_rows, percentile_scores(group_residual)):
                    domain_ranked.append((row, score))

        aggregate_rows = [item[0] for item in domain_ranked]
        aggregate_scores = [item[1] for item in domain_ranked]
        if aggregate_rows:
            aggregate = performance_row(
                values=aggregate_scores,
                rows=aggregate_rows,
                variant="domain_neutral_aggregate",
                cap_group="all",
                price_group="domain_ranked",
            )
            if aggregate is not None:
                performance.append(aggregate)
    return performance, exposures


def summarize_performance(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    grouped: dict[tuple[str, str, str], list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        grouped[(str(row["variant"]), str(row["cap_group"]), str(row["price_group"]))].append(row)
    output: list[dict[str, object]] = []
    for key, observations in sorted(grouped.items()):
        result: dict[str, object] = {
            "variant": key[0],
            "cap_group": key[1],
            "price_group": key[2],
            "n_days": len(observations),
            "n_obs": sum(int(row["n"]) for row in observations),
            "avg_names": mean(int(row["n"]) for row in observations),
        }
        for metric in ("rank_ic", "d10_d1", "d1_ret", "d10_ret"):
            values = [float(row[metric]) for row in observations if row[metric] is not None]
            result[metric], result[f"{metric}_t"] = mean_t(values)
            if metric in {"d10_d1", "d1_ret", "d10_ret"}:
                result[f"{metric}_bps"] = (
                    float(result[metric]) * 10_000 if result[metric] is not None else None
                )
        spreads = [float(row["d10_d1"]) for row in observations]
        result["d10_d1_positive_share"] = mean(value > 0 for value in spreads)
        output.append(result)
    return output


def summarize_exposures(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    result: dict[str, object] = {
        "n_days": len(rows),
        "n_obs": sum(int(row["n"]) for row in rows),
    }
    columns = ["lob5_joint_r2"] + [
        f"{style}_rank_exposure" for style in EXPOSURE_DIAGNOSTIC_COLS
    ]
    for column in columns:
        values = [float(row[column]) for row in rows if row[column] is not None]
        result[column], result[f"{column}_t"] = mean_t(values)
        result[f"{column}_mean_abs"] = mean(map(abs, values)) if values else None
    return [result]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--factor",
        default=str(PROJECT_ROOT / "data/processed/order_behavior_ratio_daily_202601.csv"),
    )
    parser.add_argument("--factor-col", default="vr_log")
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
    parser.add_argument("--date-from", type=int, default=20260105)
    parser.add_argument("--date-to", type=int, default=20260130)
    parser.add_argument("--winsor-lower", type=float, default=0.01)
    parser.add_argument("--winsor-upper", type=float, default=0.99)
    parser.add_argument(
        "--exclude-symbols",
        nargs="*",
        default=["SH689009"],
        help="Symbols excluded by the external A-share security-master audit.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(
            PROJECT_ROOT / "results/daily/order_behavior_vr_log_o2o_lob5_ex_size_202601"
        ),
    )
    args = parser.parse_args()
    if not 0 <= args.winsor_lower < args.winsor_upper <= 1:
        raise ValueError("winsor bounds must satisfy 0 <= lower < upper <= 1")

    rows, counts = load_rows(
        factor_path=args.factor,
        returns_path=args.returns,
        market_caps_path=args.market_caps,
        styles_path=args.styles,
        factor_col=args.factor_col,
        date_from=args.date_from,
        date_to=args.date_to,
        excluded_symbols=set(args.exclude_symbols),
    )
    performance_daily, exposure_daily = build_outputs(
        rows,
        args.winsor_lower,
        args.winsor_upper,
    )
    performance_summary = summarize_performance(performance_daily)
    exposure_summary = summarize_exposures(exposure_daily)

    output_dir = Path(args.output_dir)
    write_csv(output_dir / "performance_by_date.csv", performance_daily)
    write_csv(output_dir / "performance_summary.csv", performance_summary)
    write_csv(output_dir / "exposure_by_date.csv", exposure_daily)
    write_csv(output_dir / "exposure_summary.csv", exposure_summary)
    metadata = {
        "factor": args.factor,
        "factor_col": args.factor_col,
        "factor_timestamp": "end_of_day_after_145700000",
        "target": "T+1_open_to_T+2_open",
        "date_from": args.date_from,
        "date_to": args.date_to,
        "domain_rule": {
            "market_cap": "previous trading day: <50yi, 50-500yi, >=500yi",
            "price_board": "signal-date open: non-STAR <10, non-STAR >=10, STAR >=10",
            "star_below_10": "excluded",
        },
        "neutralization": {
            "name": "LOB5-ex-size",
            "style_timing": "signal-date close-known daily exposure",
            "style_columns": list(LOB5_EX_SIZE_COLS),
            "intercept": True,
            "linear_size_in_regression": False,
            "winsorization": [args.winsor_lower, args.winsor_upper],
            "domain_aggregate": "intra-domain residual percentile scores, observation-weighted",
        },
        "missing_label_policy": "neutralize on signal universe, then drop missing target independently",
        "universe_audit": {
            "source": "ods.ods_jydb_secu_main, SecuCategory=1, SecuMarket in (83,90)",
            "audit_result": "5185 A-share symbols, zero ETFs; excluded SH689009 CDR",
            "excluded_symbols": args.exclude_symbols,
        },
        "tradability_note": "open-to-open cache has no explicit ST/suspension status columns",
        "counts": counts,
        "outputs": {
            "performance_by_date": str(output_dir / "performance_by_date.csv"),
            "performance_summary": str(output_dir / "performance_summary.csv"),
            "exposure_by_date": str(output_dir / "exposure_by_date.csv"),
            "exposure_summary": str(output_dir / "exposure_summary.csv"),
        },
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    metadata_path = output_dir / "metadata.json"
    temporary = metadata_path.with_name(f".{metadata_path.name}.tmp")
    temporary.write_text(json.dumps(metadata, ensure_ascii=False, indent=2) + "\n")
    temporary.replace(metadata_path)
    print(
        f"signal_rows={len(rows)} performance_daily={len(performance_daily)} "
        f"performance_summary={len(performance_summary)} output_dir={output_dir}"
    )
    print(f"counts={counts}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
