#!/usr/bin/env python3
"""Daily open-to-open intersections of VR and buy-side large-gap ratio.

The signal is known after the close on T and predicts the open-to-open return
from T+1 to T+2.  The primary result uses within-domain LOB5-ex-size residual
percentile ranks.  Raw within-domain ranks are retained as diagnostics.
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
)
from backtest_order_behavior_daily_o2o_domains import winsorize


PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOB5_EX_SIZE_COLS = (
    "non_linear_size",
    "momentum",
    "liquidity",
    "beta",
    "residual_volatility",
)
STRATEGIES = (
    "b_baseline_30",
    "strict_both_30",
    "vr_median_filter",
    "short_vr_confirmed",
)


def mean_t(values: list[float]) -> tuple[float | None, float | None]:
    if not values:
        return None, None
    average = mean(values)
    if len(values) < 2:
        return average, None
    volatility = stdev(values)
    return average, average / (volatility / math.sqrt(len(values))) if volatility else None


def percentile_ranks(values: list[float]) -> list[float]:
    denominator = len(values) + 1.0
    return [value / denominator for value in ranks(values)]


def selection_indices(
    b_percentiles: list[float],
    vr_percentiles: list[float],
    strategy: str,
) -> tuple[list[int], list[int]]:
    if len(b_percentiles) != len(vr_percentiles):
        raise ValueError("B and VR percentile vectors must have equal length")
    if strategy == "b_baseline_30":
        long = [index for index, value in enumerate(b_percentiles) if value <= 0.30]
        short = [index for index, value in enumerate(b_percentiles) if value >= 0.70]
    elif strategy == "strict_both_30":
        long = [
            index for index, (b_value, vr_value) in enumerate(zip(b_percentiles, vr_percentiles))
            if b_value <= 0.30 and vr_value <= 0.30
        ]
        short = [
            index for index, (b_value, vr_value) in enumerate(zip(b_percentiles, vr_percentiles))
            if b_value >= 0.70 and vr_value >= 0.70
        ]
    elif strategy == "vr_median_filter":
        long = [
            index for index, (b_value, vr_value) in enumerate(zip(b_percentiles, vr_percentiles))
            if b_value <= 0.30 and vr_value <= 0.50
        ]
        short = [
            index for index, (b_value, vr_value) in enumerate(zip(b_percentiles, vr_percentiles))
            if b_value >= 0.70 and vr_value >= 0.50
        ]
    elif strategy == "short_vr_confirmed":
        long = [index for index, value in enumerate(b_percentiles) if value <= 0.30]
        short = [
            index for index, (b_value, vr_value) in enumerate(zip(b_percentiles, vr_percentiles))
            if b_value >= 0.70 and vr_value >= 0.50
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


def load_rows(args: argparse.Namespace) -> tuple[list[dict[str, object]], dict[str, int]]:
    connection = duckdb.connect()
    connection.read_csv(args.vr, all_varchar=True).create_view("vr_raw")
    connection.read_csv(args.buy_gap).create_view("buy_gap_raw")
    connection.read_csv(args.returns).create_view("returns_raw")
    connection.read_csv(args.market_caps).create_view("caps_raw")
    connection.read_csv(args.styles).create_view("styles_raw")
    style_sql = ", ".join(f"s.{column}::DOUBLE" for column in LOB5_EX_SIZE_COLS)
    rows = connection.execute(
        f"""
        WITH vr AS (
            SELECT DISTINCT symbol, date::INTEGER AS date, vr_log::DOUBLE AS vr
            FROM vr_raw WHERE lower(is_valid) = 'true'
        ),
        buy_gap AS (
            SELECT DISTINCT symbol, date::INTEGER AS date,
                   large_gap_buy_ratio::DOUBLE AS buy_gap
            FROM buy_gap_raw WHERE passes_match_rate
        ),
        returns AS (
            SELECT DISTINCT symbol, date::INTEGER AS date,
                   next_date::INTEGER AS next_date, open::DOUBLE AS open,
                   o2o_ret::DOUBLE AS o2o_ret
            FROM returns_raw WHERE next_date::INTEGER > date::INTEGER
        ),
        caps AS (
            SELECT DISTINCT symbol, date::INTEGER AS date, total_mv::DOUBLE AS total_mv
            FROM caps_raw
        ),
        previous_caps AS (
            SELECT symbol, date,
                   lag(total_mv) OVER (PARTITION BY symbol ORDER BY date) AS previous_cap
            FROM caps
        ),
        styles AS (
            SELECT DISTINCT symbol, replace(date::VARCHAR, '-', '')::INTEGER AS date,
                   {', '.join(column + '::DOUBLE AS ' + column for column in LOB5_EX_SIZE_COLS)}
            FROM styles_raw
        )
        SELECT vr.symbol, vr.date, vr.vr, b.buy_gap, r0.open, c.previous_cap,
               r0.next_date AS entry_date, r1.next_date AS exit_date,
               r1.o2o_ret AS target_return, {style_sql}
        FROM vr
        JOIN buy_gap b USING (symbol, date)
        JOIN returns r0 ON r0.symbol = vr.symbol AND r0.date = vr.date
        JOIN previous_caps c ON c.symbol = vr.symbol AND c.date = vr.date
        JOIN styles s ON s.symbol = vr.symbol AND s.date = vr.date
        LEFT JOIN returns r1 ON r1.symbol = r0.symbol AND r1.date = r0.next_date
        WHERE vr.date BETWEEN ? AND ?
        ORDER BY vr.date, vr.symbol
        """,
        [args.date_from, args.date_to],
    ).fetchall()
    connection.close()

    excluded_symbols = set(args.exclude_symbols)
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
        vr_value, buy_gap, signal_open, previous_cap = map(finite, raw[2:6])
        styles = [finite(value) for value in raw[9:]]
        if (
            vr_value is None or buy_gap is None or signal_open is None
            or signal_open <= 0 or previous_cap is None
            or any(value is None for value in styles)
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
                "vr": float(vr_value),
                "buy_gap": float(buy_gap),
                "entry_date": int(raw[6]) if raw[6] is not None else None,
                "exit_date": int(raw[7]) if raw[7] is not None else None,
                "target": finite(raw[8]),
                "styles": [float(value) for value in styles if value is not None],
                "cap_group": groups[0],
                "price_group": groups[1],
            }
        )
    counts["signal_rows"] = len(output)
    counts["signal_symbols"] = len({str(row["symbol"]) for row in output})
    counts["label_rows"] = sum(row["target"] is not None for row in output)
    return output, counts


def strategy_row(
    *,
    rows: list[dict[str, object]],
    long_indices: list[int],
    short_indices: list[int],
    signal_variant: str,
    strategy: str,
    cap_group: str,
    price_group: str,
    minimum_leg_names: int,
) -> dict[str, object] | None:
    long_eligible = [index for index in long_indices if rows[index]["target"] is not None]
    short_eligible = [index for index in short_indices if rows[index]["target"] is not None]
    universe = [float(row["target"]) for row in rows if row["target"] is not None]
    if (
        len(long_eligible) < minimum_leg_names
        or len(short_eligible) < minimum_leg_names
        or len(universe) < 20
    ):
        return None
    long_return = mean(float(rows[index]["target"]) for index in long_eligible)
    short_return = mean(float(rows[index]["target"]) for index in short_eligible)
    universe_return = mean(universe)
    entry_dates = {rows[index]["entry_date"] for index in long_eligible + short_eligible}
    exit_dates = {rows[index]["exit_date"] for index in long_eligible + short_eligible}
    return {
        "signal_variant": signal_variant,
        "strategy": strategy,
        "cap_group": cap_group,
        "price_group": price_group,
        "date": rows[0]["date"],
        "entry_date": next(iter(entry_dates)) if len(entry_dates) == 1 else "mixed",
        "exit_date": next(iter(exit_dates)) if len(exit_dates) == 1 else "mixed",
        "signal_n": len(rows),
        "long_n": len(long_eligible),
        "short_n": len(short_eligible),
        "long_share": len(long_indices) / len(rows),
        "short_share": len(short_indices) / len(rows),
        "long_ret": long_return,
        "short_ret": short_return,
        "long_short": long_return - short_return,
        "universe_ret": universe_return,
        "long_excess": long_return - universe_return,
        "short_excess": short_return - universe_return,
        "short_alpha": universe_return - short_return,
    }


def build_daily(
    rows: list[dict[str, object]],
    winsor_lower: float,
    winsor_upper: float,
    minimum_leg_names: int,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    grouped: dict[tuple[int, str, str], list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        grouped[(int(row["date"]), str(row["cap_group"]), str(row["price_group"]))].append(row)

    daily: list[dict[str, object]] = []
    correlations: list[dict[str, object]] = []
    aggregate_selections: dict[
        tuple[int, str, str], dict[str, list[dict[str, object]]]
    ] = defaultdict(lambda: {"long": [], "short": [], "universe": []})
    for (date, cap_group, price_group), group_rows in sorted(grouped.items()):
        group_rows.sort(key=lambda row: str(row["symbol"]))
        if len(group_rows) < 20:
            continue
        styles = [list(row["styles"]) for row in group_rows]
        basis = build_orthonormal_basis(styles)
        raw_b = winsorize([float(row["buy_gap"]) for row in group_rows], winsor_lower, winsor_upper)
        raw_vr = winsorize([float(row["vr"]) for row in group_rows], winsor_lower, winsor_upper)
        neutral_b = residualize(raw_b, basis)
        neutral_vr = residualize(raw_vr, basis)
        correlations.append(
            {
                "date": date,
                "cap_group": cap_group,
                "price_group": price_group,
                "n": len(group_rows),
                "raw_rank_correlation": pearson(ranks(raw_b), ranks(raw_vr)),
                "lob5_rank_correlation": pearson(ranks(neutral_b), ranks(neutral_vr)),
            }
        )
        for signal_variant, b_values, vr_values in (
            ("raw_domain", raw_b, raw_vr),
            ("lob5_ex_size_domain", neutral_b, neutral_vr),
        ):
            b_percentiles = percentile_ranks(b_values)
            vr_percentiles = percentile_ranks(vr_values)
            for strategy in STRATEGIES:
                long_indices, short_indices = selection_indices(
                    b_percentiles, vr_percentiles, strategy
                )
                result = strategy_row(
                    rows=group_rows,
                    long_indices=long_indices,
                    short_indices=short_indices,
                    signal_variant=signal_variant,
                    strategy=strategy,
                    cap_group=cap_group,
                    price_group=price_group,
                    minimum_leg_names=minimum_leg_names,
                )
                if result is not None:
                    daily.append(result)
                aggregate = aggregate_selections[(date, signal_variant, strategy)]
                aggregate["universe"].extend(group_rows)
                aggregate["long"].extend(group_rows[index] for index in long_indices)
                aggregate["short"].extend(group_rows[index] for index in short_indices)

    for (date, signal_variant, strategy), selections in sorted(aggregate_selections.items()):
        universe_rows = selections["universe"]
        row_index = {id(row): index for index, row in enumerate(universe_rows)}
        result = strategy_row(
            rows=universe_rows,
            long_indices=[row_index[id(row)] for row in selections["long"]],
            short_indices=[row_index[id(row)] for row in selections["short"]],
            signal_variant=signal_variant.replace("_domain", "_domain_aggregate"),
            strategy=strategy,
            cap_group="all",
            price_group="domain_ranked",
            minimum_leg_names=minimum_leg_names,
        )
        if result is not None:
            daily.append(result)
    return daily, correlations


def summarize_daily(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    keys = ("signal_variant", "strategy", "cap_group", "price_group")
    grouped: dict[tuple[str, ...], list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        grouped[tuple(str(row[key]) for key in keys)].append(row)
    output: list[dict[str, object]] = []
    for key, observations in sorted(grouped.items()):
        result: dict[str, object] = dict(zip(keys, key))
        result.update(
            n_days=len(observations),
            n_obs=sum(int(row["signal_n"]) for row in observations),
            avg_universe_names=mean(int(row["signal_n"]) for row in observations),
            avg_long_names=mean(int(row["long_n"]) for row in observations),
            avg_short_names=mean(int(row["short_n"]) for row in observations),
            avg_long_share=mean(float(row["long_share"]) for row in observations),
            avg_short_share=mean(float(row["short_share"]) for row in observations),
        )
        for metric in (
            "long_ret", "short_ret", "long_short", "long_excess",
            "short_excess", "short_alpha",
        ):
            values = [float(row[metric]) for row in observations]
            result[metric], result[f"{metric}_t"] = mean_t(values)
            result[f"{metric}_bps"] = float(result[metric]) * 10_000
        result["long_short_positive_share"] = mean(
            float(row["long_short"]) > 0 for row in observations
        )
        output.append(result)
    return output


def summarize_correlations(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    grouped: dict[tuple[str, str], list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        grouped[(str(row["cap_group"]), str(row["price_group"]))].append(row)
    output: list[dict[str, object]] = []
    for key, observations in sorted(grouped.items()):
        result: dict[str, object] = {
            "cap_group": key[0],
            "price_group": key[1],
            "n_days": len(observations),
            "n_obs": sum(int(row["n"]) for row in observations),
            "avg_names": mean(int(row["n"]) for row in observations),
        }
        for metric in ("raw_rank_correlation", "lob5_rank_correlation"):
            values = [float(row[metric]) for row in observations if row[metric] is not None]
            result[metric], result[f"{metric}_t"] = mean_t(values)
            result[f"{metric}_mean_abs"] = mean(map(abs, values))
        output.append(result)
    return output


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--vr",
        default=str(PROJECT_ROOT / "data/processed/order_behavior_ratio_daily_202601.csv"),
    )
    parser.add_argument(
        "--buy-gap",
        default=str(
            PROJECT_ROOT / "data/processed/passive_large_gap_bs_theta5d_match95_202512_202601.csv"
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
    parser.add_argument("--date-from", type=int, default=20260105)
    parser.add_argument("--date-to", type=int, default=20260130)
    parser.add_argument("--winsor-lower", type=float, default=0.01)
    parser.add_argument("--winsor-upper", type=float, default=0.99)
    parser.add_argument("--minimum-leg-names", type=int, default=5)
    parser.add_argument("--exclude-symbols", nargs="*", default=["SH689009"])
    parser.add_argument(
        "--output-dir",
        default=str(
            PROJECT_ROOT / "results/daily/vr_large_gap_buy_intersection_o2o_lob5_202601"
        ),
    )
    args = parser.parse_args()
    if not 0 <= args.winsor_lower < args.winsor_upper <= 1:
        raise ValueError("invalid winsor bounds")
    if args.minimum_leg_names < 1:
        raise ValueError("minimum-leg-names must be positive")

    rows, counts = load_rows(args)
    daily, correlations = build_daily(
        rows,
        args.winsor_lower,
        args.winsor_upper,
        args.minimum_leg_names,
    )
    summary = summarize_daily(daily)
    correlation_summary = summarize_correlations(correlations)
    output_dir = Path(args.output_dir)
    write_csv(output_dir / "performance_by_date.csv", daily)
    write_csv(output_dir / "performance_summary.csv", summary)
    write_csv(output_dir / "correlation_by_date.csv", correlations)
    write_csv(output_dir / "correlation_summary.csv", correlation_summary)

    metadata = {
        "signal_timestamp": "T close after 14:57",
        "target": "T+1 open to T+2 open",
        "factor_directions": {
            "long": "low B; VR confirms when low",
            "short": "high B; VR confirms when high",
        },
        "strategies": {
            "b_baseline_30": "B bottom 30% long, B top 30% short",
            "strict_both_30": "B and VR both bottom 30% long; both top 30% short",
            "vr_median_filter": "B bottom/top 30%, confirmed by matching VR half",
            "short_vr_confirmed": "B bottom 30% long; B top 30% and VR top half short",
        },
        "domain_rule": "previous-day market cap x signal-date open price/STAR board",
        "primary_neutralization": {
            "name": "LOB5-ex-size",
            "columns": list(LOB5_EX_SIZE_COLS),
            "style_timing": "signal-date close-known exposure",
            "winsorization": [args.winsor_lower, args.winsor_upper],
        },
        "missing_label_policy": "select on signal universe, then drop missing target",
        "universe_audit": {
            "source": "ods.ods_jydb_secu_main SecuCategory=1, SecuMarket in (83,90)",
            "result": "zero ETFs; SH689009 CDR excluded",
        },
        "tradability_note": "return cache has no explicit ST/suspension fields; costs not deducted",
        "counts": counts,
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    metadata_path = output_dir / "metadata.json"
    temporary = metadata_path.with_name(f".{metadata_path.name}.tmp")
    temporary.write_text(json.dumps(metadata, ensure_ascii=False, indent=2) + "\n")
    temporary.replace(metadata_path)
    print(
        f"signal_rows={len(rows)} daily_rows={len(daily)} summary_rows={len(summary)} "
        f"output_dir={output_dir}"
    )
    print(f"counts={counts}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
