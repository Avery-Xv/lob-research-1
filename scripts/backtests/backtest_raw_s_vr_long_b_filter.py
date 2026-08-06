#!/usr/bin/env python3
"""Long-only raw S plus VR strategies with raw B used only as a filter."""

from __future__ import annotations

import argparse
import csv
import json
import math
from collections import defaultdict
from pathlib import Path
from statistics import mean
from typing import NamedTuple, Sequence

import duckdb

import backtest_large_gap_by_raw_vr_state as base
from backtest_daily_domains import domain
from backtest_existing_daily_o2o_cne5 import finite


TARGETS = ("ret_1031_1040", "ret_1031_1045", "ret_1031_1100")
VR_SCOPES = ("high", "mid_high")
S_CUTS = ("top10", "top20")
B_FILTERS = ("none", "not_bottom20", "middle_20_90")


class CommonValue(NamedTuple):
    signal_price: float
    targets: tuple[float | None, ...]
    previous_market_cap: float


class Observation(NamedTuple):
    symbol: str
    buy_gap: float
    sell_gap: float
    vr_log: float
    targets: tuple[float | None, ...]
    previous_market_cap: float
    signal_price: float


def load_common(
    returns_path: str,
    caps_path: str,
    date_from: int,
    date_to: int,
) -> dict[tuple[str, int], CommonValue]:
    connection = duckdb.connect()
    connection.read_csv(returns_path).create_view("returns_raw")
    connection.read_csv(caps_path).create_view("caps_raw")
    connection.execute(
        """
        CREATE VIEW caps AS
        SELECT DISTINCT symbol, date::INTEGER AS date, total_mv::DOUBLE AS total_mv
        FROM caps_raw;
        CREATE VIEW previous_caps AS
        SELECT symbol, date,
               lag(total_mv) OVER (PARTITION BY symbol ORDER BY date) AS previous_market_cap
        FROM caps;
        """
    )
    rows = connection.execute(
        """
        SELECT r.symbol, r.date::INTEGER, r.signal_price::DOUBLE,
               r.ret_1031_1040::DOUBLE, r.ret_1031_1045::DOUBLE,
               r.ret_1031_1100::DOUBLE, c.previous_market_cap::DOUBLE
        FROM returns_raw r
        JOIN previous_caps c ON c.symbol=r.symbol AND c.date=r.date::INTEGER
        WHERE r.date::INTEGER BETWEEN ? AND ?
          AND r.is_st::INTEGER=0 AND r.is_suspended::INTEGER=0
        """,
        [date_from, date_to],
    ).fetchall()
    connection.close()
    output: dict[tuple[str, int], CommonValue] = {}
    for row in rows:
        price, cap = finite(row[2]), finite(row[6])
        if price is None or price <= 0 or cap is None or cap <= 0:
            continue
        output[(str(row[0]), int(row[1]))] = CommonValue(
            float(price), tuple(finite(value) for value in row[3:6]), float(cap)
        )
    return output


def load_groups(
    factor_path: str,
    common: dict[tuple[str, int], CommonValue],
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
                raise ValueError(f"non-SH/SZ symbol: {symbol}")
            if "ETF excluded" not in row.get("universe_rule", ""):
                raise ValueError("factor artifact does not document ETF exclusion")
            common_value = common.get(key)
            buy_gap = finite(row.get("intraday_large_gap_buy_ratio"))
            sell_gap = finite(row.get("intraday_large_gap_sell_ratio"))
            vr_log = finite(row.get("vr_log"))
            if (
                common_value is None
                or buy_gap is None
                or sell_gap is None
                or vr_log is None
                or not base.parse_bool(row.get("intraday_passes_match_rate"))
                or not base.parse_bool(row.get("ob_is_valid"))
            ):
                continue
            grouped[date].append(Observation(
                symbol=symbol,
                buy_gap=float(buy_gap),
                sell_gap=float(sell_gap),
                vr_log=float(vr_log),
                targets=common_value.targets,
                previous_market_cap=common_value.previous_market_cap,
                signal_price=common_value.signal_price,
            ))
    return grouped


def exact_top_indices(
    values: Sequence[float], symbols: Sequence[str], fraction: float
) -> list[int]:
    if len(values) != len(symbols) or not values:
        raise ValueError("selection inputs must have one equal, positive length")
    count = max(1, math.floor(len(values) * fraction))
    order = sorted(range(len(values)), key=lambda index: (values[index], symbols[index]))
    return order[-count:]


def percentile_positions(values: Sequence[float], symbols: Sequence[str]) -> list[float]:
    order = sorted(range(len(values)), key=lambda index: (values[index], symbols[index]))
    denominator = max(1, len(values) - 1)
    output = [0.0] * len(values)
    for position, index in enumerate(order):
        output[index] = position / denominator
    return output


def select_long_indices(
    rows: Sequence[Observation], s_cut: str, b_filter: str
) -> list[int]:
    fraction = {"top10": 0.10, "top20": 0.20}[s_cut]
    symbols = [row.symbol for row in rows]
    selected = exact_top_indices([row.sell_gap for row in rows], symbols, fraction)
    if b_filter == "none":
        return selected
    b_positions = percentile_positions([row.buy_gap for row in rows], symbols)
    if b_filter == "not_bottom20":
        return [index for index in selected if b_positions[index] >= 0.20]
    if b_filter == "middle_20_90":
        return [index for index in selected if 0.20 <= b_positions[index] <= 0.90]
    raise ValueError(f"unknown B filter: {b_filter}")


def append_portfolio_metrics(
    output: list[dict[str, object]],
    date: int,
    scope: str,
    cap_group: str,
    price_group: str,
    vr_scope: str,
    s_cut: str,
    b_filter: str,
    benchmark_rows: Sequence[Observation],
    selected_rows: Sequence[Observation],
) -> None:
    for target_index, target in enumerate(TARGETS):
        benchmark_returns = [
            float(row.targets[target_index]) for row in benchmark_rows
            if row.targets[target_index] is not None
        ]
        selected_returns = [
            float(row.targets[target_index]) for row in selected_rows
            if row.targets[target_index] is not None
        ]
        if not benchmark_returns or not selected_returns:
            continue
        long_return = mean(selected_returns)
        benchmark_return = mean(benchmark_returns)
        output.append({
            "portfolio": f"s_{s_cut}__b_{b_filter}",
            "s_cut": s_cut,
            "b_filter": b_filter,
            "vr_scope": vr_scope,
            "target": target,
            "scope": scope,
            "cap_group": cap_group,
            "price_group": price_group,
            "date": date,
            "n_selected": len(selected_returns),
            "n_benchmark": len(benchmark_returns),
            "long_return": long_return,
            "benchmark_return": benchmark_return,
            "excess_return": long_return - benchmark_return,
        })


def process_groups(
    grouped: dict[int, list[Observation]], minimum_cross_section: int
) -> list[dict[str, object]]:
    performance: list[dict[str, object]] = []
    for date, date_rows in sorted(grouped.items()):
        domains: dict[tuple[str, str], list[Observation]] = defaultdict(list)
        for row in date_rows:
            group = domain(row.previous_market_cap, row.signal_price, row.symbol)
            if group is not None:
                domains[group].append(row)
        pooled_benchmark: dict[str, list[Observation]] = defaultdict(list)
        pooled_selected: dict[tuple[str, str, str], list[Observation]] = defaultdict(list)
        for (cap_group, price_group), domain_rows in sorted(domains.items()):
            if len(domain_rows) < minimum_cross_section:
                continue
            domain_rows.sort(key=lambda row: row.symbol)
            states = base.assign_raw_vr_states(
                [row.vr_log for row in domain_rows], [row.symbol for row in domain_rows]
            )
            for vr_scope in VR_SCOPES:
                accepted = {"high"} if vr_scope == "high" else {"mid", "high"}
                benchmark_rows = [
                    row for row, state in zip(domain_rows, states) if state in accepted
                ]
                if len(benchmark_rows) < minimum_cross_section:
                    continue
                pooled_benchmark[vr_scope].extend(benchmark_rows)
                for s_cut in S_CUTS:
                    for b_filter in B_FILTERS:
                        selected_indices = select_long_indices(
                            benchmark_rows, s_cut, b_filter
                        )
                        selected_rows = [benchmark_rows[index] for index in selected_indices]
                        if not selected_rows:
                            continue
                        append_portfolio_metrics(
                            performance, date, "domain", cap_group, price_group,
                            vr_scope, s_cut, b_filter, benchmark_rows, selected_rows,
                        )
                        pooled_selected[(vr_scope, s_cut, b_filter)].extend(selected_rows)
        for (vr_scope, s_cut, b_filter), selected_rows in sorted(pooled_selected.items()):
            append_portfolio_metrics(
                performance, date, "domain_aggregate", "domain_aggregate", "aggregate",
                vr_scope, s_cut, b_filter, pooled_benchmark[vr_scope], selected_rows,
            )
    return performance


def summarize(rows: Sequence[dict[str, object]]) -> list[dict[str, object]]:
    keys = (
        "portfolio", "s_cut", "b_filter", "vr_scope", "target", "scope",
        "cap_group", "price_group",
    )
    grouped: dict[tuple[str, ...], list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        grouped[tuple(str(row[key]) for key in keys)].append(row)
    output: list[dict[str, object]] = []
    for key, values in sorted(grouped.items()):
        result: dict[str, object] = dict(zip(keys, key))
        result.update(
            n_days=len(values),
            n_selected=sum(int(row["n_selected"]) for row in values),
            avg_selected=mean(int(row["n_selected"]) for row in values),
            avg_benchmark=mean(int(row["n_benchmark"]) for row in values),
            long_hit_rate=mean(float(row["long_return"]) > 0 for row in values),
            excess_hit_rate=mean(float(row["excess_return"]) > 0 for row in values),
        )
        for metric in ("long_return", "benchmark_return", "excess_return"):
            result[metric], result[f"{metric}_t"] = base.mean_t(
                [float(row[metric]) for row in values]
            )
        for cost_bp in (1, 2, 3):
            cost = cost_bp / 10_000.0
            for metric in ("long_return", "excess_return"):
                net_metric = f"{metric}_net_{cost_bp}bp"
                result[net_metric], result[f"{net_metric}_t"] = base.mean_t(
                    [float(row[metric]) - cost for row in values]
                )
        output.append(result)
    return output


def build_filter_contrasts(
    rows: Sequence[dict[str, object]],
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    key_names = (
        "s_cut", "vr_scope", "target", "scope", "cap_group", "price_group", "date"
    )
    indexed = {
        (str(row["b_filter"]), *(str(row[key]) for key in key_names)): row
        for row in rows
    }
    by_date: list[dict[str, object]] = []
    for key in sorted({key[1:] for key in indexed}):
        baseline = indexed.get(("none", *key))
        if baseline is None:
            continue
        for b_filter in B_FILTERS[1:]:
            filtered = indexed.get((b_filter, *key))
            if filtered is None:
                continue
            by_date.append({
                "comparison": f"{b_filter}_minus_none",
                **dict(zip(key_names, key)),
                "delta_long_return": (
                    float(filtered["long_return"]) - float(baseline["long_return"])
                ),
                "delta_excess_return": (
                    float(filtered["excess_return"]) - float(baseline["excess_return"])
                ),
                "n_filtered": int(filtered["n_selected"]),
                "n_baseline": int(baseline["n_selected"]),
            })
    summary_keys = (
        "comparison", "s_cut", "vr_scope", "target", "scope", "cap_group",
        "price_group",
    )
    grouped: dict[tuple[str, ...], list[dict[str, object]]] = defaultdict(list)
    for row in by_date:
        grouped[tuple(str(row[key]) for key in summary_keys)].append(row)
    summary: list[dict[str, object]] = []
    for key, values in sorted(grouped.items()):
        result: dict[str, object] = dict(zip(summary_keys, key))
        result["n_days"] = len(values)
        result["avg_filtered"] = mean(int(row["n_filtered"]) for row in values)
        result["avg_baseline"] = mean(int(row["n_baseline"]) for row in values)
        for metric in ("delta_long_return", "delta_excess_return"):
            result[metric], result[f"{metric}_t"] = base.mean_t(
                [float(row[metric]) for row in values]
            )
        summary.append(result)
    return by_date, summary


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--factors", required=True)
    parser.add_argument("--returns", required=True)
    parser.add_argument("--market-caps", required=True)
    parser.add_argument("--date-from", type=int, default=20260201)
    parser.add_argument("--date-to", type=int, default=20260430)
    parser.add_argument("--minimum-cross-section", type=int, default=20)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()

    common = load_common(args.returns, args.market_caps, args.date_from, args.date_to)
    grouped = load_groups(args.factors, common, args.date_from, args.date_to)
    performance = process_groups(grouped, args.minimum_cross_section)
    summary = summarize(performance)
    contrast_by_date, contrast_summary = build_filter_contrasts(performance)
    output_dir = Path(args.output_dir)
    base.write_csv(output_dir / "performance_by_date.csv", performance)
    base.write_csv(output_dir / "performance_summary.csv", summary)
    base.write_csv(output_dir / "b_filter_contrast_by_date.csv", contrast_by_date)
    base.write_csv(output_dir / "b_filter_contrast_summary.csv", contrast_summary)
    output_dir.mkdir(parents=True, exist_ok=True)
    metadata = {
        "date_from": args.date_from,
        "date_to": args.date_to,
        "strategy": "raw S long-only, raw vr_log state filter, raw B eligibility filter",
        "neutralization": "none",
        "vr_scopes": {
            "high": "top raw-vr tercile within date-domain",
            "mid_high": "middle and top raw-vr terciles within date-domain",
        },
        "s_cuts": {
            "top10": "top 10% raw S within eligible date-domain VR scope",
            "top20": "top 20% raw S within eligible date-domain VR scope",
        },
        "b_filters": {
            "none": "no B filter",
            "not_bottom20": "exclude bottom 20% raw B within eligible date-domain VR scope",
            "middle_20_90": "retain raw B percentile from 20% through 90% inclusive",
        },
        "signal_and_entry": "factor uses 10:00-10:30 events; entry is 10:30 minute close",
        "targets": list(TARGETS),
        "costs": "1/2/3 bp are assumed total round-trip costs subtracted from gross return",
        "domain_rule": "previous-day market cap crossed with signal-time price/board",
        "universe_rule": "point-in-time Shanghai/Shenzhen A shares; ETF excluded upstream",
        "validity": "match_rate>=0.95, ob_is_valid, non-ST, non-suspended",
        "missing_label_policy": "VR states and selections formed before target-specific filtering",
        "common_rows": len(common),
        "analysis_rows": sum(len(rows) for rows in grouped.values()),
    }
    (output_dir / "metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2) + "\n"
    )
    print(
        f"dates={len(grouped)} analysis_rows={metadata['analysis_rows']} "
        f"performance_rows={len(performance)} output_dir={output_dir}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
