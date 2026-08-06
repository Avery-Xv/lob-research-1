#!/usr/bin/env python3
"""Summarize compact M1--M6 stock-day sufficient-stat shards."""

from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from collections import defaultdict
from pathlib import Path
from typing import Callable, Iterable, Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


GROUP_FIELDS = [
    "domain", "mechanism", "variant", "group_key", "stock_days",
    "observations", "weighted_mean", "stock_day_mean", "stock_day_t",
    "ci95_lower", "ci95_upper",
]
CONTRAST_FIELDS = [
    "domain", "mechanism", "contrast", "left_group", "right_group",
    "stock_days", "left_mean", "right_mean", "difference", "difference_t",
    "ci95_lower", "ci95_upper", "symbols", "symbol_mean_difference",
    "symbol_t", "dates", "date_mean_difference", "date_t",
]


def mean_t_ci(values: Sequence[float]) -> tuple[float | None, float | None, float | None, float | None]:
    if not values:
        return None, None, None, None
    mean = sum(values) / len(values)
    if len(values) < 2:
        return mean, None, None, None
    variance = sum((value - mean) ** 2 for value in values) / (len(values) - 1)
    standard_error = math.sqrt(variance / len(values))
    if standard_error == 0:
        return mean, 0.0, mean, mean
    return mean, mean / standard_error, mean - 1.96 * standard_error, mean + 1.96 * standard_error


def atomic_csv(path: Path, fields: Sequence[str], rows: Sequence[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    try:
        with temporary.open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)
        temporary.replace(path)
    finally:
        if temporary.exists():
            temporary.unlink()


def load_domains(path: Path | None) -> dict[tuple[str, int], str]:
    if path is None:
        return {}
    result: dict[tuple[str, int], str] = {}
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        required = {"symbol", "date", "domain"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"domain file missing fields: {sorted(missing)}")
        for row in reader:
            key = (row["symbol"], int(row["date"]))
            if key in result:
                raise ValueError(f"duplicate domain row: {key}")
            result[key] = row["domain"]
    return result


def load_stats(shard_dir: Path) -> list[dict[str, object]]:
    paths = sorted(shard_dir.glob("batch_*/stats.csv"))
    if not paths:
        raise ValueError(f"no stats shards under {shard_dir}")
    rows: list[dict[str, object]] = []
    for path in paths:
        with path.open(newline="") as handle:
            for row in csv.DictReader(handle):
                rows.append({
                    "symbol": row["symbol"],
                    "date": int(row["date"]),
                    "mechanism": row["mechanism"],
                    "variant": row["variant"],
                    "group_key": row["group_key"],
                    "observations": int(row["observations"]),
                    "value_sum": float(row["value_sum"]),
                    "value_sq_sum": float(row["value_sq_sum"]),
                    "weight_sum": float(row["weight_sum"]),
                })
    return rows


def attach_domains(
    rows: Sequence[dict[str, object]], domains: dict[tuple[str, int], str]
) -> list[dict[str, object]]:
    result: list[dict[str, object]] = []
    for row in rows:
        key = (str(row["symbol"]), int(row["date"]))
        domain = domains.get(key)
        if domains and domain is None:
            continue
        all_row = dict(row)
        all_row["domain"] = "all/all"
        result.append(all_row)
        if domain is not None:
            domain_row = dict(row)
            domain_row["domain"] = domain
            result.append(domain_row)
    return result


def group_summary(rows: Sequence[dict[str, object]]) -> list[dict[str, object]]:
    groups: dict[tuple[str, str, str, str], list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        key = (
            str(row["domain"]), str(row["mechanism"]), str(row["variant"]),
            str(row["group_key"]),
        )
        groups[key].append(row)
    output: list[dict[str, object]] = []
    for key, observations in sorted(groups.items()):
        daily_means = [
            float(row["value_sum"]) / float(row["weight_sum"])
            for row in observations if float(row["weight_sum"]) > 0
        ]
        stock_day_mean, stock_day_t, lower, upper = mean_t_ci(daily_means)
        total_weight = sum(float(row["weight_sum"]) for row in observations)
        weighted_mean = (
            sum(float(row["value_sum"]) for row in observations) / total_weight
            if total_weight else None
        )
        output.append({
            "domain": key[0], "mechanism": key[1], "variant": key[2],
            "group_key": key[3], "stock_days": len(daily_means),
            "observations": sum(int(row["observations"]) for row in observations),
            "weighted_mean": weighted_mean, "stock_day_mean": stock_day_mean,
            "stock_day_t": stock_day_t, "ci95_lower": lower, "ci95_upper": upper,
        })
    return output


def daily_values(
    rows: Sequence[dict[str, object]],
    mechanism: str,
    variant: str,
    predicate: Callable[[str], bool],
) -> dict[tuple[str, str, int], float]:
    sums: dict[tuple[str, str, int], float] = defaultdict(float)
    weights: dict[tuple[str, str, int], float] = defaultdict(float)
    for row in rows:
        if row["mechanism"] != mechanism or row["variant"] != variant:
            continue
        if not predicate(str(row["group_key"])):
            continue
        key = (str(row["domain"]), str(row["symbol"]), int(row["date"]))
        sums[key] += float(row["value_sum"])
        weights[key] += float(row["weight_sum"])
    return {key: sums[key] / weight for key, weight in weights.items() if weight > 0}


def add_difference_contrast(
    output: list[dict[str, object]],
    rows: Sequence[dict[str, object]],
    mechanism: str,
    contrast: str,
    variant: str,
    left_name: str,
    left: Callable[[str], bool],
    right_name: str,
    right: Callable[[str], bool],
) -> None:
    left_values = daily_values(rows, mechanism, variant, left)
    right_values = daily_values(rows, mechanism, variant, right)
    domains = sorted({key[0] for key in left_values} | {key[0] for key in right_values})
    for domain in domains:
        keys = sorted(
            set(key for key in left_values if key[0] == domain)
            & set(key for key in right_values if key[0] == domain)
        )
        differences = [left_values[key] - right_values[key] for key in keys]
        difference, difference_t, lower, upper = mean_t_ci(differences)
        by_symbol: dict[str, list[float]] = defaultdict(list)
        by_date: dict[int, list[float]] = defaultdict(list)
        for key, value in zip(keys, differences):
            by_symbol[key[1]].append(value)
            by_date[key[2]].append(value)
        symbol_values = [sum(values) / len(values) for values in by_symbol.values()]
        date_values = [sum(values) / len(values) for values in by_date.values()]
        symbol_mean, symbol_t, _symbol_lower, _symbol_upper = mean_t_ci(symbol_values)
        date_mean, date_t, _date_lower, _date_upper = mean_t_ci(date_values)
        output.append({
            "domain": domain, "mechanism": mechanism, "contrast": contrast,
            "left_group": left_name, "right_group": right_name,
            "stock_days": len(keys),
            "left_mean": sum(left_values[key] for key in keys) / len(keys) if keys else None,
            "right_mean": sum(right_values[key] for key in keys) / len(keys) if keys else None,
            "difference": difference, "difference_t": difference_t,
            "ci95_lower": lower, "ci95_upper": upper,
            "symbols": len(symbol_values), "symbol_mean_difference": symbol_mean,
            "symbol_t": symbol_t, "dates": len(date_values),
            "date_mean_difference": date_mean, "date_t": date_t,
        })


def add_within_symbol_contrast(
    output: list[dict[str, object]],
    rows: Sequence[dict[str, object]],
    mechanism: str,
    contrast: str,
    variant: str,
    left_name: str,
    left: Callable[[str], bool],
    right_name: str,
    right: Callable[[str], bool],
) -> None:
    """Compare mutually exclusive day groups after averaging within symbol."""
    left_daily = daily_values(rows, mechanism, variant, left)
    right_daily = daily_values(rows, mechanism, variant, right)
    domains = sorted({key[0] for key in left_daily} | {key[0] for key in right_daily})
    for domain in domains:
        left_by_symbol: dict[str, list[float]] = defaultdict(list)
        right_by_symbol: dict[str, list[float]] = defaultdict(list)
        for (row_domain, symbol, _date), value in left_daily.items():
            if row_domain == domain:
                left_by_symbol[symbol].append(value)
        for (row_domain, symbol, _date), value in right_daily.items():
            if row_domain == domain:
                right_by_symbol[symbol].append(value)
        symbols = sorted(set(left_by_symbol) & set(right_by_symbol))
        left_values = [sum(left_by_symbol[s]) / len(left_by_symbol[s]) for s in symbols]
        right_values = [sum(right_by_symbol[s]) / len(right_by_symbol[s]) for s in symbols]
        differences = [left - right for left, right in zip(left_values, right_values)]
        difference, difference_t, lower, upper = mean_t_ci(differences)
        output.append({
            "domain": domain, "mechanism": mechanism, "contrast": contrast,
            "left_group": left_name, "right_group": right_name,
            # Paired units are symbols; keep the legacy field for schema stability.
            "stock_days": len(symbols),
            "left_mean": sum(left_values) / len(left_values) if left_values else None,
            "right_mean": sum(right_values) / len(right_values) if right_values else None,
            "difference": difference, "difference_t": difference_t,
            "ci95_lower": lower, "ci95_upper": upper,
            "symbols": len(symbols), "symbol_mean_difference": difference,
            "symbol_t": difference_t, "dates": None,
            "date_mean_difference": None, "date_t": None,
        })


def build_contrasts(rows: Sequence[dict[str, object]]) -> list[dict[str, object]]:
    output: list[dict[str, object]] = []
    add_difference_contrast(
        output, rows, "M1", "buy_vs_sell_same_direction_persistence",
        "n10_future_signed_volume", "trigger=B", lambda group: "trigger=B" in group,
        "trigger=S", lambda group: "trigger=S" in group,
    )
    add_difference_contrast(
        output, rows, "M2", "up_buy_vs_down_sell_delta_heat",
        "n10_delta_heat", "price=up|trigger=B",
        lambda group: "price=up|trigger=B" in group,
        "price=down|trigger=S", lambda group: "price=down|trigger=S" in group,
    )
    add_difference_contrast(
        output, rows, "M3", "buy_up_vs_buy_non_up_delta_heat",
        "n10_delta_heat", "buy_up", lambda group: group == "buy_up",
        "buy_non_up", lambda group: group == "buy_non_up",
    )
    add_difference_contrast(
        output, rows, "M4", "buy_high_vs_low_ask_depth",
        "hl5_log_depth3_ask", "B1", lambda group: "state=B1" in group,
        "B0", lambda group: "state=B0" in group,
    )
    add_difference_contrast(
        output, rows, "M4", "sell_high_vs_low_bid_depth",
        "hl5_log_depth3_bid", "S1", lambda group: "S1" in group,
        "S0", lambda group: "S0" in group,
    )
    add_difference_contrast(
        output, rows, "M1", "trade_event_buy_vs_sell_same_direction_persistence",
        "teh20_n10_future_signed_volume",
        "trigger=B", lambda group: "trigger=B" in group,
        "trigger=S", lambda group: "trigger=S" in group,
    )
    add_difference_contrast(
        output, rows, "M2", "trade_event_up_buy_vs_down_sell_delta_heat",
        "teh20_n10_delta_heat", "price=up|trigger=B",
        lambda group: "price=up|trigger=B" in group,
        "price=down|trigger=S", lambda group: "price=down|trigger=S" in group,
    )
    add_difference_contrast(
        output, rows, "M3", "trade_event_buy_up_vs_buy_non_up_delta_heat",
        "teh20_n10_delta_heat", "buy_up", lambda group: group == "buy_up",
        "buy_non_up", lambda group: group == "buy_non_up",
    )
    add_difference_contrast(
        output, rows, "M4", "trade_event_buy_high_vs_low_ask_depth",
        "teh20_log_depth3_ask", "B1", lambda group: "state=B1" in group,
        "B0", lambda group: "state=B0" in group,
    )
    add_difference_contrast(
        output, rows, "M4", "trade_event_sell_high_vs_low_bid_depth",
        "teh20_log_depth3_bid", "S1", lambda group: "S1" in group,
        "S0", lambda group: "S0" in group,
    )
    for state in ("all", "B0S0", "B1S0", "B0S1", "B1S1"):
        add_within_symbol_contrast(
            output, rows, "M5", f"high_vs_low_heat_depth_{state}",
            "mean_log_total_depth3", f"high/{state}",
            lambda group, state=state: group == f"heat=high|state={state}",
            f"low/{state}",
            lambda group, state=state: group == f"heat=low|state={state}",
        )
        add_within_symbol_contrast(
            output, rows, "M5", f"trade_event_high_vs_low_heat_depth_{state}",
            "teh20_mean_log_total_depth3", f"high/{state}",
            lambda group, state=state: group == f"heat=high|state={state}",
            f"low/{state}",
            lambda group, state=state: group == f"heat=low|state={state}",
        )
    for side, own_high, own_low, opposite_high, opposite_low in (
        ("B", "B1", "B0", "S1", "S0"),
        ("S", "S1", "S0", "B1", "B0"),
    ):
        for horizon in ("60s", "continuous"):
            for clock_name, prefix in (("time", ""), ("trade_event", "teh20_")):
                variant = f"{prefix}filled_orders_{horizon}"
                contrast_prefix = "" if clock_name == "time" else "trade_event_"
                add_difference_contrast(
                    output, rows, "M6",
                    f"{contrast_prefix}{side}_own_high_vs_low_fill_{horizon}",
                    variant, own_high,
                    lambda group, side=side, flag=own_high: (
                        f"side={side}" in group
                        and "distance=best" in group
                        and flag in group
                    ),
                    own_low,
                    lambda group, side=side, flag=own_low: (
                        f"side={side}" in group
                        and "distance=best" in group
                        and flag in group
                    ),
                )
                add_difference_contrast(
                    output, rows, "M6",
                    f"{contrast_prefix}{side}_opposite_high_vs_low_fill_{horizon}",
                    variant, opposite_high,
                    lambda group, side=side, flag=opposite_high: (
                        f"side={side}" in group
                        and "distance=best" in group
                        and flag in group
                    ),
                    opposite_low,
                    lambda group, side=side, flag=opposite_low: (
                        f"side={side}" in group
                        and "distance=best" in group
                        and flag in group
                    ),
                )
    return output


def quality_summary(shard_dir: Path) -> dict[str, object]:
    paths = sorted(shard_dir.glob("batch_*/quality.csv"))
    totals: dict[str, int] = defaultdict(int)
    stock_days = 0
    symbols: set[str] = set()
    for path in paths:
        with path.open(newline="") as handle:
            for row in csv.DictReader(handle):
                stock_days += 1
                symbols.add(row["symbol"])
                for field, value in row.items():
                    if field not in {"symbol", "date", "factor_version"}:
                        totals[field] += int(value)
    return {"symbols": len(symbols), "stock_days": stock_days, "totals": dict(totals)}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Summarize compact M1-M6 shards.")
    parser.add_argument("--shard-dir", type=Path, required=True)
    parser.add_argument("--domain-file", type=Path)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    rows = attach_domains(load_stats(args.shard_dir), load_domains(args.domain_file))
    groups = group_summary(rows)
    contrasts = build_contrasts(rows)
    atomic_csv(args.output_dir / "group_summary.csv", GROUP_FIELDS, groups)
    atomic_csv(args.output_dir / "mechanism_contrasts.csv", CONTRAST_FIELDS, contrasts)
    quality = quality_summary(args.shard_dir)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    (args.output_dir / "quality_summary.json").write_text(
        json.dumps(quality, ensure_ascii=False, indent=2) + "\n"
    )
    print(
        f"stat_rows={len(rows)} groups={len(groups)} contrasts={len(contrasts)} "
        f"output_dir={args.output_dir}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
