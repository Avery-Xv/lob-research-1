#!/usr/bin/env python3
"""Summarize M1-Q chain-debias and quote-state sufficient statistics."""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Callable, Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.factors.order_shape_mechanism.summarize_mechanisms import (
    CONTRAST_FIELDS,
    GROUP_FIELDS,
    atomic_csv,
    attach_domains,
    daily_values,
    group_summary,
    load_domains,
    load_stats,
    mean_t_ci,
    quality_summary,
)


def add_contrast(
    output: list[dict[str, object]],
    rows: Sequence[dict[str, object]],
    contrast: str,
    left_variant: str,
    left_name: str,
    left: Callable[[str], bool],
    right_variant: str,
    right_name: str,
    right: Callable[[str], bool],
) -> None:
    left_values = daily_values(rows, "M1Q", left_variant, left)
    right_values = daily_values(rows, "M1Q", right_variant, right)
    domains = sorted({key[0] for key in left_values} | {key[0] for key in right_values})
    for domain in domains:
        keys = sorted(
            {key for key in left_values if key[0] == domain}
            & {key for key in right_values if key[0] == domain}
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
        symbol_mean, symbol_t, _sl, _su = mean_t_ci(symbol_values)
        date_mean, date_t, _dl, _du = mean_t_ci(date_values)
        output.append(
            {
                "domain": domain,
                "mechanism": "M1Q",
                "contrast": contrast,
                "left_group": left_name,
                "right_group": right_name,
                "stock_days": len(keys),
                "left_mean": (
                    sum(left_values[key] for key in keys) / len(keys) if keys else None
                ),
                "right_mean": (
                    sum(right_values[key] for key in keys) / len(keys) if keys else None
                ),
                "difference": difference,
                "difference_t": difference_t,
                "ci95_lower": lower,
                "ci95_upper": upper,
                "symbols": len(symbol_values),
                "symbol_mean_difference": symbol_mean,
                "symbol_t": symbol_t,
                "dates": len(date_values),
                "date_mean_difference": date_mean,
                "date_t": date_t,
            }
        )


def build_contrasts(rows: Sequence[dict[str, object]]) -> list[dict[str, object]]:
    output: list[dict[str, object]] = []
    any_group = lambda _group: True
    chain_all = lambda group: "chain=all" in group
    for clock in ("lob", "trade"):
        for horizon in (20, 50):
            prefix = f"{clock}{horizon}"
            for metric in ("future_signed_volume", "directional_mid_bps"):
                add_contrast(
                    output,
                    rows,
                    f"{prefix}_chain_minus_raw_{metric}",
                    f"{prefix}_chain_{metric}",
                    "chain/all",
                    chain_all,
                    f"{prefix}_raw_{metric}",
                    "raw",
                    any_group,
                )
                add_contrast(
                    output,
                    rows,
                    f"{prefix}_multi_minus_single_{metric}",
                    f"{prefix}_chain_{metric}",
                    "multi",
                    lambda group: "chain=multi" in group,
                    f"{prefix}_chain_{metric}",
                    "single",
                    lambda group: "chain=single" in group,
                )
                add_contrast(
                    output,
                    rows,
                    f"{prefix}_chain_buy_minus_sell_{metric}",
                    f"{prefix}_chain_{metric}",
                    "trigger=B",
                    lambda group: group == "trigger=B|chain=all",
                    f"{prefix}_chain_{metric}",
                    "trigger=S",
                    lambda group: group == "trigger=S|chain=all",
                )
            for metric in ("future_signed", "mid_bps"):
                variant = f"{prefix}_chain_{metric}_by_quote_state"
                group = lambda side, chase, replenish: (
                    f"trigger={side}|chase={chase}|replenish={replenish}|chain=all"
                )
                for side in ("B", "S"):
                    add_contrast(
                        output,
                        rows,
                        f"{prefix}_{side}_chase_effect_low_replenish_{metric}",
                        variant,
                        "chase=1/replenish=0",
                        lambda value, expected=group(side, 1, 0): value == expected,
                        variant,
                        "chase=0/replenish=0",
                        lambda value, expected=group(side, 0, 0): value == expected,
                    )
                    add_contrast(
                        output,
                        rows,
                        f"{prefix}_{side}_replenish_effect_with_chase_{metric}",
                        variant,
                        "chase=1/replenish=1",
                        lambda value, expected=group(side, 1, 1): value == expected,
                        variant,
                        "chase=1/replenish=0",
                        lambda value, expected=group(side, 1, 0): value == expected,
                    )
                    add_contrast(
                        output,
                        rows,
                        f"{prefix}_{side}_penetration_minus_recovery_{metric}",
                        variant,
                        "chase=1/replenish=0",
                        lambda value, expected=group(side, 1, 0): value == expected,
                        variant,
                        "chase=0/replenish=1",
                        lambda value, expected=group(side, 0, 1): value == expected,
                    )
    return output


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Summarize M1-Q shards.")
    parser.add_argument("--shard-dir", type=Path, required=True)
    parser.add_argument("--domain-file", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    rows = attach_domains(
        load_stats(args.shard_dir), load_domains(args.domain_file)
    )
    groups = group_summary(rows)
    contrasts = build_contrasts(rows)
    atomic_csv(args.output_dir / "group_summary.csv", GROUP_FIELDS, groups)
    atomic_csv(
        args.output_dir / "m1_quote_contrasts.csv", CONTRAST_FIELDS, contrasts
    )
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
