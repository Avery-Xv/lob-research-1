#!/usr/bin/env python3
"""Summarize symmetric M1 chain pre/post flow comparisons."""

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
    post_variant: str,
    pre_variant: str,
    group_name: str,
    predicate: Callable[[str], bool],
) -> None:
    post_values = daily_values(rows, "M1PP", post_variant, predicate)
    pre_values = daily_values(rows, "M1PP", pre_variant, predicate)
    domains = sorted({key[0] for key in post_values} | {key[0] for key in pre_values})
    for domain in domains:
        keys = sorted(
            {key for key in post_values if key[0] == domain}
            & {key for key in pre_values if key[0] == domain}
        )
        differences = [post_values[key] - pre_values[key] for key in keys]
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
                "mechanism": "M1PP",
                "contrast": contrast,
                "left_group": f"post/{group_name}",
                "right_group": f"pre/{group_name}",
                "stock_days": len(keys),
                "left_mean": (
                    sum(post_values[key] for key in keys) / len(keys) if keys else None
                ),
                "right_mean": (
                    sum(pre_values[key] for key in keys) / len(keys) if keys else None
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
    groups: tuple[tuple[str, Callable[[str], bool]], ...] = (
        ("all", lambda group: "chain=all" in group),
        ("single", lambda group: "chain=single" in group),
        ("multi", lambda group: "chain=multi" in group),
        ("buy", lambda group: group == "trigger=B|chain=all"),
        ("sell", lambda group: group == "trigger=S|chain=all"),
    )
    for clock in ("lob", "trade"):
        for horizon in (20, 50):
            prefix = f"{clock}{horizon}_chain"
            for metric in ("signed_volume", "total_volume", "signed_share"):
                for group_name, predicate in groups:
                    add_contrast(
                        output,
                        rows,
                        f"{clock}{horizon}_{group_name}_post_minus_pre_{metric}",
                        f"{prefix}_post_{metric}",
                        f"{prefix}_pre_{metric}",
                        group_name,
                        predicate,
                    )
    return output


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Summarize M1 pre/post shards.")
    parser.add_argument("--shard-dir", type=Path, required=True)
    parser.add_argument("--domain-file", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    rows = attach_domains(load_stats(args.shard_dir), load_domains(args.domain_file))
    groups = group_summary(rows)
    contrasts = build_contrasts(rows)
    atomic_csv(args.output_dir / "group_summary.csv", GROUP_FIELDS, groups)
    atomic_csv(args.output_dir / "m1_prepost_contrasts.csv", CONTRAST_FIELDS, contrasts)
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
