#!/usr/bin/env python3
"""Test point-in-time liquidity-state activation for F018."""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.backtests.analyze_f018_incremental_controls import load_rows, validate_json
from scripts.backtests.backtest_non_parent_direct_targets import run_continuous, summarize
from scripts.backtests.backtest_order_shape_batch_a_domains import (
    mean_t,
    percentile_ranks,
    write_csv,
)
from scripts.factors.order_shape_non_parent.candidates import sha256


TARGETS = ("ret_1031_1035", "ret_1031_1040", "ret_1031_1100", "ret_1031_1500")
FACTORS = ("f018_raw", "flow5_raw")
STATES = ("low", "mid", "high")


def assign_liquidity_states(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    """Assign equal-count liquidity states inside each date x frozen domain."""
    grouped: dict[tuple[int, str], list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        grouped[(int(row["date"]), str(row["domain"]))].append(row)

    diagnostics: list[dict[str, object]] = []
    for (date, domain), group in sorted(grouped.items()):
        spread = [float(row["controls"]["log_spread_5m_twap"]) for row in group]  # type: ignore[index]
        depth = [float(row["controls"]["log_depth3_5m_twap"]) for row in group]  # type: ignore[index]
        volume = [float(row["controls"]["log_active_volume_5m"]) for row in group]  # type: ignore[index]
        count = [float(row["controls"]["log_active_count_5m"]) for row in group]  # type: ignore[index]
        components = {
            "tightness_rank": [1.0 - value for value in percentile_ranks(spread)],
            "depth_rank": percentile_ranks(depth),
            "volume_rank": percentile_ranks(volume),
            "count_rank": percentile_ranks(count),
        }
        scores = [
            mean(components[name][index] for name in components)
            for index in range(len(group))
        ]
        order = sorted(
            range(len(group)),
            key=lambda index: (scores[index], str(group[index]["symbol"])),
        )
        state_by_index: dict[int, str] = {}
        for position, index in enumerate(order):
            bucket = min(2, 3 * position // len(order))
            state_by_index[index] = STATES[bucket]

        by_state: dict[str, list[int]] = defaultdict(list)
        for index, row in enumerate(group):
            state = state_by_index[index]
            row["liquidity_state"] = state
            row["liquidity_score"] = scores[index]
            row["liquidity_components"] = {
                name: values[index] for name, values in components.items()
            }
            row["candidate"] = {
                "f018_raw": float(row["f018"]),
                "flow5_raw": float(row["controls"]["flow5"]),  # type: ignore[index]
            }
            by_state[state].append(index)

        for state in STATES:
            indices = by_state[state]
            if not indices:
                continue
            diagnostics.append({
                "date": date,
                "domain": domain,
                "state": state,
                "n": len(indices),
                "coverage": len(indices) / len(group),
                "mean_liquidity_score": mean(scores[index] for index in indices),
                **{
                    f"mean_{name}": mean(values[index] for index in indices)
                    for name, values in components.items()
                },
            })
    return diagnostics


def run_conditioned(
    rows: list[dict[str, object]],
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    details: list[dict[str, object]] = []
    summaries: list[dict[str, object]] = []
    conditions = ("all", *STATES)
    for condition in conditions:
        subset = rows if condition == "all" else [
            row for row in rows if row["liquidity_state"] == condition
        ]
        performance = run_continuous(subset, {"factors": FACTORS, "targets": TARGETS})
        for row in performance:
            row["condition"] = condition
        details.extend(performance)
        summary = summarize(performance)
        for row in summary:
            row["condition"] = condition
        summaries.extend(summary)
    return details, summaries


def contrast_details(performance: list[dict[str, object]]) -> list[dict[str, object]]:
    indexed: dict[tuple[str, str, int, str, str, str], dict[str, object]] = {}
    for row in performance:
        indexed[(
            str(row["scope"]), str(row["domain"]), int(row["date"]),
            str(row["factor"]), str(row["target"]), str(row["condition"]),
        )] = row

    output: list[dict[str, object]] = []
    base_keys = {
        key[:5] for key in indexed if key[5] == "high"
    }
    for scope, domain, date, factor, target in sorted(base_keys):
        high = indexed[(scope, domain, date, factor, target, "high")]
        for reference in ("all", "low", "mid"):
            other = indexed.get((scope, domain, date, factor, target, reference))
            if other is None:
                continue
            record: dict[str, object] = {
                "scope": scope,
                "domain": domain,
                "date": date,
                "factor": factor,
                "target": target,
                "contrast": f"high_minus_{reference}",
                "high_n": high["n"],
                "reference_n": other["n"],
            }
            for metric in ("rank_ic", "d10_d1"):
                high_value = high[metric]
                reference_value = other[metric]
                record[f"high_{metric}"] = high_value
                record[f"reference_{metric}"] = reference_value
                record[f"delta_{metric}"] = (
                    float(high_value) - float(reference_value)
                    if high_value is not None and reference_value is not None else None
                )
            output.append(record)
    return output


def summarize_contrasts(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    grouped: dict[tuple[str, str, str, str, str], list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        grouped[(
            str(row["scope"]), str(row["domain"]), str(row["factor"]),
            str(row["target"]), str(row["contrast"]),
        )].append(row)
    output: list[dict[str, object]] = []
    for (scope, domain, factor, target, contrast), observations in sorted(grouped.items()):
        record: dict[str, object] = {
            "scope": scope,
            "domain": domain,
            "factor": factor,
            "target": target,
            "contrast": contrast,
            "n_dates": len({int(row["date"]) for row in observations}),
            "high_n_obs": sum(int(row["high_n"]) for row in observations),
            "reference_n_obs": sum(int(row["reference_n"]) for row in observations),
        }
        for metric in ("rank_ic", "d10_d1"):
            for prefix in ("high", "reference", "delta"):
                column = f"{prefix}_{metric}"
                values = [
                    float(row[column]) for row in observations
                    if row[column] is not None
                ]
                record[column], record[f"{column}_t"] = mean_t(values)
        output.append(record)
    return output


def summarize_state_diagnostics(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    grouped: dict[tuple[str, str], list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        grouped[(str(row["domain"]), str(row["state"]))].append(row)
    output: list[dict[str, object]] = []
    numeric = (
        "coverage", "mean_liquidity_score", "mean_tightness_rank",
        "mean_depth_rank", "mean_volume_rank", "mean_count_rank",
    )
    for (domain, state), observations in sorted(grouped.items()):
        record: dict[str, object] = {
            "domain": domain,
            "state": state,
            "n_dates": len({int(row["date"]) for row in observations}),
            "n_obs": sum(int(row["n"]) for row in observations),
        }
        for column in numeric:
            record[column] = mean(float(row[column]) for row in observations)
        output.append(record)
    return output


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--research-run", type=Path, required=True)
    parser.add_argument("--window-completion", type=Path, required=True)
    parser.add_argument("--candidate-completion", type=Path, required=True)
    parser.add_argument("--factor-spec", type=Path, required=True)
    parser.add_argument("--window-parquet", type=Path, required=True)
    parser.add_argument("--candidates", type=Path, required=True)
    parser.add_argument("--return-cache-manifest", type=Path, required=True)
    parser.add_argument("--return-prices", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    if args.output_dir.exists():
        raise SystemExit(f"Refusing to overwrite output: {args.output_dir}")

    run = validate_json(args.research_run, kind="research_run")
    if run.get("research_id") != "R017" or run.get("factor_id") != "F018":
        raise ValueError("research run is not R017/F018")
    for completion_path in (args.window_completion, args.candidate_completion):
        completion = validate_json(completion_path, kind="factor_run_completion")
        if completion.get("status") != "completed_audited" or completion.get("factor_id") != "F014":
            raise ValueError(f"not completed_audited F014: {completion_path}")
    factor_spec = validate_json(args.factor_spec)
    if factor_spec.get("factor_id") != "F018":
        raise ValueError("factor spec is not F018")
    cache = validate_json(args.return_cache_manifest, kind="research_label_cache")
    if sha256(args.return_prices) != cache.get("output_sha256"):
        raise ValueError("return price cache hash mismatch")

    rows = load_rows(args.window_parquet, args.candidates, args.return_prices)
    diagnostics = assign_liquidity_states(rows)
    performance, performance_summary = run_conditioned(rows)
    contrasts = contrast_details(performance)
    contrast_summary = summarize_contrasts(contrasts)
    diagnostic_summary = summarize_state_diagnostics(diagnostics)

    args.output_dir.mkdir(parents=True)
    write_csv(args.output_dir / "performance_by_slice.csv", performance)
    write_csv(args.output_dir / "performance_summary.csv", performance_summary)
    write_csv(args.output_dir / "liquidity_state_by_slice.csv", diagnostics)
    write_csv(args.output_dir / "liquidity_state_summary.csv", diagnostic_summary)
    write_csv(args.output_dir / "high_state_contrasts_by_slice.csv", contrasts)
    write_csv(args.output_dir / "high_state_contrasts_summary.csv", contrast_summary)
    manifest = {
        "kind": "research_result",
        "status": "completed",
        "research_id": "R017",
        "factor_id": "F018",
        "study": "f018_liquidity_conditioning",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "research_run": str(args.research_run.resolve()),
        "research_run_sha256": sha256(args.research_run),
        "implementation": str(Path(__file__).resolve()),
        "implementation_sha256": sha256(Path(__file__).resolve()),
        "window_completion": str(args.window_completion.resolve()),
        "window_completion_sha256": sha256(args.window_completion),
        "candidate_completion": str(args.candidate_completion.resolve()),
        "candidate_completion_sha256": sha256(args.candidate_completion),
        "factor_spec": str(args.factor_spec.resolve()),
        "factor_spec_sha256": sha256(args.factor_spec),
        "window_parquet": str(args.window_parquet.resolve()),
        "window_parquet_sha256": sha256(args.window_parquet),
        "candidates": str(args.candidates.resolve()),
        "candidates_sha256": sha256(args.candidates),
        "return_cache_manifest": str(args.return_cache_manifest.resolve()),
        "return_cache_manifest_sha256": sha256(args.return_cache_manifest),
        "gate_definition": {
            "partition": "within date x frozen nine-domain",
            "components": [
                "negative percentile rank of log 5m TWAP spread",
                "percentile rank of log 5m TWAP bid3+ask3 depth",
                "percentile rank of log 5m active volume",
                "percentile rank of log 5m active-order count",
            ],
            "score": "equal-weight mean of four component ranks",
            "states": "deterministic equal-count terciles; high is pre-specified enabled state",
        },
        "primary_decision_rule": (
            "support high-liquidity activation only if 10m or 30m high-state F018 Rank IC "
            "exceeds both all-state and low-state with paired high-minus-low t>=2, without "
            "opposite sign at the other short horizon; D10-D1 is secondary"
        ),
        "benchmarks": ["all-state F018", "low/mid state F018", "same-state raw Flow5"],
        "signal_cutoff": "10:30:00",
        "entry_rule": "10:31 minute close",
        "primary_scope": "raw non-neutralized; all frozen nine domains reported",
        "future_filter_used": False,
        "style_neutralization_used": False,
        "months": [202601],
        "rows": len(rows),
        "performance_rows": len(performance),
        "contrast_rows": len(contrasts),
    }
    (args.output_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({
        "research_id": "R017",
        "factor_id": "F018",
        "rows": len(rows),
        "output": str(args.output_dir),
    }))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
