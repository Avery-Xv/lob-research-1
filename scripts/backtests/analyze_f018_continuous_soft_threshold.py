#!/usr/bin/env python3
"""Test pre-registered continuous soft-threshold variants of F018."""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.backtests.analyze_f018_continuous_liquidity_composite import (
    PRIMARY_COMPOSITE as LINEAR_FACTOR,
    RAW_FACTOR,
    TARGETS,
    add_continuous_composites,
)
from scripts.backtests.analyze_f018_incremental_controls import load_rows, validate_json
from scripts.backtests.backtest_non_parent_direct_targets import run_continuous, summarize
from scripts.backtests.backtest_order_shape_batch_a_domains import mean_t, write_csv
from scripts.factors.order_shape_non_parent.candidates import sha256


LOWER_LIQUIDITY_RANK = 1.0 / 3.0
UPPER_LIQUIDITY_RANK = 1.0
HINGE_FACTOR = "f018_cl_soft_hinge"
SMOOTHSTEP_FACTOR = "f018_cl_soft_smoothstep"
FACTORS = (RAW_FACTOR, LINEAR_FACTOR, HINGE_FACTOR, SMOOTHSTEP_FACTOR)
BENCHMARKS = (RAW_FACTOR, LINEAR_FACTOR)


def threshold_position(liquidity_rank: float) -> float:
    """Map liquidity rank to [0, 1] using the frozen one-third hinge."""
    scaled = ((liquidity_rank - LOWER_LIQUIDITY_RANK) /
              (UPPER_LIQUIDITY_RANK - LOWER_LIQUIDITY_RANK))
    return min(1.0, max(0.0, scaled))


def soft_weights(liquidity_rank: float) -> tuple[float, float]:
    """Return linear-hinge and smoothstep weights at the same thresholds."""
    position = threshold_position(liquidity_rank)
    return position, position * position * (3.0 - 2.0 * position)


def add_soft_threshold_scores(rows: list[dict[str, object]]) -> None:
    """Add soft-threshold scores without reading any target values."""
    add_continuous_composites(rows)
    for row in rows:
        base = float(row["f018_centered_rank"])
        liquidity = float(row["liquidity_quality_rank"])
        hinge_weight, smoothstep_weight = soft_weights(liquidity)
        candidate = row["candidate"]
        candidate[HINGE_FACTOR] = base * hinge_weight  # type: ignore[index]
        candidate[SMOOTHSTEP_FACTOR] = base * smoothstep_weight  # type: ignore[index]
        row["soft_hinge_weight"] = hinge_weight
        row["soft_smoothstep_weight"] = smoothstep_weight
        if not all(math.isfinite(float(candidate[factor])) for factor in FACTORS):  # type: ignore[index]
            raise ValueError(f"non-finite soft-threshold score: {row['symbol']} {row['date']}")


def factor_score_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    output: list[dict[str, object]] = []
    for row in rows:
        candidate = row["candidate"]
        output.append({
            "symbol": row["symbol"],
            "date": row["date"],
            "signal_time": 1030,
            "exchange": row["exchange"],
            "domain": row["domain"],
            "f018_raw": candidate[RAW_FACTOR],  # type: ignore[index]
            "f018_centered_rank": row["f018_centered_rank"],
            "liquidity_quality_rank": row["liquidity_quality_rank"],
            "linear_weight": row["liquidity_quality_rank"],
            "soft_hinge_weight": row["soft_hinge_weight"],
            "soft_smoothstep_weight": row["soft_smoothstep_weight"],
            LINEAR_FACTOR: candidate[LINEAR_FACTOR],  # type: ignore[index]
            HINGE_FACTOR: candidate[HINGE_FACTOR],  # type: ignore[index]
            SMOOTHSTEP_FACTOR: candidate[SMOOTHSTEP_FACTOR],  # type: ignore[index]
        })
    return output


def comparison_details(performance: list[dict[str, object]]) -> list[dict[str, object]]:
    indexed: dict[tuple[str, str, int, str, str], dict[str, object]] = {}
    for row in performance:
        indexed[(str(row["scope"]), str(row["domain"]), int(row["date"]),
                 str(row["target"]), str(row["factor"]))] = row
    output: list[dict[str, object]] = []
    for (scope, domain, date, target, factor), tested in sorted(indexed.items()):
        if factor not in (HINGE_FACTOR, SMOOTHSTEP_FACTOR):
            continue
        for benchmark_factor in BENCHMARKS:
            benchmark = indexed.get((scope, domain, date, target, benchmark_factor))
            if benchmark is None:
                continue
            record: dict[str, object] = {
                "scope": scope, "domain": domain, "date": date,
                "target": target, "factor": factor,
                "benchmark_factor": benchmark_factor, "n": tested["n"],
            }
            for metric in ("rank_ic", "d10_d1"):
                benchmark_value = benchmark[metric]
                tested_value = tested[metric]
                record[f"benchmark_{metric}"] = benchmark_value
                record[f"tested_{metric}"] = tested_value
                record[f"delta_{metric}"] = (
                    float(tested_value) - float(benchmark_value)
                    if benchmark_value is not None and tested_value is not None else None
                )
            output.append(record)
    return output


def summarize_comparisons(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    grouped: dict[tuple[str, str, str, str, str], list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        grouped[(str(row["scope"]), str(row["domain"]), str(row["factor"]),
                 str(row["benchmark_factor"]), str(row["target"]))].append(row)
    output: list[dict[str, object]] = []
    for (scope, domain, factor, benchmark, target), observations in sorted(grouped.items()):
        record: dict[str, object] = {
            "scope": scope, "domain": domain, "factor": factor,
            "benchmark_factor": benchmark, "target": target,
            "n_dates": len({int(row["date"]) for row in observations}),
            "n_obs": sum(int(row["n"]) for row in observations),
        }
        for metric in ("rank_ic", "d10_d1"):
            for prefix in ("benchmark", "tested", "delta"):
                column = f"{prefix}_{metric}"
                values = [float(row[column]) for row in observations if row[column] is not None]
                record[column], record[f"{column}_t"] = mean_t(values)
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
    add_soft_threshold_scores(rows)
    scores = factor_score_rows(rows)
    performance = run_continuous(rows, {"factors": FACTORS, "targets": TARGETS})
    comparisons = comparison_details(performance)

    args.output_dir.mkdir(parents=True)
    write_csv(args.output_dir / "factor_scores.csv", scores)
    write_csv(args.output_dir / "performance_by_slice.csv", performance)
    write_csv(args.output_dir / "performance_summary.csv", summarize(performance))
    write_csv(args.output_dir / "comparison_by_slice.csv", comparisons)
    write_csv(args.output_dir / "comparison_summary.csv", summarize_comparisons(comparisons))
    manifest = {
        "kind": "research_result", "status": "completed",
        "research_id": "R017", "factor_id": "F018",
        "study": "f018_continuous_soft_threshold", "created_at": datetime.now(timezone.utc).isoformat(),
        "research_run": str(args.research_run.resolve()), "research_run_sha256": sha256(args.research_run),
        "implementation": str(Path(__file__).resolve()), "implementation_sha256": sha256(Path(__file__).resolve()),
        "window_completion": str(args.window_completion.resolve()), "window_completion_sha256": sha256(args.window_completion),
        "candidate_completion": str(args.candidate_completion.resolve()), "candidate_completion_sha256": sha256(args.candidate_completion),
        "factor_spec": str(args.factor_spec.resolve()), "factor_spec_sha256": sha256(args.factor_spec),
        "window_parquet": str(args.window_parquet.resolve()), "window_parquet_sha256": sha256(args.window_parquet),
        "candidates": str(args.candidates.resolve()), "candidates_sha256": sha256(args.candidates),
        "return_cache_manifest": str(args.return_cache_manifest.resolve()),
        "return_cache_manifest_sha256": sha256(args.return_cache_manifest),
        "primary_formula": "B * clip((L-1/3)/(2/3),0,1)",
        "robustness_formula": "B * smoothstep(clip((L-1/3)/(2/3),0,1))",
        "thresholds_pre_registered": {"lower_liquidity_rank": LOWER_LIQUIDITY_RANK, "upper_liquidity_rank": UPPER_LIQUIDITY_RANK},
        "primary_decision_rule": (
            "prefer the hinge over B*L only if 10m or 30m aggregate Rank IC or D10-D1 improves "
            "with a positive paired delta and the other short horizon does not reverse; report all domains"
        ),
        "signal_cutoff": "10:30:00", "entry_rule": "10:31 minute close",
        "primary_scope": "raw non-neutralized frozen nine domains",
        "future_filter_used": False, "style_neutralization_used": False,
        "months": [202601], "rows": len(rows), "score_rows": len(scores),
        "performance_rows": len(performance),
    }
    (args.output_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps({"research_id": "R017", "factor_id": "F018", "rows": len(rows), "output": str(args.output_dir)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
