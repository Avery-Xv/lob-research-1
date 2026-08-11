#!/usr/bin/env python3
"""Build and test continuous liquidity-weighted F018 composite scores."""

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
from scripts.backtests.analyze_f018_liquidity_conditioning import assign_liquidity_states
from scripts.backtests.backtest_non_parent_direct_targets import run_continuous, summarize
from scripts.backtests.backtest_order_shape_batch_a_domains import (
    mean_t,
    percentile_ranks,
    write_csv,
)
from scripts.factors.order_shape_non_parent.candidates import sha256


TARGETS = ("ret_1031_1035", "ret_1031_1040", "ret_1031_1100", "ret_1031_1500")
RAW_FACTOR = "f018_raw"
BASE_RANK_FACTOR = "f018_centered_rank"
PRIMARY_COMPOSITE = "f018_cl_linear"
FACTORS = (
    RAW_FACTOR,
    BASE_RANK_FACTOR,
    PRIMARY_COMPOSITE,
    "f018_cl_square",
    "f018_cl_floor25",
    "liquidity_quality_rank",
)


def add_continuous_composites(rows: list[dict[str, object]]) -> None:
    """Add continuous scores inside each date x frozen domain."""
    assign_liquidity_states(rows)
    grouped: dict[tuple[int, str], list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        grouped[(int(row["date"]), str(row["domain"]))].append(row)

    for group in grouped.values():
        base_rank = [
            2.0 * value - 1.0
            for value in percentile_ranks([float(row["f018"]) for row in group])
        ]
        liquidity_rank = percentile_ranks([
            float(row["liquidity_score"]) for row in group
        ])
        for index, row in enumerate(group):
            base = base_rank[index]
            liquidity = liquidity_rank[index]
            scores = {
                RAW_FACTOR: float(row["f018"]),
                BASE_RANK_FACTOR: base,
                PRIMARY_COMPOSITE: base * liquidity,
                "f018_cl_square": base * liquidity * liquidity,
                "f018_cl_floor25": base * (0.25 + 0.75 * liquidity),
                "liquidity_quality_rank": liquidity,
            }
            if not all(math.isfinite(value) for value in scores.values()):
                raise ValueError(f"non-finite composite score: {row['symbol']} {row['date']}")
            row["candidate"] = scores
            row["f018_centered_rank"] = base
            row["liquidity_quality_rank"] = liquidity


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
            "f018_centered_rank": candidate[BASE_RANK_FACTOR],  # type: ignore[index]
            "liquidity_quality_rank": candidate["liquidity_quality_rank"],  # type: ignore[index]
            "f018_cl_linear": candidate[PRIMARY_COMPOSITE],  # type: ignore[index]
            "f018_cl_square": candidate["f018_cl_square"],  # type: ignore[index]
            "f018_cl_floor25": candidate["f018_cl_floor25"],  # type: ignore[index]
        })
    return output


def comparison_details(performance: list[dict[str, object]]) -> list[dict[str, object]]:
    indexed: dict[tuple[str, str, int, str, str], dict[str, object]] = {}
    for row in performance:
        indexed[(
            str(row["scope"]), str(row["domain"]), int(row["date"]),
            str(row["target"]), str(row["factor"]),
        )] = row

    output: list[dict[str, object]] = []
    for key, composite in sorted(indexed.items()):
        scope, domain, date, target, factor = key
        if factor not in {PRIMARY_COMPOSITE, "f018_cl_square", "f018_cl_floor25"}:
            continue
        raw = indexed.get((scope, domain, date, target, RAW_FACTOR))
        if raw is None:
            continue
        record: dict[str, object] = {
            "scope": scope,
            "domain": domain,
            "date": date,
            "target": target,
            "factor": factor,
            "n": composite["n"],
        }
        for metric in ("rank_ic", "d10_d1"):
            raw_value = raw[metric]
            composite_value = composite[metric]
            record[f"raw_{metric}"] = raw_value
            record[f"composite_{metric}"] = composite_value
            record[f"delta_{metric}"] = (
                float(composite_value) - float(raw_value)
                if raw_value is not None and composite_value is not None else None
            )
        output.append(record)
    return output


def summarize_comparisons(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    grouped: dict[tuple[str, str, str, str], list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        grouped[(
            str(row["scope"]), str(row["domain"]),
            str(row["factor"]), str(row["target"]),
        )].append(row)
    output: list[dict[str, object]] = []
    for (scope, domain, factor, target), observations in sorted(grouped.items()):
        record: dict[str, object] = {
            "scope": scope,
            "domain": domain,
            "factor": factor,
            "target": target,
            "n_dates": len({int(row["date"]) for row in observations}),
            "n_obs": sum(int(row["n"]) for row in observations),
        }
        for metric in ("rank_ic", "d10_d1"):
            for prefix in ("raw", "composite", "delta"):
                column = f"{prefix}_{metric}"
                values = [
                    float(row[column]) for row in observations
                    if row[column] is not None
                ]
                record[column], record[f"{column}_t"] = mean_t(values)
        output.append(record)
    return output


def score_diagnostics(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    grouped: dict[tuple[int, str], list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        grouped[(int(row["date"]), str(row["domain"]))].append(row)
    output: list[dict[str, object]] = []
    for (date, domain), group in sorted(grouped.items()):
        liquidity = [float(row["liquidity_quality_rank"]) for row in group]
        base = [float(row["f018_centered_rank"]) for row in group]
        primary = [float(row["candidate"][PRIMARY_COMPOSITE]) for row in group]  # type: ignore[index]
        output.append({
            "date": date,
            "domain": domain,
            "n": len(group),
            "mean_liquidity_rank": mean(liquidity),
            "min_liquidity_rank": min(liquidity),
            "max_liquidity_rank": max(liquidity),
            "mean_base_score": mean(base),
            "mean_composite_score": mean(primary),
            "zero_composite_count": sum(value == 0.0 for value in primary),
        })
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
    add_continuous_composites(rows)
    scores = factor_score_rows(rows)
    diagnostics = score_diagnostics(rows)
    performance = run_continuous(rows, {"factors": FACTORS, "targets": TARGETS})
    performance_summary = summarize(performance)
    comparisons = comparison_details(performance)
    comparison_summary = summarize_comparisons(comparisons)

    args.output_dir.mkdir(parents=True)
    write_csv(args.output_dir / "factor_scores.csv", scores)
    write_csv(args.output_dir / "score_diagnostics_by_slice.csv", diagnostics)
    write_csv(args.output_dir / "performance_by_slice.csv", performance)
    write_csv(args.output_dir / "performance_summary.csv", performance_summary)
    write_csv(args.output_dir / "comparison_vs_raw_by_slice.csv", comparisons)
    write_csv(args.output_dir / "comparison_vs_raw_summary.csv", comparison_summary)
    manifest = {
        "kind": "research_result",
        "status": "completed",
        "research_id": "R017",
        "factor_id": "F018",
        "study": "f018_continuous_liquidity_composite",
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
        "primary_formula": "(2*within_date_domain_pct_rank(F018)-1) * within_date_domain_pct_rank(liquidity_quality)",
        "liquidity_quality": "equal-weight mean of tight-spread, total-depth, active-volume, and active-order-count percentile ranks",
        "robustness_formulas": {
            "square": "base * liquidity_rank^2",
            "floor25": "base * (0.25 + 0.75*liquidity_rank)",
        },
        "primary_decision_rule": (
            "the pre-specified linear composite becomes the candidate main version only if its "
            "10m or 30m domain-rank aggregate Rank IC exceeds raw F018 with positive paired "
            "delta and it does not reverse the other short horizon; D10-D1 and domains remain mandatory"
        ),
        "signal_cutoff": "10:30:00",
        "entry_rule": "10:31 minute close",
        "primary_scope": "raw non-neutralized frozen nine domains",
        "future_filter_used": False,
        "style_neutralization_used": False,
        "months": [202601],
        "rows": len(rows),
        "score_rows": len(scores),
        "performance_rows": len(performance),
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
