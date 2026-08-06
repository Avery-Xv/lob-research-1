#!/usr/bin/env python3
"""Conditional incremental tests for Batch A M1-chain and M6 signals."""

from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import defaultdict
from pathlib import Path
from statistics import mean


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.backtests.backtest_order_shape_batch_a_domains import (
    DIAGNOSTIC_STYLE_COLS,
    STYLE_COLS,
    load_domains,
    load_observations,
    load_previous_styles,
    mean_t,
    metric_row,
    pearson,
    percentile_ranks,
    ranks,
    residualize,
    validate_source_manifest,
    write_csv,
)


STYLE_SPEC = "LOB4-ex-size-and-nonlinear-size"
CANDIDATES = ("chain_flow", "single_chain_confirmation", "execution_pressure")
SPECIFICATIONS = ("styles_only", "m1_linear", "m1_cubic")
TARGET = "future_net_share"


def control_matrix(rows: list[dict[str, object]], specification: str) -> list[list[float]]:
    matrix: list[list[float]] = []
    for row in rows:
        diagnostics = [float(value) for value in row["styles"]]  # type: ignore[union-attr]
        controls = diagnostics[2:]
        if specification in {"m1_linear", "m1_cubic"}:
            active = float(row["factors"]["active_flow"])  # type: ignore[index]
            controls = [*controls, active]
            if specification == "m1_cubic":
                controls.extend((active * active, active * active * active))
        matrix.append(controls)
    return matrix


def score_candidate(
    rows: list[dict[str, object]], candidate: str, specification: str
) -> tuple[list[float], float | None]:
    values = [float(row["factors"][candidate]) for row in rows]  # type: ignore[index]
    residual = residualize(values, control_matrix(rows, specification))
    centered = [value - mean(values) for value in values]
    total_variance = sum(value * value for value in centered)
    residual_variance = sum(value * value for value in residual)
    explained_r2 = 1.0 - residual_variance / total_variance if total_variance > 0 else None
    return residual, explained_r2


def evaluate_scope(
    *, scope: str, domain: str, date: int, signal_time: int,
    rows: list[dict[str, object]], performance: list[dict[str, object]],
    exposures: list[dict[str, object]],
) -> dict[str, dict[str, list[float]]]:
    output: dict[str, dict[str, list[float]]] = defaultdict(dict)
    if len(rows) < 15:
        return output
    for candidate in CANDIDATES:
        raw = [float(row["factors"][candidate]) for row in rows]  # type: ignore[index]
        for specification in SPECIFICATIONS:
            scores, explained_r2 = score_candidate(rows, candidate, specification)
            output[candidate][specification] = scores
            result = metric_row(
                scope=scope, domain=domain, date=date, signal_time=signal_time,
                factor=candidate, target=TARGET, rows=rows,
                raw_scores=raw, neutral_scores=scores,
            )
            if result is not None:
                result.update({
                    "specification": specification,
                    "rank_ic": result.pop("neutral_rank_ic"),
                    "d10_d1": result.pop("neutral_d10_d1"),
                    "candidate_raw_rank_ic": result.pop("raw_rank_ic"),
                    "candidate_raw_d10_d1": result.pop("raw_d10_d1"),
                    "explained_r2": explained_r2,
                })
                performance.append(result)
            score_ranks = ranks(scores)
            diagnostic_styles = [row["styles"] for row in rows]  # type: ignore[misc]
            exposure: dict[str, object] = {
                "scope": scope, "domain": domain, "date": date,
                "signal_time": signal_time, "factor": candidate,
                "specification": specification, "n": len(rows),
                "explained_r2": explained_r2,
                "active_flow_rank_exposure": pearson(
                    score_ranks,
                    ranks([float(row["factors"]["active_flow"]) for row in rows]),  # type: ignore[index]
                ),
            }
            for index, style in enumerate(DIAGNOSTIC_STYLE_COLS):
                exposure[f"{style}_rank_exposure"] = pearson(
                    score_ranks,
                    ranks([float(row[index]) for row in diagnostic_styles]),
                )
            exposures.append(exposure)
    return output


def run_incremental(
    slices: dict[tuple[int, int], list[dict[str, object]]]
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    performance: list[dict[str, object]] = []
    exposures: list[dict[str, object]] = []
    for (date, signal_time), slice_rows in sorted(slices.items()):
        by_domain: dict[str, list[dict[str, object]]] = defaultdict(list)
        for row in slice_rows:
            by_domain[str(row["domain"])].append(row)
        domain_scores: dict[str, dict[str, dict[str, list[float]]]] = {}
        for domain, rows in sorted(by_domain.items()):
            scores = evaluate_scope(
                scope="domain", domain=domain, date=date, signal_time=signal_time,
                rows=rows, performance=performance, exposures=exposures,
            )
            if scores:
                domain_scores[domain] = scores

        for candidate in CANDIDATES:
            for specification in SPECIFICATIONS:
                pooled_rows: list[dict[str, object]] = []
                pooled_scores: list[float] = []
                for domain in sorted(domain_scores):
                    scores = domain_scores[domain].get(candidate, {}).get(specification)
                    if scores is None:
                        continue
                    rows = by_domain[domain]
                    pooled_rows.extend(rows)
                    pooled_scores.extend(percentile_ranks(scores))
                if len(pooled_rows) < 20:
                    continue
                result = metric_row(
                    scope="domain_neutral_aggregate", domain="domain_neutral",
                    date=date, signal_time=signal_time, factor=candidate, target=TARGET,
                    rows=pooled_rows, raw_scores=pooled_scores, neutral_scores=pooled_scores,
                )
                if result is not None:
                    result.pop("raw_rank_ic")
                    result.pop("raw_d10_d1")
                    result.update({
                        "specification": specification,
                        "rank_ic": result.pop("neutral_rank_ic"),
                        "d10_d1": result.pop("neutral_d10_d1"),
                        "candidate_raw_rank_ic": None,
                        "candidate_raw_d10_d1": None,
                        "explained_r2": None,
                    })
                    performance.append(result)

        evaluate_scope(
            scope="all_market", domain="all/all", date=date,
            signal_time=signal_time, rows=slice_rows,
            performance=performance, exposures=exposures,
        )
    return performance, exposures


def summarize_performance(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    daily: dict[tuple[str, str, str, str, int], list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        key = (
            str(row["scope"]), str(row["domain"]), str(row["factor"]),
            str(row["specification"]), int(row["date"]),
        )
        daily[key].append(row)
    grouped: dict[tuple[str, str, str, str], list[dict[str, object]]] = defaultdict(list)
    for key, observations in daily.items():
        record: dict[str, object] = {
            "date": key[4], "n": sum(int(row["n"]) for row in observations),
            "avg_names": mean(int(row["n"]) for row in observations),
        }
        for metric in ("rank_ic", "d10_d1", "explained_r2"):
            values = [float(row[metric]) for row in observations if row[metric] is not None]
            record[metric] = mean(values) if values else None
        grouped[key[:4]].append(record)
    output: list[dict[str, object]] = []
    for key, observations in sorted(grouped.items()):
        result: dict[str, object] = {
            "scope": key[0], "domain": key[1], "factor": key[2],
            "specification": key[3], "target": TARGET,
            "n_dates": len(observations),
            "n_obs": sum(int(row["n"]) for row in observations),
            "avg_names": mean(float(row["avg_names"]) for row in observations),
        }
        for metric in ("rank_ic", "d10_d1", "explained_r2"):
            values = [float(row[metric]) for row in observations if row[metric] is not None]
            result[metric], result[f"{metric}_t"] = mean_t(values)
            if metric != "explained_r2":
                result[f"{metric}_positive_date_share"] = (
                    sum(value > 0 for value in values) / len(values) if values else None
                )
        output.append(result)
    return output


def summarize_exposures(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    metrics = (
        "explained_r2", "active_flow_rank_exposure",
        *(f"{style}_rank_exposure" for style in DIAGNOSTIC_STYLE_COLS),
    )
    daily: dict[tuple[str, str, str, str, int], list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        key = (
            str(row["scope"]), str(row["domain"]), str(row["factor"]),
            str(row["specification"]), int(row["date"]),
        )
        daily[key].append(row)
    grouped: dict[tuple[str, str, str, str], list[dict[str, object]]] = defaultdict(list)
    for key, observations in daily.items():
        record: dict[str, object] = {"date": key[4], "n_slices": len(observations)}
        for metric in metrics:
            values = [float(row[metric]) for row in observations if row[metric] is not None]
            record[metric] = mean(values) if values else None
            record[f"{metric}_mean_abs"] = mean(map(abs, values)) if values else None
        grouped[key[:4]].append(record)
    output: list[dict[str, object]] = []
    for key, observations in sorted(grouped.items()):
        result: dict[str, object] = {
            "scope": key[0], "domain": key[1], "factor": key[2],
            "specification": key[3], "n_dates": len(observations),
            "n_slices": sum(int(row["n_slices"]) for row in observations),
        }
        for metric in metrics:
            values = [float(row[metric]) for row in observations if row[metric] is not None]
            result[metric], result[f"{metric}_t"] = mean_t(values)
            absolute = [float(row[f"{metric}_mean_abs"]) for row in observations
                        if row[f"{metric}_mean_abs"] is not None]
            result[f"{metric}_mean_abs"] = mean(absolute) if absolute else None
        output.append(result)
    return output


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--shard-dir", type=Path, required=True)
    parser.add_argument("--domain-file", type=Path, required=True)
    parser.add_argument("--styles", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    source_manifest = validate_source_manifest(args.shard_dir)
    observations = load_observations(
        args.shard_dir, load_previous_styles(args.styles), load_domains(args.domain_file)
    )
    performance, exposures = run_incremental(observations)
    write_csv(args.output_dir / "performance_by_slice.csv", performance)
    write_csv(args.output_dir / "performance_summary.csv", summarize_performance(performance))
    write_csv(args.output_dir / "exposure_by_slice.csv", exposures)
    write_csv(args.output_dir / "exposure_summary.csv", summarize_exposures(exposures))
    manifest = {
        "experiment": "Batch A conditional incremental test",
        "style_specification": STYLE_SPEC,
        "style_columns": list(STYLE_COLS),
        "explicitly_excluded_styles": ["size", "non_linear_size"],
        "diagnostic_style_columns": list(DIAGNOSTIC_STYLE_COLS),
        "candidates": list(CANDIDATES),
        "specifications": {
            "styles_only": "candidate ~ four styles",
            "m1_linear": "candidate ~ four styles + active_flow",
            "m1_cubic": "candidate ~ four styles + active_flow + active_flow^2 + active_flow^3",
        },
        "target": TARGET,
        "factor_rule": "(t-60s,t)", "label_rule": "[t,t+10m)",
        "exposure_timing": "previous trading day",
        "domain_rule": "frozen prior-day cap x price/board nine domains",
        "aggregation": "average 21 slices within date before t statistic",
        "source_manifest_fingerprint": source_manifest.get("fingerprint"),
        "source_output_etf_symbols": source_manifest["config"].get("output_etf_symbols"),
        "source_paths": {
            "shard_dir": str(args.shard_dir.resolve()),
            "domain_file": str(args.domain_file.resolve()),
            "styles": str(args.styles.resolve()),
        },
        "slices": len(observations), "performance_rows": len(performance),
        "exposure_rows": len(exposures),
    }
    args.output_dir.mkdir(parents=True, exist_ok=True)
    (args.output_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n"
    )
    print(
        f"slices={len(observations)} performance={len(performance)} "
        f"exposures={len(exposures)} output={args.output_dir}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
