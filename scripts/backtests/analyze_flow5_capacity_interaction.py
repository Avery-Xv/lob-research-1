#!/usr/bin/env python3
"""Quick R017 test of Flow5 x dynamic opponent-depth replenishment."""

from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean

from scripts.backtests.analyze_non_parent_state_returns import load_rows, validate_json
from scripts.backtests.backtest_non_parent_direct_targets import run_continuous, summarize
from scripts.backtests.backtest_order_shape_batch_a_domains import mean_t, percentile_ranks, write_csv
from scripts.factors.order_shape_non_parent.candidates import residualize, sha256


TARGETS = ("ret_1031_1035", "ret_1031_1040", "ret_1031_1100")
FACTORS = (
    "flow5_raw",
    "opponent_replenishment",
    "flow5_x_capacity_raw",
    "flow5_x_capacity_rank",
    "flow5_x_capacity_incremental",
)


def centered_ranks(values: list[float]) -> list[float]:
    return [2.0 * value - 1.0 for value in percentile_ranks(values)]


def add_interaction_candidates(rows: list[dict[str, object]]) -> None:
    """Construct interaction within each date x frozen domain.

    Capacity is the log ratio of the same-side opponent depth over the latest
    five minutes to its full 30-minute average: ask depth for buy flow and bid
    depth for sell flow.  All inputs end at the 10:30 signal cutoff.
    """
    grouped: dict[tuple[int, str], list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        grouped[(int(row["date"]), str(row["domain"]))].append(row)

    for group in grouped.values():
        flows = [float(row["flow5"]) for row in group]
        capacities: list[float] = []
        for row in group:
            if float(row["flow5"]) >= 0.0:
                recent = float(row["ask5"]); baseline = float(row["ask30"])
            else:
                recent = float(row["bid5"]); baseline = float(row["bid30"])
            capacities.append(math.log(max(recent, 1e-12) / max(baseline, 1e-12)))

        flow_ranks = centered_ranks(flows)
        capacity_ranks = centered_ranks(capacities)
        rank_products = [flow * capacity for flow, capacity in zip(flow_ranks, capacity_ranks)]
        incremental = residualize(
            rank_products,
            [[flow, capacity] for flow, capacity in zip(flow_ranks, capacity_ranks)],
        )
        for index, row in enumerate(group):
            row["interaction_flow_rank"] = flow_ranks[index]
            row["interaction_capacity_rank"] = capacity_ranks[index]
            row["candidate"] = {
                "flow5_raw": flows[index],
                "opponent_replenishment": capacities[index],
                "flow5_x_capacity_raw": flows[index] * capacities[index],
                "flow5_x_capacity_rank": rank_products[index],
                "flow5_x_capacity_incremental": incremental[index],
            }


def regression_detail(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    """Daily FWL coefficient after controlling ranked Flow5 and capacity."""
    by_date_domain: dict[tuple[int, str], list[dict[str, object]]] = defaultdict(list)
    by_date: dict[int, list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        by_date_domain[(int(row["date"]), str(row["domain"]))].append(row)
        by_date[int(row["date"])].append(row)

    output: list[dict[str, object]] = []
    groups = [
        ("domain", domain, date, observations)
        for (date, domain), observations in sorted(by_date_domain.items())
    ] + [
        ("domain_rank_aggregate", "all_nine_domains", date, observations)
        for date, observations in sorted(by_date.items())
    ]
    for scope, domain, date, observations in groups:
        for target in TARGETS:
            x = [float(row["candidate"]["flow5_x_capacity_incremental"]) for row in observations]  # type: ignore[index]
            y = [float(row["targets"][target]) for row in observations]  # type: ignore[index]
            denominator = sum(value * value for value in x)
            if denominator <= 1e-12:
                continue
            beta = sum(left * right for left, right in zip(x, y)) / denominator
            output.append({
                "scope": scope, "domain": domain, "date": date, "signal_time": 1030,
                "target": target, "n": len(observations), "interaction_beta": beta,
            })
    return output


def summarize_regression(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    grouped: dict[tuple[str, str, str], list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        grouped[(str(row["scope"]), str(row["domain"]), str(row["target"]))].append(row)
    output: list[dict[str, object]] = []
    for (scope, domain, target), observations in sorted(grouped.items()):
        values = [float(row["interaction_beta"]) for row in observations]
        average, t_value = mean_t(values)
        output.append({
            "scope": scope, "domain": domain, "target": target,
            "n_dates": len(values), "n_obs": sum(int(row["n"]) for row in observations),
            "interaction_beta": average, "interaction_beta_t": t_value,
            "positive_date_share": sum(value > 0.0 for value in values) / len(values),
        })
    return output


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--research-run", type=Path, required=True)
    parser.add_argument("--window-completion", type=Path, required=True)
    parser.add_argument("--candidate-completion", type=Path, required=True)
    parser.add_argument("--window-parquet", type=Path, required=True)
    parser.add_argument("--candidates", type=Path, required=True)
    parser.add_argument("--return-cache-manifest", type=Path, required=True)
    parser.add_argument("--return-prices", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    if args.output_dir.exists():
        raise SystemExit(f"Refusing to overwrite output: {args.output_dir}")

    run = validate_json(args.research_run, kind="research_run")
    if run.get("research_id") != "R017":
        raise ValueError("research run is not R017")
    for completion_path in (args.window_completion, args.candidate_completion):
        completion = validate_json(completion_path, kind="factor_run_completion")
        if completion.get("status") != "completed_audited" or completion.get("factor_id") != "F014":
            raise ValueError(f"not completed_audited F014: {completion_path}")
    cache = validate_json(args.return_cache_manifest, kind="research_label_cache")
    if sha256(args.return_prices) != cache.get("output_sha256"):
        raise ValueError("return price cache hash mismatch")

    rows = load_rows(args.window_parquet, args.candidates, args.return_prices)
    add_interaction_candidates(rows)
    performance = run_continuous(rows, {"factors": FACTORS, "targets": TARGETS})
    performance_summary = summarize(performance)
    coefficients = regression_detail(rows)
    coefficient_summary = summarize_regression(coefficients)

    args.output_dir.mkdir(parents=True)
    write_csv(args.output_dir / "performance_by_slice.csv", performance)
    write_csv(args.output_dir / "performance_summary.csv", performance_summary)
    write_csv(args.output_dir / "interaction_coefficients_by_slice.csv", coefficients)
    write_csv(args.output_dir / "interaction_coefficients_summary.csv", coefficient_summary)
    manifest = {
        "kind": "research_result", "status": "completed", "research_id": "R017",
        "study": "flow5_capacity_interaction_returns", "created_at": datetime.now(timezone.utc).isoformat(),
        "research_run": str(args.research_run.resolve()), "research_run_sha256": sha256(args.research_run),
        "implementation": str(Path(__file__).resolve()), "implementation_sha256": sha256(Path(__file__).resolve()),
        "window_completion": str(args.window_completion.resolve()), "window_completion_sha256": sha256(args.window_completion),
        "candidate_completion": str(args.candidate_completion.resolve()), "candidate_completion_sha256": sha256(args.candidate_completion),
        "window_parquet": str(args.window_parquet.resolve()), "window_parquet_sha256": sha256(args.window_parquet),
        "candidates": str(args.candidates.resolve()), "candidates_sha256": sha256(args.candidates),
        "return_cache_manifest": str(args.return_cache_manifest.resolve()),
        "return_cache_manifest_sha256": sha256(args.return_cache_manifest),
        "capacity_definition": "direction-selected log(depth3_twap_5m/depth3_twap_30m); ask for Flow5>=0, bid for Flow5<0",
        "interaction_definition": "within date x frozen domain centered-rank Flow5 times centered-rank capacity",
        "incremental_control": "FWL residual of rank interaction on ranked Flow5 and ranked capacity, with intercept",
        "primary_scope": "raw non-neutralized; frozen nine domains primary; exchange auxiliary",
        "signal_cutoff": "10:30:00", "entry_rule": "10:31 minute close",
        "future_filter_used": False, "months": [202601], "rows": len(rows),
        "performance_rows": len(performance), "summary_rows": len(performance_summary),
        "coefficient_rows": len(coefficients), "coefficient_summary_rows": len(coefficient_summary),
    }
    (args.output_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8",
    )
    print(json.dumps({"research_id": "R017", "rows": len(rows), "output": str(args.output_dir)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
