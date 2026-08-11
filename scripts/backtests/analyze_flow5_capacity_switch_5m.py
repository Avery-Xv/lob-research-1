#!/usr/bin/env python3
"""Test 5-minute Flow5 direction switching by opponent-depth replenishment."""

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

from scripts.backtests.analyze_non_parent_state_returns import load_rows, validate_json
from scripts.backtests.backtest_non_parent_direct_targets import run_continuous, summarize
from scripts.backtests.backtest_order_shape_batch_a_domains import percentile_ranks, write_csv
from scripts.factors.order_shape_non_parent.candidates import sha256


TARGET = "ret_1031_1035"
FACTORS = (
    "flow5_raw",
    "minus_flow_to_opponent_depth",
    "flow5_capacity_soft_switch",
    "flow5_capacity_hard_switch",
)


def centered_ranks(values: list[float]) -> list[float]:
    return [2.0 * value - 1.0 for value in percentile_ranks(values)]


def add_candidates(rows: list[dict[str, object]]) -> None:
    grouped: dict[tuple[int, str], list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        grouped[(int(row["date"]), str(row["domain"]))].append(row)

    for group in grouped.values():
        flows = [float(row["flow5"]) for row in group]
        capacity: list[float] = []
        reversal_pressure: list[float] = []
        for row in group:
            flow = float(row["flow5"])
            direction = 1.0 if flow > 0.0 else -1.0 if flow < 0.0 else 0.0
            if direction >= 0.0:
                recent_depth = float(row["ask5"]); baseline_depth = float(row["ask30"])
                active_volume = float(row["buy5"])
            else:
                recent_depth = float(row["bid5"]); baseline_depth = float(row["bid30"])
                active_volume = float(row["sell5"])
            capacity.append(math.log(max(recent_depth, 1e-12) / max(baseline_depth, 1e-12)))
            reversal_pressure.append(-direction * math.log1p(active_volume / max(recent_depth, 1e-12)))

        flow_rank = centered_ranks(flows)
        capacity_rank = centered_ranks(capacity)
        for index, row in enumerate(group):
            cap = capacity_rank[index]
            hard_switch = flow_rank[index] if cap <= -1.0 / 3.0 else -flow_rank[index] if cap >= 1.0 / 3.0 else 0.0
            row["capacity_state"] = "low" if cap <= -1.0 / 3.0 else "high" if cap >= 1.0 / 3.0 else "mid"
            row["candidate"] = {
                "flow5_raw": flows[index],
                "minus_flow_to_opponent_depth": reversal_pressure[index],
                "flow5_capacity_soft_switch": -flow_rank[index] * cap,
                "flow5_capacity_hard_switch": hard_switch,
            }


def conditioned_performance(rows: list[dict[str, object]]) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    output: list[dict[str, object]] = []
    for state in ("low", "mid", "high"):
        subset = [row for row in rows if row["capacity_state"] == state]
        results = run_continuous(subset, {"factors": ("flow5_raw",), "targets": (TARGET,)})
        for result in results:
            result["scope"] = f"capacity_{state}/{result['scope']}"
        output.extend(results)
    return output, summarize(output)


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
    add_candidates(rows)
    performance = run_continuous(rows, {"factors": FACTORS, "targets": (TARGET,)})
    performance_summary = summarize(performance)
    conditioned, conditioned_summary = conditioned_performance(rows)

    args.output_dir.mkdir(parents=True)
    write_csv(args.output_dir / "performance_by_slice.csv", performance)
    write_csv(args.output_dir / "performance_summary.csv", performance_summary)
    write_csv(args.output_dir / "conditioned_flow5_by_slice.csv", conditioned)
    write_csv(args.output_dir / "conditioned_flow5_summary.csv", conditioned_summary)
    manifest = {
        "kind": "research_result", "status": "completed", "research_id": "R017",
        "study": "flow5_capacity_switch_5m_returns", "created_at": datetime.now(timezone.utc).isoformat(),
        "research_run": str(args.research_run.resolve()), "research_run_sha256": sha256(args.research_run),
        "implementation": str(Path(__file__).resolve()), "implementation_sha256": sha256(Path(__file__).resolve()),
        "window_completion": str(args.window_completion.resolve()), "window_completion_sha256": sha256(args.window_completion),
        "candidate_completion": str(args.candidate_completion.resolve()), "candidate_completion_sha256": sha256(args.candidate_completion),
        "window_parquet": str(args.window_parquet.resolve()), "window_parquet_sha256": sha256(args.window_parquet),
        "candidates": str(args.candidates.resolve()), "candidates_sha256": sha256(args.candidates),
        "return_cache_manifest": str(args.return_cache_manifest.resolve()),
        "return_cache_manifest_sha256": sha256(args.return_cache_manifest),
        "capacity_definition": "direction-selected log(depth3_twap_5m/depth3_twap_30m); ask for Flow5>=0, bid for Flow5<0",
        "soft_switch": "-centered_rank(Flow5)*centered_rank(capacity)",
        "hard_switch": "+centered_rank(Flow5) in bottom capacity tercile; -centered_rank(Flow5) in top tercile; zero in middle",
        "target": TARGET, "signal_cutoff": "10:30:00", "entry_rule": "10:31 minute close",
        "primary_scope": "raw non-neutralized; frozen nine domains primary; exchange auxiliary",
        "future_filter_used": False, "months": [202601], "rows": len(rows),
        "performance_rows": len(performance), "conditioned_rows": len(conditioned),
    }
    (args.output_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8",
    )
    print(json.dumps({"research_id": "R017", "rows": len(rows), "output": str(args.output_dir)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
