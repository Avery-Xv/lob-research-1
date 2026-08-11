#!/usr/bin/env python3
"""Test buy/sell symmetry of the 5-minute -FlowToOpponentDepth reversal."""

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
from scripts.backtests.backtest_order_shape_batch_a_domains import mean_t, write_csv
from scripts.factors.order_shape_non_parent.candidates import sha256


FACTOR = "side_reversal_flow_to_opponent_depth"
TARGET = "ret_1031_1035"


def add_side_score(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    output: list[dict[str, object]] = []
    for row in rows:
        flow = float(row["flow5"])
        if flow > 0.0:
            side = "buy_pressure"
            score = -math.log1p(float(row["buy5"]) / max(float(row["ask5"]), 1e-12))
        elif flow < 0.0:
            side = "sell_pressure"
            score = math.log1p(float(row["sell5"]) / max(float(row["bid5"]), 1e-12))
        else:
            continue
        row["pressure_side"] = side
        row["candidate"] = {FACTOR: score}
        output.append(row)
    return output


def side_performance(rows: list[dict[str, object]]) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    output: list[dict[str, object]] = []
    for side in ("buy_pressure", "sell_pressure"):
        subset = [row for row in rows if row["pressure_side"] == side]
        results = run_continuous(subset, {"factors": (FACTOR,), "targets": (TARGET,)})
        for result in results:
            result["scope"] = f"{side}/{result['scope']}"
        output.extend(results)
    return output, summarize(output)


def symmetry_differences(performance: list[dict[str, object]]) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    paired: dict[tuple[str, str, int, str], dict[str, dict[str, object]]] = defaultdict(dict)
    for row in performance:
        side, scope = str(row["scope"]).split("/", 1)
        key = (scope, str(row["domain"]), int(row["date"]), str(row["target"]))
        paired[key][side] = row

    detail: list[dict[str, object]] = []
    for (scope, domain, date, target), sides in sorted(paired.items()):
        if set(sides) != {"buy_pressure", "sell_pressure"}:
            continue
        buy = sides["buy_pressure"]; sell = sides["sell_pressure"]
        detail.append({
            "scope": scope, "domain": domain, "date": date, "signal_time": 1030,
            "target": target, "buy_n": int(buy["n"]), "sell_n": int(sell["n"]),
            "buy_rank_ic": buy["rank_ic"], "sell_rank_ic": sell["rank_ic"],
            "rank_ic_buy_minus_sell": float(buy["rank_ic"]) - float(sell["rank_ic"]),
            "buy_d10_d1": buy["d10_d1"], "sell_d10_d1": sell["d10_d1"],
            "d10_d1_buy_minus_sell": float(buy["d10_d1"]) - float(sell["d10_d1"]),
        })

    grouped: dict[tuple[str, str, str], list[dict[str, object]]] = defaultdict(list)
    for row in detail:
        grouped[(str(row["scope"]), str(row["domain"]), str(row["target"]))].append(row)
    summary: list[dict[str, object]] = []
    for (scope, domain, target), observations in sorted(grouped.items()):
        record: dict[str, object] = {
            "scope": scope, "domain": domain, "target": target, "n_dates": len(observations),
            "buy_n": sum(int(row["buy_n"]) for row in observations),
            "sell_n": sum(int(row["sell_n"]) for row in observations),
        }
        for metric in ("rank_ic_buy_minus_sell", "d10_d1_buy_minus_sell"):
            values = [float(row[metric]) for row in observations]
            average, t_value = mean_t(values)
            record[metric] = average; record[f"{metric}_t"] = t_value
        summary.append(record)
    return detail, summary


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

    rows = add_side_score(load_rows(args.window_parquet, args.candidates, args.return_prices))
    performance, performance_summary = side_performance(rows)
    differences, difference_summary = symmetry_differences(performance)

    args.output_dir.mkdir(parents=True)
    write_csv(args.output_dir / "side_performance_by_slice.csv", performance)
    write_csv(args.output_dir / "side_performance_summary.csv", performance_summary)
    write_csv(args.output_dir / "symmetry_difference_by_slice.csv", differences)
    write_csv(args.output_dir / "symmetry_difference_summary.csv", difference_summary)
    manifest = {
        "kind": "research_result", "status": "completed", "research_id": "R017",
        "study": "flow_to_opponent_depth_buy_sell_symmetry_5m", "created_at": datetime.now(timezone.utc).isoformat(),
        "research_run": str(args.research_run.resolve()), "research_run_sha256": sha256(args.research_run),
        "implementation": str(Path(__file__).resolve()), "implementation_sha256": sha256(Path(__file__).resolve()),
        "window_completion": str(args.window_completion.resolve()), "window_completion_sha256": sha256(args.window_completion),
        "candidate_completion": str(args.candidate_completion.resolve()), "candidate_completion_sha256": sha256(args.candidate_completion),
        "window_parquet": str(args.window_parquet.resolve()), "window_parquet_sha256": sha256(args.window_parquet),
        "candidates": str(args.candidates.resolve()), "candidates_sha256": sha256(args.candidates),
        "return_cache_manifest": str(args.return_cache_manifest.resolve()),
        "return_cache_manifest_sha256": sha256(args.return_cache_manifest),
        "buy_score": "-log1p(flow5m_buy_volume/book5m_ask3_twap)",
        "sell_score": "+log1p(flow5m_sell_volume/book5m_bid3_twap)",
        "symmetry_null": "daily buy-side minus sell-side metric equals zero",
        "target": TARGET, "signal_cutoff": "10:30:00", "entry_rule": "10:31 minute close",
        "primary_scope": "raw non-neutralized; frozen nine domains primary; exchange auxiliary",
        "future_filter_used": False, "months": [202601], "rows": len(rows),
        "performance_rows": len(performance), "difference_rows": len(differences),
    }
    (args.output_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8",
    )
    print(json.dumps({"research_id": "R017", "rows": len(rows), "output": str(args.output_dir)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
