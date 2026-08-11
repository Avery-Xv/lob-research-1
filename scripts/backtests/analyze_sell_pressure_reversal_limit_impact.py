#!/usr/bin/env python3
"""Evaluate one-sided sell-pressure reversal and price-limit sensitivity."""

from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.backtests.analyze_non_parent_state_returns import load_rows, validate_json
from scripts.backtests.backtest_non_parent_direct_targets import run_continuous, summarize
from scripts.backtests.backtest_order_shape_batch_a_domains import write_csv
from scripts.factors.order_shape_non_parent.candidates import sha256


FACTOR = "side_reversal_flow_to_opponent_depth"
TARGET = "ret_1031_1035"
FLAG_FIELDS = (
    "signal_close_at_up", "signal_close_at_down", "signal_locked_up", "signal_locked_down",
    "entry_close_at_up", "entry_close_at_down", "entry_locked_up", "entry_locked_down",
    "post_entry_touch_up", "post_entry_touch_down",
    "exit_close_at_up", "exit_close_at_down", "exit_locked_up", "exit_locked_down",
)


def load_limit_states(path: Path) -> dict[tuple[str, int], dict[str, bool]]:
    output: dict[tuple[str, int], dict[str, bool]] = {}
    with path.open(newline="", encoding="utf-8") as handle:
        for source in csv.DictReader(handle):
            key = (source["symbol"], int(source["date"]))
            if key in output:
                raise ValueError(f"duplicate limit-state key: {key}")
            output[key] = {field: source[field] == "1" for field in FLAG_FIELDS}
    return output


def enrich(rows: list[dict[str, object]], states: dict[tuple[str, int], dict[str, bool]]) -> list[dict[str, object]]:
    output: list[dict[str, object]] = []
    for row in rows:
        key = (str(row["symbol"]), int(row["date"]))
        if key not in states:
            continue
        flow = float(row["flow5"])
        if flow > 0.0:
            side = "buy_pressure"
            score = -math.log1p(float(row["buy5"]) / max(float(row["ask5"]), 1e-12))
        elif flow < 0.0:
            side = "sell_pressure"
            score = math.log1p(float(row["sell5"]) / max(float(row["bid5"]), 1e-12))
        else:
            continue
        row.update(states[key]); row["pressure_side"] = side; row["candidate"] = {FACTOR: score}
        output.append(row)
    if len(output) < 95_000:
        raise ValueError(f"unexpected limit-state join coverage: {len(output)}")
    return output


def sample_rules(row: dict[str, object]) -> dict[str, bool]:
    signal_off = not any(bool(row[field]) for field in ("signal_close_at_up", "signal_close_at_down"))
    entry_off = not any(bool(row[field]) for field in ("entry_close_at_up", "entry_close_at_down"))
    entry_unlocked = not any(bool(row[field]) for field in ("entry_locked_up", "entry_locked_down"))
    exit_unlocked = not any(bool(row[field]) for field in ("exit_locked_up", "exit_locked_down"))
    post_touch = any(bool(row[field]) for field in ("post_entry_touch_up", "post_entry_touch_down"))
    return {
        "all": True,
        "entry_off_limit": entry_off,
        "signal_and_entry_off_limit": signal_off and entry_off,
        "entry_not_locked": entry_unlocked,
        "entry_off_limit_exit_not_locked": entry_off and exit_unlocked,
        "entry_off_limit_no_post_touch": entry_off and not post_touch,
        "entry_at_limit_diagnostic": not entry_off,
        "post_entry_touch_diagnostic": post_touch,
    }


def evaluate(rows: list[dict[str, object]]) -> tuple[list[dict[str, object]], list[dict[str, object]], list[dict[str, object]]]:
    performance: list[dict[str, object]] = []
    counts: list[dict[str, object]] = []
    sample_names = tuple(sample_rules(rows[0]))
    for sample in sample_names:
        sample_rows = [row for row in rows if sample_rules(row)[sample]]
        for side in ("sell_pressure", "buy_pressure"):
            subset = [row for row in sample_rows if row["pressure_side"] == side]
            counts.append({"sample": sample, "side": side, "n": len(subset)})
            results = run_continuous(subset, {"factors": (FACTOR,), "targets": (TARGET,)})
            for result in results:
                result["scope"] = f"{sample}/{side}/{result['scope']}"
            performance.extend(results)
    return performance, summarize(performance), counts


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--research-run", type=Path, required=True)
    parser.add_argument("--window-completion", type=Path, required=True)
    parser.add_argument("--candidate-completion", type=Path, required=True)
    parser.add_argument("--window-parquet", type=Path, required=True)
    parser.add_argument("--candidates", type=Path, required=True)
    parser.add_argument("--return-cache-manifest", type=Path, required=True)
    parser.add_argument("--return-prices", type=Path, required=True)
    parser.add_argument("--limit-cache-manifest", type=Path, required=True)
    parser.add_argument("--limit-states", type=Path, required=True)
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
    return_cache = validate_json(args.return_cache_manifest, kind="research_label_cache")
    if sha256(args.return_prices) != return_cache.get("output_sha256"):
        raise ValueError("return price cache hash mismatch")
    limit_cache = validate_json(args.limit_cache_manifest, kind="research_label_cache")
    if sha256(args.limit_states) != limit_cache.get("output_sha256"):
        raise ValueError("limit-state cache hash mismatch")

    rows = load_rows(args.window_parquet, args.candidates, args.return_prices)
    rows = enrich(rows, load_limit_states(args.limit_states))
    performance, performance_summary, counts = evaluate(rows)

    args.output_dir.mkdir(parents=True)
    write_csv(args.output_dir / "performance_by_slice.csv", performance)
    write_csv(args.output_dir / "performance_summary.csv", performance_summary)
    write_csv(args.output_dir / "sample_counts.csv", counts)
    manifest = {
        "kind": "research_result", "status": "completed", "research_id": "R017",
        "study": "sell_pressure_reversal_5m_limit_impact", "created_at": __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat(),
        "research_run": str(args.research_run.resolve()), "research_run_sha256": sha256(args.research_run),
        "implementation": str(Path(__file__).resolve()), "implementation_sha256": sha256(Path(__file__).resolve()),
        "window_completion": str(args.window_completion.resolve()), "window_completion_sha256": sha256(args.window_completion),
        "candidate_completion": str(args.candidate_completion.resolve()), "candidate_completion_sha256": sha256(args.candidate_completion),
        "return_cache_manifest": str(args.return_cache_manifest.resolve()), "return_cache_manifest_sha256": sha256(args.return_cache_manifest),
        "limit_cache_manifest": str(args.limit_cache_manifest.resolve()), "limit_cache_manifest_sha256": sha256(args.limit_cache_manifest),
        "factor_definition": "sell side: +log1p(flow5m_sell_volume/book5m_bid3_twap); buy side retained only as asymmetry control",
        "primary_tradability_sample": "entry 10:31 minute close is neither daily up_limit nor down_limit",
        "future_limit_usage": "post-entry touch and 10:35 exit-lock filters are secondary execution diagnostics only",
        "target": TARGET, "signal_cutoff": "10:30:00", "entry_rule": "10:31 minute close",
        "primary_scope": "raw non-neutralized; frozen nine domains primary; exchange auxiliary",
        "future_filter_used_in_primary": False, "months": [202601], "rows": len(rows),
        "performance_rows": len(performance),
    }
    (args.output_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8",
    )
    print(json.dumps({"research_id": "R017", "rows": len(rows), "output": str(args.output_dir)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
