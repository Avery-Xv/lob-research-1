#!/usr/bin/env python3
"""Evaluate the v2 multi-scale R016/R017 specifications on direct targets."""

from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import duckdb


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.backtests.backtest_non_parent_direct_targets import run_continuous, summarize
from scripts.backtests.backtest_order_shape_batch_a_domains import write_csv
from scripts.factors.order_shape_non_parent.candidates import residualize, sha256


R017_FACTORS = (
    "r017_flow1m_raw", "r017_flow5m_raw", "r017_flow30m_raw",
    "r017_endpoint_book", "r017_book5m_twap", "r017_book30m_twap",
    "r017_old_endpoint_residual", "r017_flow5m_minus_book5m",
    "r017_flow30m_minus_book30m", "r017_terminal_shock",
    "r017_flow5m_minus_book5m_over_flow1m", "r017_terminal_shock_over_flow1m",
)
R017_TARGETS = (
    "future1m_net_share", "future5m_net_share", "future10m_net_share",
    "future1m_end_bi3", "future5m_end_bi3", "future10m_end_bi3",
    "log_future10m_realized_vol", "future10m_spread_change_bps",
)
R016_CONFIGS = (
    (("r016_confirmation1m",), ("future1m_aligned_flow1m", "future5m_aligned_flow1m", "future10m_aligned_flow1m", "log_future10m_total_volume", "log_future10m_realized_vol")),
    (("r016_confirmation5m",), ("future1m_aligned_flow5m", "future5m_aligned_flow5m", "future10m_aligned_flow5m", "log_future10m_total_volume", "log_future10m_realized_vol")),
    (("r016_confirmation30m",), ("future1m_aligned_flow30m", "future5m_aligned_flow30m", "future10m_aligned_flow30m", "log_future10m_total_volume", "log_future10m_realized_vol")),
)


def validate_completion(path: Path, factor_id: str = "F014") -> dict[str, object]:
    value = json.loads(path.read_text())
    if value.get("kind") != "factor_run_completion" or value.get("status") != "completed_audited" or value.get("factor_id") != factor_id:
        raise ValueError(f"not a completed_audited {factor_id}: {path}")
    return value


def load_legacy(path: Path) -> dict[tuple[str, int], dict[str, str]]:
    rows: dict[tuple[str, int], dict[str, str]] = {}
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            key = (row["symbol"], int(row["date"]))
            if key in rows:
                raise ValueError(f"duplicate legacy candidate key: {key}")
            rows[key] = row
    return rows


def sign(value: float) -> float:
    return 1.0 if value > 0 else -1.0 if value < 0 else 0.0


def target_values(row: dict[str, object], flows: dict[str, float]) -> dict[str, float]:
    output: dict[str, float] = {}
    for minutes in (1, 5, 10):
        net = float(row[f"future{minutes}m_net_share"])
        output[f"future{minutes}m_net_share"] = net
        output[f"future{minutes}m_end_bi3"] = float(row[f"future{minutes}m_end_bi3"])
        for flow_name, flow in flows.items():
            output[f"future{minutes}m_aligned_{flow_name}"] = sign(flow) * net
    output["log_future10m_total_volume"] = math.log1p(float(row["future10m_total_volume"]))
    output["log_future10m_realized_vol"] = math.log1p(float(row["future10m_realized_vol_bps"]))
    output["future10m_spread_change_bps"] = float(row["future10m_end_spread_bps"]) - float(row["endpoint_spread_bps"])
    return output


def enrich_group(rows: list[dict[str, object]]) -> None:
    flow1 = [float(row["flow1m"]) for row in rows]
    flow5 = [float(row["flow5m"]) for row in rows]
    flow30 = [float(row["flow30m"]) for row in rows]
    endpoint = [float(row["endpoint_book"]) for row in rows]
    book5 = [float(row["book5m"]) for row in rows]
    book30 = [float(row["book30m"]) for row in rows]
    old_residual = residualize(flow1, [[value, value ** 2, value ** 3] for value in endpoint])
    residual5 = residualize(flow5, [[value, value ** 2, value ** 3] for value in book5])
    residual30 = residualize(flow30, [[value, value ** 2, value ** 3] for value in book30])
    terminal = residualize(flow5, [[
        flow30[index], book30[index], book5[index],
        float(row["book_shift"]), float(row["endpoint_minus_book5m"]),
        float(row["book5m_time_std"]),
    ] for index, row in enumerate(rows)])
    residual5_over_flow1 = residualize(residual5, [[value] for value in flow1])
    terminal_over_flow1 = residualize(terminal, [[value] for value in flow1])
    for index, row in enumerate(rows):
        pressure = float(row["execution_pressure"])
        candidate = {
            "r017_flow1m_raw": flow1[index], "r017_flow5m_raw": flow5[index],
            "r017_flow30m_raw": flow30[index], "r017_endpoint_book": endpoint[index],
            "r017_book5m_twap": book5[index], "r017_book30m_twap": book30[index],
            "r017_old_endpoint_residual": old_residual[index],
            "r017_flow5m_minus_book5m": residual5[index],
            "r017_flow30m_minus_book30m": residual30[index],
            "r017_terminal_shock": terminal[index],
            "r017_flow5m_minus_book5m_over_flow1m": residual5_over_flow1[index],
            "r017_terminal_shock_over_flow1m": terminal_over_flow1[index],
            "r016_confirmation1m": flow1[index] * pressure,
            "r016_confirmation5m": flow5[index] * pressure,
            "r016_confirmation30m": flow30[index] * pressure,
        }
        row["candidate"] = candidate


def load_rows(window_path: Path, legacy_path: Path) -> tuple[list[dict[str, object]], dict[str, int]]:
    legacy = load_legacy(legacy_path)
    connection = duckdb.connect()
    selected = connection.execute("""
        SELECT symbol,date,signal_time,
               flow1m_net_share,flow5m_net_share,flow30m_net_share,
               endpoint_bi3,book5m_bi3_twap,book30m_bi3_twap,
               book_shift_5m_minus_30m,endpoint_minus_book5m,book5m_bi3_time_std,
               endpoint_spread_bps,
               future1m_net_share,future5m_net_share,future10m_net_share,
               future1m_end_bi3,future5m_end_bi3,future10m_end_bi3,
               future10m_total_volume,future10m_realized_vol_bps,future10m_end_spread_bps
        FROM read_parquet(?)
        WHERE book30m_coverage_ratio>=0.999999 AND book5m_coverage_ratio>=0.999999
        ORDER BY date,symbol
    """, [str(window_path)]).fetchall()
    columns = [item[0] for item in connection.description]
    connection.close()
    grouped: dict[tuple[int, str], list[dict[str, object]]] = defaultdict(list)
    missing_legacy = 0
    for values in selected:
        source = dict(zip(columns, values))
        key = (str(source["symbol"]), int(source["date"]))
        old = legacy.get(key)
        if old is None:
            missing_legacy += 1
            continue
        row: dict[str, object] = {
            "symbol": key[0], "date": key[1], "signal_time": int(source["signal_time"]),
            "exchange": old["exchange"], "domain": old["domain"],
            "flow1m": float(source["flow1m_net_share"]),
            "flow5m": float(source["flow5m_net_share"]),
            "flow30m": float(source["flow30m_net_share"]),
            "endpoint_book": float(source["endpoint_bi3"]),
            "book5m": float(source["book5m_bi3_twap"]),
            "book30m": float(source["book30m_bi3_twap"]),
            "book_shift": float(source["book_shift_5m_minus_30m"]),
            "endpoint_minus_book5m": float(source["endpoint_minus_book5m"]),
            "book5m_time_std": float(source["book5m_bi3_time_std"]),
            "execution_pressure": float(old["execution_pressure"]),
        }
        flows = {name: float(row[name]) for name in ("flow1m", "flow5m", "flow30m")}
        row["targets"] = target_values(source, flows)
        grouped[(key[1], str(old["domain"]))].append(row)
    rows = []
    sparse_rows = 0
    for group_rows in grouped.values():
        if len(group_rows) < 15:
            sparse_rows += len(group_rows); continue
        enrich_group(group_rows); rows.extend(group_rows)
    rows.sort(key=lambda row: (int(row["date"]), str(row["domain"]), str(row["symbol"])))
    return rows, {
        "window_rows_after_coverage_filter": len(selected),
        "missing_legacy_domain_or_pressure": missing_legacy,
        "sparse_domain_rows": sparse_rows,
        "analysis_rows": len(rows),
    }


def flatten_candidates(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    return [{
        "symbol": row["symbol"], "date": row["date"], "signal_time": row["signal_time"],
        "exchange": row["exchange"], "domain": row["domain"], **row["candidate"],
    } for row in rows]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--research-id", choices=("R016", "R017"), required=True)
    parser.add_argument("--research-run", type=Path, required=True)
    parser.add_argument("--window-completion", type=Path, required=True)
    parser.add_argument("--window-parquet", type=Path, required=True)
    parser.add_argument("--legacy-candidate-completion", type=Path, required=True)
    parser.add_argument("--legacy-candidates", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    if args.output_dir.exists():
        raise SystemExit(f"Refusing to overwrite output: {args.output_dir}")
    validate_completion(args.window_completion); validate_completion(args.legacy_candidate_completion)
    research_run = json.loads(args.research_run.read_text())
    if research_run.get("kind") != "research_run" or research_run.get("research_id") != args.research_id:
        raise SystemExit("research run mismatch")
    if Path(research_run["factor_runs"]["F014"]).resolve() != args.window_completion.resolve():
        raise SystemExit("research run is not bound to the supplied window completion")
    rows, counts = load_rows(args.window_parquet, args.legacy_candidates)
    if args.research_id == "R017":
        performance = run_continuous(rows, {"factors": R017_FACTORS, "targets": R017_TARGETS})
    else:
        performance = []
        for factors, targets in R016_CONFIGS:
            performance.extend(run_continuous(rows, {"factors": factors, "targets": targets}))
    summary = summarize(performance)
    args.output_dir.mkdir(parents=True)
    write_csv(args.output_dir / "candidate_values.csv", flatten_candidates(rows))
    write_csv(args.output_dir / "performance_by_slice.csv", performance)
    write_csv(args.output_dir / "performance_summary.csv", summary)
    manifest = {
        "kind": "research_result", "research_id": args.research_id,
        "spec_version": "v2_window_path", "status": "completed",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "research_run": str(args.research_run.resolve()), "research_run_sha256": sha256(args.research_run),
        "research_implementation": str(Path(__file__).resolve()),
        "research_implementation_sha256": sha256(Path(__file__).resolve()),
        "window_completion": str(args.window_completion.resolve()),
        "window_completion_sha256": sha256(args.window_completion),
        "legacy_candidate_completion": str(args.legacy_candidate_completion.resolve()),
        "legacy_candidate_completion_sha256": sha256(args.legacy_candidate_completion),
        "window_parquet": str(args.window_parquet.resolve()), "window_parquet_sha256": sha256(args.window_parquet),
        "legacy_candidates": str(args.legacy_candidates.resolve()), "legacy_candidates_sha256": sha256(args.legacy_candidates),
        "primary_scope": "nine domains, raw non-neutralized direct targets",
        "signal_time": "10:30", "signal_windows": ["[10:00,10:30)", "[10:25,10:30)", "[10:29,10:30)"],
        "target_windows": ["[10:30,10:31)", "[10:30,10:35)", "[10:30,10:40)"],
        "coverage_rule": "primary sample requires complete 30m and 5m book coverage",
        "residual_rule": "OLS-equivalent residuals within date x frozen nine-domain; intercept implicit by centering",
        "future_return_used": False, **counts,
        "performance_rows": len(performance), "summary_rows": len(summary),
    }
    (args.output_dir / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({"research_id": args.research_id, "rows": len(rows), "summary_rows": len(summary), "output": str(args.output_dir)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
