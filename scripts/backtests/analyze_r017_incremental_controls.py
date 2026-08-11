#!/usr/bin/env python3
"""Test whether R017 book-flow divergence adds beyond raw 1m and 5m flow."""

from __future__ import annotations

import argparse
import csv
import json
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


FACTORS = (
    "r017_flow5m_over_flow1m",
    "r017_book5m_over_flow5m",
    "r017_divergence_over_flow5m",
)
TARGETS = ("future1m_net_share", "future5m_net_share", "future10m_net_share")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--research-run", type=Path, required=True)
    parser.add_argument("--base-result", type=Path, required=True)
    parser.add_argument("--window-parquet", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    if args.output_dir.exists():
        raise SystemExit(f"Refusing to overwrite output: {args.output_dir}")
    research_run = json.loads(args.research_run.read_text())
    if research_run.get("research_id") != "R017":
        raise SystemExit("research run is not R017")
    targets = {}
    connection = duckdb.connect()
    for symbol, date, future1, future5, future10 in connection.execute("""
        SELECT symbol,date,future1m_net_share,future5m_net_share,future10m_net_share
        FROM read_parquet(?)
    """, [str(args.window_parquet)]).fetchall():
        targets[(str(symbol), int(date))] = {
            "future1m_net_share": float(future1), "future5m_net_share": float(future5),
            "future10m_net_share": float(future10),
        }
    connection.close()
    grouped: dict[tuple[int, str], list[dict[str, object]]] = defaultdict(list)
    with args.base_result.open(newline="", encoding="utf-8") as handle:
        for source in csv.DictReader(handle):
            key = (source["symbol"], int(source["date"]))
            if key not in targets:
                raise ValueError(f"missing target: {key}")
            grouped[(key[1], source["domain"])].append({
                "symbol": key[0], "date": key[1], "signal_time": 1030,
                "exchange": source["exchange"], "domain": source["domain"],
                "flow1": float(source["r017_flow1m_raw"]),
                "flow5": float(source["r017_flow5m_raw"]),
                "book5": float(source["r017_book5m_twap"]),
                "divergence": float(source["r017_flow5m_minus_book5m"]),
                "targets": targets[key],
            })
    rows = []
    for group_rows in grouped.values():
        flow1 = [float(row["flow1"]) for row in group_rows]
        flow5 = [float(row["flow5"]) for row in group_rows]
        book5 = [float(row["book5"]) for row in group_rows]
        divergence = [float(row["divergence"]) for row in group_rows]
        flow5_over_flow1 = residualize(flow5, [[value] for value in flow1])
        book5_over_flow5 = residualize(book5, [[value] for value in flow5])
        divergence_over_flow5 = residualize(divergence, [[value] for value in flow5])
        for index, row in enumerate(group_rows):
            row["candidate"] = {
                "r017_flow5m_over_flow1m": flow5_over_flow1[index],
                "r017_book5m_over_flow5m": book5_over_flow5[index],
                "r017_divergence_over_flow5m": divergence_over_flow5[index],
            }
        rows.extend(group_rows)
    rows.sort(key=lambda row: (int(row["date"]), str(row["domain"]), str(row["symbol"])))
    performance = run_continuous(rows, {"factors": FACTORS, "targets": TARGETS})
    summary = summarize(performance)
    args.output_dir.mkdir(parents=True)
    write_csv(args.output_dir / "performance_by_slice.csv", performance)
    write_csv(args.output_dir / "performance_summary.csv", summary)
    manifest = {
        "kind": "research_result", "research_id": "R017", "status": "completed",
        "spec_version": "v2_window_path_incremental_controls",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "research_run": str(args.research_run.resolve()), "research_run_sha256": sha256(args.research_run),
        "research_implementation": str(Path(__file__).resolve()),
        "research_implementation_sha256": sha256(Path(__file__).resolve()),
        "base_result": str(args.base_result.resolve()), "base_result_sha256": sha256(args.base_result),
        "window_parquet": str(args.window_parquet.resolve()), "window_parquet_sha256": sha256(args.window_parquet),
        "primary_scope": "nine domains, raw factor residual controls, direct targets",
        "factor_control_rule": "within date x frozen domain; factor residualized on raw flow; no target residualization",
        "future_return_used": False, "rows": len(rows),
        "performance_rows": len(performance), "summary_rows": len(summary),
    }
    (args.output_dir / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({"research_id": "R017", "rows": len(rows), "summary_rows": len(summary), "output": str(args.output_dir)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
