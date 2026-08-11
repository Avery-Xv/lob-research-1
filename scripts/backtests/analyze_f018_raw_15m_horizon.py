#!/usr/bin/env python3
"""Evaluate raw F018 on the independently labelled 10:31-10:45 horizon."""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import math
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.backtests.analyze_f018_incremental_controls import validate_json
from scripts.backtests.backtest_non_parent_direct_targets import run_continuous, summarize
from scripts.backtests.backtest_order_shape_batch_a_domains import write_csv
from scripts.factors.order_shape_non_parent.candidates import sha256


TABLE = "dwd_dwd.dwd_quant_stock_none_1min_di"
FACTOR = "f018_raw"
TARGET = "ret_1031_1045"
DOMAINS = {
    f"{cap}/{group}"
    for cap in ("cap_lt_50yi", "cap_50_500yi", "cap_ge_500yi")
    for group in ("nonstar_lt_10", "nonstar_ge_10", "star_ge_10")
}


def label_sql() -> str:
    return f"""
SELECT symbol,
       toUInt32(formatDateTime(trade_date, '%Y%m%d')) AS date,
       maxIf(close, formatDateTime(dt, '%H:%i')='10:31') AS close_1031,
       maxIf(close, formatDateTime(dt, '%H:%i')='10:45') AS close_1045
FROM {TABLE}
WHERE trade_date BETWEEN toDate('2026-01-01') AND toDate('2026-01-31')
  AND (startsWith(symbol, 'SH') OR startsWith(symbol, 'SZ'))
  AND formatDateTime(dt, '%H:%i') IN ('10:31','10:45')
GROUP BY symbol, trade_date
HAVING countDistinct(formatDateTime(dt, '%H:%i')) = 2
ORDER BY date, symbol
FORMAT CSVWithNames
""".strip()


def f018_value(flow: float, buy_volume: float, sell_volume: float,
               bid_depth: float, ask_depth: float) -> float:
    direction = 1.0 if flow > 0 else -1.0 if flow < 0 else 0.0
    active_volume = buy_volume if direction >= 0 else sell_volume
    opponent_depth = ask_depth if direction >= 0 else bid_depth
    return -direction * math.log1p(active_volume / max(opponent_depth, 1e-12))


def load_signal_universe(window_path: Path, candidates: Path) -> list[dict[str, object]]:
    connection = duckdb.connect()
    selected = connection.execute("""
        SELECT w.symbol,w.date,c.exchange,c.domain,
               w.flow5m_net_share,w.flow5m_buy_volume,w.flow5m_sell_volume,
               w.book5m_bid3_twap,w.book5m_ask3_twap
        FROM read_parquet(?) AS w
        INNER JOIN read_csv_auto(?, header=true) AS c USING(symbol,date)
        WHERE w.book30m_coverage_ratio >= 0.999999
          AND w.book5m_coverage_ratio >= 0.999999
        ORDER BY w.date,c.domain,w.symbol
    """, [str(window_path), str(candidates)]).fetchall()
    columns = [item[0] for item in connection.description]
    connection.close()
    output: list[dict[str, object]] = []
    seen: set[tuple[str, int]] = set()
    for values in selected:
        source = dict(zip(columns, values))
        key = (str(source["symbol"]), int(source["date"]))
        if key in seen:
            raise ValueError(f"duplicate signal key: {key}")
        seen.add(key)
        exchange = str(source["exchange"])
        domain = str(source["domain"])
        if exchange not in {"SH", "SZ"} or domain not in DOMAINS:
            raise ValueError(f"unexpected signal universe classification: {key}")
        score = f018_value(
            float(source["flow5m_net_share"]),
            float(source["flow5m_buy_volume"]),
            float(source["flow5m_sell_volume"]),
            float(source["book5m_bid3_twap"]),
            float(source["book5m_ask3_twap"]),
        )
        output.append({
            "symbol": key[0], "date": key[1], "signal_time": 1030,
            "exchange": exchange, "domain": domain,
            "candidate": {FACTOR: score}, "targets": {},
        })
    if len(output) < 100_000 or {str(row["domain"]) for row in output} != DOMAINS:
        raise ValueError(f"unexpected signal-universe coverage: {len(output)}")
    return output


def query_labels() -> dict[tuple[str, int], tuple[float, float]]:
    required = ("CH_HOST", "CH_NATIVE_PORT", "CH_USER", "CH_PASSWORD")
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        raise ValueError("missing ClickHouse environment: " + ", ".join(missing))
    command = [
        "clickhouse-client", "--host", os.environ["CH_HOST"],
        "--port", os.environ["CH_NATIVE_PORT"], "--user", os.environ["CH_USER"],
        "--password", os.environ["CH_PASSWORD"], "--query", label_sql(),
    ]
    completed = subprocess.run(command, check=True, capture_output=True, text=True)
    labels: dict[tuple[str, int], tuple[float, float]] = {}
    for row in csv.DictReader(io.StringIO(completed.stdout)):
        key = (row["symbol"], int(row["date"]))
        entry, exit_price = float(row["close_1031"]), float(row["close_1045"])
        if key in labels or entry <= 0 or exit_price <= 0:
            raise ValueError(f"invalid label row: {key}")
        labels[key] = (entry, exit_price)
    return labels


def attach_labels(signal_rows: list[dict[str, object]],
                  labels: dict[tuple[str, int], tuple[float, float]]) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    evaluated: list[dict[str, object]] = []
    cache_rows: list[dict[str, object]] = []
    for row in signal_rows:
        key = (str(row["symbol"]), int(row["date"]))
        prices = labels.get(key)
        if prices is None:
            continue
        entry, exit_price = prices
        target = exit_price / entry - 1.0
        row["targets"] = {TARGET: target}
        evaluated.append(row)
        cache_rows.append({
            "symbol": key[0], "date": key[1],
            "close_1031": entry, "close_1045": exit_price,
            TARGET: target,
        })
    return evaluated, cache_rows


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--research-run", type=Path, required=True)
    parser.add_argument("--window-completion", type=Path, required=True)
    parser.add_argument("--candidate-completion", type=Path, required=True)
    parser.add_argument("--audit-receipt", type=Path, required=True)
    parser.add_argument("--factor-spec", type=Path, required=True)
    parser.add_argument("--window-parquet", type=Path, required=True)
    parser.add_argument("--candidates", type=Path, required=True)
    parser.add_argument("--label-cache-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    if args.label_cache_dir.exists() or args.output_dir.exists():
        raise SystemExit("Refusing to overwrite label cache or research output")
    run = validate_json(args.research_run, kind="research_run")
    if run.get("research_id") != "R017" or run.get("factor_id") != "F018":
        raise ValueError("research run is not R017/F018")
    for path in (args.window_completion, args.candidate_completion):
        completion = validate_json(path, kind="factor_run_completion")
        if completion.get("status") != "completed_audited" or completion.get("factor_id") != "F014":
            raise ValueError(f"not completed_audited F014: {path}")
    audit = validate_json(args.audit_receipt, kind="lob_preflight_receipt")
    certified = audit.get("certified_manifests", [])
    if audit.get("status") != "PASS" or not any(item.get("output_etf_symbols") == 0 for item in certified):
        raise ValueError("Q008 receipt does not certify zero ETF symbols")
    if validate_json(args.factor_spec).get("factor_id") != "F018":
        raise ValueError("factor spec is not F018")

    signal_rows = load_signal_universe(args.window_parquet, args.candidates)
    labels = query_labels()
    rows, cache_rows = attach_labels(signal_rows, labels)
    if len(rows) < 100_000:
        raise ValueError(f"unexpected 15m labelled coverage: {len(rows)}")
    performance = run_continuous(rows, {"factors": (FACTOR,), "targets": (TARGET,)})
    summary = summarize(performance)

    args.label_cache_dir.mkdir(parents=True)
    cache_path = args.label_cache_dir / "minute_prices_1031_1045.csv"
    write_csv(cache_path, cache_rows)
    cache_manifest = {
        "kind": "research_label_cache", "status": "completed",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "source_table": TABLE, "source_rule": "unadjusted stock minute bars; timestamp is minute end",
        "signal_cutoff": "10:30:00", "entry_rule": "10:31 minute close",
        "exit_rule": "10:45 minute close", "horizon_label": "15-minute conventional endpoint",
        "signal_universe_established_before_labels": True,
        "universe_rule": "completed-audited F014 point-in-time SH/SZ A-share stock universe; Q008 ETF=0",
        "missing_label_policy": "10:45 availability only; no other future horizon required",
        "query_sha256": hashlib.sha256(label_sql().encode()).hexdigest(),
        "signal_rows": len(signal_rows), "labelled_rows": len(rows),
        "output": str(cache_path.resolve()), "output_sha256": sha256(cache_path),
    }
    (args.label_cache_dir / "manifest.json").write_text(
        json.dumps(cache_manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )

    args.output_dir.mkdir(parents=True)
    write_csv(args.output_dir / "performance_by_slice.csv", performance)
    write_csv(args.output_dir / "performance_summary.csv", summary)
    result_manifest = {
        "kind": "research_result", "status": "completed",
        "research_id": "R017", "factor_id": "F018", "study": "f018_raw_15m_horizon",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "research_run": str(args.research_run.resolve()), "research_run_sha256": sha256(args.research_run),
        "implementation": str(Path(__file__).resolve()), "implementation_sha256": sha256(Path(__file__).resolve()),
        "window_completion": str(args.window_completion.resolve()), "window_completion_sha256": sha256(args.window_completion),
        "candidate_completion": str(args.candidate_completion.resolve()), "candidate_completion_sha256": sha256(args.candidate_completion),
        "audit_receipt": str(args.audit_receipt.resolve()), "audit_receipt_sha256": sha256(args.audit_receipt),
        "factor_spec": str(args.factor_spec.resolve()), "factor_spec_sha256": sha256(args.factor_spec),
        "label_cache_manifest": str((args.label_cache_dir / "manifest.json").resolve()),
        "label_cache_manifest_sha256": sha256(args.label_cache_dir / "manifest.json"),
        "signal_cutoff": "10:30:00", "entry_rule": "10:31 minute close", "exit_rule": "10:45 minute close",
        "primary_scope": "raw non-neutralized frozen nine domains", "style_neutralization_used": False,
        "future_filter_used": False, "signal_rows": len(signal_rows), "labelled_rows": len(rows),
        "performance_rows": len(performance),
    }
    (args.output_dir / "manifest.json").write_text(
        json.dumps(result_manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps({"signal_rows": len(signal_rows), "labelled_rows": len(rows), "output": str(args.output_dir)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
