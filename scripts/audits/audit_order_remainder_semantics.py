#!/usr/bin/env python3
"""Audit SH/SZ active-order publication and remainder semantics on real V4 data."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import subprocess
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


REPO_ROOT = Path(__file__).resolve().parents[2]
IMPLEMENTATIONS = (
    "scripts/factors/experiment_batch_1/engine.py",
    "scripts/factors/order_behavior_ratio/intraday_window_factor.py",
    "scripts/factors/passive_large_gap_ratio/intraday_window_factor.py",
    "scripts/factors/joint_large_gap_order_behavior/compute_v4.py",
    "scripts/factors/order_shape_mechanism/m1_quote_engine.py",
    "scripts/factors/order_shape_mechanism/batch_a_engine.py",
    "scripts/factors/stylized_fact_4_6/reproduce_d01_d03.py",
)
CASE_FIELDS = (
    "symbol", "date", "side", "order_id", "classification", "first_add_row",
    "first_trade_row", "add_qty", "active_trade_qty", "immediate_trade_qty",
    "add_events", "trade_events", "reconstructed_submit_qty",
)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def git_commit() -> str:
    return subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=REPO_ROOT, check=True,
        capture_output=True, text=True,
    ).stdout.strip()


def load_inputs(file_list: Path, symbols: set[str], month: str) -> list[Path]:
    paths = []
    for raw in file_list.read_text(encoding="utf-8").splitlines():
        path = Path(raw.strip())
        if not raw.strip() or path.parent.name != month or path.stem not in symbols:
            continue
        if not path.exists():
            raise FileNotFoundError(path)
        paths.append(path)
    missing = symbols - {path.stem for path in paths}
    if missing:
        raise ValueError(f"symbols absent from manifest/month: {sorted(missing)}")
    return sorted(paths)


def audit_file(path: Path, dates: list[int]) -> tuple[list[dict[str, Any]], dict[str, int]]:
    con = duckdb.connect()
    con.execute("PRAGMA threads=1")
    con.execute("PRAGMA memory_limit='2GB'")
    date_placeholders = ",".join("?" for _ in dates)
    query = f"""
WITH events AS (
    SELECT date,time,row_id,source_action,source_side,source_volume,
           CASE WHEN source_side='B' THEN source_buy_order_id
                WHEN source_side='S' THEN source_sell_order_id END AS order_id
    FROM read_parquet(?)
    WHERE date IN ({date_placeholders})
      AND ((time>=93000000 AND time<113000000) OR (time>=130000000 AND time<145700000))
      AND source_side IN ('B','S')
),
trades AS (
    SELECT date,source_side AS side,order_id,
           min(row_id) AS first_trade_row,max(row_id) AS last_trade_row,
           sum(source_volume)::BIGINT AS active_trade_qty,count(*)::BIGINT AS trade_events
    FROM events WHERE source_action='TRADE' AND order_id IS NOT NULL AND source_volume>0
    GROUP BY date,side,order_id
),
adds AS (
    SELECT date,source_side AS side,order_id,
           min(row_id) AS first_add_row,max(row_id) AS last_add_row,
           sum(source_volume)::BIGINT AS add_qty,count(*)::BIGINT AS add_events
    FROM events WHERE source_action='ORDER_ADD' AND order_id IS NOT NULL AND source_volume>0
    GROUP BY date,side,order_id
),
keys AS (
    SELECT coalesce(t.date,a.date) AS date,coalesce(t.side,a.side) AS side,
           coalesce(t.order_id,a.order_id) AS order_id,
           a.first_add_row,a.last_add_row,t.first_trade_row,t.last_trade_row,
           coalesce(a.add_qty,0)::BIGINT AS add_qty,
           coalesce(t.active_trade_qty,0)::BIGINT AS active_trade_qty,
           coalesce(a.add_events,0)::BIGINT AS add_events,
           coalesce(t.trade_events,0)::BIGINT AS trade_events
    FROM trades t FULL OUTER JOIN adds a USING(date,side,order_id)
),
classified AS (
    SELECT *,CASE
        WHEN first_trade_row IS NULL THEN 'passive_only'
        WHEN first_add_row IS NULL THEN 'trade_only_active'
        WHEN first_trade_row < first_add_row THEN 'posttrade_remainder'
        WHEN first_add_row < first_trade_row THEN 'pretrade_active_add'
        ELSE 'same_row_unresolved' END AS classification
    FROM keys
),
with_immediate AS (
    SELECT c.*,
           coalesce((SELECT sum(e.source_volume)::BIGINT FROM events e
                     WHERE e.source_action='TRADE' AND e.date=c.date
                       AND e.source_side=c.side AND e.order_id=c.order_id
                       AND e.row_id<c.first_add_row),0)::BIGINT AS immediate_trade_qty
    FROM classified c
)
SELECT date,side,order_id,classification,first_add_row,first_trade_row,add_qty,
       active_trade_qty,immediate_trade_qty,add_events,trade_events,
       CASE WHEN classification='posttrade_remainder'
              THEN add_qty+immediate_trade_qty
            WHEN classification='pretrade_active_add' THEN add_qty
            WHEN classification='trade_only_active' THEN active_trade_qty
            ELSE add_qty END::BIGINT AS reconstructed_submit_qty
FROM with_immediate
ORDER BY date,side,order_id
"""
    rows = con.execute(query, [str(path), *dates]).fetchall()
    columns = [item[0] for item in con.description]
    cases = [dict(zip(columns, row)) for row in rows]
    qc_query = f"""
WITH e AS (
  SELECT date,row_id,bid_px[1] AS bid1,ask_px[1] AS ask1,
         lag(row_id) OVER(PARTITION BY date ORDER BY row_id) AS prior_row
  FROM read_parquet(?) WHERE date IN ({date_placeholders})
)
SELECT count(*) FILTER(WHERE prior_row IS NOT NULL AND row_id<=prior_row) AS row_order_violations,
       count(*) FILTER(WHERE bid1 IS NULL OR ask1 IS NULL OR bid1<=0 OR ask1<=0) AS missing_books,
       count(*) FILTER(WHERE bid1=ask1 AND bid1>0) AS locked_books,
       count(*) FILTER(WHERE bid1>ask1 AND ask1>0) AS crossed_books
FROM e
"""
    qc_row = con.execute(qc_query, [str(path), *dates]).fetchone()
    con.close()
    return cases, dict(zip(("row_order_violations", "missing_books", "locked_books", "crossed_books"), qc_row))


def summarize(cases: list[dict[str, Any]], qc_rows: list[dict[str, int]]) -> dict[str, Any]:
    counts = Counter((row["symbol"][:2], row["classification"]) for row in cases)
    active_adds = {
        exchange: counts[(exchange, "pretrade_active_add")] + counts[(exchange, "posttrade_remainder")]
        for exchange in ("SH", "SZ")
    }
    sz_conservation = sum(
        1 for row in cases
        if row["symbol"].startswith("SZ") and row["classification"] == "pretrade_active_add"
        and row["active_trade_qty"] > row["add_qty"]
    )
    directionless = Counter((row["symbol"], row["date"], row["order_id"]) for row in cases if row["trade_events"])
    side_collision_ids = sum(1 for count in directionless.values() if count > 1)
    qc = {key: sum(int(row[key]) for row in qc_rows) for key in qc_rows[0]}
    summary = {
        "classification_counts": {
            exchange: {name: counts[(exchange, name)] for name in (
                "passive_only", "trade_only_active", "pretrade_active_add",
                "posttrade_remainder", "same_row_unresolved",
            )} for exchange in ("SH", "SZ")
        },
        "active_add_counts": active_adds,
        "sz_pretrade_conservation_violations": sz_conservation,
        "directionless_active_id_collisions": side_collision_ids,
        "book_qc": qc,
    }
    checks = {
        "both_exchanges_present": all(any(row["symbol"].startswith(ex) for row in cases) for ex in ("SH", "SZ")),
        "row_order_strict": qc["row_order_violations"] == 0,
        "sh_trade_before_add_observed": counts[("SH", "posttrade_remainder")] > 0,
        "sh_fully_immediate_trade_only_observed": counts[("SH", "trade_only_active")] > 0,
        "sz_add_before_trade_observed": counts[("SZ", "pretrade_active_add")] > 0,
        "sz_submit_quantity_conserved": sz_conservation == 0,
        "side_qualified_keys_complete": all(row["side"] in ("B", "S") for row in cases),
    }
    summary["checks"] = checks
    summary["status"] = "PASS" if all(checks.values()) else "FAIL"
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--file-list", type=Path, required=True)
    parser.add_argument("--universe-metadata", type=Path, required=True)
    parser.add_argument("--month", required=True)
    parser.add_argument("--symbols", nargs="+", required=True)
    parser.add_argument("--dates", nargs="+", type=int, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--sample-limit", type=int, default=300)
    args = parser.parse_args()

    metadata = json.loads(args.universe_metadata.read_text(encoding="utf-8"))
    if metadata.get("output_etf_symbols") != 0:
        raise SystemExit("Universe metadata does not certify output_etf_symbols=0")
    symbols = set(args.symbols)
    invalid_symbols = sorted(symbol for symbol in symbols if not re.fullmatch(r"(?:SH|SZ)\d{6}", symbol))
    if invalid_symbols:
        raise SystemExit(f"Invalid symbols: {invalid_symbols}")
    paths = load_inputs(args.file_list, symbols, args.month)
    all_cases: list[dict[str, Any]] = []
    qc_rows = []
    for path in paths:
        cases, qc = audit_file(path, args.dates)
        for row in cases:
            row["symbol"] = path.stem
        all_cases.extend(cases)
        qc_rows.append(qc)
    summary = summarize(all_cases, qc_rows)
    summary.update({
        "audit_version": "q003_order_remainder_v1",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "git_commit": git_commit(),
        "file_list": str(args.file_list.resolve()),
        "file_list_sha256": sha256(args.file_list),
        "universe_metadata": str(args.universe_metadata.resolve()),
        "universe_metadata_sha256": sha256(args.universe_metadata),
        "universe_rule": metadata.get("universe_rule"),
        "output_etf_symbols": metadata.get("output_etf_symbols"),
        "month": args.month,
        "symbols": sorted(symbols),
        "dates": args.dates,
        "implementation_sha256": {path: sha256(REPO_ROOT / path) for path in IMPLEMENTATIONS},
        "semantics": {
            "key": "(side, active_order_id)",
            "SH": "TRADE(s) before ORDER_ADD remainder; original quantity = immediate trades + remainder; trade-only means fully immediate.",
            "SZ": "ORDER_ADD full submitted quantity before child TRADE(s); no second remainder add.",
            "passive_rule": "Any ORDER_ADD belonging to an active-order key is excluded from passive submission and quote-arrival metrics.",
            "post_processed_links_used": False,
        },
        "passed_quality_gates": ["Q001", "Q002", "Q003", "Q005"] if summary["status"] == "PASS" else [],
    })
    args.output_dir.mkdir(parents=True, exist_ok=False)
    (args.output_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    selected = []
    per_class = Counter()
    for row in all_cases:
        key = (row["symbol"][:2], row["classification"])
        if per_class[key] < args.sample_limit:
            selected.append(row)
            per_class[key] += 1
    with (args.output_dir / "case_sample.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CASE_FIELDS)
        writer.writeheader()
        writer.writerows({key: row.get(key) for key in CASE_FIELDS} for row in selected)
    print(json.dumps({"status": summary["status"], "output": str(args.output_dir)}, ensure_ascii=False))
    return 0 if summary["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
