#!/usr/bin/env python3
"""Test whether STAR market-making-borrow proxies explain 5-minute factor behavior."""

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
from collections import defaultdict
from datetime import date as Date
from datetime import datetime, timedelta, timezone
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.backtests.analyze_non_parent_state_returns import load_rows, validate_json
from scripts.backtests.backtest_non_parent_direct_targets import evaluate_group, summarize
from scripts.backtests.backtest_order_shape_batch_a_domains import mean_t, percentile_ranks, write_csv
from scripts.factors.order_shape_non_parent.candidates import sha256


FACTOR = "minus_flow_to_opponent_depth"
TARGET = "ret_1031_1035"
SOURCE_TABLE = "jydb.RF_STradingSum"
PROXY_DEFINITIONS = {
    "ever_prior": "at least one category-2 market-making securities-lending row strictly before signal date",
    "positive_60d_prior": "positive ending balance on at least one row in [signal date - 60 calendar days, signal date)",
}


def maker_borrow_sql() -> str:
    return f"""
SELECT concat('SH', s.SecuCode) AS symbol,
       toString(toDate(r.TradingDay)) AS trading_date,
       coalesce(toFloat64(r.Endingmargin), 0.0) AS ending_balance
FROM {SOURCE_TABLE} AS r
INNER JOIN jydb.SecuMain AS s ON r.InnerCode = s.InnerCode
WHERE r.SecuLendingCategory = 2
  AND s.SecuMarket = 83
  AND s.ListedSector = 7
  AND s.SecuCategory IN (1, 41)
  AND toDate(r.TradingDay) < toDate('2026-02-01')
ORDER BY symbol, trading_date
FORMAT CSVWithNames
""".strip()


def query_maker_borrow() -> dict[str, list[tuple[Date, float]]]:
    required = ("CH_HOST", "CH_NATIVE_PORT", "CH_USER", "CH_PASSWORD")
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        raise SystemExit("missing ClickHouse environment: " + ", ".join(missing))
    command = [
        "clickhouse-client", "--host", os.environ["CH_HOST"],
        "--port", os.environ["CH_NATIVE_PORT"], "--user", os.environ["CH_USER"],
        "--password", os.environ["CH_PASSWORD"], "--query", maker_borrow_sql(),
    ]
    result = subprocess.run(command, check=True, capture_output=True, text=True)
    output: dict[str, list[tuple[Date, float]]] = defaultdict(list)
    for row in csv.DictReader(io.StringIO(result.stdout)):
        output[row["symbol"]].append(
            (Date.fromisoformat(row["trading_date"]), float(row["ending_balance"]))
        )
    return output


def proxy_values(history: list[tuple[Date, float]], signal_date: Date) -> dict[str, int]:
    prior = [(day, balance) for day, balance in history if day < signal_date]
    recent_start = signal_date - timedelta(days=60)
    return {
        "ever_prior": int(bool(prior)),
        "positive_60d_prior": int(any(recent_start <= day < signal_date and balance > 0 for day, balance in prior)),
    }


def add_factor_and_proxies(
    rows: list[dict[str, object]], history: dict[str, list[tuple[Date, float]]]
) -> list[dict[str, object]]:
    output: list[dict[str, object]] = []
    for row in rows:
        if not str(row["domain"]).endswith("/star_ge_10"):
            continue
        flow = float(row["flow5"])
        direction = 1.0 if flow > 0 else -1.0 if flow < 0 else 0.0
        active_volume = float(row["buy5"]) if direction >= 0 else float(row["sell5"])
        opponent_depth = float(row["ask5"]) if direction >= 0 else float(row["bid5"])
        score = -direction * math.log1p(active_volume / max(opponent_depth, 1e-12))
        signal_date = datetime.strptime(str(row["date"]), "%Y%m%d").date()
        row["candidate"] = {FACTOR: score}
        row["maker_borrow_proxy"] = proxy_values(history.get(str(row["symbol"]), []), signal_date)
        output.append(row)
    return output


def grouped_performance(
    rows: list[dict[str, object]], proxy: str
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    detail: list[dict[str, object]] = []
    by_state_date: dict[tuple[int, int], list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        state = int(row["maker_borrow_proxy"][proxy])  # type: ignore[index]
        by_state_date[(state, int(row["date"]))].append(row)
    for (state, _date), date_rows in sorted(by_state_date.items()):
        state_name = "proxy_yes" if state else "proxy_no"
        by_domain: dict[str, list[dict[str, object]]] = defaultdict(list)
        for row in date_rows:
            by_domain[str(row["domain"])].append(row)
        for domain, domain_rows in sorted(by_domain.items()):
            evaluate_group(
                domain_rows, scope=f"{proxy}/{state_name}/domain", domain=domain,
                factors=(FACTOR,), targets=(TARGET,), output=detail,
            )
        pooled_rows: list[dict[str, object]] = []
        pooled_scores: list[float] = []
        for domain_rows in by_domain.values():
            if len(domain_rows) < 15:
                continue
            pooled_rows.extend(domain_rows)
            pooled_scores.extend(percentile_ranks([
                float(row["candidate"][FACTOR]) for row in domain_rows  # type: ignore[index]
            ]))
        if pooled_rows:
            for row, score in zip(pooled_rows, pooled_scores):
                row["candidate"][f"{FACTOR}_pooled_rank"] = score  # type: ignore[index]
            evaluate_group(
                pooled_rows, scope=f"{proxy}/{state_name}/star_domain_rank_aggregate",
                domain="all_star_domains", factors=(f"{FACTOR}_pooled_rank",),
                targets=(TARGET,), output=detail,
            )
            for row in pooled_rows:
                row["candidate"].pop(f"{FACTOR}_pooled_rank")  # type: ignore[index]
    return detail, summarize(detail)


def performance_differences(
    detail: list[dict[str, object]], proxy: str
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    paired: dict[tuple[str, str, int], dict[str, dict[str, object]]] = defaultdict(dict)
    prefix = f"{proxy}/"
    for row in detail:
        scope = str(row["scope"])
        if not scope.startswith(prefix):
            continue
        state, remainder = scope[len(prefix):].split("/", 1)
        paired[(remainder, str(row["domain"]), int(row["date"]))][state] = row
    daily: list[dict[str, object]] = []
    for (scope, domain, signal_date), states in sorted(paired.items()):
        if set(states) != {"proxy_yes", "proxy_no"}:
            continue
        yes = states["proxy_yes"]
        no = states["proxy_no"]
        daily.append({
            "proxy": proxy, "scope": scope, "domain": domain, "date": signal_date,
            "yes_n": yes["n"], "no_n": no["n"],
            "rank_ic_yes_minus_no": float(yes["rank_ic"]) - float(no["rank_ic"]),
            "d10_d1_yes_minus_no": float(yes["d10_d1"]) - float(no["d10_d1"]),
        })
    grouped: dict[tuple[str, str], list[dict[str, object]]] = defaultdict(list)
    for row in daily:
        grouped[(str(row["scope"]), str(row["domain"]))].append(row)
    summary: list[dict[str, object]] = []
    for (scope, domain), observations in sorted(grouped.items()):
        record: dict[str, object] = {
            "proxy": proxy, "scope": scope, "domain": domain,
            "n_dates": len(observations),
            "yes_n": sum(int(row["yes_n"]) for row in observations),
            "no_n": sum(int(row["no_n"]) for row in observations),
        }
        for metric in ("rank_ic_yes_minus_no", "d10_d1_yes_minus_no"):
            values = [float(row[metric]) for row in observations]
            record[metric], record[f"{metric}_t"] = mean_t(values)
        summary.append(record)
    return daily, summary


def solve_linear(matrix: list[list[float]], vector: list[float]) -> list[float] | None:
    size = len(vector)
    augmented = [matrix[index][:] + [vector[index]] for index in range(size)]
    for column in range(size):
        pivot = max(range(column, size), key=lambda row: abs(augmented[row][column]))
        if abs(augmented[pivot][column]) < 1e-12:
            return None
        augmented[column], augmented[pivot] = augmented[pivot], augmented[column]
        scale = augmented[column][column]
        augmented[column] = [value / scale for value in augmented[column]]
        for row in range(size):
            if row == column:
                continue
            scale = augmented[row][column]
            augmented[row] = [
                value - scale * pivot_value
                for value, pivot_value in zip(augmented[row], augmented[column])
            ]
    return [augmented[index][-1] for index in range(size)]


def interaction_coefficients(
    rows: list[dict[str, object]], proxy: str
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    by_date: dict[int, list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        by_date[int(row["date"])].append(row)
    daily: list[dict[str, object]] = []
    for signal_date, date_rows in sorted(by_date.items()):
        vectors: list[tuple[float, float, float, float]] = []
        by_domain: dict[str, list[dict[str, object]]] = defaultdict(list)
        for row in date_rows:
            by_domain[str(row["domain"])].append(row)
        for domain_rows in by_domain.values():
            if len(domain_rows) < 15:
                continue
            scores = percentile_ranks([
                float(row["candidate"][FACTOR]) for row in domain_rows  # type: ignore[index]
            ])
            returns = [float(row["targets"][TARGET]) for row in domain_rows]  # type: ignore[index]
            makers = [float(row["maker_borrow_proxy"][proxy]) for row in domain_rows]  # type: ignore[index]
            mean_return = sum(returns) / len(returns)
            mean_maker = sum(makers) / len(makers)
            interactions = [(2.0 * score - 1.0) * maker for score, maker in zip(scores, makers)]
            mean_interaction = sum(interactions) / len(interactions)
            for score, target, maker, interaction in zip(scores, returns, makers, interactions):
                vectors.append((2.0 * score - 1.0, maker - mean_maker, interaction - mean_interaction, target - mean_return))
        if len(vectors) < 30 or len({vector[1] for vector in vectors}) < 2:
            continue
        predictors = [vector[:3] for vector in vectors]
        targets = [vector[3] for vector in vectors]
        cross = [[sum(row[i] * row[j] for row in predictors) for j in range(3)] for i in range(3)]
        rhs = [sum(row[i] * target for row, target in zip(predictors, targets)) for i in range(3)]
        coefficients = solve_linear(cross, rhs)
        if coefficients is None:
            continue
        daily.append({
            "proxy": proxy, "date": signal_date, "n": len(vectors),
            "factor_slope": coefficients[0], "proxy_level": coefficients[1],
            "factor_x_proxy": coefficients[2],
        })
    interactions = [float(row["factor_x_proxy"]) for row in daily]
    average, t_value = mean_t(interactions)
    summary = [{
        "proxy": proxy, "n_obs": sum(int(row["n"]) for row in daily),
        "factor_x_proxy": average, "factor_x_proxy_t": t_value,
        "n_dates": len(interactions),
    }]
    return daily, summary


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

    history = query_maker_borrow()
    rows = add_factor_and_proxies(load_rows(args.window_parquet, args.candidates, args.return_prices), history)
    if len(rows) < 10_000:
        raise ValueError(f"unexpected STAR coverage: {len(rows)}")
    performance: list[dict[str, object]] = []
    performance_summary: list[dict[str, object]] = []
    differences: list[dict[str, object]] = []
    difference_summary: list[dict[str, object]] = []
    interactions: list[dict[str, object]] = []
    interaction_summary: list[dict[str, object]] = []
    for proxy in PROXY_DEFINITIONS:
        detail, proxy_performance_summary = grouped_performance(rows, proxy)
        performance.extend(detail)
        performance_summary.extend(proxy_performance_summary)
        proxy_differences, proxy_difference_summary = performance_differences(detail, proxy)
        differences.extend(proxy_differences)
        difference_summary.extend(proxy_difference_summary)
        proxy_interactions, proxy_interaction_summary = interaction_coefficients(rows, proxy)
        interactions.extend(proxy_interactions)
        interaction_summary.extend(proxy_interaction_summary)

    sample_counts: list[dict[str, object]] = []
    for proxy in PROXY_DEFINITIONS:
        for state in (0, 1):
            subset = [row for row in rows if int(row["maker_borrow_proxy"][proxy]) == state]  # type: ignore[index]
            sample_counts.append({
                "proxy": proxy, "state": state, "stock_days": len(subset),
                "symbols": len({str(row["symbol"]) for row in subset}),
                "dates": len({int(row["date"]) for row in subset}),
            })

    args.output_dir.mkdir(parents=True)
    write_csv(args.output_dir / "proxy_group_performance_by_slice.csv", performance)
    write_csv(args.output_dir / "proxy_group_performance_summary.csv", performance_summary)
    write_csv(args.output_dir / "proxy_group_differences_by_slice.csv", differences)
    write_csv(args.output_dir / "proxy_group_differences_summary.csv", difference_summary)
    write_csv(args.output_dir / "factor_proxy_interactions_by_date.csv", interactions)
    write_csv(args.output_dir / "factor_proxy_interactions_summary.csv", interaction_summary)
    write_csv(args.output_dir / "sample_counts.csv", sample_counts)
    query = maker_borrow_sql()
    manifest = {
        "kind": "research_result", "status": "completed", "research_id": "R017",
        "study": "flow_to_opponent_depth_market_making_borrow_proxy_5m",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "research_run": str(args.research_run.resolve()), "research_run_sha256": sha256(args.research_run),
        "implementation": str(Path(__file__).resolve()), "implementation_sha256": sha256(Path(__file__).resolve()),
        "window_completion": str(args.window_completion.resolve()), "window_completion_sha256": sha256(args.window_completion),
        "candidate_completion": str(args.candidate_completion.resolve()), "candidate_completion_sha256": sha256(args.candidate_completion),
        "window_parquet": str(args.window_parquet.resolve()), "window_parquet_sha256": sha256(args.window_parquet),
        "candidates": str(args.candidates.resolve()), "candidates_sha256": sha256(args.candidates),
        "return_cache_manifest": str(args.return_cache_manifest.resolve()),
        "return_cache_manifest_sha256": sha256(args.return_cache_manifest),
        "source_table": SOURCE_TABLE, "source_query_sha256": hashlib.sha256(query.encode()).hexdigest(),
        "proxy_definitions": PROXY_DEFINITIONS,
        "proxy_warning": "activity proxy, not an official market-maker assignment flag; proxy_no does not mean no market maker",
        "point_in_time_rule": "only maker-borrow rows strictly before each 10:30 signal date",
        "factor": FACTOR, "target": TARGET, "signal_cutoff": "10:30:00",
        "entry_rule": "10:31 minute close", "primary_scope": "raw non-neutralized STAR domains",
        "future_filter_used": False, "months": [202601], "rows": len(rows),
        "source_symbols": len(history),
    }
    (args.output_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8",
    )
    print(json.dumps({"rows": len(rows), "source_symbols": len(history), "output": str(args.output_dir)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
