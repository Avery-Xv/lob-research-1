#!/usr/bin/env python3
"""Evaluate one F014 research question on fixed-10:30 P002 direct targets."""

from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.backtests.backtest_order_shape_batch_a_domains import (
    mean_t, metric_row, percentile_ranks, write_csv,
)
from scripts.factors.order_shape_non_parent.candidates import sha256


RESEARCH = {
    "R014": {
        "factors": ("np01_execution_pressure", "np01_m1_linear", "np01_m1_cubic"),
        "targets": ("future_net_share", "log_future_active_volume", "log_future_event_count"),
    },
    "R015": {
        "factors": ("np02_fillability", "np02_logit_fillability", "np02_activity_residual"),
        "targets": ("log_future_active_volume", "log_future_event_count", "log_future_realized_vol", "spread_change_bps", "log_depth_change"),
    },
    "R016": {
        "factors": ("np03_confirmation",),
        "targets": ("future_flow_aligned_m1", "log_future_active_volume", "log_future_realized_vol"),
        "state": "np03_state",
    },
    "R017": {
        "factors": ("np04_flow_minus_book", "book_imbalance3"),
        "targets": ("future_net_share", "end_book_imbalance3", "log_future_realized_vol"),
        "state": "np04_state",
    },
    "R018": {
        "factors": ("np05_cancel_intensity", "np05_abs_cancel_imbalance", "np05_signed_cancel_imbalance", "np05_buy_cancel_shock", "np05_sell_cancel_shock"),
        "targets": ("log_future_realized_vol", "spread_change_bps", "log_depth_change"),
    },
}


def target_values(row: dict[str, str]) -> dict[str, float | None]:
    buy = float(row["future_buy_volume"]); sell = float(row["future_sell_volume"])
    total = buy + sell
    start_depth = float(row["bid_depth3"]) + float(row["ask_depth3"])
    end_bid = float(row["end_bid_depth3"]); end_ask = float(row["end_ask_depth3"])
    end_depth = end_bid + end_ask
    return {
        "future_net_share": (buy - sell) / total if total > 0 else None,
        "log_future_active_volume": math.log1p(total),
        "log_future_event_count": math.log1p(float(row["future_event_count"])),
        "log_future_realized_vol": math.log1p(float(row["future_realized_vol_bps"])),
        "spread_change_bps": float(row["end_spread_bps"]) - float(row["spread_bps"]),
        "log_depth_change": math.log(end_depth / start_depth) if start_depth > 0 and end_depth > 0 else None,
        "end_book_imbalance3": (end_bid - end_ask) / end_depth if end_depth > 0 else None,
    }


def load_targets(root: Path) -> dict[tuple[str, int, int], dict[str, float | None]]:
    output = {}
    paths = sorted(root.glob("part_*/batch_*/signals.csv"))
    if len(paths) != 5160:
        raise ValueError(f"expected 5160 source files, found {len(paths)}")
    for path in paths:
        with path.open(newline="", encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                key = (row["symbol"], int(row["date"]), int(row["signal_time"]))
                if key in output:
                    raise ValueError(f"duplicate target row: {key}")
                output[key] = target_values(row)
    return output


def load_rows(candidate_path: Path, target_root: Path) -> list[dict[str, object]]:
    targets = load_targets(target_root)
    output = []
    with candidate_path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            key = (row["symbol"], int(row["date"]), int(row["signal_time"]))
            if key not in targets:
                raise ValueError(f"candidate has no source target: {key}")
            row_targets = dict(targets[key])
            future_net = row_targets["future_net_share"]
            m1 = float(row["m1"])
            direction = 1.0 if m1 > 0 else -1.0 if m1 < 0 else 0.0
            row_targets["future_flow_aligned_m1"] = direction * float(future_net) if future_net is not None else None
            output.append({
                "symbol": row["symbol"], "date": key[1], "signal_time": key[2],
                "exchange": row["exchange"], "domain": row["domain"],
                "candidate": row, "targets": row_targets,
            })
    return output


def evaluate_group(
    rows: list[dict[str, object]], *, scope: str, domain: str,
    factors: tuple[str, ...], targets: tuple[str, ...], output: list[dict[str, object]],
) -> None:
    if len(rows) < 15:
        return
    date = int(rows[0]["date"]); signal_time = int(rows[0]["signal_time"])
    for factor in factors:
        scores = [float(row["candidate"][factor]) for row in rows]  # type: ignore[index]
        for target in targets:
            result = metric_row(
                scope=scope, domain=domain, date=date, signal_time=signal_time,
                factor=factor, target=target, rows=rows, raw_scores=scores, neutral_scores=scores,
            )
            if result is None:
                continue
            result["rank_ic"] = result.pop("raw_rank_ic")
            result["d10_d1"] = result.pop("raw_d10_d1")
            result.pop("neutral_rank_ic"); result.pop("neutral_d10_d1")
            output.append(result)


def run_continuous(rows: list[dict[str, object]], config: dict[str, object]) -> list[dict[str, object]]:
    factors = config["factors"]; targets = config["targets"]
    assert isinstance(factors, tuple) and isinstance(targets, tuple)
    by_date: dict[int, list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        by_date[int(row["date"])].append(row)
    output: list[dict[str, object]] = []
    for date, date_rows in sorted(by_date.items()):
        by_domain: dict[str, list[dict[str, object]]] = defaultdict(list)
        for row in date_rows:
            by_domain[str(row["domain"])].append(row)
        for domain, domain_rows in sorted(by_domain.items()):
            evaluate_group(domain_rows, scope="domain", domain=domain, factors=factors, targets=targets, output=output)
        for factor in factors:
            pooled_rows: list[dict[str, object]] = []
            pooled_scores: list[float] = []
            for domain, domain_rows in sorted(by_domain.items()):
                if len(domain_rows) < 15:
                    continue
                pooled_rows.extend(domain_rows)
                pooled_scores.extend(percentile_ranks([float(row["candidate"][factor]) for row in domain_rows]))  # type: ignore[index]
            for target in targets:
                result = metric_row(
                    scope="domain_rank_aggregate", domain="all_nine_domains", date=date,
                    signal_time=1030, factor=factor, target=target, rows=pooled_rows,
                    raw_scores=pooled_scores, neutral_scores=pooled_scores,
                )
                if result is not None:
                    result["rank_ic"] = result.pop("raw_rank_ic"); result["d10_d1"] = result.pop("raw_d10_d1")
                    result.pop("neutral_rank_ic"); result.pop("neutral_d10_d1"); output.append(result)
        by_exchange: dict[str, list[dict[str, object]]] = defaultdict(list)
        for row in date_rows:
            by_exchange[str(row["exchange"])].append(row)
        for exchange, exchange_rows in sorted(by_exchange.items()):
            evaluate_group(exchange_rows, scope="exchange_auxiliary", domain=exchange, factors=factors, targets=targets, output=output)
        evaluate_group(date_rows, scope="all_market_diagnostic", domain="all/all", factors=factors, targets=targets, output=output)
    return output


def summarize(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    grouped: dict[tuple[str, str, str, str], dict[int, list[dict[str, object]]]] = defaultdict(lambda: defaultdict(list))
    for row in rows:
        grouped[(str(row["scope"]), str(row["domain"]), str(row["factor"]), str(row["target"]))][int(row["date"])].append(row)
    output = []
    for key, dates in sorted(grouped.items()):
        daily: list[dict[str, float | None]] = []
        for observations in dates.values():
            daily_record: dict[str, float | None] = {}
            for metric in ("rank_ic", "d10_d1"):
                metric_values = [float(row[metric]) for row in observations if row[metric] is not None]
                daily_record[metric] = mean(metric_values) if metric_values else None
            daily.append(daily_record)
        record: dict[str, object] = {"scope": key[0], "domain": key[1], "factor": key[2], "target": key[3], "n_dates": len(daily), "n_obs": sum(int(row["n"]) for observations in dates.values() for row in observations)}
        for metric in ("rank_ic", "d10_d1"):
            values = [float(row[metric]) for row in daily if row[metric] is not None]
            record[metric], record[f"{metric}_t"] = mean_t(values)
            record[f"{metric}_positive_date_share"] = sum(value > 0 for value in values) / len(values) if values else None
        output.append(record)
    return output


def state_rows(rows: list[dict[str, object]], state_column: str, targets: tuple[str, ...]) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    detail = []
    grouped: dict[tuple[int, str, str], list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        state = str(row["candidate"][state_column])  # type: ignore[index]
        if state not in {"other", "insufficient_slice"}:
            grouped[(int(row["date"]), str(row["domain"]), state)].append(row)
    for (date, domain, state), observations in sorted(grouped.items()):
        for target in targets:
            values = [float(row["targets"][target]) for row in observations if row["targets"][target] is not None]  # type: ignore[index]
            if values:
                detail.append({"date": date, "domain": domain, "state": state, "target": target, "n": len(values), "mean_target": mean(values)})
    summary = []
    pooled: dict[tuple[str, str, str], list[dict[str, object]]] = defaultdict(list)
    for row in detail:
        pooled[(str(row["domain"]), str(row["state"]), str(row["target"]))].append(row)
    for key, observations in sorted(pooled.items()):
        values = [float(row["mean_target"]) for row in observations]
        average, t_value = mean_t(values)
        summary.append({"domain": key[0], "state": key[1], "target": key[2], "n_dates": len(values), "n_obs": sum(int(row["n"]) for row in observations), "mean_target": average, "mean_target_t": t_value})
    return detail, summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--research-id", choices=sorted(RESEARCH), required=True)
    parser.add_argument("--candidates", type=Path, required=True)
    parser.add_argument("--candidate-completion", type=Path, required=True)
    parser.add_argument("--target-root", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    if args.output_dir.exists():
        raise SystemExit(f"Refusing to overwrite output: {args.output_dir}")
    completion = json.loads(args.candidate_completion.read_text(encoding="utf-8"))
    if completion.get("status") != "completed_audited" or completion.get("factor_id") != "F014":
        raise SystemExit("candidate completion is not completed_audited F014")
    rows = load_rows(args.candidates, args.target_root)
    config = RESEARCH[args.research_id]
    performance = run_continuous(rows, config)
    performance_summary = summarize(performance)
    args.output_dir.mkdir(parents=True)
    write_csv(args.output_dir / "performance_by_slice.csv", performance)
    write_csv(args.output_dir / "performance_summary.csv", performance_summary)
    state_column = config.get("state")
    if isinstance(state_column, str):
        detail, state_summary = state_rows(rows, state_column, config["targets"])  # type: ignore[arg-type]
        write_csv(args.output_dir / "state_by_slice.csv", detail)
        write_csv(args.output_dir / "state_summary.csv", state_summary)
    manifest = {
        "kind": "research_result", "research_id": args.research_id,
        "research_implementation": str(Path(__file__).resolve()),
        "research_implementation_sha256": sha256(Path(__file__).resolve()),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "candidate_completion": str(args.candidate_completion.resolve()),
        "candidate_completion_sha256": sha256(args.candidate_completion),
        "candidate_file": str(args.candidates.resolve()), "candidate_file_sha256": sha256(args.candidates),
        "primary_scope": "nine domains, raw non-neutralized direct targets",
        "aggregation": "daily cross-sectional Rank IC and D10-D1; domain results primary; exchange auxiliary",
        "missing_label_policy": "each direct target independently",
        "future_return_used": False, "rows": len(rows),
        "performance_rows": len(performance), "summary_rows": len(performance_summary),
    }
    (args.output_dir / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"research_id": args.research_id, "rows": len(rows), "output": str(args.output_dir)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
