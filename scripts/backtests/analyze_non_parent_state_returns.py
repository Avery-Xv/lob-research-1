#!/usr/bin/env python3
"""Run January-only R017/R026 state and return experiments on immutable inputs."""

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

import duckdb


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.backtests.backtest_non_parent_direct_targets import run_continuous, summarize
from scripts.backtests.backtest_order_shape_batch_a_domains import mean_t, write_csv
from scripts.factors.order_shape_non_parent.candidates import quantile, sha256


STUDY_RESEARCH = {
    "quadrants": "R017",
    "absorption_returns": "R017",
    "fillability_conditioning": "R015",
}


def validate_json(path: Path, *, kind: str | None = None) -> dict[str, object]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if kind is not None and value.get("kind") != kind:
        raise ValueError(f"unexpected manifest kind in {path}: {value.get('kind')}")
    return value


def sign(value: float) -> float:
    return 1.0 if value > 0 else -1.0 if value < 0 else 0.0


def load_rows(window_path: Path, candidates: Path, prices: Path) -> list[dict[str, object]]:
    connection = duckdb.connect()
    selected = connection.execute("""
        SELECT w.symbol,w.date,c.exchange,c.domain,
               w.flow5m_net_share,w.flow30m_net_share,
               w.flow5m_buy_volume,w.flow5m_sell_volume,
               w.book5m_bi3_twap,w.book30m_bi3_twap,
               w.book5m_bid3_twap,w.book5m_ask3_twap,
               w.book30m_bid3_twap,w.book30m_ask3_twap,
               w.flow_shift_5m_minus_30m,w.book_shift_5m_minus_30m,
               w.future1m_net_share,w.future5m_net_share,w.future10m_net_share,
               w.future10m_realized_vol_bps,
               c.np02_activity_residual,
               p.close_1025,p.close_1030,p.close_1031,p.close_1035,
               p.close_1040,p.close_1100,p.close_1500
        FROM read_parquet(?) w
        INNER JOIN read_csv_auto(?, header=true) c USING(symbol,date)
        INNER JOIN read_csv_auto(?, header=true) p USING(symbol,date)
        WHERE w.book30m_coverage_ratio>=0.999999
          AND w.book5m_coverage_ratio>=0.999999
        ORDER BY w.date,c.domain,w.symbol
    """, [str(window_path), str(candidates), str(prices)]).fetchall()
    columns = [item[0] for item in connection.description]
    connection.close()
    rows: list[dict[str, object]] = []
    seen: set[tuple[str, int]] = set()
    for values in selected:
        source = dict(zip(columns, values))
        key = (str(source["symbol"]), int(source["date"]))
        if key in seen:
            raise ValueError(f"duplicate joined key: {key}")
        seen.add(key)
        flow5 = float(source["flow5m_net_share"]); direction = sign(flow5)
        entry = float(source["close_1031"])
        targets = {
            "future1m_net_share": float(source["future1m_net_share"]),
            "future5m_net_share": float(source["future5m_net_share"]),
            "future10m_net_share": float(source["future10m_net_share"]),
            "future10m_aligned_flow5m": direction * float(source["future10m_net_share"]),
            "log_future10m_realized_vol": math.log1p(float(source["future10m_realized_vol_bps"])),
        }
        for time in (1035, 1040, 1100, 1500):
            raw_return = float(source[f"close_{time}"]) / entry - 1.0
            targets[f"ret_1031_{time}"] = raw_return
            targets[f"aligned_ret_1031_{time}"] = direction * raw_return
        row = {
            "symbol": key[0], "date": key[1], "signal_time": 1030,
            "exchange": str(source["exchange"]), "domain": str(source["domain"]),
            "flow5": flow5, "flow30": float(source["flow30m_net_share"]),
            "buy5": float(source["flow5m_buy_volume"]), "sell5": float(source["flow5m_sell_volume"]),
            "book5": float(source["book5m_bi3_twap"]), "book30": float(source["book30m_bi3_twap"]),
            "bid5": float(source["book5m_bid3_twap"]), "ask5": float(source["book5m_ask3_twap"]),
            "bid30": float(source["book30m_bid3_twap"]), "ask30": float(source["book30m_ask3_twap"]),
            "flow_shift": float(source["flow_shift_5m_minus_30m"]),
            "book_shift": float(source["book_shift_5m_minus_30m"]),
            "fillability": float(source["np02_activity_residual"]),
            "past_return": float(source["close_1030"]) / float(source["close_1025"]) - 1.0,
            "targets": targets,
        }
        rows.append(row)
    if len(rows) < 95_000:
        raise ValueError(f"unexpected joined coverage: {len(rows)}")
    return rows


def tercile(value: float, low: float, high: float) -> str:
    return "low" if value <= low else "high" if value >= high else "mid"


def enrich(rows: list[dict[str, object]], study: str) -> None:
    grouped: dict[tuple[int, str], list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        grouped[(int(row["date"]), str(row["domain"]))].append(row)
    for group in grouped.values():
        fields = ("flow5", "book5", "flow_shift", "book_shift", "fillability")
        cuts = {
            field: (quantile((float(row[field]) for row in group), 1 / 3), quantile((float(row[field]) for row in group), 2 / 3))
            for field in fields
        }
        aligned_returns = [sign(float(row["flow5"])) * float(row["past_return"]) for row in group]
        response_low = quantile(aligned_returns, 1 / 3)
        depth_ratios = []
        for row in group:
            if float(row["flow5"]) >= 0:
                ratio = float(row["ask5"]) / max(float(row["ask30"]), 1e-12)
            else:
                ratio = float(row["bid5"]) / max(float(row["bid30"]), 1e-12)
            depth_ratios.append(ratio)
        depth_median = quantile(depth_ratios, 0.5)
        for index, row in enumerate(group):
            flow = float(row["flow5"]); book = float(row["book5"]); direction = sign(flow)
            flow_band = tercile(flow, *cuts["flow5"]); book_band = tercile(book, *cuts["book5"])
            if flow_band in {"low", "high"} and book_band in {"low", "high"}:
                flow_name = "sell" if flow_band == "low" else "buy"
                book_name = "ask" if book_band == "low" else "bid"
                quadrant = f"active_{flow_name}_book_{book_name}"
            else:
                quadrant = "other"
            flow_shift = tercile(float(row["flow_shift"]), *cuts["flow_shift"])
            book_shift = tercile(float(row["book_shift"]), *cuts["book_shift"])
            path_state = f"flow_shift_{flow_shift}_book_shift_{book_shift}"
            aligned_book = direction * book
            aligned_response = direction * float(row["past_return"])
            strong_flow = abs(flow) >= quantile((abs(float(item["flow5"])) for item in group), 2 / 3)
            absorbed = strong_flow and aligned_book < 0 and aligned_response <= response_low
            if absorbed:
                absorption_state = "absorbed_depleting" if depth_ratios[index] <= depth_median else "absorbed_replenishing"
            elif strong_flow:
                absorption_state = "strong_flow_not_absorbed"
            else:
                absorption_state = "other"
            opponent_depth = float(row["ask5"]) if direction >= 0 else float(row["bid5"])
            active_volume = float(row["buy5"]) if direction >= 0 else float(row["sell5"])
            pressure = math.log1p(active_volume / max(opponent_depth, 1e-12))
            opposing_book_strength = max(0.0, -aligned_book)
            depletion = max(0.0, 1.0 - depth_ratios[index])
            replenishment = max(0.0, depth_ratios[index] - 1.0)
            row["quadrant_state"] = quadrant
            row["path_state"] = path_state
            row["absorption_state"] = absorption_state
            row["fillability_state"] = tercile(float(row["fillability"]), *cuts["fillability"])
            row["candidate"] = {
                "flow5_raw": flow,
                "minus_book5": -book,
                "joint_flow_minus_book": flow - book,
                "flow_shift": float(row["flow_shift"]),
                "minus_book_shift": -float(row["book_shift"]),
                "flow_to_opponent_depth": direction * pressure,
                "breakout_score": direction * abs(flow) * opposing_book_strength * depletion,
                "replenishing_absorption_score": direction * abs(flow) * opposing_book_strength * replenishment,
            }


def state_statistics(rows: list[dict[str, object]], state_columns: tuple[str, ...], targets: tuple[str, ...]) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    detail: list[dict[str, object]] = []
    for state_column in state_columns:
        grouped: dict[tuple[int, str, str], list[dict[str, object]]] = defaultdict(list)
        for row in rows:
            state = str(row[state_column])
            if state != "other":
                grouped[(int(row["date"]), str(row["domain"]), state)].append(row)
        for (date, domain, state), observations in sorted(grouped.items()):
            for target in targets:
                values = [float(row["targets"][target]) for row in observations]  # type: ignore[index]
                if values:
                    detail.append({"state_type": state_column, "date": date, "domain": domain, "state": state, "target": target, "n": len(values), "mean_target": mean(values)})
    summary: list[dict[str, object]] = []
    pooled: dict[tuple[str, str, str, str], list[dict[str, object]]] = defaultdict(list)
    for row in detail:
        pooled[(str(row["state_type"]), str(row["domain"]), str(row["state"]), str(row["target"]))].append(row)
    for key, observations in sorted(pooled.items()):
        values = [float(row["mean_target"]) for row in observations]
        average, t_value = mean_t(values)
        summary.append({"state_type": key[0], "domain": key[1], "state": key[2], "target": key[3], "n_dates": len(values), "n_obs": sum(int(row["n"]) for row in observations), "mean_target": average, "mean_target_t": t_value})
    return detail, summary


def conditioned_performance(rows: list[dict[str, object]], factors: tuple[str, ...], targets: tuple[str, ...]) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    performance: list[dict[str, object]] = []
    for state in ("low", "mid", "high"):
        subset = [row for row in rows if row["fillability_state"] == state]
        results = run_continuous(subset, {"factors": factors, "targets": targets})
        for result in results:
            result["scope"] = f"fillability_{state}/{result['scope']}"
        performance.extend(results)
    return performance, summarize(performance)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--study", choices=sorted(STUDY_RESEARCH), required=True)
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
    expected_research = STUDY_RESEARCH[args.study]
    if run.get("research_id") != expected_research:
        raise ValueError(f"study {args.study} requires {expected_research}")
    for completion_path in (args.window_completion, args.candidate_completion):
        completion = validate_json(completion_path, kind="factor_run_completion")
        if completion.get("status") != "completed_audited" or completion.get("factor_id") != "F014":
            raise ValueError(f"not completed_audited F014: {completion_path}")
    cache = validate_json(args.return_cache_manifest, kind="research_label_cache")
    if sha256(args.return_prices) != cache.get("output_sha256"):
        raise ValueError("return price cache hash mismatch")
    rows = load_rows(args.window_parquet, args.candidates, args.return_prices)
    enrich(rows, args.study)
    args.output_dir.mkdir(parents=True)
    if args.study == "quadrants":
        factors = ("flow5_raw", "minus_book5", "joint_flow_minus_book", "flow_shift", "minus_book_shift")
        targets = ("future1m_net_share", "future5m_net_share", "future10m_net_share", "future10m_aligned_flow5m", "log_future10m_realized_vol")
        performance = run_continuous(rows, {"factors": factors, "targets": targets})
        summary = summarize(performance)
        detail, state_summary = state_statistics(rows, ("quadrant_state", "path_state"), targets)
    elif args.study == "absorption_returns":
        factors = ("flow5_raw", "joint_flow_minus_book", "flow_to_opponent_depth", "breakout_score", "replenishing_absorption_score")
        targets = ("ret_1031_1035", "ret_1031_1040", "ret_1031_1100", "ret_1031_1500")
        performance = run_continuous(rows, {"factors": factors, "targets": targets})
        summary = summarize(performance)
        detail, state_summary = state_statistics(rows, ("absorption_state",), tuple(f"aligned_{target}" for target in targets) + ("future10m_aligned_flow5m",))
    else:
        factors = ("flow5_raw", "joint_flow_minus_book", "flow_to_opponent_depth")
        targets = ("ret_1031_1035", "ret_1031_1040", "ret_1031_1100", "ret_1031_1500")
        performance, summary = conditioned_performance(rows, factors, targets)
        detail, state_summary = state_statistics(rows, ("fillability_state",), tuple(f"aligned_{target}" for target in targets) + ("future10m_aligned_flow5m",))
    write_csv(args.output_dir / "performance_by_slice.csv", performance)
    write_csv(args.output_dir / "performance_summary.csv", summary)
    write_csv(args.output_dir / "state_by_slice.csv", detail)
    write_csv(args.output_dir / "state_summary.csv", state_summary)
    manifest = {
        "kind": "research_result", "status": "completed",
        "research_id": expected_research, "study": args.study,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "research_run": str(args.research_run.resolve()), "research_run_sha256": sha256(args.research_run),
        "implementation": str(Path(__file__).resolve()), "implementation_sha256": sha256(Path(__file__).resolve()),
        "window_completion": str(args.window_completion.resolve()), "window_completion_sha256": sha256(args.window_completion),
        "candidate_completion": str(args.candidate_completion.resolve()), "candidate_completion_sha256": sha256(args.candidate_completion),
        "window_parquet": str(args.window_parquet.resolve()), "window_parquet_sha256": sha256(args.window_parquet),
        "candidates": str(args.candidates.resolve()), "candidates_sha256": sha256(args.candidates),
        "return_cache_manifest": str(args.return_cache_manifest.resolve()), "return_cache_manifest_sha256": sha256(args.return_cache_manifest),
        "primary_scope": "raw non-neutralized; frozen nine domains primary; exchange auxiliary",
        "signal_cutoff": "10:30:00", "entry_rule": "10:31 minute close",
        "future_filter_used": False, "months": [202601], "rows": len(rows),
        "performance_rows": len(performance), "summary_rows": len(summary), "state_rows": len(detail),
    }
    (args.output_dir / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"research_id": expected_research, "study": args.study, "rows": len(rows), "output": str(args.output_dir)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
