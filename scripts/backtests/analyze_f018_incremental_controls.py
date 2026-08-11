#!/usr/bin/env python3
"""Test F018 return increment after point-in-time intraday controls."""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, stdev
from typing import Sequence

import duckdb


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.backtests.backtest_non_parent_direct_targets import run_continuous, summarize
from scripts.backtests.backtest_order_shape_batch_a_domains import mean_t, write_csv
from scripts.factors.order_shape_non_parent.candidates import (
    orthogonal_basis,
    quantile,
    residualize,
    sha256,
)


TARGETS = ("ret_1031_1035", "ret_1031_1040", "ret_1031_1100", "ret_1031_1500")
RAW_FACTOR = "f018_raw"
WINSOR_FACTOR = "f018_winsor_1_99"
CONTROL_SPECS = {
    "f018_resid_flow_cubic": (
        "flow5",
        "flow5_sq",
        "flow5_cube",
    ),
    "f018_resid_flow_cubic_prereturn": (
        "flow5",
        "flow5_sq",
        "flow5_cube",
        "pre_return_5m",
        "abs_pre_return_5m",
    ),
    "f018_resid_full_intraday_state": (
        "flow5",
        "flow5_sq",
        "flow5_cube",
        "pre_return_5m",
        "abs_pre_return_5m",
        "log_active_volume_5m",
        "log_active_count_5m",
        "log_spread_5m_twap",
        "log_depth3_5m_twap",
    ),
}
FACTORS = (RAW_FACTOR, WINSOR_FACTOR, *CONTROL_SPECS)


def validate_json(path: Path, *, kind: str | None = None) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if kind is not None and payload.get("kind") != kind:
        raise ValueError(f"unexpected manifest kind in {path}: {payload.get('kind')}")
    return payload


def winsorize(values: Sequence[float], lower: float = 0.01, upper: float = 0.99) -> list[float]:
    low = quantile(values, lower)
    high = quantile(values, upper)
    return [min(max(float(value), low), high) for value in values]


def correlation(left: Sequence[float], right: Sequence[float]) -> float | None:
    if len(left) != len(right) or len(left) < 2:
        return None
    left_mean = mean(left)
    right_mean = mean(right)
    left_center = [value - left_mean for value in left]
    right_center = [value - right_mean for value in right]
    denominator = math.sqrt(
        sum(value * value for value in left_center)
        * sum(value * value for value in right_center)
    )
    if denominator <= 1e-20:
        return None
    return sum(a * b for a, b in zip(left_center, right_center)) / denominator


def load_rows(window_path: Path, candidates: Path, prices: Path) -> list[dict[str, object]]:
    connection = duckdb.connect()
    selected = connection.execute("""
        SELECT w.symbol,w.date,c.exchange,c.domain,
               w.flow5m_net_share,w.flow5m_buy_volume,w.flow5m_sell_volume,
               w.flow5m_total_volume,w.flow5m_order_count,
               w.book5m_bid3_twap,w.book5m_ask3_twap,
               w.book5m_spread_bps_twap,
               p.close_1025,p.close_1030,p.close_1031,p.close_1035,
               p.close_1040,p.close_1100,p.close_1500
        FROM read_parquet(?) AS w
        INNER JOIN read_csv_auto(?, header=true) AS c USING(symbol,date)
        INNER JOIN read_csv_auto(?, header=true) AS p USING(symbol,date)
        WHERE w.book30m_coverage_ratio >= 0.999999
          AND w.book5m_coverage_ratio >= 0.999999
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
        flow = float(source["flow5m_net_share"])
        direction = 1.0 if flow > 0 else -1.0 if flow < 0 else 0.0
        active_volume = (
            float(source["flow5m_buy_volume"])
            if direction >= 0 else float(source["flow5m_sell_volume"])
        )
        opponent_depth = (
            float(source["book5m_ask3_twap"])
            if direction >= 0 else float(source["book5m_bid3_twap"])
        )
        f018 = -direction * math.log1p(active_volume / max(opponent_depth, 1e-12))
        pre_return = float(source["close_1030"]) / float(source["close_1025"]) - 1.0
        total_depth = float(source["book5m_bid3_twap"]) + float(source["book5m_ask3_twap"])
        controls = {
            "flow5": flow,
            "flow5_sq": flow * flow,
            "flow5_cube": flow * flow * flow,
            "pre_return_5m": pre_return,
            "abs_pre_return_5m": abs(pre_return),
            "log_active_volume_5m": math.log1p(max(float(source["flow5m_total_volume"]), 0.0)),
            "log_active_count_5m": math.log1p(max(float(source["flow5m_order_count"]), 0.0)),
            "log_spread_5m_twap": math.log1p(max(float(source["book5m_spread_bps_twap"]), 0.0)),
            "log_depth3_5m_twap": math.log(max(total_depth, 1e-12)),
        }
        entry = float(source["close_1031"])
        targets = {
            f"ret_1031_{time}": float(source[f"close_{time}"]) / entry - 1.0
            for time in (1035, 1040, 1100, 1500)
        }
        numeric = [f018, *controls.values(), *targets.values()]
        if not all(math.isfinite(value) for value in numeric):
            raise ValueError(f"non-finite value: {key}")
        rows.append({
            "symbol": key[0],
            "date": key[1],
            "signal_time": 1030,
            "exchange": str(source["exchange"]),
            "domain": str(source["domain"]),
            "f018": f018,
            "controls": controls,
            "targets": targets,
        })
    if len(rows) < 100_000:
        raise ValueError(f"unexpected joined coverage: {len(rows)}")
    return rows


def add_controlled_factors(
    rows: list[dict[str, object]],
) -> list[dict[str, object]]:
    grouped: dict[tuple[int, str], list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        grouped[(int(row["date"]), str(row["domain"]))].append(row)

    diagnostics: list[dict[str, object]] = []
    for (date, domain), group in sorted(grouped.items()):
        raw = [float(row["f018"]) for row in group]
        factor_winsor = winsorize(raw)
        for index, row in enumerate(group):
            row["candidate"] = {
                RAW_FACTOR: raw[index],
                WINSOR_FACTOR: factor_winsor[index],
            }

        factor_mean = mean(factor_winsor)
        factor_centered = [value - factor_mean for value in factor_winsor]
        factor_sst = sum(value * value for value in factor_centered)
        for factor_name, columns in CONTROL_SPECS.items():
            exposure_columns = {
                column: winsorize([
                    float(row["controls"][column])  # type: ignore[index]
                    for row in group
                ])
                for column in columns
            }
            exposures = [
                [exposure_columns[column][index] for column in columns]
                for index in range(len(group))
            ]
            residual = residualize(factor_winsor, exposures)
            basis_width = len(orthogonal_basis(exposures))
            residual_ss = sum(value * value for value in residual)
            r_squared = (
                max(0.0, min(1.0, 1.0 - residual_ss / factor_sst))
                if factor_sst > 1e-20 else 0.0
            )
            correlations = [
                abs(value)
                for column in columns
                if (value := correlation(residual, exposure_columns[column])) is not None
            ]
            diagnostics.append({
                "scope": "domain",
                "domain": domain,
                "date": date,
                "factor": factor_name,
                "n": len(group),
                "declared_controls": len(columns),
                "effective_controls": basis_width,
                "factor_r_squared": r_squared,
                "residual_std": stdev(residual) if len(residual) > 1 else 0.0,
                "max_abs_linear_control_correlation": max(correlations) if correlations else 0.0,
            })
            for index, row in enumerate(group):
                row["candidate"][factor_name] = residual[index]  # type: ignore[index]

    rows.sort(key=lambda row: (int(row["date"]), str(row["domain"]), str(row["symbol"])))
    return diagnostics


def aggregate_diagnostics(
    details: list[dict[str, object]],
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    by_factor_date: dict[tuple[str, int], list[dict[str, object]]] = defaultdict(list)
    for row in details:
        by_factor_date[(str(row["factor"]), int(row["date"]))].append(row)
    aggregates: list[dict[str, object]] = []
    for (factor, date), observations in sorted(by_factor_date.items()):
        aggregates.append({
            "scope": "domain_equal_aggregate",
            "domain": "all_nine_domains",
            "date": date,
            "factor": factor,
            "n": sum(int(row["n"]) for row in observations),
            "declared_controls": mean(float(row["declared_controls"]) for row in observations),
            "effective_controls": mean(float(row["effective_controls"]) for row in observations),
            "factor_r_squared": mean(float(row["factor_r_squared"]) for row in observations),
            "residual_std": mean(float(row["residual_std"]) for row in observations),
            "max_abs_linear_control_correlation": max(
                float(row["max_abs_linear_control_correlation"]) for row in observations
            ),
        })
    combined = details + aggregates
    grouped: dict[tuple[str, str, str], list[dict[str, object]]] = defaultdict(list)
    for row in combined:
        grouped[(str(row["scope"]), str(row["domain"]), str(row["factor"]))].append(row)
    summary: list[dict[str, object]] = []
    for (scope, domain, factor), observations in sorted(grouped.items()):
        r_squared = [float(row["factor_r_squared"]) for row in observations]
        average_r2, r2_t = mean_t(r_squared)
        summary.append({
            "scope": scope,
            "domain": domain,
            "factor": factor,
            "n_dates": len(observations),
            "n_obs": sum(int(row["n"]) for row in observations),
            "mean_effective_controls": mean(
                float(row["effective_controls"]) for row in observations
            ),
            "factor_r_squared": average_r2,
            "factor_r_squared_t": r2_t,
            "mean_residual_std": mean(float(row["residual_std"]) for row in observations),
            "max_abs_linear_control_correlation": max(
                float(row["max_abs_linear_control_correlation"]) for row in observations
            ),
        })
    return combined, summary


def incremental_comparisons(
    performance: list[dict[str, object]],
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    indexed: dict[tuple[str, str, int, str, str], dict[str, object]] = {}
    for row in performance:
        indexed[(
            str(row["scope"]),
            str(row["domain"]),
            int(row["date"]),
            str(row["target"]),
            str(row["factor"]),
        )] = row

    detail: list[dict[str, object]] = []
    for key, controlled in sorted(indexed.items()):
        scope, domain, date, target, factor = key
        if factor in {RAW_FACTOR, WINSOR_FACTOR}:
            continue
        raw = indexed.get((scope, domain, date, target, RAW_FACTOR))
        winsor = indexed.get((scope, domain, date, target, WINSOR_FACTOR))
        if raw is None or winsor is None:
            continue
        detail.append({
            "scope": scope,
            "domain": domain,
            "date": date,
            "target": target,
            "factor": factor,
            "n": controlled["n"],
            "raw_rank_ic": raw["rank_ic"],
            "winsor_rank_ic": winsor["rank_ic"],
            "controlled_rank_ic": controlled["rank_ic"],
            "rank_ic_delta_vs_raw": (
                float(controlled["rank_ic"]) - float(raw["rank_ic"])
                if controlled["rank_ic"] is not None and raw["rank_ic"] is not None else None
            ),
            "raw_d10_d1": raw["d10_d1"],
            "winsor_d10_d1": winsor["d10_d1"],
            "controlled_d10_d1": controlled["d10_d1"],
            "d10_d1_delta_vs_raw": (
                float(controlled["d10_d1"]) - float(raw["d10_d1"])
                if controlled["d10_d1"] is not None and raw["d10_d1"] is not None else None
            ),
        })

    grouped: dict[tuple[str, str, str, str], list[dict[str, object]]] = defaultdict(list)
    for row in detail:
        grouped[(
            str(row["scope"]),
            str(row["domain"]),
            str(row["factor"]),
            str(row["target"]),
        )].append(row)
    summary: list[dict[str, object]] = []
    for (scope, domain, factor, target), observations in sorted(grouped.items()):
        record: dict[str, object] = {
            "scope": scope,
            "domain": domain,
            "factor": factor,
            "target": target,
            "n_dates": len({int(row["date"]) for row in observations}),
            "n_obs": sum(int(row["n"]) for row in observations),
        }
        for metric in (
            "raw_rank_ic",
            "winsor_rank_ic",
            "controlled_rank_ic",
            "rank_ic_delta_vs_raw",
            "raw_d10_d1",
            "winsor_d10_d1",
            "controlled_d10_d1",
            "d10_d1_delta_vs_raw",
        ):
            values = [
                float(row[metric]) for row in observations
                if row[metric] is not None
            ]
            record[metric], record[f"{metric}_t"] = mean_t(values)
        raw_ic = record["raw_rank_ic"]
        controlled_ic = record["controlled_rank_ic"]
        record["rank_ic_retention_vs_raw"] = (
            float(controlled_ic) / float(raw_ic)
            if raw_ic not in (None, 0.0) and controlled_ic is not None else None
        )
        summary.append(record)
    return detail, summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--research-run", type=Path, required=True)
    parser.add_argument("--window-completion", type=Path, required=True)
    parser.add_argument("--candidate-completion", type=Path, required=True)
    parser.add_argument("--factor-spec", type=Path, required=True)
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
    factor_spec = validate_json(args.factor_spec)
    if factor_spec.get("factor_id") != "F018":
        raise ValueError("factor spec is not F018")
    cache = validate_json(args.return_cache_manifest, kind="research_label_cache")
    if sha256(args.return_prices) != cache.get("output_sha256"):
        raise ValueError("return price cache hash mismatch")

    rows = load_rows(args.window_parquet, args.candidates, args.return_prices)
    diagnostics = add_controlled_factors(rows)
    performance = run_continuous(rows, {"factors": FACTORS, "targets": TARGETS})
    performance_summary = summarize(performance)
    diagnostic_details, diagnostic_summary = aggregate_diagnostics(diagnostics)
    comparisons, comparison_summary = incremental_comparisons(performance)

    args.output_dir.mkdir(parents=True)
    write_csv(args.output_dir / "performance_by_slice.csv", performance)
    write_csv(args.output_dir / "performance_summary.csv", performance_summary)
    write_csv(args.output_dir / "incremental_comparison_by_slice.csv", comparisons)
    write_csv(args.output_dir / "incremental_comparison_summary.csv", comparison_summary)
    write_csv(args.output_dir / "factor_residual_diagnostics_by_slice.csv", diagnostic_details)
    write_csv(args.output_dir / "factor_residual_diagnostics_summary.csv", diagnostic_summary)
    manifest = {
        "kind": "research_result",
        "status": "completed",
        "research_id": "R017",
        "factor_id": "F018",
        "study": "f018_incremental_intraday_controls",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "research_run": str(args.research_run.resolve()),
        "research_run_sha256": sha256(args.research_run),
        "implementation": str(Path(__file__).resolve()),
        "implementation_sha256": sha256(Path(__file__).resolve()),
        "window_completion": str(args.window_completion.resolve()),
        "window_completion_sha256": sha256(args.window_completion),
        "candidate_completion": str(args.candidate_completion.resolve()),
        "candidate_completion_sha256": sha256(args.candidate_completion),
        "factor_spec": str(args.factor_spec.resolve()),
        "factor_spec_sha256": sha256(args.factor_spec),
        "window_parquet": str(args.window_parquet.resolve()),
        "window_parquet_sha256": sha256(args.window_parquet),
        "candidates": str(args.candidates.resolve()),
        "candidates_sha256": sha256(args.candidates),
        "return_cache_manifest": str(args.return_cache_manifest.resolve()),
        "return_cache_manifest_sha256": sha256(args.return_cache_manifest),
        "control_specs": {name: list(columns) for name, columns in CONTROL_SPECS.items()},
        "factor_processing": (
            "within date x frozen domain: 1/99 winsorize factor and controls; "
            "OLS/Gram-Schmidt residualize factor with intercept; returns are not residualized"
        ),
        "primary_scope": "raw baseline first; controlled factor residuals in frozen nine domains",
        "signal_cutoff": "10:30:00",
        "entry_rule": "10:31 minute close",
        "targets": list(TARGETS),
        "future_filter_used": False,
        "style_neutralization_used": False,
        "months": [202601],
        "rows": len(rows),
        "performance_rows": len(performance),
    }
    (args.output_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({
        "research_id": "R017",
        "factor_id": "F018",
        "rows": len(rows),
        "output": str(args.output_dir),
    }))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
