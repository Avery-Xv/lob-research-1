#!/usr/bin/env python3
"""Four-style domain backtest for Batch A ten-minute direct targets."""

from __future__ import annotations

import argparse
import csv
import json
import math
from collections import defaultdict
from pathlib import Path
from statistics import mean, stdev
from typing import Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[2]
STYLE_COLS = ("momentum", "liquidity", "beta", "residual_volatility")
DIAGNOSTIC_STYLE_COLS = ("size", "non_linear_size", *STYLE_COLS)
STYLE_SPEC = "LOB4-ex-size-and-nonlinear-size"
FACTOR_NAMES = (
    "active_flow", "chain_flow", "single_chain_confirmation",
    "multi_chain_exhaustion", "quote_confirmation", "chain_quote_confirmed",
    "quote_withdrawal", "execution_pressure", "book_imbalance3",
)
TARGET_NAMES = (
    "future_net_share", "log_future_active_volume", "log_future_event_count",
    "log_future_realized_vol", "spread_change_bps", "log_depth_change",
    "end_book_imbalance3",
)


def finite(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def ranks(values: Sequence[float]) -> list[float]:
    order = sorted(range(len(values)), key=lambda index: (values[index], index))
    output = [0.0] * len(values)
    start = 0
    while start < len(order):
        end = start + 1
        while end < len(order) and values[order[end]] == values[order[start]]:
            end += 1
        rank = (start + end - 1) / 2.0 + 1.0
        for position in range(start, end):
            output[order[position]] = rank
        start = end
    return output


def percentile_ranks(values: Sequence[float]) -> list[float]:
    ranked = ranks(values)
    return [0.5] * len(values) if len(values) < 2 else [
        (value - 1.0) / (len(values) - 1.0) for value in ranked
    ]


def pearson(xs: Sequence[float], ys: Sequence[float]) -> float | None:
    if len(xs) < 3:
        return None
    mx, my = mean(xs), mean(ys)
    vx = sum((x - mx) ** 2 for x in xs)
    vy = sum((y - my) ** 2 for y in ys)
    if vx <= 0 or vy <= 0:
        return None
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / math.sqrt(vx * vy)


def basis(exposures: Sequence[Sequence[float]]) -> list[list[float]]:
    output: list[list[float]] = []
    for column_index in range(len(exposures[0]) if exposures else 0):
        column = [row[column_index] for row in exposures]
        center = mean(column)
        vector = [value - center for value in column]
        for existing in output:
            projection = sum(value * base for value, base in zip(vector, existing))
            vector = [value - projection * base for value, base in zip(vector, existing)]
        norm = math.sqrt(sum(value * value for value in vector))
        if norm > 1e-10:
            output.append([value / norm for value in vector])
    return output


def residualize(values: Sequence[float], exposures: Sequence[Sequence[float]]) -> list[float]:
    center = mean(values)
    output = [value - center for value in values]
    for column in basis(exposures):
        projection = sum(value * base for value, base in zip(output, column))
        output = [value - projection * base for value, base in zip(output, column)]
    return output


def spread(scores: Sequence[float], targets: Sequence[float], symbols: Sequence[str]) -> float:
    order = sorted(range(len(scores)), key=lambda index: (scores[index], symbols[index]))
    bucket = max(1, len(order) // 10)
    return mean(targets[index] for index in order[-bucket:]) - mean(
        targets[index] for index in order[:bucket]
    )


def mean_t(values: Sequence[float]) -> tuple[float | None, float | None]:
    if not values:
        return None, None
    average = mean(values)
    if len(values) < 2:
        return average, None
    sigma = stdev(values)
    return average, average / (sigma / math.sqrt(len(values))) if sigma else None


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    if not rows:
        raise ValueError(f"no rows for {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    with temporary.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader(); writer.writerows(rows)
    temporary.replace(path)


def load_previous_styles(path: Path) -> dict[tuple[str, int], list[float]]:
    grouped: dict[str, list[tuple[int, list[float]]]] = defaultdict(list)
    with path.open(newline="") as handle:
        for row in csv.DictReader(handle):
            values = [finite(row.get(column)) for column in DIAGNOSTIC_STYLE_COLS]
            if any(value is None for value in values):
                continue
            date = int(row["date"].replace("-", ""))
            grouped[row["symbol"]].append((date, [float(value) for value in values if value is not None]))
    output: dict[tuple[str, int], list[float]] = {}
    for symbol, observations in grouped.items():
        previous: list[float] | None = None
        for date, values in sorted(observations):
            if previous is not None:
                output[(symbol, date)] = previous
            previous = values
    return output


def load_domains(path: Path) -> dict[tuple[str, int], str]:
    output = {}
    with path.open(newline="") as handle:
        for row in csv.DictReader(handle):
            output[(row["symbol"], int(row["date"]))] = row["domain"]
    return output


def factor_values(row: dict[str, str]) -> dict[str, float]:
    chain = float(row["chain_net_share"])
    multi = float(row["multi_chain_share"])
    add_buy, add_sell = float(row["aggressive_add_buy"]), float(row["aggressive_add_sell"])
    quote = (add_buy - add_sell) / (add_buy + add_sell) if add_buy + add_sell > 0 else 0.0
    cancel_buy, cancel_sell = float(row["near_cancel_buy"]), float(row["near_cancel_sell"])
    withdrawal = (cancel_sell - cancel_buy) / (cancel_sell + cancel_buy) if cancel_buy + cancel_sell > 0 else 0.0
    return {
        "active_flow": float(row["active_net_share"]),
        "chain_flow": chain,
        "single_chain_confirmation": chain * (1.0 - multi),
        "multi_chain_exhaustion": -chain * multi,
        "quote_confirmation": quote,
        "chain_quote_confirmed": (chain + quote) / 2.0,
        "quote_withdrawal": withdrawal,
        "execution_pressure": float(row["pred_fill_sell"]) - float(row["pred_fill_buy"]),
        "book_imbalance3": float(row["book_imbalance3"]),
    }


def target_values(row: dict[str, str]) -> dict[str, float | None]:
    buy, sell = float(row["future_buy_volume"]), float(row["future_sell_volume"])
    total = buy + sell
    start_depth = float(row["bid_depth3"]) + float(row["ask_depth3"])
    end_bid, end_ask = float(row["end_bid_depth3"]), float(row["end_ask_depth3"])
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


def load_observations(
    shard_dir: Path, styles: dict[tuple[str, int], list[float]], domains: dict[tuple[str, int], str]
) -> dict[tuple[int, int], list[dict[str, object]]]:
    output: dict[tuple[int, int], list[dict[str, object]]] = defaultdict(list)
    for path in sorted(shard_dir.glob("batch_*/signals.csv")):
        with path.open(newline="") as handle:
            for row in csv.DictReader(handle):
                key = (row["symbol"], int(row["date"]))
                style = styles.get(key); domain = domains.get(key)
                if style is None or domain is None:
                    continue
                output[(key[1], int(row["signal_time"]))].append({
                    "symbol": key[0], "domain": domain, "styles": style,
                    "factors": factor_values(row), "targets": target_values(row),
                })
    return output


def metric_row(
    *, scope: str, domain: str, date: int, signal_time: int, factor: str,
    target: str, rows: list[dict[str, object]], raw_scores: list[float],
    neutral_scores: list[float],
) -> dict[str, object] | None:
    eligible = [index for index, row in enumerate(rows) if row["targets"][target] is not None]  # type: ignore[index]
    if len(eligible) < 10:
        return None
    symbols = [str(rows[index]["symbol"]) for index in eligible]
    labels = [float(rows[index]["targets"][target]) for index in eligible]  # type: ignore[index]
    raw = [raw_scores[index] for index in eligible]
    neutral = [neutral_scores[index] for index in eligible]
    return {
        "scope": scope, "domain": domain, "date": date, "signal_time": signal_time,
        "factor": factor, "target": target, "n": len(eligible),
        "raw_rank_ic": pearson(ranks(raw), ranks(labels)),
        "neutral_rank_ic": pearson(ranks(neutral), ranks(labels)),
        "raw_d10_d1": spread(raw, labels, symbols),
        "neutral_d10_d1": spread(neutral, labels, symbols),
    }


def run_backtest(slices: dict[tuple[int, int], list[dict[str, object]]]) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    performance: list[dict[str, object]] = []
    exposures: list[dict[str, object]] = []
    for (date, signal_time), slice_rows in sorted(slices.items()):
        by_domain: dict[str, list[dict[str, object]]] = defaultdict(list)
        for row in slice_rows:
            by_domain[str(row["domain"])].append(row)
        domain_scores: dict[str, dict[str, tuple[list[dict[str, object]], list[float], list[float]]]] = defaultdict(dict)
        for domain, rows in sorted(by_domain.items()):
            if len(rows) < 15:
                continue
            diagnostic_styles = [row["styles"] for row in rows]  # type: ignore[misc]
            styles = [row[2:] for row in diagnostic_styles]
            for factor in FACTOR_NAMES:
                raw = [float(row["factors"][factor]) for row in rows]  # type: ignore[index]
                neutral = residualize(raw, styles)
                domain_scores[domain][factor] = (rows, raw, neutral)
                raw_ranks = ranks(raw)
                centered = [value - mean(raw) for value in raw]
                residual_variance = sum(value * value for value in neutral)
                total_variance = sum(value * value for value in centered)
                exposure = {
                    "scope": "domain", "domain": domain, "date": date,
                    "signal_time": signal_time, "factor": factor, "n": len(rows),
                    "joint_r2": 1.0 - residual_variance / total_variance if total_variance > 0 else None,
                }
                for index, style in enumerate(DIAGNOSTIC_STYLE_COLS):
                    exposure[f"{style}_rank_exposure"] = pearson(
                        raw_ranks, ranks([row[index] for row in diagnostic_styles])
                    )
                exposures.append(exposure)
                for target in TARGET_NAMES:
                    result = metric_row(scope="domain", domain=domain, date=date, signal_time=signal_time,
                                        factor=factor, target=target, rows=rows, raw_scores=raw,
                                        neutral_scores=neutral)
                    if result is not None: performance.append(result)
        # Domain-neutral aggregate: residualize and rank inside each domain, then concatenate.
        for factor in FACTOR_NAMES:
            pooled_rows: list[dict[str, object]] = []
            pooled_scores: list[float] = []
            for domain in sorted(domain_scores):
                if factor not in domain_scores[domain]: continue
                rows, _raw, neutral = domain_scores[domain][factor]
                pooled_rows.extend(rows); pooled_scores.extend(percentile_ranks(neutral))
            if len(pooled_rows) >= 20:
                for target in TARGET_NAMES:
                    result = metric_row(scope="domain_neutral_aggregate", domain="domain_neutral",
                                        date=date, signal_time=signal_time, factor=factor, target=target,
                                        rows=pooled_rows, raw_scores=pooled_scores, neutral_scores=pooled_scores)
                    if result is not None:
                        result["raw_rank_ic"] = None; result["raw_d10_d1"] = None
                        performance.append(result)
        # Unpartitioned all-market four-style result is secondary and retains size exposure.
        if len(slice_rows) >= 20:
            diagnostic_styles = [row["styles"] for row in slice_rows]  # type: ignore[misc]
            styles = [row[2:] for row in diagnostic_styles]
            for factor in FACTOR_NAMES:
                raw = [float(row["factors"][factor]) for row in slice_rows]  # type: ignore[index]
                neutral = residualize(raw, styles)
                raw_ranks = ranks(raw)
                centered = [value - mean(raw) for value in raw]
                total_variance = sum(value * value for value in centered)
                residual_variance = sum(value * value for value in neutral)
                exposure = {"scope": "all_market", "domain": "all/all", "date": date,
                            "signal_time": signal_time, "factor": factor, "n": len(slice_rows),
                            "joint_r2": 1.0 - residual_variance / total_variance if total_variance > 0 else None}
                for index, style in enumerate(DIAGNOSTIC_STYLE_COLS):
                    exposure[f"{style}_rank_exposure"] = pearson(
                        raw_ranks, ranks([row[index] for row in diagnostic_styles])
                    )
                exposures.append(exposure)
                for target in TARGET_NAMES:
                    result = metric_row(scope="all_market", domain="all/all", date=date,
                                        signal_time=signal_time, factor=factor, target=target,
                                        rows=slice_rows, raw_scores=raw, neutral_scores=neutral)
                    if result is not None: performance.append(result)
    return performance, exposures


def summarize_performance(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    daily: dict[tuple[str, str, str, str, int], list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        daily[(str(row["scope"]), str(row["domain"]), str(row["factor"]), str(row["target"]), int(row["date"]))].append(row)
    grouped: dict[tuple[str, str, str, str], list[dict[str, object]]] = defaultdict(list)
    for key, observations in daily.items():
        record: dict[str, object] = {"date": key[4], "n": sum(int(row["n"]) for row in observations),
                                     "avg_names": mean(int(row["n"]) for row in observations)}
        for metric in ("raw_rank_ic", "neutral_rank_ic", "raw_d10_d1", "neutral_d10_d1"):
            values = [float(row[metric]) for row in observations if row[metric] is not None]
            record[metric] = mean(values) if values else None
        grouped[key[:4]].append(record)
    output = []
    for key, observations in sorted(grouped.items()):
        result = {"scope": key[0], "domain": key[1], "factor": key[2], "target": key[3],
                  "n_dates": len(observations), "n_obs": sum(int(row["n"]) for row in observations),
                  "avg_names": mean(float(row["avg_names"]) for row in observations)}
        for metric in ("raw_rank_ic", "neutral_rank_ic", "raw_d10_d1", "neutral_d10_d1"):
            values = [float(row[metric]) for row in observations if row[metric] is not None]
            result[metric], result[f"{metric}_t"] = mean_t(values)
            result[f"{metric}_positive_date_share"] = sum(value > 0 for value in values) / len(values) if values else None
        output.append(result)
    return output


def summarize_exposures(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    daily: dict[tuple[str, str, str, int], list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        daily[(str(row["scope"]), str(row["domain"]), str(row["factor"]), int(row["date"]))].append(row)
    grouped: dict[tuple[str, str, str], list[dict[str, object]]] = defaultdict(list)
    metrics = ("joint_r2", *(f"{style}_rank_exposure" for style in DIAGNOSTIC_STYLE_COLS))
    for key, observations in daily.items():
        record: dict[str, object] = {
            "date": key[3], "n_slices": len(observations),
            "n_obs": sum(int(row["n"]) for row in observations),
        }
        for metric in metrics:
            values = [float(row[metric]) for row in observations if row[metric] is not None]
            record[metric] = mean(values) if values else None
            record[f"{metric}_mean_abs"] = mean(map(abs, values)) if values else None
        grouped[key[:3]].append(record)
    output = []
    for key, observations in sorted(grouped.items()):
        result = {"scope": key[0], "domain": key[1], "factor": key[2],
                  "n_dates": len(observations),
                  "n_slices": sum(int(row["n_slices"]) for row in observations),
                  "n_obs": sum(int(row["n_obs"]) for row in observations)}
        for metric in metrics:
            values = [float(row[metric]) for row in observations if row[metric] is not None]
            result[metric], result[f"{metric}_t"] = mean_t(values)
            absolute_values = [float(row[f"{metric}_mean_abs"]) for row in observations
                               if row[f"{metric}_mean_abs"] is not None]
            result[f"{metric}_mean_abs"] = mean(absolute_values) if absolute_values else None
        output.append(result)
    return output


def validate_source_manifest(shard_dir: Path) -> dict[str, object]:
    path = shard_dir / "manifest.json"
    source = json.loads(path.read_text())
    config = source.get("config", {})
    if config.get("output_etf_symbols") != 0:
        raise ValueError(f"Batch A source did not certify zero ETF symbols: {path}")
    if config.get("factor_version") != "order_shape_batch_a_v1_20260805":
        raise ValueError(f"unexpected Batch A factor version: {config.get('factor_version')}")
    return source


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--shard-dir", type=Path, required=True)
    parser.add_argument("--domain-file", type=Path, required=True)
    parser.add_argument("--styles", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    source_manifest = validate_source_manifest(args.shard_dir)
    styles = load_previous_styles(args.styles)
    domains = load_domains(args.domain_file)
    observations = load_observations(args.shard_dir, styles, domains)
    performance, exposures = run_backtest(observations)
    performance_summary = summarize_performance(performance)
    exposure_summary = summarize_exposures(exposures)
    write_csv(args.output_dir / "performance_by_slice.csv", performance)
    write_csv(args.output_dir / "performance_summary.csv", performance_summary)
    write_csv(args.output_dir / "exposure_by_slice.csv", exposures)
    write_csv(args.output_dir / "exposure_summary.csv", exposure_summary)
    manifest = {
        "style_specification": STYLE_SPEC, "style_columns": list(STYLE_COLS),
        "explicitly_excluded_styles": ["size", "non_linear_size"],
        "diagnostic_style_columns": list(DIAGNOSTIC_STYLE_COLS),
        "exposure_timing": "previous trading day", "winsorization": "none; candidate factors are bounded shares/probability differences",
        "signal_rule": "fixed grid; factor uses (t-60s,t)", "label_rule": "direct targets use [t,t+10m)",
        "domain_rule": "frozen prior-day cap x price/board nine domains",
        "all_market_note": "four-style neutralized but intentionally retains size and nonlinear-size exposure",
        "factors": list(FACTOR_NAMES), "targets": list(TARGET_NAMES),
        "source_paths": {"shard_dir": str(args.shard_dir.resolve()),
                         "domain_file": str(args.domain_file.resolve()),
                         "styles": str(args.styles.resolve())},
        "source_manifest_fingerprint": source_manifest.get("fingerprint"),
        "source_universe_rule": source_manifest["config"].get("universe_rule"),
        "source_output_etf_symbols": source_manifest["config"].get("output_etf_symbols"),
        "slices": len(observations), "performance_rows": len(performance), "exposure_rows": len(exposures),
    }
    args.output_dir.mkdir(parents=True, exist_ok=True)
    (args.output_dir / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n")
    print(f"slices={len(observations)} performance={len(performance)} exposures={len(exposures)} output={args.output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
