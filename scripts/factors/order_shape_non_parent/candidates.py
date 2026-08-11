#!/usr/bin/env python3
"""Build leakage-safe F014 candidates from the audited fixed-10:30 P002 cache."""

from __future__ import annotations

import argparse
import bisect
import csv
import hashlib
import json
import math
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Iterable, Sequence


REPO_ROOT = Path(__file__).resolve().parents[3]
CAP_GROUPS = ("cap_lt_50yi", "cap_50_500yi", "cap_ge_500yi")
PRICE_GROUPS = ("nonstar_lt_10", "nonstar_ge_10", "star_ge_10")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def classify_domain(market_cap: float, price: float, symbol: str) -> str | None:
    if market_cap < 500_000:
        cap = CAP_GROUPS[0]
    elif market_cap < 5_000_000:
        cap = CAP_GROUPS[1]
    else:
        cap = CAP_GROUPS[2]
    if symbol.startswith(("SH688", "SH689")):
        if price < 10:
            return None
        price_group = PRICE_GROUPS[2]
    else:
        price_group = PRICE_GROUPS[0] if price < 10 else PRICE_GROUPS[1]
    return f"{cap}/{price_group}"


def load_numeric_history(path: Path, field: str) -> dict[str, tuple[list[int], list[float]]]:
    grouped: dict[str, list[tuple[int, float]]] = defaultdict(list)
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            raw = row.get(field)
            if raw in (None, ""):
                continue
            grouped[row["symbol"]].append((int(row["date"].replace("-", "")), float(raw)))
    output = {}
    for symbol, observations in grouped.items():
        ordered = sorted(set(observations))
        output[symbol] = ([item[0] for item in ordered], [item[1] for item in ordered])
    return output


def previous_value(history: tuple[list[int], list[float]] | None, date: int) -> float | None:
    if history is None:
        return None
    dates, values = history
    index = bisect.bisect_left(dates, date) - 1
    return values[index] if index >= 0 else None


def orthogonal_basis(exposures: Sequence[Sequence[float]]) -> list[list[float]]:
    basis: list[list[float]] = []
    width = len(exposures[0]) if exposures else 0
    for column_index in range(width):
        column = [row[column_index] for row in exposures]
        center = mean(column)
        vector = [value - center for value in column]
        for existing in basis:
            projection = sum(value * base for value, base in zip(vector, existing))
            vector = [value - projection * base for value, base in zip(vector, existing)]
        norm = math.sqrt(sum(value * value for value in vector))
        if norm > 1e-10:
            basis.append([value / norm for value in vector])
    return basis


def residualize(values: Sequence[float], exposures: Sequence[Sequence[float]]) -> list[float]:
    centered = [value - mean(values) for value in values]
    for column in orthogonal_basis(exposures):
        projection = sum(value * base for value, base in zip(centered, column))
        centered = [value - projection * base for value, base in zip(centered, column)]
    return centered


def quantile(values: Iterable[float], probability: float) -> float:
    ordered = sorted(values)
    if not ordered:
        raise ValueError("quantile requires observations")
    position = probability * (len(ordered) - 1)
    lower = int(math.floor(position)); upper = int(math.ceil(position))
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def logit(probability: float) -> float:
    clipped = min(max(probability, 1e-6), 1.0 - 1e-6)
    return math.log(clipped / (1.0 - clipped))


def base_values(row: dict[str, str]) -> dict[str, float]:
    active_buy = float(row["active_buy_volume"]); active_sell = float(row["active_sell_volume"])
    active_count = float(row["active_buy_count"]) + float(row["active_sell_count"])
    active_volume = active_buy + active_sell
    m1 = (active_buy - active_sell) / active_volume if active_volume > 0 else 0.0
    pred_buy = float(row["pred_fill_buy"]); pred_sell = float(row["pred_fill_sell"])
    execution_pressure = pred_sell - pred_buy
    cancel_buy = float(row["near_cancel_buy"]); cancel_sell = float(row["near_cancel_sell"])
    cancel_total = cancel_buy + cancel_sell
    bid_depth = float(row["bid_depth3"]); ask_depth = float(row["ask_depth3"])
    depth = bid_depth + ask_depth
    return {
        "m1": m1,
        "execution_pressure": execution_pressure,
        "np02_fillability": (pred_buy + pred_sell) / 2.0,
        "np02_logit_fillability": (logit(pred_buy) + logit(pred_sell)) / 2.0,
        "np03_confirmation": m1 * execution_pressure,
        "book_imbalance3": float(row["book_imbalance3"]),
        "np05_cancel_intensity": cancel_total / (depth + 1e-12),
        "np05_abs_cancel_imbalance": abs(cancel_sell - cancel_buy) / (cancel_total + 1e-12),
        "np05_signed_cancel_imbalance": (cancel_sell - cancel_buy) / (cancel_total + 1e-12),
        "np05_buy_cancel_shock": cancel_buy / (bid_depth + 1e-12),
        "np05_sell_cancel_shock": cancel_sell / (ask_depth + 1e-12),
        "log_active_volume": math.log1p(active_volume),
        "log_active_count": math.log1p(active_count),
        "log_fill_history_buy": math.log1p(float(row["fill_history_buy"])),
        "log_fill_history_sell": math.log1p(float(row["fill_history_sell"])),
        "spread_bps": float(row["spread_bps"]),
        "log_depth3": math.log1p(depth),
    }


def assign_states(rows: list[dict[str, object]]) -> None:
    m1_abs = [abs(float(row["m1"])) for row in rows]
    ep_abs = [abs(float(row["execution_pressure"])) for row in rows]
    book_abs = [abs(float(row["book_imbalance3"])) for row in rows]
    m1_low, m1_high = quantile(m1_abs, 1 / 3), quantile(m1_abs, 2 / 3)
    ep_low, ep_high = quantile(ep_abs, 1 / 3), quantile(ep_abs, 2 / 3)
    book_high = quantile(book_abs, 2 / 3)
    for row in rows:
        m1 = float(row["m1"]); ep = float(row["execution_pressure"])
        book = float(row["book_imbalance3"])
        if abs(m1) >= m1_high and abs(ep) >= ep_high and m1 * ep > 0:
            np03 = "both_strong_same"
        elif abs(m1) <= m1_low and abs(ep) >= ep_high:
            np03 = "m1_weak_ep_strong"
        elif abs(m1) >= m1_high and m1 * ep < 0:
            np03 = "m1_strong_ep_opposite"
        elif abs(m1) <= m1_low and abs(ep) <= ep_low:
            np03 = "both_weak"
        else:
            np03 = "other"
        row["np03_state"] = np03
        if abs(m1) < m1_high or abs(book) < book_high:
            row["np04_state"] = "other"
        else:
            flow = "buy" if m1 > 0 else "sell"
            book_side = "buy" if book > 0 else "sell"
            row["np04_state"] = f"active_{flow}_book_{book_side}"


def enrich_slice(rows: list[dict[str, object]]) -> None:
    m1 = [float(row["m1"]) for row in rows]
    ep = [float(row["execution_pressure"]) for row in rows]
    linear = residualize(ep, [[value] for value in m1])
    cubic = residualize(ep, [[value, value ** 2, value ** 3] for value in m1])
    fillability = [float(row["np02_fillability"]) for row in rows]
    activity_controls = [[
        float(row["log_active_volume"]), float(row["log_active_count"]),
        float(row["log_fill_history_buy"]), float(row["log_fill_history_sell"]),
        float(row["spread_bps"]), float(row["log_depth3"]),
    ] for row in rows]
    fillability_residual = residualize(fillability, activity_controls)
    book = [float(row["book_imbalance3"]) for row in rows]
    book_flow_residual = residualize(m1, [[value, value ** 2, value ** 3] for value in book])
    for index, row in enumerate(rows):
        row["np01_execution_pressure"] = ep[index]
        row["np01_m1_linear"] = linear[index]
        row["np01_m1_cubic"] = cubic[index]
        row["np02_activity_residual"] = fillability_residual[index]
        row["np04_flow_minus_book"] = book_flow_residual[index]
    assign_states(rows)


def source_paths(root: Path) -> list[Path]:
    return sorted(root.glob("part_*/batch_*/signals.csv"))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--source-completion", type=Path, required=True)
    parser.add_argument("--market-caps", type=Path, required=True)
    parser.add_argument("--controls", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    output_dir = args.output_dir.resolve()
    if output_dir.exists():
        raise SystemExit(f"Refusing to overwrite output: {output_dir}")
    completion = json.loads(args.source_completion.read_text(encoding="utf-8"))
    if completion.get("status") != "completed_audited" or completion.get("factor_id") != "F013":
        raise SystemExit("Source completion is not completed_audited F013")
    paths = source_paths(args.source_root)
    if len(paths) != 5160:
        raise SystemExit(f"Expected 5160 source signal files, found {len(paths)}")
    caps = load_numeric_history(args.market_caps, "total_mv")
    closes = load_numeric_history(args.controls, "close")
    grouped: dict[tuple[int, int, str], list[dict[str, object]]] = defaultdict(list)
    seen: set[tuple[str, int, int]] = set()
    missing_domain = 0
    missing_domain_keys: list[tuple[str, int, int]] = []
    for path in paths:
        with path.open(newline="", encoding="utf-8") as handle:
            for source in csv.DictReader(handle):
                symbol = source["symbol"]; date = int(source["date"]); signal_time = int(source["signal_time"])
                key = (symbol, date, signal_time)
                if key in seen:
                    raise ValueError(f"duplicate signal row: {key}")
                seen.add(key)
                if signal_time != 1030:
                    raise ValueError(f"unexpected signal time: {key}")
                cap = previous_value(caps.get(symbol), date)
                close = previous_value(closes.get(symbol), date)
                if cap is None or close is None:
                    missing_domain += 1
                    missing_domain_keys.append(key)
                    continue
                domain = classify_domain(cap, close, symbol)
                if domain is None:
                    continue
                row: dict[str, object] = {
                    "symbol": symbol, "date": date, "signal_time": signal_time,
                    "exchange": symbol[:2], "domain": domain,
                }
                row.update(base_values(source))
                grouped[(date, signal_time, domain)].append(row)
    for rows in grouped.values():
        if len(rows) >= 15:
            enrich_slice(rows)
        else:
            for row in rows:
                row["np03_state"] = "insufficient_slice"
                row["np04_state"] = "insufficient_slice"
    output_rows = [row for key in sorted(grouped) for row in sorted(grouped[key], key=lambda item: str(item["symbol"])) if "np01_m1_cubic" in row]
    if not output_rows:
        raise ValueError("no eligible candidate rows")
    output_dir.mkdir(parents=True)
    output_csv = output_dir / "candidates.csv"
    with output_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(output_rows[0]))
        writer.writeheader(); writer.writerows(output_rows)
    domain_counts: dict[str, int] = defaultdict(int)
    for row in output_rows:
        domain_counts[str(row["domain"])] += 1
    manifest = {
        "kind": "F014_candidate_output", "status": "completed",
        "definition_version": "v1_fixed_1030_raw_pilot",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "source_completion": str(args.source_completion.resolve()),
        "source_completion_sha256": sha256(args.source_completion),
        "market_caps": str(args.market_caps.resolve()), "market_caps_sha256": sha256(args.market_caps),
        "controls": str(args.controls.resolve()), "controls_sha256": sha256(args.controls),
        "domain_rule": "previous-trading-day total_mv and close; fixed nine domains; STAR<10 excluded",
        "signal_rule": "fixed 10:30; no future target used in candidate construction",
        "np01_rule": "execution_pressure residualized cross-sectionally within date/domain on M1, M1^2, M1^3",
        "state_threshold_rule": "within-date-domain absolute 1/3 and 2/3 quantiles, frozen before target evaluation",
        "source_signal_rows": len(seen), "candidate_rows": len(output_rows),
        "missing_prior_domain_rows": missing_domain, "missing_prior_domain_sample": missing_domain_keys[:20],
        "symbols": len({str(row["symbol"]) for row in output_rows}),
        "dates": sorted({int(row["date"]) for row in output_rows}),
        "domain_counts": dict(sorted(domain_counts.items())),
        "output": str(output_csv), "output_sha256": sha256(output_csv),
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"rows": len(output_rows), "symbols": manifest["symbols"], "output": str(output_dir)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
