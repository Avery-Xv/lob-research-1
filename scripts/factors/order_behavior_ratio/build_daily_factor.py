#!/usr/bin/env python3
"""Combine continuous-auction session aggregates into daily log-ratio factors."""

from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path

if __package__:
    from .intraday_window_factor import calculate_log_factors
else:
    from intraday_window_factor import calculate_log_factors


PROJECT_ROOT = Path(__file__).resolve().parents[3]
ADDITIVE_FIELDS = (
    "trade_qty",
    "trade_count",
    "aggr_order_count",
    "passive_submit_qty",
    "passive_order_count",
    "aggressive_order_add_qty_excluded",
    "aggressive_order_add_count_excluded",
    "unidentified_aggr_trade_qty",
    "unidentified_aggr_trade_count",
    "duplicate_trade_rows_excluded",
    "invalid_order_add_count",
)
FIELDS = [
    "symbol",
    "date",
    "session_count",
    *ADDITIVE_FIELDS[:5],
    "vr_log",
    "cr_log",
    "single_size_ratio_log",
    *ADDITIVE_FIELDS[5:],
    "is_valid",
    "invalid_reason",
    "factor_version",
]


def combine_sessions(
    session_paths: list[str],
    output: str,
    expected_sessions: int,
) -> int:
    aggregates: dict[tuple[str, str], dict[str, object]] = defaultdict(
        lambda: {
            "session_count": 0,
            "session_valid": True,
            "reasons": [],
            **{field: 0 for field in ADDITIVE_FIELDS},
        }
    )
    seen_session_keys: set[tuple[int, str, str]] = set()
    for session_index, path in enumerate(session_paths):
        with open(path, newline="") as handle:
            reader = csv.DictReader(handle)
            required = ADDITIVE_FIELDS + ("symbol", "date", "is_valid", "invalid_reason")
            missing = set(required) - set(reader.fieldnames or [])
            if missing:
                raise ValueError(f"session input missing columns {sorted(missing)}: {path}")
            for row in reader:
                key = (row["symbol"], row["date"])
                session_key = (session_index, *key)
                if session_key in seen_session_keys:
                    raise ValueError(f"duplicate session row: {session_key}")
                seen_session_keys.add(session_key)
                aggregate = aggregates[key]
                aggregate["session_count"] = int(aggregate["session_count"]) + 1
                aggregate["session_valid"] = bool(aggregate["session_valid"]) and (
                    row["is_valid"].lower() == "true"
                )
                if row["invalid_reason"]:
                    reasons = aggregate["reasons"]
                    assert isinstance(reasons, list)
                    reasons.append(f"session_{session_index + 1}:{row['invalid_reason']}")
                for field in ADDITIVE_FIELDS:
                    aggregate[field] = int(aggregate[field]) + int(row[field])

    Path(output).parent.mkdir(parents=True, exist_ok=True)
    with open(output, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        writer.writeheader()
        for (symbol, date), aggregate in sorted(aggregates.items()):
            vr_log, cr_log, single_size_ratio_log = calculate_log_factors(
                int(aggregate["trade_qty"]),
                int(aggregate["aggr_order_count"]),
                int(aggregate["passive_submit_qty"]),
                int(aggregate["passive_order_count"]),
            )
            complete = int(aggregate["session_count"]) == expected_sessions
            valid = complete and bool(aggregate["session_valid"])
            reasons = list(aggregate["reasons"])
            if not complete:
                reasons.append(
                    f"incomplete_sessions:{aggregate['session_count']}/{expected_sessions}"
                )
            writer.writerow(
                {
                    "symbol": symbol,
                    "date": date,
                    "session_count": aggregate["session_count"],
                    **{field: aggregate[field] for field in ADDITIVE_FIELDS[:5]},
                    "vr_log": vr_log,
                    "cr_log": cr_log,
                    "single_size_ratio_log": single_size_ratio_log,
                    **{field: aggregate[field] for field in ADDITIVE_FIELDS[5:]},
                    "is_valid": valid,
                    "invalid_reason": ";".join(reasons),
                    "factor_version": "daily_continuous_arrival_v1",
                }
            )
    return len(aggregates)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("session_files", nargs="+")
    parser.add_argument(
        "--output",
        default=str(PROJECT_ROOT / "data/processed/order_behavior_ratio_daily_202601.csv"),
    )
    parser.add_argument("--expected-sessions", type=int, default=2)
    args = parser.parse_args()
    if args.expected_sessions < 1:
        raise ValueError("expected-sessions must be at least 1")
    rows = combine_sessions(args.session_files, args.output, args.expected_sessions)
    print(f"done rows={rows} output={args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
