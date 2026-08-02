#!/usr/bin/env python3
"""Compute daily active-buy/active-sell large passive-order gap ratios."""

from __future__ import annotations

import argparse
import csv
import glob
import math
import os
import time
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Sequence

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[3]
FIELDS = [
    "symbol",
    "date",
    "theta_5d_raw",
    "total_trade_volume",
    "matched_trade_volume",
    "match_rate",
    "large_gap_buy_volume",
    "large_gap_sell_volume",
    "large_gap_buy_ratio",
    "large_gap_sell_ratio",
    "valid_trade_count",
    "matched_trade_count",
    "large_gap_buy_trade_count",
    "large_gap_sell_trade_count",
    "passes_match_rate",
]


def expand_inputs(patterns: Sequence[str]) -> list[str]:
    paths: list[str] = []
    for pattern in patterns:
        paths.extend(glob.glob(pattern) or [pattern])
    return sorted(dict.fromkeys(paths), key=lambda path: (os.path.getsize(path), path))


def task_key(path: str) -> tuple[str, str]:
    file_path = Path(path)
    return file_path.stem, file_path.parent.name


def load_theta(path: str) -> dict[tuple[str, int], float]:
    result = {}
    with open(path, newline="") as handle:
        for row in csv.DictReader(handle):
            value = row.get("theta_5d_raw", "")
            if not value:
                continue
            theta = float(value)
            if math.isfinite(theta) and theta > 0:
                result[(row["symbol"], int(row["date"]))] = theta
    return result


def read_completed_tasks(path: str) -> set[tuple[str, str]]:
    if not Path(path).exists():
        return set()
    completed = set()
    with open(path, newline="") as handle:
        for row in csv.DictReader(handle):
            completed.add((row["symbol"], row["date"][:6]))
    return completed


def compute_one(
    path: str,
    theta_rows: list[tuple[int, float]],
    minimum_match_rate: float,
    memory_limit: str,
):
    if not theta_rows:
        return path, []
    con = duckdb.connect()
    con.execute("PRAGMA threads=1")
    con.execute(f"PRAGMA memory_limit='{memory_limit}'")
    con.execute("PRAGMA preserve_insertion_order=false")
    con.execute("CREATE TEMP TABLE theta(date INTEGER, theta_5d_raw DOUBLE)")
    con.executemany("INSERT INTO theta VALUES (?, ?)", theta_rows)
    query = """
WITH events AS (
    SELECT
        regexp_replace(regexp_extract(filename, '[^/]+$'), '\\.parquet$', '') AS symbol,
        date,
        row_id,
        CASE
            WHEN time >= 93000000 AND time < 113000000 THEN 'AM'
            WHEN time >= 130000000 AND time < 145700000 THEN 'PM'
            ELSE NULL
        END AS session,
        source_action,
        source_recid,
        source_trade_id,
        source_side,
        source_price,
        source_volume,
        source_buy_order_recid,
        source_sell_order_recid,
        bid_px[1]::DOUBLE AS bid1,
        ask_px[1]::DOUBLE AS ask1
    FROM read_parquet(?, filename=true)
),
continuous AS (
    SELECT
        *,
        lag(bid1) OVER (
            PARTITION BY symbol, date, session ORDER BY row_id
        ) AS pre_bid1,
        lag(ask1) OVER (
            PARTITION BY symbol, date, session ORDER BY row_id
        ) AS pre_ask1
    FROM events
    WHERE session IS NOT NULL
),
orders AS (
    SELECT
        symbol,
        date,
        source_recid AS order_recid,
        row_id AS entry_row_id,
        source_side AS order_side,
        CASE
            WHEN source_side = 'B' AND pre_bid1 > 0
                THEN pre_bid1 - source_price
            WHEN source_side = 'S' AND pre_ask1 > 0
                THEN source_price - pre_ask1
            ELSE NULL
        END AS initial_gap,
        row_number() OVER (
            PARTITION BY symbol, date, source_recid ORDER BY row_id
        ) AS occurrence
    FROM continuous
    WHERE source_action = 'ORDER_ADD'
      AND source_recid IS NOT NULL
      AND source_price > 0
),
deduplicated_trades AS (
    SELECT * EXCLUDE (trade_occurrence)
    FROM (
        SELECT
            *,
            row_number() OVER (
                PARTITION BY symbol, date,
                    coalesce(source_trade_id, -row_id)
                ORDER BY row_id
            ) AS trade_occurrence
        FROM continuous
        WHERE source_action = 'TRADE'
          AND source_side IN ('B', 'S')
          AND source_volume > 0
    )
    WHERE trade_occurrence = 1
),
matched AS (
    SELECT
        t.symbol,
        t.date,
        t.row_id,
        t.source_side AS active_side,
        t.source_volume,
        o.initial_gap,
        th.theta_5d_raw,
        o.order_recid IS NOT NULL AS is_matched
    FROM deduplicated_trades t
    INNER JOIN theta th ON th.date = t.date
    LEFT JOIN orders o
      ON o.symbol = t.symbol
     AND o.date = t.date
     AND o.order_recid = CASE
            WHEN t.source_side = 'B' THEN t.source_sell_order_recid
            WHEN t.source_side = 'S' THEN t.source_buy_order_recid
         END
     AND o.order_side = CASE
            WHEN t.source_side = 'B' THEN 'S'
            WHEN t.source_side = 'S' THEN 'B'
         END
     AND o.entry_row_id < t.row_id
     AND o.occurrence = 1
)
SELECT
    symbol,
    date,
    any_value(theta_5d_raw) AS theta_5d_raw,
    sum(source_volume)::BIGINT AS total_trade_volume,
    coalesce(sum(source_volume) FILTER (WHERE is_matched), 0)::BIGINT
        AS matched_trade_volume,
    coalesce(sum(source_volume) FILTER (WHERE is_matched), 0)::DOUBLE
        / sum(source_volume) AS match_rate,
    coalesce(sum(source_volume) FILTER (
        WHERE is_matched AND active_side = 'B' AND initial_gap > theta_5d_raw
    ), 0)::BIGINT AS large_gap_buy_volume,
    coalesce(sum(source_volume) FILTER (
        WHERE is_matched AND active_side = 'S' AND initial_gap > theta_5d_raw
    ), 0)::BIGINT AS large_gap_sell_volume,
    coalesce(sum(source_volume) FILTER (
        WHERE is_matched AND active_side = 'B' AND initial_gap > theta_5d_raw
    ), 0)::DOUBLE / sum(source_volume) AS large_gap_buy_ratio,
    coalesce(sum(source_volume) FILTER (
        WHERE is_matched AND active_side = 'S' AND initial_gap > theta_5d_raw
    ), 0)::DOUBLE / sum(source_volume) AS large_gap_sell_ratio,
    count(*) AS valid_trade_count,
    count(*) FILTER (WHERE is_matched) AS matched_trade_count,
    count(*) FILTER (
        WHERE is_matched AND active_side = 'B' AND initial_gap > theta_5d_raw
    ) AS large_gap_buy_trade_count,
    count(*) FILTER (
        WHERE is_matched AND active_side = 'S' AND initial_gap > theta_5d_raw
    ) AS large_gap_sell_trade_count,
    match_rate >= ? AS passes_match_rate
FROM matched
GROUP BY symbol, date
ORDER BY date
"""
    try:
        return path, con.execute(query, [path, minimum_match_rate]).fetchall()
    finally:
        con.close()


def append_rows(path: str, rows, header: bool) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w" if header else "a", newline="") as handle:
        writer = csv.writer(handle)
        if header:
            writer.writerow(FIELDS)
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("inputs", nargs="+", help="Monthly parquet globs.")
    parser.add_argument("--theta", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--memory-limit-per-worker", default="4GB")
    parser.add_argument("--minimum-match-rate", type=float, default=0.95)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--limit-files", type=int)
    args = parser.parse_args()

    theta = load_theta(args.theta)
    theta_by_task: dict[tuple[str, str], list[tuple[int, float]]] = defaultdict(list)
    for (symbol, date), value in theta.items():
        theta_by_task[(symbol, str(date)[:6])].append((date, value))
    for rows in theta_by_task.values():
        rows.sort()
    paths = expand_inputs(args.inputs)
    if args.limit_files is not None:
        paths = paths[: args.limit_files]
    completed = read_completed_tasks(args.output) if args.resume else set()
    paths = [path for path in paths if task_key(path) not in completed]
    write_header = not (args.resume and Path(args.output).exists())
    started = time.perf_counter()

    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        futures = []
        for path in paths:
            symbol, month = task_key(path)
            rows = theta_by_task.get((symbol, month), [])
            futures.append(
                executor.submit(
                    compute_one,
                    path,
                    rows,
                    args.minimum_match_rate,
                    args.memory_limit_per_worker,
                )
            )

        factor_rows = 0
        for index, future in enumerate(as_completed(futures), start=1):
            path, rows = future.result()
            append_rows(args.output, rows, write_header)
            write_header = False
            factor_rows += len(rows)
            if index % 25 == 0 or index == len(paths):
                print(
                    f"done={index}/{len(paths)} rows={factor_rows} "
                    f"last={Path(path).name} elapsed_sec={time.perf_counter()-started:.1f}",
                    flush=True,
                )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
