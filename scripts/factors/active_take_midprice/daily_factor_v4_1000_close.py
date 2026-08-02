#!/usr/bin/env python3
"""Compute a full-day v4 active-take mid-gap normalized by the 10:00 close."""

from __future__ import annotations

import argparse
import csv
import glob
import os
import random
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Sequence

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parents[3]
FACTOR_VERSION = "active_take_mid_gap_full_day_over_1000_close_v4"
FIELDS = [
    "symbol",
    "date",
    "normalizer_time",
    "close_1000",
    "active_take_mid_gap",
    "active_take_mid_gap_signed",
    "all_mid_gap",
    "active_take_mid_gap_ratio",
    "active_take_mid_gap_over_1000_close",
    "active_take_mid_gap_signed_over_1000_close",
    "active_take_mid_events",
    "all_mid_move_events",
    "valid_lag_events",
    "am_valid_lag_events",
    "pm_valid_lag_events",
    "factor_version",
]


def chunks(items: Sequence[str], size: int):
    for index in range(0, len(items), size):
        yield items[index : index + size]


def load_stock_symbols(close_file: str) -> set[str]:
    """Load the explicit stock universe carried by the ClickHouse close export."""
    symbols: set[str] = set()
    seen_keys: set[tuple[str, str]] = set()
    with open(close_file, newline="") as handle:
        reader = csv.DictReader(handle)
        required = {"symbol", "date", "close_1000", "security_category"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"close file missing columns: {sorted(missing)}")
        for row in reader:
            if row["security_category"] != "1":
                raise ValueError(
                    f"non-stock security in close file: {row['symbol']} "
                    f"category={row['security_category']}"
                )
            close = float(row["close_1000"])
            if close <= 0:
                raise ValueError(
                    f"non-positive 10:00 close: {row['symbol']} {row['date']}"
                )
            key = row["symbol"], row["date"]
            if key in seen_keys:
                raise ValueError(f"duplicate close row: {key}")
            seen_keys.add(key)
            symbols.add(row["symbol"])
    return symbols


def symbol_from_path(path: str) -> str:
    return Path(path).stem


def expand_stock_inputs(patterns: Sequence[str], stock_symbols: set[str]) -> list[str]:
    paths: list[str] = []
    for pattern in patterns:
        paths.extend(glob.glob(pattern) or [pattern])
    unique_paths = sorted(dict.fromkeys(paths))
    return [path for path in unique_paths if symbol_from_path(path) in stock_symbols]


def compute_batch(
    con: duckdb.DuckDBPyConnection,
    paths: Sequence[str],
    close_file: str,
) -> list[tuple]:
    query = """
WITH closes AS (
    SELECT
        symbol,
        date::BIGINT AS date,
        close_1000::DOUBLE AS close_1000
    FROM read_csv_auto(?, header=true)
    WHERE security_category::INTEGER = 1
      AND close_1000::DOUBLE > 0
),
base AS (
    SELECT
        regexp_replace(regexp_extract(filename, '[^/]+$'), '\\.parquet$', '') AS symbol,
        e.date,
        e.row_id,
        CASE
            WHEN e.time >= 93000000 AND e.time < 113000000 THEN 'AM'
            WHEN e.time >= 130000000 AND e.time < 145700000 THEN 'PM'
        END AS session,
        e.source_action,
        e.source_side,
        e.bid_px[1]::BIGINT AS bid1,
        e.ask_px[1]::BIGINT AS ask1,
        ((e.bid_px[1]::DOUBLE + e.ask_px[1]::DOUBLE) / 2.0) AS mid,
        c.close_1000
    FROM read_parquet(?, filename=true) e
    INNER JOIN closes c
      ON c.symbol = regexp_replace(
          regexp_extract(filename, '[^/]+$'), '\\.parquet$', ''
      )
     AND c.date = e.date
    WHERE (
            (e.time >= 93000000 AND e.time < 113000000)
         OR (e.time >= 130000000 AND e.time < 145700000)
          )
      AND array_length(e.bid_px) > 0
      AND array_length(e.ask_px) > 0
),
windowed AS (
    SELECT
        *,
        lag(mid) OVER (
            PARTITION BY symbol, date, session ORDER BY row_id
        ) AS prev_mid,
        lag(bid1) OVER (
            PARTITION BY symbol, date, session ORDER BY row_id
        ) AS prev_bid1,
        lag(ask1) OVER (
            PARTITION BY symbol, date, session ORDER BY row_id
        ) AS prev_ask1
    FROM base
),
events AS (
    SELECT
        *,
        mid - prev_mid AS delta_mid,
        abs(mid - prev_mid) AS abs_delta_mid,
        (
            source_action = 'TRADE'
            AND (
                (source_side = 'B' AND ask1 > prev_ask1 AND mid > prev_mid)
                OR
                (source_side = 'S' AND bid1 < prev_bid1 AND mid < prev_mid)
            )
        ) AS is_active_take_mid
    FROM windowed
    WHERE prev_mid IS NOT NULL
)
SELECT
    symbol,
    date,
    100000000::BIGINT AS normalizer_time,
    any_value(close_1000) AS close_1000,
    sum(CASE WHEN is_active_take_mid THEN abs_delta_mid ELSE 0.0 END) / 10000.0
        AS active_take_mid_gap,
    sum(CASE WHEN is_active_take_mid THEN delta_mid ELSE 0.0 END) / 10000.0
        AS active_take_mid_gap_signed,
    sum(abs_delta_mid) / 10000.0 AS all_mid_gap,
    CASE
        WHEN sum(abs_delta_mid) = 0 THEN 0.0
        ELSE sum(CASE WHEN is_active_take_mid THEN abs_delta_mid ELSE 0.0 END)
            / sum(abs_delta_mid)
    END AS active_take_mid_gap_ratio,
    (
        sum(CASE WHEN is_active_take_mid THEN abs_delta_mid ELSE 0.0 END)
        / 10000.0
    ) / any_value(close_1000) AS active_take_mid_gap_over_1000_close,
    (
        sum(CASE WHEN is_active_take_mid THEN delta_mid ELSE 0.0 END)
        / 10000.0
    ) / any_value(close_1000) AS active_take_mid_gap_signed_over_1000_close,
    count(*) FILTER (WHERE is_active_take_mid) AS active_take_mid_events,
    count(*) FILTER (WHERE abs_delta_mid > 0) AS all_mid_move_events,
    count(*) AS valid_lag_events,
    count(*) FILTER (WHERE session = 'AM') AS am_valid_lag_events,
    count(*) FILTER (WHERE session = 'PM') AS pm_valid_lag_events,
    ? AS factor_version
FROM events
GROUP BY symbol, date
ORDER BY symbol, date
"""
    return con.execute(
        query,
        [close_file, list(paths), FACTOR_VERSION],
    ).fetchall()


def compute_worker(
    paths: Sequence[str],
    close_file: str,
    memory_limit: str,
) -> list[tuple]:
    con = duckdb.connect()
    con.execute("PRAGMA threads=1")
    con.execute(f"PRAGMA memory_limit='{memory_limit}'")
    con.execute("PRAGMA preserve_insertion_order=false")
    try:
        return compute_batch(con, paths, close_file)
    finally:
        con.close()


def write_rows(output: str, rows: Sequence[tuple], append: bool) -> None:
    Path(output).parent.mkdir(parents=True, exist_ok=True)
    with open(output, "a" if append else "w", newline="") as handle:
        writer = csv.writer(handle)
        if not append:
            writer.writerow(FIELDS)
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Compute the full continuous-auction v4 active-take mid-gap "
            "normalized by the ClickHouse 10:00 minute close."
        )
    )
    parser.add_argument(
        "inputs",
        nargs="*",
        default=["/hdd_data/lob/event_depth10_v4/202601/*.parquet"],
    )
    parser.add_argument("--close-file", required=True)
    parser.add_argument(
        "--output",
        default=str(
            PROJECT_ROOT
            / "data/processed/active_take_mid_gap_daily_close1000_v4_202601.csv"
        ),
    )
    parser.add_argument("--batch-size", type=int, default=20)
    parser.add_argument("--limit-files", type=int)
    parser.add_argument("--sample-files", type=int)
    parser.add_argument("--seed", type=int, default=20260729)
    parser.add_argument(
        "--workers",
        type=int,
        default=max(1, min(8, os.cpu_count() or 1)),
    )
    parser.add_argument("--memory-limit-per-worker", default="2GB")
    args = parser.parse_args()

    stock_symbols = load_stock_symbols(args.close_file)
    paths = expand_stock_inputs(args.inputs, stock_symbols)
    if args.sample_files and args.sample_files < len(paths):
        paths = sorted(random.Random(args.seed).sample(paths, args.sample_files))
    if args.limit_files:
        paths = paths[: args.limit_files]
    if not paths:
        raise SystemExit("no stock v4 parquet inputs matched the close universe")

    batches = list(chunks(paths, args.batch_size))
    all_rows: list[tuple] = []
    total_rows = 0
    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        futures = [
            executor.submit(
                compute_worker,
                batch,
                args.close_file,
                args.memory_limit_per_worker,
            )
            for batch in batches
        ]
        for completed, future in enumerate(as_completed(futures), start=1):
            rows = future.result()
            all_rows.extend(rows)
            total_rows += len(rows)
            if completed % 25 == 0 or completed == len(futures):
                print(
                    f"batches={completed}/{len(futures)} rows={total_rows}",
                    flush=True,
                )

    all_rows.sort(key=lambda row: (row[0], row[1]))
    write_rows(args.output, all_rows, append=False)
    print(
        f"files={len(paths)} workers={args.workers} rows={total_rows} "
        f"output={args.output} factor_version={FACTOR_VERSION}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
