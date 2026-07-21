#!/usr/bin/env python3
"""
Infer active-take midprice movement from full-depth LOB snapshots.

The parquet files contain event-after full book snapshots, not raw order/trade
messages. This script therefore uses a conservative FIFO-consumption heuristic:

* use only continuous auction time by default;
* keep only adjacent snapshots from the same symbol/date;
* require the mid price to change;
* upward mid move must come from ask-side best levels being consumed;
* downward mid move must come from bid-side best levels being consumed;
* consumption at touched levels must be FIFO from the front of the order queue.

The resulting factor is the daily total absolute midprice change explained by
these inferred active-take events.
"""

from __future__ import annotations

import argparse
import csv
import glob
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[3]


Row = Dict[str, object]
OrdersByPrice = Dict[int, Tuple[int, ...]]


@dataclass
class DailyStats:
    rows: int = 0
    transitions: int = 0
    mid_moves: int = 0
    active_take_events: int = 0
    ambiguous_mid_moves: int = 0
    all_abs_mid_delta: float = 0.0
    active_abs_mid_delta: float = 0.0
    active_signed_mid_delta: float = 0.0
    active_take_qty: int = 0


def parquet_rows(path: str) -> Iterator[Row]:
    query = f"""
SELECT
    date, time,
    bid_px, bid_vol, bid_cnt, bid_ordvol,
    ask_px, ask_vol, ask_cnt, ask_ordvol
FROM file('{path}', Parquet)
FORMAT JSONEachRow
"""
    proc = subprocess.Popen(
        ["clickhouse-local", "--query", query],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert proc.stdout is not None
    for line in proc.stdout:
        if line.strip():
            yield json.loads(line)
    stderr = proc.stderr.read() if proc.stderr is not None else ""
    rc = proc.wait()
    if rc != 0:
        raise RuntimeError(f"clickhouse-local failed for {path}: {stderr.strip()}")


def in_continuous_session(t: int) -> bool:
    return (93000000 <= t < 113000000) or (130000000 <= t < 145700000)


def best(row: Row, side: str) -> Optional[int]:
    px = row[f"{side}_px"]
    if not px:
        return None
    return int(px[0])


def mid(row: Row) -> Optional[float]:
    bid = best(row, "bid")
    ask = best(row, "ask")
    if bid is None or ask is None:
        return None
    return (bid + ask) / 2.0


def orders_by_price(row: Row, side: str) -> OrdersByPrice:
    pxs: Sequence[object] = row[f"{side}_px"]  # type: ignore[assignment]
    cnts: Sequence[object] = row[f"{side}_cnt"]  # type: ignore[assignment]
    ordvol: Sequence[object] = row[f"{side}_ordvol"]  # type: ignore[assignment]
    out: OrdersByPrice = {}
    pos = 0
    for px, cnt in zip(pxs, cnts):
        n = int(cnt)
        out[int(px)] = tuple(int(x) for x in ordvol[pos : pos + n])
        pos += n
    return out


def consume_fifo(old_orders: Tuple[int, ...], qty: int) -> Tuple[int, ...]:
    remaining = qty
    out: List[int] = []
    for vol in old_orders:
        if remaining <= 0:
            out.append(vol)
        elif remaining < vol:
            out.append(vol - remaining)
            remaining = 0
        else:
            remaining -= vol
    if remaining > 0:
        return ()
    return tuple(out)


def fifo_consumed_qty(
    old_orders: Tuple[int, ...], new_orders: Tuple[int, ...]
) -> Optional[int]:
    old_sum = sum(old_orders)
    new_sum = sum(new_orders)
    if new_sum >= old_sum:
        return None
    qty = old_sum - new_sum
    if consume_fifo(old_orders, qty) == new_orders:
        return qty
    return None


def side_consumed_through_price(
    prev: Row, cur: Row, side: str, old_best: int, new_best: int
) -> Optional[int]:
    old_book = orders_by_price(prev, side)
    new_book = orders_by_price(cur, side)

    if side == "ask":
        swept_prices = [p for p in old_book if old_best <= p < new_best]
    else:
        swept_prices = [p for p in old_book if new_best < p <= old_best]

    if old_best not in old_book or not swept_prices:
        return None

    qty = 0
    for price in swept_prices:
        if price in new_book:
            return None
        qty += sum(old_book[price])

    if new_best in old_book and new_best in new_book:
        partial = fifo_consumed_qty(old_book[new_best], new_book[new_best])
        if partial is not None:
            qty += partial

    return qty if qty > 0 else None


def infer_active_take(prev: Row, cur: Row) -> Tuple[bool, int]:
    prev_mid = mid(prev)
    cur_mid = mid(cur)
    if prev_mid is None or cur_mid is None or cur_mid == prev_mid:
        return False, 0

    prev_bid = best(prev, "bid")
    prev_ask = best(prev, "ask")
    cur_bid = best(cur, "bid")
    cur_ask = best(cur, "ask")
    if None in (prev_bid, prev_ask, cur_bid, cur_ask):
        return False, 0

    assert prev_bid is not None and prev_ask is not None
    assert cur_bid is not None and cur_ask is not None

    if cur_mid > prev_mid and cur_ask > prev_ask:
        qty = side_consumed_through_price(prev, cur, "ask", prev_ask, cur_ask)
        return (qty is not None), (qty or 0)

    if cur_mid < prev_mid and cur_bid < prev_bid:
        qty = side_consumed_through_price(prev, cur, "bid", prev_bid, cur_bid)
        return (qty is not None), (qty or 0)

    return False, 0


def symbol_from_path(path: str) -> str:
    return os.path.splitext(os.path.basename(path))[0]


def process_file(path: str) -> Dict[Tuple[str, int], DailyStats]:
    symbol = symbol_from_path(path)
    stats: Dict[Tuple[str, int], DailyStats] = {}
    prev: Optional[Row] = None

    for row in parquet_rows(path):
        date = int(row["date"])
        time = int(row["time"])
        key = (symbol, date)
        stats.setdefault(key, DailyStats()).rows += 1

        if prev is None or int(prev["date"]) != date:
            prev = row
            continue

        if not in_continuous_session(time) or not in_continuous_session(int(prev["time"])):
            prev = row
            continue

        day = stats[key]
        day.transitions += 1
        prev_mid = mid(prev)
        cur_mid = mid(row)
        if prev_mid is None or cur_mid is None:
            prev = row
            continue

        delta = cur_mid - prev_mid
        if delta != 0:
            day.mid_moves += 1
            day.all_abs_mid_delta += abs(delta)
            is_active, qty = infer_active_take(prev, row)
            if is_active:
                day.active_take_events += 1
                day.active_abs_mid_delta += abs(delta)
                day.active_signed_mid_delta += delta
                day.active_take_qty += qty
            else:
                day.ambiguous_mid_moves += 1

        prev = row

    return stats


def write_csv(stats: Dict[Tuple[str, int], DailyStats], output_path: str) -> None:
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "symbol",
        "date",
        "active_take_mid_gap",
        "active_take_mid_gap_signed",
        "active_take_events",
        "active_take_qty",
        "all_mid_gap",
        "mid_moves",
        "ambiguous_mid_moves",
        "transitions",
        "rows",
        "active_mid_move_share",
    ]
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for (symbol, date), s in sorted(stats.items()):
            share = s.active_abs_mid_delta / s.all_abs_mid_delta if s.all_abs_mid_delta else 0.0
            writer.writerow(
                {
                    "symbol": symbol,
                    "date": date,
                    "active_take_mid_gap": s.active_abs_mid_delta / 10000.0,
                    "active_take_mid_gap_signed": s.active_signed_mid_delta / 10000.0,
                    "active_take_events": s.active_take_events,
                    "active_take_qty": s.active_take_qty,
                    "all_mid_gap": s.all_abs_mid_delta / 10000.0,
                    "mid_moves": s.mid_moves,
                    "ambiguous_mid_moves": s.ambiguous_mid_moves,
                    "transitions": s.transitions,
                    "rows": s.rows,
                    "active_mid_move_share": share,
                }
            )


def expand_inputs(patterns: Sequence[str]) -> List[str]:
    paths: List[str] = []
    for pattern in patterns:
        matches = glob.glob(pattern)
        paths.extend(matches or [pattern])
    return sorted(dict.fromkeys(paths))


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Compute active-take midprice gap factor from LOB parquet snapshots."
    )
    parser.add_argument("inputs", nargs="+", help="Parquet file path(s) or glob pattern(s)")
    parser.add_argument(
        "-o",
        "--output",
        default=str(PROJECT_ROOT / "data/processed/active_take_midprice_factor.csv"),
    )
    args = parser.parse_args(argv)

    all_stats: Dict[Tuple[str, int], DailyStats] = {}
    for path in expand_inputs(args.inputs):
        if not os.path.exists(path):
            raise FileNotFoundError(path)
        for key, value in process_file(path).items():
            all_stats[key] = value

    write_csv(all_stats, args.output)
    print(f"wrote {len(all_stats)} rows to {args.output}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
