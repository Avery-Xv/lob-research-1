#!/usr/bin/env python3
"""Extract raw event traces for audited SH remainder and SZ add-before-trade cases."""

from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path

import duckdb


FIELDS = (
    "symbol", "audit_class", "date", "side", "order_id", "time", "row_id",
    "source_action", "source_volume", "source_price", "bid1", "ask1",
)


def load_paths(file_list: Path) -> dict[tuple[str, str], str]:
    return {
        (Path(line.strip()).stem, Path(line.strip()).parent.name): line.strip()
        for line in file_list.read_text(encoding="utf-8").splitlines()
        if line.strip()
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case-sample", type=Path, required=True)
    parser.add_argument("--file-list", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--per-exchange", type=int, default=20)
    args = parser.parse_args()

    wanted = {"SH": "posttrade_remainder", "SZ": "pretrade_active_add"}
    selected: dict[str, list[dict[str, str]]] = defaultdict(list)
    with args.case_sample.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            exchange = row["symbol"][:2]
            if row["classification"] == wanted.get(exchange) and len(selected[exchange]) < args.per_exchange:
                selected[exchange].append(row)
    if any(len(selected[exchange]) < args.per_exchange for exchange in wanted):
        raise SystemExit(f"insufficient trace cases: {dict((key, len(value)) for key, value in selected.items())}")

    paths = load_paths(args.file_list)
    output_rows = []
    by_symbol: dict[str, list[dict[str, str]]] = defaultdict(list)
    for rows in selected.values():
        for row in rows:
            by_symbol[row["symbol"]].append(row)
    for symbol, cases in sorted(by_symbol.items()):
        con = duckdb.connect()
        for case in cases:
            side = case["side"]
            order_expression = "source_buy_order_id" if side == "B" else "source_sell_order_id"
            rows = con.execute(
                f"""
                SELECT time,row_id,source_action,source_volume,source_price,
                       bid_px[1] AS bid1,ask_px[1] AS ask1
                FROM read_parquet(?)
                WHERE date=? AND source_side=? AND {order_expression}=?
                  AND source_action IN ('ORDER_ADD','TRADE','CANCEL')
                ORDER BY row_id
                """,
                [paths[(symbol, case["date"][:6])], int(case["date"]), side, int(case["order_id"])],
            ).fetchall()
            for values in rows:
                output_rows.append(dict(zip(FIELDS, (
                    symbol, case["classification"], int(case["date"]), side,
                    int(case["order_id"]), *values,
                ))))
        con.close()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(output_rows)
    print(f"wrote {len(output_rows)} rows for {sum(map(len, selected.values()))} cases to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
