#!/usr/bin/env python3
"""Audit a V4 parquet schema and obvious unsafe implementation field usage."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

import duckdb


REQUIRED = {
    "date", "time", "row_id", "source_action", "source_recid",
    "source_buy_order_id", "source_sell_order_id", "source_side",
    "source_price", "source_volume", "bid_px", "ask_px",
}
FORBIDDEN_V4_FIELDS = {"source_order_id", "source_trade_id"}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--parquet", type=Path, required=True)
    parser.add_argument("--implementation", type=Path)
    parser.add_argument("--allow-offline-link-label", action="store_true")
    args = parser.parse_args()

    columns = {
        row[0]: row[1]
        for row in duckdb.connect().execute(
            "DESCRIBE SELECT * FROM read_parquet(?)", [str(args.parquet)]
        ).fetchall()
    }
    errors = []
    missing = sorted(REQUIRED - columns.keys())
    if missing:
        errors.append("missing required V4 fields: " + ", ".join(missing))
    if args.implementation:
        source = args.implementation.read_text(encoding="utf-8")
        for field in sorted(FORBIDDEN_V4_FIELDS):
            if re.search(rf"(?m)^\s*{field}\s*,", source):
                errors.append(f"V4 does not expose {field}; derive IDs from side-specific fields")
        if not args.allow_offline_link_label and re.search(r"\bsource_link_status\b", source):
            errors.append("point-in-time implementation reads post-processed source_link_status")
        side_fields = "source_buy_order_id" in source and "source_sell_order_id" in source
        if "source_action" in source and not side_fields:
            errors.append("event implementation does not reference both side-specific order ID fields")
    payload = {
        "status": "PASS" if not errors else "FAIL",
        "parquet": str(args.parquet.resolve()), "columns": columns, "errors": errors,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
