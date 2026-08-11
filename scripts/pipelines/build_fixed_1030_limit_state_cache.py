#!/usr/bin/env python3
"""Build immutable 10:30-10:35 price-limit state labels for January 2026."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path


MINUTE_TABLE = "dwd_dwd.dwd_quant_stock_none_1min_di"
LIMIT_TABLE = "dwd_dwd.dwd_quant_stk_limit_eod_1day_di"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_completion(path: Path) -> None:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("kind") != "factor_run_completion" or payload.get("status") != "completed_audited" or payload.get("factor_id") != "F014":
        raise ValueError(f"not a completed_audited F014 completion: {path}")


def sql() -> str:
    tolerance = "1e-6"
    return f"""
SELECT symbol,date,up_limit,down_limit,
       signal_close_at_up,signal_close_at_down,signal_locked_up,signal_locked_down,
       entry_close_at_up,entry_close_at_down,entry_locked_up,entry_locked_down,
       post_entry_touch_up,post_entry_touch_down,
       exit_close_at_up,exit_close_at_down,exit_locked_up,exit_locked_down
FROM
(
    SELECT symbol,date,up_limit,down_limit,
           toUInt8(abs(signal_close-up_limit)<={tolerance}) AS signal_close_at_up,
           toUInt8(abs(signal_close-down_limit)<={tolerance}) AS signal_close_at_down,
           toUInt8(greatest(abs(signal_open-up_limit),abs(signal_high-up_limit),abs(signal_low-up_limit),abs(signal_close-up_limit))<={tolerance}) AS signal_locked_up,
           toUInt8(greatest(abs(signal_open-down_limit),abs(signal_high-down_limit),abs(signal_low-down_limit),abs(signal_close-down_limit))<={tolerance}) AS signal_locked_down,
           toUInt8(abs(entry_close-up_limit)<={tolerance}) AS entry_close_at_up,
           toUInt8(abs(entry_close-down_limit)<={tolerance}) AS entry_close_at_down,
           toUInt8(greatest(abs(entry_open-up_limit),abs(entry_high-up_limit),abs(entry_low-up_limit),abs(entry_close-up_limit))<={tolerance}) AS entry_locked_up,
           toUInt8(greatest(abs(entry_open-down_limit),abs(entry_high-down_limit),abs(entry_low-down_limit),abs(entry_close-down_limit))<={tolerance}) AS entry_locked_down,
           toUInt8(post_entry_high>=up_limit-{tolerance}) AS post_entry_touch_up,
           toUInt8(post_entry_low<=down_limit+{tolerance}) AS post_entry_touch_down,
           toUInt8(abs(exit_close-up_limit)<={tolerance}) AS exit_close_at_up,
           toUInt8(abs(exit_close-down_limit)<={tolerance}) AS exit_close_at_down,
           toUInt8(greatest(abs(exit_open-up_limit),abs(exit_high-up_limit),abs(exit_low-up_limit),abs(exit_close-up_limit))<={tolerance}) AS exit_locked_up,
           toUInt8(greatest(abs(exit_open-down_limit),abs(exit_high-down_limit),abs(exit_low-down_limit),abs(exit_close-down_limit))<={tolerance}) AS exit_locked_down
    FROM
    (
        SELECT m.symbol,toUInt32(formatDateTime(m.trade_date,'%Y%m%d')) AS date,
               any(l.up_limit) AS up_limit,any(l.down_limit) AS down_limit,
               maxIf(m.open,formatDateTime(m.dt,'%H:%i')='10:30') AS signal_open,
               maxIf(m.high,formatDateTime(m.dt,'%H:%i')='10:30') AS signal_high,
               maxIf(m.low,formatDateTime(m.dt,'%H:%i')='10:30') AS signal_low,
               maxIf(m.close,formatDateTime(m.dt,'%H:%i')='10:30') AS signal_close,
               maxIf(m.open,formatDateTime(m.dt,'%H:%i')='10:31') AS entry_open,
               maxIf(m.high,formatDateTime(m.dt,'%H:%i')='10:31') AS entry_high,
               maxIf(m.low,formatDateTime(m.dt,'%H:%i')='10:31') AS entry_low,
               maxIf(m.close,formatDateTime(m.dt,'%H:%i')='10:31') AS entry_close,
               maxIf(m.high,formatDateTime(m.dt,'%H:%i') BETWEEN '10:32' AND '10:35') AS post_entry_high,
               minIf(m.low,formatDateTime(m.dt,'%H:%i') BETWEEN '10:32' AND '10:35') AS post_entry_low,
               maxIf(m.open,formatDateTime(m.dt,'%H:%i')='10:35') AS exit_open,
               maxIf(m.high,formatDateTime(m.dt,'%H:%i')='10:35') AS exit_high,
               maxIf(m.low,formatDateTime(m.dt,'%H:%i')='10:35') AS exit_low,
               maxIf(m.close,formatDateTime(m.dt,'%H:%i')='10:35') AS exit_close
        FROM {MINUTE_TABLE} m
        INNER JOIN {LIMIT_TABLE} l ON m.symbol=l.symbol AND m.trade_date=l.trade_date
        WHERE m.trade_date BETWEEN toDate('2026-01-01') AND toDate('2026-01-31')
          AND formatDateTime(m.dt,'%H:%i') BETWEEN '10:30' AND '10:35'
          AND (startsWith(m.symbol,'SH') OR startsWith(m.symbol,'SZ'))
        GROUP BY m.symbol,m.trade_date
        HAVING countDistinct(formatDateTime(m.dt,'%H:%i'))=6
    )
)
ORDER BY date,symbol
FORMAT CSVWithNames
""".strip()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--factor-completion", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    validate_completion(args.factor_completion)
    if args.output_dir.exists():
        raise SystemExit(f"Refusing to overwrite output: {args.output_dir}")
    required = ("CH_HOST", "CH_NATIVE_PORT", "CH_USER", "CH_PASSWORD")
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        raise SystemExit("missing ClickHouse environment: " + ", ".join(missing))

    args.output_dir.mkdir(parents=True)
    output = args.output_dir / "limit_states.csv"
    command = [
        "clickhouse-client", "--host", os.environ["CH_HOST"], "--port", os.environ["CH_NATIVE_PORT"],
        "--user", os.environ["CH_USER"], "--password", os.environ["CH_PASSWORD"], "--query", sql(),
    ]
    with output.open("wb") as handle:
        subprocess.run(command, check=True, stdout=handle)

    rows = 0; dates: set[int] = set(); symbols: set[str] = set(); seen: set[tuple[str, int]] = set()
    counts: dict[str, int] = {}
    with output.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            key = (row["symbol"], int(row["date"]))
            if key in seen:
                raise ValueError(f"duplicate limit-state key: {key}")
            seen.add(key); symbols.add(key[0]); dates.add(key[1]); rows += 1
            for field, value in row.items():
                if field.endswith(("_up", "_down")) and value == "1":
                    counts[field] = counts.get(field, 0) + 1
    if rows < 90_000 or len(dates) != 20:
        raise ValueError(f"unexpected cache coverage: rows={rows}, dates={len(dates)}")

    manifest = {
        "kind": "research_label_cache", "status": "completed",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "source_tables": [MINUTE_TABLE, LIMIT_TABLE],
        "source_rule": "unadjusted 1-minute OHLC joined to authoritative daily up_limit/down_limit",
        "signal_bar": "10:30 minute", "entry_bar": "10:31 minute", "exit_bar": "10:35 minute",
        "post_entry_touch_window": "10:32 through 10:35 minute bars",
        "locked_definition": "minute open=high=low=close=the corresponding daily limit within 1e-6",
        "factor_completion": str(args.factor_completion.resolve()),
        "factor_completion_sha256": sha256(args.factor_completion),
        "query_sha256": hashlib.sha256(sql().encode()).hexdigest(),
        "rows": rows, "symbols": len(symbols), "dates": sorted(dates), "flag_counts": counts,
        "output": str(output.resolve()), "output_sha256": sha256(output),
    }
    (args.output_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8",
    )
    print(json.dumps({"rows": rows, "symbols": len(symbols), "counts": counts, "output": str(args.output_dir)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
