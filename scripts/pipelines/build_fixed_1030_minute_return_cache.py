#!/usr/bin/env python3
"""Build an immutable January 2026 minute-return label cache for fixed-10:30 studies."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path


TABLE = "dwd_dwd.dwd_quant_stock_none_1min_di"
TIMES = ("10:25", "10:30", "10:31", "10:35", "10:40", "11:00", "15:00")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_completion(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("kind") != "factor_run_completion" or payload.get("status") != "completed_audited" or payload.get("factor_id") != "F014":
        raise ValueError(f"not a completed_audited F014 completion: {path}")
    return payload


def sql() -> str:
    columns = ",\n       ".join(
        f"maxIf(close, formatDateTime(dt, '%H:%i')='{value}') AS close_{value.replace(':', '')}"
        for value in TIMES
    )
    quoted_times = ",".join(f"'{value}'" for value in TIMES)
    return f"""
SELECT symbol,
       toUInt32(formatDateTime(trade_date, '%Y%m%d')) AS date,
       {columns}
FROM {TABLE}
WHERE trade_date BETWEEN toDate('2026-01-01') AND toDate('2026-01-31')
  AND (startsWith(symbol, 'SH') OR startsWith(symbol, 'SZ'))
  AND formatDateTime(dt, '%H:%i') IN ({quoted_times})
GROUP BY symbol, trade_date
HAVING countDistinct(formatDateTime(dt, '%H:%i')) = {len(TIMES)}
ORDER BY date, symbol
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
    output = args.output_dir / "minute_prices.csv"
    command = [
        "clickhouse-client", "--host", os.environ["CH_HOST"],
        "--port", os.environ["CH_NATIVE_PORT"], "--user", os.environ["CH_USER"],
        "--password", os.environ["CH_PASSWORD"], "--query", sql(),
    ]
    with output.open("wb") as handle:
        subprocess.run(command, check=True, stdout=handle)
    rows = 0
    symbols: set[str] = set()
    dates: set[int] = set()
    seen: set[tuple[str, int]] = set()
    with output.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            key = (row["symbol"], int(row["date"]))
            if key in seen:
                raise ValueError(f"duplicate minute-price key: {key}")
            if not row["symbol"].startswith(("SH", "SZ")):
                raise ValueError(f"unexpected symbol: {row['symbol']}")
            if any(float(row[f"close_{value.replace(':', '')}"]) <= 0 for value in TIMES):
                raise ValueError(f"non-positive minute close: {key}")
            seen.add(key); symbols.add(key[0]); dates.add(key[1]); rows += 1
    if rows < 90_000 or len(dates) != 20:
        raise ValueError(f"unexpected cache coverage: rows={rows}, dates={len(dates)}")
    manifest = {
        "kind": "research_label_cache", "status": "completed",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "source_table": TABLE,
        "source_rule": "unadjusted stock minute bars; timestamp is minute end",
        "signal_cutoff": "10:30:00",
        "entry_rule": "10:31 minute close; one full minute after signal cutoff",
        "exit_rules": ["10:35 close", "10:40 close", "11:00 close", "15:00 close"],
        "factor_completion": str(args.factor_completion.resolve()),
        "factor_completion_sha256": sha256(args.factor_completion),
        "query_sha256": hashlib.sha256(sql().encode()).hexdigest(),
        "rows": rows, "symbols": len(symbols), "dates": sorted(dates),
        "output": str(output.resolve()), "output_sha256": sha256(output),
    }
    (args.output_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps({"rows": rows, "symbols": len(symbols), "output": str(args.output_dir)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
