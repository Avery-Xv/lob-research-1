#!/usr/bin/env python3
"""Build point-in-time A-share stock manifests for V4 LOB factor jobs."""

from __future__ import annotations

import argparse
import calendar
import json
import os
import subprocess
from datetime import date, datetime
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_V4_ROOT = Path("/hdd_data/lob/event_depth10_v4")


def month_bounds(month: str) -> tuple[date, date]:
    if len(month) != 6 or not month.isdigit():
        raise ValueError(f"invalid month: {month}")
    year, month_number = int(month[:4]), int(month[4:])
    last_day = calendar.monthrange(year, month_number)[1]
    return date(year, month_number, 1), date(year, month_number, last_day)


def query_tsv(sql: str) -> list[list[str]]:
    required = ("CH_HOST", "CH_NATIVE_PORT", "CH_USER", "CH_PASSWORD")
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        raise RuntimeError(
            "missing ClickHouse environment variables: " + ", ".join(missing)
        )
    command = [
        "clickhouse-client",
        "--host",
        os.environ["CH_HOST"],
        "--port",
        os.environ["CH_NATIVE_PORT"],
        "--user",
        os.environ["CH_USER"],
        "--password",
        os.environ["CH_PASSWORD"],
        "-q",
        sql.rstrip().rstrip(";") + " FORMAT TSV",
    ]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or "ClickHouse query failed")
    return [line.split("\t") for line in result.stdout.splitlines() if line]


def load_stock_lifetimes() -> dict[str, tuple[date, date | None]]:
    stock_rows = query_tsv(
        """
        SELECT concat(if(SecuMarket = 83, 'SH', 'SZ'), SecuCode) AS symbol, toString(toDate(ListedDate))
        FROM ods.ods_jydb_secu_main
        WHERE SecuCategory = 1
          AND SecuMarket IN (83, 90)
          AND ListedDate IS NOT NULL
        """
    )
    delist_rows = query_tsv(
        """
        SELECT concat(if(s.SecuMarket = 83, 'SH', 'SZ'), s.SecuCode) AS symbol,
               toString(min(toDate(l.ChangeDate)))
        FROM ods.ods_jydb_lc_list_status l
        INNER JOIN ods.ods_jydb_secu_main s ON l.InnerCode = s.InnerCode
        WHERE l.ChangeType = 4 AND s.SecuCategory = 1 AND s.SecuMarket IN (83, 90)
        GROUP BY symbol
        """
    )
    delist_dates = {
        symbol: date.fromisoformat(delist_date)
        for symbol, delist_date in delist_rows
    }
    return {
        symbol: (date.fromisoformat(listed_date), delist_dates.get(symbol))
        for symbol, listed_date in stock_rows
    }


def is_active_during_month(
    listed_date: date,
    delisted_date: date | None,
    month: str,
) -> bool:
    first_day, last_day = month_bounds(month)
    return listed_date <= last_day and (
        delisted_date is None or delisted_date >= first_day
    )


def build_manifest(
    months: list[str],
    v4_root: Path,
    output: Path,
    metadata_output: Path,
) -> dict[str, object]:
    lifetimes = load_stock_lifetimes()
    included_paths: list[str] = []
    month_stats: dict[str, dict[str, int]] = {}
    included_symbols: set[str] = set()

    for month in months:
        month_dir = v4_root / month
        if not month_dir.is_dir():
            raise FileNotFoundError(f"missing V4 month directory: {month_dir}")
        all_paths = sorted(month_dir.glob("*.parquet"))
        month_paths = []
        for path in all_paths:
            lifetime = lifetimes.get(path.stem)
            if lifetime is None or not is_active_during_month(*lifetime, month):
                continue
            month_paths.append(path)
            included_symbols.add(path.stem)
        included_paths.extend(str(path.resolve()) for path in month_paths)
        month_stats[month] = {
            "all_v4_files": len(all_paths),
            "included_stock_files": len(month_paths),
            "excluded_non_stock_or_inactive_files": len(all_paths) - len(month_paths),
        }

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("".join(f"{path}\n" for path in included_paths))
    metadata = {
        "created_at": datetime.now().astimezone().isoformat(),
        "v4_root": str(v4_root.resolve()),
        "months": months,
        "manifest": str(output.resolve()),
        "universe_rule": (
            "point-in-time Shanghai/Shenzhen A shares only: "
            "SecuCategory=1, SecuMarket in (83,90), listed by month end, "
            "and not terminated before month start"
        ),
        "security_type_whitelist": {"SecuCategory": [1], "SecuMarket": [83, 90]},
        "included_files": len(included_paths),
        "included_symbols": len(included_symbols),
        "output_etf_symbols": 0,
        "month_stats": month_stats,
    }
    metadata_output.parent.mkdir(parents=True, exist_ok=True)
    metadata_output.write_text(json.dumps(metadata, ensure_ascii=False, indent=2) + "\n")
    return metadata


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build point-in-time A-share-only V4 parquet manifests."
    )
    parser.add_argument("--months", nargs="+", required=True)
    parser.add_argument("--v4-root", type=Path, default=DEFAULT_V4_ROOT)
    parser.add_argument(
        "--output",
        type=Path,
        default=PROJECT_ROOT / "data/manifests/v4_a_share_stock_paths.txt",
    )
    parser.add_argument("--metadata-output", type=Path)
    args = parser.parse_args()
    months = sorted(dict.fromkeys(args.months))
    for month in months:
        month_bounds(month)
    metadata_output = args.metadata_output or args.output.with_suffix(".metadata.json")
    metadata = build_manifest(
        months=months,
        v4_root=args.v4_root,
        output=args.output,
        metadata_output=metadata_output,
    )
    print(json.dumps(metadata, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
