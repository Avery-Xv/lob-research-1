#!/usr/bin/env python3
"""Consolidate and quality-check a completed F014 window-path shard run."""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import duckdb


EXPECTED_VERSION = "non_parent_window_path_1000_1030_v1_20260810"


def write_json(path: Path, value: dict[str, object]) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")
    os.replace(temporary, path)


def quoted_paths(paths: list[Path]) -> str:
    return "[" + ",".join("'" + str(path.resolve()).replace("'", "''") + "'" for path in paths) + "]"


def target_symbols(file_list: Path, month: str) -> set[str]:
    return {
        path.stem for raw in file_list.read_text().splitlines()
        if raw.strip() and (path := Path(raw.strip())).parent.name == month
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--shard-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    shard_dir = args.shard_dir.resolve(); output_dir = args.output_dir.resolve()
    if output_dir.exists():
        raise SystemExit(f"Refusing to overwrite output: {output_dir}")
    manifest = json.loads((shard_dir / "manifest.json").read_text())
    config = manifest.get("config", {})
    if config.get("factor_version") != EXPECTED_VERSION or config.get("target_month") != "202601":
        raise SystemExit("unexpected shard definition")
    expected_symbols = int(config["symbols"])
    batch_size = int(config["batch_size"])
    expected_batches = (expected_symbols + batch_size - 1) // batch_size
    batches = sorted(shard_dir.glob("batch_*"))
    if len(batches) != expected_batches:
        raise SystemExit(f"expected {expected_batches} batches, found {len(batches)}")
    signal_paths: list[Path] = []; quality_paths: list[Path] = []; done_symbols: list[str] = []
    for number, batch in enumerate(batches, start=1):
        if batch.name != f"batch_{number:06d}":
            raise SystemExit(f"non-contiguous batch: {batch.name}")
        signal = batch / "window_paths.csv"; quality = batch / "quality.csv"; done = batch / "done.json"
        if not all(path.is_file() for path in (signal, quality, done)):
            raise SystemExit(f"incomplete batch: {batch}")
        payload = json.loads(done.read_text())
        if payload.get("factor_version") != EXPECTED_VERSION:
            raise SystemExit(f"version mismatch: {done}")
        done_symbols.extend(str(symbol) for symbol in payload["symbols"])
        signal_paths.append(signal); quality_paths.append(quality)
    file_list = Path(str(config["file_list"]))
    metadata_path = Path(str(config["universe_metadata"]))
    metadata = json.loads(metadata_path.read_text())
    whitelist = target_symbols(file_list, "202601")
    symbol_checks = {
        "done_symbol_count": len(done_symbols) == expected_symbols,
        "done_symbols_unique": len(set(done_symbols)) == len(done_symbols),
        "done_symbols_match_certified_manifest": set(done_symbols) == whitelist,
        "metadata_etf_zero": metadata.get("output_etf_symbols") == 0,
    }
    output_dir.mkdir(parents=True)
    signal_output = output_dir / "window_paths.parquet"
    quality_output = output_dir / "quality.parquet"
    connection = duckdb.connect()
    connection.execute("PRAGMA threads=4")
    connection.execute("PRAGMA memory_limit='8GB'")
    connection.execute(
        f"COPY (SELECT * FROM read_csv_auto({quoted_paths(signal_paths)}, header=true, union_by_name=true)) "
        f"TO '{str(signal_output).replace("'", "''")}' (FORMAT PARQUET, COMPRESSION ZSTD)"
    )
    connection.execute(
        f"COPY (SELECT * FROM read_csv_auto({quoted_paths(quality_paths)}, header=true, union_by_name=true)) "
        f"TO '{str(quality_output).replace("'", "''")}' (FORMAT PARQUET, COMPRESSION ZSTD)"
    )
    signal_stats = connection.execute("""
        SELECT count(*) AS rows, count(DISTINCT symbol) AS symbols,
               count(DISTINCT date) AS dates, min(date) AS min_date, max(date) AS max_date,
               count(*)-count(DISTINCT symbol || '/' || date::VARCHAR || '/' || signal_time::VARCHAR) AS duplicate_keys,
               count(*) FILTER (WHERE signal_time<>1030) AS bad_signal_time,
               count(DISTINCT factor_version) AS versions,
               count(*) FILTER (WHERE factor_version<>?) AS bad_version,
               count(*) FILTER (WHERE book30m_coverage_ratio<0 OR book30m_coverage_ratio>1.000001
                                  OR book5m_coverage_ratio<0 OR book5m_coverage_ratio>1.000001) AS bad_coverage,
               count(*) FILTER (WHERE invalid_chain_seconds<0 OR invalid_chain_seconds>1800.001) AS bad_invalid_seconds,
               count(*) FILTER (WHERE flow1m_buy_volume>flow5m_buy_volume OR flow1m_sell_volume>flow5m_sell_volume
                                  OR flow5m_buy_volume>flow30m_buy_volume OR flow5m_sell_volume>flow30m_sell_volume) AS bad_signal_nesting,
               count(*) FILTER (WHERE future1m_buy_volume>future5m_buy_volume OR future1m_sell_volume>future5m_sell_volume
                                  OR future5m_buy_volume>future10m_buy_volume OR future5m_sell_volume>future10m_sell_volume
                                  OR future1m_event_count>future5m_event_count OR future5m_event_count>future10m_event_count) AS bad_target_nesting,
               count(*) FILTER (WHERE symbol LIKE 'SH%') AS sh_rows,
               count(*) FILTER (WHERE symbol LIKE 'SZ%') AS sz_rows,
               count(*) FILTER (WHERE book30m_coverage_ratio<0.999999) AS incomplete_30m_coverage,
               count(*) FILTER (WHERE book5m_coverage_ratio<0.999999) AS incomplete_5m_coverage
        FROM read_parquet(?)
    """, [EXPECTED_VERSION, str(signal_output)]).fetchone()
    signal_columns = [item[0] for item in connection.description]
    signal_summary = dict(zip(signal_columns, signal_stats))
    quality_summary_rows = connection.execute("""
        SELECT left(symbol,2) AS exchange, count(*) AS stock_days,
               sum(total_events)::BIGINT AS total_events,
               sum(duplicate_trades)::BIGINT AS duplicate_trades,
               sum(missing_active_order_id)::BIGINT AS missing_active_order_id,
               sum(missing_book_rows)::BIGINT AS missing_book_rows,
               sum(locked_book_rows)::BIGINT AS locked_book_rows,
               sum(crossed_book_rows)::BIGINT AS crossed_book_rows,
               sum(invalid_chain_seconds) AS invalid_chain_seconds
        FROM read_parquet(?) GROUP BY 1 ORDER BY 1
    """, [str(quality_output)]).fetchall()
    quality_columns = [item[0] for item in connection.description]
    quality_summary = [dict(zip(quality_columns, row)) for row in quality_summary_rows]
    output_symbols = {row[0] for row in connection.execute("SELECT DISTINCT symbol FROM read_parquet(?)", [str(signal_output)]).fetchall()}
    connection.close()
    checks = {
        **symbol_checks,
        "output_symbols_subset_of_certified_manifest": output_symbols <= whitelist,
        "no_duplicate_signal_keys": signal_summary["duplicate_keys"] == 0,
        "signal_time_is_1030": signal_summary["bad_signal_time"] == 0,
        "single_expected_version": signal_summary["versions"] == 1 and signal_summary["bad_version"] == 0,
        "coverage_in_bounds": signal_summary["bad_coverage"] == 0,
        "invalid_duration_in_bounds": signal_summary["bad_invalid_seconds"] == 0,
        "signal_windows_nested": signal_summary["bad_signal_nesting"] == 0,
        "target_windows_nested": signal_summary["bad_target_nesting"] == 0,
        "both_exchanges_present": signal_summary["sh_rows"] > 0 and signal_summary["sz_rows"] > 0,
    }
    status = "PASS" if all(checks.values()) else "FAIL"
    summary = {
        "kind": "window_path_full_market_qc", "schema_version": 1,
        "status": status, "created_at": datetime.now(timezone.utc).isoformat(),
        "definition_version": EXPECTED_VERSION,
        "source_shard_dir": str(shard_dir), "expected_batches": expected_batches,
        "expected_symbols": expected_symbols, "certified_universe_symbols": len(whitelist),
        "checks": checks, "signal_summary": signal_summary,
        "quality_by_exchange": quality_summary,
        "universe_rule": metadata.get("universe_rule"), "output_etf_symbols": 0,
        "book_rule": config.get("book_rule"), "signal_rule": config.get("signal_rule"),
        "direct_targets": config.get("direct_targets"),
        "outputs": {"window_paths": str(signal_output), "quality": str(quality_output)},
    }
    write_json(output_dir / "qc_summary.json", summary)
    print(json.dumps({"status": status, "rows": signal_summary["rows"], "symbols": signal_summary["symbols"], "output": str(output_dir)}))
    return 0 if status == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
