from __future__ import annotations

import csv
import importlib.util
from pathlib import Path

import duckdb


SCRIPT = (
    Path(__file__).resolve().parents[3]
    / "scripts/factors/stylized_fact_4_6/reproduce_d07.py"
)
SPEC = importlib.util.spec_from_file_location("reproduce_d07", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


def test_event_milliseconds_sql() -> None:
    connection = duckdb.connect()
    expression = MODULE.event_milliseconds_sql("value")
    assert connection.execute(
        f"SELECT {expression} FROM (VALUES (93000260::BIGINT)) x(value)"
    ).fetchone()[0] == 34_200_260


def test_load_mean_thresholds_deduplicates_windows(tmp_path: Path) -> None:
    path = tmp_path / "thresholds.csv"
    with path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["symbol", "date", "threshold_mean_qty"])
        writer.writerow(["SH600000", 20260105, 1000])
        writer.writerow(["SH600000", 20260105, 1000])
        writer.writerow(["SH600000", 20251231, 900])
    assert MODULE.load_mean_thresholds(str(path), 20260101, 20260131) == {
        ("SH600000", 20260105): 1000.0
    }


def test_compute_batch_combines_contiguous_same_order_trades(tmp_path: Path) -> None:
    parquet = tmp_path / "SH600000.parquet"
    connection = duckdb.connect()
    connection.execute(
        """
        COPY (
          SELECT * FROM (VALUES
            (20260105,93000000::BIGINT,1::BIGINT,'ORDER_ADD',1::BIGINT,NULL::BIGINT,'B',102000::BIGINT,1000::BIGINT,'NA',[100000::BIGINT],[102000::BIGINT]),
            (20260105,93000100::BIGINT,2::BIGINT,'TRADE',1::BIGINT,2::BIGINT,'B',102000::BIGINT,500::BIGINT,'FULL',[100000::BIGINT],[104000::BIGINT]),
            (20260105,93000200::BIGINT,3::BIGINT,'TRADE',1::BIGINT,3::BIGINT,'B',104000::BIGINT,500::BIGINT,'FULL',[100000::BIGINT],[106000::BIGINT]),
            (20260105,93005000::BIGINT,4::BIGINT,'ORDER_ADD',NULL::BIGINT,4::BIGINT,'S',104000::BIGINT,100::BIGINT,'NA',[100000::BIGINT],[104000::BIGINT]),
            (20260105,93006000::BIGINT,5::BIGINT,'CANCEL',NULL::BIGINT,4::BIGINT,'S',104000::BIGINT,100::BIGINT,'FULL',[100000::BIGINT],[102000::BIGINT])
          ) t(date,time,row_id,source_action,source_buy_order_id,source_sell_order_id,source_side,source_price,source_volume,source_link_status,bid_px,ask_px)
        ) TO ? (FORMAT PARQUET)
        """,
        [str(parquet)],
    )
    rows = MODULE.compute_batch(
        connection, [str(parquet)], [("SH600000", 20260105, 1000.0)],
        20260105, 20260105, 1_000.0, 5.0,
    )
    records = [dict(zip(MODULE.FIELDS, row)) for row in rows]
    selected = next(
        row for row in records
        if row["window_name"] == "daily_0930_close"
        and row["threshold_version"] == "mean_x05"
        and row["clock_type"] == "event" and row["horizon"] == 1
    )
    assert selected["episode_count"] == 1
    assert selected["buy_episode_count"] == 1
    assert selected["valid_retained_count"] == 1
    assert abs(float(selected["d07_permanent_ratio"]) - 0.5) < 1e-12
    time_selected = next(
        row for row in records
        if row["window_name"] == "daily_0930_close"
        and row["threshold_version"] == "mean_x05"
        and row["clock_type"] == "time" and row["horizon"] == 5000
    )
    assert time_selected["valid_retained_count"] == 1
    assert time_selected["mean_elapsed_ms"] == 5800
