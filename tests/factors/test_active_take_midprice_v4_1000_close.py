from __future__ import annotations

import csv
from pathlib import Path

import duckdb
import pytest

from scripts.factors.active_take_midprice.daily_factor_v4_1000_close import (
    FIELDS,
    compute_batch,
    expand_stock_inputs,
    load_stock_symbols,
)


def write_events(path: Path, symbol: str) -> Path:
    parquet = path / f"{symbol}.parquet"
    con = duckdb.connect()
    con.execute(
        """
        CREATE TABLE events(
            date INTEGER,
            time BIGINT,
            row_id BIGINT,
            source_action VARCHAR,
            source_side VARCHAR,
            bid_px BIGINT[],
            ask_px BIGINT[]
        )
        """
    )
    con.executemany(
        "INSERT INTO events VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            (20260105, 93000000, 1, "ORDER_ADD", "B", [99900], [100100]),
            (20260105, 100001000, 2, "TRADE", "B", [99900], [100300]),
            (20260105, 112959000, 3, "ORDER_ADD", "S", [99900], [100100]),
            # The 1-yuan lunch jump must not be counted across sessions.
            (20260105, 130000000, 4, "ORDER_ADD", "B", [109900], [110100]),
            (20260105, 130001000, 5, "TRADE", "S", [109700], [110100]),
        ],
    )
    escaped = str(parquet).replace("'", "''")
    con.execute(f"COPY events TO '{escaped}' (FORMAT PARQUET)")
    con.close()
    return parquet


def write_closes(path: Path) -> Path:
    close_file = path / "closes.csv"
    with close_file.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["symbol", "date", "close_1000", "security_category"])
        writer.writerow(["SH600000", "20260105", "10.0", "1"])
    return close_file


def test_factor_uses_1000_close_and_filters_non_stock_inputs(tmp_path: Path) -> None:
    stock = write_events(tmp_path, "SH600000")
    etf = write_events(tmp_path, "SH510300")
    close_file = write_closes(tmp_path)

    symbols = load_stock_symbols(str(close_file))
    inputs = expand_stock_inputs([str(tmp_path / "*.parquet")], symbols)
    assert inputs == [str(stock)]
    assert str(etf) not in inputs

    con = duckdb.connect()
    rows = compute_batch(
        con,
        inputs,
        str(close_file),
    )
    con.close()

    assert len(rows) == 1
    row = dict(zip(FIELDS, rows[0]))
    assert row["close_1000"] == pytest.approx(10.0)
    assert row["active_take_mid_gap"] == pytest.approx(0.02)
    assert row["active_take_mid_gap_over_1000_close"] == pytest.approx(0.002)
    assert row["active_take_mid_events"] == 2
    assert row["all_mid_gap"] == pytest.approx(0.03)
    assert row["am_valid_lag_events"] == 2
    assert row["pm_valid_lag_events"] == 1


def test_close_file_rejects_non_stock_security(tmp_path: Path) -> None:
    close_file = tmp_path / "bad.csv"
    close_file.write_text(
        "symbol,date,close_1000,security_category\n"
        "SH510300,20260105,4.0,8\n"
    )
    with pytest.raises(ValueError, match="non-stock security"):
        load_stock_symbols(str(close_file))
