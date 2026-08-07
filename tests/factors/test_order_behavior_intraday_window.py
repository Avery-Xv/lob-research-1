from __future__ import annotations

import math
from pathlib import Path

import duckdb
import pytest

from scripts.factors.order_behavior_ratio.intraday_window_factor import (
    calculate_log_factors,
    compute_one,
    validate_window,
)


def test_validate_window_accepts_1000_to_1030() -> None:
    validate_window(100000000, 103000000)


def test_validate_window_rejects_cross_session_window() -> None:
    with pytest.raises(ValueError):
        validate_window(110000000, 133000000)


def test_calculate_log_factors_matches_definitions() -> None:
    vr_log, cr_log, single_size_ratio_log = calculate_log_factors(
        trade_qty=800,
        aggr_order_count=4,
        passive_submit_qty=1000,
        passive_order_count=10,
    )

    assert vr_log == pytest.approx(math.log(800) - math.log(1000))
    assert cr_log == pytest.approx(math.log(4) - math.log(10))
    assert single_size_ratio_log == pytest.approx(vr_log - cr_log)
    assert single_size_ratio_log == pytest.approx(math.log(2.0))


@pytest.mark.parametrize(
    "inputs",
    [
        (0, 4, 1000, 10),
        (800, 0, 1000, 10),
        (800, 4, 0, 10),
        (800, 4, 1000, 0),
    ],
)
def test_calculate_log_factors_does_not_smooth_zero_counts(inputs: tuple[int, ...]) -> None:
    vr_log, cr_log, single_size_ratio_log = calculate_log_factors(*inputs)
    assert single_size_ratio_log is None
    if inputs[0] == 0 or inputs[2] == 0:
        assert vr_log is None
    if inputs[1] == 0 or inputs[3] == 0:
        assert cr_log is None


def test_compute_one_excludes_active_order_adds_before_and_after_trades(
    tmp_path: Path,
) -> None:
    """Cover Shenzhen add-then-trade and Shanghai trade-then-remainder orderings."""
    parquet = tmp_path / "SH600000.parquet"
    con = duckdb.connect()
    con.execute(
        """
        CREATE TABLE events(
            date INTEGER,
            time BIGINT,
            row_id BIGINT,
            source_action VARCHAR,
            source_recid BIGINT,
            source_buy_order_id BIGINT,
            source_sell_order_id BIGINT,
            source_side VARCHAR,
            source_volume BIGINT
        )
        """
    )
    con.executemany(
        "INSERT INTO events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            # True passive orders.
            (20260105, 100001000, 1, "ORDER_ADD", 1, 10, None, "B", 1000),
            (20260105, 100002000, 2, "ORDER_ADD", 2, None, 30, "S", 500),
            # Same numeric ID as the later active buy, but opposite side: still passive.
            # Fully executed Shanghai aggressive sell: TRADE without ORDER_ADD.
            (20260105, 100003000, 3, "TRADE", 101, 10, 20, "S", 300),
            # Shanghai aggressive buy: TRADE followed by published remainder.
            (20260105, 100004000, 4, "TRADE", 102, 30, 40, "B", 200),
            (20260105, 100004000, 5, "ORDER_ADD", 5, 30, None, "B", 500),
            # Shenzhen aggressive buy: ORDER_ADD followed by immediate TRADE.
            (20260105, 100005000, 6, "ORDER_ADD", 6, 50, None, "B", 400),
            (20260105, 100005000, 7, "TRADE", 103, 50, 40, "B", 400),
        ],
    )
    escaped_path = str(parquet).replace("'", "''")
    con.execute(f"COPY events TO '{escaped_path}' (FORMAT PARQUET)")
    con.close()

    _, rows = compute_one(str(parquet), 100000000, 103000000, "1GB")
    assert len(rows) == 1
    row = dict(zip(
        [
            "symbol",
            "date",
            "window_start",
            "window_end",
            "trade_qty",
            "trade_count",
            "aggr_order_count",
            "passive_submit_qty",
            "passive_order_count",
            "vr_log",
            "cr_log",
            "single_size_ratio_log",
            "aggressive_order_add_qty_excluded",
            "aggressive_order_add_count_excluded",
            "unidentified_aggr_trade_qty",
            "unidentified_aggr_trade_count",
            "duplicate_trade_rows_excluded",
            "invalid_order_add_count",
            "is_valid",
            "invalid_reason",
        ],
        rows[0],
    ))
    assert row["trade_qty"] == 900
    assert row["trade_count"] == 3
    assert row["aggr_order_count"] == 3
    assert row["passive_submit_qty"] == 1500
    assert row["passive_order_count"] == 2
    assert row["aggressive_order_add_qty_excluded"] == 900
    assert row["aggressive_order_add_count_excluded"] == 2
    assert row["single_size_ratio_log"] == pytest.approx(
        row["vr_log"] - row["cr_log"]
    )
    assert row["is_valid"] is True
