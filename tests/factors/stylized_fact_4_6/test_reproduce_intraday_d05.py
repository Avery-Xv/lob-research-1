from __future__ import annotations

import csv
import importlib.util
from pathlib import Path


SCRIPT = (
    Path(__file__).resolve().parents[3]
    / "scripts/factors/stylized_fact_4_6/reproduce_intraday_d05.py"
)
SPEC = importlib.util.spec_from_file_location("reproduce_intraday_d05", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


def test_d05_requires_strictly_lagged_same_window_observations() -> None:
    state = MODULE.D05State(length=3)
    assert state.update(1.0, 2.0, 3.0)["d05_surprise_60"] is None
    assert state.update(2.0, 3.0, 4.0)["d05_surprise_60"] is None
    assert state.update(3.0, 4.0, 5.0)["d05_surprise_60"] is None
    result = state.update(10.0, 11.0, 12.0)
    assert result["d05_history_observations"] == 3
    assert float(result["d05_surprise_60"]) > 0
    assert float(result["d05_buy_surprise_60"]) > 0
    assert float(result["d05_sell_surprise_60"]) > 0


def test_previous_market_dates_skips_weekend() -> None:
    controls = {
        ("SH600000", 20260109): {},
        ("SH600000", 20260112): {},
        ("SZ000001", 20260109): {},
        ("SZ000001", 20260112): {},
    }
    assert MODULE.previous_market_dates(controls)[20260112] == 20260109


def _primitive(symbol: str, date: int, alf: float) -> dict[str, object]:
    row: dict[str, object] = {field: 0 for field in MODULE.PRIMITIVE_FIELDS}
    row.update({
        "symbol": symbol, "date": date, "frequency": "intraday",
        "window_name": MODULE.WINDOW_NAME, "window_start": MODULE.WINDOW_START,
        "window_end": MODULE.WINDOW_END, "threshold_history_days": 20,
        "threshold_history_order_count": 1000, "is_valid": True,
        "invalid_reason": "", "factor_version": MODULE.FACTOR_VERSION,
    })
    for version in MODULE.THRESHOLD_VERSIONS:
        row[f"{version}_buy_exec_qty"] = 100.0 + alf
        row[f"{version}_sell_exec_qty"] = 50.0
        row[f"{version}_buy_order_count"] = 2
        row[f"{version}_sell_order_count"] = 1
        row[f"{version}_alf"] = alf
    return row


def test_finalize_uses_previous_market_day_controls(tmp_path: Path) -> None:
    primitive_path = tmp_path / "primitive.csv"
    controls_path = tmp_path / "controls.csv"
    dates = (20260109, 20260112, 20260113, 20260114)
    with primitive_path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=MODULE.PRIMITIVE_FIELDS)
        writer.writeheader()
        for index, date in enumerate(dates[1:]):
            writer.writerow(_primitive("SH600000", date, float(index + 1)))
    control_fields = [
        "symbol", "date", "security_category", "board", "is_st",
        "is_suspended", "listing_days", "liquidity_history_days",
        *MODULE.STYLE_COLUMNS,
    ]
    with controls_path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=control_fields)
        writer.writeheader()
        for index, date in enumerate(dates):
            writer.writerow({
                "symbol": "SH600000", "date": date, "security_category": 1,
                "board": "MAIN", "is_st": 0, "is_suspended": 0,
                "listing_days": 100, "liquidity_history_days": 20,
                **{name: float(index) for name in MODULE.STYLE_COLUMNS},
            })
    rows = MODULE.finalize_factors(
        str(primitive_path), str(controls_path), 20260112, 20260114,
        min_cross_section=1, surprise_observations=2,
    )
    mean_rows = [row for row in rows if row["threshold_version"] == "mean_x05"]
    assert [row["control_date"] for row in mean_rows] == [20260109, 20260112, 20260113]
    assert mean_rows[-1]["d05_history_observations"] == 2
    assert mean_rows[-1]["d05_surprise_60"] == 0.0
    assert all(row["style_specification"] == "LOB5-ex-size" for row in rows)


def test_sql_source_has_explicit_point_in_time_guards() -> None:
    source = SCRIPT.read_text()
    assert "signal_known AS MATERIALIZED" in source
    assert "time<" in source and "WINDOW_END" in source
    assert "h.day_seq BETWEEN c.day_seq-? AND c.day_seq-1" in source
    assert "source_link_status" not in source
    assert "source_buy_order_recid" not in source
    assert "source_sell_order_recid" not in source
