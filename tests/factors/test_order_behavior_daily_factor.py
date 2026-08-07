from __future__ import annotations

import csv
import math
from pathlib import Path

from scripts.factors.order_behavior_ratio.build_daily_factor import combine_sessions
from scripts.factors.order_behavior_ratio.intraday_window_factor import FIELDS


def write_session(path: Path, values: dict[str, object]) -> None:
    row = {field: 0 for field in FIELDS}
    row.update(
        {
            "symbol": "SH600000",
            "date": "20260105",
            "window_start": 93000000,
            "window_end": 113000000,
            "is_valid": True,
            "invalid_reason": "",
            **values,
        }
    )
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerow(row)


def test_combine_sessions_recomputes_daily_ratios(tmp_path: Path) -> None:
    morning = tmp_path / "morning.csv"
    afternoon = tmp_path / "afternoon.csv"
    output = tmp_path / "daily.csv"
    write_session(
        morning,
        {
            "trade_qty": 600,
            "aggr_order_count": 3,
            "passive_submit_qty": 1000,
            "passive_order_count": 5,
        },
    )
    write_session(
        afternoon,
        {
            "trade_qty": 400,
            "aggr_order_count": 2,
            "passive_submit_qty": 1000,
            "passive_order_count": 5,
        },
    )

    assert combine_sessions([str(morning), str(afternoon)], str(output), 2) == 1
    row = next(csv.DictReader(output.open()))
    assert row["session_count"] == "2"
    assert math.isclose(float(row["vr_log"]), math.log(0.5), rel_tol=0.0, abs_tol=1e-15)
    assert math.isclose(float(row["cr_log"]), math.log(0.5), rel_tol=0.0, abs_tol=1e-15)
    assert math.isclose(float(row["single_size_ratio_log"]), 0.0, rel_tol=0.0, abs_tol=1e-15)
    assert row["is_valid"] == "True"
