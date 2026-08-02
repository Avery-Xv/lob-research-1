from __future__ import annotations

import math
import sys
from pathlib import Path


BACKTEST_DIR = Path(__file__).resolve().parents[2] / "scripts" / "backtests"
sys.path.insert(0, str(BACKTEST_DIR))

from analyze_existing_factors_lob5_ex_size import exposure_by_date  # noqa: E402


def test_exposure_by_date_identifies_non_linear_size_proxy() -> None:
    rows = []
    for index in range(20):
        non_linear_size = float(index)
        styles = [
            non_linear_size,
            -non_linear_size,
            float(index % 3),
            non_linear_size / 2.0,
            float(index % 5),
        ]
        rows.append((f"SH600{index:03d}", non_linear_size, (0.0,), styles))
    grouped = {("daily", "style_proxy", "daily_0930_close", "20260105"): rows}

    result = exposure_by_date(grouped)[0]

    assert math.isclose(float(result["non_linear_size_rank_exposure"]), 1.0)
    assert math.isclose(float(result["joint_r2"]), 1.0)
