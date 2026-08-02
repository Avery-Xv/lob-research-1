from __future__ import annotations

import math
import sys
from pathlib import Path


BACKTEST_DIR = Path(__file__).resolve().parents[2] / "scripts" / "backtests"
sys.path.insert(0, str(BACKTEST_DIR))

from analyze_existing_factors_lob6 import exposure_by_date  # noqa: E402


def test_exposure_by_date_identifies_style_factor() -> None:
    rows = []
    for index in range(20):
        size = float(index)
        styles = [size, size**2, -size, float(index % 3), size / 2.0, float(index % 5)]
        rows.append((f"SH600{index:03d}", size, (0.0,), styles))
    grouped = {("daily", "size_proxy", "daily_0930_close", "20260105"): rows}

    result = exposure_by_date(grouped)[0]

    assert math.isclose(float(result["size_rank_exposure"]), 1.0)
    assert math.isclose(float(result["joint_r2"]), 1.0)
