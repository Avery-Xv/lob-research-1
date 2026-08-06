from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "scripts" / "backtests"))

from backtest_intraday_vr_large_gap_intersection import selection_indices  # noqa: E402


def test_strict_b_intersection_uses_low_low_and_high_high() -> None:
    long, short = selection_indices(
        "large_gap_B",
        [0.1, 0.2, 0.8, 0.9],
        [0.2, 0.8, 0.2, 0.8],
        "strict_both_30",
    )
    assert long == [0]
    assert short == [3]


def test_strict_s_intersection_reverses_gap_but_not_vr() -> None:
    long, short = selection_indices(
        "large_gap_S",
        [0.1, 0.2, 0.8, 0.9],
        [0.8, 0.2, 0.8, 0.2],
        "strict_both_30",
    )
    assert long == [3]
    assert short == [0]
