from __future__ import annotations

import sys
import unittest
from pathlib import Path


BACKTEST_DIR = Path(__file__).resolve().parents[2] / "scripts" / "backtests"
sys.path.insert(0, str(BACKTEST_DIR))

from backtest_raw_s_vr_long_b_filter import (
    Observation,
    exact_top_indices,
    select_long_indices,
)


def row(index: int, buy: float, sell: float) -> Observation:
    return Observation(
        symbol=f"S{index:02d}",
        buy_gap=buy,
        sell_gap=sell,
        vr_log=float(index),
        targets=(None, None, None),
        previous_market_cap=100.0,
        signal_price=10.0,
    )


class RawSVrLongBFilterTest(unittest.TestCase):
    def test_exact_top_selection_is_deterministic(self) -> None:
        indices = exact_top_indices(
            [float(value) for value in range(20)],
            [f"S{value:02d}" for value in range(20)],
            0.10,
        )
        self.assertEqual(indices, [18, 19])

    def test_b_filter_only_removes_eligibility(self) -> None:
        rows = [row(index, float(index), float(index)) for index in range(20)]
        baseline = select_long_indices(rows, "top20", "none")
        filtered = select_long_indices(rows, "top20", "middle_20_90")
        self.assertEqual(baseline, [16, 17, 18, 19])
        self.assertEqual(filtered, [16, 17])


if __name__ == "__main__":
    unittest.main()
