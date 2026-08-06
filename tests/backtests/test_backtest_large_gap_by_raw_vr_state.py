from __future__ import annotations

import sys
import unittest
from pathlib import Path


BACKTEST_DIR = Path(__file__).resolve().parents[2] / "scripts" / "backtests"
sys.path.insert(0, str(BACKTEST_DIR))

from backtest_large_gap_by_raw_vr_state import assign_raw_vr_states, score_spread


class RawVrStateTest(unittest.TestCase):
    def test_assigns_exact_terciles_without_neutralizing_values(self) -> None:
        values = [9.0, 1.0, 5.0, 3.0, 7.0, 2.0]
        symbols = ["F", "A", "D", "C", "E", "B"]
        self.assertEqual(
            assign_raw_vr_states(values, symbols),
            ["high", "low", "mid", "mid", "high", "low"],
        )

    def test_score_spread_uses_integer_deciles(self) -> None:
        scores = list(range(20))
        returns = list(range(20))
        symbols = [f"S{index:02d}" for index in range(20)]
        self.assertEqual(score_spread(scores, returns, symbols), 18.0)


if __name__ == "__main__":
    unittest.main()
