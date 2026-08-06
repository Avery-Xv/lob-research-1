from __future__ import annotations

import sys
import unittest
from pathlib import Path


BACKTEST_DIR = Path(__file__).resolve().parents[2] / "scripts" / "backtests"
sys.path.insert(0, str(BACKTEST_DIR))

from backtest_large_gap_b_single_versions import build_scores, decile_means


class BSingleVersionTest(unittest.TestCase):
    def test_reversal_score_penalizes_high_buy_gap(self) -> None:
        scores = build_scores(
            buy_residuals=[1.0, 2.0, 3.0],
            vr_values=[1.0, 2.0, 3.0],
            matched_trade_counts=[10, 20, 30],
            symbols=["A", "B", "C"],
        )
        self.assertGreater(scores["b_reversal"][0], scores["b_reversal"][2])
        self.assertEqual(scores["b_reversal"][1], 0.0)
        self.assertLess(
            abs(scores["b_reversal_vr_reliability"][2]),
            abs(scores["b_reversal_vr"][2]),
        )

    def test_deciles_preserve_low_to_high_score_order(self) -> None:
        values = list(range(100))
        buckets = decile_means(values, values, [f"S{i:03d}" for i in values])
        self.assertEqual(len(buckets), 10)
        self.assertTrue(all(right > left for left, right in zip(buckets, buckets[1:])))


if __name__ == "__main__":
    unittest.main()
