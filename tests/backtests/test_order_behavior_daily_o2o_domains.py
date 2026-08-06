from __future__ import annotations

import sys
import unittest
from pathlib import Path


BACKTEST_DIR = Path(__file__).resolve().parents[2] / "scripts" / "backtests"
sys.path.insert(0, str(BACKTEST_DIR))

from backtest_order_behavior_daily_o2o_domains import (  # noqa: E402
    percentile_scores,
    quantile,
    winsorize,
)


class OrderBehaviorDailyO2ODomainsTest(unittest.TestCase):
    def test_quantile_interpolates(self) -> None:
        self.assertEqual(quantile([0.0, 10.0], 0.25), 2.5)

    def test_winsorize_clips_both_tails(self) -> None:
        self.assertEqual(winsorize([0.0, 1.0, 2.0, 100.0], 0.25, 0.75), [0.75, 1.0, 2.0, 26.5])

    def test_percentile_scores_preserve_ties(self) -> None:
        scores = percentile_scores([1.0, 1.0, 3.0])
        self.assertEqual(scores[0], scores[1])
        self.assertLess(scores[0], scores[2])


if __name__ == "__main__":
    unittest.main()
