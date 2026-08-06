from __future__ import annotations

import sys
import unittest
from pathlib import Path


BACKTEST_DIR = Path(__file__).resolve().parents[2] / "scripts" / "backtests"
sys.path.insert(0, str(BACKTEST_DIR))

from backtest_daily_vr_large_gap_intersection import selection_indices  # noqa: E402


class DailyVrLargeGapIntersectionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.b = [0.1, 0.2, 0.8, 0.9]
        self.vr = [0.2, 0.8, 0.2, 0.9]

    def test_b_baseline_ignores_vr(self) -> None:
        self.assertEqual(selection_indices(self.b, self.vr, "b_baseline_30"), ([0, 1], [2, 3]))

    def test_strict_intersection_requires_matching_extremes(self) -> None:
        self.assertEqual(selection_indices(self.b, self.vr, "strict_both_30"), ([0], [3]))

    def test_median_filter_keeps_confirmed_b_tails(self) -> None:
        self.assertEqual(selection_indices(self.b, self.vr, "vr_median_filter"), ([0], [3]))

    def test_short_confirmation_leaves_long_leg_unfiltered(self) -> None:
        self.assertEqual(selection_indices(self.b, self.vr, "short_vr_confirmed"), ([0, 1], [3]))

    def test_mismatched_vectors_are_rejected(self) -> None:
        with self.assertRaises(ValueError):
            selection_indices([0.1], [0.1, 0.2], "b_baseline_30")


if __name__ == "__main__":
    unittest.main()
