from __future__ import annotations

import sys
import unittest
from pathlib import Path


BACKTEST_DIR = Path(__file__).resolve().parents[2] / "scripts" / "backtests"
sys.path.insert(0, str(BACKTEST_DIR))

from backtest_large_gap_by_raw_vr_state_all_market_neutral import (
    all_market_residuals,
    load_common,
    rename_neutral_metrics,
)


class AllMarketNeutralTest(unittest.TestCase):
    def test_residualizes_before_domain_without_using_market_cap(self) -> None:
        rows = [
            ("A", 0.1, 1.0, (0.0,), [0.0], 100.0, 5.0),
            ("B", 0.2, 3.0, (0.0,), [1.0], 200.0, 15.0),
            ("C", 0.3, 2.0, (0.0,), [2.0], 300.0, 25.0),
        ]
        residuals = all_market_residuals(rows)
        self.assertEqual(set(residuals), {"A", "B", "C"})
        self.assertAlmostEqual(sum(residuals.values()), 0.0)
        self.assertAlmostEqual(sum(residuals[row[0]] * row[4][0] for row in rows), 0.0)

    def test_rejects_unsafe_target_identifier(self) -> None:
        with self.assertRaisesRegex(ValueError, "snake_case"):
            load_common("missing", "missing", "missing", ["ret;drop"], 1, 2)


    def test_lob4_metric_names_are_not_labeled_lob5(self) -> None:
        rows = [{"lob5_ex_size_rank_ic": 0.1, "raw_rank_ic": 0.2}]
        rename_neutral_metrics(rows, "lob4_no_size")
        self.assertEqual(
            rows,
            [{"lob4_no_size_rank_ic": 0.1, "raw_rank_ic": 0.2}],
        )


if __name__ == "__main__":
    unittest.main()
