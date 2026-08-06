from __future__ import annotations

import sys
import unittest
from pathlib import Path


BACKTEST_DIR = Path(__file__).resolve().parents[2] / "scripts" / "backtests"
sys.path.insert(0, str(BACKTEST_DIR))

from evaluate_intraday_bottom_inventory_overlay import overlay_day


class BottomInventoryOverlayTest(unittest.TestCase):
    def test_underweight_leg_counts_avoided_loss(self) -> None:
        result = overlay_day(
            long_return=-0.001,
            base_return=-0.003,
            overlay_share=0.2,
            buy_cost_bp=0.0,
            sell_cost_bp=0.0,
        )
        self.assertAlmostEqual(result["long_leg_contribution"], -0.0002)
        self.assertAlmostEqual(result["underweight_leg_contribution"], 0.0006)
        self.assertAlmostEqual(result["gross_active_return"], 0.0004)

    def test_roundtrip_cost_covers_both_legs_and_reversal(self) -> None:
        result = overlay_day(
            long_return=0.0,
            base_return=0.0,
            overlay_share=0.2,
            buy_cost_bp=3.0,
            sell_cost_bp=8.0,
        )
        self.assertAlmostEqual(result["trading_cost"], 0.00044)
        self.assertAlmostEqual(result["net_active_return"], -0.00044)


if __name__ == "__main__":
    unittest.main()
