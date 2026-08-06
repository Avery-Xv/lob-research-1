from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path

from scripts.backtests.backtest_order_shape_batch_a_domains import (
    DIAGNOSTIC_STYLE_COLS,
    STYLE_COLS,
    factor_values,
    load_previous_styles,
    residualize,
    target_values,
)


class OrderShapeBatchADomainsTest(unittest.TestCase):
    def test_style_spec_excludes_both_size_columns(self) -> None:
        self.assertEqual(STYLE_COLS, ("momentum", "liquidity", "beta", "residual_volatility"))

    def test_intraday_style_is_previous_trading_day(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "styles.csv"
            with path.open("w", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=("symbol", "date", *DIAGNOSTIC_STYLE_COLS))
                writer.writeheader()
                writer.writerow({"symbol": "SH600000", "date": "2025-12-31", "size": -1,
                                 "non_linear_size": -2, "momentum": 1,
                                 "liquidity": 2, "beta": 3, "residual_volatility": 4})
                writer.writerow({"symbol": "SH600000", "date": "2026-01-05", "size": -10,
                                 "non_linear_size": -20, "momentum": 10,
                                 "liquidity": 20, "beta": 30, "residual_volatility": 40})
            values = load_previous_styles(path)
            self.assertEqual(values[("SH600000", 20260105)], [-1.0, -2.0, 1.0, 2.0, 3.0, 4.0])

    def test_factor_and_target_directions(self) -> None:
        row = {
            "chain_net_share": "0.6", "multi_chain_share": "0.25",
            "aggressive_add_buy": "30", "aggressive_add_sell": "10",
            "near_cancel_buy": "5", "near_cancel_sell": "15",
            "active_net_share": "0.5", "pred_fill_buy": "0.3", "pred_fill_sell": "0.7",
            "book_imbalance3": "0.2", "future_buy_volume": "80", "future_sell_volume": "20",
            "future_event_count": "99", "future_realized_vol_bps": "9",
            "end_spread_bps": "12", "spread_bps": "10", "bid_depth3": "100",
            "ask_depth3": "100", "end_bid_depth3": "150", "end_ask_depth3": "50",
        }
        factors = factor_values(row)
        targets = target_values(row)
        self.assertAlmostEqual(factors["single_chain_confirmation"], 0.45)
        self.assertAlmostEqual(factors["multi_chain_exhaustion"], -0.15)
        self.assertAlmostEqual(factors["execution_pressure"], 0.4)
        self.assertAlmostEqual(targets["future_net_share"], 0.6)
        self.assertAlmostEqual(targets["spread_change_bps"], 2.0)
        self.assertAlmostEqual(targets["end_book_imbalance3"], 0.5)

    def test_residualization_removes_linear_style_component(self) -> None:
        exposures = [[float(index)] for index in range(10)]
        values = [2.0 * row[0] + 3.0 for row in exposures]
        residuals = residualize(values, exposures)
        self.assertLess(max(map(abs, residuals)), 1e-10)


if __name__ == "__main__":
    unittest.main()
