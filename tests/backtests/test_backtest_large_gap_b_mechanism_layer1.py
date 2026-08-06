from __future__ import annotations

import sys
import unittest
from pathlib import Path


BACKTEST_DIR = Path(__file__).resolve().parents[2] / "scripts" / "backtests"
sys.path.insert(0, str(BACKTEST_DIR))

from backtest_large_gap_b_mechanism_layer1 import (
    Observation,
    build_model_residuals,
    exact_bucket_labels,
)


def observation(symbol: str, buy: float, pre_return: float) -> Observation:
    return Observation(
        symbol=symbol,
        buy_gap=buy,
        sell_gap=0.3,
        vr_log=0.2,
        cr_log=0.1,
        single_size_ratio_log=0.1,
        pre_return=pre_return,
        targets=(0.0, 0.0, 0.0),
        styles=(0.0, 0.0, 0.0, 0.0),
        previous_market_cap=100.0,
        signal_price=10.0,
    )


class MechanismLayerOneTest(unittest.TestCase):
    def test_pre_return_control_removes_monotonic_b_component(self) -> None:
        rows = [
            observation("A", 1.0, 1.0),
            observation("B", 2.0, 2.0),
            observation("C", 3.0, 3.0),
            observation("D", 4.0, 4.0),
        ]
        residuals = build_model_residuals(rows)
        self.assertGreater(max(abs(value) for value in residuals["m0_b_only"]), 0.1)
        self.assertLess(max(abs(value) for value in residuals["m1_pre_return"]), 1e-10)

    def test_exact_bucket_labels_have_deterministic_counts(self) -> None:
        labels = exact_bucket_labels(
            [float(value) for value in range(10)],
            [f"S{value}" for value in range(10)],
            3,
            "g",
        )
        self.assertEqual(labels.count("g1"), 4)
        self.assertEqual(labels.count("g2"), 3)
        self.assertEqual(labels.count("g3"), 3)


if __name__ == "__main__":
    unittest.main()
