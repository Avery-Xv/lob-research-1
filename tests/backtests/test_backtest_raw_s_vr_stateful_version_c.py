from __future__ import annotations

import sys
import unittest
from pathlib import Path


BACKTEST_DIR = Path(__file__).resolve().parents[2] / "scripts" / "backtests"
sys.path.insert(0, str(BACKTEST_DIR))

from backtest_raw_s_vr_stateful_version_c import (
    Candidate,
    Observation,
    build_target,
    portfolio_turnover,
    symbol_returns,
)


def candidate(symbol: str, s_percentile: float, b_percentile: float) -> Candidate:
    observation = Observation(symbol, 0.1, 0.1, 0.1, 1_000_000.0, 10.0)
    return Candidate(
        observation, "cap_50_500yi", "non_star_ge_10", "high",
        s_percentile, b_percentile,
    )


class StatefulVersionCTest(unittest.TestCase):
    def test_hysteresis_uses_looser_exit_thresholds(self) -> None:
        candidates = {
            "KEEP": candidate("KEEP", 0.85, 0.20),
            "DROP_S": candidate("DROP_S", 0.79, 0.90),
            "DROP_B": candidate("DROP_B", 0.99, 0.14),
            "ENTER": candidate("ENTER", 0.95, 0.30),
            "NO_ENTER": candidate("NO_ENTER", 0.95, 0.24),
        }
        target, retained, entered = build_target(
            candidates, {"KEEP", "DROP_S", "DROP_B"}, 0.90, 0.80, 0.25, 0.15
        )
        self.assertEqual(retained, {"KEEP"})
        self.assertEqual(entered, {"ENTER"})
        self.assertEqual(target, {"KEEP", "ENTER"})

    def test_turnover_only_uses_weight_deltas(self) -> None:
        buy, sell = portfolio_turnover(
            {"A": 0.5, "B": 0.5}, {"B": 0.5, "C": 0.5}
        )
        self.assertAlmostEqual(buy, 0.5)
        self.assertAlmostEqual(sell, 0.5)

    def test_next_date_is_not_skipped_when_price_missing(self) -> None:
        returns, missing = symbol_returns(
            ["A"], 20260202, 20260203,
            {("A", 20260202): (10.0, 10.1), ("A", 20260204): (12.0, 12.0)},
        )
        self.assertEqual(missing, 1)
        self.assertEqual(returns["A"], (0.0, 0.0, 0.0))

    def test_return_attribution_adds_to_total(self) -> None:
        returns, missing = symbol_returns(
            ["A"], 20260202, 20260203,
            {("A", 20260202): (10.0, 10.2), ("A", 20260203): (10.5, 10.4)},
        )
        first, remainder, total = returns["A"]
        self.assertEqual(missing, 0)
        self.assertAlmostEqual(first + remainder, total)
        self.assertAlmostEqual(total, 0.05)


if __name__ == "__main__":
    unittest.main()
