from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


SCRIPT = (
    Path(__file__).resolve().parents[2]
    / "scripts/backtests/backtest_stylized_d01_d03_domains.py"
)
SPEC = importlib.util.spec_from_file_location("domain_diagnostics", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class DomainDiagnosticsTest(unittest.TestCase):
    def test_domain_uses_documented_boundaries(self) -> None:
        self.assertEqual(
            MODULE.domain(499_999, 9.99, "SH600000"),
            ("cap_lt_50yi", "non_star_lt_10"),
        )
        self.assertEqual(
            MODULE.domain(500_000, 10.0, "SH600000"),
            ("cap_50_500yi", "non_star_ge_10"),
        )
        self.assertEqual(
            MODULE.domain(5_000_000, 20.0, "SH688001"),
            ("cap_ge_500yi", "star_ge_10"),
        )
        self.assertIsNone(MODULE.domain(5_000_000, 9.0, "SH688001"))

    def test_daily_stat_event_spread_and_deciles(self) -> None:
        rows = [
            (f"S{index:02d}", float(index), float(index) / 10_000, index >= 18)
            for index in range(20)
        ]
        result = MODULE.daily_stat(rows)
        assert result is not None
        self.assertAlmostEqual(float(result["rank_ic"]), 1.0)
        self.assertGreater(float(result["d10_d1"]), 0.0)
        self.assertEqual(result["event_n"], 2)
        self.assertGreater(float(result["event_minus_non_event"]), 0.0)


if __name__ == "__main__":
    unittest.main()
