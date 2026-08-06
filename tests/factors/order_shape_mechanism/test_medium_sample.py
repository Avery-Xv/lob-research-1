from __future__ import annotations

import unittest

from scripts.factors.order_shape_mechanism.build_medium_sample import (
    allocate_quotas,
    classify_domain,
    select_domain_symbols,
)


class MediumSampleTest(unittest.TestCase):
    def test_domain_boundaries_and_star_exclusion(self) -> None:
        self.assertEqual(
            classify_domain(499_999, 9.99, "SZ000001"),
            "cap_lt_50yi/nonstar_lt_10",
        )
        self.assertEqual(
            classify_domain(500_000, 10.0, "SH600000"),
            "cap_50_500yi/nonstar_ge_10",
        )
        self.assertEqual(
            classify_domain(5_000_000, 10.0, "SH688001"),
            "cap_ge_500yi/star_ge_10",
        )
        self.assertIsNone(classify_domain(5_000_000, 9.99, "SH688001"))

    def test_three_hundred_quota_is_complete(self) -> None:
        quotas = allocate_quotas(300)
        self.assertEqual(len(quotas), 9)
        self.assertEqual(sum(quotas.values()), 300)
        self.assertEqual(sorted(quotas.values()), [33] * 6 + [34] * 3)

    def test_nonstar_selection_balances_exchanges(self) -> None:
        symbols = [f"SH60{index:04d}" for index in range(50)] + [
            f"SZ00{index:04d}" for index in range(50)
        ]
        selected = select_domain_symbols(
            symbols, 33, "cap_lt_50yi/nonstar_lt_10"
        )
        sh = sum(symbol.startswith("SH") for symbol in selected)
        sz = sum(symbol.startswith("SZ") for symbol in selected)
        self.assertEqual(len(selected), 33)
        self.assertLessEqual(abs(sh - sz), 1)


if __name__ == "__main__":
    unittest.main()
