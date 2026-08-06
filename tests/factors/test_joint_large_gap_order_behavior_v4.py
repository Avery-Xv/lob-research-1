from __future__ import annotations

import math
import unittest
from unittest.mock import patch

from scripts.factors.joint_large_gap_order_behavior.compute_v4 import (
    calculate_log_factors,
    calculate_strict_theta,
    validate_v4_path,
)


class JointV4FactorTest(unittest.TestCase):
    def test_strict_theta_uses_market_days_and_requires_all_five(self) -> None:
        calendar = [20260105, 20260106, 20260107, 20260108, 20260109, 20260112]
        observations = {
            20260105: (1.0, 10),
            20260106: (2.0, 10),
            20260107: (3.0, 10),
            20260108: (4.0, 10),
            20260109: (5.0, 10),
            20260112: (9.0, 10),
        }
        result = calculate_strict_theta(observations, calendar, {"202601"})
        self.assertEqual(result[20260112], (9.0, 3.0, 5, 10))

        missing = dict(observations)
        del missing[20260107]
        result = calculate_strict_theta(missing, calendar, {"202601"})
        self.assertEqual(result[20260112], (9.0, None, 4, 10))

        result = calculate_strict_theta(observations, calendar, {"202602"})
        self.assertEqual(result, {})

    def test_log_factor_identity(self) -> None:
        vr, cr, single = calculate_log_factors(200, 10, 100, 20)
        self.assertAlmostEqual(vr, math.log(2))
        self.assertAlmostEqual(cr, math.log(0.5))
        self.assertAlmostEqual(single, math.log(4))

    def test_v4_path_validation_rejects_other_datasets(self) -> None:
        with patch(
            "scripts.factors.joint_large_gap_order_behavior.compute_v4.V4_ROOT",
            __import__("pathlib").Path("/hdd_data/lob/event_depth10_v4"),
        ):
            self.assertEqual(
                validate_v4_path(
                    "/hdd_data/lob/event_depth10_v4/202602/SH600000.parquet",
                    {"202602"},
                ),
                ("SH600000", "202602"),
            )
            with self.assertRaises(ValueError):
                validate_v4_path(
                    "/hdd_data/lob/event_full_depth_v3/202602/SH600000.parquet",
                    {"202602"},
                )


if __name__ == "__main__":
    unittest.main()
