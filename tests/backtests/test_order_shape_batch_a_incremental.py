from __future__ import annotations

import unittest

from scripts.backtests.backtest_order_shape_batch_a_incremental import (
    control_matrix,
    run_incremental,
    score_candidate,
)


def observation(active: float, candidate: float, style: float) -> dict[str, object]:
    return {
        "styles": [10.0, 20.0, style, 0.0, 0.0, 0.0],
        "factors": {
            "active_flow": active,
            "chain_flow": candidate,
            "single_chain_confirmation": candidate,
            "execution_pressure": candidate,
        },
    }


class OrderShapeBatchAIncrementalTest(unittest.TestCase):
    def test_linear_spec_adds_active_flow_after_four_styles(self) -> None:
        rows = [observation(float(index), 0.0, float(index % 2)) for index in range(6)]
        matrix = control_matrix(rows, "m1_linear")
        self.assertEqual(len(matrix[0]), 5)
        self.assertEqual([row[-1] for row in matrix], [float(index) for index in range(6)])

    def test_cubic_spec_removes_nonlinear_active_flow_component(self) -> None:
        rows = []
        for index in range(-5, 6):
            active = index / 5.0
            candidate = 2.0 * active - active * active + 0.5 * active ** 3
            rows.append(observation(active, candidate, float(index % 2)))
        scores, explained_r2 = score_candidate(rows, "execution_pressure", "m1_cubic")
        self.assertLess(max(map(abs, scores)), 1e-10)
        self.assertAlmostEqual(float(explained_r2), 1.0)

    def test_styles_only_does_not_control_active_flow(self) -> None:
        rows = [observation(float(index), float(index), float(index % 2)) for index in range(8)]
        scores, _ = score_candidate(rows, "chain_flow", "styles_only")
        self.assertGreater(max(map(abs, scores)), 1.0)

    def test_domain_aggregate_and_all_market_share_output_schema(self) -> None:
        rows = []
        for index in range(30):
            row = observation(index / 30.0, (index % 7) / 7.0, float(index % 3))
            row.update({
                "symbol": f"SH{index:06d}",
                "domain": "d1" if index < 15 else "d2",
                "targets": {"future_net_share": (index - 15) / 15.0},
            })
            rows.append(row)
        performance, exposures = run_incremental({(20260105, 940): rows})
        self.assertTrue(performance)
        self.assertTrue(exposures)
        self.assertEqual(len({tuple(row) for row in performance}), 1)
        self.assertEqual(len({tuple(row) for row in exposures}), 1)


if __name__ == "__main__":
    unittest.main()
