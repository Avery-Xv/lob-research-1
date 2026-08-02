from __future__ import annotations

import pytest

from scripts.backtests.backtest_order_behavior_intraday import decile_means, neutralize, ranks


def test_ranks_average_ties() -> None:
    assert ranks([3.0, 1.0, 1.0, 2.0]) == [4.0, 1.5, 1.5, 3.0]


def test_decile_means_orders_low_to_high_factor() -> None:
    factors = [float(value) for value in range(20, 0, -1)]
    targets = [value / 100.0 for value in factors]
    symbols = [f"SH{value:06d}" for value in range(20)]

    buckets = decile_means(factors, targets, symbols)

    assert len(buckets) == 10
    assert buckets[0] == pytest.approx(0.015)
    assert buckets[-1] == pytest.approx(0.195)


def test_neutralize_removes_linear_exposure() -> None:
    exposure = [[1.0], [2.0], [3.0], [4.0]]
    residual = neutralize([3.0, 5.0, 7.0, 9.0], exposure)
    assert residual == pytest.approx([0.0, 0.0, 0.0, 0.0], abs=1e-12)
