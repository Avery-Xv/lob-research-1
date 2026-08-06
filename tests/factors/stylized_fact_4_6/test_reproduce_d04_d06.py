from __future__ import annotations

import importlib.util
from pathlib import Path


SCRIPT = (
    Path(__file__).resolve().parents[3]
    / "scripts/factors/stylized_fact_4_6/reproduce_d04_d06.py"
)
SPEC = importlib.util.spec_from_file_location("reproduce_d04_d06", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


def test_percentile_ranks_average_ties() -> None:
    assert MODULE.percentile_ranks([1.0, 2.0, 2.0, 4.0]) == [0.0, 0.5, 0.5, 1.0]


def test_quintile_boundaries() -> None:
    assert [MODULE.quintile(value) for value in (0.0, 0.2, 0.4, 0.8, 1.0)] == [1, 2, 3, 5, 5]


def test_residualize_removes_intercept_and_exposure() -> None:
    exposures = [[1.0], [2.0], [3.0], [4.0]]
    basis = MODULE.build_orthonormal_basis(exposures)
    residual = MODULE.residualize([3.0, 5.0, 7.0, 9.0], basis)
    assert max(map(abs, residual)) < 1e-10


def test_lagged_surprise_requires_strictly_lagged_60() -> None:
    history = [float(value) for value in range(59)]
    assert MODULE.lagged_surprise(60.0, history) is None
    history.append(59.0)
    result = MODULE.lagged_surprise(60.0, history)
    assert result is not None
    assert result > 0


def test_ewma_includes_current_observation() -> None:
    history = [0.0] * 20
    assert MODULE.ewma(history + [1.0], 3) > MODULE.ewma(history + [1.0], 20)


def test_incremental_d05_matches_reference_history_scan() -> None:
    state = MODULE.D05State()
    d04_history: list[float] = []
    buy_history: list[float] = []
    sell_history: list[float] = []
    for index in range(120):
        current = ((index * 17) % 31 - 15) / 7.0
        buy = ((index * 11) % 37) / 5.0
        sell = ((index * 13) % 41) / 6.0
        result = state.update(current, buy, sell)
        expected = (
            MODULE.lagged_surprise(current, d04_history),
            MODULE.lagged_surprise(buy, buy_history),
            MODULE.lagged_surprise(sell, sell_history),
            MODULE.ewma(d04_history + [current], 3)
            - MODULE.ewma(d04_history + [current], 20),
        )
        actual = (
            result["d05_surprise_60"],
            result["d05_buy_surprise_60"],
            result["d05_sell_surprise_60"],
            result["d05_acceleration_3_20"],
        )
        for observed, reference in zip(actual, expected):
            if reference is None:
                assert observed is None
            else:
                assert observed is not None
                assert abs(float(observed) - reference) < 1e-10
        assert result["d05_history_observations"] == len(d04_history)
        d04_history.append(current)
        buy_history.append(buy)
        sell_history.append(sell)
