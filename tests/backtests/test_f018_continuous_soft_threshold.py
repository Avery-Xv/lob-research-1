from scripts.backtests.analyze_f018_continuous_soft_threshold import (
    HINGE_FACTOR,
    SMOOTHSTEP_FACTOR,
    add_soft_threshold_scores,
    soft_weights,
)
from tests.backtests.test_f018_continuous_liquidity_composite import make_row


def test_soft_weights_have_frozen_boundaries_and_are_monotone() -> None:
    points = [0.0, 1.0 / 3.0, 0.5, 2.0 / 3.0, 1.0]
    weights = [soft_weights(point) for point in points]
    assert weights[0] == (0.0, 0.0)
    assert weights[1] == (0.0, 0.0)
    assert weights[-1] == (1.0, 1.0)
    assert [item[0] for item in weights] == sorted(item[0] for item in weights)
    assert [item[1] for item in weights] == sorted(item[1] for item in weights)


def test_soft_threshold_changes_magnitude_not_direction() -> None:
    rows = [make_row(index) for index in range(12)]
    add_soft_threshold_scores(rows)
    for row in rows:
        base = float(row["f018_centered_rank"])
        for factor in (HINGE_FACTOR, SMOOTHSTEP_FACTOR):
            score = float(row["candidate"][factor])  # type: ignore[index]
            assert abs(score) <= abs(base) + 1e-12
            assert score == 0.0 or base == 0.0 or (score > 0.0) == (base > 0.0)


def test_soft_threshold_does_not_read_future_targets() -> None:
    rows = [make_row(index) for index in range(12)]
    add_soft_threshold_scores(rows)
    original = {
        (str(row["symbol"]), factor): float(row["candidate"][factor])  # type: ignore[index]
        for row in rows for factor in (HINGE_FACTOR, SMOOTHSTEP_FACTOR)
    }
    for row in rows:
        row["targets"] = {"ret_1031_1035": 999999.0}
    add_soft_threshold_scores(rows)
    assert original == {
        (str(row["symbol"]), factor): float(row["candidate"][factor])  # type: ignore[index]
        for row in rows for factor in (HINGE_FACTOR, SMOOTHSTEP_FACTOR)
    }
