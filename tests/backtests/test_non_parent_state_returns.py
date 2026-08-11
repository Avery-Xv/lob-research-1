from scripts.backtests.analyze_non_parent_state_returns import sign, tercile


def test_sign() -> None:
    assert sign(2.0) == 1.0
    assert sign(-2.0) == -1.0
    assert sign(0.0) == 0.0


def test_tercile_boundaries_are_frozen_and_inclusive() -> None:
    assert tercile(-1.0, -1.0, 1.0) == "low"
    assert tercile(0.0, -1.0, 1.0) == "mid"
    assert tercile(1.0, -1.0, 1.0) == "high"
