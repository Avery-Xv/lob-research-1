from scripts.backtests.analyze_f018_raw_15m_horizon import (
    TARGET,
    attach_labels,
    f018_value,
)


def test_f018_direction_and_opponent_depth() -> None:
    assert f018_value(0.2, 100.0, 20.0, 10.0, 25.0) < 0.0
    assert f018_value(-0.2, 100.0, 20.0, 10.0, 25.0) > 0.0
    assert f018_value(0.0, 100.0, 20.0, 10.0, 25.0) == 0.0


def test_15m_label_uses_only_1031_and_1045() -> None:
    row = {
        "symbol": "SH600000", "date": 20260105, "signal_time": 1030,
        "exchange": "SH", "domain": "cap_50_500yi/nonstar_ge_10",
        "candidate": {"f018_raw": 1.0}, "targets": {},
    }
    evaluated, cache = attach_labels([row], {("SH600000", 20260105): (10.0, 10.1)})
    assert len(evaluated) == 1
    assert abs(float(evaluated[0]["targets"][TARGET]) - 0.01) < 1e-12  # type: ignore[index]
    assert cache[0]["close_1031"] == 10.0
    assert cache[0]["close_1045"] == 10.1


def test_missing_1045_label_is_handled_independently() -> None:
    row = {
        "symbol": "SZ000001", "date": 20260105, "signal_time": 1030,
        "exchange": "SZ", "domain": "cap_ge_500yi/nonstar_ge_10",
        "candidate": {"f018_raw": -1.0}, "targets": {},
    }
    evaluated, cache = attach_labels([row], {})
    assert evaluated == []
    assert cache == []
