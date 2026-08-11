from scripts.backtests.analyze_f018_continuous_liquidity_composite import (
    PRIMARY_COMPOSITE,
    add_continuous_composites,
)


def make_row(index: int) -> dict[str, object]:
    value = float(index + 1)
    return {
        "symbol": f"SH{600000 + index}",
        "date": 20260105,
        "domain": "cap_50_500yi/nonstar_ge_10",
        "f018": value,
        "controls": {
            "flow5": value / 100.0,
            "log_spread_5m_twap": 20.0 - value,
            "log_depth3_5m_twap": value,
            "log_active_volume_5m": value,
            "log_active_count_5m": value,
        },
        "targets": {"ret_1031_1035": 1000.0 - value},
    }


def test_primary_formula_is_continuous_multiplicative_weight() -> None:
    rows = [make_row(index) for index in range(12)]
    add_continuous_composites(rows)
    for row in rows:
        base = float(row["f018_centered_rank"])
        liquidity = float(row["liquidity_quality_rank"])
        score = float(row["candidate"][PRIMARY_COMPOSITE])  # type: ignore[index]
        assert abs(score - base * liquidity) < 1e-12
        assert 0.0 <= liquidity <= 1.0
    assert len({float(row["candidate"][PRIMARY_COMPOSITE]) for row in rows}) > 6  # type: ignore[index]


def test_liquidity_changes_magnitude_not_direction() -> None:
    rows = [make_row(index) for index in range(12)]
    add_continuous_composites(rows)
    for row in rows:
        base = float(row["f018_centered_rank"])
        score = float(row["candidate"][PRIMARY_COMPOSITE])  # type: ignore[index]
        assert score == 0.0 or base == 0.0 or (score > 0.0) == (base > 0.0)
        assert abs(score) <= abs(base) + 1e-12


def test_composite_does_not_read_future_targets() -> None:
    rows = [make_row(index) for index in range(12)]
    add_continuous_composites(rows)
    original = {
        str(row["symbol"]): float(row["candidate"][PRIMARY_COMPOSITE])  # type: ignore[index]
        for row in rows
    }
    for row in rows:
        row["targets"] = {"ret_1031_1035": -float(row["targets"]["ret_1031_1035"])}  # type: ignore[index]
    add_continuous_composites(rows)
    assert original == {
        str(row["symbol"]): float(row["candidate"][PRIMARY_COMPOSITE])  # type: ignore[index]
        for row in rows
    }
