from scripts.backtests.analyze_f018_liquidity_conditioning import (
    assign_liquidity_states,
    summarize_state_diagnostics,
)


def make_row(index: int) -> dict[str, object]:
    value = float(index + 1)
    return {
        "symbol": f"SH{600000 + index}",
        "date": 20260105,
        "domain": "cap_50_500yi/nonstar_ge_10",
        "f018": value / 10.0,
        "controls": {
            "flow5": value / 100.0,
            "log_spread_5m_twap": 20.0 - value,
            "log_depth3_5m_twap": value,
            "log_active_volume_5m": value,
            "log_active_count_5m": value,
        },
        "targets": {"ret_1031_1035": 999.0 - value},
    }


def test_liquidity_states_are_equal_count_and_ordered() -> None:
    rows = [make_row(index) for index in range(12)]
    diagnostics = assign_liquidity_states(rows)
    counts = {
        state: sum(row["liquidity_state"] == state for row in rows)
        for state in ("low", "mid", "high")
    }
    assert counts == {"low": 4, "mid": 4, "high": 4}
    scores = {
        state: [float(row["liquidity_score"]) for row in rows if row["liquidity_state"] == state]
        for state in counts
    }
    assert max(scores["low"]) < min(scores["mid"])
    assert max(scores["mid"]) < min(scores["high"])
    assert len(diagnostics) == 3


def test_state_assignment_does_not_read_future_targets() -> None:
    rows = [make_row(index) for index in range(12)]
    assign_liquidity_states(rows)
    original = {str(row["symbol"]): str(row["liquidity_state"]) for row in rows}
    for row in rows:
        row["targets"] = {"ret_1031_1035": -float(row["targets"]["ret_1031_1035"])}  # type: ignore[index]
        row.pop("liquidity_state")
    assign_liquidity_states(rows)
    assert original == {str(row["symbol"]): str(row["liquidity_state"]) for row in rows}


def test_state_summary_preserves_domain_and_coverage() -> None:
    rows = [make_row(index) for index in range(12)]
    details = assign_liquidity_states(rows)
    summary = summarize_state_diagnostics(details)
    assert len(summary) == 3
    assert all(abs(float(row["coverage"]) - 1.0 / 3.0) < 1e-12 for row in summary)
    assert sum(int(row["n_obs"]) for row in summary) == 12
