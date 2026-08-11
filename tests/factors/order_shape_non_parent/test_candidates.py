from __future__ import annotations

import math

from scripts.factors.order_shape_non_parent.candidates import (
    assign_states,
    base_values,
    classify_domain,
    enrich_slice,
)


def source_row(**overrides: str) -> dict[str, str]:
    row = {
        "active_buy_volume": "60", "active_sell_volume": "40",
        "active_buy_count": "6", "active_sell_count": "4",
        "pred_fill_buy": "0.2", "pred_fill_sell": "0.7",
        "near_cancel_buy": "10", "near_cancel_sell": "30",
        "bid_depth3": "100", "ask_depth3": "200",
        "book_imbalance3": "-0.3333333333333333",
        "fill_history_buy": "100", "fill_history_sell": "120",
        "spread_bps": "5",
    }
    row.update(overrides)
    return row


def test_mirror_symmetric_cancel_fields_and_signed_field() -> None:
    original = base_values(source_row())
    mirrored = base_values(source_row(
        active_buy_volume="40", active_sell_volume="60",
        pred_fill_buy="0.7", pred_fill_sell="0.2",
        near_cancel_buy="30", near_cancel_sell="10",
        bid_depth3="200", ask_depth3="100",
        book_imbalance3="0.3333333333333333",
        fill_history_buy="120", fill_history_sell="100",
    ))
    assert math.isclose(original["np05_cancel_intensity"], mirrored["np05_cancel_intensity"])
    assert math.isclose(original["np05_abs_cancel_imbalance"], mirrored["np05_abs_cancel_imbalance"])
    assert math.isclose(original["np05_signed_cancel_imbalance"], -mirrored["np05_signed_cancel_imbalance"])
    assert math.isclose(original["execution_pressure"], -mirrored["execution_pressure"])


def test_cubic_m1_control_removes_exact_polynomial() -> None:
    rows = []
    for index in range(-10, 11):
        m1 = index / 10
        rows.append({
            "m1": m1, "execution_pressure": 0.4 * m1 - 0.3 * m1 ** 2 + 0.2 * m1 ** 3,
            "np02_fillability": 0.5, "log_active_volume": 1.0,
            "log_active_count": 1.0, "log_fill_history_buy": 1.0,
            "log_fill_history_sell": 1.0, "spread_bps": 1.0,
            "log_depth3": 1.0, "book_imbalance3": m1,
        })
    enrich_slice(rows)
    assert max(abs(float(row["np01_m1_cubic"])) for row in rows) < 1e-10


def test_domain_rule_and_state_thresholds() -> None:
    assert classify_domain(400_000, 9.0, "SH600000") == "cap_lt_50yi/nonstar_lt_10"
    assert classify_domain(600_000, 11.0, "SZ000001") == "cap_50_500yi/nonstar_ge_10"
    assert classify_domain(6_000_000, 11.0, "SH688001") == "cap_ge_500yi/star_ge_10"
    assert classify_domain(6_000_000, 9.0, "SH688001") is None
    rows = [
        {"m1": -0.9 + index * 0.1, "execution_pressure": 0.9 - index * 0.08,
         "book_imbalance3": -0.8 + index * 0.08}
        for index in range(20)
    ]
    assign_states(rows)
    assert {str(row["np03_state"]) for row in rows}
    assert {str(row["np04_state"]) for row in rows}
