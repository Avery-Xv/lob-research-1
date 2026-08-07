from __future__ import annotations

import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "scripts" / "audits"))

from audit_order_remainder_semantics import summarize  # noqa: E402


def _case(symbol: str, side: str, order_id: int, classification: str, add: int, trades: int):
    return {
        "symbol": symbol, "date": 20260105, "side": side, "order_id": order_id,
        "classification": classification, "add_qty": add,
        "active_trade_qty": trades, "trade_events": int(trades > 0),
    }


def test_summary_requires_both_exchange_publication_orders_and_side_key() -> None:
    cases = [
        _case("SH600000", "B", 7, "posttrade_remainder", 40, 60),
        _case("SH600000", "S", 7, "trade_only_active", 0, 50),
        _case("SZ000001", "B", 8, "pretrade_active_add", 100, 60),
    ]
    summary = summarize(cases, [{
        "row_order_violations": 0, "missing_books": 1,
        "locked_books": 1, "crossed_books": 1,
    }])
    assert summary["status"] == "PASS"
    assert summary["directionless_active_id_collisions"] == 1


def test_all_order_identity_sql_is_side_qualified() -> None:
    order_behavior = (REPO_ROOT / "scripts/factors/order_behavior_ratio/intraday_window_factor.py").read_text()
    passive_gap = (REPO_ROOT / "scripts/factors/passive_large_gap_ratio/intraday_window_factor.py").read_text()
    joint = (REPO_ROOT / "scripts/factors/joint_large_gap_order_behavior/compute_v4.py").read_text()
    assert "a.active_side = o.source_side" in order_behavior
    assert "PARTITION BY symbol, date, source_side" in order_behavior
    assert "PARTITION BY symbol, date, source_side" in passive_gap
    assert "a.active_side = o.order_side" in joint
    assert "PARTITION BY w.window_name, e.date, e.source_side" in joint
