from __future__ import annotations

import unittest

from scripts.factors.order_shape_mechanism.engine import Event
from scripts.factors.order_shape_mechanism.m1_quote_engine import (
    M1QuoteConfig,
    M1QuoteEngine,
)


def event(
    row_id: int,
    action: str = "OTHER",
    side: str | None = None,
    price: int | None = None,
    volume: int | None = None,
    buy_order_id: int | None = None,
    sell_order_id: int | None = None,
    time: int | None = None,
) -> Event:
    return Event(
        date=20260105,
        time=time if time is not None else 93_000_000 + row_id,
        row_id=row_id,
        action=action,
        recid=row_id if action == "TRADE" else None,
        buy_order_id=buy_order_id,
        sell_order_id=sell_order_id,
        side=side,
        price=price,
        volume=volume,
        bid1=100,
        ask1=102,
        bid_depths=(100, 300, 1_000),
        ask_depths=(120, 360, 1_200),
    )


def stat_value(
    rows: list[dict[str, object]], variant: str, group: str
) -> tuple[int, float]:
    row = next(
        row
        for row in rows
        if row["variant"] == variant and row["group_key"] == group
    )
    return int(row["observations"]), float(row["value_sum"])


class M1QuoteEngineTest(unittest.TestCase):
    def test_consecutive_child_trades_collapse_to_one_chain(self) -> None:
        engine = M1QuoteEngine(
            "SZ000001", M1QuoteConfig(lob_horizons=(2,), trade_horizons=(1,))
        )
        rows = [
            event(0),
            event(1, "TRADE", "B", 102, 10, buy_order_id=7, sell_order_id=70),
            event(2, "TRADE", "B", 102, 20, buy_order_id=7, sell_order_id=71),
            event(3, "ORDER_ADD", "B", 101, 30, buy_order_id=8),
            event(4, "ORDER_ADD", "S", 101, 40, sell_order_id=9),
            event(5),
            event(6, "TRADE", "S", 100, 5, buy_order_id=80, sell_order_id=10),
        ]
        for row in rows:
            engine.process(row)
        stats, quality = engine.finish()
        self.assertEqual(quality[0]["raw_trade_triggers"], 3)
        self.assertEqual(quality[0]["multi_trade_chains"], 1)
        observations, value = stat_value(
            stats, "lob2_chain_future_signed_volume", "trigger=B|chain=multi"
        )
        self.assertEqual(observations, 1)
        self.assertEqual(value, 0.0)

    def test_quote_state_uses_passive_best_or_improving_adds(self) -> None:
        engine = M1QuoteEngine(
            "SZ000001", M1QuoteConfig(lob_horizons=(2,), trade_horizons=(1,))
        )
        for row in (
            event(0),
            event(1, "TRADE", "B", 102, 10, buy_order_id=7, sell_order_id=70),
            event(2, "ORDER_ADD", "B", 101, 30, buy_order_id=8),
            event(3, "ORDER_ADD", "S", 101, 40, sell_order_id=9),
            event(4),
        ):
            engine.process(row)
        stats, _quality = engine.finish()
        observations, _value = stat_value(
            stats,
            "lob2_chain_future_signed_by_quote_state",
            "trigger=B|chase=1|replenish=1|chain=all",
        )
        self.assertEqual(observations, 1)

    def test_marketable_order_add_is_not_counted_as_passive_chase(self) -> None:
        engine = M1QuoteEngine(
            "SH600000", M1QuoteConfig(lob_horizons=(1,), trade_horizons=(1,))
        )
        for row in (
            event(0),
            event(1, "TRADE", "B", 102, 10, buy_order_id=7, sell_order_id=70),
            event(2, "ORDER_ADD", "B", 102, 30, buy_order_id=8),
            event(3),
        ):
            engine.process(row)
        stats, quality = engine.finish()
        self.assertEqual(quality[0]["marketable_adds_excluded"], 1)
        observations, _value = stat_value(
            stats,
            "lob1_chain_future_signed_by_quote_state",
            "trigger=B|chase=0|replenish=0|chain=all",
        )
        self.assertEqual(observations, 1)

    def test_shanghai_active_remainder_is_not_counted_as_quote_add(self) -> None:
        engine = M1QuoteEngine(
            "SH600000", M1QuoteConfig(lob_horizons=(1,), trade_horizons=(1,))
        )
        for row in (
            event(0),
            event(1, "TRADE", "B", 102, 10, buy_order_id=7, sell_order_id=70),
            event(2, "ORDER_ADD", "B", 101, 30, buy_order_id=7),
            event(3),
        ):
            engine.process(row)
        stats, quality = engine.finish()
        self.assertEqual(quality[0]["active_remainders_excluded"], 1)
        self.assertEqual(quality[0]["passive_adds"], 0)
        observations, _value = stat_value(
            stats,
            "lob1_chain_future_signed_by_quote_state",
            "trigger=B|chase=0|replenish=0|chain=all",
        )
        self.assertEqual(observations, 1)

    def test_future_labels_do_not_cross_lunch(self) -> None:
        engine = M1QuoteEngine(
            "SH600000", M1QuoteConfig(lob_horizons=(2,), trade_horizons=(2,))
        )
        engine.process(event(0, time=112_959_000))
        engine.process(
            event(
                1,
                "TRADE",
                "B",
                102,
                10,
                buy_order_id=7,
                sell_order_id=70,
                time=112_959_500,
            )
        )
        engine.process(event(2, time=130_000_000))
        _stats, quality = engine.finish()
        self.assertGreater(quality[0]["incomplete_raw_labels"], 0)
        self.assertGreater(quality[0]["terminal_chains"], 0)


if __name__ == "__main__":
    unittest.main()
