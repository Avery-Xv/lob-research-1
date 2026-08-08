from __future__ import annotations

import json
import unittest

from scripts.factors.order_shape_mechanism.non_parent_order_state_engine import (
    PROFILE_GRID_SECONDS,
    TARGET_SIGNAL_SECONDS,
    NonParentOrderStateConfig,
    NonParentOrderStateEngine,
)
from scripts.factors.order_shape_mechanism.engine import Event


def event(
    date: int,
    row_id: int,
    time: int,
    action: str = "OTHER",
    side: str | None = None,
    price: int | None = None,
    volume: int | None = None,
    buy_order_id: int | None = None,
    sell_order_id: int | None = None,
) -> Event:
    return Event(
        date=date,
        time=time,
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


class NonParentOrderStateEngineTest(unittest.TestCase):
    def test_grid_is_local_to_fixed_1030_signal(self) -> None:
        self.assertEqual(PROFILE_GRID_SECONDS, (36000, 36600, 37200, 37800))
        self.assertEqual(TARGET_SIGNAL_SECONDS, 37800)

    def test_signal_uses_past_and_label_uses_future(self) -> None:
        engine = NonParentOrderStateEngine(
            "SZ000001",
            NonParentOrderStateConfig(target_month="202601", minimum_fill_history=1),
        )
        # First warmup day establishes local AM01/AM02 thresholds.
        for row in (
            event(20251201, 0, 95_900_000),
            event(20251201, 1, 100_000_000),
            event(20251201, 2, 104_059_000),
        ):
            engine.process(row)
        # Second warmup day creates one known-state filled order on each side.
        for row in (
            event(20251202, 0, 95_900_000),
            event(20251202, 1, 100_500_000, "ORDER_ADD", "B", 100, 10, buy_order_id=10),
            event(20251202, 2, 100_510_000, "TRADE", "S", 100, 10, buy_order_id=10, sell_order_id=20),
            event(20251202, 3, 100_520_000, "ORDER_ADD", "S", 102, 10, sell_order_id=11),
            event(20251202, 4, 100_530_000, "TRADE", "B", 102, 10, buy_order_id=21, sell_order_id=11),
            event(20251202, 5, 103_000_000),
            event(20251202, 6, 104_059_000),
        ):
            engine.process(row)
        # Target signal at 10:30 sees the 10:29 trade, not the 10:30 event.
        for row in (
            event(20260105, 0, 95_900_000),
            event(20260105, 1, 102_930_000, "TRADE", "B", 102, 10, buy_order_id=30, sell_order_id=31),
            event(20260105, 2, 103_000_000, "TRADE", "B", 102, 20, buy_order_id=32, sell_order_id=33),
            event(20260105, 3, 103_500_000, "TRADE", "S", 100, 5, buy_order_id=34, sell_order_id=35),
            event(20260105, 4, 104_000_000),
            event(20260105, 5, 104_059_000),
        ):
            engine.process(row)
        signals, quality = engine.finish()
        row = next(value for value in signals if value["date"] == 20260105 and value["signal_time"] == 1030)
        self.assertEqual(row["active_buy_volume"], 10.0)
        self.assertEqual(row["future_buy_volume"], 20.0)
        self.assertEqual(row["future_sell_volume"], 5.0)
        self.assertEqual(row["future_net_flow"], 15.0)
        self.assertIsNotNone(row["pred_fill_buy"])
        self.assertIsNotNone(row["pred_fill_sell"])
        target_quality = next(value for value in quality if value["date"] == 20260105)
        self.assertGreater(target_quality["completed_target_signals"], 0)

    def test_history_snapshot_matches_continuous_processing(self) -> None:
        config = NonParentOrderStateConfig(
            target_month="202601", minimum_fill_history=1,
        )
        history = [
            event(20251201, 0, 95_900_000),
            event(20251201, 1, 100_000_000),
            event(20251201, 2, 104_059_000),
            event(20251202, 0, 95_900_000),
            event(20251202, 1, 100_500_000, "ORDER_ADD", "B", 100, 10, buy_order_id=10),
            event(20251202, 2, 100_510_000, "TRADE", "S", 100, 10, buy_order_id=10, sell_order_id=20),
            event(20251202, 3, 100_520_000, "ORDER_ADD", "S", 102, 10, sell_order_id=11),
            event(20251202, 4, 100_530_000, "TRADE", "B", 102, 10, buy_order_id=21, sell_order_id=11),
            event(20251202, 5, 103_000_000),
            event(20251202, 6, 104_059_000),
        ]
        target = [
            event(20260105, 0, 95_900_000),
            event(20260105, 1, 102_930_000, "TRADE", "B", 102, 10, buy_order_id=30, sell_order_id=31),
            event(20260105, 2, 103_000_000, "TRADE", "B", 102, 20, buy_order_id=32, sell_order_id=33),
            event(20260105, 3, 103_500_000, "TRADE", "S", 100, 5, buy_order_id=34, sell_order_id=35),
            event(20260105, 4, 104_000_000),
            event(20260105, 5, 104_059_000),
        ]
        continuous = NonParentOrderStateEngine("SZ000001", config)
        for row in history + target:
            continuous.process(row)
        continuous_signals, continuous_quality = continuous.finish()

        history_engine = NonParentOrderStateEngine("SZ000001", config)
        for row in history:
            history_engine.process(row)
        snapshot = json.loads(json.dumps(history_engine.export_history_snapshot()))
        target_engine = NonParentOrderStateEngine("SZ000001", config)
        target_engine.load_history_snapshot(snapshot)
        for row in target:
            target_engine.process(row)
        snapshot_signals, snapshot_quality = target_engine.finish()

        self.assertEqual(snapshot_signals, continuous_signals)
        expected_target_quality = [
            row for row in continuous_quality if row["date"] // 100 == 202601
        ]
        self.assertEqual(snapshot_quality, expected_target_quality)

    def test_orders_at_or_after_1040_are_not_right_censored_into_model(self) -> None:
        engine = NonParentOrderStateEngine("SZ000001")
        for row in (
            event(20251201, 0, 95_900_000),
            event(20251201, 1, 103_930_000, "ORDER_ADD", "B", 100, 10, buy_order_id=1),
            event(20251201, 2, 104_030_000, "ORDER_ADD", "B", 100, 10, buy_order_id=2),
            event(20251201, 3, 104_059_000),
        ):
            engine.process(row)
        _signals, quality = engine.finish()
        self.assertEqual(quality[0]["candidate_passive_orders"], 1)

    def test_shanghai_active_remainder_is_excluded_from_quote_signal(self) -> None:
        engine = NonParentOrderStateEngine(
            "SH600000",
            NonParentOrderStateConfig(target_month="202601", minimum_fill_history=1),
        )
        for row in (
            event(20260105, 0, 95_900_000),
            event(
                20260105,
                1,
                102_900_000,
                "TRADE",
                "B",
                102,
                10,
                buy_order_id=7,
                sell_order_id=70,
            ),
            event(
                20260105,
                2,
                102_910_000,
                "ORDER_ADD",
                "B",
                101,
                30,
                buy_order_id=7,
            ),
            event(20260105, 3, 103_000_000),
            event(20260105, 4, 104_000_000),
            event(20260105, 5, 104_059_000),
        ):
            engine.process(row)
        rolling = engine._rolling_values()
        self.assertEqual(rolling["aggressive_add_buy"], 0.0)
        self.assertEqual(rolling["aggressive_add_sell"], 0.0)
        _signals, quality = engine.finish()
        target_quality = next(row for row in quality if row["date"] == 20260105)
        self.assertEqual(target_quality["quote_active_remainders_excluded"], 1)


if __name__ == "__main__":
    unittest.main()
