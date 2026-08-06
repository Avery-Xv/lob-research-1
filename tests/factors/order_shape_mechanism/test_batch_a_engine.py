from __future__ import annotations

import unittest

from scripts.factors.order_shape_mechanism.batch_a_engine import (
    SIGNAL_GRID_SECONDS,
    BatchAConfig,
    BatchAEngine,
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


class BatchAEngineTest(unittest.TestCase):
    def test_grid_has_21_non_crossing_signals(self) -> None:
        self.assertEqual(len(SIGNAL_GRID_SECONDS), 21)
        self.assertEqual(SIGNAL_GRID_SECONDS[0], 9 * 3600 + 40 * 60)
        self.assertEqual(SIGNAL_GRID_SECONDS[-1], 14 * 3600 + 40 * 60)

    def test_signal_uses_past_and_label_uses_future(self) -> None:
        engine = BatchAEngine(
            "SZ000001",
            BatchAConfig(target_month="202601", minimum_fill_history=1),
        )
        # First warmup day establishes an AM00 historical intensity threshold.
        for row in (
            event(20251201, 0, 93_000_000),
            event(20251201, 1, 94_000_000),
            event(20251201, 2, 112_959_000),
        ):
            engine.process(row)
        # Second warmup day creates one known-state filled order on each side.
        for row in (
            event(20251202, 0, 93_800_000),
            event(20251202, 1, 93_900_000, "ORDER_ADD", "B", 100, 10, buy_order_id=10),
            event(20251202, 2, 93_910_000, "TRADE", "S", 100, 10, buy_order_id=10, sell_order_id=20),
            event(20251202, 3, 93_920_000, "ORDER_ADD", "S", 102, 10, sell_order_id=11),
            event(20251202, 4, 93_930_000, "TRADE", "B", 102, 10, buy_order_id=21, sell_order_id=11),
            event(20251202, 5, 94_000_000),
            event(20251202, 6, 112_959_000),
        ):
            engine.process(row)
        # Target signal at 09:40 sees the 09:39 trade, not the 09:40 event.
        for row in (
            event(20260105, 0, 93_800_000),
            event(20260105, 1, 93_930_000, "TRADE", "B", 102, 10, buy_order_id=30, sell_order_id=31),
            event(20260105, 2, 94_000_000, "TRADE", "B", 102, 20, buy_order_id=32, sell_order_id=33),
            event(20260105, 3, 94_500_000, "TRADE", "S", 100, 5, buy_order_id=34, sell_order_id=35),
            event(20260105, 4, 95_000_000),
            event(20260105, 5, 112_959_000),
        ):
            engine.process(row)
        signals, quality = engine.finish()
        row = next(value for value in signals if value["date"] == 20260105 and value["signal_time"] == 940)
        self.assertEqual(row["active_buy_volume"], 10.0)
        self.assertEqual(row["future_buy_volume"], 20.0)
        self.assertEqual(row["future_sell_volume"], 5.0)
        self.assertEqual(row["future_net_flow"], 15.0)
        self.assertIsNotNone(row["pred_fill_buy"])
        self.assertIsNotNone(row["pred_fill_sell"])
        target_quality = next(value for value in quality if value["date"] == 20260105)
        self.assertGreater(target_quality["completed_target_signals"], 0)


if __name__ == "__main__":
    unittest.main()
