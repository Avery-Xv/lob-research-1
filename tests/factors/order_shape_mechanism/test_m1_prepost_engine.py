from __future__ import annotations

import unittest

from scripts.factors.order_shape_mechanism.engine import Event
from scripts.factors.order_shape_mechanism.m1_prepost_engine import (
    M1PrePostConfig,
    M1PrePostEngine,
)


def event(
    row_id: int,
    action: str = "OTHER",
    side: str | None = None,
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
        price=None,
        volume=volume,
        bid1=None,
        ask1=None,
    )


def stat_value(rows: list[dict[str, object]], variant: str, group: str) -> tuple[int, float]:
    row = next(
        row for row in rows if row["variant"] == variant and row["group_key"] == group
    )
    return int(row["observations"]), float(row["value_sum"])


class M1PrePostEngineTest(unittest.TestCase):
    def test_equal_windows_exclude_chain_volume(self) -> None:
        engine = M1PrePostEngine(
            "SZ000001", M1PrePostConfig(lob_horizons=(2,), trade_horizons=(2,))
        )
        rows = (
            event(0, "TRADE", "B", 7, buy_order_id=1, sell_order_id=101),
            event(1),
            event(2, "TRADE", "S", 10, buy_order_id=102, sell_order_id=2),
            event(3),
            event(4, "TRADE", "B", 20, buy_order_id=7, sell_order_id=103),
            event(5, "TRADE", "B", 30, buy_order_id=7, sell_order_id=104),
            event(6),
            event(7, "TRADE", "B", 15, buy_order_id=8, sell_order_id=105),
            event(8, "TRADE", "S", 5, buy_order_id=106, sell_order_id=9),
            event(9),
        )
        for row in rows:
            engine.process(row)
        stats, quality = engine.finish()
        group = "trigger=B|chain=multi"
        self.assertEqual(stat_value(stats, "lob2_chain_pre_signed_volume", group), (1, -10.0))
        self.assertEqual(stat_value(stats, "lob2_chain_post_signed_volume", group), (1, 15.0))
        self.assertEqual(
            stat_value(stats, "lob2_chain_post_minus_pre_signed_volume", group),
            (1, 25.0),
        )
        self.assertEqual(stat_value(stats, "trade2_chain_pre_signed_volume", group), (1, -3.0))
        self.assertEqual(stat_value(stats, "trade2_chain_post_signed_volume", group), (1, 10.0))
        self.assertEqual(
            stat_value(stats, "trade2_chain_post_minus_pre_signed_volume", group),
            (1, 13.0),
        )
        self.assertEqual(quality[0]["missing_active_order_id"], 0)

    def test_missing_pre_window_is_not_labeled(self) -> None:
        engine = M1PrePostEngine(
            "SH600000", M1PrePostConfig(lob_horizons=(2,), trade_horizons=(2,))
        )
        for row in (
            event(0, "TRADE", "B", 10, buy_order_id=7, sell_order_id=70),
            event(1),
            event(2, "TRADE", "S", 5, buy_order_id=80, sell_order_id=8),
            event(3, "TRADE", "B", 5, buy_order_id=9, sell_order_id=90),
        ):
            engine.process(row)
        stats, quality = engine.finish()
        self.assertFalse(
            any(row["group_key"] == "trigger=B|chain=single" for row in stats)
        )
        self.assertGreater(quality[0]["insufficient_pre_trade_labels"], 0)

    def test_pair_does_not_cross_lunch(self) -> None:
        engine = M1PrePostEngine(
            "SH600000", M1PrePostConfig(lob_horizons=(2,), trade_horizons=(1,))
        )
        for row in (
            event(0, time=112_957_000),
            event(1, time=112_958_000),
            event(
                2,
                "TRADE",
                "B",
                10,
                buy_order_id=7,
                sell_order_id=70,
                time=112_959_000,
            ),
            event(3, time=112_959_500),
            event(4, time=130_000_000),
        ):
            engine.process(row)
        _stats, quality = engine.finish()
        self.assertGreater(quality[0]["incomplete_post_lob_labels"], 0)


if __name__ == "__main__":
    unittest.main()
