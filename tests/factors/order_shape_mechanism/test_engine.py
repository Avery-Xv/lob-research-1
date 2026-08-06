from __future__ import annotations

import math
import unittest

from scripts.factors.order_shape_mechanism.engine import (
    Event,
    MechanismConfig,
    MechanismEngine,
    hhmmssmmm_to_seconds,
    session_name,
)


def event(
    date: int,
    row_id: int,
    time: int,
    action: str = "ORDER_ADD",
    side: str = "B",
    volume: int = 100,
    buy_id: int | None = None,
    sell_id: int | None = None,
    recid: int | None = None,
    price: int = 9_990,
) -> Event:
    return Event(
        date=date,
        time=time,
        row_id=row_id,
        action=action,
        recid=recid,
        buy_order_id=buy_id,
        sell_order_id=sell_id,
        side=side,
        price=price,
        volume=volume,
        bid1=9_990,
        ask1=10_010,
        bid_depths=(1_000, 3_000, 10_000),
        ask_depths=(1_100, 3_300, 11_000),
    )


class MechanismEngineTest(unittest.TestCase):
    def config(self) -> MechanismConfig:
        return MechanismConfig(
            target_month="202601",
            half_lives=(5.0,),
            primary_half_life=5.0,
            horizons=(2,),
            price_windows=(1.0,),
            primary_price_window=1.0,
            minimum_threshold_samples=1,
            reservoir_size=100,
            profile_sample_stride=1,
            depth_sample_stride=1,
            rolling_trade_sizes=2,
            audit_dates=frozenset({20260105}),
        )

    def seed_warmup(self, engine: MechanismEngine) -> None:
        rows = [
            event(20251231, 1, 93_000_000, "TRADE", "B", buy_id=1, sell_id=11, recid=1),
            event(20251231, 2, 93_001_000, "TRADE", "S", buy_id=2, sell_id=12, recid=2),
            event(20251231, 3, 93_002_000, "TRADE", "B", buy_id=3, sell_id=13, recid=3),
            event(20251231, 4, 93_003_000, "TRADE", "S", buy_id=4, sell_id=14, recid=4),
            event(20251231, 5, 93_004_000, "ORDER_ADD", "B", buy_id=5),
        ]
        for row in rows:
            engine.process(row, "warmup")

    def test_clock_and_session_boundaries(self) -> None:
        self.assertEqual(hhmmssmmm_to_seconds(93_001_500), 9 * 3600 + 30 * 60 + 1.5)
        self.assertEqual(session_name(92_500_000), None)
        self.assertEqual(session_name(93_000_000), "AM")
        self.assertEqual(session_name(145_700_000), None)

    def test_trade_event_clock_only_decays_on_valid_trades(self) -> None:
        engine = MechanismEngine("SH600000", self.config())
        engine.process(
            event(20251231, 1, 93_000_000, "TRADE", "B", volume=100, recid=1),
            "warmup",
        )
        first = engine.trade_event_intensities[(5.0, "B")].value
        engine.process(
            event(20251231, 2, 93_100_000, "ORDER_ADD", "B", volume=100),
            "warmup",
        )
        self.assertEqual(engine.trade_event_intensities[(5.0, "B")].value, first)
        engine.process(
            event(20251231, 3, 93_200_000, "TRADE", "B", volume=100, recid=2),
            "warmup",
        )
        expected = first * math.exp(-math.log(2.0) / 5.0) + 100.0
        self.assertAlmostEqual(
            engine.trade_event_intensities[(5.0, "B")].value, expected
        )

    def test_future_labels_and_passive_fill_share_one_stream(self) -> None:
        engine = MechanismEngine("SH600000", self.config())
        self.seed_warmup(engine)
        target = [
            event(20260105, 1, 93_000_000, "ORDER_ADD", "B", buy_id=100),
            event(20260105, 2, 93_001_000, "ORDER_ADD", "S", sell_id=200, price=10_010),
            event(20260105, 3, 93_002_000, "TRADE", "B", 40, buy_id=300, sell_id=200, recid=300),
            event(20260105, 4, 93_003_000, "TRADE", "B", 20, buy_id=301, sell_id=201, recid=301),
            event(20260105, 5, 93_004_000, "TRADE", "S", 10, buy_id=202, sell_id=302, recid=302),
        ]
        for row in target:
            engine.process(row, "target")
        stats, quality, audit = engine.finish()

        m1 = [row for row in stats if row["mechanism"] == "M1"]
        self.assertTrue(m1)
        self.assertTrue(any(str(row["variant"]).startswith("teh20_") for row in m1))
        submitted_sell = [
            row for row in stats
            if row["mechanism"] == "M6"
            and row["variant"] == "submitted_volume"
            and "side=S" in str(row["group_key"])
        ]
        filled_sell = [
            row for row in stats
            if row["mechanism"] == "M6"
            and row["variant"] == "filled_volume_60s"
            and "side=S" in str(row["group_key"])
        ]
        self.assertEqual(sum(float(row["value_sum"]) for row in submitted_sell), 100.0)
        self.assertEqual(sum(float(row["value_sum"]) for row in filled_sell), 40.0)
        self.assertEqual(quality[0]["fill_over_submit"], 0)
        self.assertTrue(any(row["kind"] == "event" for row in audit))

    def test_active_order_add_is_excluded_at_day_end(self) -> None:
        engine = MechanismEngine("SZ000001", self.config())
        self.seed_warmup(engine)
        rows = [
            event(20260105, 1, 93_000_000, "BOOK", "N", volume=0),
            event(20260105, 2, 93_001_000, "ORDER_ADD", "S", sell_id=900, price=10_010),
            event(20260105, 3, 93_002_000, "ORDER_ADD", "B", buy_id=800, price=9_990),
            event(20260105, 4, 93_003_000, "TRADE", "B", 50, buy_id=800, sell_id=900, recid=1),
        ]
        for row in rows:
            engine.process(row, "target")
        stats, quality, _audit = engine.finish()
        submitted_buy = [
            row for row in stats
            if row["mechanism"] == "M6"
            and row["variant"] == "submitted_orders"
            and "side=B" in str(row["group_key"])
        ]
        self.assertEqual(submitted_buy, [])
        self.assertEqual(quality[0]["passive_orders"], 1)

    def test_row_order_violation_aborts(self) -> None:
        engine = MechanismEngine("SH600000", self.config())
        engine.process(event(20251231, 2, 93_000_000), "warmup")
        with self.assertRaisesRegex(ValueError, "non-increasing row_id"):
            engine.process(event(20251231, 1, 93_001_000), "warmup")


    def test_crossed_shenzhen_chain_retains_last_valid_prebook(self) -> None:
        engine = MechanismEngine("SZ000001", self.config())
        self.seed_warmup(engine)
        valid = event(20260105, 1, 93_000_000, "BOOK", "N", volume=0)
        crossed = Event(
            **{
                **event(
                    20260105, 2, 93_000_020, "ORDER_ADD", "B",
                    buy_id=100, price=10_020,
                ).__dict__,
                "bid1": 10_020,
                "ask1": 10_010,
            }
        )
        child_trade = Event(
            **{
                **event(
                    20260105, 3, 93_000_020, "TRADE", "B",
                    buy_id=100, sell_id=200, recid=3, price=10_010,
                ).__dict__,
                "bid1": 10_020,
                "ask1": 10_010,
            }
        )
        settled = event(20260105, 4, 93_000_040, "BOOK", "N", volume=0)
        for row in (valid, crossed, child_trade, settled):
            engine.process(row, "target")
        _stats, quality, audit = engine.finish()

        events = [row for row in audit if row["kind"] == "event"]
        self.assertEqual(events[1]["pre_bid1"], 9_990.0)
        self.assertEqual(events[2]["pre_bid1"], 9_990.0)
        self.assertEqual(events[3]["pre_bid1"], 9_990.0)
        self.assertEqual(quality[0]["invalid_book"], 2)


if __name__ == "__main__":
    unittest.main()
