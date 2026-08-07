from __future__ import annotations

import unittest

from scripts.factors.experiment_batch_1.engine import BatchEngine, Event


def event(
    row_id: int,
    time: int,
    action: str,
    side: str | None,
    price: int | None,
    volume: int | None,
    bid1: int,
    ask1: int,
    buy_id: int | None = None,
    sell_id: int | None = None,
) -> Event:
    return Event(
        date=20260105,
        time=time,
        row_id=row_id,
        action=action,
        recid=row_id,
        buy_order_id=buy_id,
        sell_order_id=sell_id,
        side=side,
        price=price,
        volume=volume,
        bid1=bid1,
        ask1=ask1,
        bid_depth1=1000,
        bid_depth3=3000,
        ask_depth1=1200,
        ask_depth3=3500,
        bid_count1=5,
        ask_count1=6,
    )


class ExperimentBatchEngineTest(unittest.TestCase):
    def test_chain_impact_and_quote_lifecycle(self) -> None:
        engine = BatchEngine("SH600000")
        rows = [
            event(1, 100000000, "ORDER_ADD", "B", 10000, 1000, 10000, 10010),
            event(2, 100001000, "TRADE", "B", 10010, 200, 10000, 10020, buy_id=11, sell_id=21),
            event(3, 100002000, "TRADE", "B", 10020, 300, 10000, 10030, buy_id=11, sell_id=22),
            event(4, 100008000, "ORDER_ADD", "S", 10020, 100, 10000, 10020),
            event(5, 100009000, "TRADE", "B", 10020, 100, 10000, 10030, buy_id=12, sell_id=23),
            event(6, 100040000, "ORDER_ADD", "B", 10000, 100, 10000, 10010),
        ]
        for row in rows:
            engine.process(row)
        signals, chains, quotes, quality = engine.finish()
        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0]["active_buy_count"], 3)
        self.assertEqual(signals[0]["multi_trade_chain_count"], 1)
        self.assertEqual(len(chains), 2)
        self.assertEqual(chains[0]["trade_count"], 2)
        self.assertEqual(len(quotes), 1)
        self.assertEqual(quotes[0]["side"], "S")
        self.assertEqual(quotes[0]["rehit"], 1)
        self.assertEqual(quality[0]["missing_active_order_id"], 0)
        self.assertGreater(signals[0]["impact_observations_5s"], 0)

    def test_rejects_non_increasing_row_id(self) -> None:
        engine = BatchEngine("SH600000")
        engine.process(event(2, 100000000, "ORDER_ADD", "B", 10000, 1, 10000, 10010))
        with self.assertRaises(ValueError):
            engine.process(event(2, 100001000, "ORDER_ADD", "B", 10000, 1, 10000, 10010))

    def test_crossed_shenzhen_chain_uses_last_valid_prebook(self) -> None:
        engine = BatchEngine("SZ000001")
        rows = [
            event(1, 100000000, "BOOK", None, None, 0, 10000, 10010),
            event(2, 100001000, "ORDER_ADD", "B", 10020, 1000, 10020, 10010, buy_id=50),
            event(3, 100001000, "TRADE", "B", 10010, 200, 10020, 10010, buy_id=50, sell_id=60),
            event(4, 100001000, "TRADE", "B", 10010, 300, 10020, 10020, buy_id=50, sell_id=61),
            event(5, 100001000, "TRADE", "B", 10020, 100, 10010, 10030, buy_id=50, sell_id=62),
            event(6, 100007000, "BOOK", None, None, 0, 10010, 10030),
        ]
        for row in rows:
            engine.process(row)
        signals, chains, quotes, quality = engine.finish()
        self.assertEqual(signals[0]["active_buy_count"], 3)
        self.assertEqual(signals[0]["passive_improve_buy_count"], 0)
        self.assertEqual(signals[0]["impact_observations_5s"], 1)
        self.assertEqual(len(chains), 1)
        self.assertEqual(chains[0]["trade_count"], 3)
        self.assertGreater(chains[0]["directional_impact_bps_sum"], 0)
        self.assertEqual(quotes, [])
        self.assertEqual(quality[0]["crossed_books"], 2)
        self.assertEqual(quality[0]["locked_books"], 1)
        self.assertEqual(quality[0]["atomic_book_chains"], 1)
        self.assertEqual(quality[0]["atomic_impact_events"], 1)
        self.assertEqual(quality[0]["atomic_ambiguous_chains"], 0)

    def test_invalid_deadline_row_does_not_drop_pending_impact(self) -> None:
        engine = BatchEngine("SZ000001")
        rows = [
            event(1, 100000000, "BOOK", None, None, 0, 10000, 10010),
            event(2, 100001000, "TRADE", "B", 10010, 100, 10000, 10020, buy_id=1, sell_id=2),
            event(3, 100007000, "ORDER_ADD", "B", 10030, 100, 10030, 10020, buy_id=3),
            event(4, 100008000, "BOOK", None, None, 0, 10010, 10020),
        ]
        for row in rows:
            engine.process(row)
        signals, _chains, _quotes, quality = engine.finish()
        self.assertEqual(signals[0]["impact_observations_5s"], 1)
        self.assertEqual(quality[0]["atomic_ambiguous_chains"], 1)

    def test_shanghai_trade_before_remainder_is_not_passive_quote(self) -> None:
        engine = BatchEngine("SH600004")
        rows = [
            event(1, 100000000, "BOOK", None, None, 0, 10000, 10020),
            event(2, 100001000, "TRADE", "S", 10000, 300, 9990, 10020, buy_id=10, sell_id=50),
            event(3, 100001000, "TRADE", "S", 9990, 200, 9980, 10020, buy_id=11, sell_id=50),
            event(4, 100001000, "ORDER_ADD", "S", 10000, 700, 9980, 10000, sell_id=50),
        ]
        for row in rows:
            engine.process(row)
        signals, chains, quotes, quality = engine.finish()
        self.assertEqual(len(chains), 1)
        self.assertEqual(chains[0]["trade_count"], 2)
        self.assertEqual(signals[0]["passive_improve_sell_count"], 0)
        self.assertEqual(quotes, [])
        self.assertEqual(quality[0]["atomic_book_chains"], 0)

    def test_day_boundary_resets_unresolved_atomic_chain(self) -> None:
        engine = BatchEngine("SZ000001")
        first_day = [
            event(1, 100000000, "BOOK", None, None, 0, 10000, 10010),
            event(2, 100001000, "ORDER_ADD", "B", 10020, 100, 10020, 10010, buy_id=1),
        ]
        second_day = [
            Event(**{
                **event(1, 100000000, "BOOK", None, None, 0, 10000, 10010).__dict__,
                "date": 20260106,
            }),
            Event(**{
                **event(
                    2, 100001000, "TRADE", "B", 10010, 100,
                    10000, 10020, buy_id=2, sell_id=3,
                ).__dict__,
                "date": 20260106,
            }),
        ]
        for row in first_day + second_day:
            engine.process(row)
        _signals, _chains, _quotes, quality = engine.finish()
        self.assertEqual(len(quality), 2)
        self.assertEqual(quality[0]["unresolved_atomic_chains"], 1)
        self.assertEqual(quality[1]["atomic_book_chains"], 0)


if __name__ == "__main__":
    unittest.main()
