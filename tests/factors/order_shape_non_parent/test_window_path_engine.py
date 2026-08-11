from __future__ import annotations

import tempfile
import unittest
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import get_context
from pathlib import Path

from scripts.factors.order_shape_mechanism.engine import Event
from scripts.factors.order_shape_non_parent.compute_window_path_v4 import compute_batch_worker
from scripts.factors.order_shape_non_parent.window_path_engine import WindowPathEngine
from tests.factors.order_shape_mechanism.test_stream_integration import write_parquet


def event(row_id: int, time: int, *, date: int = 20260105, action: str = "OTHER",
          side: str | None = None, buy_id: int | None = None,
          sell_id: int | None = None, volume: int = 0,
          bid: int | None = 100, ask: int | None = 102,
          bid_depth: tuple[int | None, int | None, int | None] = (100, 300, 1000),
          ask_depth: tuple[int | None, int | None, int | None] = (100, 300, 1000)) -> Event:
    return Event(
        date=date, time=time, row_id=row_id, action=action,
        recid=row_id if action == "TRADE" else None,
        buy_order_id=buy_id, sell_order_id=sell_id, side=side,
        price=101, volume=volume, bid1=bid, ask1=ask,
        bid_depths=bid_depth, ask_depths=ask_depth,
    )


def parquet_row(row_id: int, time: int, action: str = "OTHER", side: str = "B") -> tuple:
    return (
        20260105, time, row_id, action, row_id, 100 + row_id, 200 + row_id,
        side, 101, 10 if action == "TRADE" else 0, "FULL",
        [100, 99, 98], [100] * 10, [1] * 10, [1] * 10,
        [102, 103, 104], [100] * 10, [1] * 10, [1] * 10,
    )


class WindowPathEngineTest(unittest.TestCase):
    def test_duration_weighting_and_exact_flow_boundaries(self) -> None:
        engine = WindowPathEngine("SH600000")
        for row in (
            event(1, 95_900_000),
            event(2, 100_000_000, bid_depth=(100, 600, 1000), ask_depth=(100, 200, 1000)),
            event(3, 102_500_000, bid_depth=(100, 200, 1000), ask_depth=(100, 600, 1000)),
            event(4, 102_600_000, action="TRADE", side="B", buy_id=7, sell_id=70, volume=30,
                  bid_depth=(100, 200, 1000), ask_depth=(100, 600, 1000)),
            event(5, 102_900_000, action="TRADE", side="S", buy_id=8, sell_id=80, volume=10,
                  bid_depth=(100, 200, 1000), ask_depth=(100, 600, 1000)),
            event(6, 103_000_000, action="TRADE", side="B", buy_id=9, sell_id=90, volume=99,
                  bid_depth=(100, 200, 1000), ask_depth=(100, 600, 1000)),
            event(7, 103_100_000), event(8, 103_500_000), event(9, 104_000_000),
        ):
            engine.process(row)
        result = engine.finish()[0][0]
        self.assertAlmostEqual(result["book30m_bi3_twap"], 0.5 * 25 / 30 - 0.5 * 5 / 30)
        self.assertAlmostEqual(result["book5m_bi3_twap"], -0.5)
        self.assertEqual(result["flow5m_buy_volume"], 30.0)
        self.assertEqual(result["flow5m_sell_volume"], 10.0)
        self.assertEqual(result["flow1m_buy_volume"], 0.0)
        self.assertEqual(result["flow1m_sell_volume"], 10.0)
        self.assertEqual(result["future1m_buy_volume"], 99.0)

    def test_invalid_chain_carries_valid_book_and_resets_by_date(self) -> None:
        engine = WindowPathEngine("SZ000001")
        for row in (
            event(1, 95_900_000, bid_depth=(100, 600, 1000), ask_depth=(100, 200, 1000)),
            event(2, 101_000_000, bid=101, ask=101),
            event(3, 101_100_000, bid=103, ask=102),
            event(4, 101_200_000, bid=None, ask=None, bid_depth=(None, None, None), ask_depth=(None, None, None)),
            event(5, 101_500_000, bid_depth=(100, 200, 1000), ask_depth=(100, 600, 1000)),
            event(6, 103_000_000), event(7, 104_000_000),
            event(1, 95_900_000, date=20260106, bid_depth=(100, 200, 1000), ask_depth=(100, 600, 1000)),
            event(2, 103_000_000, date=20260106), event(3, 104_000_000, date=20260106),
        ):
            engine.process(row)
        signals, quality = engine.finish()
        first = next(row for row in signals if row["date"] == 20260105)
        self.assertAlmostEqual(first["book30m_bi3_twap"], 0.5 * 15 / 30 - 0.5 * 15 / 30)
        first_quality = next(row for row in quality if row["date"] == 20260105)
        self.assertEqual(first_quality["locked_book_rows"], 1)
        self.assertEqual(first_quality["crossed_book_rows"], 1)
        self.assertEqual(first_quality["missing_book_rows"], 1)
        self.assertEqual(first_quality["invalid_chain_seconds"], 300.0)
        second = next(row for row in signals if row["date"] == 20260106)
        self.assertAlmostEqual(second["book30m_bi3_twap"], -0.5)

    def test_exchange_publication_orders_count_one_active_order(self) -> None:
        for symbol, sequence in (
            ("SH600000", (("TRADE", 10), ("TRADE", 5), ("ORDER_ADD", 30))),
            ("SZ000001", (("ORDER_ADD", 45), ("TRADE", 10), ("TRADE", 5))),
        ):
            engine = WindowPathEngine(symbol)
            engine.process(event(1, 95_900_000))
            for index, (action, volume) in enumerate(sequence, start=2):
                engine.process(event(index, 102_600_000 + index * 1000, action=action,
                                     side="B", buy_id=7, sell_id=70 + index, volume=volume))
            engine.process(event(5, 103_000_000)); engine.process(event(6, 104_000_000))
            result = engine.finish()[0][0]
            self.assertEqual(result["flow5m_buy_volume"], 15.0)
            self.assertEqual(result["flow5m_buy_order_count"], 1)

    def test_serial_and_spawned_worker_outputs_are_byte_identical(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); parquet = root / "sample.parquet"
            write_parquet(parquet, [
                parquet_row(1, 95_900_000), parquet_row(2, 100_000_000),
                parquet_row(3, 102_600_000, "TRADE", "B"),
                parquet_row(4, 103_000_000, "TRADE", "S"),
                parquet_row(5, 104_000_000),
            ])
            inputs = {"SH600000": {"202601": str(parquet)}}
            serial = root / "serial"; parallel = root / "parallel"
            compute_batch_worker(1, ["SH600000"], inputs, "202601", "256MB", 2, str(serial))
            with ProcessPoolExecutor(max_workers=1, mp_context=get_context("spawn")) as executor:
                executor.submit(compute_batch_worker, 1, ["SH600000"], inputs,
                                "202601", "256MB", 2, str(parallel)).result()
            for name in ("window_paths.csv", "quality.csv", "done.json"):
                self.assertEqual((serial / "batch_000001" / name).read_bytes(),
                                 (parallel / "batch_000001" / name).read_bytes())


if __name__ == "__main__":
    unittest.main()
