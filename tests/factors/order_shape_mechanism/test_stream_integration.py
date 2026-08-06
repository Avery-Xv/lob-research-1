from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import duckdb

from scripts.factors.order_shape_mechanism.engine import MechanismConfig
from scripts.factors.order_shape_mechanism.reproduce_mechanisms_v4 import process_symbol


SCHEMA = """
CREATE TABLE events(
    date INTEGER, time BIGINT, row_id BIGINT, source_action VARCHAR,
    source_recid BIGINT, source_buy_order_id BIGINT, source_sell_order_id BIGINT,
    source_side VARCHAR, source_price BIGINT, source_volume BIGINT,
    source_link_status VARCHAR, bid_px BIGINT[], bid_vol BIGINT[], bid_cnt BIGINT[],
    bid_ordvol BIGINT[], ask_px BIGINT[], ask_vol BIGINT[], ask_cnt BIGINT[],
    ask_ordvol BIGINT[]
)
"""


def row(
    date: int,
    row_id: int,
    action: str,
    side: str,
    buy_id: int,
    sell_id: int,
    recid: int,
    volume: int = 100,
) -> tuple:
    return (
        date, 93_000_000 + row_id * 1_000, row_id, action, recid,
        buy_id, sell_id, side, 10_000, volume, "FULL",
        [9_990, 9_980, 9_970], [1_000, 2_000, 3_000], [1, 2, 3], [100] * 20,
        [10_010, 10_020, 10_030], [1_100, 2_100, 3_100], [1, 2, 3], [100] * 20,
    )


def write_parquet(path: Path, rows: list[tuple]) -> None:
    connection = duckdb.connect()
    try:
        connection.execute(SCHEMA)
        connection.executemany("INSERT INTO events VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
        escaped = str(path).replace("'", "''")
        connection.execute(f"COPY events TO '{escaped}' (FORMAT PARQUET)")
    finally:
        connection.close()


class StreamIntegrationTest(unittest.TestCase):
    def test_projected_warmup_and_target_queries_feed_one_engine(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            warm = root / "warm.parquet"
            target = root / "target.parquet"
            write_parquet(warm, [
                row(20251231, 1, "TRADE", "B", 1, 11, 1),
                row(20251231, 2, "TRADE", "S", 2, 12, 2),
                row(20251231, 3, "TRADE", "B", 3, 13, 3),
                row(20251231, 4, "TRADE", "S", 4, 14, 4),
            ])
            write_parquet(target, [
                row(20260105, 1, "ORDER_ADD", "B", 100, 0, 101),
                row(20260105, 2, "TRADE", "B", 200, 300, 102),
                row(20260105, 3, "TRADE", "B", 201, 301, 103),
                row(20260105, 4, "TRADE", "S", 202, 302, 104),
            ])
            config = MechanismConfig(
                target_month="202601",
                half_lives=(5.0,), primary_half_life=5.0,
                horizons=(2,), price_windows=(1.0,), primary_price_window=1.0,
                minimum_threshold_samples=1, reservoir_size=100,
            profile_sample_stride=1,
            depth_sample_stride=1,
            )
            stats, quality, _audit, profile = process_symbol(
                "SH600000",
                {"202512": str(warm), "202601": str(target)},
                ["202512"], "202601", config, "256MB", 2,
            )
            self.assertTrue(any(item["mechanism"] == "M1" for item in stats))
            self.assertEqual(len(quality), 1)
            self.assertEqual(quality[0]["date"], 20260105)
            self.assertTrue(profile["intensity_thresholds"])


if __name__ == "__main__":
    unittest.main()
