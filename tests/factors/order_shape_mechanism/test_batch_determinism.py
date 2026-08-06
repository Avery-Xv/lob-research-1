from __future__ import annotations

import tempfile
import unittest
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from multiprocessing import get_context

from scripts.factors.order_shape_mechanism.engine import MechanismConfig
from scripts.factors.order_shape_mechanism.reproduce_mechanisms_v4 import (
    compute_batch_worker,
)
from tests.factors.order_shape_mechanism.test_stream_integration import row, write_parquet


class BatchDeterminismTest(unittest.TestCase):
    def test_serial_and_process_worker_outputs_match(self) -> None:
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
            inputs = {
                "SH600000": {"202512": str(warm), "202601": str(target)}
            }
            config = MechanismConfig(
                target_month="202601", half_lives=(5.0,),
                primary_half_life=5.0, horizons=(2,), price_windows=(1.0,),
                primary_price_window=1.0, minimum_threshold_samples=1,
                reservoir_size=100,
            profile_sample_stride=1,
            depth_sample_stride=1,
            )
            serial_root = root / "serial"
            process_root = root / "process"
            compute_batch_worker(
                1, ["SH600000"], inputs, ["202512"], "202601", config,
                "256MB", 2, str(serial_root),
            )
            with ProcessPoolExecutor(max_workers=1, mp_context=get_context("spawn")) as executor:
                future = executor.submit(
                    compute_batch_worker,
                    1, ["SH600000"], inputs, ["202512"], "202601", config,
                    "256MB", 2, str(process_root),
                )
                future.result()
            for name in ("stats.csv", "quality.csv", "profiles.jsonl", "done.json"):
                self.assertEqual(
                    (serial_root / "batch_000001" / name).read_bytes(),
                    (process_root / "batch_000001" / name).read_bytes(),
                )


if __name__ == "__main__":
    unittest.main()
