from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from scripts.factors.stylized_fact_4_6.reproduce_d01_d03 import (
    PRIMITIVE_FIELDS,
    build_run_manifest,
    historical_order_statistics,
    prepare_run_manifest,
    read_tuple_rows,
    validate_shard,
    write_tuple_rows,
)


class HistoricalOrderStatisticsTest(unittest.TestCase):
    def test_current_observation_is_excluded_from_history(self) -> None:
        rows = [
            {
                "symbol": "SH600000",
                "date": date,
                "window_name": "daily_1000_close",
                "order_impact_over_normalizer": value,
            }
            for date, value in (
                (20260105, 1.0),
                (20260106, 2.0),
                (20260107, 3.0),
            )
        ]
        stats = historical_order_statistics(rows, lookback_days=2, min_history=2)
        self.assertEqual(
            stats[("SH600000", 20260105, "daily_1000_close")],
            (None, None, 0),
        )
        self.assertEqual(
            stats[("SH600000", 20260106, "daily_1000_close")],
            (None, None, 1),
        )
        history_z, history_rank, observations = stats[
            ("SH600000", 20260107, "daily_1000_close")
        ]
        self.assertEqual(observations, 2)
        self.assertAlmostEqual(history_z or 0.0, 3.0)
        self.assertEqual(history_rank, 1.0)


class ResumeShardTest(unittest.TestCase):
    def test_atomic_shard_round_trip_and_manifest_guard(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            reference = root / "reference.csv"
            reference.write_text(
                "symbol,date,close_1000,security_category\n"
                "SH600000,20260105,10.0,1\n"
            )
            shard = root / "shards" / "batch_000001.csv"
            row = tuple("0" for _ in PRIMITIVE_FIELDS)
            write_tuple_rows(str(shard), PRIMITIVE_FIELDS, [row])
            validate_shard(shard)
            self.assertEqual(read_tuple_rows(shard, PRIMITIVE_FIELDS), [row])

            manifest = build_run_manifest(
                ["/tmp/SH600000.parquet"],
                str(reference),
                date_from=20260105,
                date_to=20260105,
                batch_size=1,
            )
            manifest_dir = root / "manifest_only"
            prepare_run_manifest(manifest_dir, manifest)
            prepare_run_manifest(manifest_dir, manifest)
            incompatible = build_run_manifest(
                ["/tmp/SH600000.parquet"],
                str(reference),
                date_from=20260105,
                date_to=20260106,
                batch_size=1,
            )
            with self.assertRaisesRegex(ValueError, "manifest mismatch"):
                prepare_run_manifest(manifest_dir, incompatible)


if __name__ == "__main__":
    unittest.main()
