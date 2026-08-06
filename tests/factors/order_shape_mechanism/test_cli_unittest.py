from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.factors.order_shape_mechanism import reproduce_mechanisms_v4 as module


class MechanismCliTest(unittest.TestCase):
    def test_queries_keep_large_queue_columns_out(self) -> None:
        self.assertNotIn("bid_ordvol", module.WARMUP_QUERY)
        self.assertNotIn("ask_ordvol", module.WARMUP_QUERY)
        self.assertNotIn("bid_vol", module.WARMUP_QUERY)
        self.assertNotIn("source_buy_order_id", module.WARMUP_QUERY)
        self.assertIn("bid_vol", module.TARGET_QUERY)
        self.assertIn("source_buy_order_id", module.TARGET_QUERY)
        self.assertNotIn("bid_ordvol", module.TARGET_QUERY)
        self.assertNotIn("source_link_status", module.TARGET_QUERY)

    def test_load_inputs_enforces_stock_metadata_and_month_split(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            temp = Path(directory)
            root = temp / "event_depth10_v4"
            warm = root / "202512" / "SH600000.parquet"
            target = root / "202601" / "SH600000.parquet"
            warm.parent.mkdir(parents=True)
            target.parent.mkdir(parents=True)
            warm.touch()
            target.touch()
            file_list = temp / "paths.txt"
            file_list.write_text(f"{warm}\n{target}\n")
            metadata = temp / "metadata.json"
            metadata.write_text(json.dumps({
                "months": ["202512", "202601"],
                "output_etf_symbols": 0,
                "security_type_whitelist": {
                    "SecuCategory": [1], "SecuMarket": [83, 90]
                },
            }))
            with patch.object(module, "V4_ROOT", root):
                inputs, _ = module.load_inputs(
                    file_list, metadata, ["202512"], "202601"
                )
            self.assertEqual(list(inputs), ["SH600000"])
            self.assertEqual(set(inputs["SH600000"]), {"202512", "202601"})

    def test_load_inputs_rejects_uncertified_etf_count(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            temp = Path(directory)
            file_list = temp / "paths.txt"
            file_list.write_text("")
            metadata = temp / "metadata.json"
            metadata.write_text(json.dumps({
                "months": ["202512", "202601"],
                "output_etf_symbols": 1,
                "security_type_whitelist": {
                    "SecuCategory": [1], "SecuMarket": [83, 90]
                },
            }))
            with self.assertRaisesRegex(ValueError, "zero ETF"):
                module.load_inputs(file_list, metadata, ["202512"], "202601")


if __name__ == "__main__":
    unittest.main()
