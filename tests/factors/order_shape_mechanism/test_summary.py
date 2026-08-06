from __future__ import annotations

import unittest

from scripts.factors.order_shape_mechanism.summarize_mechanisms import (
    build_contrasts,
    group_summary,
)


def stat(
    symbol: str,
    date: int,
    mechanism: str,
    variant: str,
    group: str,
    value: float,
) -> dict[str, object]:
    return {
        "symbol": symbol, "date": date, "domain": "all/all",
        "mechanism": mechanism, "variant": variant, "group_key": group,
        "observations": 1, "value_sum": value, "value_sq_sum": value * value,
        "weight_sum": 1.0,
    }


class SummaryTest(unittest.TestCase):
    def test_group_summary_keeps_stock_day_and_event_weighted_means(self) -> None:
        rows = [
            stat("SH600000", 20260105, "M1", "n10_future_signed_volume", "trigger=B", 1),
            stat("SH600001", 20260105, "M1", "n10_future_signed_volume", "trigger=B", 3),
        ]
        summary = group_summary(rows)
        self.assertEqual(len(summary), 1)
        self.assertEqual(summary[0]["weighted_mean"], 2.0)
        self.assertEqual(summary[0]["stock_day_mean"], 2.0)

    def test_m2_contrast_pairs_same_stock_day(self) -> None:
        rows = [
            stat("SH600000", 20260105, "M2", "n10_delta_heat", "price=up|trigger=B|state=B1S0", 4),
            stat("SH600000", 20260105, "M2", "n10_delta_heat", "price=down|trigger=S|state=B0S1", 1),
            stat("SH600001", 20260105, "M2", "n10_delta_heat", "price=up|trigger=B|state=B1S0", 6),
            stat("SH600001", 20260105, "M2", "n10_delta_heat", "price=down|trigger=S|state=B0S1", 2),
        ]
        contrasts = build_contrasts(rows)
        result = next(
            row for row in contrasts
            if row["contrast"] == "up_buy_vs_down_sell_delta_heat"
        )
        self.assertEqual(result["stock_days"], 2)
        self.assertEqual(result["difference"], 3.5)
        self.assertEqual(result["symbols"], 2)
        self.assertEqual(result["dates"], 1)
        self.assertEqual(result["symbol_mean_difference"], 3.5)


    def test_m5_contrast_pairs_high_and_low_days_within_symbol(self) -> None:
        rows = [
            stat("SH600000", 20260105, "M5", "mean_log_total_depth3", "heat=high|state=all", 4),
            stat("SH600000", 20260106, "M5", "mean_log_total_depth3", "heat=high|state=all", 6),
            stat("SH600000", 20260107, "M5", "mean_log_total_depth3", "heat=low|state=all", 1),
            stat("SH600000", 20260108, "M5", "mean_log_total_depth3", "heat=low|state=all", 3),
            stat("SH600001", 20260105, "M5", "mean_log_total_depth3", "heat=high|state=all", 10),
            stat("SH600001", 20260106, "M5", "mean_log_total_depth3", "heat=low|state=all", 4),
        ]
        result = next(
            row for row in build_contrasts(rows)
            if row["contrast"] == "high_vs_low_heat_depth_all"
        )
        self.assertEqual(result["stock_days"], 2)
        self.assertEqual(result["difference"], 4.5)
        self.assertEqual(result["symbols"], 2)
        self.assertEqual(result["symbol_t"], result["difference_t"])
        self.assertIsNone(result["dates"])

    def test_m6_contrasts_hold_distance_at_best(self) -> None:
        rows = [
            stat("SH600000", 20260105, "M6", "filled_orders_60s",
                 "side=B|state=B0S0|distance=best", 0.2),
            stat("SH600000", 20260105, "M6", "filled_orders_60s",
                 "side=B|state=B0S1|distance=best", 0.4),
            stat("SH600000", 20260105, "M6", "filled_orders_60s",
                 "side=B|state=B1S0|distance=best", 0.8),
            stat("SH600000", 20260105, "M6", "filled_orders_60s",
                 "side=B|state=B1S1|distance=best", 0.6),
            stat("SH600000", 20260105, "M6", "filled_orders_60s",
                 "side=B|state=B1S1|distance=near_mid", 0.0),
        ]
        result = next(
            row for row in build_contrasts(rows)
            if row["contrast"] == "B_own_high_vs_low_fill_60s"
        )
        self.assertEqual(result["stock_days"], 1)
        self.assertAlmostEqual(result["difference"], 0.4)


if __name__ == "__main__":
    unittest.main()
