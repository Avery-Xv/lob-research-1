from __future__ import annotations

import csv
from pathlib import Path

from scripts.backtests.backtest_intraday_domains import load_previous_vectors, order_returns


def test_load_previous_vectors_normalizes_hyphenated_dates(tmp_path: Path) -> None:
    path = tmp_path / "risk.csv"
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["symbol", "date", "risk"])
        writer.writeheader()
        writer.writerows(
            [
                {"symbol": "SH600000", "date": "2026-01-05", "risk": "1"},
                {"symbol": "SH600000", "date": "2026-01-06", "risk": "2"},
            ]
        )

    previous = load_previous_vectors(str(path), ["risk"])

    assert previous[("SH600000", "20260106")] == [1.0]


def test_order_returns_uses_neutralized_factor_order() -> None:
    factors = [2.0, -1.0, 1.0]
    returns = [0.02, -0.01, 0.01]
    symbols = ["SH600000", "SH600001", "SH600002"]

    assert order_returns(factors, returns, symbols) == [-0.01, 0.01, 0.02]
