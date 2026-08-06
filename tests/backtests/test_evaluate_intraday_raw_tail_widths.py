from __future__ import annotations

import sys
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "scripts" / "backtests"))

from evaluate_intraday_raw_tail_widths import tail_spread  # noqa: E402


def test_tail_spread_uses_requested_width() -> None:
    scores = list(range(10))
    returns = [value / 100.0 for value in range(10)]
    symbols = [f"S{value:02d}" for value in range(10)]

    top, bottom, spread, bucket = tail_spread(scores, returns, symbols, 0.20)

    assert bucket == 2
    assert top == pytest.approx(0.085)
    assert bottom == pytest.approx(0.005)
    assert spread == pytest.approx(0.08)


def test_tail_spread_breaks_score_ties_by_symbol() -> None:
    top, bottom, spread, bucket = tail_spread(
        scores=[1.0, 1.0, 2.0, 2.0],
        returns=[0.03, 0.01, 0.04, 0.02],
        symbols=["B", "A", "D", "C"],
        width=0.25,
    )

    assert bucket == 1
    assert bottom == pytest.approx(0.01)
    assert top == pytest.approx(0.04)
    assert spread == pytest.approx(0.03)


@pytest.mark.parametrize("width", [0.0, -0.1, 0.51])
def test_tail_spread_rejects_invalid_width(width: float) -> None:
    with pytest.raises(ValueError):
        tail_spread([1.0], [0.0], ["S"], width)
