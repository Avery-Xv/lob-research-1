from __future__ import annotations

import pytest

from scripts.factors.passive_large_gap_ratio.intraday_window_factor import validate_window


def test_validate_window_accepts_1000_to_1030() -> None:
    validate_window(100000000, 103000000)


def test_validate_window_rejects_lunch_break() -> None:
    with pytest.raises(ValueError):
        validate_window(113000000, 130000000)


def test_validate_window_rejects_closing_auction() -> None:
    with pytest.raises(ValueError):
        validate_window(143000000, 150000000)
