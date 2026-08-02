from __future__ import annotations

import sys
from pathlib import Path


BACKTEST_DIR = Path(__file__).resolve().parents[2] / "scripts" / "backtests"
sys.path.insert(0, str(BACKTEST_DIR))

from backtest_daily_domains import neutralize  # noqa: E402
from backtest_existing_daily_o2o_cne5 import (  # noqa: E402
    build_orthonormal_basis,
    residualize,
)


def test_cached_projection_matches_original_neutralize() -> None:
    exposures = [
        [1.0, 2.0],
        [2.0, 1.0],
        [3.0, 4.0],
        [4.0, 3.0],
        [5.0, 7.0],
    ]
    values = [2.0, 1.0, 5.0, 3.0, 8.0]

    expected = neutralize(values, exposures)
    actual = residualize(values, build_orthonormal_basis(exposures))

    assert max(abs(left - right) for left, right in zip(expected, actual)) < 1e-12
