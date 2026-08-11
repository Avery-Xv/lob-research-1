from __future__ import annotations

from scripts.backtests.analyze_f018_incremental_controls import (
    correlation,
    winsorize,
)
from scripts.factors.order_shape_non_parent.candidates import residualize


def test_winsorize_clips_one_percent_tails() -> None:
    values = [float(value) for value in range(100)]
    result = winsorize(values)
    assert result[0] == 0.99
    assert result[-1] == 98.01


def test_residual_is_orthogonal_to_multiple_controls() -> None:
    exposures = [[float(value), float(value * value)] for value in range(-20, 21)]
    factor = [2.0 * row[0] - 0.25 * row[1] + (index % 3) for index, row in enumerate(exposures)]
    residual = residualize(factor, exposures)
    assert abs(correlation(residual, [row[0] for row in exposures]) or 0.0) < 1e-12
    assert abs(correlation(residual, [row[1] for row in exposures]) or 0.0) < 1e-12
