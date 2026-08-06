from __future__ import annotations

import importlib.util
from pathlib import Path


SCRIPT = (
    Path(__file__).resolve().parents[2]
    / "scripts/backtests/backtest_stylized_d04_d06_domains.py"
)
SPEC = importlib.util.spec_from_file_location("d04_d06_domains", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_domain_boundaries_and_star_exclusion() -> None:
    assert MODULE.domain(499_999, 9.99, "SH600000") == (
        "cap_lt_50yi", "non_star_lt_10"
    )
    assert MODULE.domain(500_000, 10.0, "SH600000") == (
        "cap_50_500yi", "non_star_ge_10"
    )
    assert MODULE.domain(5_000_000, 20.0, "SH688001") == (
        "cap_ge_500yi", "star_ge_10"
    )
    assert MODULE.domain(5_000_000, 9.0, "SH688001") is None


def test_residualization_removes_intercept_and_style() -> None:
    exposures = [[1.0], [2.0], [3.0], [4.0]]
    basis = MODULE.build_orthonormal_basis(exposures)
    residual = MODULE.residualize([3.0, 5.0, 7.0, 9.0], basis)
    assert max(map(abs, residual)) < 1e-10


def test_compounded_requires_each_horizon_label() -> None:
    assert abs(float(MODULE.compounded([0.1, -0.1])) - (0.99 - 1.0)) < 1e-15
    assert MODULE.compounded([0.1, None]) is None


def test_event_gap_uses_zero_events_as_controls() -> None:
    returns = [0.03, -0.02, 0.01, 0.00]
    events = [1, -1, 0, 0]
    assert abs(float(MODULE.event_gap(returns, events, 1)) - 0.025) < 1e-15
    assert abs(float(MODULE.event_gap(returns, events, -1)) + 0.025) < 1e-15


def test_horizon_missing_labels_are_filtered_independently() -> None:
    rows = []
    for index in range(20):
        targets = (
            index / 10_000,
            index / 10_000 if index < 19 else None,
            index / 10_000,
            index / 10_000,
        )
        rows.append(
            (
                f"SH60{index:04d}",
                float(index),
                targets,
                [float(index), 0.0, 0.0, 0.0, 0.0],
                0,
            )
        )
    output: list[dict[str, object]] = []
    MODULE.append_scope_metrics(
        output,
        factor_name="test",
        window_name="daily",
        threshold_version="p90",
        date=20260105,
        scope="all_market",
        cap_group="all",
        price_group="all",
        rows=rows,
        neutral_scores=[float(index) for index in range(20)],
    )
    assert {row["target"] for row in output} == {
        "open_to_open_d1", "open_to_open_d3", "open_to_open_d5"
    }



def test_newey_west_lag_zero_matches_standard_t() -> None:
    values = [0.01, -0.02, 0.03, 0.01]
    assert MODULE.newey_west_mean_t(values, 0) == MODULE.mean_t(values)
