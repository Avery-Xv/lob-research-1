from __future__ import annotations

import importlib.util
from argparse import Namespace
from pathlib import Path


SCRIPT = (
    Path(__file__).resolve().parents[2]
    / "scripts/backtests/backtest_stylized_d07_domains.py"
)
SPEC = importlib.util.spec_from_file_location("d07_domains", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_factor_dimension_round_trip() -> None:
    encoded = MODULE.encode_factor("d07_directional", "event", 20, "events")
    assert MODULE.decode_factor(encoded) == (
        "d07_directional", "event", 20, "events"
    )
    rows = [{"factor": encoded}]
    MODULE.add_factor_dimensions(rows)
    assert rows == [{
        "factor": "d07_directional",
        "clock_type": "event",
        "horizon": 20,
        "horizon_unit": "events",
    }]


def test_factor_column_validation() -> None:
    assert MODULE.selected_factor_columns("d07_directional") == ("d07_directional",)
    try:
        MODULE.selected_factor_columns("future_return")
    except ValueError as error:
        assert "unknown factor columns" in str(error)
    else:
        raise AssertionError("unknown columns must be rejected")


def test_query_keeps_signal_dimensions_and_filters() -> None:
    args = Namespace(
        factors="factor.csv",
        date_from=20260105,
        date_to=20260130,
        window_name="daily_0930_close",
        threshold_version="mean_x05",
        clock_type="event",
        horizon=20,
    )
    query, parameters = MODULE.build_query(args, ("d07_directional",))
    assert "ORDER BY date,window_name,threshold_version,clock_type,horizon,symbol" in query
    assert "d07_directional::DOUBLE" in query
    assert parameters == [
        "factor.csv", 20260105, 20260130,
        "daily_0930_close", "mean_x05", "event", 20,
    ]


def test_global_first_uses_one_basis_before_domain_split() -> None:
    rows = []
    for index in range(60):
        cap = 100_000.0 if index < 30 else 1_000_000.0
        price = 8.0
        style = float(index)
        value = style + (index % 7) / 10.0
        targets = (index / 10_000.0,) * 4
        rows.append((
            f"SH60{index:04d}", value, targets,
            [style, 0.0, 0.0, 0.0, 0.0], 0, cap, price,
        ))
    performance: list[dict[str, object]] = []
    exposures: list[dict[str, object]] = []
    MODULE.process_factor_global_then_domain(
        performance,
        exposures,
        factor_name="test|event|20|events",
        window_name="daily",
        threshold_version="mean_x05",
        date=20260105,
        rows=rows,
        min_cross_section=20,
    )
    assert {row["scope"] for row in performance} == {
        "domain", "domain_neutral_aggregate", "all_market"
    }
    assert len(exposures) == 1
