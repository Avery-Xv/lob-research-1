from __future__ import annotations

import csv
import importlib.util
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[2] / "scripts/backtests/backtest_daily_domains.py"
SPEC = importlib.util.spec_from_file_location("backtest_daily_domains", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_neutralize_removes_intercept_and_exposure() -> None:
    values = [3.0, 4.0, 8.0, 12.0, 17.0]
    exposures = [[1.0], [2.0], [4.0], [7.0], [11.0]]

    residuals = MODULE.neutralize(values, exposures)
    centered_exposure = [row[0] - 5.0 for row in exposures]

    assert abs(sum(residuals)) < 1e-12
    assert abs(sum(x * y for x, y in zip(residuals, centered_exposure))) < 1e-12


def test_domain_boundaries() -> None:
    assert MODULE.domain(499_999.0, 9.99, "SZ000001") == (
        "cap_lt_50yi",
        "non_star_lt_10",
    )
    assert MODULE.domain(500_000.0, 10.0, "SZ000001") == (
        "cap_50_500yi",
        "non_star_ge_10",
    )
    assert MODULE.domain(5_000_000.0, 10.0, "SH688001") == (
        "cap_ge_500yi",
        "star_ge_10",
    )
    assert MODULE.domain(5_000_000.0, 9.99, "SH688001") is None


def test_load_return_rows_supports_same_day_intraday_exit(tmp_path: Path) -> None:
    path = tmp_path / "returns.csv"
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["symbol", "date", "next_date", "open", "intraday_ret"],
        )
        writer.writeheader()
        writer.writerows(
            [
                {
                    "symbol": "SH600000",
                    "date": "20260105",
                    "next_date": "20260106",
                    "open": "10",
                    "intraday_ret": "0.01",
                },
                {
                    "symbol": "SH600000",
                    "date": "20260106",
                    "next_date": "20260107",
                    "open": "11",
                    "intraday_ret": "-0.02",
                },
            ]
        )

    rows, previous_dates = MODULE.load_return_rows(
        path, "intraday_ret", "date"
    )

    assert rows[("SH600000", "20260106")]["ret"] == -0.02
    assert rows[("SH600000", "20260106")]["exit_date"] == "20260106"
    assert previous_dates["20260106"] == "20260105"


def test_select_return_row_supports_signal_date_overnight_return() -> None:
    returns = {
        ("SH600000", "20260105"): {
            "ret": 0.01,
            "next_date": "20260106",
            "exit_date": "20260106",
        },
        ("SH600000", "20260106"): {
            "ret": -0.02,
            "next_date": "20260107",
            "exit_date": "20260107",
        },
    }

    same_day = MODULE.select_return_row(returns, "SH600000", "20260105", 0)
    next_day = MODULE.select_return_row(returns, "SH600000", "20260105", 1)

    assert same_day is not None and same_day[0] == "20260105"
    assert same_day[1]["ret"] == 0.01
    assert next_day is not None and next_day[0] == "20260106"
    assert next_day[1]["ret"] == -0.02
