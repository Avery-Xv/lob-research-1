from __future__ import annotations

from scripts.backtests.backtest_non_parent_direct_targets import summarize


def test_summary_keeps_other_dates_when_one_rank_ic_is_undefined() -> None:
    rows = [
        {"scope": "domain", "domain": "d1", "factor": "f", "target": "y", "date": 20260105, "n": 20, "rank_ic": None, "d10_d1": 0.1},
        {"scope": "domain", "domain": "d1", "factor": "f", "target": "y", "date": 20260106, "n": 20, "rank_ic": 0.2, "d10_d1": 0.3},
    ]
    result = summarize(rows)[0]
    assert result["rank_ic"] == 0.2
    assert result["d10_d1"] == 0.2
    assert result["n_dates"] == 2
