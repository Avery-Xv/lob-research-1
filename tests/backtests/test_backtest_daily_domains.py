from scripts.backtests.backtest_daily_domains import domain


def test_domain_boundaries() -> None:
    assert domain(499_999, 9.99, "SH600000") == ("cap_lt_50yi", "non_star_lt_10")
    assert domain(500_000, 10.0, "SZ000001") == ("cap_50_500yi", "non_star_ge_10")
    assert domain(5_000_000, 10.0, "SH688001") == ("cap_ge_500yi", "star_ge_10")
    assert domain(5_000_000, 9.99, "SH688001") is None
