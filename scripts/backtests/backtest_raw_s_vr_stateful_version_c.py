#!/usr/bin/env python3
"""Stateful Version C backtest for raw S + raw VR with a raw B entry/exit filter."""

from __future__ import annotations

import argparse
import csv
import json
import math
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from statistics import mean
from typing import Mapping, Sequence

import duckdb

import backtest_large_gap_by_raw_vr_state as base
from backtest_daily_domains import domain
from backtest_existing_daily_o2o_cne5 import finite
from backtest_raw_s_vr_long_b_filter import percentile_positions


PROJECT_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class Observation:
    symbol: str
    buy_gap: float
    sell_gap: float
    vr_log: float
    previous_market_cap: float
    signal_price: float


@dataclass(frozen=True)
class Candidate:
    observation: Observation
    cap_group: str
    price_group: str
    vr_state: str
    s_percentile: float
    b_percentile: float


def compound(values: Sequence[float]) -> float:
    wealth = 1.0
    for value in values:
        wealth *= 1.0 + value
    return wealth - 1.0


def relative_compound(total: Sequence[float], benchmark: Sequence[float]) -> float:
    return (1.0 + compound(total)) / (1.0 + compound(benchmark)) - 1.0


def portfolio_turnover(
    previous: Mapping[str, float], target: Mapping[str, float]
) -> tuple[float, float]:
    symbols = set(previous) | set(target)
    buy = sum(max(target.get(symbol, 0.0) - previous.get(symbol, 0.0), 0.0)
              for symbol in symbols)
    sell = sum(max(previous.get(symbol, 0.0) - target.get(symbol, 0.0), 0.0)
               for symbol in symbols)
    return buy, sell


def build_target(
    candidates: Mapping[str, Candidate],
    previous_symbols: set[str],
    s_entry: float,
    s_exit: float,
    b_entry: float,
    b_exit: float,
) -> tuple[set[str], set[str], set[str]]:
    retained = {
        symbol for symbol in previous_symbols
        if symbol in candidates
        and candidates[symbol].s_percentile >= s_exit
        and candidates[symbol].b_percentile >= b_exit
    }
    entered = {
        symbol for symbol, candidate in candidates.items()
        if symbol not in previous_symbols
        and candidate.s_percentile >= s_entry
        and candidate.b_percentile >= b_entry
    }
    return retained | entered, retained, entered


def equal_weights(symbols: set[str]) -> dict[str, float]:
    if not symbols:
        return {}
    weight = 1.0 / len(symbols)
    return {symbol: weight for symbol in symbols}


def load_common(
    returns_path: str, caps_path: str, date_from: int, date_to: int
) -> dict[tuple[str, int], tuple[float, float]]:
    """Load signal-time price and previous-day cap with the established status screen."""
    connection = duckdb.connect()
    connection.read_csv(returns_path).create_view("returns_raw")
    connection.read_csv(caps_path).create_view("caps_raw")
    connection.execute(
        """
        CREATE VIEW caps AS
        SELECT DISTINCT symbol, date::INTEGER AS date, total_mv::DOUBLE AS total_mv
        FROM caps_raw;
        CREATE VIEW previous_caps AS
        SELECT symbol, date,
               lag(total_mv) OVER (PARTITION BY symbol ORDER BY date) AS previous_market_cap
        FROM caps;
        """
    )
    rows = connection.execute(
        """
        SELECT r.symbol, r.date::INTEGER, r.signal_price::DOUBLE,
               c.previous_market_cap::DOUBLE
        FROM returns_raw r
        JOIN previous_caps c ON c.symbol=r.symbol AND c.date=r.date::INTEGER
        WHERE r.date::INTEGER BETWEEN ? AND ?
          AND r.is_st::INTEGER=0 AND r.is_suspended::INTEGER=0
        """,
        [date_from, date_to],
    ).fetchall()
    connection.close()
    output: dict[tuple[str, int], tuple[float, float]] = {}
    for symbol, date, signal_price, cap in rows:
        parsed_price, parsed_cap = finite(signal_price), finite(cap)
        if parsed_price is None or parsed_price <= 0 or parsed_cap is None or parsed_cap <= 0:
            continue
        output[(str(symbol), int(date))] = (float(parsed_price), float(parsed_cap))
    return output


def load_observations(
    factor_path: str,
    common: Mapping[tuple[str, int], tuple[float, float]],
    date_from: int,
    date_to: int,
) -> dict[int, list[Observation]]:
    grouped: dict[int, list[Observation]] = defaultdict(list)
    seen: set[tuple[str, int]] = set()
    with Path(factor_path).open(newline="") as handle:
        for row in csv.DictReader(handle):
            symbol, date = row["symbol"], int(row["date"])
            if not date_from <= date <= date_to:
                continue
            key = (symbol, date)
            if key in seen:
                raise ValueError(f"duplicate factor row: {key}")
            seen.add(key)
            if not symbol.startswith(("SH", "SZ")):
                raise ValueError(f"non-SH/SZ symbol: {symbol}")
            if "ETF excluded" not in row.get("universe_rule", ""):
                raise ValueError("factor artifact does not document ETF exclusion")
            common_value = common.get(key)
            buy_gap = finite(row.get("intraday_large_gap_buy_ratio"))
            sell_gap = finite(row.get("intraday_large_gap_sell_ratio"))
            vr_log = finite(row.get("vr_log"))
            if (
                common_value is None
                or buy_gap is None
                or sell_gap is None
                or vr_log is None
                or not base.parse_bool(row.get("intraday_passes_match_rate"))
                or not base.parse_bool(row.get("ob_is_valid"))
            ):
                continue
            signal_price, previous_cap = common_value
            grouped[date].append(Observation(
                symbol=symbol,
                buy_gap=float(buy_gap),
                sell_gap=float(sell_gap),
                vr_log=float(vr_log),
                previous_market_cap=previous_cap,
                signal_price=signal_price,
            ))
    return grouped


def load_prices(path: str) -> tuple[dict[tuple[str, int], tuple[float, float]], list[int]]:
    prices: dict[tuple[str, int], tuple[float, float]] = {}
    dates: set[int] = set()
    with Path(path).open(newline="") as handle:
        for row in csv.DictReader(handle):
            symbol, date = row["symbol"], int(row["date"])
            close_1030, close_1100 = finite(row.get("close_1030")), finite(row.get("close_1100"))
            if close_1030 is None or close_1030 <= 0:
                continue
            if close_1100 is None or close_1100 <= 0:
                close_1100 = close_1030
            key = (symbol, date)
            if key in prices:
                raise ValueError(f"duplicate price row: {key}")
            prices[key] = (float(close_1030), float(close_1100))
            dates.add(date)
    return prices, sorted(dates)


def form_candidates(
    rows: Sequence[Observation], minimum_cross_section: int
) -> tuple[dict[str, Candidate], dict[str, Observation]]:
    candidates: dict[str, Candidate] = {}
    benchmark: dict[str, Observation] = {}
    domains: dict[tuple[str, str], list[Observation]] = defaultdict(list)
    for row in rows:
        group = domain(row.previous_market_cap, row.signal_price, row.symbol)
        if group is not None:
            domains[group].append(row)
    for (cap_group, price_group), domain_rows in sorted(domains.items()):
        if len(domain_rows) < minimum_cross_section:
            continue
        domain_rows.sort(key=lambda item: item.symbol)
        states = base.assign_raw_vr_states(
            [row.vr_log for row in domain_rows], [row.symbol for row in domain_rows]
        )
        accepted = [
            (row, state) for row, state in zip(domain_rows, states)
            if state in {"mid", "high"}
        ]
        if len(accepted) < minimum_cross_section:
            continue
        accepted_rows = [item[0] for item in accepted]
        s_positions = percentile_positions(
            [row.sell_gap for row in accepted_rows], [row.symbol for row in accepted_rows]
        )
        b_positions = percentile_positions(
            [row.buy_gap for row in accepted_rows], [row.symbol for row in accepted_rows]
        )
        for (row, state), s_position, b_position in zip(
            accepted, s_positions, b_positions
        ):
            benchmark[row.symbol] = row
            candidates[row.symbol] = Candidate(
                observation=row,
                cap_group=cap_group,
                price_group=price_group,
                vr_state=state,
                s_percentile=s_position,
                b_percentile=b_position,
            )
    return candidates, benchmark


def symbol_returns(
    symbols: Sequence[str],
    date: int,
    next_date: int,
    prices: Mapping[tuple[str, int], tuple[float, float]],
) -> tuple[dict[str, tuple[float, float, float]], int]:
    output: dict[str, tuple[float, float, float]] = {}
    missing = 0
    for symbol in symbols:
        current = prices.get((symbol, date))
        following = prices.get((symbol, next_date))
        if current is None or following is None:
            output[symbol] = (0.0, 0.0, 0.0)
            missing += 1
            continue
        close_1030, close_1100 = current
        next_close_1030 = following[0]
        first_30m = close_1100 / close_1030 - 1.0
        total = next_close_1030 / close_1030 - 1.0
        output[symbol] = (first_30m, total - first_30m, total)
    return output, missing


def weighted_return(
    weights: Mapping[str, float], returns: Mapping[str, tuple[float, float, float]], index: int
) -> float:
    return sum(weight * returns[symbol][index] for symbol, weight in weights.items())


def run_backtest(
    grouped: Mapping[int, Sequence[Observation]],
    prices: Mapping[tuple[str, int], tuple[float, float]],
    calendar: Sequence[int],
    minimum_cross_section: int,
    overlay_share: float,
    buy_cost_bp: float,
    sell_cost_bp: float,
    s_entry: float,
    s_exit: float,
    b_entry: float,
    b_exit: float,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    next_dates = {date: calendar[index + 1] for index, date in enumerate(calendar[:-1])}
    previous_weights: dict[str, float] = {}
    by_date: list[dict[str, object]] = []
    holdings: list[dict[str, object]] = []
    for date in sorted(grouped):
        next_date = next_dates.get(date)
        if next_date is None:
            continue
        candidates, benchmark = form_candidates(grouped[date], minimum_cross_section)
        target_symbols, retained, entered = build_target(
            candidates, set(previous_weights), s_entry, s_exit, b_entry, b_exit
        )
        if not target_symbols or not benchmark:
            continue
        target_weights = equal_weights(target_symbols)
        benchmark_weights = equal_weights(set(benchmark))
        buy_turnover, sell_turnover = portfolio_turnover(previous_weights, target_weights)
        if not previous_weights:
            # Funding the initial active sleeve also requires selling the same amount of base.
            sell_turnover = 1.0
        all_symbols = sorted(set(benchmark) | target_symbols)
        returns, missing = symbol_returns(all_symbols, date, next_date, prices)
        selected_first = weighted_return(target_weights, returns, 0)
        selected_remainder = weighted_return(target_weights, returns, 1)
        selected_total = weighted_return(target_weights, returns, 2)
        base_first = weighted_return(benchmark_weights, returns, 0)
        base_remainder = weighted_return(benchmark_weights, returns, 1)
        base_total = weighted_return(benchmark_weights, returns, 2)
        gross_first = (1.0 - overlay_share) * base_first + overlay_share * selected_first
        gross_remainder = (
            (1.0 - overlay_share) * base_remainder + overlay_share * selected_remainder
        )
        gross_total = gross_first + gross_remainder
        trading_cost = overlay_share * (
            buy_turnover * buy_cost_bp + sell_turnover * sell_cost_bp
        ) / 10_000.0
        net_total = gross_total - trading_cost
        by_date.append({
            "date": date,
            "next_date": next_date,
            "n_benchmark": len(benchmark_weights),
            "n_selected": len(target_weights),
            "n_retained": len(retained),
            "n_entered": len(entered),
            "n_exited": len(set(previous_weights) - target_symbols),
            "holding_overlap": len(retained) / len(previous_weights) if previous_weights else 0.0,
            "selected_buy_turnover": buy_turnover,
            "selected_sell_turnover": sell_turnover,
            "nav_two_way_turnover": overlay_share * (buy_turnover + sell_turnover),
            "missing_next_price_all": missing,
            "missing_next_price_selected": sum(
                prices.get((symbol, next_date)) is None for symbol in target_symbols
            ),
            "base_first_30m_return": base_first,
            "selected_first_30m_return": selected_first,
            "gross_first_30m_return": gross_first,
            "base_remainder_return_contribution": base_remainder,
            "selected_remainder_return_contribution": selected_remainder,
            "gross_remainder_return_contribution": gross_remainder,
            "base_total_return": base_total,
            "selected_total_return": selected_total,
            "gross_total_return": gross_total,
            "gross_active_return": gross_total - base_total,
            "trading_cost": trading_cost,
            "net_total_return": net_total,
            "net_active_return": net_total - base_total,
        })
        for symbol, weight in sorted(target_weights.items()):
            candidate = candidates[symbol]
            holdings.append({
                "date": date,
                "next_date": next_date,
                "symbol": symbol,
                "status": "retained" if symbol in retained else "entered",
                "weight_in_active_sleeve": weight,
                "weight_in_total_nav": overlay_share * weight,
                "cap_group": candidate.cap_group,
                "price_group": candidate.price_group,
                "vr_state": candidate.vr_state,
                "s_percentile": candidate.s_percentile,
                "b_percentile": candidate.b_percentile,
                "first_30m_return": returns[symbol][0],
                "remainder_return_contribution": returns[symbol][1],
                "total_return": returns[symbol][2],
                "missing_next_price": int(prices.get((symbol, next_date)) is None),
            })
        previous_weights = target_weights
    return by_date, holdings


def summarize(rows: Sequence[dict[str, object]]) -> dict[str, object]:
    if not rows:
        raise ValueError("no backtest rows")
    output: dict[str, object] = {
        "n_periods": len(rows),
        "first_date": rows[0]["date"],
        "last_date": rows[-1]["date"],
        "last_exit_date": rows[-1]["next_date"],
        "avg_selected": mean(float(row["n_selected"]) for row in rows),
        "avg_holding_overlap": mean(float(row["holding_overlap"]) for row in rows[1:]),
        "avg_nav_two_way_turnover": mean(float(row["nav_two_way_turnover"]) for row in rows),
        "annualized_nav_one_way_turnover": (
            mean(float(row["nav_two_way_turnover"]) for row in rows) * 252.0 / 2.0
        ),
        "total_trading_cost": sum(float(row["trading_cost"]) for row in rows),
        "missing_next_price_selected": sum(
            int(row["missing_next_price_selected"]) for row in rows
        ),
    }
    metrics = (
        "base_first_30m_return", "selected_first_30m_return", "gross_first_30m_return",
        "base_remainder_return_contribution", "selected_remainder_return_contribution",
        "gross_remainder_return_contribution", "base_total_return", "selected_total_return",
        "gross_total_return", "gross_active_return", "trading_cost", "net_total_return",
        "net_active_return",
    )
    for metric in metrics:
        values = [float(row[metric]) for row in rows]
        output[f"mean_{metric}"], output[f"{metric}_t"] = base.mean_t(values)
    base_values = [float(row["base_total_return"]) for row in rows]
    gross_values = [float(row["gross_total_return"]) for row in rows]
    net_values = [float(row["net_total_return"]) for row in rows]
    output["cumulative_base_return"] = compound(base_values)
    output["cumulative_gross_total_return"] = compound(gross_values)
    output["cumulative_net_total_return"] = compound(net_values)
    output["cumulative_gross_active_excess"] = relative_compound(gross_values, base_values)
    output["cumulative_net_active_excess"] = relative_compound(net_values, base_values)
    return output


def monthly_summary(rows: Sequence[dict[str, object]]) -> list[dict[str, object]]:
    grouped: dict[int, list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        grouped[int(row["date"]) // 100].append(row)
    output: list[dict[str, object]] = []
    for month, values in sorted(grouped.items()):
        base_values = [float(row["base_total_return"]) for row in values]
        gross_values = [float(row["gross_total_return"]) for row in values]
        net_values = [float(row["net_total_return"]) for row in values]
        output.append({
            "month": month,
            "n_periods": len(values),
            "avg_selected": mean(float(row["n_selected"]) for row in values),
            "avg_nav_two_way_turnover": mean(
                float(row["nav_two_way_turnover"]) for row in values
            ),
            "cumulative_base_return": compound(base_values),
            "cumulative_gross_total_return": compound(gross_values),
            "cumulative_net_total_return": compound(net_values),
            "cumulative_gross_active_excess": relative_compound(gross_values, base_values),
            "cumulative_net_active_excess": relative_compound(net_values, base_values),
            "total_trading_cost": sum(float(row["trading_cost"]) for row in values),
        })
    return output


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--factors", default=str(PROJECT_ROOT / "data/processed/joint_large_gap_order_behavior_v4_a_share_1000_1030_202602_202604.csv"))
    parser.add_argument("--returns", default=str(PROJECT_ROOT / "data/cache/min1_ret_1031_1040_1045_1100_202602_202604.csv"))
    parser.add_argument("--market-caps", default=str(PROJECT_ROOT / "data/cache/daily_market_cap_202601_202604.csv"))
    parser.add_argument("--prices", default=str(PROJECT_ROOT / "data/cache/min1_close_1030_1100_stock_202602_202605.csv"))
    parser.add_argument("--date-from", type=int, default=20260201)
    parser.add_argument("--date-to", type=int, default=20260430)
    parser.add_argument("--minimum-cross-section", type=int, default=20)
    parser.add_argument("--overlay-share", type=float, default=0.20)
    parser.add_argument("--buy-cost-bp", type=float, default=3.0)
    parser.add_argument("--sell-cost-bp", type=float, default=8.0)
    parser.add_argument("--s-entry", type=float, default=0.90)
    parser.add_argument("--s-exit", type=float, default=0.80)
    parser.add_argument("--b-entry", type=float, default=0.25)
    parser.add_argument("--b-exit", type=float, default=0.15)
    parser.add_argument("--output-dir", default=str(PROJECT_ROOT / "results/raw/stateful_version_c_202602_202604"))
    args = parser.parse_args()
    if not 0 < args.overlay_share <= 1:
        raise ValueError("overlay share must lie in (0, 1]")
    if not 0 <= args.s_exit <= args.s_entry <= 1:
        raise ValueError("S thresholds must satisfy 0 <= exit <= entry <= 1")
    if not 0 <= args.b_exit <= args.b_entry <= 1:
        raise ValueError("B thresholds must satisfy 0 <= exit <= entry <= 1")

    common = load_common(args.returns, args.market_caps, args.date_from, args.date_to)
    grouped = load_observations(args.factors, common, args.date_from, args.date_to)
    prices, calendar = load_prices(args.prices)
    by_date, holdings = run_backtest(
        grouped, prices, calendar, args.minimum_cross_section, args.overlay_share,
        args.buy_cost_bp, args.sell_cost_bp, args.s_entry, args.s_exit,
        args.b_entry, args.b_exit,
    )
    summary = summarize(by_date)
    output_dir = Path(args.output_dir)
    base.write_csv(output_dir / "performance_by_date.csv", by_date)
    base.write_csv(output_dir / "holdings_by_date.csv", holdings)
    base.write_csv(output_dir / "monthly_summary.csv", monthly_summary(by_date))
    output_dir.mkdir(parents=True, exist_ok=True)
    metadata = {
        "strategy": "Version C: stateful raw S + raw VR, raw B entry/exit filter",
        "neutralization": "none",
        "vr_scope": "middle and top raw-vr terciles within each date-domain",
        "entry": {"S_percentile_min": args.s_entry, "B_percentile_min": args.b_entry},
        "retention": {"S_percentile_min": args.s_exit, "B_percentile_min": args.b_exit},
        "overlay_share": args.overlay_share,
        "execution": "rebalance once at 10:30; hold through overnight to next market trading day 10:30; no 11:00 reversal",
        "costs": {"buy_bp": args.buy_cost_bp, "sell_bp": args.sell_cost_bp, "basis": "adjacent active-sleeve target-weight deltas"},
        "base_inventory": "synthetic equal-weight medium+high VR benchmark in valid structural domains; actual bottom holdings not supplied",
        "domain_rule": "previous-day market cap crossed with signal-time price/board",
        "universe_rule": "point-in-time Shanghai/Shenzhen A shares; ETF excluded upstream and validated in price cache",
        "validity": "match_rate>=0.95, ob_is_valid, non-ST, non-suspended at signal time",
        "missing_next_price_policy": "zero return, never skip to a later tradable date; count disclosed",
        "return_attribution": "first 30m plus arithmetic remainder contribution equals 10:30-to-next-10:30 return",
        "summary": summary,
    }
    (output_dir / "metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2) + "\n"
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
