#!/usr/bin/env python3
"""Run point-in-time domain diagnostics for stylized-fact D01--D03."""

from __future__ import annotations

import argparse
import csv
import math
from collections import defaultdict
from pathlib import Path
from statistics import mean, stdev

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CAP_GROUPS = ("cap_lt_50yi", "cap_50_500yi", "cap_ge_500yi")
PRICE_GROUPS = ("non_star_lt_10", "non_star_ge_10", "star_ge_10")


def finite(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def domain(previous_market_cap: float, price: float, symbol: str) -> tuple[str, str] | None:
    if previous_market_cap < 500_000:
        cap_group = CAP_GROUPS[0]
    elif previous_market_cap < 5_000_000:
        cap_group = CAP_GROUPS[1]
    else:
        cap_group = CAP_GROUPS[2]
    star = symbol.startswith(("SH688", "SH689"))
    if not star:
        price_group = PRICE_GROUPS[0] if price < 10 else PRICE_GROUPS[1]
    elif price >= 10:
        price_group = PRICE_GROUPS[2]
    else:
        return None
    return cap_group, price_group


def ranks(values: list[float]) -> list[float]:
    order = sorted(range(len(values)), key=values.__getitem__)
    result = [0.0] * len(values)
    start = 0
    while start < len(order):
        end = start + 1
        while end < len(order) and values[order[end]] == values[order[start]]:
            end += 1
        rank = (start + end - 1) / 2.0 + 1.0
        for position in range(start, end):
            result[order[position]] = rank
        start = end
    return result


def pearson(xs: list[float], ys: list[float]) -> float | None:
    if len(xs) < 3:
        return None
    mx, my = mean(xs), mean(ys)
    vx = sum((x - mx) ** 2 for x in xs)
    vy = sum((y - my) ** 2 for y in ys)
    if vx <= 0 or vy <= 0:
        return None
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / math.sqrt(vx * vy)


def mean_t(values: list[float]) -> tuple[float | None, float | None]:
    if not values:
        return None, None
    average = mean(values)
    if len(values) < 2:
        return average, None
    volatility = stdev(values)
    return average, average / (volatility / math.sqrt(len(values))) if volatility > 0 else None


def compounded(values: list[float | None]) -> float | None:
    if any(value is None for value in values):
        return None
    return math.prod(1.0 + float(value) for value in values) - 1.0


def daily_stat(rows: list[tuple[str, float, float, bool]]) -> dict[str, object] | None:
    if len(rows) < 20:
        return None
    ordered = sorted(rows, key=lambda row: (row[1], row[0]))
    factors = [row[1] for row in ordered]
    returns = [row[2] for row in ordered]
    bucket = max(1, len(rows) // 10)
    events = [row[2] for row in rows if row[3]]
    controls = [row[2] for row in rows if not row[3]]
    return {
        "n": len(rows),
        "rank_ic": pearson(ranks(factors), ranks(returns)),
        "d10_d1": mean(returns[-bucket:]) - mean(returns[:bucket]),
        "event_n": len(events),
        "event_ret": mean(events) if events else None,
        "non_event_ret": mean(controls) if controls else None,
        "event_minus_non_event": (
            mean(events) - mean(controls) if events and controls else None
        ),
    }


def summarize(daily_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    grouped: dict[tuple[str, ...], list[dict[str, object]]] = defaultdict(list)
    keys = ("frequency", "window_name", "factor", "target", "cap_group", "price_group")
    for row in daily_rows:
        grouped[tuple(str(row[key]) for key in keys)].append(row)
    output = []
    for group, rows in sorted(grouped.items()):
        rank_ics = [float(row["rank_ic"]) for row in rows if row["rank_ic"] is not None]
        spreads = [float(row["d10_d1"]) for row in rows if row["d10_d1"] is not None]
        event_spreads = [
            float(row["event_minus_non_event"])
            for row in rows
            if row["event_minus_non_event"] is not None
        ]
        rank_ic, rank_ic_t = mean_t(rank_ics)
        spread, spread_t = mean_t(spreads)
        event_spread, event_spread_t = mean_t(event_spreads)
        output.append(
            dict(
                zip(keys, group),
                n_days=len(rows),
                n_obs=sum(int(row["n"]) for row in rows),
                avg_names=mean(int(row["n"]) for row in rows),
                rank_ic=rank_ic,
                rank_ic_t=rank_ic_t,
                d10_d1=spread,
                d10_d1_t=spread_t,
                event_n=sum(int(row["event_n"]) for row in rows),
                event_minus_non_event=event_spread,
                event_minus_non_event_t=event_spread_t,
            )
        )
    return output


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        raise ValueError(f"no rows for {path}")
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def add_observations(
    grouped: dict[tuple[str, ...], list[tuple[str, float, float, bool]]],
    *,
    frequency: str,
    window_name: str,
    symbol: str,
    date: str,
    cap_group: str,
    price_group: str,
    factors: dict[str, float | None],
    targets: dict[str, float | None],
) -> None:
    for factor_name, factor in factors.items():
        if factor is None:
            continue
        is_event_factor = factor_name.startswith("D03")
        event = is_event_factor and factor != 0.0
        for target_name, target in targets.items():
            if target is None:
                continue
            for cap, price in (("all", "all"), (cap_group, price_group)):
                key = (frequency, window_name, factor_name, target_name, cap, price, date)
                grouped[key].append((symbol, factor, target, event))


def build_daily_rows(grouped: dict[tuple[str, ...], list[tuple[str, float, float, bool]]]) -> list[dict[str, object]]:
    output = []
    for key, rows in sorted(grouped.items()):
        stats = daily_stat(rows)
        if stats is None:
            continue
        frequency, window_name, factor, target, cap_group, price_group, date = key
        output.append(
            {
                "frequency": frequency,
                "window_name": window_name,
                "factor": factor,
                "target": target,
                "cap_group": cap_group,
                "price_group": price_group,
                "date": date,
                **stats,
            }
        )
    return output


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--factors",
        default=str(PROJECT_ROOT / "data/processed/stylized_fact_4_6/g1_d01_d03_factors_202512_202601_history20_v3.csv"),
    )
    parser.add_argument(
        "--market-caps",
        default=str(PROJECT_ROOT / "data/cache/daily_market_cap_202512_202601.csv"),
    )
    parser.add_argument(
        "--intraday-returns",
        default=str(PROJECT_ROOT / "data/cache/min1_ret_1031_decay_horizons_202601_clean_with_status.csv"),
    )
    parser.add_argument(
        "--daily-o2c",
        default=str(PROJECT_ROOT / "data/cache/daily_open_to_close_market_calendar_202512_20260206.csv"),
    )
    parser.add_argument(
        "--daily-overnight",
        default=str(PROJECT_ROOT / "data/cache/daily_close_to_next_open_market_calendar_202512_20260206.csv"),
    )
    parser.add_argument("--date-from", default="20260101")
    parser.add_argument("--date-to", default="20260130")
    parser.add_argument(
        "--output-dir",
        default=str(PROJECT_ROOT / "results/daily/stylized_fact_4_6/domain_diagnostics_202601"),
    )
    args = parser.parse_args()

    connection = duckdb.connect()
    connection.read_csv(args.factors).create_view("factors")
    connection.read_csv(args.market_caps).create_view("market_caps_raw")
    connection.execute(
        "CREATE VIEW market_caps AS SELECT DISTINCT symbol, date, total_mv FROM market_caps_raw"
    )
    connection.execute(
        """
        CREATE VIEW previous_caps AS
        SELECT symbol, date,
               lag(total_mv) OVER (PARTITION BY symbol ORDER BY date) AS previous_market_cap
        FROM market_caps
        """
    )
    connection.read_csv(args.daily_o2c).create_view("o2c_raw")
    connection.execute(
        """
        CREATE VIEW o2c AS
        SELECT DISTINCT symbol, date, next_date, open, close, intraday_ret
        FROM o2c_raw
        """
    )
    connection.read_csv(args.daily_overnight).create_view("overnight")
    connection.execute(
        """
        CREATE VIEW daily_base AS
        SELECT i.symbol, i.date, i.open, i.intraday_ret, n.overnight_ret
        FROM o2c i JOIN overnight n USING (symbol, date)
        """
    )
    lead_columns = []
    lead_columns.extend(
        [
            "lead(open, 1) OVER w AS open_p1",
            "lead(open, 2) OVER w AS open_p2",
        ]
    )
    for offset in range(1, 6):
        lead_columns.extend(
            [
                f"lead(intraday_ret, {offset}) OVER w AS intraday_p{offset}",
                f"lead(overnight_ret, {offset}) OVER w AS overnight_p{offset}",
            ]
        )
    connection.execute(
        f"""
        CREATE VIEW daily_targets AS
        SELECT *, {', '.join(lead_columns)}
        FROM daily_base
        WINDOW w AS (PARTITION BY symbol ORDER BY date)
        """
    )

    grouped_daily: dict[tuple[str, ...], list[tuple[str, float, float, bool]]] = defaultdict(list)
    daily_query = """
        SELECT f.symbol, f.date, f.window_name,
               f.d01_trade_reversal, f.d02_trade_momentum,
               f.d03_positive_order_ts_extreme90, f.d03_positive_order_ts_extreme95,
               d.open, c.previous_market_cap, d.open_p1, d.open_p2,
               d.overnight_ret, d.intraday_p1, d.intraday_p2,
               d.intraday_p3, d.intraday_p4, d.intraday_p5,
               d.overnight_p1, d.overnight_p2, d.overnight_p3, d.overnight_p4
        FROM factors f
        JOIN daily_targets d USING (symbol, date)
        JOIN previous_caps c USING (symbol, date)
        WHERE f.frequency = 'daily' AND f.date BETWEEN ? AND ?
    """
    for row in connection.execute(daily_query, [int(args.date_from), int(args.date_to)]).fetchall():
        symbol, date, window_name = str(row[0]), str(row[1]), str(row[2])
        price, previous_cap = finite(row[7]), finite(row[8])
        if price is None or previous_cap is None:
            continue
        groups = domain(previous_cap, price, symbol)
        if groups is None:
            continue
        open_p1, open_p2 = finite(row[9]), finite(row[10])
        values = [finite(value) for value in row[11:]]
        overnight_d1, intraday_d1 = values[0], values[1]
        intraday_d2_d5 = compounded(values[2:6])
        overnight_d3_d5 = compounded(values[7:10])
        common = {
            "frequency": "daily",
            "window_name": window_name,
            "symbol": symbol,
            "date": date,
            "cap_group": groups[0],
            "price_group": groups[1],
        }
        factors = {
            "D01": finite(row[3]),
            "D02": finite(row[4]),
            "D03_90": finite(row[5]),
            "D03_95": finite(row[6]),
        }
        add_observations(
            grouped_daily,
            **common,
            factors={"D01": factors["D01"]},
            targets={
                "overnight_d1": overnight_d1,
                "intraday_d1": intraday_d1,
                "close_to_close_d1": compounded([overnight_d1, intraday_d1]),
                "open_to_open_d1": (
                    open_p2 / open_p1 - 1.0
                    if open_p1 is not None and open_p2 is not None and open_p1 > 0
                    else None
                ),
            },
        )
        open_to_open_d1 = (
            open_p2 / open_p1 - 1.0
            if open_p1 is not None and open_p2 is not None and open_p1 > 0
            else None
        )
        add_observations(
            grouped_daily,
            **common,
            factors={name: factors[name] for name in ("D03_90", "D03_95")},
            targets={"open_to_open_d1": open_to_open_d1},
        )
        medium_targets = {
            "intraday_d2": values[2],
            "intraday_d2_d5": intraday_d2_d5,
            "overnight_d3": values[7],
            "overnight_d3_d5": overnight_d3_d5,
        }
        add_observations(
            grouped_daily,
            **common,
            factors={name: factors[name] for name in ("D02", "D03_90", "D03_95")},
            targets=medium_targets,
        )

    connection.read_csv(args.intraday_returns).create_view("intraday")
    intraday_query = """
        SELECT f.symbol, f.date, f.window_name,
               f.d01_trade_reversal, f.d02_trade_momentum,
               f.d03_positive_order_ts_extreme90, f.d03_positive_order_ts_extreme95,
               f.normalizer_price, c.previous_market_cap,
               r.ret_1031_1035, r.ret_1031_1040, r.ret_1031_1045,
               r.ret_1031_1100, r.ret_1031_1457
        FROM factors f
        JOIN intraday r USING (symbol, date)
        JOIN previous_caps c USING (symbol, date)
        WHERE f.frequency = 'intraday' AND f.date BETWEEN ? AND ?
          AND r.is_st = 0 AND r.is_suspended = 0
    """
    grouped_intraday: dict[tuple[str, ...], list[tuple[str, float, float, bool]]] = defaultdict(list)
    for row in connection.execute(intraday_query, [int(args.date_from), int(args.date_to)]).fetchall():
        symbol, date, window_name = str(row[0]), str(row[1]), str(row[2])
        price, previous_cap = finite(row[7]), finite(row[8])
        if price is None or previous_cap is None:
            continue
        groups = domain(previous_cap, price, symbol)
        if groups is None:
            continue
        add_observations(
            grouped_intraday,
            frequency="intraday",
            window_name=window_name,
            symbol=symbol,
            date=date,
            cap_group=groups[0],
            price_group=groups[1],
            factors={
                "D01": finite(row[3]),
                "D02": finite(row[4]),
                "D03_90": finite(row[5]),
                "D03_95": finite(row[6]),
            },
            targets={
                "ret_1031_1035": finite(row[9]),
                "ret_1031_1040": finite(row[10]),
                "ret_1031_1045": finite(row[11]),
                "ret_1031_1100": finite(row[12]),
                "ret_1031_1457": finite(row[13]),
            },
        )

    output_dir = Path(args.output_dir)
    daily_detail = build_daily_rows(grouped_daily)
    intraday_detail = build_daily_rows(grouped_intraday)
    write_csv(output_dir / "daily_by_date.csv", daily_detail)
    write_csv(output_dir / "daily_summary.csv", summarize(daily_detail))
    write_csv(output_dir / "intraday_by_date.csv", intraday_detail)
    write_csv(output_dir / "intraday_summary.csv", summarize(intraday_detail))
    print(
        f"daily_rows={len(daily_detail)} intraday_rows={len(intraday_detail)} "
        f"output_dir={output_dir}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
