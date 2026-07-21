#!/usr/bin/env python3
"""Simple daily cross-sectional open-to-open backtest for the LOB factor."""

from __future__ import annotations

import argparse
import csv
import math
from collections import defaultdict
from pathlib import Path
from statistics import mean, stdev

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def parse_float(value: str) -> float | None:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(out):
        return None
    return out


def pearson(xs: list[float], ys: list[float]) -> float | None:
    if len(xs) < 3:
        return None
    mx = mean(xs)
    my = mean(ys)
    vx = sum((x - mx) ** 2 for x in xs)
    vy = sum((y - my) ** 2 for y in ys)
    if vx <= 0 or vy <= 0:
        return None
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / math.sqrt(vx * vy)


def ranks(values: list[float]) -> list[float]:
    order = sorted(range(len(values)), key=values.__getitem__)
    out = [0.0] * len(values)
    i = 0
    while i < len(order):
        j = i + 1
        while j < len(order) and values[order[j]] == values[order[i]]:
            j += 1
        rank = (i + j - 1) / 2.0 + 1.0
        for k in range(i, j):
            out[order[k]] = rank
        i = j
    return out


def load_returns(path: str, entry_lag_opens: int) -> dict[tuple[str, str], dict[str, str]]:
    raw = {}
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            ret = parse_float(row.get("o2o_ret", ""))
            if ret is None:
                continue
            raw[(row["symbol"], row["date"])] = {
                "ret": ret,
                "open": parse_float(row.get("open", "")),
                "next_date": row.get("next_date", ""),
            }

    if entry_lag_opens == 0:
        return raw
    if entry_lag_opens != 1:
        raise ValueError("only entry_lag_opens=0 or 1 is supported")

    rows = {}
    for (symbol, signal_date), row in raw.items():
        entry_date = row["next_date"]
        entry_row = raw.get((symbol, entry_date))
        if entry_row is None:
            continue
        rows[(symbol, signal_date)] = {
            "ret": entry_row["ret"],
            "entry_date": entry_date,
            "exit_date": entry_row["next_date"],
            "signal_open": row["open"],
        }
    return rows


def load_merged(factor_path: str, return_path: str, factor_col: str, entry_lag_opens: int):
    returns = load_returns(return_path, entry_lag_opens)
    by_date = defaultdict(list)
    with open(factor_path, newline="") as f:
        for row in csv.DictReader(f):
            key = (row["symbol"], row["date"])
            ret_row = returns.get(key)
            if ret_row is None:
                continue
            fac = parse_float(row.get(factor_col, ""))
            if fac is None:
                continue
            by_date[row["date"]].append(
                {
                    "symbol": row["symbol"],
                    "factor": fac,
                    "ret": ret_row["ret"],
                    "entry_date": ret_row.get("entry_date", row["date"]),
                    "exit_date": ret_row.get("exit_date", ret_row.get("next_date", "")),
                }
            )
    return dict(sorted(by_date.items()))


def summarize_returns(values: list[float]) -> dict[str, float]:
    n = len(values)
    avg = mean(values) if values else 0.0
    vol = stdev(values) if n > 1 else 0.0
    cum = math.prod(1.0 + x for x in values) - 1.0 if values else 0.0
    sharpe = avg / vol * math.sqrt(252.0) if vol > 0 else 0.0
    t_stat = avg / (vol / math.sqrt(n)) if vol > 0 and n > 1 else 0.0
    return {
        "n_days": n,
        "avg_daily_ret": avg,
        "cum_ret": cum,
        "ann_ret_simple": avg * 252.0,
        "ann_vol": vol * math.sqrt(252.0),
        "sharpe": sharpe,
        "t_stat": t_stat,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--factor",
        default=str(PROJECT_ROOT / "data/processed/active_take_midprice_ratio_v3_full_sorted.csv"),
    )
    parser.add_argument(
        "--returns",
        default=str(PROJECT_ROOT / "data/cache/daily_open_to_open_202601_20260213.csv"),
    )
    parser.add_argument("--factor-col", default="active_take_mid_gap_ratio")
    parser.add_argument(
        "--entry-lag-opens",
        type=int,
        default=1,
        choices=[0, 1],
        help="0 trades signal-date open to next open; 1 trades next open to following open.",
    )
    parser.add_argument(
        "--daily-out", default=str(PROJECT_ROOT / "results/daily/o2o_daily_deciles.csv")
    )
    parser.add_argument(
        "--summary-out", default=str(PROJECT_ROOT / "results/daily/o2o_summary.csv")
    )
    args = parser.parse_args()

    Path(args.daily_out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.summary_out).parent.mkdir(parents=True, exist_ok=True)

    by_date = load_merged(args.factor, args.returns, args.factor_col, args.entry_lag_opens)
    daily_rows = []
    long_short = []
    rank_ics = []
    pearson_ics = []

    for date, rows in by_date.items():
        rows = sorted(rows, key=lambda r: (r["factor"], r["symbol"]))
        n = len(rows)
        if n < 10:
            continue

        decile_rets = []
        for decile in range(10):
            lo = decile * n // 10
            hi = (decile + 1) * n // 10
            bucket = rows[lo:hi]
            decile_rets.append(mean(r["ret"] for r in bucket))

        factors = [r["factor"] for r in rows]
        rets = [r["ret"] for r in rows]
        pic = pearson(factors, rets)
        ric = pearson(ranks(factors), ranks(rets))
        if pic is not None:
            pearson_ics.append(pic)
        if ric is not None:
            rank_ics.append(ric)

        ls = decile_rets[-1] - decile_rets[0]
        long_short.append(ls)
        daily_rows.append(
            {
                "date": date,
                "entry_date": rows[0]["entry_date"],
                "exit_date": rows[0]["exit_date"],
                "n": n,
                **{f"d{i + 1}": decile_rets[i] for i in range(10)},
                "top_bottom": ls,
                "pearson_ic": pic if pic is not None else "",
                "rank_ic": ric if ric is not None else "",
            }
        )

    with open(args.daily_out, "w", newline="") as f:
        fields = ["date", "entry_date", "exit_date", "n"] + [f"d{i}" for i in range(1, 11)] + [
            "top_bottom",
            "pearson_ic",
            "rank_ic",
        ]
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(daily_rows)

    summary = summarize_returns(long_short)
    summary.update(
        {
            "n_obs": sum(row["n"] for row in daily_rows),
            "avg_names_per_day": mean(row["n"] for row in daily_rows) if daily_rows else 0.0,
            "avg_pearson_ic": mean(pearson_ics) if pearson_ics else 0.0,
            "avg_rank_ic": mean(rank_ics) if rank_ics else 0.0,
            "win_rate": mean(1.0 if x > 0 else 0.0 for x in long_short) if long_short else 0.0,
        }
    )

    with open(args.summary_out, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(summary))
        writer.writeheader()
        writer.writerow(summary)

    print(f"dates={summary['n_days']} obs={summary['n_obs']} avg_n={summary['avg_names_per_day']:.1f}")
    print(
        "top-bottom "
        f"avg={summary['avg_daily_ret']:.6%} cum={summary['cum_ret']:.6%} "
        f"sharpe={summary['sharpe']:.3f} t={summary['t_stat']:.3f} "
        f"rank_ic={summary['avg_rank_ic']:.6f}"
    )
    print(f"wrote {args.daily_out} and {args.summary_out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
