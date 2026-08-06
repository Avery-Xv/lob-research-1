#!/usr/bin/env python3
"""Freeze a deterministic nine-domain A-share sample for mechanism tests."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import defaultdict
from pathlib import Path
from typing import Iterable


CAP_GROUPS = ("cap_lt_50yi", "cap_50_500yi", "cap_ge_500yi")
PRICE_GROUPS = ("nonstar_lt_10", "nonstar_ge_10", "star_ge_10")
DOMAINS = tuple(f"{cap}/{price}" for cap in CAP_GROUPS for price in PRICE_GROUPS)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def classify_domain(market_cap: float, price: float, symbol: str) -> str | None:
    if market_cap < 500_000:
        cap = CAP_GROUPS[0]
    elif market_cap < 5_000_000:
        cap = CAP_GROUPS[1]
    else:
        cap = CAP_GROUPS[2]
    star = symbol.startswith(("SH688", "SH689"))
    if star:
        if price < 10:
            return None
        price_group = PRICE_GROUPS[2]
    else:
        price_group = PRICE_GROUPS[0] if price < 10 else PRICE_GROUPS[1]
    return f"{cap}/{price_group}"


def allocate_quotas(sample_size: int) -> dict[str, int]:
    if sample_size < len(DOMAINS):
        raise ValueError(f"sample_size must be at least {len(DOMAINS)}")
    base, remainder = divmod(sample_size, len(DOMAINS))
    return {
        domain: base + int(index < remainder)
        for index, domain in enumerate(DOMAINS)
    }


def deterministic_order(symbols: Iterable[str], seed: str) -> list[str]:
    return sorted(
        symbols,
        key=lambda symbol: (
            hashlib.sha256(f"{seed}|{symbol}".encode()).hexdigest(), symbol
        ),
    )


def select_domain_symbols(symbols: list[str], quota: int, domain: str) -> list[str]:
    if domain.endswith("star_ge_10"):
        ordered = deterministic_order(symbols, domain)
        if len(ordered) < quota:
            raise ValueError(f"domain {domain} has {len(ordered)} candidates for quota {quota}")
        return ordered[:quota]
    by_exchange = {
        exchange: deterministic_order(
            (symbol for symbol in symbols if symbol.startswith(exchange)), domain
        )
        for exchange in ("SH", "SZ")
    }
    sh_quota = quota // 2 + int(
        quota % 2 == 1
        and int(hashlib.sha256(domain.encode()).hexdigest(), 16) % 2 == 0
    )
    targets = {"SH": sh_quota, "SZ": quota - sh_quota}
    selected = [
        symbol
        for exchange in ("SH", "SZ")
        for symbol in by_exchange[exchange][:targets[exchange]]
    ]
    if len(selected) < quota:
        remainder = deterministic_order(
            (symbol for symbol in symbols if symbol not in selected), f"{domain}|fill"
        )
        selected.extend(remainder[:quota - len(selected)])
    if len(selected) != quota:
        raise ValueError(f"domain {domain} has insufficient candidates")
    return sorted(selected)


def load_complete_lob_paths(
    path: Path, months: set[str]
) -> tuple[dict[str, dict[str, str]], list[str]]:
    by_symbol: dict[str, dict[str, str]] = defaultdict(dict)
    lines = [line.strip() for line in path.read_text().splitlines() if line.strip()]
    for raw_path in lines:
        item = Path(raw_path)
        month = item.parent.name
        symbol = item.stem
        if month in months:
            by_symbol[symbol][month] = raw_path
    complete = {
        symbol: paths for symbol, paths in by_symbol.items()
        if set(paths) == months
    }
    return complete, lines


def load_numeric_csv(path: Path, field: str) -> dict[tuple[str, int], float]:
    result: dict[tuple[str, int], float] = {}
    with path.open(newline="") as handle:
        for row in csv.DictReader(handle):
            value = row.get(field)
            if value not in (None, ""):
                result[(row["symbol"], int(row["date"]))] = float(value)
    return result


def previous_values(
    symbol: str,
    dates: list[int],
    market_caps: dict[tuple[str, int], float],
    closes: dict[tuple[str, int], float],
) -> dict[int, tuple[float, float]]:
    available = sorted(
        date for candidate, date in market_caps
        if candidate == symbol and (symbol, date) in closes
    )
    output: dict[int, tuple[float, float]] = {}
    for date in dates:
        earlier = [candidate for candidate in available if candidate < date]
        if earlier:
            previous = earlier[-1]
            output[date] = (
                market_caps[(symbol, previous)], closes[(symbol, previous)]
            )
    return output


def atomic_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(text)
    temporary.replace(path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--full-file-list", type=Path, required=True)
    parser.add_argument("--universe-metadata", type=Path, required=True)
    parser.add_argument("--market-caps", type=Path, required=True)
    parser.add_argument("--controls", type=Path, required=True)
    parser.add_argument("--months", nargs="+", required=True)
    parser.add_argument("--target-month", required=True)
    parser.add_argument("--asof-date", type=int, required=True)
    parser.add_argument("--sample-size", type=int, default=300)
    parser.add_argument("--output-file-list", type=Path, required=True)
    parser.add_argument("--output-domain-file", type=Path, required=True)
    parser.add_argument("--output-metadata", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    universe = json.loads(args.universe_metadata.read_text())
    if universe.get("output_etf_symbols") != 0:
        raise ValueError("universe metadata does not certify zero ETF symbols")
    months = set(args.months)
    complete, original_lines = load_complete_lob_paths(args.full_file_list, months)
    market_caps = load_numeric_csv(args.market_caps, "total_mv")
    closes = load_numeric_csv(args.controls, "close")
    candidates: dict[str, list[str]] = defaultdict(list)
    for symbol in sorted(complete):
        key = (symbol, args.asof_date)
        if key not in market_caps or key not in closes:
            continue
        domain = classify_domain(market_caps[key], closes[key], symbol)
        if domain is not None:
            candidates[domain].append(symbol)
    quotas = allocate_quotas(args.sample_size)
    selected_by_domain = {
        domain: select_domain_symbols(candidates[domain], quotas[domain], domain)
        for domain in DOMAINS
    }
    selected = sorted(
        symbol for symbols in selected_by_domain.values() for symbol in symbols
    )
    if len(selected) != args.sample_size or len(set(selected)) != args.sample_size:
        raise ValueError("sample selection is not unique or complete")
    selected_set = set(selected)
    selected_lines = [
        line for line in original_lines
        if Path(line).stem in selected_set and Path(line).parent.name in months
    ]
    if len(selected_lines) != args.sample_size * len(months):
        raise ValueError("selected file list does not contain every required stock-month")

    target_dates = sorted({
        date for symbol, date in closes
        if symbol in selected_set and str(date).startswith(args.target_month)
    })
    domain_rows: list[tuple[str, int, str]] = []
    missing_dynamic: list[tuple[str, int]] = []
    for symbol in selected:
        values = previous_values(symbol, target_dates, market_caps, closes)
        for date in target_dates:
            if date not in values:
                missing_dynamic.append((symbol, date))
                continue
            cap, price = values[date]
            domain = classify_domain(cap, price, symbol)
            if domain is not None:
                domain_rows.append((symbol, date, domain))
    if missing_dynamic:
        raise ValueError(f"missing previous-day domain inputs: {missing_dynamic[:5]}")

    atomic_text(args.output_file_list, "\n".join(selected_lines) + "\n")
    domain_text = "symbol,date,domain\n" + "".join(
        f"{symbol},{date},{domain}\n" for symbol, date, domain in domain_rows
    )
    atomic_text(args.output_domain_file, domain_text)
    metadata = {
        **universe,
        "sample_size": args.sample_size,
        "asof_date": args.asof_date,
        "months": sorted(months),
        "target_month": args.target_month,
        "selection_rule": (
            "nine frozen as-of domains; deterministic SHA256 within domain; "
            "non-STAR exchange-balanced; complete required stock-months"
        ),
        "domain_rule": (
            "previous-trading-day total_mv and close; cap <50yi/50-500yi/>=500yi; "
            "non-STAR <10/>=10; STAR >=10; STAR <10 excluded"
        ),
        "universe_rule": universe.get("universe_rule"),
        "output_etf_symbols": 0,
        "quotas": quotas,
        "candidate_counts": {domain: len(candidates[domain]) for domain in DOMAINS},
        "selected_symbols": selected_by_domain,
        "exchange_counts": {
            exchange: sum(symbol.startswith(exchange) for symbol in selected)
            for exchange in ("SH", "SZ")
        },
        "target_dates": target_dates,
        "dynamic_domain_rows": len(domain_rows),
        "inputs": {
            "full_file_list": str(args.full_file_list.resolve()),
            "full_file_list_sha256": sha256_file(args.full_file_list),
            "universe_metadata": str(args.universe_metadata.resolve()),
            "universe_metadata_sha256": sha256_file(args.universe_metadata),
            "market_caps": str(args.market_caps.resolve()),
            "market_caps_sha256": sha256_file(args.market_caps),
            "controls": str(args.controls.resolve()),
            "controls_sha256": sha256_file(args.controls),
        },
    }
    atomic_text(
        args.output_metadata,
        json.dumps(metadata, ensure_ascii=False, indent=2) + "\n",
    )
    print(
        f"selected={len(selected)} files={len(selected_lines)} "
        f"domain_rows={len(domain_rows)} exchange_counts={metadata['exchange_counts']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
