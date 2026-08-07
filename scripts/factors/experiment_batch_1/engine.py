"""Streaming primitives for the first unified intraday experiment batch.

The engine consumes V4 events in stored row order for [10:00, 10:30).  It
produces four independent caches from that single pass: stock-day signals,
active-order chains, quote-improvement lifecycles, and quality diagnostics.
No post-processed link-status field is accepted by this module.
"""

from __future__ import annotations

import math
import statistics
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field


FACTOR_VERSION = "experiment_batch_1_v2_safe_prebook_20260807"
HORIZONS = (5, 30, 60)


def clock_seconds(value: int) -> float:
    milliseconds = value % 1000
    seconds = (value // 1000) % 100
    minutes = (value // 100000) % 100
    hours = value // 10000000
    return hours * 3600.0 + minutes * 60.0 + seconds + milliseconds / 1000.0


@dataclass(frozen=True)
class Event:
    date: int
    time: int
    row_id: int
    action: str
    recid: int | None
    buy_order_id: int | None
    sell_order_id: int | None
    side: str | None
    price: int | None
    volume: int | None
    bid1: int | None
    ask1: int | None
    bid_depth1: int | None
    bid_depth3: int | None
    ask_depth1: int | None
    ask_depth3: int | None
    bid_count1: int | None
    ask_count1: int | None

    @property
    def mid(self) -> float | None:
        if self.bid1 is None or self.ask1 is None or self.bid1 <= 0 or self.ask1 <= self.bid1:
            return None
        return (self.bid1 + self.ask1) / 2.0

    @property
    def book_status(self) -> str:
        if self.bid1 is None or self.ask1 is None or self.bid1 <= 0 or self.ask1 <= 0:
            return "missing"
        if self.bid1 > self.ask1:
            return "crossed"
        if self.bid1 == self.ask1:
            return "locked"
        return "valid"


@dataclass
class Chain:
    side: str
    order_id: int
    first_seconds: float
    last_seconds: float
    trade_count: int = 0
    volume: int = 0
    notional: int = 0
    directional_impact_bps_sum: float = 0.0
    depth_loss: int = 0
    times: list[float] = field(default_factory=list)

    def add(
        self,
        seconds: float,
        volume: int,
        price: int | None,
        impact_bps: float | None,
        depth_loss: int | None,
    ) -> None:
        self.last_seconds = seconds
        self.trade_count += 1
        self.volume += volume
        self.notional += volume * int(price or 0)
        if impact_bps is not None:
            self.directional_impact_bps_sum += impact_bps
        if depth_loss is not None and depth_loss > 0:
            self.depth_loss += depth_loss
        self.times.append(seconds)

    def add_book_effect(
        self,
        directional_impact_bps: float | None,
        depth_loss: int | None,
    ) -> None:
        if directional_impact_bps is not None:
            self.directional_impact_bps_sum += directional_impact_bps
        if depth_loss is not None and depth_loss > 0:
            self.depth_loss += depth_loss

    @property
    def acceleration_seconds(self) -> float | None:
        if len(self.times) < 4:
            return None
        gaps = [right - left for left, right in zip(self.times, self.times[1:])]
        split = max(1, len(gaps) // 2)
        first = statistics.fmean(gaps[:split])
        second = statistics.fmean(gaps[-split:])
        return first - second


@dataclass
class PendingImpact:
    deadline: float
    before_mid: float
    immediate: float
    side: str


@dataclass
class QuoteLifecycle:
    side: str
    start_seconds: float
    start_row_id: int
    price: int
    quantity: int | None
    count: int | None
    prior_side_quantity: int | None
    prior_bid: int
    prior_ask: int
    relative_depth: float | None
    rehit: bool = False


@dataclass
class Quality:
    total_events: int = 0
    valid_books: int = 0
    invalid_books: int = 0
    missing_books: int = 0
    locked_books: int = 0
    crossed_books: int = 0
    trade_events: int = 0
    missing_active_order_id: int = 0
    impact_events: int = 0
    impact_censored_5s: int = 0
    impact_censored_30s: int = 0
    impact_censored_60s: int = 0
    quote_improvements: int = 0
    quote_censored: int = 0
    atomic_book_chains: int = 0
    atomic_impact_events: int = 0
    atomic_ambiguous_chains: int = 0
    unresolved_atomic_chains: int = 0


class DayState:
    def __init__(self, symbol: str, date: int) -> None:
        self.symbol = symbol
        self.date = date
        self.last_row_id: int | None = None
        self.previous_valid: Event | None = None
        self.atomic_prebook: Event | None = None
        self.atomic_start_seconds: float | None = None
        self.atomic_trade_keys: list[tuple[str, int | None]] = []
        self.quality = Quality()
        self.active_volume = defaultdict(int)
        self.active_count = defaultdict(int)
        self.chains: dict[tuple[str, int], Chain] = {}
        self.pending_impacts = {h: deque() for h in HORIZONS}
        self.impact_stats = {
            h: {"n": 0, "immediate": 0.0, "retained": 0.0, "reversed": 0}
            for h in HORIZONS
        }
        self.open_quotes: list[QuoteLifecycle] = []
        self.quote_rows: list[dict[str, object]] = []
        self.book_samples = 0
        self.spread_bps_sum = 0.0
        self.bid_depth1_sum = 0.0
        self.ask_depth1_sum = 0.0
        self.bid_depth3_sum = 0.0
        self.ask_depth3_sum = 0.0
        self.bid_count1_sum = 0.0
        self.ask_count1_sum = 0.0
        self.quote_improve_count = defaultdict(int)
        self.quote_improve_volume = defaultdict(int)

    def process(self, event: Event) -> None:
        if self.last_row_id is not None and event.row_id <= self.last_row_id:
            raise ValueError(
                f"non-increasing row_id {self.symbol} {self.date}: "
                f"{event.row_id} <= {self.last_row_id}"
            )
        self.last_row_id = event.row_id
        self.quality.total_events += 1
        current_mid = event.mid
        if current_mid is None:
            self.quality.invalid_books += 1
            setattr(
                self.quality,
                f"{event.book_status}_books",
                getattr(self.quality, f"{event.book_status}_books") + 1,
            )
        else:
            self.quality.valid_books += 1
            self._sample_book(event, current_mid)

        seconds = clock_seconds(event.time)
        self._resolve_impacts(seconds, current_mid)
        self._update_open_quotes(event, seconds)

        previous = self.previous_valid
        if current_mid is None:
            if previous is not None and self.atomic_prebook is None:
                self.atomic_prebook = previous
                self.atomic_start_seconds = seconds
            if previous is not None and event.action == "TRADE" and event.side in {"B", "S"}:
                self.atomic_trade_keys.append(self._process_trade(
                    previous, event, seconds, include_book_effect=False
                ))
            return

        if self.atomic_prebook is not None:
            if event.action == "TRADE" and event.side in {"B", "S"}:
                self.atomic_trade_keys.append(self._process_trade(
                    self.atomic_prebook, event, seconds, include_book_effect=False
                ))
            self._settle_atomic_chain(event, seconds)
        elif previous is not None:
            self._detect_quote_improvement(previous, event, seconds)
            if event.action == "TRADE" and event.side in {"B", "S"}:
                self._process_trade(previous, event, seconds)
        self.previous_valid = event

    def _sample_book(self, event: Event, mid: float) -> None:
        self.book_samples += 1
        self.spread_bps_sum += (event.ask1 - event.bid1) / mid * 10_000
        for value, name in (
            (event.bid_depth1, "bid_depth1_sum"),
            (event.ask_depth1, "ask_depth1_sum"),
            (event.bid_depth3, "bid_depth3_sum"),
            (event.ask_depth3, "ask_depth3_sum"),
            (event.bid_count1, "bid_count1_sum"),
            (event.ask_count1, "ask_count1_sum"),
        ):
            if value is not None:
                setattr(self, name, getattr(self, name) + value)

    def _resolve_impacts(self, seconds: float, current_mid: float | None) -> None:
        if current_mid is None:
            return
        for horizon, queue in self.pending_impacts.items():
            while queue and queue[0].deadline <= seconds:
                pending = queue.popleft()
                retained = current_mid - pending.before_mid
                directional_retained = retained if pending.side == "B" else -retained
                directional_immediate = pending.immediate if pending.side == "B" else -pending.immediate
                stat = self.impact_stats[horizon]
                stat["n"] += 1
                stat["immediate"] += directional_immediate
                stat["retained"] += directional_retained
                stat["reversed"] += int(
                    directional_immediate != 0 and directional_retained * directional_immediate < 0
                )

    def _update_open_quotes(self, event: Event, seconds: float) -> None:
        for quote in self.open_quotes:
            if event.action == "TRADE" and event.price == quote.price:
                quote.rehit = True
        if event.mid is None:
            return
        remaining: list[QuoteLifecycle] = []
        for quote in self.open_quotes:
            current_price = event.bid1 if quote.side == "B" else event.ask1
            if current_price == quote.price:
                remaining.append(quote)
                continue
            restored = event.bid1 == quote.prior_bid and event.ask1 == quote.prior_ask
            self.quote_rows.append(self._quote_row(
                quote,
                end_seconds=seconds,
                end_row_id=event.row_id,
                censored=False,
                restored=restored,
                removal_action=event.action,
            ))
        self.open_quotes = remaining

    def _detect_quote_improvement(self, previous: Event, event: Event, seconds: float) -> None:
        if event.action != "ORDER_ADD" or event.side not in {"B", "S"}:
            return
        if previous.mid is None or event.mid is None:
            return
        order_id = event.buy_order_id if event.side == "B" else event.sell_order_id
        if (
            self.symbol.startswith("SH")
            and order_id is not None
            and (event.side, int(order_id)) in self.chains
        ):
            # Shanghai publishes an aggressive order's unmatched remainder
            # after its immediate TRADE rows. It is not a new passive arrival.
            return
        if event.side == "B":
            improved = event.bid1 is not None and previous.bid1 is not None and event.bid1 > previous.bid1
            inside = event.bid1 is not None and previous.ask1 is not None and event.bid1 < previous.ask1
            price, quantity, count, prior_quantity = (
                event.bid1, event.bid_depth1, event.bid_count1, previous.bid_depth1
            )
        else:
            improved = event.ask1 is not None and previous.ask1 is not None and event.ask1 < previous.ask1
            inside = event.ask1 is not None and previous.bid1 is not None and event.ask1 > previous.bid1
            price, quantity, count, prior_quantity = (
                event.ask1, event.ask_depth1, event.ask_count1, previous.ask_depth1
            )
        if not improved or not inside or price is None:
            return
        relative = None
        if quantity is not None and prior_quantity is not None and prior_quantity > 0:
            relative = quantity / prior_quantity
        quote = QuoteLifecycle(
            side=event.side,
            start_seconds=seconds,
            start_row_id=event.row_id,
            price=price,
            quantity=quantity,
            count=count,
            prior_side_quantity=prior_quantity,
            prior_bid=int(previous.bid1),
            prior_ask=int(previous.ask1),
            relative_depth=relative,
        )
        self.open_quotes.append(quote)
        self.quality.quote_improvements += 1
        self.quote_improve_count[event.side] += 1
        self.quote_improve_volume[event.side] += int(quantity or 0)

    def _process_trade(
        self,
        previous: Event,
        event: Event,
        seconds: float,
        include_book_effect: bool = True,
    ) -> tuple[str, int | None]:
        self.quality.trade_events += 1
        volume = int(event.volume or 0)
        if volume <= 0:
            return str(event.side), None
        side = str(event.side)
        self.active_volume[side] += volume
        self.active_count[side] += 1
        order_id = event.buy_order_id if side == "B" else event.sell_order_id
        if order_id is None:
            self.quality.missing_active_order_id += 1
        before_mid = previous.mid if include_book_effect else None
        after_mid = event.mid if include_book_effect else None
        impact = after_mid - before_mid if before_mid is not None and after_mid is not None else None
        directional_bps = None
        if impact is not None and before_mid and before_mid > 0:
            directional_bps = (impact if side == "B" else -impact) / before_mid * 10_000
            self.quality.impact_events += 1
            for horizon in HORIZONS:
                self.pending_impacts[horizon].append(PendingImpact(
                    deadline=seconds + horizon,
                    before_mid=before_mid,
                    immediate=impact,
                    side=side,
                ))
        before_depth = (
            previous.ask_depth3 if side == "B" else previous.bid_depth3
        ) if include_book_effect else None
        after_depth = (
            event.ask_depth3 if side == "B" else event.bid_depth3
        ) if include_book_effect else None
        depth_loss = None
        if before_depth is not None and after_depth is not None:
            depth_loss = before_depth - after_depth
        if order_id is not None:
            key = (side, int(order_id))
            chain = self.chains.get(key)
            if chain is None:
                chain = Chain(side=side, order_id=int(order_id), first_seconds=seconds, last_seconds=seconds)
                self.chains[key] = chain
            chain.add(seconds, volume, event.price, directional_bps, depth_loss)
        return side, int(order_id) if order_id is not None else None

    def _settle_atomic_chain(self, event: Event, seconds: float) -> None:
        prebook = self.atomic_prebook
        if prebook is None:
            return
        self.quality.atomic_book_chains += 1
        sides = {side for side, _order_id in self.atomic_trade_keys}
        if len(sides) != 1 or not self.atomic_trade_keys:
            self.quality.atomic_ambiguous_chains += 1
            self._clear_atomic_chain()
            return
        side = next(iter(sides))
        before_mid = prebook.mid
        after_mid = event.mid
        if before_mid is None or after_mid is None or before_mid <= 0:
            self.quality.atomic_ambiguous_chains += 1
            self._clear_atomic_chain()
            return
        impact = after_mid - before_mid
        directional_bps = (impact if side == "B" else -impact) / before_mid * 10_000
        self.quality.impact_events += 1
        self.quality.atomic_impact_events += 1
        for horizon in HORIZONS:
            self.pending_impacts[horizon].append(PendingImpact(
                deadline=seconds + horizon,
                before_mid=before_mid,
                immediate=impact,
                side=side,
            ))
        before_depth = prebook.ask_depth3 if side == "B" else prebook.bid_depth3
        after_depth = event.ask_depth3 if side == "B" else event.bid_depth3
        depth_loss = None
        if before_depth is not None and after_depth is not None:
            depth_loss = before_depth - after_depth
        _last_side, last_order_id = self.atomic_trade_keys[-1]
        if last_order_id is not None:
            chain = self.chains.get((side, last_order_id))
            if chain is not None:
                chain.add_book_effect(directional_bps, depth_loss)
        self._clear_atomic_chain()

    def _clear_atomic_chain(self) -> None:
        self.atomic_prebook = None
        self.atomic_start_seconds = None
        self.atomic_trade_keys = []

    def _quote_row(
        self,
        quote: QuoteLifecycle,
        end_seconds: float,
        end_row_id: int | None,
        censored: bool,
        restored: bool,
        removal_action: str,
    ) -> dict[str, object]:
        return {
            "symbol": self.symbol,
            "date": self.date,
            "side": quote.side,
            "start_row_id": quote.start_row_id,
            "end_row_id": end_row_id,
            "quote_price": quote.price,
            "quote_quantity": quote.quantity,
            "quote_count": quote.count,
            "prior_side_quantity": quote.prior_side_quantity,
            "relative_depth": quote.relative_depth,
            "lifetime_seconds": max(0.0, end_seconds - quote.start_seconds),
            "rehit": int(quote.rehit),
            "restored_pre_event_book": int(restored),
            "censored_at_signal": int(censored),
            "removal_action": removal_action,
            "factor_version": FACTOR_VERSION,
        }

    def finish(self) -> tuple[dict[str, object], list[dict[str, object]], list[dict[str, object]], dict[str, object]]:
        signal_seconds = 10.5 * 3600
        if self.atomic_prebook is not None:
            self.quality.unresolved_atomic_chains += 1
            self._clear_atomic_chain()
        for quote in self.open_quotes:
            self.quote_rows.append(self._quote_row(
                quote,
                end_seconds=signal_seconds,
                end_row_id=None,
                censored=True,
                restored=False,
                removal_action="CENSORED",
            ))
            self.quality.quote_censored += 1
        for horizon, queue in self.pending_impacts.items():
            setattr(self.quality, f"impact_censored_{horizon}s", len(queue))

        chain_rows = [self._chain_row(chain) for chain in sorted(
            self.chains.values(), key=lambda item: (item.side, item.order_id)
        )]
        signal = self._signal_row(chain_rows)
        quality = {
            "symbol": self.symbol,
            "date": self.date,
            **asdict(self.quality),
            "factor_version": FACTOR_VERSION,
        }
        return signal, chain_rows, self.quote_rows, quality

    def _chain_row(self, chain: Chain) -> dict[str, object]:
        return {
            "symbol": self.symbol,
            "date": self.date,
            "side": chain.side,
            "active_order_id": chain.order_id,
            "first_seconds": chain.first_seconds,
            "last_seconds": chain.last_seconds,
            "duration_seconds": chain.last_seconds - chain.first_seconds,
            "trade_count": chain.trade_count,
            "volume": chain.volume,
            "notional": chain.notional,
            "directional_impact_bps_sum": chain.directional_impact_bps_sum,
            "depth_loss": chain.depth_loss,
            "acceleration_seconds": chain.acceleration_seconds,
            "factor_version": FACTOR_VERSION,
        }

    def _signal_row(self, chain_rows: list[dict[str, object]]) -> dict[str, object]:
        volumes = [int(row["volume"]) for row in chain_rows]
        total_chain_volume = sum(volumes)
        shares = [value / total_chain_volume for value in volumes] if total_chain_volume else []
        multi_volume = sum(
            int(row["volume"]) for row in chain_rows if int(row["trade_count"]) > 1
        )
        closed_quotes = [row for row in self.quote_rows if not row["censored_at_signal"]]
        relative_depths = [
            float(row["relative_depth"]) for row in self.quote_rows
            if row["relative_depth"] is not None
        ]
        divisor = self.book_samples or 1
        row: dict[str, object] = {
            "symbol": self.symbol,
            "date": self.date,
            "signal_time": "10:30:00",
            "window_start": "10:00:00",
            "window_end_exclusive": "10:30:00",
            "active_buy_volume": self.active_volume["B"],
            "active_sell_volume": self.active_volume["S"],
            "active_buy_count": self.active_count["B"],
            "active_sell_count": self.active_count["S"],
            "active_net_share": (
                (self.active_volume["B"] - self.active_volume["S"])
                / (self.active_volume["B"] + self.active_volume["S"])
                if self.active_volume["B"] + self.active_volume["S"] else None
            ),
            "chain_count": len(chain_rows),
            "multi_trade_chain_count": sum(int(row["trade_count"]) > 1 for row in chain_rows),
            "multi_trade_chain_volume_share": multi_volume / total_chain_volume if total_chain_volume else None,
            "chain_volume_hhi": sum(share * share for share in shares) if shares else None,
            "chain_volume_entropy": -sum(share * math.log(share) for share in shares if share > 0) if shares else None,
            "largest_chain_volume_share": max(shares) if shares else None,
            "mean_spread_bps": self.spread_bps_sum / divisor,
            "mean_bid_depth1": self.bid_depth1_sum / divisor,
            "mean_ask_depth1": self.ask_depth1_sum / divisor,
            "mean_bid_depth3": self.bid_depth3_sum / divisor,
            "mean_ask_depth3": self.ask_depth3_sum / divisor,
            "mean_bid_count1": self.bid_count1_sum / divisor,
            "mean_ask_count1": self.ask_count1_sum / divisor,
            "passive_improve_buy_count": self.quote_improve_count["B"],
            "passive_improve_sell_count": self.quote_improve_count["S"],
            "passive_improve_buy_volume": self.quote_improve_volume["B"],
            "passive_improve_sell_volume": self.quote_improve_volume["S"],
            "new_quote_count": len(self.quote_rows),
            "new_quote_relative_depth_mean": statistics.fmean(relative_depths) if relative_depths else None,
            "new_quote_thin_share_lt_0_5": (
                sum(value < 0.5 for value in relative_depths) / len(relative_depths)
                if relative_depths else None
            ),
            "new_quote_rehit_share": (
                sum(int(row["rehit"]) for row in self.quote_rows) / len(self.quote_rows)
                if self.quote_rows else None
            ),
            "new_quote_restored_share": (
                sum(int(row["restored_pre_event_book"]) for row in closed_quotes) / len(closed_quotes)
                if closed_quotes else None
            ),
            "new_quote_censored_count": self.quality.quote_censored,
            "factor_version": FACTOR_VERSION,
        }
        for horizon in HORIZONS:
            stat = self.impact_stats[horizon]
            n = int(stat["n"])
            row[f"impact_observations_{horizon}s"] = n
            row[f"directional_immediate_impact_mean_{horizon}s"] = stat["immediate"] / n if n else None
            row[f"directional_retained_impact_mean_{horizon}s"] = stat["retained"] / n if n else None
            row[f"impact_reversal_share_{horizon}s"] = stat["reversed"] / n if n else None
        return row


class BatchEngine:
    def __init__(self, symbol: str) -> None:
        self.symbol = symbol
        self.current: DayState | None = None
        self.signals: list[dict[str, object]] = []
        self.chains: list[dict[str, object]] = []
        self.quotes: list[dict[str, object]] = []
        self.quality: list[dict[str, object]] = []

    def process(self, event: Event) -> None:
        if self.current is None or event.date != self.current.date:
            self._flush()
            self.current = DayState(self.symbol, event.date)
        self.current.process(event)

    def _flush(self) -> None:
        if self.current is None:
            return
        signal, chains, quotes, quality = self.current.finish()
        self.signals.append(signal)
        self.chains.extend(chains)
        self.quotes.extend(quotes)
        self.quality.append(quality)

    def finish(self) -> tuple[list[dict[str, object]], list[dict[str, object]], list[dict[str, object]], list[dict[str, object]]]:
        self._flush()
        self.current = None
        return self.signals, self.chains, self.quotes, self.quality
