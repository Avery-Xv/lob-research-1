"""Streaming 10:00-10:30 book-path and active-flow cache for R016/R017."""

from __future__ import annotations

import math
from dataclasses import dataclass, field

from scripts.factors.order_shape_mechanism.engine import Event, hhmmssmmm_to_seconds, session_name


FACTOR_VERSION = "non_parent_window_path_1000_1030_v1_20260810"
WINDOW_START = 10 * 3600.0
RECENT_START = 10 * 3600.0 + 25 * 60.0
LEGACY_START = 10 * 3600.0 + 29 * 60.0
SIGNAL_SECONDS = 10.5 * 3600.0
HORIZONS = (60, 300, 600)


@dataclass
class Book:
    bid: float
    ask: float
    bid1: float
    bid3: float
    bid10: float
    ask1: float
    ask3: float
    ask10: float

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2.0

    def imbalance(self, depth: int) -> float:
        bid = getattr(self, f"bid{depth}")
        ask = getattr(self, f"ask{depth}")
        total = bid + ask
        return (bid - ask) / total if total > 0 else 0.0


@dataclass
class WeightedBook:
    seconds: float = 0.0
    bi1_sum: float = 0.0
    bi3_sum: float = 0.0
    bi3_sq_sum: float = 0.0
    bi10_sum: float = 0.0
    spread_sum: float = 0.0
    bid1_sum: float = 0.0
    bid3_sum: float = 0.0
    bid10_sum: float = 0.0
    ask1_sum: float = 0.0
    ask3_sum: float = 0.0
    ask10_sum: float = 0.0
    positive_seconds: float = 0.0
    negative_seconds: float = 0.0

    def add(self, book: Book, seconds: float) -> None:
        if seconds <= 0:
            return
        bi1 = book.imbalance(1)
        bi3 = book.imbalance(3)
        bi10 = book.imbalance(10)
        self.seconds += seconds
        self.bi1_sum += bi1 * seconds
        self.bi3_sum += bi3 * seconds
        self.bi3_sq_sum += bi3 * bi3 * seconds
        self.bi10_sum += bi10 * seconds
        self.spread_sum += (book.ask - book.bid) / book.mid * 10_000.0 * seconds
        for name in ("bid1", "bid3", "bid10", "ask1", "ask3", "ask10"):
            setattr(self, f"{name}_sum", getattr(self, f"{name}_sum") + getattr(book, name) * seconds)
        if bi3 > 0:
            self.positive_seconds += seconds
        elif bi3 < 0:
            self.negative_seconds += seconds

    def values(self, prefix: str, expected_seconds: float) -> dict[str, float]:
        denominator = self.seconds
        mean = self.bi3_sum / denominator if denominator else 0.0
        variance = self.bi3_sq_sum / denominator - mean * mean if denominator else 0.0
        values = {
            f"{prefix}_coverage_seconds": self.seconds,
            f"{prefix}_coverage_ratio": self.seconds / expected_seconds,
            f"{prefix}_bi1_twap": self.bi1_sum / denominator if denominator else 0.0,
            f"{prefix}_bi3_twap": mean,
            f"{prefix}_bi3_time_std": math.sqrt(max(0.0, variance)),
            f"{prefix}_bi10_twap": self.bi10_sum / denominator if denominator else 0.0,
            f"{prefix}_spread_bps_twap": self.spread_sum / denominator if denominator else 0.0,
            f"{prefix}_positive_time_share": self.positive_seconds / denominator if denominator else 0.0,
            f"{prefix}_negative_time_share": self.negative_seconds / denominator if denominator else 0.0,
        }
        for name in ("bid1", "bid3", "bid10", "ask1", "ask3", "ask10"):
            values[f"{prefix}_{name}_twap"] = getattr(self, f"{name}_sum") / denominator if denominator else 0.0
        total1 = values[f"{prefix}_bid1_twap"] + values[f"{prefix}_ask1_twap"]
        total3 = values[f"{prefix}_bid3_twap"] + values[f"{prefix}_ask3_twap"]
        total10 = values[f"{prefix}_bid10_twap"] + values[f"{prefix}_ask10_twap"]
        values[f"{prefix}_depth1_to_depth3"] = total1 / total3 if total3 > 0 else 0.0
        values[f"{prefix}_depth3_to_depth10"] = total3 / total10 if total10 > 0 else 0.0
        return values


@dataclass
class Flow:
    buy_volume: float = 0.0
    sell_volume: float = 0.0
    buy_orders: set[tuple[str, int]] = field(default_factory=set)
    sell_orders: set[tuple[str, int]] = field(default_factory=set)

    def add(self, side: str, order_id: int, volume: float) -> None:
        if side == "B":
            self.buy_volume += volume
            self.buy_orders.add((side, order_id))
        else:
            self.sell_volume += volume
            self.sell_orders.add((side, order_id))

    def values(self, prefix: str) -> dict[str, float | int]:
        total = self.buy_volume + self.sell_volume
        return {
            f"{prefix}_buy_volume": self.buy_volume,
            f"{prefix}_sell_volume": self.sell_volume,
            f"{prefix}_net_share": (self.buy_volume - self.sell_volume) / total if total > 0 else 0.0,
            f"{prefix}_total_volume": total,
            f"{prefix}_buy_order_count": len(self.buy_orders),
            f"{prefix}_sell_order_count": len(self.sell_orders),
            f"{prefix}_order_count": len(self.buy_orders) + len(self.sell_orders),
        }


@dataclass
class WindowPathQuality:
    total_events: int = 0
    duplicate_trades: int = 0
    missing_active_order_id: int = 0
    missing_book_rows: int = 0
    locked_book_rows: int = 0
    crossed_book_rows: int = 0
    invalid_chain_seconds: float = 0.0
    book_sign_flips: int = 0


class WindowPathEngine:
    """Consume one symbol in stored row order; V4 snapshots are post-event."""

    def __init__(self, symbol: str, target_month: str = "202601") -> None:
        self.symbol = symbol
        self.target_month = int(target_month)
        self.current_date: int | None = None
        self.previous_row_id: int | None = None
        self.last_event_seconds: float | None = None
        self.last_valid_book: Book | None = None
        self.invalid_chain_start: float | None = None
        self.last_nonzero_sign = 0
        self.current_run_sign = 0
        self.current_run_start: float | None = None
        self.longest_positive = 0.0
        self.longest_negative = 0.0
        self.seen_trade_recids: set[int] = set()
        self.book30 = WeightedBook()
        self.book5 = WeightedBook()
        self.book_bins = [WeightedBook() for _ in range(6)]
        self.flow30 = Flow()
        self.flow5 = Flow()
        self.flow1 = Flow()
        self.flow_bins = [Flow() for _ in range(6)]
        self.future_flows = {horizon: Flow() for horizon in HORIZONS}
        self.future_events = {horizon: 0 for horizon in HORIZONS}
        self.future_mid_sq = {horizon: 0.0 for horizon in HORIZONS}
        self.future_end_book: dict[int, Book | None] = {horizon: None for horizon in HORIZONS}
        self.label_last_mid: float | None = None
        self.signal_captured = False
        self.signal_row: dict[str, object] | None = None
        self.quality = WindowPathQuality()
        self.rows: list[dict[str, object]] = []
        self.quality_rows: list[dict[str, object]] = []

    def process(self, event: Event) -> None:
        if session_name(event.time) != "AM":
            return
        seconds = hhmmssmmm_to_seconds(event.time)
        if self.current_date != event.date:
            self._finish_day()
            self._start_day(event.date)
        if self.previous_row_id is not None and event.row_id <= self.previous_row_id:
            raise ValueError(f"non-increasing row_id for {self.symbol} {event.date}")
        self.previous_row_id = event.row_id
        self._advance_book(seconds)
        self._capture_boundaries(seconds)
        self.quality.total_events += 1
        self._record_event(event, seconds)
        book, invalid_class = self._parse_book(event)
        if book is None:
            if WINDOW_START <= seconds < SIGNAL_SECONDS:
                setattr(self.quality, f"{invalid_class}_book_rows", getattr(self.quality, f"{invalid_class}_book_rows") + 1)
                if self.invalid_chain_start is None:
                    self.invalid_chain_start = seconds
        else:
            if self.invalid_chain_start is not None:
                self.quality.invalid_chain_seconds += max(
                    0.0, min(seconds, SIGNAL_SECONDS) - self.invalid_chain_start
                )
                self.invalid_chain_start = None
            if SIGNAL_SECONDS <= seconds < SIGNAL_SECONDS + max(HORIZONS):
                if self.label_last_mid is not None and self.label_last_mid > 0:
                    move_sq = math.log(book.mid / self.label_last_mid) ** 2
                    for horizon in HORIZONS:
                        if seconds < SIGNAL_SECONDS + horizon:
                            self.future_mid_sq[horizon] += move_sq
                self.label_last_mid = book.mid
            self.last_valid_book = book
        self.last_event_seconds = seconds

    def finish(self) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
        self._finish_day()
        return self.rows, self.quality_rows

    def _start_day(self, date: int) -> None:
        self.current_date = date
        self.previous_row_id = None
        self.last_event_seconds = None
        self.last_valid_book = None
        self.invalid_chain_start = None
        self.last_nonzero_sign = 0
        self.current_run_sign = 0
        self.current_run_start = None
        self.longest_positive = self.longest_negative = 0.0
        self.seen_trade_recids = set()
        self.book30 = WeightedBook(); self.book5 = WeightedBook()
        self.book_bins = [WeightedBook() for _ in range(6)]
        self.flow30 = Flow(); self.flow5 = Flow(); self.flow1 = Flow()
        self.flow_bins = [Flow() for _ in range(6)]
        self.future_flows = {horizon: Flow() for horizon in HORIZONS}
        self.future_events = {horizon: 0 for horizon in HORIZONS}
        self.future_mid_sq = {horizon: 0.0 for horizon in HORIZONS}
        self.future_end_book = {horizon: None for horizon in HORIZONS}
        self.label_last_mid = None
        self.signal_captured = False
        self.signal_row = None
        self.quality = WindowPathQuality()

    def _finish_day(self) -> None:
        if self.current_date is None:
            return
        self._advance_book(SIGNAL_SECONDS)
        self._capture_boundaries(SIGNAL_SECONDS + max(HORIZONS))
        if self.signal_row is not None:
            self._finish_run(SIGNAL_SECONDS)
            self.signal_row["invalid_chain_seconds"] = self.quality.invalid_chain_seconds + (
                max(0.0, SIGNAL_SECONDS - self.invalid_chain_start)
                if self.invalid_chain_start is not None and self.invalid_chain_start < SIGNAL_SECONDS else 0.0
            )
            self.signal_row["book_sign_flips"] = self.quality.book_sign_flips
            self.signal_row["longest_positive_seconds"] = self.longest_positive
            self.signal_row["longest_negative_seconds"] = self.longest_negative
            self.signal_row["factor_version"] = FACTOR_VERSION
            self.rows.append(self.signal_row)
        self.quality_rows.append({
            "symbol": self.symbol, "date": self.current_date,
            **self.quality.__dict__, "factor_version": FACTOR_VERSION,
        })
        self.current_date = None

    def _advance_book(self, seconds: float) -> None:
        if self.last_event_seconds is None or self.last_valid_book is None:
            return
        start = max(self.last_event_seconds, WINDOW_START)
        end = min(seconds, SIGNAL_SECONDS)
        if end <= start:
            return
        self._add_interval(self.book30, self.last_valid_book, start, end)
        recent_start = max(start, RECENT_START)
        if end > recent_start:
            self._add_interval(self.book5, self.last_valid_book, recent_start, end)
        for index in range(6):
            bin_start = WINDOW_START + index * 300.0
            bin_end = bin_start + 300.0
            overlap_start = max(start, bin_start)
            overlap_end = min(end, bin_end)
            if overlap_end > overlap_start:
                self.book_bins[index].add(self.last_valid_book, overlap_end - overlap_start)

    def _add_interval(self, accumulator: WeightedBook, book: Book, start: float, end: float) -> None:
        accumulator.add(book, end - start)
        sign = 1 if book.imbalance(3) > 0 else -1 if book.imbalance(3) < 0 else 0
        if sign and sign != self.current_run_sign:
            self._finish_run(start)
            if self.last_nonzero_sign and sign != self.last_nonzero_sign:
                self.quality.book_sign_flips += 1
            self.current_run_sign = sign
            self.current_run_start = start
            self.last_nonzero_sign = sign

    def _finish_run(self, end: float) -> None:
        if self.current_run_start is None:
            return
        duration = max(0.0, end - self.current_run_start)
        if self.current_run_sign > 0:
            self.longest_positive = max(self.longest_positive, duration)
        elif self.current_run_sign < 0:
            self.longest_negative = max(self.longest_negative, duration)
        self.current_run_start = None

    def _capture_boundaries(self, seconds: float) -> None:
        if not self.signal_captured and seconds >= SIGNAL_SECONDS:
            self.signal_captured = True
            if self.current_date is not None and self.current_date // 100 == self.target_month and self.last_valid_book is not None:
                endpoint = self.last_valid_book
                row: dict[str, object] = {
                    "symbol": self.symbol, "date": self.current_date, "signal_time": 1030,
                    **self.book30.values("book30m", 1800.0),
                    **self.book5.values("book5m", 300.0),
                    **self.flow30.values("flow30m"),
                    **self.flow5.values("flow5m"),
                    **self.flow1.values("flow1m"),
                    "endpoint_bi3": endpoint.imbalance(3),
                    "endpoint_spread_bps": (endpoint.ask - endpoint.bid) / endpoint.mid * 10_000.0,
                    "endpoint_bid_depth3": endpoint.bid3,
                    "endpoint_ask_depth3": endpoint.ask3,
                }
                for index, (book, flow) in enumerate(zip(self.book_bins, self.flow_bins), start=1):
                    row.update(book.values(f"bin{index}_book", 300.0))
                    row.update(flow.values(f"bin{index}_flow"))
                row["book_shift_5m_minus_30m"] = row["book5m_bi3_twap"] - row["book30m_bi3_twap"]
                row["flow_shift_5m_minus_30m"] = row["flow5m_net_share"] - row["flow30m_net_share"]
                row["endpoint_minus_book5m"] = row["endpoint_bi3"] - row["book5m_bi3_twap"]
                self.signal_row = row
                self.label_last_mid = endpoint.mid
        if self.signal_row is None:
            return
        for horizon in HORIZONS:
            if self.future_end_book[horizon] is None and seconds >= SIGNAL_SECONDS + horizon:
                self.future_end_book[horizon] = self.last_valid_book
                self.signal_row.update(self.future_flows[horizon].values(f"future{horizon // 60}m"))
                self.signal_row[f"future{horizon // 60}m_event_count"] = self.future_events[horizon]
                self.signal_row[f"future{horizon // 60}m_realized_vol_bps"] = math.sqrt(self.future_mid_sq[horizon]) * 10_000.0
                book = self.last_valid_book
                self.signal_row[f"future{horizon // 60}m_end_bi3"] = book.imbalance(3) if book else 0.0
                self.signal_row[f"future{horizon // 60}m_end_spread_bps"] = (
                    (book.ask - book.bid) / book.mid * 10_000.0 if book else 0.0
                )

    def _record_event(self, event: Event, seconds: float) -> None:
        if SIGNAL_SECONDS <= seconds < SIGNAL_SECONDS + max(HORIZONS):
            for horizon in HORIZONS:
                if seconds < SIGNAL_SECONDS + horizon:
                    self.future_events[horizon] += 1
        if event.action != "TRADE" or event.side not in ("B", "S") or (event.volume or 0) <= 0:
            return
        if event.recid is not None:
            if event.recid in self.seen_trade_recids:
                self.quality.duplicate_trades += 1
                return
            self.seen_trade_recids.add(event.recid)
        order_id = event.buy_order_id if event.side == "B" else event.sell_order_id
        if order_id is None:
            self.quality.missing_active_order_id += 1
            order_id = -(event.recid if event.recid is not None else event.row_id) - 1
        side = str(event.side); volume = float(event.volume or 0)
        if WINDOW_START <= seconds < SIGNAL_SECONDS:
            self.flow30.add(side, int(order_id), volume)
            index = min(5, int((seconds - WINDOW_START) // 300.0))
            self.flow_bins[index].add(side, int(order_id), volume)
            if seconds >= RECENT_START:
                self.flow5.add(side, int(order_id), volume)
            if seconds >= LEGACY_START:
                self.flow1.add(side, int(order_id), volume)
        elif SIGNAL_SECONDS <= seconds < SIGNAL_SECONDS + max(HORIZONS):
            for horizon in HORIZONS:
                if seconds < SIGNAL_SECONDS + horizon:
                    self.future_flows[horizon].add(side, int(order_id), volume)

    @staticmethod
    def _parse_book(event: Event) -> tuple[Book | None, str]:
        bid = event.bid1; ask = event.ask1
        depths = (*event.bid_depths, *event.ask_depths)
        if bid is None or ask is None or bid <= 0 or ask <= 0 or any(value is None or value < 0 for value in depths):
            return None, "missing"
        if bid == ask:
            return None, "locked"
        if bid > ask:
            return None, "crossed"
        return Book(float(bid), float(ask), *(float(value) for value in depths)), "valid"
