"""Leakage-safe fixed-grid Batch A intraday mechanism factor engine."""

from __future__ import annotations

import math
from collections import defaultdict, deque
from dataclasses import dataclass

from scripts.factors.order_shape_mechanism.engine import Event, Reservoir, hhmmssmmm_to_seconds, session_name, time_bucket


FACTOR_VERSION = "order_shape_batch_a_sh_remainder_v2_20260807"
SIDES = ("B", "S")


def signal_grid_seconds() -> tuple[float, ...]:
    morning = tuple(9 * 3600 + minute * 60 for minute in range(40, 81, 10))
    morning += tuple(10 * 3600 + minute * 60 for minute in range(0, 61, 10))
    morning += (11 * 3600 + 10 * 60, 11 * 3600 + 20 * 60)
    # Remove duplicates introduced by the compact construction above.
    morning = tuple(sorted(set(morning)))
    afternoon = tuple(13 * 3600 + minute * 60 for minute in range(10, 51, 10))
    afternoon += tuple(14 * 3600 + minute * 60 for minute in range(0, 41, 10))
    return tuple(sorted(set(morning + afternoon)))


SIGNAL_GRID_SECONDS = signal_grid_seconds()


@dataclass(frozen=True)
class BatchAConfig:
    target_month: str = "202601"
    observation_seconds: float = 60.0
    label_seconds: float = 600.0
    intensity_reservoir_size: int = 5_000
    minimum_fill_history: int = 100
    near_bps: float = 10.0

    def __post_init__(self) -> None:
        if len(self.target_month) != 6 or not self.target_month.isdigit():
            raise ValueError("target_month must be YYYYMM")
        if self.observation_seconds <= 0 or self.label_seconds <= 0:
            raise ValueError("observation and label windows must be positive")


@dataclass
class BatchAQuality:
    total_events: int = 0
    valid_trades: int = 0
    duplicate_trades: int = 0
    missing_active_order_id: int = 0
    candidate_passive_orders: int = 0
    model_passive_orders: int = 0
    excluded_active_orders: int = 0
    quote_active_remainders_excluded: int = 0
    fill_over_submit: int = 0
    scheduled_target_signals: int = 0
    completed_target_signals: int = 0
    incomplete_target_signals: int = 0
    signals_missing_book: int = 0
    signals_missing_history: int = 0


@dataclass
class RollingEvent:
    seconds: float
    buy_volume: float = 0.0
    sell_volume: float = 0.0
    buy_count: int = 0
    sell_count: int = 0
    aggressive_add_buy: float = 0.0
    aggressive_add_sell: float = 0.0
    near_cancel_buy: float = 0.0
    near_cancel_sell: float = 0.0


@dataclass
class ChainSummary:
    seconds: float
    side: str
    volume: float
    children: int


@dataclass
class ActiveChain:
    side: str
    active_order_id: int
    volume: float
    children: int
    last_seconds: float


@dataclass
class PassiveOrder:
    side: str
    order_id: int
    submit_qty: int
    submit_seconds: float
    state: str
    distance: str
    filled_60s: int = 0


@dataclass
class PendingSignal:
    end_seconds: float
    row: dict[str, object]
    start_buy_volume: float
    start_sell_volume: float
    start_buy_count: int
    start_sell_count: int
    start_events: int
    start_mid_sq: float
    start_mid_moves: int


class BatchAEngine:
    """Process all months for one symbol in chronological event order."""

    def __init__(self, symbol: str, config: BatchAConfig | None = None) -> None:
        self.symbol = symbol
        self.config = config or BatchAConfig()
        self.target_month = int(self.config.target_month)
        self.current_date: int | None = None
        self.current_session: str | None = None
        self.previous_row_id: int | None = None
        self.seen_trade_recids: set[int] = set()
        self.previous_book: tuple[float, float, float, float] | None = None
        self.last_mid: float | None = None
        self.rolling_events: deque[RollingEvent] = deque()
        self.rolling_chains: deque[ChainSummary] = deque()
        self.rolling_sums: dict[str, float] = defaultdict(float)
        self.chain_sums: dict[str, float] = defaultdict(float)
        self.chain: ActiveChain | None = None
        self.orders: dict[tuple[str, int], PassiveOrder] = {}
        self.active_order_keys: set[tuple[str, int]] = set()
        self.pending: deque[PendingSignal] = deque()
        self.next_grid = 0
        self.buy_volume = 0.0
        self.sell_volume = 0.0
        self.buy_count = 0
        self.sell_count = 0
        self.event_count = 0
        self.mid_sq = 0.0
        self.mid_moves = 0
        self.profile_reservoirs: dict[tuple[str, str], Reservoir] = {}
        self.day_profile_samples: list[tuple[str, float, float]] = []
        self.day_thresholds: dict[tuple[str, str], float | None] = {}
        self.fill_cells: dict[tuple[str, str, str], list[int]] = defaultdict(lambda: [0, 0])
        self.fill_globals: dict[tuple[str, str], list[int]] = defaultdict(lambda: [0, 0])
        self.quality = BatchAQuality()
        self.signal_rows: list[dict[str, object]] = []
        self.quality_rows: list[dict[str, object]] = []

    def process(self, event: Event) -> None:
        session = session_name(event.time)
        if session is None:
            return
        if self.current_date != event.date:
            self._finish_day()
            self._start_day(event.date)
        if self.previous_row_id is not None and event.row_id <= self.previous_row_id:
            raise ValueError(f"non-increasing row_id for {self.symbol} {event.date}")
        self.previous_row_id = event.row_id
        if self.current_session != session:
            self._finish_session()
            self._start_session(session)

        seconds = hhmmssmmm_to_seconds(event.time)
        self._schedule_grids_before(seconds)
        self._complete_signals_before(seconds)
        self.quality.total_events += 1

        valid_trade = self._valid_trade(event)
        active_id = self._active_order_id(event) if valid_trade else None
        continues = (
            valid_trade
            and self.chain is not None
            and self.chain.side == event.side
            and self.chain.active_order_id == active_id
        )
        if self.chain is not None and not continues:
            self._finalize_chain()
        if valid_trade and not continues:
            self.chain = ActiveChain(str(event.side), int(active_id), 0.0, 0, seconds)

        contribution = RollingEvent(seconds)
        self._update_order_model(event, seconds, valid_trade)
        self._classify_quote_event(event, contribution)
        if valid_trade:
            assert self.chain is not None
            volume = float(event.volume or 0)
            self.chain.volume += volume
            self.chain.children += 1
            self.chain.last_seconds = seconds
            if event.side == "B":
                self.buy_volume += volume
                self.buy_count += 1
                contribution.buy_volume = volume
                contribution.buy_count = 1
            else:
                self.sell_volume += volume
                self.sell_count += 1
                contribution.sell_volume = volume
                contribution.sell_count = 1
            self.quality.valid_trades += 1

        if any(value for value in contribution.__dict__.values() if value != seconds):
            self.rolling_events.append(contribution)
            self._adjust_rolling_event(contribution, 1.0)
        self.event_count += 1
        current_book = self._book_tuple(event)
        if current_book is not None:
            mid = (current_book[0] + current_book[1]) / 2.0
            if self.last_mid is not None and self.last_mid > 0:
                move = math.log(mid / self.last_mid)
                self.mid_sq += move * move
                self.mid_moves += 1
            self.last_mid = mid
            self.previous_book = current_book

    def finish(self) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
        self._finish_day()
        return self.signal_rows, self.quality_rows

    def _valid_trade(self, event: Event) -> bool:
        if event.action != "TRADE" or event.side not in SIDES or (event.volume or 0) <= 0:
            return False
        if event.recid is not None:
            if event.recid in self.seen_trade_recids:
                self.quality.duplicate_trades += 1
                return False
            self.seen_trade_recids.add(event.recid)
        return True

    def _active_order_id(self, event: Event) -> int:
        value = event.buy_order_id if event.side == "B" else event.sell_order_id
        if value is None:
            self.quality.missing_active_order_id += 1
            return -(event.recid if event.recid is not None else event.row_id) - 1
        return int(value)

    def _schedule_grids_before(self, seconds: float) -> None:
        while self.next_grid < len(SIGNAL_GRID_SECONDS):
            grid = SIGNAL_GRID_SECONDS[self.next_grid]
            grid_session = "AM" if grid < 12 * 3600 else "PM"
            if grid_session != self.current_session or grid > seconds:
                break
            self._sample_grid(grid)
            self.next_grid += 1

    def _sample_grid(self, grid: float) -> None:
        self._trim_rolling(grid)
        bucket = self._seconds_bucket(grid)
        values = self._rolling_values()
        self.day_profile_samples.append((bucket, values["active_buy_volume"], values["active_sell_volume"]))
        if self.current_date is None or self.current_date // 100 != self.target_month:
            return
        self.quality.scheduled_target_signals += 1
        if self.previous_book is None:
            self.quality.signals_missing_book += 1
            return
        state = self._state(bucket, values["active_buy_volume"], values["active_sell_volume"])
        p_buy, n_buy = self._predict_fill("B", state, "best")
        p_sell, n_sell = self._predict_fill("S", state, "best")
        if state == "unknown" or p_buy is None or p_sell is None:
            self.quality.signals_missing_history += 1
            return
        bid, ask, bid_depth3, ask_depth3 = self.previous_book
        mid = (bid + ask) / 2.0
        total_active = values["active_buy_volume"] + values["active_sell_volume"]
        chain_total = values["chain_buy_volume"] + values["chain_sell_volume"]
        row: dict[str, object] = {
            "symbol": self.symbol,
            "date": self.current_date,
            "signal_seconds": int(grid),
            "signal_time": self._seconds_hhmm(grid),
            "state": state,
            **values,
            "active_net_share": (
                (values["active_buy_volume"] - values["active_sell_volume"]) / total_active
                if total_active > 0 else 0.0
            ),
            "chain_net_share": (
                (values["chain_buy_volume"] - values["chain_sell_volume"]) / chain_total
                if chain_total > 0 else 0.0
            ),
            "multi_chain_share": (
                values["multi_chain_count"] / values["chain_count"]
                if values["chain_count"] > 0 else 0.0
            ),
            "quote_aggressive_net": values["aggressive_add_buy"] - values["aggressive_add_sell"],
            "quote_cancel_net": values["near_cancel_sell"] - values["near_cancel_buy"],
            "spread_bps": (ask - bid) / mid * 10_000,
            "bid_depth3": bid_depth3,
            "ask_depth3": ask_depth3,
            "book_imbalance3": (
                (bid_depth3 - ask_depth3) / (bid_depth3 + ask_depth3)
                if bid_depth3 + ask_depth3 > 0 else 0.0
            ),
            "pred_fill_buy": p_buy,
            "pred_fill_sell": p_sell,
            "fill_opportunity_diff": p_buy - p_sell,
            "fill_history_buy": n_buy,
            "fill_history_sell": n_sell,
            "factor_version": FACTOR_VERSION,
        }
        self.pending.append(
            PendingSignal(
                grid + self.config.label_seconds,
                row,
                self.buy_volume,
                self.sell_volume,
                self.buy_count,
                self.sell_count,
                self.event_count,
                self.mid_sq,
                self.mid_moves,
            )
        )

    def _complete_signals_before(self, seconds: float) -> None:
        while self.pending and self.pending[0].end_seconds <= seconds:
            pending = self.pending.popleft()
            if self.previous_book is None:
                self.quality.incomplete_target_signals += 1
                continue
            bid, ask, bid_depth3, ask_depth3 = self.previous_book
            mid = (bid + ask) / 2.0
            buy_volume = self.buy_volume - pending.start_buy_volume
            sell_volume = self.sell_volume - pending.start_sell_volume
            pending.row.update(
                {
                    "future_buy_volume": buy_volume,
                    "future_sell_volume": sell_volume,
                    "future_net_flow": buy_volume - sell_volume,
                    "future_total_active_volume": buy_volume + sell_volume,
                    "future_buy_count": self.buy_count - pending.start_buy_count,
                    "future_sell_count": self.sell_count - pending.start_sell_count,
                    "future_event_count": self.event_count - pending.start_events,
                    "future_realized_vol_bps": math.sqrt(max(0.0, self.mid_sq - pending.start_mid_sq)) * 10_000,
                    "future_mid_moves": self.mid_moves - pending.start_mid_moves,
                    "end_spread_bps": (ask - bid) / mid * 10_000,
                    "end_bid_depth3": bid_depth3,
                    "end_ask_depth3": ask_depth3,
                }
            )
            self.signal_rows.append(pending.row)
            self.quality.completed_target_signals += 1

    def _rolling_values(self) -> dict[str, float]:
        values = {
            "active_buy_volume": 0.0, "active_sell_volume": 0.0,
            "active_buy_count": 0.0, "active_sell_count": 0.0,
            "aggressive_add_buy": 0.0, "aggressive_add_sell": 0.0,
            "near_cancel_buy": 0.0, "near_cancel_sell": 0.0,
            "chain_buy_volume": 0.0, "chain_sell_volume": 0.0,
            "single_chain_count": 0.0, "multi_chain_count": 0.0, "chain_count": 0.0,
        }
        values.update(self.rolling_sums)
        values.update(self.chain_sums)
        return values

    def _trim_rolling(self, seconds: float) -> None:
        cutoff = seconds - self.config.observation_seconds
        while self.rolling_events and self.rolling_events[0].seconds <= cutoff:
            self._adjust_rolling_event(self.rolling_events.popleft(), -1.0)
        while self.rolling_chains and self.rolling_chains[0].seconds <= cutoff:
            self._adjust_chain(self.rolling_chains.popleft(), -1.0)

    def _finalize_chain(self) -> None:
        if self.chain is None:
            return
        summary = ChainSummary(self.chain.last_seconds, self.chain.side, self.chain.volume, self.chain.children)
        self.rolling_chains.append(summary)
        self._adjust_chain(summary, 1.0)
        self.chain = None

    def _adjust_rolling_event(self, event: RollingEvent, sign: float) -> None:
        for name in (
            "buy_volume", "sell_volume", "buy_count", "sell_count",
            "aggressive_add_buy", "aggressive_add_sell", "near_cancel_buy", "near_cancel_sell",
        ):
            target = f"active_{name}" if name in {"buy_volume", "sell_volume", "buy_count", "sell_count"} else name
            self.rolling_sums[target] += sign * float(getattr(event, name))

    def _adjust_chain(self, chain: ChainSummary, sign: float) -> None:
        self.chain_sums[f"chain_{'buy' if chain.side == 'B' else 'sell'}_volume"] += sign * chain.volume
        self.chain_sums["chain_count"] += sign
        self.chain_sums["single_chain_count" if chain.children == 1 else "multi_chain_count"] += sign

    def _classify_quote_event(self, event: Event, contribution: RollingEvent) -> None:
        if self.previous_book is None or event.side not in SIDES or event.price is None or (event.volume or 0) <= 0:
            return
        bid, ask, _bd, _ad = self.previous_book
        side = str(event.side)
        price = float(event.price)
        volume = float(event.volume or 0)
        if event.action == "ORDER_ADD":
            order_id = event.buy_order_id if side == "B" else event.sell_order_id
            if order_id is not None and (side, int(order_id)) in self.active_order_keys:
                self.quality.quote_active_remainders_excluded += 1
                return
            marketable = price >= ask if side == "B" else price <= bid
            aggressive = price >= bid if side == "B" else price <= ask
            if not marketable and aggressive:
                setattr(contribution, f"aggressive_add_{'buy' if side == 'B' else 'sell'}", volume)
        elif event.action == "CANCEL":
            mid = (bid + ask) / 2.0
            near = price >= bid - mid * self.config.near_bps / 10_000 if side == "B" else price <= ask + mid * self.config.near_bps / 10_000
            if near:
                setattr(contribution, f"near_cancel_{'buy' if side == 'B' else 'sell'}", volume)

    def _update_order_model(self, event: Event, seconds: float, valid_trade: bool) -> None:
        if event.action == "ORDER_ADD" and event.side in SIDES and event.price is not None and (event.volume or 0) > 0 and self.previous_book is not None:
            order_id = event.buy_order_id if event.side == "B" else event.sell_order_id
            if order_id is None:
                return
            distance = self._distance(event.side, float(event.price))
            if distance is None:
                return
            bucket = time_bucket(event.time, 30)
            values = self._rolling_values()
            state = self._state(bucket, values["active_buy_volume"], values["active_sell_volume"])
            key = (str(event.side), int(order_id))
            self.orders.setdefault(key, PassiveOrder(str(event.side), int(order_id), int(event.volume or 0), seconds, state, distance))
            self.quality.candidate_passive_orders += 1
            return
        if valid_trade:
            active_id = event.buy_order_id if event.side == "B" else event.sell_order_id
            passive_id = event.sell_order_id if event.side == "B" else event.buy_order_id
            if active_id is not None:
                self.active_order_keys.add((str(event.side), int(active_id)))
            passive_side = "S" if event.side == "B" else "B"
            key = (passive_side, int(passive_id)) if passive_id is not None else None
            if key in self.orders:
                order = self.orders[key]
                if seconds - order.submit_seconds <= 60.0:
                    order.filled_60s += int(event.volume or 0)

    def _distance(self, side: str, price: float) -> str | None:
        assert self.previous_book is not None
        bid, ask, _bd, _ad = self.previous_book
        marketable = price >= ask if side == "B" else price <= bid
        if marketable:
            return None
        best = price >= bid if side == "B" else price <= ask
        if best:
            return "best"
        mid = (bid + ask) / 2.0
        near = price >= mid * (1 - self.config.near_bps / 10_000) if side == "B" else price <= mid * (1 + self.config.near_bps / 10_000)
        return "near" if near else None

    def _predict_fill(self, side: str, state: str, distance: str) -> tuple[float | None, int]:
        cell = self.fill_cells.get((side, state, distance), [0, 0])
        global_cell = self.fill_globals.get((side, distance), [0, 0])
        if global_cell[0] < self.config.minimum_fill_history:
            return None, global_cell[0]
        prior = global_cell[1] / global_cell[0]
        return (cell[1] + 20.0 * prior) / (cell[0] + 20.0), cell[0]

    def _state(self, bucket: str, buy: float, sell: float) -> str:
        buy_threshold = self.day_thresholds.get((bucket, "B"))
        sell_threshold = self.day_thresholds.get((bucket, "S"))
        if buy_threshold is None or sell_threshold is None:
            return "unknown"
        return f"B{int(buy > buy_threshold)}S{int(sell > sell_threshold)}"

    def _start_day(self, date: int) -> None:
        self.current_date = date
        self.current_session = None
        self.previous_row_id = None
        self.seen_trade_recids = set()
        self.orders = {}
        self.active_order_keys = set()
        self.day_profile_samples = []
        self.day_thresholds = {
            (bucket, side): self._reservoir(bucket, side).q(0.5)
            for bucket in [f"AM{i:02d}" for i in range(4)] + [f"PM{i:02d}" for i in range(4)]
            for side in SIDES
        }
        self.quality = BatchAQuality()

    def _start_session(self, session: str) -> None:
        self.current_session = session
        self.previous_book = None
        self.last_mid = None
        self.rolling_events.clear()
        self.rolling_chains.clear()
        self.rolling_sums = defaultdict(float)
        self.chain_sums = defaultdict(float)
        self.chain = None
        self.pending.clear()
        self.next_grid = next((i for i, value in enumerate(SIGNAL_GRID_SECONDS) if (value < 12 * 3600) == (session == "AM")), len(SIGNAL_GRID_SECONDS))
        self.buy_volume = self.sell_volume = 0.0
        self.buy_count = self.sell_count = 0
        self.event_count = 0
        self.mid_sq = 0.0
        self.mid_moves = 0

    def _finish_session(self) -> None:
        if self.current_session is None:
            return
        self._finalize_chain()
        session_end = 11.5 * 3600 if self.current_session == "AM" else 14.95 * 3600
        self._complete_signals_before(session_end)
        self.quality.incomplete_target_signals += len(self.pending)
        self.pending.clear()
        self.current_session = None

    def _finish_day(self) -> None:
        if self.current_date is None:
            return
        self._finish_session()
        for key, order in self.orders.items():
            if key in self.active_order_keys:
                self.quality.excluded_active_orders += 1
                continue
            if order.filled_60s > order.submit_qty:
                self.quality.fill_over_submit += 1
            if order.state == "unknown":
                continue
            filled = int(order.filled_60s > 0)
            cell = self.fill_cells[(order.side, order.state, order.distance)]
            cell[0] += 1
            cell[1] += filled
            global_cell = self.fill_globals[(order.side, order.distance)]
            global_cell[0] += 1
            global_cell[1] += filled
            self.quality.model_passive_orders += 1
        for bucket, buy, sell in self.day_profile_samples:
            self._reservoir(bucket, "B").add(buy)
            self._reservoir(bucket, "S").add(sell)
        self.quality_rows.append({"symbol": self.symbol, "date": self.current_date, **self.quality.__dict__, "factor_version": FACTOR_VERSION})
        self.current_date = None

    def _reservoir(self, bucket: str, side: str) -> Reservoir:
        key = (bucket, side)
        if key not in self.profile_reservoirs:
            seed = sum(ord(ch) for ch in f"{self.symbol}|{bucket}|{side}")
            self.profile_reservoirs[key] = Reservoir(self.config.intensity_reservoir_size, seed)
        return self.profile_reservoirs[key]

    @staticmethod
    def _book_tuple(event: Event) -> tuple[float, float, float, float] | None:
        if event.mid is None:
            return None
        bid_depth3 = event.bid_depths[1]
        ask_depth3 = event.ask_depths[1]
        if bid_depth3 is None or ask_depth3 is None or bid_depth3 < 0 or ask_depth3 < 0:
            return None
        return float(event.bid1), float(event.ask1), float(bid_depth3), float(ask_depth3)

    @staticmethod
    def _seconds_bucket(seconds: float) -> str:
        if seconds < 12 * 3600:
            return f"AM{int((seconds - 9.5 * 3600) // 1800):02d}"
        return f"PM{int((seconds - 13 * 3600) // 1800):02d}"

    @staticmethod
    def _seconds_hhmm(seconds: float) -> int:
        return int(seconds // 3600) * 100 + int(seconds % 3600 // 60)
