"""Streaming M1-Q engine for chain-debiased flow and quote responses."""

from __future__ import annotations

import math
from collections import defaultdict, deque
from dataclasses import dataclass

from scripts.factors.order_shape_mechanism.engine import (
    Event,
    RunningMoments,
    session_name,
)


FACTOR_VERSION = "m1_quote_mechanism_v1_20260804"
SIDES = ("B", "S")


@dataclass(frozen=True)
class M1QuoteConfig:
    lob_horizons: tuple[int, ...] = (20, 50)
    trade_horizons: tuple[int, ...] = (20, 50)
    near_bps: float = 10.0

    def __post_init__(self) -> None:
        if not self.lob_horizons or min(self.lob_horizons) <= 0:
            raise ValueError("lob_horizons must be positive")
        if not self.trade_horizons or min(self.trade_horizons) <= 0:
            raise ValueError("trade_horizons must be positive")
        if self.near_bps <= 0:
            raise ValueError("near_bps must be positive")


@dataclass
class M1QuoteQuality:
    total_events: int = 0
    valid_trades: int = 0
    duplicate_trades: int = 0
    missing_active_order_id: int = 0
    raw_trade_triggers: int = 0
    chain_triggers: int = 0
    single_trade_chains: int = 0
    multi_trade_chains: int = 0
    terminal_chains: int = 0
    passive_adds: int = 0
    marketable_adds_excluded: int = 0
    quote_adds_missing_prebook: int = 0
    cancels_missing_prebook: int = 0
    incomplete_raw_labels: int = 0
    incomplete_chain_labels: int = 0


@dataclass
class ActiveChain:
    side: str
    active_order_id: int
    child_trades: int
    volume: float
    trigger_mid: float | None
    opposite_depth3: float | None


@dataclass
class Pending:
    target_index: int
    clock: str
    horizon: int
    kind: str
    trigger_side: str
    chain_bucket: str
    trigger_mid: float | None
    opposite_depth3: float | None
    start_buy_volume: float
    start_sell_volume: float
    counters: dict[str, float]


class M1QuoteEngine:
    """Consume one target-month symbol stream without historical warmup."""

    COUNTER_NAMES = tuple(
        f"{metric}_{side}"
        for metric in ("add_total", "add_aggressive", "cancel_near")
        for side in SIDES
    )

    def __init__(self, symbol: str, config: M1QuoteConfig | None = None) -> None:
        self.symbol = symbol
        self.config = config or M1QuoteConfig()
        self.current_date: int | None = None
        self.current_session: str | None = None
        self.previous_row_id: int | None = None
        self.previous_book: tuple[float, float, tuple[int | None, ...], tuple[int | None, ...]] | None = None
        self.last_mid: float | None = None
        self.event_index = 0
        self.trade_index = 0
        self.buy_volume = 0.0
        self.sell_volume = 0.0
        self.counters = {name: 0.0 for name in self.COUNTER_NAMES}
        self.pending_lob = {
            horizon: deque() for horizon in self.config.lob_horizons
        }
        self.pending_trade = {
            horizon: deque() for horizon in self.config.trade_horizons
        }
        self.chain: ActiveChain | None = None
        self.seen_trade_recids: set[int] = set()
        self.day_stats: dict[tuple[str, str], RunningMoments] = defaultdict(RunningMoments)
        self.day_quality = M1QuoteQuality()
        self.stat_rows: list[dict[str, object]] = []
        self.quality_rows: list[dict[str, object]] = []

    def process(self, event: Event) -> None:
        session = session_name(event.time)
        if session is None:
            return
        if self.current_date != event.date:
            self._finish_day()
            self._start_day(event.date)
        if self.previous_row_id is not None and event.row_id <= self.previous_row_id:
            raise ValueError(
                f"non-increasing row_id for {self.symbol} {event.date}: "
                f"{event.row_id} <= {self.previous_row_id}"
            )
        self.previous_row_id = event.row_id
        if self.current_session != session:
            self._start_session(session)

        self.day_quality.total_events += 1
        valid_trade = self._valid_trade(event)
        active_order_id = self._active_order_id(event) if valid_trade else None
        continues_chain = (
            valid_trade
            and self.chain is not None
            and self.chain.side == event.side
            and self.chain.active_order_id == active_order_id
        )
        if self.chain is not None and not continues_chain:
            self._finalize_chain(create_labels=True)

        if valid_trade:
            if not continues_chain:
                self.chain = ActiveChain(
                    side=str(event.side),
                    active_order_id=int(active_order_id),
                    child_trades=0,
                    volume=0.0,
                    trigger_mid=self._prebook_mid(),
                    opposite_depth3=self._opposite_depth3(str(event.side)),
                )
            assert self.chain is not None
            self.chain.child_trades += 1
            self.chain.volume += float(event.volume or 0)

        self._update_quote_counters(event)
        if valid_trade:
            self.trade_index += 1
            if event.side == "B":
                self.buy_volume += float(event.volume or 0)
            else:
                self.sell_volume += float(event.volume or 0)
            self.day_quality.valid_trades += 1

        if event.mid is not None:
            self.last_mid = event.mid
        self._complete_pending()

        if valid_trade:
            self._create_raw_triggers(event)

        current_book = self._book_tuple(event)
        if current_book is not None:
            self.previous_book = current_book
        self.event_index += 1

    def finish(self) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
        self._finish_day()
        return self.stat_rows, self.quality_rows

    def _valid_trade(self, event: Event) -> bool:
        if event.action != "TRADE" or event.side not in SIDES or (event.volume or 0) <= 0:
            return False
        if event.recid is not None:
            if event.recid in self.seen_trade_recids:
                self.day_quality.duplicate_trades += 1
                return False
            self.seen_trade_recids.add(event.recid)
        return True

    def _active_order_id(self, event: Event) -> int:
        order_id = event.buy_order_id if event.side == "B" else event.sell_order_id
        if order_id is None:
            self.day_quality.missing_active_order_id += 1
            return -(event.recid if event.recid is not None else event.row_id) - 1
        return int(order_id)

    def _create_raw_triggers(self, event: Event) -> None:
        self.day_quality.raw_trade_triggers += 1
        for clock, horizons in (
            ("lob", self.config.lob_horizons),
            ("trade", self.config.trade_horizons),
        ):
            current = self.event_index if clock == "lob" else self.trade_index
            for horizon in horizons:
                pending = self._new_pending(
                    target_index=current + horizon,
                    clock=clock,
                    horizon=horizon,
                    kind="raw",
                    trigger_side=str(event.side),
                    chain_bucket="raw",
                    trigger_mid=self._prebook_mid(),
                    opposite_depth3=None,
                )
                self._queue(clock, horizon).append(pending)

    def _finalize_chain(self, create_labels: bool) -> None:
        chain = self.chain
        if chain is None:
            return
        bucket = "single" if chain.child_trades == 1 else "multi"
        self.day_quality.chain_triggers += 1
        if bucket == "single":
            self.day_quality.single_trade_chains += 1
        else:
            self.day_quality.multi_trade_chains += 1
        if create_labels:
            for clock, horizons in (
                ("lob", self.config.lob_horizons),
                ("trade", self.config.trade_horizons),
            ):
                current = self.event_index if clock == "lob" else self.trade_index
                for horizon in horizons:
                    target = current + horizon - 1 if clock == "lob" else current + horizon
                    self._queue(clock, horizon).append(
                        self._new_pending(
                            target_index=target,
                            clock=clock,
                            horizon=horizon,
                            kind="chain",
                            trigger_side=chain.side,
                            chain_bucket=bucket,
                            trigger_mid=chain.trigger_mid,
                            opposite_depth3=chain.opposite_depth3,
                        )
                    )
        else:
            self.day_quality.terminal_chains += 1
        self.chain = None

    def _new_pending(
        self,
        *,
        target_index: int,
        clock: str,
        horizon: int,
        kind: str,
        trigger_side: str,
        chain_bucket: str,
        trigger_mid: float | None,
        opposite_depth3: float | None,
    ) -> Pending:
        return Pending(
            target_index=target_index,
            clock=clock,
            horizon=horizon,
            kind=kind,
            trigger_side=trigger_side,
            chain_bucket=chain_bucket,
            trigger_mid=trigger_mid,
            opposite_depth3=opposite_depth3,
            start_buy_volume=self.buy_volume,
            start_sell_volume=self.sell_volume,
            counters=dict(self.counters),
        )

    def _complete_pending(self) -> None:
        for clock, queues in (("lob", self.pending_lob), ("trade", self.pending_trade)):
            current = self.event_index if clock == "lob" else self.trade_index
            for queue in queues.values():
                while queue and queue[0].target_index <= current:
                    self._record(queue.popleft())

    def _record(self, pending: Pending) -> None:
        sign = 1.0 if pending.trigger_side == "B" else -1.0
        future_signed = sign * (
            (self.buy_volume - pending.start_buy_volume)
            - (self.sell_volume - pending.start_sell_volume)
        )
        prefix = f"{pending.clock}{pending.horizon}"
        base_groups = [f"trigger={pending.trigger_side}"]
        if pending.kind == "chain":
            base_groups = [
                f"trigger={pending.trigger_side}|chain=all",
                f"trigger={pending.trigger_side}|chain={pending.chain_bucket}",
            ]
        self._add_for_groups(
            f"{prefix}_{pending.kind}_future_signed_volume", base_groups, future_signed
        )
        if pending.trigger_mid and pending.trigger_mid > 0 and self.last_mid:
            directional_return = sign * math.log(self.last_mid / pending.trigger_mid) * 10_000
            self._add_for_groups(
                f"{prefix}_{pending.kind}_directional_mid_bps",
                base_groups,
                directional_return,
            )
        if pending.kind != "chain":
            return
        same = pending.trigger_side
        opposite = "S" if same == "B" else "B"
        same_total = self.counters[f"add_total_{same}"] - pending.counters[f"add_total_{same}"]
        same_aggressive = (
            self.counters[f"add_aggressive_{same}"]
            - pending.counters[f"add_aggressive_{same}"]
        )
        opposite_total = (
            self.counters[f"add_total_{opposite}"]
            - pending.counters[f"add_total_{opposite}"]
        )
        opposite_aggressive = (
            self.counters[f"add_aggressive_{opposite}"]
            - pending.counters[f"add_aggressive_{opposite}"]
        )
        opposite_cancel = (
            self.counters[f"cancel_near_{opposite}"]
            - pending.counters[f"cancel_near_{opposite}"]
        )
        chase = int(same_aggressive > 0)
        replenish = int(opposite_aggressive > 0)
        joint_group = (
            f"trigger={same}|chase={chase}|replenish={replenish}|chain=all"
        )
        self._add_stat(f"{prefix}_chain_future_signed_by_quote_state", joint_group, future_signed)
        if pending.trigger_mid and pending.trigger_mid > 0 and self.last_mid:
            directional_return = sign * math.log(self.last_mid / pending.trigger_mid) * 10_000
            self._add_stat(
                f"{prefix}_chain_mid_bps_by_quote_state", joint_group, directional_return
            )
        if same_total > 0:
            self._add_for_groups(
                f"{prefix}_same_aggressive_add_share",
                base_groups,
                same_aggressive / same_total,
            )
        if opposite_total > 0:
            self._add_for_groups(
                f"{prefix}_opposite_aggressive_add_share",
                base_groups,
                opposite_aggressive / opposite_total,
            )
        if pending.opposite_depth3 and pending.opposite_depth3 > 0:
            self._add_for_groups(
                f"{prefix}_opposite_replenish_depth3",
                base_groups,
                opposite_aggressive / pending.opposite_depth3,
            )
            self._add_for_groups(
                f"{prefix}_opposite_near_cancel_depth3",
                base_groups,
                opposite_cancel / pending.opposite_depth3,
            )

    def _update_quote_counters(self, event: Event) -> None:
        if event.action not in {"ORDER_ADD", "CANCEL"} or event.side not in SIDES:
            return
        if event.price is None or event.price <= 0 or (event.volume or 0) <= 0:
            return
        if self.previous_book is None:
            if event.action == "ORDER_ADD":
                self.day_quality.quote_adds_missing_prebook += 1
            else:
                self.day_quality.cancels_missing_prebook += 1
            return
        bid, ask, _bid_depths, _ask_depths = self.previous_book
        side = str(event.side)
        price = float(event.price)
        volume = float(event.volume or 0)
        if event.action == "ORDER_ADD":
            marketable = price >= ask if side == "B" else price <= bid
            if marketable:
                self.day_quality.marketable_adds_excluded += 1
                return
            self.day_quality.passive_adds += 1
            self.counters[f"add_total_{side}"] += volume
            aggressive = price >= bid if side == "B" else price <= ask
            if aggressive:
                self.counters[f"add_aggressive_{side}"] += volume
            return
        mid = (bid + ask) / 2.0
        near = (
            price >= bid - mid * self.config.near_bps / 10_000
            if side == "B"
            else price <= ask + mid * self.config.near_bps / 10_000
        )
        if near:
            self.counters[f"cancel_near_{side}"] += volume

    def _prebook_mid(self) -> float | None:
        if self.previous_book is None:
            return None
        return (self.previous_book[0] + self.previous_book[1]) / 2.0

    def _opposite_depth3(self, trigger_side: str) -> float | None:
        if self.previous_book is None:
            return None
        depths = self.previous_book[3] if trigger_side == "B" else self.previous_book[2]
        value = depths[1] if len(depths) > 1 else None
        return float(value) if value is not None and value > 0 else None

    @staticmethod
    def _book_tuple(
        event: Event,
    ) -> tuple[float, float, tuple[int | None, ...], tuple[int | None, ...]] | None:
        if event.mid is None:
            return None
        return (
            float(event.bid1),
            float(event.ask1),
            event.bid_depths,
            event.ask_depths,
        )

    def _queue(self, clock: str, horizon: int) -> deque[Pending]:
        queues = self.pending_lob if clock == "lob" else self.pending_trade
        return queues[horizon]

    def _add_for_groups(self, variant: str, groups: list[str], value: float) -> None:
        for group in groups:
            self._add_stat(variant, group, value)

    def _add_stat(self, variant: str, group: str, value: float) -> None:
        self.day_stats[(variant, group)].add(value)

    def _start_day(self, date: int) -> None:
        self.current_date = date
        self.current_session = None
        self.previous_row_id = None
        self.seen_trade_recids = set()
        self.day_stats = defaultdict(RunningMoments)
        self.day_quality = M1QuoteQuality()

    def _start_session(self, session: str) -> None:
        if self.current_session is not None:
            self._discard_pending()
            self._finalize_chain(create_labels=False)
        self.current_session = session
        self.previous_book = None
        self.last_mid = None
        self.event_index = 0
        self.trade_index = 0
        self.buy_volume = 0.0
        self.sell_volume = 0.0
        self.counters = {name: 0.0 for name in self.COUNTER_NAMES}

    def _discard_pending(self) -> None:
        for queues in (self.pending_lob, self.pending_trade):
            for queue in queues.values():
                for pending in queue:
                    if pending.kind == "raw":
                        self.day_quality.incomplete_raw_labels += 1
                    else:
                        self.day_quality.incomplete_chain_labels += 1
                queue.clear()

    def _finish_day(self) -> None:
        if self.current_date is None:
            return
        self._discard_pending()
        self._finalize_chain(create_labels=False)
        for (variant, group), moments in sorted(self.day_stats.items()):
            self.stat_rows.append(
                {
                    "symbol": self.symbol,
                    "date": self.current_date,
                    "mechanism": "M1Q",
                    "variant": variant,
                    "group_key": group,
                    "observations": moments.observations,
                    "value_sum": moments.value_sum,
                    "value_sq_sum": moments.value_sq_sum,
                    "weight_sum": moments.weight_sum,
                    "factor_version": FACTOR_VERSION,
                }
            )
        self.quality_rows.append(
            {
                "symbol": self.symbol,
                "date": self.current_date,
                **self.day_quality.__dict__,
                "factor_version": FACTOR_VERSION,
            }
        )
        self.current_date = None
        self.current_session = None
