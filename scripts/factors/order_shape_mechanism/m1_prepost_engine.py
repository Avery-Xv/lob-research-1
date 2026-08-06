"""Symmetric pre/post flow comparison around contiguous active-order chains."""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field

from scripts.factors.order_shape_mechanism.engine import Event, RunningMoments, session_name


FACTOR_VERSION = "m1_prepost_v1_20260804"
SIDES = ("B", "S")


@dataclass(frozen=True)
class M1PrePostConfig:
    lob_horizons: tuple[int, ...] = (20, 50)
    trade_horizons: tuple[int, ...] = (20, 50)

    def __post_init__(self) -> None:
        if not self.lob_horizons or min(self.lob_horizons) <= 0:
            raise ValueError("lob_horizons must be positive")
        if not self.trade_horizons or min(self.trade_horizons) <= 0:
            raise ValueError("trade_horizons must be positive")


@dataclass
class M1PrePostQuality:
    total_events: int = 0
    valid_trades: int = 0
    duplicate_trades: int = 0
    missing_active_order_id: int = 0
    chain_triggers: int = 0
    single_trade_chains: int = 0
    multi_trade_chains: int = 0
    terminal_chains: int = 0
    insufficient_pre_lob_labels: int = 0
    insufficient_pre_trade_labels: int = 0
    incomplete_post_lob_labels: int = 0
    incomplete_post_trade_labels: int = 0
    completed_pair_labels: int = 0


@dataclass
class WindowFlow:
    buy: float
    sell: float


@dataclass
class ActiveChain:
    side: str
    active_order_id: int
    child_trades: int = 0
    pre: dict[tuple[str, int], WindowFlow] = field(default_factory=dict)


@dataclass
class PendingPair:
    target_index: int
    clock: str
    horizon: int
    side: str
    chain_bucket: str
    pre: WindowFlow
    start_buy: float
    start_sell: float


class M1PrePostEngine:
    """Consume one symbol stream and compare equal windows around each chain.

    The pre-window ends immediately before the first child trade.  The post-window
    starts with the first LOB/trade event after the final child trade.  The chain's
    own volume is excluded from both windows.
    """

    def __init__(self, symbol: str, config: M1PrePostConfig | None = None) -> None:
        self.symbol = symbol
        self.config = config or M1PrePostConfig()
        self.current_date: int | None = None
        self.current_session: str | None = None
        self.previous_row_id: int | None = None
        self.seen_trade_recids: set[int] = set()
        self.event_index = 0
        self.trade_index = 0
        self.buy_volume = 0.0
        self.sell_volume = 0.0
        self.event_prefix: deque[tuple[float, float]] = deque(
            maxlen=max(self.config.lob_horizons) + 1
        )
        self.trade_prefix: deque[tuple[float, float]] = deque(
            maxlen=max(self.config.trade_horizons) + 1
        )
        self.pending_lob = {horizon: deque() for horizon in self.config.lob_horizons}
        self.pending_trade = {
            horizon: deque() for horizon in self.config.trade_horizons
        }
        self.chain: ActiveChain | None = None
        self.day_stats: dict[tuple[str, str], RunningMoments] = defaultdict(RunningMoments)
        self.day_quality = M1PrePostQuality()
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

        if valid_trade and not continues_chain:
            self.chain = ActiveChain(
                side=str(event.side),
                active_order_id=int(active_order_id),
                pre=self._pre_windows(),
            )
        if valid_trade:
            assert self.chain is not None
            self.chain.child_trades += 1
            volume = float(event.volume or 0)
            if event.side == "B":
                self.buy_volume += volume
            else:
                self.sell_volume += volume
            self.trade_index += 1
            self.trade_prefix.append((self.buy_volume, self.sell_volume))
            self.day_quality.valid_trades += 1

        self.event_index += 1
        self.event_prefix.append((self.buy_volume, self.sell_volume))
        self._complete_pending()

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

    def _pre_windows(self) -> dict[tuple[str, int], WindowFlow]:
        result: dict[tuple[str, int], WindowFlow] = {}
        for clock, horizons, index, history in (
            ("lob", self.config.lob_horizons, self.event_index, self.event_prefix),
            ("trade", self.config.trade_horizons, self.trade_index, self.trade_prefix),
        ):
            for horizon in horizons:
                if index < horizon:
                    field_name = f"insufficient_pre_{clock}_labels"
                    setattr(self.day_quality, field_name, getattr(self.day_quality, field_name) + 1)
                    continue
                start_buy, start_sell = history[-horizon - 1]
                result[(clock, horizon)] = WindowFlow(
                    self.buy_volume - start_buy,
                    self.sell_volume - start_sell,
                )
        return result

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
            for (clock, horizon), pre in chain.pre.items():
                current = self.event_index if clock == "lob" else self.trade_index
                pending = PendingPair(
                    target_index=current + horizon,
                    clock=clock,
                    horizon=horizon,
                    side=chain.side,
                    chain_bucket=bucket,
                    pre=pre,
                    start_buy=self.buy_volume,
                    start_sell=self.sell_volume,
                )
                queues = self.pending_lob if clock == "lob" else self.pending_trade
                queues[horizon].append(pending)
        else:
            self.day_quality.terminal_chains += 1
        self.chain = None

    def _complete_pending(self) -> None:
        for clock, queues in (("lob", self.pending_lob), ("trade", self.pending_trade)):
            current = self.event_index if clock == "lob" else self.trade_index
            for queue in queues.values():
                while queue and queue[0].target_index <= current:
                    pending = queue.popleft()
                    post = WindowFlow(
                        self.buy_volume - pending.start_buy,
                        self.sell_volume - pending.start_sell,
                    )
                    self._record_pair(pending, post)

    def _record_pair(self, pending: PendingPair, post: WindowFlow) -> None:
        sign = 1.0 if pending.side == "B" else -1.0
        pre_signed = sign * (pending.pre.buy - pending.pre.sell)
        post_signed = sign * (post.buy - post.sell)
        pre_total = pending.pre.buy + pending.pre.sell
        post_total = post.buy + post.sell
        prefix = f"{pending.clock}{pending.horizon}_chain"
        groups = [
            f"trigger={pending.side}|chain=all",
            f"trigger={pending.side}|chain={pending.chain_bucket}",
        ]
        metrics = {
            "pre_signed_volume": pre_signed,
            "post_signed_volume": post_signed,
            "post_minus_pre_signed_volume": post_signed - pre_signed,
            "pre_total_volume": pre_total,
            "post_total_volume": post_total,
            "post_minus_pre_total_volume": post_total - pre_total,
        }
        if pre_total > 0 and post_total > 0:
            pre_share = pre_signed / pre_total
            post_share = post_signed / post_total
            metrics.update(
                {
                    "pre_signed_share": pre_share,
                    "post_signed_share": post_share,
                    "post_minus_pre_signed_share": post_share - pre_share,
                }
            )
        for metric, value in metrics.items():
            for group in groups:
                self.day_stats[(f"{prefix}_{metric}", group)].add(value)
        self.day_quality.completed_pair_labels += 1

    def _start_day(self, date: int) -> None:
        self.current_date = date
        self.current_session = None
        self.previous_row_id = None
        self.seen_trade_recids = set()
        self.day_stats = defaultdict(RunningMoments)
        self.day_quality = M1PrePostQuality()

    def _start_session(self, session: str) -> None:
        if self.current_session is not None:
            self._discard_pending()
            self._finalize_chain(create_labels=False)
        self.current_session = session
        self.event_index = 0
        self.trade_index = 0
        self.buy_volume = 0.0
        self.sell_volume = 0.0
        self.event_prefix.clear()
        self.trade_prefix.clear()
        self.event_prefix.append((0.0, 0.0))
        self.trade_prefix.append((0.0, 0.0))

    def _discard_pending(self) -> None:
        for clock, queues in (("lob", self.pending_lob), ("trade", self.pending_trade)):
            field_name = f"incomplete_post_{clock}_labels"
            count = sum(len(queue) for queue in queues.values())
            setattr(self.day_quality, field_name, getattr(self.day_quality, field_name) + count)
            for queue in queues.values():
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
                    "mechanism": "M1PP",
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
