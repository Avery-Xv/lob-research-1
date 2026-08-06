"""Streaming state engine for the order-shape mechanism experiments.

The engine consumes V4 events in their stored ``(date, row_id)`` order.  It is
deliberately independent of DuckDB and the filesystem so its timing, state,
future-window, and order-lifecycle rules can be tested with synthetic events.
"""

from __future__ import annotations

import hashlib
import math
import random
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Iterable, Sequence


FACTOR_VERSION = "order_shape_mechanism_v4_20260804"
SIDES = ("B", "S")


def hhmmssmmm_to_seconds(value: int) -> float:
    """Convert the repository HHMMSSmmm integer clock to seconds."""
    milliseconds = value % 1000
    seconds = (value // 1000) % 100
    minutes = (value // 100000) % 100
    hours = value // 10000000
    return hours * 3600.0 + minutes * 60.0 + seconds + milliseconds / 1000.0


def session_name(time_value: int) -> str | None:
    if 93_000_000 <= time_value < 113_000_000:
        return "AM"
    if 130_000_000 <= time_value < 145_700_000:
        return "PM"
    return None


def time_bucket(time_value: int, minutes: int) -> str:
    session = session_name(time_value)
    if session is None:
        raise ValueError(f"event is outside continuous auction: {time_value}")
    seconds = hhmmssmmm_to_seconds(time_value)
    start = 9.5 * 3600 if session == "AM" else 13 * 3600
    index = int((seconds - start) // (minutes * 60))
    return f"{session}{index:02d}"


def quantile(values: Sequence[float], probability: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = probability * (len(ordered) - 1)
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


class Reservoir:
    """Deterministic bounded-memory sample for historical quantiles."""

    def __init__(self, capacity: int, seed: int) -> None:
        self.capacity = capacity
        self.seen = 0
        self.values: list[float] = []
        self._random = random.Random(seed)

    def add(self, value: float | None) -> None:
        if value is None or not math.isfinite(value):
            return
        self.seen += 1
        if len(self.values) < self.capacity:
            self.values.append(float(value))
            return
        replacement = self._random.randrange(self.seen)
        if replacement < self.capacity:
            self.values[replacement] = float(value)

    def q(self, probability: float) -> float | None:
        return quantile(self.values, probability)


@dataclass
class RunningMoments:
    observations: int = 0
    value_sum: float = 0.0
    value_sq_sum: float = 0.0
    weight_sum: float = 0.0

    def add(self, value: float, weight: float = 1.0) -> None:
        if not math.isfinite(value) or not math.isfinite(weight) or weight <= 0:
            return
        self.observations += 1
        self.value_sum += value * weight
        self.value_sq_sum += value * value * weight
        self.weight_sum += weight

    @property
    def mean(self) -> float | None:
        return self.value_sum / self.weight_sum if self.weight_sum else None


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
    bid_depths: tuple[int | None, int | None, int | None] = (None, None, None)
    ask_depths: tuple[int | None, int | None, int | None] = (None, None, None)

    @property
    def mid(self) -> float | None:
        if self.bid1 is None or self.ask1 is None or self.bid1 <= 0 or self.ask1 < self.bid1:
            return None
        return (self.bid1 + self.ask1) / 2.0


@dataclass(frozen=True)
class MechanismConfig:
    target_month: str
    half_lives: tuple[float, ...] = (1.0, 5.0, 30.0)
    primary_half_life: float = 5.0
    trade_event_half_lives: tuple[float, ...] = (5.0, 20.0, 50.0)
    primary_trade_event_half_life: float = 20.0
    horizons: tuple[int, ...] = (5, 10, 20, 50)
    price_windows: tuple[float, ...] = (10.0, 30.0, 60.0)
    primary_price_window: float = 30.0
    price_sigma_multiplier: float = 1.5
    intensity_quantile: float = 0.5
    time_bucket_minutes: int = 30
    rolling_trade_sizes: int = 500
    reservoir_size: int = 5_000
    profile_sample_stride: int = 10
    depth_sample_stride: int = 10
    minimum_threshold_samples: int = 100
    near_mid_bps: float = 10.0
    audit_dates: frozenset[int] = frozenset()
    audit_max_events: int = 200
    audit_max_orders: int = 60

    def __post_init__(self) -> None:
        if len(self.target_month) != 6 or not self.target_month.isdigit():
            raise ValueError("target_month must be YYYYMM")
        if self.primary_half_life not in self.half_lives:
            raise ValueError("primary_half_life must be included in half_lives")
        if self.primary_trade_event_half_life not in self.trade_event_half_lives:
            raise ValueError(
                "primary_trade_event_half_life must be included in trade_event_half_lives"
            )
        if self.primary_price_window not in self.price_windows:
            raise ValueError("primary_price_window must be included in price_windows")
        if not self.horizons or min(self.horizons) <= 0:
            raise ValueError("horizons must be positive")
        if self.profile_sample_stride <= 0:
            raise ValueError("profile_sample_stride must be positive")
        if self.depth_sample_stride <= 0:
            raise ValueError("depth_sample_stride must be positive")
        if not 0 < self.intensity_quantile < 1:
            raise ValueError("intensity_quantile must be in (0, 1)")


class DecayedIntensity:
    def __init__(self, half_life: float) -> None:
        self.half_life = half_life
        self.value = 0.0
        self.last_seconds: float | None = None

    def reset(self) -> None:
        self.value = 0.0
        self.last_seconds = None

    def update(self, seconds: float, addition: float = 0.0) -> float:
        if self.last_seconds is not None:
            elapsed = max(0.0, seconds - self.last_seconds)
            self.value *= math.exp(-math.log(2.0) * elapsed / self.half_life)
        self.value += addition
        self.last_seconds = seconds
        return self.value


class PriceReturnWindow:
    def __init__(self, seconds: float) -> None:
        self.seconds = seconds
        self.points: deque[tuple[float, float]] = deque()

    def reset(self) -> None:
        self.points.clear()

    def update(self, seconds: float, mid: float | None) -> float | None:
        if mid is None or mid <= 0:
            return None
        cutoff = seconds - self.seconds
        while len(self.points) >= 2 and self.points[1][0] <= cutoff:
            self.points.popleft()
        reference = self.points[0][1] if self.points and self.points[0][0] <= cutoff else None
        self.points.append((seconds, mid))
        if reference is None or reference <= 0:
            return None
        return math.log(mid / reference)


@dataclass
class PendingTrigger:
    target_index: int
    trigger_side: str
    state: str
    trade_event_state: str | None
    price_states: dict[str, str]
    start_buy_volume: float
    start_sell_volume: float
    start_buy_count: int
    start_sell_count: int
    start_heat: float | None
    start_trade_event_heat: float | None


@dataclass
class PassiveOrder:
    order_id: int
    side: str
    submit_qty: int
    submit_seconds: float
    state: str
    trade_event_state: str
    distance_bucket: str
    filled_total: int = 0
    filled_10s: int = 0
    filled_60s: int = 0
    filled_300s: int = 0
    cancel_qty: int = 0

    def add_fill(self, seconds: float, quantity: int) -> None:
        self.filled_total += quantity
        elapsed = max(0.0, seconds - self.submit_seconds)
        if elapsed <= 10:
            self.filled_10s += quantity
        if elapsed <= 60:
            self.filled_60s += quantity
        if elapsed <= 300:
            self.filled_300s += quantity


@dataclass
class DayQuality:
    total_events: int = 0
    trade_b: int = 0
    trade_s: int = 0
    trade_n: int = 0
    order_add: int = 0
    cancel: int = 0
    invalid_book: int = 0
    invalid_prebook: int = 0
    duplicate_trades: int = 0
    incomplete_future_labels: int = 0
    passive_orders: int = 0
    fill_over_submit: int = 0
    unmatched_passive_trades: int = 0
    insufficient_state_history: int = 0


class MechanismEngine:
    """Consume one symbol's chronological warmup and target event stream."""

    def __init__(self, symbol: str, config: MechanismConfig) -> None:
        self.symbol = symbol
        self.config = config
        seed = int(hashlib.sha256(symbol.encode()).hexdigest()[:16], 16)
        self.intensity_samples: dict[tuple[float, str, str], Reservoir] = {}
        self.trade_event_intensity_samples: dict[tuple[float, str, str], Reservoir] = {}
        self.return_samples: dict[tuple[float, str], Reservoir] = {}
        self.daily_heat_sample = Reservoir(config.reservoir_size, seed ^ 0xD411)
        for half_life in config.half_lives:
            for bucket_index in range(8):
                session = "AM" if bucket_index < 4 else "PM"
                local_index = bucket_index if bucket_index < 4 else bucket_index - 4
                bucket = f"{session}{local_index:02d}"
                for side_index, side in enumerate(SIDES):
                    key_seed = seed ^ int(half_life * 1000) ^ bucket_index ^ side_index
                    self.intensity_samples[(half_life, bucket, side)] = Reservoir(
                        config.reservoir_size, key_seed
                    )
                for window in config.price_windows:
                    self.return_samples[(window, bucket)] = Reservoir(
                        config.reservoir_size,
                        seed ^ int(window * 10_000) ^ bucket_index,
                    )
        for half_life in config.trade_event_half_lives:
            for bucket_index in range(8):
                session = "AM" if bucket_index < 4 else "PM"
                local_index = bucket_index if bucket_index < 4 else bucket_index - 4
                bucket = f"{session}{local_index:02d}"
                for side_index, side in enumerate(SIDES):
                    key_seed = (
                        seed ^ 0x7EAD ^ int(half_life * 1000)
                        ^ bucket_index ^ side_index
                    )
                    self.trade_event_intensity_samples[
                        (half_life, bucket, side)
                    ] = Reservoir(config.reservoir_size, key_seed)

        self.intensity_thresholds: dict[tuple[float, str, str], float] = {}
        self.trade_event_intensity_thresholds: dict[
            tuple[float, str, str], float
        ] = {}
        self.return_sigmas: dict[tuple[float, str], float] = {}
        self.daily_heat_threshold: float | None = None
        self.profiles_frozen = False

        self.intensities = {
            (half_life, side): DecayedIntensity(half_life)
            for half_life in config.half_lives
            for side in SIDES
        }
        self.trade_event_intensities = {
            (half_life, side): DecayedIntensity(half_life)
            for half_life in config.trade_event_half_lives
            for side in SIDES
        }
        self.trade_sizes = {
            side: deque(maxlen=config.rolling_trade_sizes) for side in SIDES
        }
        self.price_windows = {
            window: PriceReturnWindow(window) for window in config.price_windows
        }

        self.current_date: int | None = None
        self.current_session: str | None = None
        self.previous_row_id: int | None = None
        self.previous_close: float | None = None
        self.open_mid: float | None = None
        self.last_mid: float | None = None
        self.previous_book: tuple[float, float, tuple[int | None, ...], tuple[int | None, ...]] | None = None
        self.event_index = 0
        self.trade_event_index = 0
        self.cumulative_buy_volume = 0.0
        self.cumulative_sell_volume = 0.0
        self.cumulative_buy_count = 0
        self.cumulative_sell_count = 0
        self.pending = {horizon: deque() for horizon in config.horizons}
        self.seen_trade_recids: set[int] = set()

        self.phase = "warmup"
        self.daily_heat = 0.0
        self.daily_depth_by_state: dict[str, RunningMoments] = defaultdict(RunningMoments)
        self.daily_trade_event_depth_by_state: dict[str, RunningMoments] = defaultdict(
            RunningMoments
        )
        self.day_stats: dict[tuple[str, str, str], RunningMoments] = defaultdict(RunningMoments)
        self.day_quality = DayQuality()
        self.orders: dict[tuple[str, int], PassiveOrder] = {}
        self.active_order_ids: set[tuple[str, int]] = set()
        self.stat_rows: list[dict[str, object]] = []
        self.quality_rows: list[dict[str, object]] = []
        self.audit_rows: list[dict[str, object]] = []
        self._audit_event_count = 0
        self._audit_order_count = 0

    def freeze_profiles(self) -> None:
        if self.profiles_frozen:
            return
        for key, sample in self.intensity_samples.items():
            if sample.seen >= self.config.minimum_threshold_samples:
                threshold = sample.q(self.config.intensity_quantile)
                if threshold is not None:
                    self.intensity_thresholds[key] = threshold
        for key, sample in self.trade_event_intensity_samples.items():
            if sample.seen >= self.config.minimum_threshold_samples:
                threshold = sample.q(self.config.intensity_quantile)
                if threshold is not None:
                    self.trade_event_intensity_thresholds[key] = threshold
        for key, sample in self.return_samples.items():
            if sample.seen >= self.config.minimum_threshold_samples and sample.values:
                mean = sum(sample.values) / len(sample.values)
                variance = sum((value - mean) ** 2 for value in sample.values) / len(sample.values)
                if variance > 0:
                    self.return_sigmas[key] = math.sqrt(variance)
        self.daily_heat_threshold = self.daily_heat_sample.q(0.5)
        self.profiles_frozen = True

    def process(self, event: Event, phase: str) -> None:
        if phase not in {"warmup", "target"}:
            raise ValueError(f"invalid phase: {phase}")
        session = session_name(event.time)
        if session is None:
            return
        if phase == "target" and not self.profiles_frozen:
            self._finish_day()
            self.freeze_profiles()
            self.phase = "target"
        elif phase == "warmup" and self.profiles_frozen:
            raise ValueError("warmup events cannot follow target events")

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

        seconds = hhmmssmmm_to_seconds(event.time)
        bucket = time_bucket(event.time, self.config.time_bucket_minutes)
        mid = event.mid
        if self.open_mid is None and mid is not None:
            self.open_mid = mid
        if mid is not None:
            self.last_mid = mid

        is_trade = event.action == "TRADE" and event.side in SIDES and (event.volume or 0) > 0
        duplicate_trade = False
        if is_trade and event.recid is not None:
            if event.recid in self.seen_trade_recids:
                duplicate_trade = True
                self.day_quality.duplicate_trades += 1
            else:
                self.seen_trade_recids.add(event.recid)
        valid_trade = is_trade and not duplicate_trade

        prior_means = {
            side: (sum(values) / len(values) if values else None)
            for side, values in self.trade_sizes.items()
        }
        normalized: dict[tuple[float, str], float | None] = {}
        for half_life in self.config.half_lives:
            for side in SIDES:
                addition = float(event.volume or 0) if valid_trade and event.side == side else 0.0
                raw = self.intensities[(half_life, side)].update(seconds, addition)
                mean_size = prior_means[side]
                normalized[(half_life, side)] = raw / mean_size if mean_size and mean_size > 0 else None
        trade_event_normalized: dict[tuple[float, str], float | None] = {}
        if valid_trade:
            self.trade_event_index += 1
            for half_life in self.config.trade_event_half_lives:
                for side in SIDES:
                    addition = float(event.volume or 0) if event.side == side else 0.0
                    self.trade_event_intensities[(half_life, side)].update(
                        float(self.trade_event_index), addition
                    )
        for half_life in self.config.trade_event_half_lives:
            for side in SIDES:
                raw = self.trade_event_intensities[(half_life, side)].value
                mean_size = prior_means[side]
                trade_event_normalized[(half_life, side)] = (
                    raw / mean_size if mean_size and mean_size > 0 else None
                )
        if valid_trade:
            self.trade_sizes[event.side].append(float(event.volume or 0))

        returns = {
            window: tracker.update(seconds, mid)
            for window, tracker in self.price_windows.items()
        }
        states = {
            half_life: self._intensity_state(half_life, bucket, normalized)
            for half_life in self.config.half_lives
        }
        primary_state = states[self.config.primary_half_life]
        trade_event_states = {
            half_life: self._trade_event_intensity_state(
                half_life, bucket, trade_event_normalized
            )
            for half_life in self.config.trade_event_half_lives
        }
        primary_trade_event_state = trade_event_states[
            self.config.primary_trade_event_half_life
        ]
        price_states = {
            f"w{int(window)}_k{self.config.price_sigma_multiplier:g}": self._price_state(
                window, bucket, returns[window], mid
            )
            for window in self.config.price_windows
        }

        self._update_quality(event)
        if valid_trade:
            if event.side == "B":
                self.cumulative_buy_volume += float(event.volume or 0)
                self.cumulative_buy_count += 1
            else:
                self.cumulative_sell_volume += float(event.volume or 0)
                self.cumulative_sell_count += 1
            self.daily_heat += float(event.volume or 0)

        current_heat = self._heat(normalized, self.config.primary_half_life)
        current_trade_event_heat = self._heat(
            trade_event_normalized, self.config.primary_trade_event_half_life
        )
        if phase == "warmup" and self.event_index % self.config.profile_sample_stride == 0:
            for half_life in self.config.half_lives:
                for side in SIDES:
                    self.intensity_samples[(half_life, bucket, side)].add(
                        normalized[(half_life, side)]
                    )
            for half_life in self.config.trade_event_half_lives:
                for side in SIDES:
                    self.trade_event_intensity_samples[(half_life, bucket, side)].add(
                        trade_event_normalized[(half_life, side)]
                    )
            for window, value in returns.items():
                self.return_samples[(window, bucket)].add(value)
        elif phase == "target":
            self._complete_pending(current_heat, current_trade_event_heat)
            if self.event_index % self.config.depth_sample_stride == 0:
                self._accumulate_depth(states, trade_event_states)
            self._update_orders(
                event, valid_trade, seconds, primary_state, primary_trade_event_state
            )
            if valid_trade and primary_state is not None:
                trigger = PendingTrigger(
                    target_index=0,
                    trigger_side=str(event.side),
                    state=primary_state,
                    trade_event_state=primary_trade_event_state,
                    price_states=price_states,
                    start_buy_volume=self.cumulative_buy_volume,
                    start_sell_volume=self.cumulative_sell_volume,
                    start_buy_count=self.cumulative_buy_count,
                    start_sell_count=self.cumulative_sell_count,
                    start_heat=current_heat,
                    start_trade_event_heat=current_trade_event_heat,
                )
                for horizon in self.config.horizons:
                    pending = PendingTrigger(**{**trigger.__dict__, "target_index": self.event_index + horizon})
                    self.pending[horizon].append(pending)

        self._append_audit(event, normalized, primary_state, price_states)
        current_book = self._book_tuple(event)
        if current_book is not None:
            # Shenzhen can expose an incoming marketable order before all of
            # its child TRADE events have consumed the opposite side. Those
            # intermediate post-event snapshots are crossed. They are not a
            # valid pre-event liquidity state for the next child event, so
            # retain the latest uncrossed snapshot until the chain settles.
            self.previous_book = current_book
        self.event_index += 1

    def finish(self) -> tuple[list[dict[str, object]], list[dict[str, object]], list[dict[str, object]]]:
        self._finish_day()
        if not self.profiles_frozen:
            self.freeze_profiles()
        return self.stat_rows, self.quality_rows, self.audit_rows

    def profile_summary(self) -> dict[str, object]:
        return {
            "symbol": self.symbol,
            "intensity_thresholds": {
                "|".join(map(str, key)): value
                for key, value in sorted(self.intensity_thresholds.items())
            },
            "trade_event_intensity_thresholds": {
                "|".join(map(str, key)): value
                for key, value in sorted(self.trade_event_intensity_thresholds.items())
            },
            "return_sigmas": {
                "|".join(map(str, key)): value
                for key, value in sorted(self.return_sigmas.items())
            },
            "intensity_sample_counts": {
                "|".join(map(str, key)): sample.seen
                for key, sample in sorted(self.intensity_samples.items())
            },
            "trade_event_intensity_sample_counts": {
                "|".join(map(str, key)): sample.seen
                for key, sample in sorted(self.trade_event_intensity_samples.items())
            },
            "return_sample_counts": {
                "|".join(map(str, key)): sample.seen
                for key, sample in sorted(self.return_samples.items())
            },
            "daily_heat_threshold": self.daily_heat_threshold,
            "daily_heat_history_days": self.daily_heat_sample.seen,
        }

    def _start_day(self, date: int) -> None:
        self.current_date = date
        self.current_session = None
        self.previous_row_id = None
        self.open_mid = None
        self.last_mid = None
        self.previous_book = None
        self.daily_heat = 0.0
        self.daily_depth_by_state = defaultdict(RunningMoments)
        self.daily_trade_event_depth_by_state = defaultdict(RunningMoments)
        self.day_stats = defaultdict(RunningMoments)
        self.day_quality = DayQuality()
        self.orders = {}
        self.active_order_ids = set()
        self.seen_trade_recids = set()

    def _start_session(self, session: str) -> None:
        if self.current_session is not None:
            self._discard_pending()
        self.current_session = session
        self.previous_book = None
        self.event_index = 0
        self.trade_event_index = 0
        self.cumulative_buy_volume = 0.0
        self.cumulative_sell_volume = 0.0
        self.cumulative_buy_count = 0
        self.cumulative_sell_count = 0
        for tracker in self.intensities.values():
            tracker.reset()
        for tracker in self.trade_event_intensities.values():
            tracker.reset()
        for tracker in self.price_windows.values():
            tracker.reset()

    def _finish_day(self) -> None:
        if self.current_date is None:
            return
        self._discard_pending()
        if self.phase == "warmup":
            self.daily_heat_sample.add(self.daily_heat)
        else:
            heat_group = (
                "high"
                if self.daily_heat_threshold is not None and self.daily_heat > self.daily_heat_threshold
                else "low"
            )
            for state, moments in self.daily_depth_by_state.items():
                if state == "all":
                    continue
                if moments.mean is not None:
                    self._add_stat("M5", "mean_log_total_depth3", f"heat={heat_group}|state={state}", moments.mean)
            all_depth = self.daily_depth_by_state.get("all")
            if all_depth and all_depth.mean is not None:
                self._add_stat("M5", "mean_log_total_depth3", f"heat={heat_group}|state=all", all_depth.mean)
            for state, moments in self.daily_trade_event_depth_by_state.items():
                if state == "all":
                    continue
                if moments.mean is not None:
                    self._add_stat(
                        "M5", "teh20_mean_log_total_depth3",
                        f"heat={heat_group}|state={state}", moments.mean,
                    )
            trade_all_depth = self.daily_trade_event_depth_by_state.get("all")
            if trade_all_depth and trade_all_depth.mean is not None:
                self._add_stat(
                    "M5", "teh20_mean_log_total_depth3",
                    f"heat={heat_group}|state=all", trade_all_depth.mean,
                )
            self._add_stat("M5", "daily_active_volume", f"heat={heat_group}", self.daily_heat)
            self._finalize_orders()
            for (mechanism, variant, group_key), moments in sorted(self.day_stats.items()):
                self.stat_rows.append(
                    {
                        "symbol": self.symbol,
                        "date": self.current_date,
                        "mechanism": mechanism,
                        "variant": variant,
                        "group_key": group_key,
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
        if self.last_mid is not None:
            self.previous_close = self.last_mid
        self.current_date = None
        self.current_session = None

    def _discard_pending(self) -> None:
        discarded = sum(len(queue) for queue in self.pending.values())
        if self.phase == "target":
            self.day_quality.incomplete_future_labels += discarded
        for queue in self.pending.values():
            queue.clear()

    def _intensity_state(
        self,
        half_life: float,
        bucket: str,
        normalized: dict[tuple[float, str], float | None],
    ) -> str | None:
        values = [normalized[(half_life, side)] for side in SIDES]
        thresholds = [self.intensity_thresholds.get((half_life, bucket, side)) for side in SIDES]
        if any(value is None for value in values) or any(value is None for value in thresholds):
            if self.phase == "target":
                self.day_quality.insufficient_state_history += 1
            return None
        buy_high = int(float(values[0]) > float(thresholds[0]))
        sell_high = int(float(values[1]) > float(thresholds[1]))
        return f"B{buy_high}S{sell_high}"

    def _trade_event_intensity_state(
        self,
        half_life: float,
        bucket: str,
        normalized: dict[tuple[float, str], float | None],
    ) -> str | None:
        values = [normalized[(half_life, side)] for side in SIDES]
        thresholds = [
            self.trade_event_intensity_thresholds.get((half_life, bucket, side))
            for side in SIDES
        ]
        if any(value is None for value in values) or any(
            value is None for value in thresholds
        ):
            return None
        buy_high = int(float(values[0]) > float(thresholds[0]))
        sell_high = int(float(values[1]) > float(thresholds[1]))
        return f"B{buy_high}S{sell_high}"

    def _price_state(
        self,
        window: float,
        bucket: str,
        value: float | None,
        mid: float | None,
    ) -> str:
        sigma = self.return_sigmas.get((window, bucket))
        if value is None or sigma is None or mid is None or self.open_mid is None or self.previous_close is None:
            return "unknown"
        cutoff = self.config.price_sigma_multiplier * sigma
        if value > cutoff and mid > self.open_mid and mid > self.previous_close:
            return "up"
        if value < -cutoff and mid < self.open_mid and mid < self.previous_close:
            return "down"
        return "normal"

    @staticmethod
    def _heat(
        normalized: dict[tuple[float, str], float | None], half_life: float
    ) -> float | None:
        buy = normalized[(half_life, "B")]
        sell = normalized[(half_life, "S")]
        return buy + sell if buy is not None and sell is not None else None

    def _complete_pending(
        self,
        current_heat: float | None,
        current_trade_event_heat: float | None,
    ) -> None:
        for horizon, queue in self.pending.items():
            while queue and queue[0].target_index == self.event_index:
                trigger = queue.popleft()
                buy_volume = self.cumulative_buy_volume - trigger.start_buy_volume
                sell_volume = self.cumulative_sell_volume - trigger.start_sell_volume
                buy_count = self.cumulative_buy_count - trigger.start_buy_count
                sell_count = self.cumulative_sell_count - trigger.start_sell_count
                sign = 1.0 if trigger.trigger_side == "B" else -1.0
                signed_volume = sign * (buy_volume - sell_volume)
                signed_count = sign * (buy_count - sell_count)
                group = f"trigger={trigger.trigger_side}|state={trigger.state}"
                self._add_stat("M1", f"n{horizon}_future_signed_volume", group, signed_volume)
                self._add_stat("M1", f"n{horizon}_future_signed_count", group, signed_count)
                primary_key = f"w{int(self.config.primary_price_window)}_k{self.config.price_sigma_multiplier:g}"
                price_state = trigger.price_states.get(primary_key, "unknown")
                if current_heat is not None and trigger.start_heat is not None:
                    delta_heat = current_heat - trigger.start_heat
                    for price_key, candidate_state in trigger.price_states.items():
                        candidate_group = (
                            f"price={candidate_state}|trigger={trigger.trigger_side}"
                            f"|state={trigger.state}"
                        )
                        self._add_stat(
                            "M2", f"{price_key}_n{horizon}_delta_heat",
                            candidate_group, delta_heat,
                        )
                    m2_group = (
                        f"price={price_state}|trigger={trigger.trigger_side}"
                        f"|state={trigger.state}"
                    )
                    self._add_stat("M2", f"n{horizon}_delta_heat", m2_group, delta_heat)
                    positive_heat = max(delta_heat, 0.0)
                    self._add_stat("M3", f"n{horizon}_positive_heat", "all", positive_heat)
                    if price_state == "up" and trigger.trigger_side == "B":
                        self._add_stat(
                            "M3", f"n{horizon}_positive_heat", "up_buy", positive_heat
                        )
                    if trigger.trigger_side == "B":
                        group_name = "buy_up" if price_state == "up" else "buy_non_up"
                        self._add_stat(
                            "M3", f"n{horizon}_delta_heat", group_name, delta_heat
                        )
                if trigger.trade_event_state is None:
                    continue
                trade_group = (
                    f"trigger={trigger.trigger_side}|state={trigger.trade_event_state}"
                )
                self._add_stat(
                    "M1", f"teh20_n{horizon}_future_signed_volume",
                    trade_group, signed_volume,
                )
                self._add_stat(
                    "M1", f"teh20_n{horizon}_future_signed_count",
                    trade_group, signed_count,
                )
                if (
                    current_trade_event_heat is None
                    or trigger.start_trade_event_heat is None
                ):
                    continue
                trade_delta_heat = (
                    current_trade_event_heat - trigger.start_trade_event_heat
                )
                trade_m2_group = (
                    f"price={price_state}|trigger={trigger.trigger_side}"
                    f"|state={trigger.trade_event_state}"
                )
                self._add_stat(
                    "M2", f"teh20_n{horizon}_delta_heat",
                    trade_m2_group, trade_delta_heat,
                )
                if trigger.trigger_side == "B":
                    trade_group_name = (
                        "buy_up" if price_state == "up" else "buy_non_up"
                    )
                    self._add_stat(
                        "M3", f"teh20_n{horizon}_delta_heat",
                        trade_group_name, trade_delta_heat,
                    )

    def _accumulate_depth(
        self,
        states: dict[float, str | None],
        trade_event_states: dict[float, str | None],
    ) -> None:
        if self.previous_book is None:
            self.day_quality.invalid_prebook += 1
            return
        _bid1, _ask1, bid_depths, ask_depths = self.previous_book
        primary_state = states[self.config.primary_half_life]
        if primary_state is not None:
            bid3, ask3 = bid_depths[1], ask_depths[1]
            if bid3 is not None and ask3 is not None and bid3 >= 0 and ask3 >= 0:
                value = math.log1p(bid3 + ask3)
                self.daily_depth_by_state[primary_state].add(value)
                self.daily_depth_by_state["all"].add(value)
        primary_trade_event_state = trade_event_states[
            self.config.primary_trade_event_half_life
        ]
        if primary_trade_event_state is not None:
            bid3, ask3 = bid_depths[1], ask_depths[1]
            if bid3 is not None and ask3 is not None and bid3 >= 0 and ask3 >= 0:
                value = math.log1p(bid3 + ask3)
                self.daily_trade_event_depth_by_state[primary_trade_event_state].add(
                    value
                )
                self.daily_trade_event_depth_by_state["all"].add(value)
        for half_life, state in states.items():
            if state is None:
                continue
            for depth_index, level in enumerate((1, 3, 10)):
                for side, depths in (("bid", bid_depths), ("ask", ask_depths)):
                    value = depths[depth_index]
                    if value is not None and value >= 0:
                        self._add_stat(
                            "M4",
                            f"hl{half_life:g}_log_depth{level}_{side}",
                            f"state={state}",
                            math.log1p(value),
                        )
        for half_life, state in trade_event_states.items():
            if state is None:
                continue
            for depth_index, level in enumerate((1, 3, 10)):
                for side, depths in (("bid", bid_depths), ("ask", ask_depths)):
                    value = depths[depth_index]
                    if value is not None and value >= 0:
                        self._add_stat(
                            "M4",
                            f"teh{half_life:g}_log_depth{level}_{side}",
                            f"state={state}",
                            math.log1p(value),
                        )

    def _update_orders(
        self,
        event: Event,
        valid_trade: bool,
        seconds: float,
        state: str | None,
        trade_event_state: str | None,
    ) -> None:
        if event.action == "ORDER_ADD" and event.side in SIDES and (event.volume or 0) > 0:
            order_id = event.buy_order_id if event.side == "B" else event.sell_order_id
            order_key = (event.side, order_id) if order_id is not None else None
            if order_key is not None and order_key not in self.orders and self.previous_book is not None:
                distance = self._distance_bucket(event.side, event.price, self.previous_book)
                if distance != "outside":
                    self.orders[order_key] = PassiveOrder(
                        order_id=order_id,
                        side=event.side,
                        submit_qty=int(event.volume or 0),
                        submit_seconds=seconds,
                        state=state or "unknown",
                        trade_event_state=trade_event_state or "unknown",
                        distance_bucket=distance,
                    )
        elif valid_trade:
            active_id = event.buy_order_id if event.side == "B" else event.sell_order_id
            passive_id = event.sell_order_id if event.side == "B" else event.buy_order_id
            active_key = (event.side, active_id) if active_id is not None else None
            passive_side = "S" if event.side == "B" else "B"
            passive_key = (passive_side, passive_id) if passive_id is not None else None
            if active_key is not None:
                self.active_order_ids.add(active_key)
            if passive_key is not None and passive_key in self.orders:
                self.orders[passive_key].add_fill(seconds, int(event.volume or 0))
            elif passive_id is not None:
                self.day_quality.unmatched_passive_trades += 1
        elif event.action == "CANCEL" and event.side in SIDES and (event.volume or 0) > 0:
            order_id = event.buy_order_id if event.side == "B" else event.sell_order_id
            order_key = (event.side, order_id) if order_id is not None else None
            if order_key is not None and order_key in self.orders:
                self.orders[order_key].cancel_qty += int(event.volume or 0)

    def _finalize_orders(self) -> None:
        for order_key, order in self.orders.items():
            if order_key in self.active_order_ids:
                continue
            self.day_quality.passive_orders += 1
            if order.filled_total > order.submit_qty:
                self.day_quality.fill_over_submit += 1
            group = (
                f"side={order.side}|state={order.state}|distance={order.distance_bucket}"
            )
            metrics = {
                "submitted_orders": 1,
                "submitted_volume": order.submit_qty,
                "filled_orders_10s": int(order.filled_10s > 0),
                "filled_orders_60s": int(order.filled_60s > 0),
                "filled_orders_300s": int(order.filled_300s > 0),
                "filled_orders_continuous": int(order.filled_total > 0),
                "filled_volume_10s": order.filled_10s,
                "filled_volume_60s": order.filled_60s,
                "filled_volume_300s": order.filled_300s,
                "filled_volume_continuous": order.filled_total,
                "cancel_volume": order.cancel_qty,
            }
            for variant, value in metrics.items():
                self._add_stat("M6", variant, group, float(value))
                trade_group = (
                    f"side={order.side}|state={order.trade_event_state}"
                    f"|distance={order.distance_bucket}"
                )
                self._add_stat("M6", f"teh20_{variant}", trade_group, float(value))
            if self.current_date in self.config.audit_dates and self._audit_order_count < self.config.audit_max_orders:
                self.audit_rows.append(
                    {
                        "kind": "passive_order",
                        "symbol": self.symbol,
                        "date": self.current_date,
                        **order.__dict__,
                    }
                )
                self._audit_order_count += 1

    def _distance_bucket(
        self,
        side: str,
        price: int | None,
        book: tuple[float, float, tuple[int | None, ...], tuple[int | None, ...]],
    ) -> str:
        if price is None or price <= 0:
            return "outside"
        bid1, ask1, _bid_depths, _ask_depths = book
        mid = (bid1 + ask1) / 2.0
        if mid <= 0:
            return "outside"
        if side == "B":
            distance_bps = (mid - price) / mid * 10_000
            at_best = price == bid1
            marketable = price >= ask1
        else:
            distance_bps = (price - mid) / mid * 10_000
            at_best = price == ask1
            marketable = price <= bid1
        if marketable or distance_bps < 0 or distance_bps > self.config.near_mid_bps:
            return "outside"
        return "best" if at_best else "near_mid"

    def _update_quality(self, event: Event) -> None:
        self.day_quality.total_events += 1
        if event.mid is None:
            self.day_quality.invalid_book += 1
        if event.action == "TRADE":
            if event.side == "B":
                self.day_quality.trade_b += 1
            elif event.side == "S":
                self.day_quality.trade_s += 1
            else:
                self.day_quality.trade_n += 1
        elif event.action == "ORDER_ADD":
            self.day_quality.order_add += 1
        elif event.action == "CANCEL":
            self.day_quality.cancel += 1

    def _append_audit(
        self,
        event: Event,
        normalized: dict[tuple[float, str], float | None],
        state: str | None,
        price_states: dict[str, str],
    ) -> None:
        if event.date not in self.config.audit_dates or self._audit_event_count >= self.config.audit_max_events:
            return
        self.audit_rows.append(
            {
                "kind": "event",
                "symbol": self.symbol,
                "date": event.date,
                "time": event.time,
                "row_id": event.row_id,
                "action": event.action,
                "side": event.side,
                "volume": event.volume,
                "mid": event.mid,
                "pre_bid1": self.previous_book[0] if self.previous_book else None,
                "pre_ask1": self.previous_book[1] if self.previous_book else None,
                "intensity_buy": normalized[(self.config.primary_half_life, "B")],
                "intensity_sell": normalized[(self.config.primary_half_life, "S")],
                "state": state,
                "price_states": price_states,
            }
        )
        self._audit_event_count += 1

    def _add_stat(
        self, mechanism: str, variant: str, group_key: str, value: float
    ) -> None:
        self.day_stats[(mechanism, variant, group_key)].add(float(value))

    @staticmethod
    def _book_tuple(
        event: Event,
    ) -> tuple[float, float, tuple[int | None, ...], tuple[int | None, ...]] | None:
        if event.mid is None or event.bid1 is None or event.ask1 is None:
            return None
        return (
            float(event.bid1),
            float(event.ask1),
            event.bid_depths,
            event.ask_depths,
        )


def rows_mean(rows: Iterable[dict[str, object]]) -> float | None:
    """Return the weighted mean represented by generic sufficient-stat rows."""
    value_sum = sum(float(row["value_sum"]) for row in rows)
    weight_sum = sum(float(row["weight_sum"]) for row in rows)
    return value_sum / weight_sum if weight_sum else None
