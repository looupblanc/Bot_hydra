"""Deterministic event-sourced microstructure state engine.

The engine deliberately exposes one mutation primitive, :meth:`step`.  Batch
replay is only a loop over that primitive, so research and future streaming
processing cannot silently acquire different book or feature semantics.

The supported MBO action contract is intentionally small and explicit:

``A``
    Add a displayed order.
``M``
    Replace the price/size of an existing displayed order.  Per the official
    Databento reconstruction example, an unseen order on ``M`` is inserted.
``C``
    Cancel ``size`` contracts from an existing displayed order; remove it only
    when the remaining size reaches zero.
``F``
    Informational fill marker.  It does not mutate the reconstructed book.
``R``
    Clear the instrument book.  A reset marked ``is_snapshot=True`` is also the
    only operation permitted to recover a stream after a sequence gap.
``T``
    Public trade.  It updates tape, session volume and anchored session VWAP,
    but it does not mutate displayed orders.

Prices and sizes are carried as vendor values.  No economic decision should be
made from a returned state unless ``available_at_ns <= decision_time_ns``;
``step`` enforces that invariant before touching state.
"""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from hashlib import sha256
import json
import math
from typing import Any, Callable, Iterable, Mapping, Sequence


EVENT_ENGINE_SCHEMA = "hydra_microstructure_event_engine_v1"
CHECKPOINT_SCHEMA = "hydra_microstructure_event_checkpoint_v1"
SUPPORTED_ACTIONS = frozenset({"A", "M", "C", "F", "R", "T"})
BOOK_SIDES = frozenset({"B", "A"})
TRADE_SIDES = frozenset({"B", "A", "N"})
F_MAYBE_BAD_BOOK = 0x04
F_SNAPSHOT = 0x20
F_LAST = 0x80


class MicrostructureEventEngineError(RuntimeError):
    """Base class for a fail-closed event-engine error."""


class CausalityViolation(MicrostructureEventEngineError):
    """An event or feature was used before it became available."""


class SequenceGapError(MicrostructureEventEngineError):
    """A contiguous stream sequence was skipped."""


class SnapshotRecoveryRequired(MicrostructureEventEngineError):
    """A stream is blocked until an explicit snapshot reset is applied."""


class EventOrderError(MicrostructureEventEngineError):
    """An old or conflicting event was received."""


class BookStateError(MicrostructureEventEngineError):
    """An MBO mutation is inconsistent with the reconstructed book."""


class CheckpointError(MicrostructureEventEngineError):
    """A checkpoint is corrupt or belongs to another engine contract."""


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def stable_hash(value: Any) -> str:
    return sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _timestamp_ns(value: Any, *, field: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field} must be a timestamp, not bool")
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"{field} is not finite")
        return int(value)
    if isinstance(value, datetime):
        item = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        return int(item.timestamp() * 1_000_000_000)
    if isinstance(value, str):
        text = value.strip().replace("Z", "+00:00")
        try:
            item = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ValueError(f"{field} is not a valid timestamp") from exc
        if item.tzinfo is None:
            item = item.replace(tzinfo=timezone.utc)
        return int(item.timestamp() * 1_000_000_000)
    # numpy.datetime64 and pandas.Timestamp both expose a stable integer view.
    if hasattr(value, "value"):
        try:
            return int(value.value)
        except (TypeError, ValueError, OverflowError):
            pass
    raise ValueError(f"{field} has unsupported timestamp type")


def _normal_side(value: Any) -> str:
    text = str(value if value is not None else "N").strip().upper()
    aliases = {
        "BID": "B",
        "BUY": "B",
        "ASK": "A",
        "SELL": "A",
        "NONE": "N",
        "": "N",
    }
    return aliases.get(text, text)


def _session_from_timestamp(ts_event_ns: int) -> str:
    return datetime.fromtimestamp(ts_event_ns / 1_000_000_000, tz=timezone.utc).date().isoformat()


@dataclass(frozen=True, slots=True)
class MarketEvent:
    """Canonical persisted market event consumed by :class:`MicrostructureEventEngine`."""

    ts_event_ns: int
    available_at_ns: int
    sequence: int
    publisher_id: str
    instrument_id: str
    action: str
    side: str = "N"
    price: float | None = None
    size: int = 0
    order_id: str | None = None
    flags: int = 0
    session_id: str = ""
    is_snapshot: bool = False
    schema: str = "mbo"
    ts_recv_ns: int | None = None

    @classmethod
    def from_record(cls, value: MarketEvent | Mapping[str, Any]) -> MarketEvent:
        if isinstance(value, cls):
            return value
        if not isinstance(value, Mapping):
            raise TypeError("market event must be MarketEvent or a mapping")

        ts_event = value.get("ts_event_ns", value.get("ts_event"))
        available = value.get(
            "available_at_ns",
            value.get("available_at", value.get("ts_recv_ns", value.get("ts_recv"))),
        )
        if ts_event is None or available is None:
            raise ValueError("market event requires ts_event and available_at/ts_recv")
        ts_recv = value.get("ts_recv_ns", value.get("ts_recv"))
        raw_order_id = value.get("order_id")
        raw_price = value.get("price")
        raw_size = value.get("size", 0)
        if isinstance(raw_size, bool) or int(raw_size) != raw_size:
            raise ValueError("market event size must be an integer")
        return cls(
            ts_event_ns=_timestamp_ns(ts_event, field="ts_event"),
            available_at_ns=_timestamp_ns(available, field="available_at"),
            sequence=int(value["sequence"]),
            publisher_id=str(value.get("publisher_id", value.get("publisher", ""))),
            instrument_id=str(value.get("instrument_id", value.get("instrument", ""))),
            action=str(value["action"]).strip().upper(),
            side=_normal_side(value.get("side", "N")),
            price=None if raw_price is None else float(raw_price),
            size=int(raw_size),
            order_id=None if raw_order_id is None else str(raw_order_id),
            flags=int(value.get("flags", 0)),
            session_id=str(value.get("session_id", "")),
            is_snapshot=bool(value.get("is_snapshot", False)),
            schema=str(value.get("schema", "mbo")).lower(),
            ts_recv_ns=None
            if ts_recv is None
            else _timestamp_ns(ts_recv, field="ts_recv"),
        )

    def validated(self) -> MarketEvent:
        action = self.action.upper()
        side = _normal_side(self.side)
        if action not in SUPPORTED_ACTIONS:
            raise ValueError(f"unsupported MBO action: {action}")
        if self.ts_event_ns < 0 or self.available_at_ns < 0:
            raise ValueError("timestamps must be non-negative")
        if self.available_at_ns < self.ts_event_ns:
            raise CausalityViolation("event is available before its source event time")
        if self.ts_recv_ns is not None:
            if self.ts_recv_ns < self.ts_event_ns:
                raise CausalityViolation("ts_recv precedes ts_event")
            if self.available_at_ns < self.ts_recv_ns:
                raise CausalityViolation("available_at precedes ts_recv")
        if self.sequence < 0:
            raise ValueError("sequence must be non-negative")
        if self.flags < 0:
            raise ValueError("flags must be non-negative")
        if self.flags & F_MAYBE_BAD_BOOK:
            raise BookStateError("F_MAYBE_BAD_BOOK marks the stream unrecoverable")
        snapshot_flag = bool(self.flags & F_SNAPSHOT)
        if self.is_snapshot and not snapshot_flag:
            raise ValueError("snapshot marker is not authenticated by F_SNAPSHOT")
        if snapshot_flag and action not in {"R", "A"}:
            raise BookStateError(
                f"unexpected {action} action inside an authenticated snapshot"
            )
        if not self.publisher_id or not self.instrument_id:
            raise ValueError("publisher and instrument are required")
        if self.size < 0:
            raise ValueError("size must be non-negative")
        if self.price is not None and not math.isfinite(self.price):
            raise ValueError("price must be finite")
        if action in {"A", "M"}:
            if self.order_id is None or side not in BOOK_SIDES:
                raise ValueError(f"{action} requires order_id and book side")
            if self.price is None or self.price <= 0 or self.size <= 0:
                raise ValueError(f"{action} requires positive price and size")
        elif action == "C":
            if self.order_id is None or self.size <= 0:
                raise ValueError("C requires order_id and positive cancelled size")
        elif action == "F":
            # Databento's official reconstruction treats F as informational;
            # feeds may omit a usable displayed-order identity on this marker.
            pass
        elif action == "T":
            if side not in TRADE_SIDES:
                raise ValueError("T requires aggressor side or neutral/unknown side")
            if self.price is None or self.price <= 0 or self.size <= 0:
                raise ValueError("T requires positive price and size")
        elif action == "R" and self.order_id is not None:
            raise ValueError("R must not carry an order_id")
        return replace(
            self,
            action=action,
            side=side,
            is_snapshot=snapshot_flag,
            session_id=self.session_id or _session_from_timestamp(self.ts_event_ns),
        )

    def to_record(self) -> dict[str, Any]:
        return asdict(self)

    @property
    def fingerprint(self) -> str:
        return stable_hash(self.to_record())


@dataclass(frozen=True, slots=True)
class BookView:
    bid_price: float | None
    bid_size: int
    ask_price: float | None
    ask_size: int
    spread: float | None
    microprice: float | None
    bid_depth: tuple[tuple[float, int], ...]
    ask_depth: tuple[tuple[float, int], ...]
    order_count: int


@dataclass(frozen=True, slots=True)
class TapeView:
    trade_count: int
    buy_volume: int
    sell_volume: int
    total_volume: int
    notional: float
    vwap: float | None
    last_trade_price: float | None


@dataclass(frozen=True, slots=True)
class SessionView:
    session_id: str
    event_count: int
    trade_count: int
    buy_volume: int
    sell_volume: int
    volume: int
    notional: float
    vwap: float | None


@dataclass(frozen=True, slots=True)
class EventResult:
    event_fingerprint: str
    decision_time_ns: int
    publisher_id: str
    instrument_id: str
    sequence: int
    action: str
    book: BookView
    tape: TapeView
    session: SessionView
    chain_hash: str
    state_hash: str
    state_hash_scope: str
    duplicate: bool = False

    def to_record(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_record(cls, value: Mapping[str, Any]) -> EventResult:
        return cls(
            event_fingerprint=str(value["event_fingerprint"]),
            decision_time_ns=int(value["decision_time_ns"]),
            publisher_id=str(value["publisher_id"]),
            instrument_id=str(value["instrument_id"]),
            sequence=int(value["sequence"]),
            action=str(value["action"]),
            book=BookView(
                **{
                    **value["book"],
                    "bid_depth": tuple(tuple(item) for item in value["book"]["bid_depth"]),
                    "ask_depth": tuple(tuple(item) for item in value["book"]["ask_depth"]),
                }
            ),
            tape=TapeView(**value["tape"]),
            session=SessionView(**value["session"]),
            chain_hash=str(value["chain_hash"]),
            state_hash=str(value["state_hash"]),
            state_hash_scope=str(value["state_hash_scope"]),
            duplicate=bool(value.get("duplicate", False)),
        )


@dataclass(frozen=True, slots=True)
class CompactEventResult:
    """Accepted event identity without an O(book-size) state materialization."""

    event_fingerprint: str
    decision_time_ns: int
    publisher_id: str
    instrument_id: str
    sequence: int
    action: str
    chain_hash: str
    duplicate: bool = False

    def to_record(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_record(cls, value: Mapping[str, Any]) -> CompactEventResult:
        return cls(
            event_fingerprint=str(value["event_fingerprint"]),
            decision_time_ns=int(value["decision_time_ns"]),
            publisher_id=str(value["publisher_id"]),
            instrument_id=str(value["instrument_id"]),
            sequence=int(value["sequence"]),
            action=str(value["action"]),
            chain_hash=str(value["chain_hash"]),
            duplicate=bool(value.get("duplicate", False)),
        )


@dataclass(frozen=True, slots=True)
class CompactBatchResult:
    """Bounded materializations produced by a high-throughput replay."""

    input_count: int
    applied_event_count: int
    duplicate_event_count: int
    snapshots: tuple[EventResult, ...]
    final_state_hash: str


@dataclass(slots=True)
class _Order:
    order_id: str
    side: str
    price: float
    size: int


@dataclass(slots=True)
class _SequenceState:
    last_sequence: int | None = None
    blocked: bool = False
    expected_sequence: int | None = None
    chain_hash: str = ""
    snapshot_in_progress: bool = False
    snapshot_complete: bool = False
    awaiting_live_sequence: bool = False
    snapshot_count: int = 0


class _InstrumentState:
    def __init__(self) -> None:
        self.orders: dict[str, _Order] = {}
        self.bids: dict[float, int] = {}
        self.asks: dict[float, int] = {}
        self.trade_count = 0
        self.buy_volume = 0
        self.sell_volume = 0
        self.total_volume = 0
        self.notional = 0.0
        self.last_trade_price: float | None = None
        self.session_id = ""
        self.session_event_count = 0
        self.session_trade_count = 0
        self.session_buy_volume = 0
        self.session_sell_volume = 0
        self.session_volume = 0
        self.session_notional = 0.0

    def level_map(self, side: str) -> dict[float, int]:
        return self.bids if side == "B" else self.asks

    def adjust_level(self, side: str, price: float, delta: int) -> None:
        levels = self.level_map(side)
        next_size = levels.get(price, 0) + delta
        if next_size < 0:
            raise BookStateError("price-level size became negative")
        if next_size == 0:
            levels.pop(price, None)
        else:
            levels[price] = next_size

    def clear_book(self) -> None:
        self.orders.clear()
        self.bids.clear()
        self.asks.clear()

    def ensure_session(self, session_id: str) -> None:
        if self.session_id == session_id:
            return
        self.session_id = session_id
        self.session_event_count = 0
        self.session_trade_count = 0
        self.session_buy_volume = 0
        self.session_sell_volume = 0
        self.session_volume = 0
        self.session_notional = 0.0


class MicrostructureEventEngine:
    """Single-source MBO/MBP/TBBO state engine for batch and streaming use."""

    def __init__(
        self,
        *,
        depth_levels: int = 10,
        recent_event_limit: int = 4_096,
        strict_contiguous_sequence: bool = True,
    ) -> None:
        if depth_levels <= 0:
            raise ValueError("depth_levels must be positive")
        if recent_event_limit <= 0:
            raise ValueError("recent_event_limit must be positive")
        self.depth_levels = int(depth_levels)
        self.recent_event_limit = int(recent_event_limit)
        self.strict_contiguous_sequence = bool(strict_contiguous_sequence)
        self._instruments: dict[str, _InstrumentState] = {}
        self._sequences: dict[str, _SequenceState] = {}
        self._recent: OrderedDict[
            str, EventResult | CompactEventResult
        ] = OrderedDict()

    @staticmethod
    def _key(publisher_id: str, instrument_id: str) -> str:
        return f"{publisher_id}\u001f{instrument_id}"

    def step(
        self,
        record: MarketEvent | Mapping[str, Any],
        *,
        decision_time_ns: int | None = None,
        materialize: bool = True,
        full_state_hash: bool = False,
    ) -> EventResult | CompactEventResult:
        """Apply one available event and return the resulting deterministic state.

        A failed causality or book mutation check never advances the sequence.
        A detected sequence gap intentionally blocks that stream until an
        explicit ``R`` snapshot event is supplied.  With ``materialize=False``
        the mutation remains identical, but no depth sort or whole-state hash is
        computed; this is the intended path for full MBO tapes.
        """

        event = MarketEvent.from_record(record).validated()
        decision_time = (
            event.available_at_ns
            if decision_time_ns is None
            else _timestamp_ns(decision_time_ns, field="decision_time")
        )
        compact = self._step_core(event, decision_time=decision_time)
        if not materialize:
            if isinstance(compact, EventResult):
                return CompactEventResult(
                    event_fingerprint=compact.event_fingerprint,
                    decision_time_ns=compact.decision_time_ns,
                    publisher_id=compact.publisher_id,
                    instrument_id=compact.instrument_id,
                    sequence=compact.sequence,
                    action=compact.action,
                    chain_hash=compact.chain_hash,
                    duplicate=compact.duplicate,
                )
            return compact
        if isinstance(compact, EventResult):
            return compact
        # A compact duplicate may refer to a deliberately non-materialized past
        # state.  Do not manufacture a historical BookView from current state.
        if compact.duplicate:
            return compact
        return self.materialize(compact, full_state_hash=full_state_hash)

    def _step_core(
        self,
        event: MarketEvent,
        *,
        decision_time: int,
    ) -> CompactEventResult | EventResult:
        """The sole market-state mutation core used by all replay modes."""

        if event.available_at_ns > decision_time:
            raise CausalityViolation("event was used before available_at")

        fingerprint = event.fingerprint
        cached = self._recent.get(fingerprint)
        if cached is not None:
            self._recent.move_to_end(fingerprint)
            return replace(cached, duplicate=True)

        key = self._key(event.publisher_id, event.instrument_id)
        sequence = self._sequences.setdefault(key, _SequenceState())
        recovery_reset = event.action == "R" and event.is_snapshot
        snapshot_last = event.is_snapshot and bool(event.flags & F_LAST)

        if sequence.blocked and not recovery_reset:
            raise SnapshotRecoveryRequired(
                f"stream {key} requires snapshot reset; expected sequence "
                f"{sequence.expected_sequence}"
            )
        if event.is_snapshot:
            if event.action == "R":
                # Historical MBO snapshots are an authenticated new base. Their
                # clear record conventionally restarts sequence at zero, even
                # after a live sequence with a much larger value. Snapshot Add
                # records preserve queue priority and are not required to be
                # monotonic venue events, so sequence checks resume only after
                # F_LAST and the first following non-snapshot event.
                pass
            elif not sequence.snapshot_in_progress:
                raise SnapshotRecoveryRequired(
                    f"snapshot Add for {key} arrived without an authenticated "
                    "snapshot clear"
                )
        elif sequence.snapshot_in_progress:
            raise SnapshotRecoveryRequired(
                f"stream {key} emitted a live event before snapshot F_LAST"
            )
        elif sequence.awaiting_live_sequence:
            # The first live event establishes the post-snapshot venue sequence
            # baseline. It may skip sequence values represented by the
            # synthetic snapshot, but it may never regress behind the F_LAST
            # record that completed that image.
            if (
                sequence.last_sequence is not None
                and event.sequence <= sequence.last_sequence
            ):
                raise EventOrderError(
                    f"post-snapshot sequence {event.sequence} is not newer than "
                    f"{sequence.last_sequence} for {key}"
                )
        elif sequence.last_sequence is not None:
            sequence_regressed = (
                event.sequence < sequence.last_sequence
                or (
                    self.strict_contiguous_sequence
                    and event.sequence == sequence.last_sequence
                )
            )
            if sequence_regressed:
                raise EventOrderError(
                    f"sequence {event.sequence} is not newer than "
                    f"{sequence.last_sequence} for {key}"
                )
            expected = sequence.last_sequence + 1
            if self.strict_contiguous_sequence and event.sequence != expected:
                sequence.blocked = True
                sequence.expected_sequence = expected
                raise SequenceGapError(
                    f"sequence gap for {key}: expected {expected}, got {event.sequence}"
                )

        state = self._instruments.setdefault(key, _InstrumentState())
        self._prevalidate_mutation(state, event)
        state.ensure_session(event.session_id)
        self._apply(state, event)
        state.session_event_count += 1

        if recovery_reset:
            sequence.snapshot_in_progress = True
            sequence.snapshot_complete = False
            sequence.awaiting_live_sequence = False
            sequence.snapshot_count += 1
        if snapshot_last:
            sequence.snapshot_in_progress = False
            sequence.snapshot_complete = True
            sequence.awaiting_live_sequence = True
        elif not event.is_snapshot:
            sequence.awaiting_live_sequence = False
        sequence.last_sequence = event.sequence
        sequence.blocked = False
        sequence.expected_sequence = (
            None
            if sequence.snapshot_in_progress or sequence.awaiting_live_sequence
            else event.sequence + 1
        )
        prior_chain = sequence.chain_hash or stable_hash(
            {"schema": EVENT_ENGINE_SCHEMA, "stream": key, "chain": "GENESIS"}
        )
        sequence.chain_hash = stable_hash(
            {"previous_chain_hash": prior_chain, "event_fingerprint": fingerprint}
        )

        result = CompactEventResult(
            event_fingerprint=fingerprint,
            decision_time_ns=decision_time,
            publisher_id=event.publisher_id,
            instrument_id=event.instrument_id,
            sequence=event.sequence,
            action=event.action,
            chain_hash=sequence.chain_hash,
        )
        self._recent[fingerprint] = result
        self._recent.move_to_end(fingerprint)
        while len(self._recent) > self.recent_event_limit:
            self._recent.popitem(last=False)
        return result

    def materialize(
        self,
        result: CompactEventResult,
        *,
        full_state_hash: bool = False,
    ) -> EventResult:
        """Materialize the current state immediately after a compact step.

        Callers must invoke this before applying a later event if they need the
        state *at* ``result``.  :meth:`process_batch_compact` enforces that rule
        internally at every requested cadence.
        """

        if result.duplicate:
            raise EventOrderError("cannot materialize a past compact duplicate")
        key = self._key(result.publisher_id, result.instrument_id)
        sequence = self._sequences.get(key)
        if (
            sequence is None
            or sequence.last_sequence != result.sequence
            or sequence.chain_hash != result.chain_hash
        ):
            raise EventOrderError("compact result is not the current stream state")
        state = self._instruments[key]
        snapshot_hash = self.state_hash() if full_state_hash else result.chain_hash
        full = EventResult(
            event_fingerprint=result.event_fingerprint,
            decision_time_ns=result.decision_time_ns,
            publisher_id=result.publisher_id,
            instrument_id=result.instrument_id,
            sequence=result.sequence,
            action=result.action,
            book=self._book_view(state),
            tape=self._tape_view(state),
            session=self._session_view(state),
            chain_hash=result.chain_hash,
            state_hash=snapshot_hash,
            state_hash_scope=(
                "FULL_ENGINE_STATE" if full_state_hash else "STREAM_EVENT_CHAIN"
            ),
        )
        self._recent[result.event_fingerprint] = full
        self._recent.move_to_end(result.event_fingerprint)
        return full

    def process_batch(
        self,
        records: Iterable[MarketEvent | Mapping[str, Any]],
    ) -> tuple[EventResult, ...]:
        """Replay a batch through the same one-event mutation primitive."""

        results: list[EventResult] = []
        for record in records:
            result = self.step(record)
            if not isinstance(result, EventResult):
                raise EventOrderError(
                    "materialized batch contains a compact duplicate from an "
                    "earlier non-materialized replay"
                )
            results.append(result)
        return tuple(results)

    def process_batch_compact(
        self,
        records: Iterable[MarketEvent | Mapping[str, Any]],
        *,
        snapshot_every_ns: int | None = None,
        callback: Callable[[EventResult], None] | None = None,
        materialize_final: bool = True,
    ) -> CompactBatchResult:
        """Replay a large tape without building a view/hash for every event.

        When ``snapshot_every_ns`` is set, the first accepted event is
        materialized and establishes the cadence; subsequent materializations
        occur on the first event at or after each interval.  The last accepted
        event is additionally materialized by default.  A callback can persist
        each bounded snapshot without retaining per-event states.
        """

        if snapshot_every_ns is not None and snapshot_every_ns <= 0:
            raise ValueError("snapshot_every_ns must be positive")
        snapshots: list[EventResult] = []
        input_count = 0
        applied_count = 0
        duplicate_count = 0
        next_snapshot_ns: int | None = None
        last_applied: CompactEventResult | None = None

        for record in records:
            input_count += 1
            event = MarketEvent.from_record(record).validated()
            compact_or_cached = self._step_core(
                event,
                decision_time=event.available_at_ns,
            )
            if compact_or_cached.duplicate:
                duplicate_count += 1
                continue
            applied_count += 1
            compact = (
                CompactEventResult(
                    event_fingerprint=compact_or_cached.event_fingerprint,
                    decision_time_ns=compact_or_cached.decision_time_ns,
                    publisher_id=compact_or_cached.publisher_id,
                    instrument_id=compact_or_cached.instrument_id,
                    sequence=compact_or_cached.sequence,
                    action=compact_or_cached.action,
                    chain_hash=compact_or_cached.chain_hash,
                )
                if isinstance(compact_or_cached, EventResult)
                else compact_or_cached
            )
            last_applied = compact

            due = False
            if snapshot_every_ns is not None:
                if next_snapshot_ns is None:
                    due = True
                    next_snapshot_ns = event.available_at_ns + snapshot_every_ns
                elif event.available_at_ns >= next_snapshot_ns:
                    due = True
                    intervals = (
                        (event.available_at_ns - next_snapshot_ns)
                        // snapshot_every_ns
                    ) + 1
                    next_snapshot_ns += intervals * snapshot_every_ns
            if due:
                full = self.materialize(compact)
                snapshots.append(full)
                if callback is not None:
                    callback(full)

        if (
            materialize_final
            and last_applied is not None
            and (
                not snapshots
                or snapshots[-1].event_fingerprint
                != last_applied.event_fingerprint
            )
        ):
            full = self.materialize(last_applied)
            snapshots.append(full)
            if callback is not None:
                callback(full)

        # One full deterministic reconciliation at the batch boundary; cadence
        # snapshots above remain bounded O(1) event-chain hashes.
        final_hash = self.state_hash()
        return CompactBatchResult(
            input_count=input_count,
            applied_event_count=applied_count,
            duplicate_event_count=duplicate_count,
            snapshots=tuple(snapshots),
            final_state_hash=final_hash,
        )

    @classmethod
    def replay_batch(
        cls,
        records: Iterable[MarketEvent | Mapping[str, Any]],
        **engine_kwargs: Any,
    ) -> tuple[MicrostructureEventEngine, tuple[EventResult, ...]]:
        engine = cls(**engine_kwargs)
        return engine, engine.process_batch(records)

    @staticmethod
    def _prevalidate_mutation(state: _InstrumentState, event: MarketEvent) -> None:
        """Validate all fallible book conditions before session state is touched."""

        if event.action == "A" and event.order_id in state.orders:
            raise BookStateError(f"duplicate add for order {event.order_id}")
        if event.action == "C":
            order = state.orders.get(str(event.order_id))
            if order is None:
                raise BookStateError(f"cancel for unknown order {event.order_id}")
            if event.size > order.size:
                raise BookStateError(
                    f"cancel size {event.size} exceeds order size {order.size}"
                )

    def _apply(self, state: _InstrumentState, event: MarketEvent) -> None:
        if event.action == "R":
            state.clear_book()
            return
        if event.action == "A":
            assert event.order_id is not None
            assert event.price is not None
            order = _Order(event.order_id, event.side, event.price, event.size)
            state.orders[event.order_id] = order
            state.adjust_level(order.side, order.price, order.size)
            return
        if event.action == "M":
            assert event.order_id is not None
            assert event.price is not None
            order = state.orders.get(event.order_id)
            if order is not None:
                state.adjust_level(order.side, order.price, -order.size)
            next_order = _Order(event.order_id, event.side, event.price, event.size)
            state.orders[event.order_id] = next_order
            state.adjust_level(next_order.side, next_order.price, next_order.size)
            return
        if event.action == "C":
            assert event.order_id is not None
            order = state.orders.get(event.order_id)
            assert order is not None
            state.adjust_level(order.side, order.price, -event.size)
            remaining = order.size - event.size
            if remaining == 0:
                del state.orders[event.order_id]
            else:
                state.orders[event.order_id] = _Order(
                    order.order_id, order.side, order.price, remaining
                )
            return
        if event.action == "F":
            # Official Databento reconstruction semantics: F and T are
            # informational and do not change displayed order state.
            return
        if event.action == "T":
            assert event.price is not None
            state.trade_count += 1
            state.total_volume += event.size
            state.notional += event.price * event.size
            state.last_trade_price = event.price
            state.session_trade_count += 1
            state.session_volume += event.size
            state.session_notional += event.price * event.size
            if event.side == "B":
                state.buy_volume += event.size
                state.session_buy_volume += event.size
            elif event.side == "A":
                state.sell_volume += event.size
                state.session_sell_volume += event.size
            return
        raise AssertionError(f"unreachable action {event.action}")

    def _book_view(self, state: _InstrumentState) -> BookView:
        bids = tuple(sorted(state.bids.items(), reverse=True)[: self.depth_levels])
        asks = tuple(sorted(state.asks.items())[: self.depth_levels])
        bid_price, bid_size = bids[0] if bids else (None, 0)
        ask_price, ask_size = asks[0] if asks else (None, 0)
        spread = (
            None
            if bid_price is None or ask_price is None
            else float(ask_price - bid_price)
        )
        total_bbo_size = bid_size + ask_size
        microprice = (
            None
            if bid_price is None or ask_price is None or total_bbo_size <= 0
            else float((ask_price * bid_size + bid_price * ask_size) / total_bbo_size)
        )
        return BookView(
            bid_price=bid_price,
            bid_size=bid_size,
            ask_price=ask_price,
            ask_size=ask_size,
            spread=spread,
            microprice=microprice,
            bid_depth=bids,
            ask_depth=asks,
            order_count=len(state.orders),
        )

    @staticmethod
    def _tape_view(state: _InstrumentState) -> TapeView:
        return TapeView(
            trade_count=state.trade_count,
            buy_volume=state.buy_volume,
            sell_volume=state.sell_volume,
            total_volume=state.total_volume,
            notional=state.notional,
            vwap=None
            if state.total_volume == 0
            else state.notional / state.total_volume,
            last_trade_price=state.last_trade_price,
        )

    @staticmethod
    def _session_view(state: _InstrumentState) -> SessionView:
        return SessionView(
            session_id=state.session_id,
            event_count=state.session_event_count,
            trade_count=state.session_trade_count,
            buy_volume=state.session_buy_volume,
            sell_volume=state.session_sell_volume,
            volume=state.session_volume,
            notional=state.session_notional,
            vwap=None
            if state.session_volume == 0
            else state.session_notional / state.session_volume,
        )

    def state_hash(self) -> str:
        """Hash economic state (the duplicate cache is intentionally excluded)."""

        return stable_hash(self._state_payload())

    def _state_payload(self) -> dict[str, Any]:
        instruments: dict[str, Any] = {}
        for key, state in sorted(self._instruments.items()):
            instruments[key] = {
                "orders": [
                    {
                        "order_id": order.order_id,
                        "side": order.side,
                        "price": order.price,
                        "size": order.size,
                    }
                    for _, order in sorted(state.orders.items())
                ],
                "bids": [[price, size] for price, size in sorted(state.bids.items())],
                "asks": [[price, size] for price, size in sorted(state.asks.items())],
                "trade_count": state.trade_count,
                "buy_volume": state.buy_volume,
                "sell_volume": state.sell_volume,
                "total_volume": state.total_volume,
                "notional": state.notional,
                "last_trade_price": state.last_trade_price,
                "session_id": state.session_id,
                "session_event_count": state.session_event_count,
                "session_trade_count": state.session_trade_count,
                "session_buy_volume": state.session_buy_volume,
                "session_sell_volume": state.session_sell_volume,
                "session_volume": state.session_volume,
                "session_notional": state.session_notional,
            }
        return {
            "schema": EVENT_ENGINE_SCHEMA,
            "depth_levels": self.depth_levels,
            "strict_contiguous_sequence": self.strict_contiguous_sequence,
            "instruments": instruments,
            "sequences": {
                key: {
                    "last_sequence": state.last_sequence,
                    "blocked": state.blocked,
                    "expected_sequence": state.expected_sequence,
                    "chain_hash": state.chain_hash,
                    "snapshot_in_progress": state.snapshot_in_progress,
                    "snapshot_complete": state.snapshot_complete,
                    "awaiting_live_sequence": state.awaiting_live_sequence,
                    "snapshot_count": state.snapshot_count,
                }
                for key, state in sorted(self._sequences.items())
            },
        }

    def checkpoint(self) -> dict[str, Any]:
        """Return a self-verifying checkpoint, including the idempotence window."""

        payload = {
            "checkpoint_schema": CHECKPOINT_SCHEMA,
            "engine_state": self._state_payload(),
            "recent_event_limit": self.recent_event_limit,
            "recent_results": [
                [
                    fingerprint,
                    "materialized"
                    if isinstance(result, EventResult)
                    else "compact",
                    result.to_record(),
                ]
                for fingerprint, result in self._recent.items()
            ],
        }
        return {**payload, "checkpoint_hash": stable_hash(payload)}

    @classmethod
    def restore(cls, checkpoint: Mapping[str, Any]) -> MicrostructureEventEngine:
        payload = {key: value for key, value in checkpoint.items() if key != "checkpoint_hash"}
        if checkpoint.get("checkpoint_schema") != CHECKPOINT_SCHEMA:
            raise CheckpointError("checkpoint schema mismatch")
        if checkpoint.get("checkpoint_hash") != stable_hash(payload):
            raise CheckpointError("checkpoint hash mismatch")
        raw_state = checkpoint.get("engine_state")
        if not isinstance(raw_state, Mapping) or raw_state.get("schema") != EVENT_ENGINE_SCHEMA:
            raise CheckpointError("event-engine state schema mismatch")
        engine = cls(
            depth_levels=int(raw_state["depth_levels"]),
            recent_event_limit=int(checkpoint["recent_event_limit"]),
            strict_contiguous_sequence=bool(raw_state["strict_contiguous_sequence"]),
        )
        for key, raw in raw_state["instruments"].items():
            state = _InstrumentState()
            for item in raw["orders"]:
                order = _Order(
                    order_id=str(item["order_id"]),
                    side=str(item["side"]),
                    price=float(item["price"]),
                    size=int(item["size"]),
                )
                state.orders[order.order_id] = order
            state.bids = {float(price): int(size) for price, size in raw["bids"]}
            state.asks = {float(price): int(size) for price, size in raw["asks"]}
            state.trade_count = int(raw["trade_count"])
            state.buy_volume = int(raw["buy_volume"])
            state.sell_volume = int(raw["sell_volume"])
            state.total_volume = int(raw["total_volume"])
            state.notional = float(raw["notional"])
            state.last_trade_price = (
                None if raw["last_trade_price"] is None else float(raw["last_trade_price"])
            )
            state.session_id = str(raw["session_id"])
            state.session_event_count = int(raw["session_event_count"])
            state.session_trade_count = int(raw["session_trade_count"])
            state.session_buy_volume = int(raw["session_buy_volume"])
            state.session_sell_volume = int(raw["session_sell_volume"])
            state.session_volume = int(raw["session_volume"])
            state.session_notional = float(raw["session_notional"])
            engine._instruments[str(key)] = state
        for key, raw in raw_state["sequences"].items():
            engine._sequences[str(key)] = _SequenceState(
                last_sequence=None
                if raw["last_sequence"] is None
                else int(raw["last_sequence"]),
                blocked=bool(raw["blocked"]),
                expected_sequence=None
                if raw["expected_sequence"] is None
                else int(raw["expected_sequence"]),
                chain_hash=str(raw.get("chain_hash") or ""),
                snapshot_in_progress=bool(raw.get("snapshot_in_progress", False)),
                snapshot_complete=bool(raw.get("snapshot_complete", False)),
                awaiting_live_sequence=bool(raw.get("awaiting_live_sequence", False)),
                snapshot_count=int(raw.get("snapshot_count", 0)),
            )
        for fingerprint, kind, raw_result in checkpoint.get("recent_results", []):
            if kind == "materialized":
                result: EventResult | CompactEventResult = EventResult.from_record(
                    raw_result
                )
            elif kind == "compact":
                result = CompactEventResult.from_record(raw_result)
            else:
                raise CheckpointError("recent-result kind mismatch")
            if fingerprint != result.event_fingerprint:
                raise CheckpointError("recent-result fingerprint mismatch")
            engine._recent[str(fingerprint)] = result
        if engine.state_hash() != stable_hash(raw_state):
            # raw_state hashes itself through the same canonical representation.
            raise CheckpointError("restored event-engine state drift")
        return engine

    def stream_status(self, publisher_id: str, instrument_id: str) -> dict[str, Any]:
        key = self._key(str(publisher_id), str(instrument_id))
        state = self._sequences.get(key, _SequenceState())
        return {
            "last_sequence": state.last_sequence,
            "blocked": state.blocked,
            "expected_sequence": state.expected_sequence,
            "snapshot_in_progress": state.snapshot_in_progress,
            "snapshot_complete": state.snapshot_complete,
            "book_ready": state.snapshot_complete and not state.snapshot_in_progress,
            "snapshot_count": state.snapshot_count,
        }


def adapt_depth_snapshot(
    *,
    ts_event_ns: int,
    available_at_ns: int,
    base_sequence: int,
    publisher_id: str,
    instrument_id: str,
    bids: Sequence[tuple[float, int]],
    asks: Sequence[tuple[float, int]],
    session_id: str = "",
    schema: str = "mbp-1",
    flags: int = 0,
) -> tuple[MarketEvent, ...]:
    """Adapt an atomic TBBO/MBP snapshot to deterministic synthetic MBO events.

    Aggregated levels have no vendor order IDs.  The adapter therefore creates
    clearly namespaced synthetic IDs.  They are suitable for BBO/depth feature
    reconstruction, but never for exact MBO queue-position claims.
    """

    normalized_schema = schema.lower()
    if normalized_schema not in {"tbbo", "mbp-1", "mbp-10"}:
        raise ValueError("snapshot adapter supports tbbo, mbp-1 or mbp-10")
    snapshot_flags = int(flags) | F_SNAPSHOT
    levels = tuple(
        (side, depth, float(price), int(size))
        for side, values in (("B", bids), ("A", asks))
        for depth, (price, size) in enumerate(values)
        if int(size) > 0
    )
    events: list[MarketEvent] = [
        MarketEvent(
            ts_event_ns=int(ts_event_ns),
            available_at_ns=int(available_at_ns),
            sequence=int(base_sequence),
            publisher_id=str(publisher_id),
            instrument_id=str(instrument_id),
            action="R",
            flags=snapshot_flags | (F_LAST if not levels else 0),
            session_id=session_id,
            is_snapshot=True,
            schema=normalized_schema,
        )
    ]
    sequence = int(base_sequence)
    for position, (side, depth, price, size) in enumerate(levels):
            sequence += 1
            events.append(
                MarketEvent(
                    ts_event_ns=int(ts_event_ns),
                    available_at_ns=int(available_at_ns),
                    sequence=sequence,
                    publisher_id=str(publisher_id),
                    instrument_id=str(instrument_id),
                    action="A",
                    side=side,
                    price=float(price),
                    size=int(size),
                    order_id=(
                        f"snapshot:{publisher_id}:{instrument_id}:"
                        f"{base_sequence}:{side}:{depth}"
                    ),
                    flags=snapshot_flags | (
                        F_LAST if position == len(levels) - 1 else 0
                    ),
                    session_id=session_id,
                    is_snapshot=True,
                    schema=normalized_schema,
                )
            )
    return tuple(events)


__all__ = [
    "BOOK_SIDES",
    "CHECKPOINT_SCHEMA",
    "EVENT_ENGINE_SCHEMA",
    "F_LAST",
    "F_MAYBE_BAD_BOOK",
    "F_SNAPSHOT",
    "SUPPORTED_ACTIONS",
    "BookStateError",
    "BookView",
    "CausalityViolation",
    "CheckpointError",
    "CompactBatchResult",
    "CompactEventResult",
    "EventOrderError",
    "EventResult",
    "MarketEvent",
    "MicrostructureEventEngine",
    "MicrostructureEventEngineError",
    "SequenceGapError",
    "SessionView",
    "SnapshotRecoveryRequired",
    "TapeView",
    "adapt_depth_snapshot",
    "stable_hash",
]
