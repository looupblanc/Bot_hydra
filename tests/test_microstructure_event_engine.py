from __future__ import annotations

from copy import deepcopy
from time import perf_counter

import pytest

from hydra.production.microstructure_event_engine import (
    BookStateError,
    CausalityViolation,
    CheckpointError,
    CompactEventResult,
    EventOrderError,
    F_LAST,
    F_MAYBE_BAD_BOOK,
    F_SNAPSHOT,
    MarketEvent,
    MicrostructureEventEngine,
    SequenceGapError,
    SnapshotRecoveryRequired,
    adapt_depth_snapshot,
)


BASE_TS = 1_720_000_000_000_000_000


def _event(sequence: int, action: str, **overrides: object) -> MarketEvent:
    flags_explicit = "flags" in overrides
    values: dict[str, object] = {
        "ts_event_ns": BASE_TS + sequence * 1_000,
        "available_at_ns": BASE_TS + sequence * 1_000 + 100,
        "sequence": sequence,
        "publisher_id": "GLBX.MDP3",
        "instrument_id": "NQU4",
        "action": action,
        "side": "N",
        "price": None,
        "size": 0,
        "order_id": None,
        "flags": 0,
        "session_id": "2024-07-08",
    }
    values.update(overrides)
    if values.get("is_snapshot") is True and not flags_explicit:
        values["flags"] = int(values["flags"]) | F_SNAPSHOT | F_LAST
    return MarketEvent(**values)  # type: ignore[arg-type]


def _complete_action_tape() -> tuple[MarketEvent, ...]:
    return (
        _event(1, "R", is_snapshot=True),
        _event(2, "A", side="B", price=100.0, size=10, order_id="bid-1"),
        _event(3, "A", side="A", price=101.0, size=12, order_id="ask-1"),
        _event(4, "M", side="B", price=100.25, size=8, order_id="bid-1"),
        _event(5, "T", side="B", price=101.0, size=3),
        _event(6, "F", side="A", price=101.0, size=2, order_id="ask-1"),
        _event(7, "C", size=8, order_id="bid-1"),
    )


def test_batch_is_only_streaming_step_and_all_actions_are_deterministic() -> None:
    records = _complete_action_tape()
    batch_engine, batch_results = MicrostructureEventEngine.replay_batch(records)
    stream_engine = MicrostructureEventEngine()
    stream_results = tuple(stream_engine.step(record) for record in records)

    assert [result.to_record() for result in batch_results] == [
        result.to_record() for result in stream_results
    ]
    assert batch_engine.state_hash() == stream_engine.state_hash()
    assert {result.action for result in batch_results} == {"A", "M", "C", "F", "R", "T"}

    final = batch_results[-1]
    assert final.book.bid_price is None
    assert final.book.ask_price == 101.0
    assert final.book.ask_size == 12
    assert final.book.order_count == 1
    # Official semantics leave displayed depth unchanged for both F and T.
    assert final.tape.trade_count == 1
    assert final.tape.total_volume == 3
    assert final.tape.vwap == 101.0
    assert final.session.trade_count == 1


def test_neutral_trade_preserves_tape_economics_without_signed_side_distortion() -> None:
    records = (
        _event(1, "R", is_snapshot=True),
        _event(2, "T", side="N", price=100.5, size=7),
    )
    batch_engine, batch_results = MicrostructureEventEngine.replay_batch(records)
    stream_engine = MicrostructureEventEngine()
    stream_results = tuple(stream_engine.step(record) for record in records)

    assert [value.to_record() for value in batch_results] == [
        value.to_record() for value in stream_results
    ]
    assert batch_engine.state_hash() == stream_engine.state_hash()
    final = batch_results[-1]
    assert final.tape.trade_count == 1
    assert final.tape.total_volume == 7
    assert final.tape.notional == pytest.approx(703.5)
    assert final.tape.last_trade_price == pytest.approx(100.5)
    assert final.tape.buy_volume == 0
    assert final.tape.sell_volume == 0
    assert final.session.trade_count == 1
    assert final.session.volume == 7
    assert final.session.buy_volume == 0
    assert final.session.sell_volume == 0


def test_availability_contract_fails_before_any_state_mutation() -> None:
    engine = MicrostructureEventEngine()
    record = _event(1, "R", is_snapshot=True)
    pristine = engine.state_hash()
    with pytest.raises(CausalityViolation, match="before available_at"):
        engine.step(record, decision_time_ns=record.available_at_ns - 1)
    assert engine.state_hash() == pristine
    assert engine.stream_status("GLBX.MDP3", "NQU4")["last_sequence"] is None


def test_sequence_gap_blocks_until_explicit_snapshot_reset() -> None:
    engine = MicrostructureEventEngine()
    engine.step(_event(1, "R", is_snapshot=True))
    engine.step(_event(2, "A", side="B", price=100.0, size=3, order_id="old"))

    with pytest.raises(SequenceGapError, match="expected 3, got 4"):
        engine.step(_event(4, "A", side="A", price=101.0, size=2, order_id="lost"))
    status = engine.stream_status("GLBX.MDP3", "NQU4")
    assert {
        key: status[key]
        for key in ("last_sequence", "blocked", "expected_sequence")
    } == {"last_sequence": 2, "blocked": True, "expected_sequence": 3}
    with pytest.raises(SnapshotRecoveryRequired):
        engine.step(_event(5, "T", side="B", price=101.0, size=1))
    with pytest.raises(ValueError, match="authenticated by F_SNAPSHOT"):
        engine.step(_event(1, "R", is_snapshot=True, flags=1))

    reset = engine.step(_event(10, "R", is_snapshot=True))
    assert reset.book.order_count == 0
    recovered = engine.step(
        _event(11, "A", side="A", price=102.0, size=7, order_id="fresh")
    )
    assert recovered.book.ask_price == 102.0
    assert engine.stream_status("GLBX.MDP3", "NQU4")["blocked"] is False


def test_duplicate_is_idempotent_but_conflicting_or_old_sequence_fails() -> None:
    engine = MicrostructureEventEngine()
    record = _event(1, "R", is_snapshot=True)
    first = engine.step(record)
    before = engine.state_hash()
    duplicate = engine.step(record)
    assert duplicate.duplicate is True
    assert duplicate.state_hash == first.state_hash
    assert engine.state_hash() == before

    with pytest.raises(EventOrderError):
        engine.step(_event(1, "R", is_snapshot=False, flags=1))

    engine.step(_event(2, "A", side="B", price=100.0, size=1, order_id="one"))
    with pytest.raises(EventOrderError):
        engine.step(_event(1, "T", side="B", price=100.0, size=1))


def test_checkpoint_restore_resume_and_repeated_resume_are_exact() -> None:
    records = _complete_action_tape()
    uninterrupted = MicrostructureEventEngine()
    uninterrupted_results = uninterrupted.process_batch(records)

    interrupted = MicrostructureEventEngine()
    prefix = interrupted.process_batch(records[:4])
    checkpoint = interrupted.checkpoint()
    resumed = MicrostructureEventEngine.restore(checkpoint)
    suffix = resumed.process_batch(records[4:])

    assert [item.to_record() for item in prefix + suffix] == [
        item.to_record() for item in uninterrupted_results
    ]
    assert resumed.state_hash() == uninterrupted.state_hash()

    state_before_duplicate = resumed.state_hash()
    duplicate = resumed.step(records[-1])
    assert duplicate.duplicate is True
    assert resumed.state_hash() == state_before_duplicate

    bad = deepcopy(checkpoint)
    bad["engine_state"]["depth_levels"] = 99
    with pytest.raises(CheckpointError, match="hash mismatch"):
        MicrostructureEventEngine.restore(bad)


def test_session_state_rolls_without_destroying_book_or_global_tape() -> None:
    engine = MicrostructureEventEngine()
    engine.step(_event(1, "R", is_snapshot=True))
    first = engine.step(_event(2, "T", side="B", price=100.0, size=2))
    second = engine.step(
        _event(
            3,
            "T",
            side="A",
            price=102.0,
            size=4,
            session_id="2024-07-09",
        )
    )
    assert first.session.volume == 2
    assert second.session.session_id == "2024-07-09"
    assert second.session.event_count == 1
    assert second.session.volume == 4
    assert second.session.vwap == 102.0
    assert second.tape.total_volume == 6
    assert second.tape.vwap == pytest.approx((100.0 * 2 + 102.0 * 4) / 6)


def test_depth_snapshot_adapter_recovers_aggregated_tbbo_or_mbp_state() -> None:
    events = adapt_depth_snapshot(
        ts_event_ns=BASE_TS,
        available_at_ns=BASE_TS + 100,
        base_sequence=100,
        publisher_id="GLBX.MDP3",
        instrument_id="ESU4",
        bids=((5_200.00, 12), (5_199.75, 7)),
        asks=((5_200.25, 9), (5_200.50, 11)),
        session_id="2024-07-08",
        schema="mbp-10",
    )
    engine = MicrostructureEventEngine(depth_levels=2)
    results = engine.process_batch(events)
    final = results[-1]
    assert final.book.bid_depth == ((5_200.0, 12), (5_199.75, 7))
    assert final.book.ask_depth == ((5_200.25, 9), (5_200.5, 11))
    assert final.book.order_count == 4
    assert final.book.spread == 0.25
    assert all(event.is_snapshot for event in events)
    assert all((event.order_id or "").startswith("snapshot:") for event in events[1:])


def test_book_mutations_fail_closed_and_do_not_advance_sequence() -> None:
    engine = MicrostructureEventEngine()
    engine.step(_event(1, "R", is_snapshot=True))
    with pytest.raises(BookStateError, match="unknown order"):
        engine.step(
            _event(
                2,
                "C",
                size=1,
                order_id="missing",
                session_id="2024-07-09",
            )
        )
    assert engine.stream_status("GLBX.MDP3", "NQU4")["last_sequence"] == 1
    accepted = engine.step(
        _event(2, "A", side="A", price=101.0, size=2, order_id="present")
    )
    assert accepted.book.ask_size == 2
    assert accepted.session.session_id == "2024-07-08"


def test_official_cancel_modify_fill_semantics() -> None:
    engine = MicrostructureEventEngine()
    engine.step(_event(1, "R", is_snapshot=True))
    added = engine.step(
        _event(2, "M", side="B", price=100.0, size=10, order_id="late-add")
    )
    assert added.book.bid_size == 10
    partial = engine.step(_event(3, "C", size=4, order_id="late-add"))
    assert partial.book.bid_size == 6
    fill = engine.step(
        _event(4, "F", side="B", price=100.0, size=5, order_id="late-add")
    )
    assert fill.book.bid_size == 6
    removed = engine.step(_event(5, "C", size=6, order_id="late-add"))
    assert removed.book.bid_price is None


def test_compact_batch_matches_materialized_stream_only_at_checkpoints() -> None:
    records = (
        _event(10, "R", is_snapshot=True),
        _event(20, "A", side="B", price=100.0, size=5, order_id="bid-1"),
        _event(30, "A", side="A", price=101.0, size=6, order_id="ask-1"),
        _event(40, "T", side="B", price=101.0, size=2),
        _event(50, "M", side="B", price=100.25, size=4, order_id="bid-1"),
        _event(60, "F", side="A", price=101.0, size=1, order_id="ask-1"),
        _event(70, "T", side="A", price=100.25, size=3),
    )
    streaming = MicrostructureEventEngine(strict_contiguous_sequence=False)
    materialized = {
        result.sequence: result
        for result in (streaming.step(record) for record in records)
    }

    callbacks: list[int] = []
    compact = MicrostructureEventEngine(strict_contiguous_sequence=False)
    replay = compact.process_batch_compact(
        records,
        snapshot_every_ns=30_000,
        callback=lambda result: callbacks.append(result.sequence),
    )

    assert [result.sequence for result in replay.snapshots] == [10, 40, 70]
    assert callbacks == [10, 40, 70]
    assert [result.to_record() for result in replay.snapshots] == [
        materialized[sequence].to_record() for sequence in (10, 40, 70)
    ]
    assert replay.input_count == replay.applied_event_count == len(records)
    assert replay.duplicate_event_count == 0
    assert replay.final_state_hash == streaming.state_hash() == compact.state_hash()
    assert all(
        snapshot.state_hash_scope == "STREAM_EVENT_CHAIN"
        and snapshot.state_hash == snapshot.chain_hash
        for snapshot in replay.snapshots
    )


def test_compact_checkpoint_preserves_filtered_sequence_and_idempotence() -> None:
    engine = MicrostructureEventEngine(strict_contiguous_sequence=False)
    reset = engine.step(_event(10, "R", is_snapshot=True), materialize=False)
    assert isinstance(reset, CompactEventResult)
    add = _event(20, "A", side="B", price=100.0, size=5, order_id="bid")
    compact = engine.step(add, materialize=False)
    assert isinstance(compact, CompactEventResult)

    restored = MicrostructureEventEngine.restore(engine.checkpoint())
    before = restored.state_hash()
    duplicate = restored.step(add, materialize=False)
    assert isinstance(duplicate, CompactEventResult)
    assert duplicate.duplicate is True
    assert restored.state_hash() == before
    status = restored.stream_status("GLBX.MDP3", "NQU4")
    assert {
        key: status[key]
        for key in ("last_sequence", "blocked", "expected_sequence")
    } == {"last_sequence": 20, "blocked": False, "expected_sequence": 21}

    # Filtered vendor feeds can contain several distinct messages from the same
    # packet sequence.  Non-contiguous mode is monotone, not strictly monotone.
    same_packet = restored.step(
        _event(20, "A", side="A", price=101.0, size=4, order_id="ask"),
        materialize=False,
    )
    assert isinstance(same_packet, CompactEventResult)
    assert same_packet.duplicate is False
    with pytest.raises(EventOrderError):
        restored.step(_event(19, "T", side="B", price=100.0, size=1))


def test_compact_batch_functional_benchmark_bounds_expensive_materializations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # This is a functional benchmark, not a machine-dependent speed gate.  It
    # proves that thousands of order mutations cause exactly one whole-engine
    # hash at the batch boundary. Cadence snapshots use the O(1) event chain
    # instead, while still recording a positive observed throughput.
    records = [_event(1, "R", is_snapshot=True)]
    records.extend(
        _event(
            sequence,
            "A",
            side="B" if sequence % 2 else "A",
            price=100.0 - sequence * 0.0001
            if sequence % 2
            else 101.0 + sequence * 0.0001,
            size=(sequence % 9) + 1,
            order_id=f"order-{sequence}",
        )
        for sequence in range(2, 2_002)
    )
    engine = MicrostructureEventEngine(strict_contiguous_sequence=False)
    original_state_hash = engine.state_hash
    state_hash_calls = 0

    def counted_state_hash() -> str:
        nonlocal state_hash_calls
        state_hash_calls += 1
        return original_state_hash()

    monkeypatch.setattr(engine, "state_hash", counted_state_hash)
    started = perf_counter()
    replay = engine.process_batch_compact(records, snapshot_every_ns=500_000)
    elapsed = perf_counter() - started

    assert replay.applied_event_count == 2_001
    assert 1 < len(replay.snapshots) < 10
    assert state_hash_calls == 1
    assert replay.final_state_hash == original_state_hash()
    assert all(
        snapshot.state_hash_scope == "STREAM_EVENT_CHAIN"
        and snapshot.state_hash == snapshot.chain_hash
        for snapshot in replay.snapshots
    )
    assert elapsed > 0.0
    assert replay.applied_event_count / elapsed > 0.0


def test_incremental_chain_matches_between_full_stream_and_compact_checkpoints() -> None:
    records = (
        _event(10, "R", is_snapshot=True),
        _event(20, "A", side="B", price=100.0, size=5, order_id="bid"),
        _event(30, "A", side="A", price=101.0, size=4, order_id="ask"),
        _event(40, "T", side="B", price=101.0, size=2),
    )
    full_engine = MicrostructureEventEngine(strict_contiguous_sequence=False)
    full_results = full_engine.process_batch(records)
    expected_chain = {result.sequence: result.chain_hash for result in full_results}

    compact_engine = MicrostructureEventEngine(strict_contiguous_sequence=False)
    replay = compact_engine.process_batch_compact(
        records,
        snapshot_every_ns=10_000,
    )

    assert [snapshot.chain_hash for snapshot in replay.snapshots] == [
        expected_chain[snapshot.sequence] for snapshot in replay.snapshots
    ]
    assert replay.final_state_hash == full_engine.state_hash()


def test_authenticated_daily_snapshots_rebase_sequence_and_gate_until_f_last() -> None:
    def at(index: int, sequence: int, action: str, **values: object) -> MarketEvent:
        return _event(
            sequence,
            action,
            ts_event_ns=BASE_TS + index * 10_000,
            available_at_ns=BASE_TS + index * 10_000 + 100,
            **values,
        )

    engine = MicrostructureEventEngine(strict_contiguous_sequence=False)
    reset = at(1, 0, "R", is_snapshot=True, flags=F_SNAPSHOT)
    engine.step(reset, materialize=False)
    assert engine.stream_status("GLBX.MDP3", "NQU4")["book_ready"] is False
    engine.step(
        at(
            2,
            100,
            "A",
            side="B",
            price=100.0,
            size=5,
            order_id="day1-bid",
            is_snapshot=True,
            flags=F_SNAPSHOT,
        ),
        materialize=False,
    )
    with pytest.raises(SnapshotRecoveryRequired, match="before snapshot F_LAST"):
        engine.step(at(3, 101, "T", side="B", price=101.0, size=1))
    final_snapshot = engine.step(
        at(
            4,
            50,
            "A",
            side="A",
            price=101.0,
            size=6,
            order_id="day1-ask",
            is_snapshot=True,
            flags=F_SNAPSHOT | F_LAST,
        )
    )
    assert final_snapshot.book.bid_price == 100.0
    assert engine.stream_status("GLBX.MDP3", "NQU4")["book_ready"] is True
    engine.step(at(5, 51, "T", side="B", price=101.0, size=2))

    # The next UTC-day snapshot legitimately restarts at sequence zero and
    # replaces, rather than merges with, yesterday's displayed orders.
    engine.step(
        at(6, 0, "R", is_snapshot=True, flags=F_SNAPSHOT),
        materialize=False,
    )
    engine.step(
        at(
            7,
            700,
            "A",
            side="B",
            price=102.0,
            size=3,
            order_id="day2-bid",
            is_snapshot=True,
            flags=F_SNAPSHOT,
        ),
        materialize=False,
    )
    daily_last = engine.step(
        at(
            8,
            600,
            "A",
            side="A",
            price=103.0,
            size=4,
            order_id="day2-ask",
            is_snapshot=True,
            flags=F_SNAPSHOT | F_LAST,
        )
    )
    assert daily_last.book.bid_price == 102.0
    assert daily_last.book.ask_price == 103.0
    assert daily_last.book.order_count == 2
    status = engine.stream_status("GLBX.MDP3", "NQU4")
    assert status["snapshot_count"] == 2
    assert status["book_ready"] is True


def test_daily_snapshot_batch_and_streaming_states_are_identical() -> None:
    records = (
        _event(0, "R", is_snapshot=True, flags=F_SNAPSHOT),
        _event(
            9,
            "A",
            side="B",
            price=100.0,
            size=2,
            order_id="b1",
            is_snapshot=True,
            flags=F_SNAPSHOT,
        ),
        _event(
            7,
            "A",
            side="A",
            price=101.0,
            size=3,
            order_id="a1",
            is_snapshot=True,
            flags=F_SNAPSHOT | F_LAST,
        ),
        _event(8, "T", side="B", price=101.0, size=1),
        _event(
            0,
            "R",
            is_snapshot=True,
            flags=F_SNAPSHOT,
            session_id="2024-07-09",
        ),
        _event(
            20,
            "A",
            side="B",
            price=102.0,
            size=4,
            order_id="b2",
            is_snapshot=True,
            flags=F_SNAPSHOT,
        ),
        _event(
            18,
            "A",
            side="A",
            price=103.0,
            size=5,
            order_id="a2",
            is_snapshot=True,
            flags=F_SNAPSHOT | F_LAST,
        ),
        _event(19, "T", side="A", price=102.0, size=2),
    )
    batch = MicrostructureEventEngine(strict_contiguous_sequence=False)
    batch_rows = batch.process_batch(records)
    streaming = MicrostructureEventEngine(strict_contiguous_sequence=False)
    stream_rows = tuple(streaming.step(record) for record in records)

    assert [value.to_record() for value in batch_rows] == [
        value.to_record() for value in stream_rows
    ]
    assert batch.state_hash() == streaming.state_hash()
    assert batch.stream_status("GLBX.MDP3", "NQU4")["snapshot_count"] == 2


def test_maybe_bad_book_fails_closed_without_state_mutation() -> None:
    engine = MicrostructureEventEngine()
    engine.step(_event(1, "R", is_snapshot=True))
    before = engine.state_hash()
    with pytest.raises(BookStateError, match="F_MAYBE_BAD_BOOK"):
        engine.step(
            _event(
                2,
                "T",
                side="B",
                price=100.0,
                size=1,
                flags=F_MAYBE_BAD_BOOK,
            )
        )
    assert engine.state_hash() == before
