from __future__ import annotations

from dataclasses import asdict, replace

import pytest

from hydra.production.microstructure_opportunity_episode import (
    OpportunityCausalityError,
    OpportunityDuplicateConflict,
    OpportunityEpisodeFSM,
    OpportunityEpisodeSpec,
    OpportunityObservation,
    OpportunityOrderError,
)


BASE = 1_720_000_000_000_000_000


def _spec(**overrides: object) -> OpportunityEpisodeSpec:
    values: dict[str, object] = {
        "policy_id": "opportunity-policy-0032",
        "mechanism": "ABSORPTION_REVERSAL",
        "direction": 1,
        "activation_threshold": 0.70,
        "reset_threshold": 0.40,
        "meta_label_threshold": 0.60,
        "consolidation_window_ns": 10_000,
        "price_zone_ticks": 2.0,
        "tick_size": 0.25,
        "minimum_confirmations": 1,
    }
    values.update(overrides)
    return OpportunityEpisodeSpec(**values)  # type: ignore[arg-type]


def _observation(index: int, **overrides: object) -> OpportunityObservation:
    event_time = BASE + index * 1_000
    values: dict[str, object] = {
        "event_fingerprint": f"event-{index}",
        "market": "NQ",
        "contract": "NQU4",
        "session_id": "2024-07-08",
        "event_time_ns": event_time,
        "available_at_ns": event_time + 100,
        "price": 20_000.0,
        "mechanism": "ABSORPTION_REVERSAL",
        "direction": 1,
        "activation_score": 0.80,
        "meta_score": 0.75,
        "feature_fingerprint": f"feature-{index}",
    }
    values.update(overrides)
    return OpportunityObservation(**values)  # type: ignore[arg-type]


def test_time_price_consolidation_emits_at_most_one_decision_per_episode() -> None:
    engine = OpportunityEpisodeFSM(_spec())

    first = engine.step(_observation(1))
    second = engine.step(_observation(2, price=20_000.25))
    price_boundary = engine.step(_observation(3, price=20_001.0))

    assert first.decision is not None and first.decision.action == "ENTER"
    assert second.consolidated is True
    assert second.decision is None
    assert price_boundary.reset_reason == "PRICE_ZONE_BOUNDARY"
    assert price_boundary.decision is not None
    assert len(engine.decisions) == 2

    episodes = engine.finalize()
    assert len(episodes) == 2
    assert episodes[0].observation_count == 2
    assert episodes[0].decision == first.decision
    assert episodes[1].observation_count == 1
    assert all(episode.decision is not None for episode in episodes)
    assert len({episode.decision.decision_hash for episode in episodes}) == 2


def test_hysteresis_reset_and_frozen_meta_abstention_are_transparent() -> None:
    engine = OpportunityEpisodeFSM(_spec(minimum_confirmations=2))

    armed = engine.step(_observation(1))
    abstained = engine.step(_observation(2, meta_score=0.20))
    latched = engine.step(_observation(3, meta_score=0.99))
    reset = engine.step(
        _observation(4, activation_score=0.30, meta_score=0.99)
    )
    rearmed = engine.step(_observation(5))
    entered = engine.step(_observation(6))

    assert armed.transition == "EPISODE_STARTED" and armed.decision is None
    assert abstained.decision is not None
    assert abstained.decision.action == "ABSTAIN"
    assert abstained.decision.reason == "META_LABEL_ABSTAINED"
    assert latched.decision is None
    assert reset.transition == "EPISODE_RESET"
    assert reset.reset_reason == "RESET_THRESHOLD"
    assert rearmed.decision is None
    assert entered.decision is not None and entered.decision.action == "ENTER"

    episodes = engine.finalize()
    assert [episode.decision.action for episode in episodes] == [
        "ABSTAIN",
        "ENTER",
    ]


def test_batch_is_only_streaming_step_and_final_hashes_are_identical() -> None:
    observations = (
        _observation(1),
        _observation(2),
        _observation(3, activation_score=0.20),
        _observation(4, session_id="2024-07-09"),
        _observation(20, price=20_001.0),
    )
    batch = OpportunityEpisodeFSM(_spec())
    batch_rows = batch.process_batch(observations)
    batch_episodes = batch.finalize(final_time_ns=BASE + 30_000)

    streaming = OpportunityEpisodeFSM(_spec())
    stream_rows = tuple(streaming.step(value) for value in observations)
    stream_episodes = streaming.finalize(final_time_ns=BASE + 30_000)

    assert [value.to_record() for value in batch_rows] == [
        value.to_record() for value in stream_rows
    ]
    assert [value.to_record() for value in batch_episodes] == [
        value.to_record() for value in stream_episodes
    ]
    assert batch.state_hash == streaming.state_hash


def test_duplicate_and_checkpoint_resume_are_idempotent() -> None:
    spec = _spec()
    first = _observation(1)
    second = _observation(2)

    uninterrupted = OpportunityEpisodeFSM(spec)
    first_result = uninterrupted.step(first)
    checkpoint = uninterrupted.checkpoint()

    resumed = OpportunityEpisodeFSM.restore(checkpoint)
    before_duplicate = resumed.state_hash
    duplicate = resumed.step(first)
    assert duplicate.duplicate is True
    assert duplicate.decision is None
    assert resumed.state_hash == before_duplicate
    assert resumed.decisions == (first_result.decision,)

    uninterrupted_duplicate = uninterrupted.step(first)
    uninterrupted_second = uninterrupted.step(second)
    resumed_second = resumed.step(second)
    assert uninterrupted_duplicate.to_record() == duplicate.to_record()
    assert uninterrupted_second.to_record() == resumed_second.to_record()

    # An older duplicate remains a true no-op even after a newer event was
    # accepted; deduplication must not reorder checkpoint state.
    before_old_duplicate = resumed.state_hash
    old_duplicate = resumed.step(first)
    assert old_duplicate.duplicate is True
    assert resumed.state_hash == before_old_duplicate

    uninterrupted.finalize()
    resumed.finalize()
    assert uninterrupted.state_hash == resumed.state_hash
    assert [value.to_record() for value in uninterrupted.episodes] == [
        value.to_record() for value in resumed.episodes
    ]


def test_future_fields_and_unavailable_observations_fail_before_state_mutation() -> None:
    engine = OpportunityEpisodeFSM(_spec())
    pristine = engine.state_hash
    value = asdict(_observation(1))
    value["future_label"] = True
    with pytest.raises(OpportunityCausalityError, match="cannot enter step"):
        engine.step(value)
    assert engine.state_hash == pristine

    with pytest.raises(OpportunityCausalityError, match="before available_at"):
        engine.step(
            _observation(1),
            decision_time_ns=_observation(1).available_at_ns - 1,
        )
    assert engine.state_hash == pristine


def test_duplicate_conflict_and_nonduplicate_time_regression_fail_closed() -> None:
    engine = OpportunityEpisodeFSM(_spec())
    original = _observation(2)
    engine.step(original)
    before = engine.state_hash

    with pytest.raises(OpportunityDuplicateConflict):
        engine.step(replace(original, price=20_000.25))
    assert engine.state_hash == before

    with pytest.raises(OpportunityOrderError):
        engine.step(_observation(1))
    assert engine.state_hash == before


def test_mechanism_and_direction_are_frozen_per_fsm() -> None:
    engine = OpportunityEpisodeFSM(_spec())
    pristine = engine.state_hash
    with pytest.raises(ValueError, match="mechanism/direction"):
        engine.step(_observation(1, direction=-1))
    with pytest.raises(ValueError, match="mechanism/direction"):
        engine.step(_observation(1, mechanism="LIQUIDITY_VACUUM"))
    assert engine.state_hash == pristine
