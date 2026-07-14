from __future__ import annotations

from dataclasses import replace

from hydra.account_policy.router import AccountDecisionState, EntryIntent
from hydra.economic_evolution.account_loss_streak_buffer_ratchet import (
    LossStreakBufferRatchetPolicy,
    route_loss_streak_buffer_ratchet_entry,
)


def _policy() -> LossStreakBufferRatchetPolicy:
    return LossStreakBufferRatchetPolicy(
        policy_id="ratchet-child",
        parent_policy_id="frozen-0018-parent",
        parent_policy_fingerprint="a" * 64,
        component_ids=tuple(f"sleeve-{index}" for index in range(10)),
        retained_added_sleeve_id="sleeve-9",
        mutation_family="BUFFER_ACCELERATION",
        failure_target="TARGET_VELOCITY_WITH_SEQUENCE_RISK",
        exact_change=(("loss_streak_derisk_after", 2),),
        expected_effect="Accelerate only with a favorable realized account state.",
        high_risk_units=4,
        daily_loss_guard=1_000.0,
        daily_profit_lock=2_250.0,
        critical_buffer=750.0,
        high_zone_buffer=3_750.0,
        high_zone_remaining_target=4_500.0,
        middle_zone_buffer=3_000.0,
        middle_zone_remaining_target=2_250.0,
        middle_risk_units=2,
        maximum_simultaneous_positions=3,
        maximum_mini_equivalent=15,
        loss_streak_derisk_after=2,
        derisked_units=1,
        middle_zone_concurrency=2,
        minimum_realized_progress_for_high_risk=1_000.0,
        daily_gain_derisk_threshold=2_000.0,
    )


def _intent() -> EntryIntent:
    return EntryIntent(
        event_id="event",
        component_id="sleeve-0",
        market="ES",
        side=1,
        decision_ns=1,
        session_day=1,
        regime="VOLATILITY_NORMAL",
        base_quantity=1,
        base_mini_equivalent=1.0,
    )


def _state(**changes: object) -> AccountDecisionState:
    values: dict[str, object] = {
        "balance": 151_500.0,
        "mll_floor": 145_500.0,
        "mll_buffer": 4_500.0,
        "daily_realized_pnl": 0.0,
        "consecutive_losing_days": 0,
        "remaining_target": 7_500.0,
        "open_exposures": (),
    }
    values.update(changes)
    return AccountDecisionState(**values)  # type: ignore[arg-type]


def test_ratchet_uses_only_frozen_past_account_state() -> None:
    policy = _policy()
    favorable = route_loss_streak_buffer_ratchet_entry(
        _intent(), _state(), policy=policy
    )
    losing = route_loss_streak_buffer_ratchet_entry(
        _intent(), _state(consecutive_losing_days=2), policy=policy
    )
    after_large_gain = route_loss_streak_buffer_ratchet_entry(
        _intent(), _state(daily_realized_pnl=2_000.0), policy=policy
    )
    assert favorable.allow and favorable.quantity == 4
    assert losing.allow and losing.quantity == 1
    assert after_large_gain.allow and after_large_gain.quantity == 1


def test_ratchet_fingerprint_includes_every_account_state_rule() -> None:
    policy = _policy()
    assert policy.structural_fingerprint != replace(
        policy,
        policy_id="ratchet-child-2",
        loss_streak_derisk_after=3,
    ).structural_fingerprint
    assert policy.inherited_status is None
