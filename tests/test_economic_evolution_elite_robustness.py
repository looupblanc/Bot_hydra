from __future__ import annotations

from hydra.account_policy.router import AccountDecisionState, EntryIntent
from hydra.economic_evolution.account_elite_robustness import (
    EliteRobustnessPolicy,
    route_elite_robustness_entry,
)


def _policy(**changes) -> EliteRobustnessPolicy:
    values = {
        "policy_id": "child",
        "parent_policy_id": "parent",
        "parent_policy_fingerprint": "a" * 64,
        "component_ids": tuple(f"sleeve_{index}" for index in range(10)),
        "retained_added_sleeve_id": "sleeve_9",
        "mutation_family": "BUFFER_ACCELERATION",
        "failure_target": "INSUFFICIENT_TARGET_VELOCITY",
        "exact_change": (("high_risk_units", 4),),
        "expected_effect": "Bounded acceleration.",
        "high_risk_units": 4,
        "daily_loss_guard": 1_000.0,
        "daily_profit_lock": 2_250.0,
        "critical_buffer": 750.0,
        "high_zone_buffer": 3_750.0,
        "high_zone_remaining_target": 4_500.0,
        "middle_zone_buffer": 3_000.0,
        "middle_zone_remaining_target": 2_250.0,
        "middle_risk_units": 2,
        "maximum_simultaneous_positions": 3,
        "maximum_mini_equivalent": 15,
    }
    values.update(changes)
    return EliteRobustnessPolicy(**values)


def test_elite_policy_fingerprint_is_behavioral_not_label_driven() -> None:
    first = _policy(policy_id="first", exact_change=(("x", 1),))
    second = _policy(policy_id="second", exact_change=(("x", 2),))
    assert first.structural_fingerprint == second.structural_fingerprint


def test_buffer_acceleration_is_bounded_by_shared_contract_limit() -> None:
    policy = _policy()
    intent = EntryIntent(
        event_id="event",
        component_id="sleeve_0",
        market="ES",
        side=1,
        decision_ns=1,
        session_day=1,
        regime="TEST",
        base_quantity=1,
        base_mini_equivalent=5.0,
    )
    state = AccountDecisionState(
        balance=150_000.0,
        mll_floor=145_500.0,
        mll_buffer=4_500.0,
        daily_realized_pnl=0.0,
        consecutive_losing_days=0,
        remaining_target=9_000.0,
        open_exposures=(),
    )
    decision = route_elite_robustness_entry(intent, state, policy=policy)
    assert decision.allow is False
    assert decision.reason == "SHARED_CONTRACT_LIMIT"
