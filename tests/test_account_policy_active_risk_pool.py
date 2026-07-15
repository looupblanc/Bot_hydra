from __future__ import annotations

from dataclasses import replace

import pytest

from hydra.account_policy.active_risk_pool import (
    ActivePoolDecisionStatus,
    ActiveRiskPoolError,
    ActiveRiskPoolPolicy,
    ConcurrencyScaling,
    SameInstrumentConflictRule,
    TargetProtectionMode,
    active_risk_utilisation,
    policy_from_mapping,
    route_active_risk_pool_entry,
)
from hydra.account_policy.active_pool_replay import (
    RoutedTrade,
    run_shared_account_episode,
)
from hydra.account_policy.router import (
    AccountDecisionState,
    EntryIntent,
    OpenExposure,
)
from hydra.propfirm.combine_episode import TradePathEvent


def _policy(
    *components: str,
    scaling: ConcurrencyScaling = ConcurrencyScaling.PROPORTIONAL,
    conflict: SameInstrumentConflictRule = SameInstrumentConflictRule.PRIORITY,
    ceiling: float = 2_000.0,
    maximum_mini: float = 15.0,
    tier: float = 1.0,
    target_mode: TargetProtectionMode = TargetProtectionMode.NONE,
) -> ActiveRiskPoolPolicy:
    return ActiveRiskPoolPolicy(
        policy_id="active-pool-test",
        component_priority=tuple(components),
        nominal_risk_charge_per_mini=tuple(
            (component_id, 250.0) for component_id in components
        ),
        maximum_concurrent_sleeves=min(3, len(components)),
        aggregate_open_risk_ceiling=ceiling,
        maximum_mll_buffer_fraction=1.0,
        protected_mll_buffer=500.0,
        maximum_mini_equivalent=maximum_mini,
        concurrency_scaling=scaling,
        same_instrument_conflict_rule=conflict,
        daily_loss_guard=1_500.0,
        daily_consistency_profit_guard=4_500.0,
        target_protection_distance=1_000.0,
        target_protection_mode=target_mode,
        static_risk_tier=tier,
    )


def _state(*exposures: OpenExposure, **changes: float) -> AccountDecisionState:
    values = {
        "balance": 150_000.0,
        "mll_floor": 145_500.0,
        "mll_buffer": 4_500.0,
        "daily_realized_pnl": 0.0,
        "consecutive_losing_days": 0,
        "remaining_target": 9_000.0,
        "open_exposures": tuple(exposures),
    }
    values.update(changes)
    return AccountDecisionState(**values)


def _intent(
    component: str = "a",
    market: str = "ES",
    *,
    quantity: int = 4,
    mini: float = 4.0,
    side: int = 1,
) -> EntryIntent:
    return EntryIntent(
        event_id=f"{component}-entry",
        component_id=component,
        market=market,
        side=side,
        decision_ns=1_000,
        session_day=20260715,
        regime="AVAILABLE_STATE_ONLY",
        base_quantity=quantity,
        base_mini_equivalent=mini,
    )


def _exposure(
    component: str,
    market: str,
    *,
    mini: float,
    side: int = 1,
) -> OpenExposure:
    return OpenExposure(
        component_id=component,
        market=market,
        side=side,
        mini_equivalent=mini,
        exit_ns=2_000,
    )


def test_sole_active_sleeve_preserves_nominal_size_and_inactive_membership_is_free() -> None:
    solo = _policy("a")
    with_inactive_members = _policy("a", "b", "c")

    solo_decision = route_active_risk_pool_entry(
        _intent(), _state(), policy=solo
    )
    expanded_decision = route_active_risk_pool_entry(
        _intent(), _state(), policy=with_inactive_members
    )

    assert solo_decision.allow is expanded_decision.allow is True
    assert solo_decision.quantity == expanded_decision.quantity == 4
    assert solo_decision.mini_equivalent == expanded_decision.mini_equivalent == 4.0
    assert solo_decision.admitted_declared_nominal_risk == 1_000.0
    assert expanded_decision.admitted_declared_nominal_risk == 1_000.0
    assert expanded_decision.risk_before.active_sleeve_count == 0
    assert expanded_decision.risk_before.open_declared_nominal_risk == 0.0
    assert expanded_decision.reason == "ACTIVE_POOL_NOMINAL_RISK_PRESERVED"


def test_concurrent_sleeves_share_only_remaining_pool_by_proportional_size() -> None:
    policy = _policy("a", "b", ceiling=1_500.0)
    state = _state(_exposure("a", "ES", mini=4.0))
    decision = route_active_risk_pool_entry(
        _intent("b", "CL", quantity=4, mini=4.0), state, policy=policy
    )

    assert decision.allow is True
    assert decision.decision_status is ActivePoolDecisionStatus.SIZE_REDUCED
    assert decision.quantity == 2
    assert decision.requested_quantity == 4
    assert decision.risk_before.open_declared_nominal_risk == 1_000.0
    assert decision.risk_before.utilisation == pytest.approx(2.0 / 3.0)
    assert decision.risk_after.open_declared_nominal_risk == 1_500.0
    assert decision.risk_after.utilisation == 1.0
    assert decision.to_dict()["size_reduced"] is True
    assert decision.to_dict()["admission_fraction"] == 0.5
    assert decision.binding_constraint == "AGGREGATE_NOMINAL_RISK_LIMIT"


def test_priority_scaling_rejects_partial_entry_with_explicit_mll_reason() -> None:
    policy = _policy(
        "a", "b", ceiling=1_500.0, scaling=ConcurrencyScaling.PRIORITY
    )
    state = _state(_exposure("a", "ES", mini=4.0))
    decision = route_active_risk_pool_entry(
        _intent("b", "CL", quantity=4, mini=4.0), state, policy=policy
    )

    assert decision.allow is False
    assert decision.decision_status is ActivePoolDecisionStatus.MLL_RISK_REJECTED
    assert decision.reason == "AGGREGATE_NOMINAL_RISK_LIMIT"
    assert decision.requested_declared_nominal_risk == 1_000.0
    assert decision.admitted_declared_nominal_risk == 0.0
    assert decision.to_dict()["mll_risk_rejected"] is True


def test_contract_limit_and_conflict_have_distinct_machine_readable_statuses() -> None:
    contract_policy = _policy("a", "b", ceiling=4_000.0, maximum_mini=5.0)
    contract = route_active_risk_pool_entry(
        _intent("b", "CL", quantity=2, mini=2.0),
        _state(_exposure("a", "ES", mini=5.0)),
        policy=contract_policy,
    )
    conflict = route_active_risk_pool_entry(
        _intent("b", "ES"),
        _state(_exposure("a", "ES", mini=1.0)),
        policy=_policy("a", "b"),
    )

    assert contract.decision_status is ActivePoolDecisionStatus.CONTRACT_LIMIT_REJECTED
    assert contract.to_dict()["contract_limit_rejected"] is True
    assert conflict.decision_status is ActivePoolDecisionStatus.CONFLICT_REJECTED
    assert conflict.to_dict()["conflict_rejected"] is True
    assert conflict.to_dict()["emitted"] is True
    assert conflict.to_dict()["rejected"] is True


def test_allow_same_direction_never_allows_opposing_same_instrument() -> None:
    policy = _policy(
        "a", "b", conflict=SameInstrumentConflictRule.ALLOW_SAME_DIRECTION
    )
    state = _state(_exposure("a", "ES", mini=1.0, side=1))

    aligned = route_active_risk_pool_entry(
        _intent("b", "ES", quantity=1, mini=1.0, side=1), state, policy=policy
    )
    opposing = route_active_risk_pool_entry(
        _intent("b", "ES", quantity=1, mini=1.0, side=-1), state, policy=policy
    )

    assert aligned.allow is True
    assert opposing.allow is False
    assert opposing.decision_status is ActivePoolDecisionStatus.CONFLICT_REJECTED


@pytest.mark.parametrize(
    ("state_change", "reason", "status"),
    [
        (
            {"daily_realized_pnl": -1_500.0},
            "DAILY_LOSS_GUARD",
            ActivePoolDecisionStatus.REJECTED,
        ),
        (
            {"daily_realized_pnl": 4_500.0},
            "DAILY_CONSISTENCY_GUARD",
            ActivePoolDecisionStatus.REJECTED,
        ),
        (
            {"mll_buffer": 500.0},
            "PROTECTED_MLL_BUFFER_REACHED",
            ActivePoolDecisionStatus.MLL_RISK_REJECTED,
        ),
    ],
)
def test_account_guards_are_causal_and_explicit(
    state_change: dict[str, float],
    reason: str,
    status: ActivePoolDecisionStatus,
) -> None:
    decision = route_active_risk_pool_entry(
        _intent(), _state(**state_change), policy=_policy("a")
    )
    assert decision.allow is False
    assert decision.reason == reason
    assert decision.decision_status is status


def test_target_protection_is_frozen_and_does_not_read_trade_outcome() -> None:
    policy = _policy("a", target_mode=TargetProtectionMode.SCALE_50)
    decision = route_active_risk_pool_entry(
        _intent(quantity=4, mini=4.0),
        _state(remaining_target=500.0),
        policy=policy,
    )

    assert decision.allow is True
    assert decision.quantity == 2
    assert decision.reason == "TARGET_PROTECTION_SIZE_REDUCTION"
    assert decision.decision_status is ActivePoolDecisionStatus.SIZE_REDUCED
    assert decision.requested_quantity == 4
    assert decision.binding_constraint == "TARGET_PROTECTION"
    payload = decision.to_dict()
    assert payload["foregone_realized_pnl_available_at_decision"] is False
    assert policy.to_dict()["future_outcome_fields_used"] is False
    assert policy.to_dict()["actual_stop_risk_available"] is False


def test_risk_utilisation_counts_active_sleeves_not_frozen_membership() -> None:
    policy = _policy("a", "b", "c", ceiling=2_000.0)
    state = _state(
        _exposure("a", "ES", mini=1.0),
        _exposure("a", "NQ", mini=1.0),
        _exposure("b", "CL", mini=2.0),
    )
    audit = active_risk_utilisation(policy, state)

    assert audit.active_sleeve_count == 2
    assert audit.open_declared_nominal_risk == 1_000.0
    assert audit.maximum_admissible_declared_nominal_risk == 2_000.0
    assert audit.utilisation == 0.5
    assert audit.to_dict()["actual_stop_risk_available"] is False


def test_manifest_round_trip_and_bounded_frontier() -> None:
    policy = _policy("a", "b", tier=3.0)
    restored = policy_from_mapping(policy.to_dict())
    assert restored == policy
    assert restored.structural_fingerprint == policy.structural_fingerprint

    with pytest.raises(ActiveRiskPoolError, match="discrete frontier"):
        replace(policy, static_risk_tier=2.5)
    with pytest.raises(ActiveRiskPoolError, match="capital reservation"):
        replace(policy, preserve_sole_sleeve_nominal_risk=False)
    with pytest.raises(ActiveRiskPoolError, match="cannot submit orders"):
        replace(policy, outbound_order_capability=True)


def test_shared_replay_uses_frozen_component_priority_for_simultaneous_conflicts() -> None:
    policy = _policy("a", "b")
    events = {
        "a": (
            RoutedTrade(
                component_id="a",
                market="ES",
                side=1,
                event=TradePathEvent(
                    event_id="zzz-a",
                    session_day=1,
                    decision_ns=100,
                    exit_ns=200,
                    net_pnl=100.0,
                    gross_pnl=101.0,
                    worst_unrealized_pnl=-10.0,
                    best_unrealized_pnl=110.0,
                    quantity=1,
                    mini_equivalent=1.0,
                ),
            ),
        ),
        "b": (
            RoutedTrade(
                component_id="b",
                market="ES",
                side=1,
                event=TradePathEvent(
                    event_id="aaa-b",
                    session_day=1,
                    decision_ns=100,
                    exit_ns=200,
                    net_pnl=1_000.0,
                    gross_pnl=1_001.0,
                    worst_unrealized_pnl=-10.0,
                    best_unrealized_pnl=1_100.0,
                    quantity=1,
                    mini_equivalent=1.0,
                ),
            ),
        ),
    }

    episode = run_shared_account_episode(
        events,
        (1,),
        basket=policy,
        active_pool_policy=policy,
        start_day=1,
        maximum_duration_days=1,
    )

    assert [row["component_id"] for row in episode.risk_allocation_path] == [
        "a",
        "b",
    ]
    assert episode.risk_allocation_path[0]["decision_status"] == "ACCEPTED"
    assert episode.risk_allocation_path[1]["decision_status"] == (
        "CONFLICT_REJECTED"
    )
    assert episode.component_contribution == {"a": 100.0}
