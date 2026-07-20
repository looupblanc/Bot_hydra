from __future__ import annotations

from hydra.account_policy.active_risk_pool import (
    ActiveRiskPoolPolicy,
    ConcurrencyScaling,
    SameInstrumentConflictRule,
    TargetProtectionMode,
)
from hydra.account_policy.router import AccountDecisionState, EntryIntent
import hydra.account_policy.causal_active_pool_replay as causal_replay
from hydra.research.pnl_state_risk_frontier import (
    _isolated_router_patch,
    frozen_pnl_state_profiles,
    pnl_state_multiplier,
)


def _state(
    *, daily: float = 0.0, buffer: float = 2_000.0, remaining: float = 3_000.0
) -> AccountDecisionState:
    return AccountDecisionState(
        balance=50_000.0 + daily,
        mll_floor=48_000.0,
        mll_buffer=buffer,
        daily_realized_pnl=daily,
        consecutive_losing_days=0,
        remaining_target=remaining,
        open_exposures=(),
    )


def _policy() -> ActiveRiskPoolPolicy:
    return ActiveRiskPoolPolicy(
        policy_id="isolated-test-policy",
        component_priority=("sleeve-a",),
        nominal_risk_charge_per_mini=(("sleeve-a", 100.0),),
        maximum_concurrent_sleeves=1,
        aggregate_open_risk_ceiling=2_000.0,
        maximum_mll_buffer_fraction=1.0,
        protected_mll_buffer=0.0,
        maximum_mini_equivalent=5.0,
        concurrency_scaling=ConcurrencyScaling.PRIORITY,
        same_instrument_conflict_rule=SameInstrumentConflictRule.PRIORITY,
        daily_loss_guard=2_000.0,
        daily_consistency_profit_guard=3_000.0,
        target_protection_distance=0.0,
        target_protection_mode=TargetProtectionMode.NONE,
        static_risk_tier=1.0,
    )


def _intent() -> EntryIntent:
    return EntryIntent(
        event_id="event-a",
        component_id="sleeve-a",
        market="MES",
        side=1,
        decision_ns=1,
        session_day=1,
        regime="TEST",
        base_quantity=4,
        base_mini_equivalent=0.4,
    )


def test_frozen_frontier_is_small_unique_and_anti_martingale() -> None:
    profiles = frozen_pnl_state_profiles()
    assert len(profiles) == 5
    assert len({row.profile_id for row in profiles}) == 5
    assert all(row.loss_multiplier <= row.base_multiplier for row in profiles)
    assert all(row.progress_step_2_multiplier >= row.loss_multiplier for row in profiles)


def test_profit_ladder_uses_only_current_account_state() -> None:
    profile = next(
        row
        for row in frozen_pnl_state_profiles()
        if row.profile_id == "pnl_state_profit_ladder"
    )
    multiplier, reason, headroom = pnl_state_multiplier(
        profile,
        _state(remaining=1_400.0),
        target_usd=3_000.0,
        mll_usd=2_000.0,
        consistency_fraction=0.50,
    )
    assert multiplier == 1.5
    assert reason == "PROFIT_STEP_2"
    assert headroom == 1_500.0

    multiplier, reason, _ = pnl_state_multiplier(
        profile,
        _state(daily=-200.0, buffer=600.0, remaining=1_400.0),
        target_usd=3_000.0,
        mll_usd=2_000.0,
        consistency_fraction=0.50,
    )
    assert multiplier == 0.5
    assert reason == "LOSS_OR_LOW_MLL_BUFFER"


def test_consistency_and_target_guards_precede_profit_scaling() -> None:
    profile = next(
        row
        for row in frozen_pnl_state_profiles()
        if row.profile_id == "pnl_state_fast_ladder"
    )
    multiplier, reason, _ = pnl_state_multiplier(
        profile,
        _state(daily=1_250.0, remaining=900.0),
        target_usd=3_000.0,
        mll_usd=2_000.0,
        consistency_fraction=0.50,
    )
    assert multiplier == 0.75
    assert reason == "CONSISTENCY_HEADROOM"


def test_process_local_router_patch_changes_quantity_and_restores_router() -> None:
    original = causal_replay.route_active_risk_entry
    profile = next(
        row
        for row in frozen_pnl_state_profiles()
        if row.profile_id == "pnl_state_profit_ladder"
    )
    with _isolated_router_patch(
        profile,
        target_usd=3_000.0,
        mll_usd=2_000.0,
        consistency_fraction=0.50,
    ):
        decision = causal_replay.route_active_risk_entry(
            _intent(), _state(remaining=1_400.0), policy=_policy()
        )
        assert decision.quantity == 6
        assert decision.to_dict()["pnl_state_multiplier"] == 1.5
        assert decision.to_dict()["future_outcome_fields_used_for_sizing"] is False
    assert causal_replay.route_active_risk_entry is original


def test_process_local_router_patch_restores_after_exception() -> None:
    original = causal_replay.route_active_risk_entry
    profile = frozen_pnl_state_profiles()[1]
    try:
        with _isolated_router_patch(
            profile,
            target_usd=3_000.0,
            mll_usd=2_000.0,
            consistency_fraction=0.50,
        ):
            raise RuntimeError("synthetic failure")
    except RuntimeError:
        pass
    assert causal_replay.route_active_risk_entry is original
