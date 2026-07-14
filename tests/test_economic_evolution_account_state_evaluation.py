from __future__ import annotations

from dataclasses import replace

import pytest

import hydra.account_policy.basket as basket_engine
from hydra.account_policy.basket import RoutedTrade
from hydra.account_policy.router import (
    AccountDecisionState,
    EntryIntent,
    OpenExposure,
)
from hydra.economic_evolution.account_evaluation import ExactSleeveRuntime
from hydra.economic_evolution.account_state_evaluation import (
    AccountStateMode,
    AccountStatePolicyPair,
    AccountStateRoutingPolicy,
    _patched_account_router,
    account_state_mode,
    evaluate_account_state_policy_pair,
    evaluate_account_state_policy_pairs,
    route_account_state_entry,
)
from hydra.economic_evolution.schema import EconomicRole, stable_hash
from hydra.propfirm.combine_episode import TradePathEvent
from hydra.propfirm.rolling_combine import EpisodeStartPolicy


DAY_NS = 100_000_000_000_000
COMPONENTS = tuple(f"component-{index}" for index in range(6))
REAL_ROLES = (
    (COMPONENTS[0], "PRIMARY_ALPHA"),
    (COMPONENTS[1], "TARGET_ACCELERATOR"),
    (COMPONENTS[2], "MLL_STABILIZER"),
    (COMPONENTS[3], "CONSISTENCY_SMOOTHER"),
    (COMPONENTS[4], "SESSION_DIVERSIFIER"),
    (COMPONENTS[5], "MARKET_DIVERSIFIER"),
)
CONTROL_ROLES = tuple(
    (component, REAL_ROLES[(index + 2) % len(REAL_ROLES)][1])
    for index, component in enumerate(COMPONENTS)
)


def _policy(
    policy_id: str,
    *,
    roles: tuple[tuple[str, str], ...] = REAL_ROLES,
) -> AccountStateRoutingPolicy:
    return AccountStateRoutingPolicy(
        policy_id=policy_id,
        component_ids=COMPONENTS,
        component_roles=roles,
        daily_loss_guard=1_000.0,
        daily_profit_lock=1_500.0,
        critical_buffer=750.0,
        protect_buffer=2_250.0,
        accelerate_buffer=4_000.0,
        accelerate_remaining_target=6_000.0,
        loss_streak_protect_after=2,
        balanced_maximum_positions=2,
        accelerate_maximum_positions=3,
        protect_maximum_positions=1,
        accelerate_risk_units=2,
        maximum_mini_equivalent=15,
    )


def _state(**updates: object) -> AccountDecisionState:
    values: dict[str, object] = {
        "balance": 150_000.0,
        "mll_floor": 145_500.0,
        "mll_buffer": 4_500.0,
        "daily_realized_pnl": 0.0,
        "consecutive_losing_days": 0,
        "remaining_target": 9_000.0,
        "open_exposures": (),
    }
    values.update(updates)
    return AccountDecisionState(**values)  # type: ignore[arg-type]


def _intent(component: str = COMPONENTS[0], *, market: str = "ES") -> EntryIntent:
    return EntryIntent(
        event_id=f"intent-{component}",
        component_id=component,
        market=market,
        side=1,
        decision_ns=10,
        session_day=0,
        regime="VOLATILITY_NORMAL",
        base_quantity=1,
        base_mini_equivalent=1.0,
    )


def _trade(component: str, market: str, day: int, net: float) -> RoutedTrade:
    decision = day * DAY_NS + COMPONENTS.index(component) * 1_000
    event = TradePathEvent(
        event_id=f"{component}-{day}",
        decision_ns=decision,
        exit_ns=decision + 100,
        session_day=day,
        net_pnl=net,
        gross_pnl=net + 5.0,
        worst_unrealized_pnl=-40.0,
        best_unrealized_pnl=max(net, 0.0) + 20.0,
        quantity=1,
        mini_equivalent=1.0,
        regime="VOLATILITY_NORMAL",
    )
    return RoutedTrade(component, market, 1, event)


def _runtimes() -> dict[str, ExactSleeveRuntime]:
    markets = ("ES", "NQ", "CL", "GC", "RTY", "YM")
    output: dict[str, ExactSleeveRuntime] = {}
    for index, (component, market) in enumerate(zip(COMPONENTS, markets, strict=True)):
        events = tuple(
            _trade(component, market, day, 120.0 if index == 0 else 25.0)
            for day in range(40)
        )
        output[component] = ExactSleeveRuntime(
            sleeve_id=component,
            signal_market=market,
            execution_market=market,
            role=EconomicRole(REAL_ROLES[index][1]),
            source_campaign="TEST_ACCOUNT_STATE_0011",
            specification_hash=stable_hash({"component": component}),
            eligible_session_days=tuple(range(40)),
            events=events,
            event_count=len(events),
            net_pnl=sum(row.event.net_pnl for row in events),
            cost_stress_1_5x_net=sum(
                row.event.net_pnl - 2.5 for row in events
            ),
            maximum_drawdown=0.0,
            best_positive_event_share=1.0 / len(events),
            exit_implementation="EXACT_TIME_EXIT",
        )
    return output


def _pair() -> AccountStatePolicyPair:
    return AccountStatePolicyPair(
        pair_id="pair-0011",
        real_policy=_policy("real-0011"),
        matched_control_policy=_policy("control-0011", roles=CONTROL_ROLES),
        membership_hash=stable_hash({"component_ids": list(COMPONENTS)}),
    )


def test_account_state_modes_use_only_frozen_current_account_state() -> None:
    policy = _policy("mode-policy")

    assert account_state_mode(policy, _state()) is AccountStateMode.ACCELERATE
    assert account_state_mode(
        policy, _state(mll_buffer=3_000.0)
    ) is AccountStateMode.BALANCED
    assert account_state_mode(
        policy, _state(mll_buffer=2_000.0)
    ) is AccountStateMode.PROTECT
    assert account_state_mode(
        policy, _state(consecutive_losing_days=2)
    ) is AccountStateMode.PROTECT
    assert account_state_mode(
        policy, _state(daily_realized_pnl=-1_000.0)
    ) is AccountStateMode.LOCKED
    assert account_state_mode(
        policy, _state(daily_realized_pnl=1_500.0)
    ) is AccountStateMode.LOCKED
    assert "net_pnl" not in EntryIntent.__dataclass_fields__
    assert "future" not in " ".join(AccountDecisionState.__dataclass_fields__)


def test_account_state_router_accelerates_and_protects_by_frozen_role() -> None:
    policy = _policy("routing-policy")
    accelerated = route_account_state_entry(_intent(), _state(), policy=policy)
    assert accelerated.allow is True
    assert accelerated.quantity == 2
    assert accelerated.mini_equivalent == 2.0
    assert accelerated.reason.startswith("ACCELERATE_ROLE_PRIMARY_ALPHA")

    protected_alpha = route_account_state_entry(
        _intent(), _state(mll_buffer=2_000.0), policy=policy
    )
    assert protected_alpha.allow is False
    assert protected_alpha.reason == "PROTECT_MODE_ROLE_VETO"

    protected_stabilizer = route_account_state_entry(
        _intent(COMPONENTS[2], market="CL"),
        _state(mll_buffer=2_000.0),
        policy=policy,
    )
    assert protected_stabilizer.allow is True
    assert protected_stabilizer.quantity == 1


def test_account_state_router_enforces_concurrency_market_and_contract_limits() -> None:
    policy = _policy("limits-policy")
    exposure = OpenExposure(COMPONENTS[3], "NQ", 1, 14.0, 100)
    contract = route_account_state_entry(
        _intent(COMPONENTS[2], market="CL"),
        _state(mll_buffer=2_000.0, open_exposures=(exposure,)),
        policy=policy,
    )
    assert contract.allow is False
    assert contract.reason == "PROTECT_MAXIMUM_POSITIONS"

    same_market = route_account_state_entry(
        _intent(COMPONENTS[1], market="NQ"),
        _state(open_exposures=(replace(exposure, mini_equivalent=1.0),)),
        policy=policy,
    )
    assert same_market.allow is False
    assert same_market.reason == "SAME_MARKET_CONFLICT"

    high_exposure = replace(exposure, market="GC", mini_equivalent=14.0)
    shared_limit = route_account_state_entry(
        _intent(COMPONENTS[0], market="ES"),
        _state(open_exposures=(high_exposure,)),
        policy=policy,
    )
    assert shared_limit.allow is False
    assert shared_limit.reason == "SHARED_CONTRACT_LIMIT"


def test_matched_control_pair_keeps_membership_limits_and_role_multiset() -> None:
    pair = _pair()
    payload = pair.to_dict()
    assert payload["identical_sleeve_membership"] is True
    assert payload["same_state_thresholds"] is True
    assert sorted(payload["real_component_roles"].values()) == sorted(
        payload["matched_control_component_roles"].values()
    )

    with pytest.raises(ValueError, match="permute roles"):
        AccountStatePolicyPair(
            pair_id="invalid-same-roles",
            real_policy=pair.real_policy,
            matched_control_policy=replace(
                pair.real_policy, policy_id="invalid-control"
            ),
            membership_hash=pair.membership_hash,
        )
    with pytest.raises(ValueError, match="state limits"):
        AccountStatePolicyPair(
            pair_id="invalid-limits",
            real_policy=pair.real_policy,
            matched_control_policy=replace(
                pair.matched_control_policy, daily_profit_lock=1_600.0
            ),
            membership_hash=pair.membership_hash,
        )


def test_local_router_patch_restores_frozen_shared_engine() -> None:
    original = basket_engine.route_entry
    with _patched_account_router():
        assert basket_engine.route_entry is route_account_state_entry
    assert basket_engine.route_entry is original

    with pytest.raises(RuntimeError):
        with _patched_account_router():
            raise RuntimeError("fail closed")
    assert basket_engine.route_entry is original


def test_pair_evaluation_is_deterministic_same_start_and_no_order() -> None:
    pair = _pair()
    starts = (0, 10)
    episode_policy = EpisodeStartPolicy(
        maximum_starts=2,
        minimum_spacing_sessions=5,
        minimum_observation_sessions=20,
        maximum_duration_sessions=20,
        regime_balanced=False,
    )
    first = evaluate_account_state_policy_pair(
        pair,
        _runtimes(),
        starts=starts,
        episode_policy=episode_policy,
    )
    second = evaluate_account_state_policy_pairs(
        (pair,),
        _runtimes(),
        starts=starts,
        episode_policy=episode_policy,
        worker_count=1,
    )[0]

    assert first == second
    assert first["identical_episode_starts"] is True
    assert first["episode_start_count"] == 2
    assert first["real_evaluation"]["episode_start_days"] == list(starts)
    assert first["matched_control_evaluation"]["episode_start_days"] == list(starts)
    assert first["new_data_purchase_count"] == 0
    assert first["q4_access_delta"] == 0
    assert first["orders"] == 0
    assert first["validated"] is False


def test_policy_rejects_order_capability_and_unbounded_state_parameters() -> None:
    with pytest.raises(ValueError, match="cannot submit orders"):
        replace(_policy("no-orders"), outbound_order_capability=True)
    with pytest.raises(ValueError, match="buffer thresholds"):
        replace(_policy("bad-buffers"), protect_buffer=4_100.0)
    with pytest.raises(ValueError, match="concurrency limits"):
        replace(_policy("bad-concurrency"), accelerate_maximum_positions=4)
