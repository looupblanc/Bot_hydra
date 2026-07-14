from __future__ import annotations

from dataclasses import replace

import pytest

import hydra.account_policy.basket as basket_module
from hydra.account_policy.basket import (
    RoutedTrade,
    evaluate_account_policy,
    run_shared_account_episode,
)
from hydra.account_policy.controller import generate_controller_population
from hydra.account_policy.fitness import paired_controller_evidence
from hydra.account_policy.router import (
    AccountDecisionState,
    EntryIntent,
    OpenExposure,
    RoutingDecision,
    route_entry,
)
from hydra.account_policy.schema import BasketPolicy, ControllerPolicy
from hydra.propfirm.combine_episode import CombineTerminal, TradePathEvent
from hydra.propfirm.rolling_combine import EpisodeStartPolicy


DAY_NS = 100_000_000_000_000


def _trade(
    component: str,
    market: str,
    day: int,
    net: float,
    *,
    worst: float = -100.0,
    start_offset: int = 0,
    duration: int = 100,
    quantity: int = 1,
    side: int = 1,
) -> RoutedTrade:
    decision = day * DAY_NS + start_offset
    event = TradePathEvent(
        event_id=f"{component}-{day}-{start_offset}",
        decision_ns=decision,
        exit_ns=decision + duration,
        session_day=day,
        net_pnl=net,
        gross_pnl=net + 10.0 * quantity,
        worst_unrealized_pnl=worst,
        best_unrealized_pnl=max(net, 0.0) + 50.0,
        quantity=quantity,
        mini_equivalent=float(quantity),
        regime="VOLATILITY_NORMAL",
    )
    return RoutedTrade(component, market, side, event)


def _basket(*components: str) -> BasketPolicy:
    return BasketPolicy(
        policy_id="basket-test",
        component_ids=tuple(components),
        archetype="BALANCED_PASS_PROBABILITY",
        maximum_simultaneous_positions=4,
        maximum_mini_equivalent=15,
        component_priority=tuple(components),
    )


def _controller(*components: str, loss: float = 1500.0, lock: float = 2500.0) -> ControllerPolicy:
    return ControllerPolicy(
        controller_id="controller-test",
        basket_policy_id="basket-test",
        component_priority=tuple(components),
        daily_loss_limit=loss,
        daily_profit_lock=lock,
        loss_streak_derisk_after=2,
        low_buffer_threshold=1800.0,
        critical_buffer_threshold=700.0,
        maximum_simultaneous_positions=3,
    )


def test_shared_account_hits_one_target_without_summing_standalone_results() -> None:
    events = {
        "left": tuple(_trade("left", "ES", day, 2250.0) for day in (0, 1)),
        "right": tuple(_trade("right", "CL", day, 2250.0, start_offset=10) for day in (0, 1)),
    }
    result = run_shared_account_episode(
        events,
        list(range(60)),
        basket=_basket("left", "right"),
        start_day=0,
        maximum_duration_days=60,
    )

    assert result.terminal is CombineTerminal.PASSED
    assert result.net_pnl == 9000.0
    assert result.days_to_target == 2
    assert result.accepted_events == 4
    assert result.component_contribution == {"left": 4500.0, "right": 4500.0}
    assert result.maximum_mini_equivalent == 2.0


def test_correlated_open_adverse_excursions_breach_one_shared_mll() -> None:
    events = {
        "left": (_trade("left", "ES", 0, 1000.0, worst=-2500.0, duration=1000),),
        "right": (_trade("right", "CL", 0, 1000.0, worst=-2500.0, start_offset=10, duration=1000),),
    }
    result = run_shared_account_episode(
        events,
        list(range(60)),
        basket=_basket("left", "right"),
        start_day=0,
        maximum_duration_days=60,
    )

    assert result.terminal is CombineTerminal.MLL_BREACH
    assert result.minimum_mll_buffer <= 0.0
    assert result.terminal_reason == "correlated_open_position_mll_touch_or_breach"


def test_conflict_resolution_and_shared_contract_limit_are_executable() -> None:
    same_market = {
        "left": (_trade("left", "ES", 0, 100.0, duration=1000, quantity=8),),
        "right": (_trade("right", "ES", 0, 100.0, start_offset=10, duration=1000, quantity=8),),
    }
    result = run_shared_account_episode(
        same_market,
        list(range(30)),
        basket=_basket("left", "right"),
        start_day=0,
        maximum_duration_days=30,
    )
    assert result.accepted_events == 1
    assert result.skipped_events == 1
    assert result.conflict_count == 1
    assert result.maximum_mini_equivalent == 8.0


def test_v72_simultaneous_entry_uses_frozen_component_priority() -> None:
    low = _trade("low", "ES", 0, 100.0, start_offset=1, duration=10)
    high = _trade("high", "ES", 0, 300.0, start_offset=1, duration=10)
    events = {
        "low": (replace(low, event=replace(low.event, event_id="a-sorts-first")),),
        "high": (replace(high, event=replace(high.event, event_id="z-sorts-last")),),
    }
    basket = BasketPolicy(
        policy_id="v72-priority",
        component_ids=("low", "high"),
        archetype="STATIC_CROSS_FIT",
        maximum_simultaneous_positions=1,
        component_priority=("high", "low"),
        policy_version="hydra_account_policy_v7_2_crossfit_v1",
    )

    result = run_shared_account_episode(
        events,
        list(range(10)),
        basket=basket,
        start_day=0,
        maximum_duration_days=10,
    )

    assert result.component_contribution == {"high": 300.0}
    assert result.conflict_count == 1
    assert result.skipped_reasons == {"MAXIMUM_SIMULTANEOUS_POSITIONS": 1}


def test_controller_profit_lock_and_daily_loss_guard_use_realized_state_only() -> None:
    profit_events = {
        "left": (
            _trade("left", "ES", 0, 2600.0, duration=10),
            _trade("left", "ES", 0, 2600.0, start_offset=20, duration=10),
        ),
        "right": (),
    }
    profit = run_shared_account_episode(
        profit_events,
        list(range(30)),
        basket=_basket("left", "right"),
        controller=_controller("left", "right"),
        start_day=0,
        maximum_duration_days=30,
    )
    assert profit.accepted_events == 1
    assert profit.skipped_reasons["DAILY_PROFIT_LOCK"] == 1

    loss_events = {
        "left": (
            _trade("left", "ES", 0, -1600.0, duration=10),
            _trade("left", "ES", 0, 500.0, start_offset=20, duration=10),
        ),
        "right": (),
    }
    loss = run_shared_account_episode(
        loss_events,
        list(range(30)),
        basket=_basket("left", "right"),
        controller=_controller("left", "right"),
        start_day=0,
        maximum_duration_days=30,
    )
    assert loss.accepted_events == 1
    assert loss.skipped_reasons["DAILY_LOSS_GUARD"] == 1


def test_router_interface_cannot_receive_future_trade_outcome() -> None:
    intent = EntryIntent(
        event_id="intent",
        component_id="left",
        market="ES",
        side=1,
        decision_ns=1,
        session_day=0,
        regime="VOLATILITY_NORMAL",
        base_quantity=2,
        base_mini_equivalent=2.0,
    )
    state = AccountDecisionState(
        balance=150000.0,
        mll_floor=145500.0,
        mll_buffer=4500.0,
        daily_realized_pnl=0.0,
        consecutive_losing_days=0,
        remaining_target=9000.0,
        open_exposures=(),
    )
    policy = _controller("left", "right")

    assert route_entry(intent, state, policy=policy) == route_entry(
        intent, state, policy=policy
    )
    assert "net_pnl" not in EntryIntent.__dataclass_fields__
    assert "worst_unrealized_pnl" not in EntryIntent.__dataclass_fields__


def test_shadow_timeline_exposes_outcome_only_after_frozen_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _trade("left", "ES", 0, 200.0, duration=10)
    second = _trade("left", "ES", 0, -300.0, start_offset=20, duration=10)
    observed: list[tuple[int, dict[str, tuple[float, ...]]]] = []

    def capture(
        intent: EntryIntent,
        state: AccountDecisionState,
        *,
        policy: ControllerPolicy,
    ) -> RoutingDecision:
        observed.append((intent.decision_ns, state.shadow_outcome_map))
        return RoutingDecision(False, 0, 0.0, "TEST_BLOCK", policy.controller_id)

    monkeypatch.setattr(basket_module, "route_entry", capture)
    result = run_shared_account_episode(
        {"left": (first, second)},
        list(range(30)),
        basket=_basket("left"),
        controller=_controller("left"),
        start_day=0,
        maximum_duration_days=30,
    )

    assert result.accepted_events == 0
    assert len(observed) == 2
    assert observed[0][1] == {"left": ()}
    assert observed[1][1] == {"left": (2.0,)}
    assert -3.0 not in observed[1][1]["left"]


def test_parent_static_and_controller_share_identical_episode_starts() -> None:
    days = list(range(220))
    events = {
        "left": tuple(_trade("left", "ES", day, 300.0) for day in range(0, 220, 5)),
        "right": tuple(_trade("right", "CL", day, 250.0) for day in range(2, 220, 5)),
    }
    starts = tuple(range(0, 120, 10))
    policy = EpisodeStartPolicy(
        maximum_starts=24,
        minimum_spacing_sessions=5,
        minimum_observation_sessions=30,
        maximum_duration_sessions=60,
        regime_balanced=False,
    )
    static = evaluate_account_policy(
        events,
        days,
        basket=_basket("left", "right"),
        episode_policy=policy,
        explicit_start_days=starts,
    )
    adaptive = evaluate_account_policy(
        events,
        days,
        basket=_basket("left", "right"),
        controller=_controller("left", "right"),
        episode_policy=policy,
        explicit_start_days=starts,
    )

    assert static.episode_start_days == adaptive.episode_start_days == starts
    assert static.episode_start_count == adaptive.episode_start_count == len(starts)


def test_controller_population_is_deterministic_and_contains_random_control() -> None:
    basket = _basket("left", "right")
    first = generate_controller_population(basket, generation_index=3)
    second = generate_controller_population(basket, generation_index=3)

    assert first == second
    assert len(first) == 5
    assert sum(item.random_control_seed is not None for item in first) == 1
    assert len({item.fingerprint for item in first}) == len(first)


def test_controller_paired_evidence_uses_identical_starts_and_sign_test() -> None:
    baseline = [
        {
            "start_day": day,
            "passed": False,
            "mll_breached": False,
            "target_progress": 0.30,
            "consistency_ok": True,
            "net_pnl": 2700.0,
        }
        for day in range(12)
    ]
    candidate = [
        {
            **row,
            "target_progress": 0.60,
            "net_pnl": 5400.0,
        }
        for row in baseline
    ]
    random = [
        {
            **row,
            "target_progress": 0.20,
            "net_pnl": 1800.0,
        }
        for row in baseline
    ]

    evidence = paired_controller_evidence(candidate, baseline, random)

    assert evidence["paired_start_count"] == 12.0
    assert evidence["static_median_utility_delta"] > 0.0
    assert evidence["random_median_utility_delta"] > 0.0
    assert evidence["static_one_sided_p"] < 0.01
    assert evidence["random_one_sided_p"] < 0.01
