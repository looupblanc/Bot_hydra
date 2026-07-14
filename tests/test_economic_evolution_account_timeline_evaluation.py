from __future__ import annotations

from dataclasses import replace

import hydra.account_policy.basket as basket_engine
from hydra.account_policy.basket import RoutedTrade
from hydra.account_policy.router import AccountDecisionState, EntryIntent
from hydra.economic_evolution.account_evaluation import ExactSleeveRuntime
from hydra.economic_evolution.account_timeline import (
    ACCOUNT_TIMELINE_LIMITS,
    AccountTimelinePolicy,
    AccountTimelinePolicyPair,
)
from hydra.economic_evolution.account_timeline_evaluation import (
    _patched_account_timeline_router,
    evaluate_account_timeline_policy_pair,
    evaluate_account_timeline_policy_pairs,
)
from hydra.economic_evolution.schema import EconomicRole, stable_hash
from hydra.propfirm.combine_episode import TradePathEvent
from hydra.propfirm.rolling_combine import EpisodeStartPolicy


DAY_NS = 100_000_000_000_000
COMPONENTS = tuple(f"timeline-component-{index}" for index in range(6))
MARKETS = ("ES", "NQ", "CL", "GC", "RTY", "YM")


def _policy(policy_id: str, *, rotate: int = 0) -> AccountTimelinePolicy:
    return AccountTimelinePolicy(
        policy_id=policy_id,
        component_ids=COMPONENTS,
        score_source_map=tuple(
            (component, COMPONENTS[(index + rotate) % len(COMPONENTS)])
            for index, component in enumerate(COMPONENTS)
        ),
        **ACCOUNT_TIMELINE_LIMITS,
    )


def _pair() -> AccountTimelinePolicyPair:
    return AccountTimelinePolicyPair(
        pair_id="timeline-pair",
        real_policy=_policy("timeline-real"),
        matched_control_policy=_policy("timeline-control", rotate=1),
        membership_hash=stable_hash({"components": list(COMPONENTS)}),
    )


def _trade(
    component: str,
    market: str,
    day: int,
    net: float,
    *,
    offset: int = 0,
) -> RoutedTrade:
    decision = day * DAY_NS + offset
    return RoutedTrade(
        component,
        market,
        1,
        TradePathEvent(
            event_id=f"{component}-{day}-{offset}",
            decision_ns=decision,
            exit_ns=decision + 100,
            session_day=day,
            net_pnl=net,
            gross_pnl=net + 5.0,
            worst_unrealized_pnl=-100.0,
            best_unrealized_pnl=max(net, 0.0) + 20.0,
            quantity=1,
            mini_equivalent=1.0,
            regime="VOLATILITY_NORMAL",
        ),
    )


def _state() -> AccountDecisionState:
    return AccountDecisionState(
        balance=150_000.0,
        mll_floor=145_500.0,
        mll_buffer=4_500.0,
        daily_realized_pnl=0.0,
        consecutive_losing_days=0,
        remaining_target=9_000.0,
        open_exposures=(),
    )


def _runtimes() -> dict[str, ExactSleeveRuntime]:
    output: dict[str, ExactSleeveRuntime] = {}
    for index, (component, market) in enumerate(
        zip(COMPONENTS, MARKETS, strict=True)
    ):
        sign = 1.0 if index % 2 == 0 else -1.0
        events = tuple(
            _trade(
                component,
                market,
                day,
                140.0 if (day // 4) % 2 == index % 2 else -35.0 * sign,
                offset=index * 1_000,
            )
            for day in range(50)
        )
        output[component] = ExactSleeveRuntime(
            sleeve_id=component,
            signal_market=market,
            execution_market=market,
            role=EconomicRole.PRIMARY_ALPHA,
            source_campaign="TEST_ACCOUNT_TIMELINE_0012",
            specification_hash=stable_hash({"component": component}),
            eligible_session_days=tuple(range(50)),
            events=events,
            event_count=len(events),
            net_pnl=sum(row.event.net_pnl for row in events),
            cost_stress_1_5x_net=sum(row.event.net_pnl - 2.5 for row in events),
            maximum_drawdown=500.0,
            best_positive_event_share=0.1,
            exit_implementation="EXACT_TIME_EXIT",
        )
    return output


def test_timeline_patch_uses_only_outcomes_completed_by_decision() -> None:
    component = COMPONENTS[0]
    rows = tuple(
        _trade(component, "ES", 0, 100.0, offset=index * 200)
        for index in range(4)
    ) + (_trade(component, "ES", 0, -300.0, offset=10_000),)
    intent = EntryIntent(
        event_id="decision",
        component_id=component,
        market="ES",
        side=1,
        decision_ns=1_000,
        session_day=0,
        regime="VOLATILITY_NORMAL",
        base_quantity=1,
        base_mini_equivalent=1.0,
    )
    original = basket_engine.route_entry
    with _patched_account_timeline_router({component: rows}):
        decision = basket_engine.route_entry(
            intent,
            _state(),
            policy=_policy("causal-timeline"),  # type: ignore[arg-type]
        )
        assert decision.allow is True
        assert decision.quantity == 2
        assert decision.reason == "POSITIVE_COMPLETED_TIMELINE_SCALE"
    assert basket_engine.route_entry is original


def test_timeline_patch_restores_historical_engine_on_error() -> None:
    original = basket_engine.route_entry
    try:
        with _patched_account_timeline_router({}):
            raise RuntimeError("fail closed")
    except RuntimeError:
        pass
    assert basket_engine.route_entry is original


def test_pair_evaluation_is_deterministic_same_start_and_governed() -> None:
    pair = _pair()
    starts = (0, 10)
    episode_policy = EpisodeStartPolicy(
        maximum_starts=2,
        minimum_spacing_sessions=5,
        minimum_observation_sessions=20,
        maximum_duration_sessions=20,
        regime_balanced=False,
    )
    first = evaluate_account_timeline_policy_pair(
        pair,
        _runtimes(),
        starts=starts,
        episode_policy=episode_policy,
    )
    second = evaluate_account_timeline_policy_pairs(
        (pair,),
        _runtimes(),
        starts=starts,
        episode_policy=episode_policy,
        worker_count=2,
    )[0]

    assert first == second
    assert first["identical_episode_starts"] is True
    assert first["real_evaluation"]["episode_start_days"] == list(starts)
    assert first["matched_control_evaluation"]["episode_start_days"] == list(starts)
    assert first["new_data_purchase_count"] == 0
    assert first["q4_access_delta"] == 0
    assert first["orders"] == 0
    assert first["validated"] is False


def test_timeline_policy_cannot_gain_order_capability() -> None:
    try:
        replace(_policy("orders-forbidden"), outbound_order_capability=True)
    except ValueError as error:
        assert "cannot submit orders" in str(error)
    else:
        raise AssertionError("outbound order capability was accepted")
