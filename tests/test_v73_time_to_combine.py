from __future__ import annotations

import pytest

from hydra.account_policy.basket import RoutedTrade
from hydra.account_policy.schema import BasketPolicy
from hydra.propfirm.combine_episode import TradePathEvent
from hydra.selection.time_to_combine import (
    AccountObservationStatus,
    FROZEN_TRADING_DAY_HORIZONS,
    evaluate_time_to_combine,
    run_censored_shared_account_episode,
)


DAY_NS = 100_000_000_000_000


def _trade(
    day: int,
    net: float,
    *,
    worst: float = -100.0,
    session_compliant: bool = True,
) -> RoutedTrade:
    decision = day * DAY_NS
    event = TradePathEvent(
        event_id=f"component-{day}-{net}-{session_compliant}",
        decision_ns=decision,
        exit_ns=decision + 1_000,
        session_day=day,
        net_pnl=net,
        gross_pnl=net + 10.0,
        worst_unrealized_pnl=worst,
        best_unrealized_pnl=max(net, 0.0),
        quantity=1,
        mini_equivalent=1.0,
        session_compliant=session_compliant,
    )
    return RoutedTrade("component", "MES", 1, event)


def _basket() -> BasketPolicy:
    return BasketPolicy(
        policy_id="time-to-combine-test",
        component_ids=("component",),
        archetype="STATIC_CROSS_FIT",
        component_priority=("component",),
        policy_version="hydra_account_policy_v7_2_crossfit_v1",
    )


def test_profitable_survivor_is_censored_not_failed() -> None:
    result = run_censored_shared_account_episode(
        {"component": (_trade(0, 500.0),)},
        tuple(range(100)),
        basket=_basket(),
        start_day=0,
        horizon_days=20,
    )

    assert result.observation_status is (
        AccountObservationStatus.OPERATIONAL_HORIZON_NOT_REACHED
    )
    assert result.legacy_episode.net_pnl == 500.0
    assert result.censored
    assert not result.target_reached
    assert not result.hard_rule_failed


def test_data_end_and_full_available_are_data_censored() -> None:
    events = {"component": (_trade(0, 500.0),)}
    fixed = run_censored_shared_account_episode(
        events,
        tuple(range(10)),
        basket=_basket(),
        start_day=0,
        horizon_days=20,
    )
    full = run_censored_shared_account_episode(
        events,
        tuple(range(10)),
        basket=_basket(),
        start_day=0,
        horizon_days=None,
    )

    assert fixed.observation_status is AccountObservationStatus.DATA_CENSORED
    assert fixed.available_horizon_days == fixed.observed_days == 10
    assert full.observation_status is AccountObservationStatus.DATA_CENSORED
    assert full.requested_horizon_days is None


def test_true_account_terminals_keep_their_scientific_status() -> None:
    passed = run_censored_shared_account_episode(
        {"component": (_trade(0, 4500.0), _trade(1, 4500.0))},
        tuple(range(30)),
        basket=_basket(),
        start_day=0,
        horizon_days=20,
    )
    breached = run_censored_shared_account_episode(
        {"component": (_trade(0, 100.0, worst=-4500.0),)},
        tuple(range(30)),
        basket=_basket(),
        start_day=0,
        horizon_days=20,
    )
    hard_failure = run_censored_shared_account_episode(
        {"component": (_trade(0, 100.0, session_compliant=False),)},
        tuple(range(30)),
        basket=_basket(),
        start_day=0,
        horizon_days=20,
    )

    assert passed.observation_status is AccountObservationStatus.TARGET_REACHED
    assert passed.legacy_episode.days_to_target == 2
    assert breached.observation_status is AccountObservationStatus.MLL_BREACHED
    assert hard_failure.observation_status is (
        AccountObservationStatus.HARD_RULE_FAILURE
    )


def test_frozen_horizons_report_probability_curve_censoring_and_net_costs() -> None:
    events = {
        "component": (
            _trade(0, 4500.0),
            _trade(1, 4500.0),
            _trade(20, 500.0),
        )
    }
    result = evaluate_time_to_combine(
        events,
        tuple(range(100)),
        basket=_basket(),
        start_days=(0, 20, 90),
        block_id="BLOCK_A",
    )

    assert tuple(result) == ("20", "40", "60", "90", "full_available")
    twenty = result["20"]
    assert twenty.block_id == "BLOCK_A"
    assert twenty.episode_count == 3
    assert twenty.pass_count == 1
    assert twenty.pass_probability == pytest.approx(1.0 / 3.0)
    assert twenty.mll_breach_probability == 0.0
    assert twenty.operational_horizon_not_reached_count == 1
    assert twenty.data_censored_count == 1
    assert twenty.censored_count == 2
    assert twenty.expected_trading_days_to_pass_conditional == 2.0
    assert twenty.median_trading_days_to_pass_conditional == 2.0
    assert twenty.net_after_costs_total == 9500.0
    assert twenty.total_execution_cost == 30.0
    assert twenty.target_progress_curve
    assert twenty.target_progress_curve[0]["observed_episode_count"] == 3
    assert twenty.target_progress_curve[-1]["pass_cumulative_probability"] == (
        pytest.approx(1.0 / 3.0)
    )
    assert result["full_available"].data_censored_count == 2


def test_horizon_policy_cannot_be_changed_at_evaluation_time() -> None:
    assert FROZEN_TRADING_DAY_HORIZONS == (20, 40, 60, 90)
    with pytest.raises(ValueError, match="frozen 20/40/60/90"):
        evaluate_time_to_combine(
            {"component": ()},
            tuple(range(100)),
            basket=_basket(),
            start_days=(0,),
            horizons=(20, 40, 60),
        )
