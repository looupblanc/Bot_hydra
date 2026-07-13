from __future__ import annotations

from hydra.propfirm.censored_combine import (
    CombineObservationStatus,
    evaluate_censored_combine_horizons,
    run_censored_combine_episode,
)
from hydra.propfirm.combine_episode import TradePathEvent


def _event(day: int, net: float, *, worst: float = -100.0) -> TradePathEvent:
    decision = day * 1_000_000_000_000
    return TradePathEvent(
        event_id=f"event-{day}",
        decision_ns=decision,
        exit_ns=decision + 60_000_000_000,
        session_day=day,
        net_pnl=net,
        gross_pnl=net + 10.0,
        worst_unrealized_pnl=worst,
        best_unrealized_pnl=max(net, 0.0),
        quantity=1,
        mini_equivalent=1.0,
    )


def test_operational_horizon_is_not_an_official_failure() -> None:
    result = run_censored_combine_episode(
        [_event(0, 500.0)],
        list(range(100)),
        start_day=0,
        horizon_days=20,
    )

    assert result.observation_status is (
        CombineObservationStatus.OPERATIONAL_HORIZON_NOT_REACHED
    )
    assert result.legacy_result.net_pnl == 500.0
    assert result.censored
    assert not result.target_reached


def test_short_data_window_is_data_censored() -> None:
    result = run_censored_combine_episode(
        [_event(0, 500.0)],
        list(range(10)),
        start_day=0,
        horizon_days=20,
    )

    assert result.observation_status is CombineObservationStatus.DATA_CENSORED
    assert result.available_horizon_days == 10
    assert result.observed_days == 10


def test_full_available_nonterminal_episode_is_data_censored() -> None:
    result = run_censored_combine_episode(
        [_event(0, 500.0)],
        list(range(30)),
        start_day=0,
        horizon_days=None,
    )

    assert result.observation_status is CombineObservationStatus.DATA_CENSORED
    assert result.requested_horizon_days is None


def test_target_and_mll_remain_true_terminal_events() -> None:
    passed = run_censored_combine_episode(
        [_event(0, 4500.0), _event(1, 4500.0)],
        list(range(30)),
        start_day=0,
        horizon_days=20,
    )
    breached = run_censored_combine_episode(
        [_event(0, 100.0, worst=-4500.0)],
        list(range(30)),
        start_day=0,
        horizon_days=20,
    )

    assert passed.observation_status is CombineObservationStatus.TARGET_REACHED
    assert passed.legacy_result.days_to_target == 2
    assert breached.observation_status is CombineObservationStatus.MLL_BREACHED


def test_multi_horizon_summary_reports_censoring_and_time_curve() -> None:
    result = evaluate_censored_combine_horizons(
        [_event(day, 1000.0) for day in range(0, 100, 4)],
        list(range(100)),
        start_days=(0, 50, 90),
        horizons=(20, 40, 60, 90),
    )

    assert tuple(result) == ("20", "40", "60", "90", "full_available")
    assert result["20"].episode_count == 3
    assert result["20"].operational_horizon_not_reached_count >= 1
    assert result["20"].data_censored_count >= 1
    assert result["full_available"].data_censored_count >= 1
    assert result["20"].target_time_curve
    assert result["20"].median_observed_subscription_cost_usd >= 149.0
