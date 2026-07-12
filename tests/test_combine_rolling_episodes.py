from __future__ import annotations

import math

import pytest

from hydra.propfirm.combine_episode import (
    CombineTerminal,
    TradePathEvent,
    run_combine_episode,
)
from hydra.propfirm.payout_episode import evaluate_rolling_xfa
from hydra.propfirm.rolling_combine import (
    EpisodeStartPolicy,
    evaluate_rolling_combine,
    select_episode_starts,
)
from hydra.propfirm.xfa_episode import XfaTerminal, run_xfa_episode


def _event(
    day: int,
    net: float,
    *,
    worst: float = -100.0,
    quantity: int = 1,
    mini_equivalent: float | None = None,
    offset: int = 0,
) -> TradePathEvent:
    decision = day * 1_000_000_000_000 + offset * 1_000_000_000
    return TradePathEvent(
        event_id=f"event-{day}-{offset}",
        decision_ns=decision,
        exit_ns=decision + 500_000_000,
        session_day=day,
        net_pnl=net,
        gross_pnl=net + 10.0,
        worst_unrealized_pnl=worst,
        best_unrealized_pnl=max(net, 0.0) + 50.0,
        quantity=quantity,
        mini_equivalent=float(mini_equivalent or quantity),
        regime="NORMAL",
    )


def test_combine_episode_passes_on_target_with_consistency() -> None:
    result = run_combine_episode(
        [_event(0, 4500.0), _event(1, 4500.0)],
        list(range(60)),
        start_day=0,
    )
    assert result.terminal == CombineTerminal.PASSED
    assert result.days_to_target == 2
    assert result.net_pnl == 9000.0
    assert result.consistency_ok
    assert result.best_day_concentration == 0.50
    assert not result.mll_breached


def test_combine_episode_uses_unrealized_pnl_and_touch_is_breach() -> None:
    result = run_combine_episode(
        [_event(0, 1000.0, worst=-4500.0)],
        list(range(60)),
        start_day=0,
    )
    assert result.terminal == CombineTerminal.MLL_BREACH
    assert result.mll_breached
    assert result.minimum_mll_buffer == 0.0
    assert result.net_pnl == 0.0
    assert result.terminal_reason == "intraday_unrealized_mll_touch_or_breach"


def test_combine_episode_times_out_and_inflates_consistency_target() -> None:
    result = run_combine_episode(
        [_event(0, 6000.0), _event(1, 3000.0)],
        list(range(30)),
        start_day=0,
        maximum_duration_days=30,
    )
    assert result.terminal == CombineTerminal.TIMEOUT
    assert result.required_target == 12000.0
    assert not result.consistency_ok
    assert math.isclose(result.target_progress, 0.75)


def test_combine_episode_rejects_contract_limit_and_overlap() -> None:
    violation = run_combine_episode(
        [_event(0, 100.0, quantity=16, mini_equivalent=16.0)],
        list(range(30)),
        start_day=0,
    )
    assert violation.terminal == CombineTerminal.COMPLIANCE_FAILURE
    assert not violation.contract_limit_compliant

    left = _event(0, 100.0, offset=0)
    right = TradePathEvent(
        **{
            **_event(0, 100.0, offset=1).to_dict(),
            "decision_ns": left.decision_ns + 1,
            "exit_ns": left.exit_ns + 100,
        }
    )
    with pytest.raises(ValueError, match="overlapping"):
        run_combine_episode([left, right], list(range(30)), start_day=0)


def test_episode_starts_are_deterministic_spaced_and_regime_balanced() -> None:
    days = list(range(180))
    regimes = {
        day: ("EXPANSION" if day % 3 == 0 else "NORMAL" if day % 3 == 1 else "CONTRACTION")
        for day in days
    }
    policy = EpisodeStartPolicy(
        maximum_starts=24,
        minimum_spacing_sessions=5,
        minimum_observation_sessions=30,
        maximum_duration_sessions=60,
    )
    first = select_episode_starts(days, day_regimes=regimes, policy=policy)
    second = select_episode_starts(days, day_regimes=regimes, policy=policy)
    assert first == second
    assert len(first) == 24
    assert all(right - left >= 5 for left, right in zip(first, first[1:]))
    assert len({regimes[day] for day in first}) == 3


def test_rolling_combine_reports_pass_breach_and_path_distributions() -> None:
    days = list(range(180))
    events = [_event(day, 1000.0, worst=-150.0) for day in range(0, 180, 4)]
    summary = evaluate_rolling_combine(
        events,
        days,
        day_regimes={day: "NORMAL" for day in days},
        policy=EpisodeStartPolicy(
            maximum_starts=12,
            minimum_spacing_sessions=10,
            minimum_observation_sessions=30,
            maximum_duration_sessions=60,
        ),
    )
    assert summary.episode_start_count == 12
    assert 1 < summary.effective_block_count < summary.episode_start_count
    assert summary.pass_count > 0
    assert summary.mll_breach_count == 0
    assert summary.pass_rate > 0
    assert summary.minimum_mll_buffer > 0
    assert summary.net_pnl_after_costs_unique_events == 45_000.0
    assert summary.account_path_distribution["terminal_net_median"] > 0


def test_xfa_episode_payout_and_scaling_plan() -> None:
    days = list(range(130))
    standard = run_xfa_episode(
        [_event(day, 200.0, worst=-20.0) for day in range(5)],
        days,
        start_day=0,
        path="STANDARD",
    )
    assert standard.terminal == XfaTerminal.SURVIVED_WINDOW
    assert standard.payout_cycles == 1
    assert standard.first_payout_day == 5
    assert standard.trader_net_payout == 450.0

    violation = run_xfa_episode(
        [_event(0, 200.0, quantity=4, mini_equivalent=4.0)],
        days,
        start_day=0,
        path="STANDARD",
    )
    assert violation.terminal == XfaTerminal.COMPLIANCE_FAILURE
    assert not violation.contract_limit_compliant


def test_rolling_xfa_reports_cycles_survival_and_timing() -> None:
    days = list(range(220))
    events = [_event(day, 200.0, worst=-20.0) for day in range(220)]
    summary = evaluate_rolling_xfa(events, days, maximum_starts=8)
    assert summary.episode_start_count == 8
    assert summary.expected_payout_cycles_before_ruin > 1.0
    assert summary.payout_probability == 1.0
    assert summary.survival_rate == 1.0
    assert summary.median_first_payout_day is not None
