from __future__ import annotations

from types import SimpleNamespace

import pytest

from hydra.account_policy.active_risk_pool import (
    ActiveRiskPoolPolicy,
    ConcurrencyScaling,
    SameInstrumentConflictRule,
    TargetProtectionMode,
)
from hydra.account_policy.causal_active_pool_replay import (
    run_causal_shared_account_episode,
)
from hydra.propfirm.combine_episode import CombineTerminal, TradePathEvent


DAY = 19_541


def _policy(*component_ids: str) -> ActiveRiskPoolPolicy:
    return ActiveRiskPoolPolicy(
        policy_id="causal-account-test",
        component_priority=tuple(component_ids),
        nominal_risk_charge_per_mini=tuple(
            (component_id, 2_000.0) for component_id in component_ids
        ),
        maximum_concurrent_sleeves=len(component_ids),
        aggregate_open_risk_ceiling=4_500.0,
        maximum_mll_buffer_fraction=1.0,
        protected_mll_buffer=0.0,
        maximum_mini_equivalent=15.0,
        concurrency_scaling=ConcurrencyScaling.PROPORTIONAL,
        same_instrument_conflict_rule=SameInstrumentConflictRule.PRIORITY,
        daily_loss_guard=4_500.0,
        daily_consistency_profit_guard=9_000.0,
        target_protection_distance=0.0,
        target_protection_mode=TargetProtectionMode.NONE,
        static_risk_tier=1.0,
    )


def _trajectory(
    component_id: str,
    *,
    entry_ns: int,
    exit_ns: int,
    marks: tuple[tuple[int, float, float, float], ...],
    net_pnl: float = 100.0,
    completed: bool = True,
    censor_time_ns: int | None = None,
) -> SimpleNamespace:
    event = TradePathEvent(
        event_id=f"{component_id}:NORMAL",
        decision_ns=entry_ns,
        exit_ns=exit_ns,
        session_day=DAY,
        net_pnl=net_pnl,
        gross_pnl=net_pnl + 1.24,
        worst_unrealized_pnl=min(row[1] for row in marks),
        best_unrealized_pnl=max(row[3] for row in marks),
        quantity=1,
        mini_equivalent=0.1,
    )
    return SimpleNamespace(
        component_id=component_id,
        market=f"M-{component_id}",
        side=1,
        event=event,
        marks=tuple(
            SimpleNamespace(
                availability_time_ns=timestamp,
                worst_unrealized_pnl=worst,
                current_unrealized_pnl=current,
                best_unrealized_pnl=best,
            )
            for timestamp, worst, current, best in marks
        ),
        initial_unrealized_pnl=-1.24,
        completed=completed,
        censor_time_ns=censor_time_ns,
        censor_reason=(None if completed else "MISSING_CONTIGUOUS_FUTURE_BAR"),
    )


def test_open_unrealized_loss_reduces_mll_capacity_for_next_entry() -> None:
    first = _trajectory(
        "a",
        entry_ns=1,
        exit_ns=5,
        marks=((2, -4_400.0, -4_400.0, -4_300.0), (5, -4_400.0, 100.0, 100.0)),
    )
    second = _trajectory(
        "b",
        entry_ns=2,
        exit_ns=5,
        marks=((3, -10.0, 0.0, 10.0), (5, -10.0, 100.0, 100.0)),
    )
    result = run_causal_shared_account_episode(
        {"a": (first,), "b": (second,)},
        (DAY,),
        policy=_policy("a", "b"),
        start_day=DAY,
        maximum_duration_days=1,
    )

    second_decision = next(
        row for row in result.risk_allocation_path if row["component_id"] == "b"
    )
    assert second_decision["allow"] is False
    assert second_decision["reason"] == "AGGREGATE_NOMINAL_RISK_LIMIT"
    assert second_decision["risk_before"]["maximum_admissible_declared_nominal_risk"] == pytest.approx(100.0)


def test_mll_breach_materializes_loss_and_component_attribution() -> None:
    trajectory = _trajectory(
        "a",
        entry_ns=1,
        exit_ns=5,
        marks=((2, -4_600.0, -4_600.0, -4_500.0), (5, -4_600.0, 100.0, 100.0)),
    )
    result = run_causal_shared_account_episode(
        {"a": (trajectory,)},
        (DAY,),
        policy=_policy("a"),
        start_day=DAY,
        maximum_duration_days=1,
    )

    assert result.terminal is CombineTerminal.MLL_BREACH
    assert result.net_pnl == pytest.approx(-4_600.0)
    assert result.minimum_mll_buffer == pytest.approx(-100.0)
    assert result.component_contribution == {"a": pytest.approx(-4_600.0)}
    assert result.daily_path[-1]["balance"] == pytest.approx(145_400.0)
    assert result.daily_path[-1]["open_positions"] == 0


def test_filled_then_missing_future_coverage_is_an_economic_censor() -> None:
    trajectory = _trajectory(
        "a",
        entry_ns=1,
        exit_ns=4,
        marks=((2, -10.0, 25.0, 30.0),),
        net_pnl=48.76,
        completed=False,
        censor_time_ns=3,
    )
    result = run_causal_shared_account_episode(
        {"a": (trajectory,)},
        (DAY,),
        policy=_policy("a"),
        start_day=DAY,
        maximum_duration_days=1,
    )

    assert result.terminal is CombineTerminal.TIMEOUT
    assert result.terminal_reason == "CENSORED_FUTURE_COVERAGE"
    assert result.net_pnl == pytest.approx(25.0)
    assert result.component_contribution == {"a": pytest.approx(25.0)}
    assert result.daily_path[-1]["unrealized_pnl"] == pytest.approx(25.0)
    assert result.daily_path[-1]["open_positions"] == 1


def test_censored_consistency_uses_realized_account_basis_and_matches_path() -> None:
    completed = _trajectory(
        "a",
        entry_ns=1,
        exit_ns=2,
        marks=((2, -10.0, 100.0, 100.0),),
        net_pnl=100.0,
    )
    censored = _trajectory(
        "b",
        entry_ns=2,
        exit_ns=4,
        marks=((3, -10.0, -10.0, 0.0),),
        net_pnl=25.0,
        completed=False,
        censor_time_ns=3,
    )
    result = run_causal_shared_account_episode(
        {"a": (completed,), "b": (censored,)},
        (DAY,),
        policy=_policy("a", "b"),
        start_day=DAY,
        maximum_duration_days=1,
    )

    assert result.terminal_reason == "CENSORED_FUTURE_COVERAGE"
    assert result.net_pnl == pytest.approx(90.0)
    assert result.daily_path[-1]["realized_pnl"] == pytest.approx(100.0)
    assert result.daily_path[-1]["unrealized_pnl"] == pytest.approx(-10.0)
    assert result.best_day_concentration == pytest.approx(1.0)
    assert result.consistency_ok is False
    assert result.daily_path[-1]["consistency_ok"] is False
