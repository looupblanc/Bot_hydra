from __future__ import annotations

from dataclasses import replace

import pytest

from hydra.account_policy.basket import RoutedTrade, run_shared_account_episode
from hydra.account_policy.schema import BasketPolicy
from hydra.production.mll_accounting import (
    CORRELATED_OPEN_POSITION_MLL_REASON,
    realize_correlated_open_position_mll_breach,
)
from hydra.propfirm.combine_episode import CombineTerminal, TradePathEvent


def _trade(
    component_id: str,
    market: str,
    day: int,
    *,
    decision_offset: int,
    net_pnl: float,
    worst_unrealized_pnl: float,
    duration: int = 10_000,
) -> RoutedTrade:
    decision_ns = day * 1_000_000 + decision_offset
    return RoutedTrade(
        component_id=component_id,
        market=market,
        side=1,
        event=TradePathEvent(
            event_id=f"{component_id}:{day}:{decision_offset}",
            decision_ns=decision_ns,
            exit_ns=decision_ns + duration,
            session_day=day,
            net_pnl=net_pnl,
            gross_pnl=net_pnl + 10.0,
            worst_unrealized_pnl=worst_unrealized_pnl,
            best_unrealized_pnl=max(net_pnl, 0.0) + 50.0,
            quantity=1,
            mini_equivalent=0.1,
            regime="NORMAL",
        ),
    )


def _basket(*component_ids: str) -> BasketPolicy:
    return BasketPolicy(
        policy_id="production-mll-test",
        component_ids=tuple(component_ids),
        archetype="PRODUCTION_MLL_ACCOUNTING_TEST",
        maximum_simultaneous_positions=len(component_ids),
        maximum_mini_equivalent=15,
        component_priority=tuple(component_ids),
        policy_version="hydra_account_policy_v7_2_production_replay_v1",
    )


def test_realizes_open_position_at_worst_not_eventual_profit() -> None:
    eventual_winner = _trade(
        "velocity",
        "MES",
        1,
        decision_offset=100,
        net_pnl=1_000.0,
        worst_unrealized_pnl=-5_000.0,
    )
    frozen_events = {"velocity": (eventual_winner,)}
    raw = run_shared_account_episode(
        frozen_events,
        (1,),
        basket=_basket("velocity"),
        start_day=1,
        maximum_duration_days=1,
    )

    assert raw.terminal_reason == CORRELATED_OPEN_POSITION_MLL_REASON
    assert raw.net_pnl == 0.0  # frozen simulator's pre-liquidation cash balance

    corrected = realize_correlated_open_position_mll_breach(raw, frozen_events)

    assert corrected.terminal is CombineTerminal.MLL_BREACH
    assert corrected.net_pnl == pytest.approx(-5_000.0)
    assert corrected.daily_path[-1]["balance"] == pytest.approx(145_000.0)
    assert corrected.daily_path[-1]["day_pnl"] == pytest.approx(-5_000.0)
    assert corrected.target_progress == pytest.approx(-5_000.0 / 9_000.0)
    assert corrected.consistency_ok is False
    assert corrected.component_contribution == {"velocity": -5_000.0}
    assert eventual_winner.event.net_pnl == 1_000.0

    # Absolute reconstruction, not delta application: a repeat is an identity
    # operation and cannot realize the adverse excursion twice.
    assert realize_correlated_open_position_mll_breach(
        corrected, frozen_events
    ) is corrected


def test_multi_component_loss_attribution_preserves_prior_realized_profit() -> None:
    prior = _trade(
        "defensive",
        "MGC",
        1,
        decision_offset=100,
        net_pnl=500.0,
        worst_unrealized_pnl=-50.0,
        duration=10,
    )
    left = _trade(
        "left",
        "MES",
        2,
        decision_offset=100,
        net_pnl=1_000.0,
        worst_unrealized_pnl=-2_500.0,
    )
    right = _trade(
        "right",
        "MCL",
        2,
        decision_offset=110,
        net_pnl=1_000.0,
        worst_unrealized_pnl=-2_500.0,
    )
    frozen_events = {
        "defensive": (prior,),
        "left": (left,),
        "right": (right,),
    }
    raw = run_shared_account_episode(
        frozen_events,
        (1, 2),
        basket=_basket("defensive", "left", "right"),
        start_day=1,
        maximum_duration_days=2,
    )

    corrected = realize_correlated_open_position_mll_breach(raw, frozen_events)

    assert corrected.net_pnl == pytest.approx(-4_500.0)
    assert corrected.daily_path[-1]["balance"] == pytest.approx(145_500.0)
    assert corrected.daily_path[-1]["day_pnl"] == pytest.approx(-5_000.0)
    assert corrected.component_contribution == {
        "defensive": 500.0,
        "left": -2_500.0,
        "right": -2_500.0,
    }
    assert sum(corrected.component_contribution.values()) == pytest.approx(
        corrected.net_pnl
    )
    assert corrected.shared_loss_days == 1


def test_non_correlated_terminals_are_returned_bit_identically() -> None:
    trade = _trade(
        "component",
        "MES",
        1,
        decision_offset=100,
        net_pnl=100.0,
        worst_unrealized_pnl=-50.0,
        duration=10,
    )
    frozen_events = {"component": (trade,)}
    timeout = run_shared_account_episode(
        frozen_events,
        (1,),
        basket=_basket("component"),
        start_day=1,
        maximum_duration_days=1,
    )
    realized_mll = replace(
        timeout,
        terminal=CombineTerminal.MLL_BREACH,
        terminal_reason="realized_mll_touch_or_breach",
        mll_breached=True,
    )

    assert realize_correlated_open_position_mll_breach(
        timeout, frozen_events
    ) is timeout
    assert realize_correlated_open_position_mll_breach(
        realized_mll, frozen_events
    ) is realized_mll
