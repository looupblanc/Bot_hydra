from __future__ import annotations

from dataclasses import replace

import pandas as pd

from hydra.account_policy.basket import RoutedTrade, run_shared_account_episode
from hydra.account_policy.schema import BasketPolicy
from hydra.propfirm.combine_episode import (
    CombineTerminal,
    TradePathEvent,
    run_combine_episode,
)
from hydra.propfirm.intraday_mll import conservative_intraday_mll_audit
from hydra.propfirm.mll_variants import MllMode, MllVariant
from hydra.propfirm.topstep_150k import Topstep150KConfig
from hydra.propfirm.xfa_episode import XfaTerminal, run_xfa_episode


def _event(
    day: int,
    *,
    net: float = 100.0,
    worst: float = -100.0,
    best: float = 6000.0,
) -> TradePathEvent:
    decision = day * 1_000_000_000_000
    return TradePathEvent(
        event_id=f"event-{day}",
        decision_ns=decision,
        exit_ns=decision + 60_000_000_000,
        session_day=day,
        net_pnl=net,
        gross_pnl=net + 10.0,
        worst_unrealized_pnl=worst,
        best_unrealized_pnl=best,
        quantity=1,
        mini_equivalent=1.0,
        regime="NORMAL",
    )


def _live_config(**changes: object) -> Topstep150KConfig:
    return replace(
        Topstep150KConfig(),
        mll_variant=MllVariant.INTRADAY_LIVE_EQUITY_HWM_CONSERVATIVE_MFE_FIRST,
        **changes,
    )


def test_combine_mll_variants_are_selectable_and_diverge_conservatively() -> None:
    event = _event(0)
    default = run_combine_episode([event], list(range(30)), start_day=0)
    live_hwm = run_combine_episode(
        [event], list(range(30)), start_day=0, config=_live_config()
    )

    assert default.terminal is CombineTerminal.TIMEOUT
    assert not default.mll_breached
    assert live_hwm.terminal is CombineTerminal.MLL_BREACH
    assert live_hwm.minimum_mll_buffer < 0.0


def test_public_mll_mode_flag_has_exact_contract_values_and_legacy_aliases() -> None:
    public_default = Topstep150KConfig(mll_mode="eod_level_rt_breach")
    public_live = Topstep150KConfig(mll_mode="intraday_hwm")
    legacy_live = Topstep150KConfig(
        mll_variant=(
            MllVariant.INTRADAY_LIVE_EQUITY_HWM_CONSERVATIVE_MFE_FIRST
        )
    )

    assert public_default.resolved_mll_mode is MllMode.EOD_LEVEL_RT_BREACH
    assert public_live.resolved_mll_mode is MllMode.INTRADAY_HWM
    assert legacy_live.resolved_mll_mode is MllMode.INTRADAY_HWM


def test_conflicting_public_and_legacy_mll_flags_fail_closed() -> None:
    import pytest

    with pytest.raises(ValueError, match="disagree"):
        Topstep150KConfig(
            mll_mode=MllMode.EOD_LEVEL_RT_BREACH,
            mll_variant=(
                MllVariant.INTRADAY_LIVE_EQUITY_HWM_CONSERVATIVE_MFE_FIRST
            ),
        )


def test_shared_account_uses_the_same_explicit_mll_variant() -> None:
    event = _event(0)
    routed = {"alpha": (RoutedTrade("alpha", "ES", 1, event),)}
    basket = BasketPolicy(
        policy_id="basket",
        component_ids=("alpha",),
        archetype="INDIVIDUAL_STRATEGY",
        maximum_simultaneous_positions=1,
        maximum_mini_equivalent=15,
        component_priority=("alpha",),
    )
    default = run_shared_account_episode(
        routed,
        list(range(30)),
        basket=basket,
        start_day=0,
        maximum_duration_days=30,
    )
    live_hwm = run_shared_account_episode(
        routed,
        list(range(30)),
        basket=basket,
        start_day=0,
        maximum_duration_days=30,
        config=_live_config(),
    )

    assert default.terminal is CombineTerminal.TIMEOUT
    assert live_hwm.terminal is CombineTerminal.MLL_BREACH


def test_xfa_uses_the_same_explicit_mll_variant() -> None:
    event = _event(0)
    default = run_xfa_episode([event], list(range(130)), start_day=0)
    live_hwm = run_xfa_episode(
        [event], list(range(130)), start_day=0, config=_live_config()
    )

    assert default.terminal is XfaTerminal.SURVIVED_WINDOW
    assert live_hwm.terminal is XfaTerminal.MLL_BREACH


def test_optional_dll_is_a_session_stop_not_an_account_failure() -> None:
    config = replace(
        Topstep150KConfig(),
        use_optional_daily_loss_limit=True,
        no_daily_loss_limit=False,
    )
    result = run_combine_episode(
        [_event(0, net=100.0, worst=-3200.0, best=50.0)],
        list(range(30)),
        start_day=0,
        maximum_duration_days=30,
        config=config,
    )

    assert result.terminal is CombineTerminal.TIMEOUT
    assert not result.mll_breached
    assert result.net_pnl == -3000.0
    assert result.daily_path[0]["dll_triggered"] is True


def test_legacy_intraday_audit_no_longer_trails_eod_variant_per_trade() -> None:
    frame = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                ["2024-01-02T14:30:00Z", "2024-01-02T14:31:00Z"]
            ),
            "symbol": ["ES", "ES"],
            "close": [5000.0, 5000.0],
            "high": [5120.0, 5001.0],
            "low": [4999.8, 4999.8],
        }
    )
    trade = {
        "entry_i": 0,
        "exit_i": 1,
        "side": 1,
        "symbol": "ES",
        "entry_price": 5000.0,
        "pnl": 100.0,
    }
    default = conservative_intraday_mll_audit(
        [trade], frame, 150000.0, 145500.0, 4500.0, 150000.0,
        forced_liquidation_slippage_bps=0.0,
    )
    live_hwm = conservative_intraday_mll_audit(
        [trade], frame, 150000.0, 145500.0, 4500.0, 150000.0,
        forced_liquidation_slippage_bps=0.0,
        mll_variant=MllVariant.INTRADAY_LIVE_EQUITY_HWM_CONSERVATIVE_MFE_FIRST,
    )

    assert not default.breached
    assert live_hwm.breached
