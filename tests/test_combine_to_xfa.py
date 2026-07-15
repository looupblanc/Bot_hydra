from __future__ import annotations

from dataclasses import replace
from datetime import date

import pytest

from hydra.account_policy.basket import RoutedTrade
from hydra.account_policy.schema import BasketPolicy, ControllerPolicy
from hydra.propfirm.combine_episode import TradePathEvent
from hydra.propfirm.combine_to_xfa import (
    CombineLifecycleStatus,
    FrozenRiskProfile,
    RULE_SNAPSHOT_VERSION,
    RuleSnapshot,
    XfaTerminal,
    run_combine_to_xfa_episode,
)


def _trade(
    component: str,
    market: str,
    day: int,
    pnl: float,
    *,
    offset: int = 0,
    worst: float = -100.0,
    best: float | None = None,
    quantity: int = 1,
    mini: float = 1.0,
    session_ok: bool = True,
    contract_ok: bool = True,
) -> RoutedTrade:
    decision = day * 1_000_000 + offset
    return RoutedTrade(
        component_id=component,
        market=market,
        side=1,
        event=TradePathEvent(
            event_id=f"{component}:{day}:{offset}",
            decision_ns=decision,
            exit_ns=decision + 100,
            session_day=day,
            net_pnl=pnl,
            gross_pnl=pnl + 10.0,
            worst_unrealized_pnl=worst,
            best_unrealized_pnl=(max(pnl, 0.0) if best is None else best),
            quantity=quantity,
            mini_equivalent=mini,
            regime="FROZEN_TEST",
            session_compliant=session_ok,
            contract_limit_compliant=contract_ok,
        ),
    )


def _basket(*components: str) -> BasketPolicy:
    return BasketPolicy(
        policy_id="graduation-candidate",
        component_ids=tuple(components),
        archetype="CANDIDATE_GRADUATION",
        component_priority=tuple(components),
        policy_version="hydra_account_policy_v7_2_test",
    )


def _profile(name: str) -> FrozenRiskProfile:
    return FrozenRiskProfile(
        profile_id=name,
        risk_multiplier=1.0,
        maximum_simultaneous_positions=4,
        maximum_mini_equivalent=15,
    )


def test_official_rule_snapshot_is_frozen_no_dll_post_january_2026() -> None:
    rules = RuleSnapshot()

    assert rules.rule_version == RULE_SNAPSHOT_VERSION
    assert rules.verified_at_utc == "2026-07-15T00:00:00Z"
    assert rules.no_daily_loss_limit is True
    assert rules.post_2026_01_12_profit_split is True
    assert rules.combine_profit_target == 9_000.0
    assert rules.maximum_loss_limit == 4_500.0
    assert rules.combine_consistency_limit == 0.50
    assert rules.trader_profit_split == 0.90
    assert rules.standard_payout_cap == 5_000.0
    assert rules.consistency_payout_cap == 6_000.0
    assert rules.xfa_session_limit(0.0) == 3.0
    assert rules.xfa_session_limit(4_500.0) == 15.0
    assert rules.xfa_session_limit(4_500.0, "CL") == 9.0
    assert rules.xfa_session_limit(4_500.0, "MGC") == 9.0
    assert rules.fingerprint == RuleSnapshot().fingerprint

    with pytest.raises(ValueError, match="no-DLL"):
        replace(rules, no_daily_loss_limit=False)
    with pytest.raises(ValueError, match="frozen snapshot"):
        replace(rules, combine_profit_target=8_999.0)


def test_xfa_starts_only_after_combine_pass_with_zero_balance_and_two_paths() -> None:
    days = tuple(range(20260701, 20260711))
    events = {
        "alpha": (
            _trade("alpha", "ES", days[0], 4_500.0),
            _trade("alpha", "ES", days[1], 4_500.0),
            *tuple(_trade("alpha", "ES", day, 1_000.0) for day in days[2:]),
        )
    }

    result = run_combine_to_xfa_episode(
        events,
        days,
        basket=_basket("alpha"),
        combine_profile=_profile("combine"),
        xfa_profile=_profile("xfa"),
        start_day=days[0],
        combine_horizon_days=2,
        xfa_horizon_days=8,
    )

    assert result.combine_status is CombineLifecycleStatus.TARGET_REACHED
    assert result.combine_episode.net_pnl == 9_000.0
    assert result.xfa_started is True
    assert result.xfa_start_day == days[2]
    assert result.xfa_standard is not None
    assert result.xfa_consistency is not None
    assert result.xfa_standard.daily_ledger[0]["opening_balance"] == 0.0
    assert result.xfa_standard.daily_ledger[0]["mll_floor_open"] == -4_500.0
    assert result.xfa_consistency.daily_ledger[0]["opening_balance"] == 0.0
    assert result.xfa_standard.payout_cycles >= 1
    assert result.xfa_consistency.payout_cycles >= 1
    assert result.xfa_standard.first_payout_day == 5
    assert result.xfa_consistency.first_payout_day == 3
    assert result.xfa_standard.trader_net_payout == pytest.approx(2_250.0)
    assert result.xfa_consistency.trader_net_payout > 0.0
    assert result.xfa_standard.ending_mll_floor == 0.0
    assert result.xfa_consistency.ending_mll_floor == 0.0
    assert result.to_dict()["combine_profit_transferred_to_xfa"] is False
    assert result.to_dict()["payout_path_oracle_used"] is False
    assert "selected_path" not in result.to_dict()

    repeated = run_combine_to_xfa_episode(
        events,
        days,
        basket=_basket("alpha"),
        combine_profile=_profile("combine"),
        xfa_profile=_profile("xfa"),
        start_day=days[0],
        combine_horizon_days=2,
        xfa_horizon_days=8,
    )
    assert repeated.evidence_hash == result.evidence_hash
    assert repeated.xfa_standard.path_hash == result.xfa_standard.path_hash


def test_optional_frozen_controller_is_used_for_combine_only() -> None:
    days = (20260701, 20260702, 20260703)
    basket = _basket("alpha")
    controller = ControllerPolicy(
        controller_id="legacy-0018-controller",
        basket_policy_id=basket.policy_id,
        component_priority=("alpha",),
        daily_loss_limit=2_000.0,
        daily_profit_lock=5_000.0,
        loss_streak_derisk_after=3,
        low_buffer_threshold=1_500.0,
        critical_buffer_threshold=500.0,
        maximum_simultaneous_positions=1,
        maximum_mini_equivalent=15,
        policy_version="frozen-legacy-controller-v1",
    )
    events = {
        "alpha": (
            _trade("alpha", "ES", days[0], 4_500.0),
            _trade("alpha", "ES", days[1], 4_500.0),
            _trade("alpha", "ES", days[2], 200.0),
        )
    }

    result = run_combine_to_xfa_episode(
        events,
        days,
        basket=basket,
        combine_profile=_profile("combine"),
        xfa_profile=_profile("xfa"),
        controller=controller,
        start_day=days[0],
        combine_horizon_days=2,
        xfa_horizon_days=1,
    )

    assert result.combine_episode.policy_id == controller.controller_id
    assert result.combine_controller == controller
    assert result.to_dict()["xfa_routing_semantics"] == (
        "FROZEN_STATIC_ACCOUNT_OVERLAY"
    )


def test_failed_or_censored_combine_never_creates_xfa() -> None:
    days = (20260701, 20260702, 20260703)
    events = {
        "alpha": tuple(_trade("alpha", "ES", day, 100.0) for day in days)
    }

    result = run_combine_to_xfa_episode(
        events,
        days,
        basket=_basket("alpha"),
        combine_profile=_profile("combine"),
        xfa_profile=_profile("xfa"),
        start_day=days[1],
        combine_horizon_days=20,
        xfa_horizon_days=20,
    )

    assert result.combine_status is CombineLifecycleStatus.DATA_CENSORED
    assert result.xfa_started is False
    assert result.xfa_start_day is None
    assert result.xfa_standard is None
    assert result.xfa_consistency is None


def test_last_available_day_combine_pass_creates_explicit_censored_xfa_paths() -> None:
    days = (20260701, 20260702)
    events = {
        "alpha": (
            _trade("alpha", "ES", days[0], 4_500.0),
            _trade("alpha", "ES", days[1], 4_500.0),
        )
    }

    result = run_combine_to_xfa_episode(
        events,
        days,
        basket=_basket("alpha"),
        combine_profile=_profile("combine"),
        xfa_profile=_profile("xfa"),
        start_day=days[0],
        combine_horizon_days=2,
        xfa_horizon_days=120,
    )

    assert result.combine_status is CombineLifecycleStatus.TARGET_REACHED
    assert result.xfa_started is True
    assert result.xfa_start_day is None
    assert result.xfa_standard is not None
    assert result.xfa_standard.terminal is XfaTerminal.DATA_CENSORED
    assert result.xfa_standard.observed_days == 0
    assert result.xfa_consistency is not None
    assert result.xfa_consistency.terminal is XfaTerminal.DATA_CENSORED


def test_xfa_mll_checks_unrealized_path_in_real_time() -> None:
    days = (20260701, 20260702, 20260703, 20260704)
    events = {
        "alpha": (
            _trade("alpha", "ES", days[0], 4_500.0),
            _trade("alpha", "ES", days[1], 4_500.0),
            _trade("alpha", "ES", days[2], 500.0, worst=-4_500.0),
        )
    }

    result = run_combine_to_xfa_episode(
        events,
        days,
        basket=_basket("alpha"),
        combine_profile=_profile("combine"),
        xfa_profile=_profile("xfa"),
        start_day=days[0],
        combine_horizon_days=2,
        xfa_horizon_days=2,
    )

    assert result.xfa_standard is not None
    assert result.xfa_consistency is not None
    assert result.xfa_standard.terminal is XfaTerminal.MLL_BREACHED
    assert result.xfa_consistency.terminal is XfaTerminal.MLL_BREACHED
    assert result.xfa_standard.ending_balance == 0.0
    assert result.xfa_standard.minimum_mll_buffer == 0.0
    assert "unrealized" in result.xfa_standard.terminal_reason


def test_standard_later_payout_cycle_requires_new_positive_profit() -> None:
    days = tuple(range(20260701, 20260715))
    xfa_pnls = (
        1_000.0,
        1_000.0,
        1_000.0,
        1_000.0,
        1_000.0,
        -1_000.0,
        150.0,
        150.0,
        150.0,
        150.0,
        150.0,
        300.0,
    )
    events = {
        "alpha": (
            _trade("alpha", "ES", days[0], 4_500.0),
            _trade("alpha", "ES", days[1], 4_500.0),
            *tuple(
                _trade("alpha", "ES", day, pnl)
                for day, pnl in zip(days[2:], xfa_pnls, strict=True)
            ),
        )
    }

    result = run_combine_to_xfa_episode(
        events,
        days,
        basket=_basket("alpha"),
        combine_profile=_profile("combine"),
        xfa_profile=_profile("xfa"),
        start_day=days[0],
        combine_horizon_days=2,
        xfa_horizon_days=12,
    )

    assert result.xfa_standard is not None
    assert result.xfa_standard.payout_cycles == 2
    # Five new $150 winning days are not enough while the cycle remains down.
    assert result.xfa_standard.daily_ledger[-2]["winning_days_in_cycle"] == 5
    assert result.xfa_standard.daily_ledger[-2]["profit_since_payout"] < 0.01
    assert result.xfa_standard.daily_ledger[-2]["payout_requested"] is False
    assert result.xfa_standard.daily_ledger[-1]["payout_requested"] is True


def test_payout_on_last_observed_day_is_not_post_payout_survival() -> None:
    days = tuple(range(20260701, 20260708))
    events = {
        "alpha": (
            _trade("alpha", "ES", days[0], 4_500.0),
            _trade("alpha", "ES", days[1], 4_500.0),
            *tuple(_trade("alpha", "ES", day, 1_000.0) for day in days[2:]),
        )
    }

    result = run_combine_to_xfa_episode(
        events,
        days,
        basket=_basket("alpha"),
        combine_profile=_profile("combine"),
        xfa_profile=_profile("xfa"),
        start_day=days[0],
        combine_horizon_days=2,
        xfa_horizon_days=5,
    )

    assert result.xfa_standard is not None
    assert result.xfa_standard.payout_cycles == 1
    assert result.xfa_standard.post_payout_observed_days == 0
    assert result.xfa_standard.post_payout_survived is False
    assert result.xfa_standard.minimum_mll_buffer == pytest.approx(2_500.0)


def test_xfa_scaling_is_frozen_at_session_open_and_cl_is_capped_at_nine() -> None:
    days = (20260701, 20260702, 20260703, 20260704, 20260705)
    events = {
        "alpha": (
            _trade("alpha", "ES", days[0], 4_500.0),
            _trade("alpha", "ES", days[1], 4_500.0),
            _trade("alpha", "ES", days[2], 5_000.0, offset=0),
        ),
        "oil": (
            _trade(
                "oil",
                "CL",
                days[2],
                0.0,
                offset=1_000,
                worst=0.0,
                quantity=15,
                mini=15.0,
            ),
            _trade(
                "oil",
                "CL",
                days[3],
                0.0,
                worst=0.0,
                quantity=15,
                mini=15.0,
            ),
        ),
    }

    result = run_combine_to_xfa_episode(
        events,
        days,
        basket=_basket("alpha", "oil"),
        combine_profile=_profile("combine"),
        xfa_profile=_profile("xfa"),
        start_day=days[0],
        combine_horizon_days=2,
        xfa_horizon_days=3,
    )

    assert result.xfa_standard is not None
    first, second = result.xfa_standard.daily_ledger[:2]
    assert first["opening_balance"] == 0.0
    assert first["scaling_limit_mini_equivalent"] == 3.0
    assert first["restricted_market_limits"]["CL"] == 1.0
    assert first["maximum_mini_equivalent"] == 1.0
    assert first["closing_balance"] == 5_000.0
    assert second["opening_balance"] == 5_000.0
    assert second["scaling_limit_mini_equivalent"] == 15.0
    assert second["restricted_market_limits"]["CL"] == 9.0
    assert second["maximum_mini_equivalent"] == 9.0
    assert result.xfa_standard.maximum_mini_equivalent == 9.0


def test_current_cl_gc_nine_lot_cap_also_applies_to_combine() -> None:
    days = (20260701, 20260702, 20260703)
    events = {
        "oil": (
            _trade(
                "oil",
                "CL",
                days[0],
                7_500.0,
                quantity=15,
                mini=15.0,
            ),
            _trade(
                "oil",
                "CL",
                days[1],
                7_500.0,
                quantity=15,
                mini=15.0,
            ),
        )
    }

    result = run_combine_to_xfa_episode(
        events,
        days,
        basket=_basket("oil"),
        combine_profile=_profile("combine"),
        xfa_profile=_profile("xfa"),
        start_day=days[0],
        combine_horizon_days=2,
        xfa_horizon_days=1,
    )

    assert result.combine_status is CombineLifecycleStatus.TARGET_REACHED
    assert result.combine_episode.maximum_mini_equivalent == 9.0


def test_xfa_inactivity_is_distinct_from_data_censoring_and_mll_failure() -> None:
    days = (20260102, 20260103, 20260104, 20260205)
    events = {
        "alpha": (
            _trade("alpha", "ES", days[0], 4_500.0),
            _trade("alpha", "ES", days[1], 4_500.0),
        )
    }

    result = run_combine_to_xfa_episode(
        events,
        days,
        basket=_basket("alpha"),
        combine_profile=_profile("combine"),
        xfa_profile=_profile("xfa"),
        start_day=days[0],
        combine_horizon_days=2,
        xfa_horizon_days=3,
    )

    assert result.xfa_standard is not None
    assert result.xfa_standard.terminal is XfaTerminal.INACTIVITY_RISK
    assert result.xfa_standard.terminal is not XfaTerminal.DATA_CENSORED
    assert result.xfa_standard.terminal is not XfaTerminal.MLL_BREACHED
    assert "30_calendar_days" in result.xfa_standard.terminal_reason


def test_epoch_day_calendar_enforces_xfa_inactivity_rule() -> None:
    epoch = date(1970, 1, 1)
    values = tuple(
        (day - epoch).days
        for day in (
            date(2026, 1, 2),
            date(2026, 1, 3),
            date(2026, 1, 4),
            date(2026, 2, 5),
        )
    )
    events = {
        "alpha": (
            _trade("alpha", "ES", values[0], 4_500.0),
            _trade("alpha", "ES", values[1], 4_500.0),
        )
    }

    result = run_combine_to_xfa_episode(
        events,
        values,
        basket=_basket("alpha"),
        combine_profile=_profile("combine"),
        xfa_profile=_profile("xfa"),
        start_day=values[0],
        combine_horizon_days=2,
        xfa_horizon_days=3,
    )

    assert result.xfa_standard is not None
    assert result.xfa_standard.calendar_inactivity_auditable is True
    assert result.xfa_standard.terminal is XfaTerminal.INACTIVITY_RISK
