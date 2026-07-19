from __future__ import annotations

from dataclasses import replace

import pytest

from hydra.propfirm.account_size_xfa import (
    AccountSizeXfaError,
    freeze_account_size_xfa_handoff,
    load_account_size_xfa_rules,
    run_account_size_xfa_alternatives,
)
from hydra.propfirm.combine_episode import TradePathEvent
from hydra.propfirm.combine_to_xfa import XfaTerminal
from hydra.research.causal_sleeve_replay import (
    CausalTradeMark,
    CausalTradeTrajectory,
)


def _trajectory(
    day: int,
    pnl: float,
    *,
    worst: float = -50.0,
    offset: int = 0,
    component: str = "candidate",
    market: str = "MYM",
) -> CausalTradeTrajectory:
    decision = day * 1_000_000 + offset
    exit_ns = decision + 100
    event = TradePathEvent(
        event_id=f"{component}:{day}:{offset}",
        decision_ns=decision,
        exit_ns=exit_ns,
        session_day=day,
        net_pnl=float(pnl),
        gross_pnl=float(pnl + 10.0),
        worst_unrealized_pnl=float(worst),
        best_unrealized_pnl=float(max(pnl, 0.0)),
        quantity=1,
        mini_equivalent=1.0,
        regime="FROZEN_XFA_TEST",
        session_compliant=True,
        contract_limit_compliant=True,
    )
    return CausalTradeTrajectory(
        component_id=component,
        market=market,
        side=1,
        event=event,
        marks=(
            CausalTradeMark(
                availability_time_ns=decision + 50,
                worst_unrealized_pnl=float(worst),
                best_unrealized_pnl=float(max(pnl, 0.0)),
                current_unrealized_pnl=float(min(pnl, 0.0)),
            ),
            CausalTradeMark(
                availability_time_ns=exit_ns,
                worst_unrealized_pnl=float(worst),
                best_unrealized_pnl=float(max(pnl, 0.0)),
                current_unrealized_pnl=float(pnl),
            ),
        ),
        initial_unrealized_pnl=0.0,
    )


def _run(
    pnls: list[float],
    *,
    worst: float = -50.0,
    horizon: int | None = None,
):
    rules = load_account_size_xfa_rules("50K")
    handoff = freeze_account_size_xfa_handoff(
        candidate_id="candidate",
        combine_book_hash="1" * 64,
        component_priority=("candidate",),
        rules=rules,
        maximum_mini_equivalent=5.0,
    )
    days = tuple(20_000 + index for index in range(len(pnls)))
    events = {
        "candidate": tuple(
            _trajectory(day, pnl, worst=worst)
            for day, pnl in zip(days, pnls, strict=True)
        )
    }
    result = run_account_size_xfa_alternatives(
        events,
        days,
        handoff=handoff,
        rules=rules,
        transition_id="transition-1",
        combine_path_hash="2" * 64,
        start_day=days[0],
        horizon_days=(len(days) if horizon is None else horizon),
    )
    return rules, handoff, result


def test_official_account_size_rules_cover_50k_100k_and_150k() -> None:
    rules_50 = load_account_size_xfa_rules("50K")
    rules_100 = load_account_size_xfa_rules("100K")
    rules_150 = load_account_size_xfa_rules("150K")

    assert (rules_50.maximum_loss_limit, rules_50.standard_payout_cap) == (
        2_000.0,
        2_000.0,
    )
    assert rules_50.consistency_payout_cap == 3_000.0
    assert rules_50.session_limit(0.0) == 2.0
    assert rules_50.session_limit(2_000.0) == 5.0
    assert rules_100.session_limit(0.0) == 3.0
    assert rules_150.session_limit(4_500.0) == 15.0
    assert len({rules_50.fingerprint, rules_100.fingerprint, rules_150.fingerprint}) == 3


def test_causal_unrealized_mll_touch_kills_both_alternative_paths() -> None:
    _rules, _handoff, result = _run([100.0], worst=-2_000.0)

    assert result.standard.terminal is XfaTerminal.MLL_BREACHED
    assert result.consistency.terminal is XfaTerminal.MLL_BREACHED
    assert result.standard.minimum_mll_buffer == pytest.approx(0.0)
    assert result.standard.payout_cycles == 0


def test_standard_winning_days_and_consistency_eligibility_are_separate() -> None:
    _rules, _handoff, result = _run([500.0] * 5)

    assert result.standard.first_payout_day == 5
    assert result.standard.payout_cycles == 1
    assert result.consistency.first_payout_day == 3
    assert result.consistency.payout_cycles == 1
    assert result.standard.path_hash != result.consistency.path_hash


def test_payout_minimum_cap_split_and_no_subminimum_execution() -> None:
    _rules, _handoff, capped = _run([1_000.0] * 5)
    standard_row = next(
        row for row in capped.standard.daily_ledger if row["payout_reset_marker"]
    )

    assert standard_row["pre_payout_balance"] == pytest.approx(5_000.0)
    assert standard_row["gross_payout"] == pytest.approx(2_000.0)
    assert standard_row["trader_net_payout"] == pytest.approx(1_800.0)
    assert capped.standard.trader_net_payout == pytest.approx(1_800.0)

    _rules, _handoff, subminimum = _run([30.0] * 3)
    assert subminimum.consistency.payout_cycles == 0
    assert subminimum.consistency.first_payout_day is None
    assert all(row["gross_payout"] == 0.0 for row in subminimum.consistency.daily_ledger)


def test_cycle_reset_post_payout_floor_and_first_payout_uniqueness() -> None:
    _rules, _handoff, result = _run([500.0] * 10)
    standard = result.standard
    payouts = [row for row in standard.daily_ledger if row["payout_reset_marker"]]

    assert standard.payout_cycles == 2
    assert standard.first_payout_count == 1
    assert standard.first_payout_day == 5
    assert [row["payout_cycle"] for row in payouts] == [1, 2]
    assert all(row["mll_after_payout"] == 0.0 for row in payouts)
    assert payouts[0]["winning_days_in_cycle"] == 0
    assert standard.ending_mll_floor == 0.0


def test_handoff_hash_drift_fails_closed_and_alternative_ev_is_never_summed() -> None:
    _rules, handoff, result = _run([500.0] * 5)
    payload = result.to_dict()

    assert payload["standard_and_consistency_are_alternatives"] is True
    assert payload["sum_standard_and_consistency_ev_allowed"] is False
    assert payload["selected_path"] is None
    assert "combined_ev" not in payload
    assert set(payload["alternatives"]) == {"STANDARD", "CONSISTENCY"}

    with pytest.raises(AccountSizeXfaError, match="hash drift"):
        replace(handoff, handoff_hash="0" * 64)
