from __future__ import annotations

from dataclasses import replace

import pytest

from hydra.account_policy.basket import RoutedTrade
from hydra.account_policy.schema import BasketPolicy
from hydra.propfirm.combine_episode import TradePathEvent
from hydra.propfirm.combine_to_xfa import (
    CombineLifecycleStatus,
    FrozenRiskProfile,
    XfaTerminal,
)
from hydra.propfirm.portfolio_combine_to_xfa import (
    PortfolioBasketPolicy,
    PortfolioBookRole,
    PortfolioLifecycleError,
    freeze_portfolio_book,
    run_portfolio_combine_to_xfa_episode,
)


def _trade(
    component: str,
    market: str,
    day: int,
    pnl: float,
    *,
    offset: int = 0,
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
            worst_unrealized_pnl=-100.0,
            best_unrealized_pnl=max(pnl, 0.0),
            quantity=1,
            mini_equivalent=1.0,
            regime="FROZEN_PORTFOLIO_TEST",
        ),
    )


def _basket(policy_id: str, *components: str) -> BasketPolicy:
    return BasketPolicy(
        policy_id=policy_id,
        component_ids=tuple(components),
        archetype="PORTFOLIO_LIFECYCLE",
        component_priority=tuple(components),
        policy_version="hydra_account_policy_v7_2_portfolio_test",
    )


def _profile(profile_id: str, *, risk: float = 1.0) -> FrozenRiskProfile:
    return FrozenRiskProfile(
        profile_id=profile_id,
        risk_multiplier=risk,
        maximum_simultaneous_positions=2,
        maximum_mini_equivalent=15,
    )


def _passing_timelines() -> tuple[tuple[int, ...], dict[str, tuple[RoutedTrade, ...]]]:
    days = tuple(range(20260701, 20260711))
    timelines = {
        "velocity": (
            _trade("velocity", "ES", days[0], 4_500.0),
            _trade("velocity", "ES", days[1], 4_500.0),
        ),
        "defensive": tuple(
            _trade("defensive", "NQ", day, 1_000.0) for day in days[2:]
        ),
    }
    return days, timelines


def test_frozen_combine_book_transitions_to_distinct_xfa_subset() -> None:
    days, timelines = _passing_timelines()
    combine_book = freeze_portfolio_book(
        book_id="candidate:COMBINE_BOOK",
        role=PortfolioBookRole.COMBINE_BOOK,
        basket=_basket("candidate-combine", "velocity", "defensive"),
        risk_profile=_profile("candidate:COMBINE_PROFILE"),
        sleeve_timelines=timelines,
    )
    xfa_book = freeze_portfolio_book(
        book_id="candidate:XFA_BOOK",
        role=PortfolioBookRole.XFA_BOOK,
        basket=_basket("candidate-xfa", "defensive"),
        risk_profile=_profile("candidate:XFA_PROFILE"),
        sleeve_timelines=timelines,
    )

    result = run_portfolio_combine_to_xfa_episode(
        timelines,
        days,
        combine_book=combine_book,
        xfa_book=xfa_book,
        start_day=days[0],
        combine_horizon_days=2,
        xfa_horizon_days=8,
    )

    assert result.combine_status is CombineLifecycleStatus.TARGET_REACHED
    assert result.combine_episode.component_contribution == {"velocity": 9_000.0}
    assert result.xfa_started is True
    assert result.xfa_start_day == days[2]
    assert result.xfa_standard is not None
    assert result.xfa_consistency is not None
    assert result.xfa_standard.payout_cycles >= 1
    assert result.xfa_consistency.payout_cycles >= 1
    assert set(result.xfa_standard.component_contribution) == {"defensive"}
    assert set(result.xfa_consistency.component_contribution) == {"defensive"}
    assert "velocity" not in result.xfa_standard.component_contribution

    payload = result.to_dict()
    assert payload["book_membership_changed_at_transition"] is True
    assert payload["xfa_book_is_subset_of_combine_book"] is True
    assert payload["books_frozen_before_replay"] is True
    assert payload["xfa_book_selected_from_outcomes"] is False
    assert payload["payout_path_oracle_used"] is False
    assert payload["combine_profit_transferred_to_xfa"] is False
    assert payload["broker_connection_count"] == 0
    assert payload["order_count"] == 0
    assert payload["outbound_order_capability"] is False

    repeated = run_portfolio_combine_to_xfa_episode(
        timelines,
        days,
        combine_book=combine_book,
        xfa_book=xfa_book,
        start_day=days[0],
        combine_horizon_days=2,
        xfa_horizon_days=8,
    )
    assert repeated.union_timeline_hash == result.union_timeline_hash
    assert repeated.evidence_hash == result.evidence_hash


def test_failed_combine_does_not_activate_preregistered_xfa_book() -> None:
    days, source = _passing_timelines()
    timelines = {
        **source,
        "velocity": (
            _trade("velocity", "ES", days[0], 100.0),
            _trade("velocity", "ES", days[1], 100.0),
        ),
    }
    combine_book = freeze_portfolio_book(
        book_id="weak:COMBINE_BOOK",
        role="COMBINE_BOOK",
        basket=_basket("weak-combine", "velocity"),
        risk_profile=_profile("weak:COMBINE_PROFILE"),
        sleeve_timelines=timelines,
    )
    xfa_book = freeze_portfolio_book(
        book_id="weak:XFA_BOOK",
        role="XFA_BOOK",
        basket=_basket("weak-xfa", "defensive"),
        risk_profile=_profile("weak:XFA_PROFILE"),
        sleeve_timelines=timelines,
    )

    result = run_portfolio_combine_to_xfa_episode(
        timelines,
        days,
        combine_book=combine_book,
        xfa_book=xfa_book,
        start_day=days[0],
        combine_horizon_days=2,
        xfa_horizon_days=8,
    )

    assert result.combine_status is (
        CombineLifecycleStatus.OPERATIONAL_HORIZON_NOT_REACHED
    )
    assert result.xfa_started is False
    assert result.xfa_start_day is None
    assert result.xfa_standard is None
    assert result.xfa_consistency is None
    # The XFA book is still present in immutable evidence: it was declared
    # before, rather than selected after, the failed Combine path.
    assert result.to_dict()["xfa_book"]["fingerprint"] == xfa_book.fingerprint
    assert result.to_dict()["xfa_book_selected_from_outcomes"] is False


def test_each_book_freezes_its_own_per_sleeve_risk_allocation() -> None:
    days, timelines = _passing_timelines()
    combine_book = freeze_portfolio_book(
        book_id="risk:COMBINE_BOOK",
        role="COMBINE_BOOK",
        basket=_basket("risk-combine", "velocity"),
        risk_profile=_profile("risk:COMBINE_PROFILE"),
        sleeve_timelines=timelines,
        sleeve_risk_multipliers={"velocity": 2.0},
    )
    xfa_book = freeze_portfolio_book(
        book_id="risk:XFA_BOOK",
        role="XFA_BOOK",
        basket=_basket("risk-xfa", "defensive"),
        risk_profile=_profile("risk:XFA_PROFILE"),
        sleeve_timelines=timelines,
        sleeve_risk_multipliers={"defensive": 2.0},
    )
    result = run_portfolio_combine_to_xfa_episode(
        timelines,
        days,
        combine_book=combine_book,
        xfa_book=xfa_book,
        start_day=days[0],
        combine_horizon_days=2,
        xfa_horizon_days=8,
    )
    assert result.combine_episode.net_pnl == 18_000.0
    assert result.xfa_started is True
    assert dict(combine_book.sleeve_risk_multipliers) == {"velocity": 2.0}
    assert dict(xfa_book.sleeve_risk_multipliers) == {"defensive": 2.0}


def test_timeline_hash_drift_fails_before_account_replay() -> None:
    days, timelines = _passing_timelines()
    combine_book = freeze_portfolio_book(
        book_id="drift:COMBINE_BOOK",
        role="COMBINE_BOOK",
        basket=_basket("drift-combine", "velocity"),
        risk_profile=_profile("drift:COMBINE_PROFILE"),
        sleeve_timelines=timelines,
    )
    xfa_book = freeze_portfolio_book(
        book_id="drift:XFA_BOOK",
        role="XFA_BOOK",
        basket=_basket("drift-xfa", "defensive"),
        risk_profile=_profile("drift:XFA_PROFILE"),
        sleeve_timelines=timelines,
    )
    drifted = {
        **timelines,
        "defensive": (
            *timelines["defensive"],
            _trade("defensive", "NQ", 20260711, 1_000.0),
        ),
    }

    with pytest.raises(PortfolioLifecycleError, match="timeline drift"):
        run_portfolio_combine_to_xfa_episode(
            drifted,
            (*days, 20260711),
            combine_book=combine_book,
            xfa_book=xfa_book,
            start_day=days[0],
            combine_horizon_days=2,
            xfa_horizon_days=8,
        )


def test_books_require_immutable_timelines_and_distinct_roles() -> None:
    _days, timelines = _passing_timelines()
    mutable = {**timelines, "velocity": list(timelines["velocity"])}
    with pytest.raises(PortfolioLifecycleError, match="immutable tuple"):
        freeze_portfolio_book(
            book_id="mutable:COMBINE_BOOK",
            role="COMBINE_BOOK",
            basket=_basket("mutable-combine", "velocity"),
            risk_profile=_profile("mutable:COMBINE_PROFILE"),
            sleeve_timelines=mutable,
        )

    xfa = freeze_portfolio_book(
        book_id="candidate:XFA_BOOK",
        role="XFA_BOOK",
        basket=_basket("candidate-xfa", "defensive"),
        risk_profile=_profile("candidate:XFA_PROFILE"),
        sleeve_timelines=timelines,
    )
    with pytest.raises(PortfolioLifecycleError, match="static risk overlay"):
        replace(xfa, controller=object())


def test_priority_aware_portfolio_default_beats_lexical_event_id_order() -> None:
    day = 20260701
    timelines = {
        "z_priority": (_trade("z_priority", "ES", day, 1_000.0),),
        "a_low": (_trade("a_low", "NQ", day, 100.0),),
    }
    combine_book = freeze_portfolio_book(
        book_id="priority:COMBINE_BOOK",
        role="COMBINE_BOOK",
        basket=PortfolioBasketPolicy(
            policy_id="priority-combine",
            component_ids=("z_priority", "a_low"),
            archetype="PORTFOLIO_PRIORITY_TEST",
            maximum_simultaneous_positions=1,
            component_priority=("z_priority", "a_low"),
        ),
        risk_profile=_profile("priority:COMBINE_PROFILE"),
        sleeve_timelines=timelines,
    )
    xfa_book = freeze_portfolio_book(
        book_id="priority:XFA_BOOK",
        role="XFA_BOOK",
        basket=PortfolioBasketPolicy(
            policy_id="priority-xfa",
            component_ids=("z_priority",),
            archetype="PORTFOLIO_PRIORITY_TEST",
            maximum_simultaneous_positions=1,
            component_priority=("z_priority",),
        ),
        risk_profile=_profile("priority:XFA_PROFILE"),
        sleeve_timelines=timelines,
    )

    result = run_portfolio_combine_to_xfa_episode(
        timelines,
        (day,),
        combine_book=combine_book,
        xfa_book=xfa_book,
        start_day=day,
        combine_horizon_days=1,
        xfa_horizon_days=1,
    )

    assert result.combine_episode.component_contribution == {"z_priority": 1_000.0}


def test_cross_sleeve_event_id_collision_fails_before_freeze() -> None:
    day = 20260701
    first = _trade("first", "ES", day, 100.0)
    second = _trade("second", "NQ", day, 100.0)
    second = replace(second, event=replace(second.event, event_id=first.event.event_id))
    timelines = {"first": (first,), "second": (second,)}

    with pytest.raises(PortfolioLifecycleError, match="event_id collides"):
        freeze_portfolio_book(
            book_id="collision:COMBINE_BOOK",
            role="COMBINE_BOOK",
            basket=PortfolioBasketPolicy(
                policy_id="collision-combine",
                component_ids=("first", "second"),
                archetype="PORTFOLIO_COLLISION_TEST",
                maximum_simultaneous_positions=2,
                component_priority=("first", "second"),
            ),
            risk_profile=_profile("collision:COMBINE_PROFILE"),
            sleeve_timelines=timelines,
        )


def test_portfolio_pass_uses_separate_remaining_xfa_chronology() -> None:
    combine_days = (20260701, 20260702)
    xfa_days = (*combine_days, 20260703, 20260704)
    timelines = {
        "velocity": (
            _trade("velocity", "ES", combine_days[0], 4_500.0),
            _trade("velocity", "ES", combine_days[1], 4_500.0),
        ),
        "defensive": (
            _trade("defensive", "NQ", xfa_days[2], 200.0),
            _trade("defensive", "NQ", xfa_days[3], 200.0),
        ),
    }
    combine_book = freeze_portfolio_book(
        book_id="calendar:COMBINE_BOOK",
        role="COMBINE_BOOK",
        basket=_basket("calendar-combine", "velocity"),
        risk_profile=_profile("calendar:COMBINE_PROFILE"),
        sleeve_timelines=timelines,
    )
    xfa_book = freeze_portfolio_book(
        book_id="calendar:XFA_BOOK",
        role="XFA_BOOK",
        basket=_basket("calendar-xfa", "defensive"),
        risk_profile=_profile("calendar:XFA_PROFILE"),
        sleeve_timelines=timelines,
    )

    result = run_portfolio_combine_to_xfa_episode(
        timelines,
        combine_days,
        xfa_eligible_session_days=xfa_days,
        combine_book=combine_book,
        xfa_book=xfa_book,
        start_day=combine_days[0],
        combine_horizon_days=2,
        xfa_horizon_days=2,
    )

    assert result.xfa_started is True
    assert result.xfa_start_day == 20260703
    assert result.xfa_standard is not None
    assert result.xfa_standard.terminal is XfaTerminal.SURVIVED_HORIZON
