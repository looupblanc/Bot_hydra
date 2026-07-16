from __future__ import annotations

from dataclasses import replace

import pytest

from hydra.account_policy.basket import RoutedTrade
from hydra.account_policy.schema import BasketPolicy
from hydra.economic_evolution.schema import stable_hash
from hydra.propfirm.combine_episode import TradePathEvent
from hydra.propfirm.combine_to_xfa import FrozenRiskProfile, XfaTerminal
from hydra.propfirm.xfa_post_payout import (
    DllScenario,
    FrozenXfaTransition,
    FrontierRole,
    XfaPostPayoutPolicy,
    preregistered_post_payout_frontier,
    run_xfa_only_from_transition,
)
from hydra.propfirm.xfa_source_tape import (
    XFA_SOURCE_TAPE_SCHEMA,
    XfaSourceTape,
)


DAYS = (
    20260105,
    20260106,
    20260107,
    20260108,
    20260109,
    20260112,
    20260113,
    20260114,
    20260115,
    20260116,
)


def _trade(day: int, index: int, *, net: float = 400.0, worst: float = -40.0) -> RoutedTrade:
    event = TradePathEvent(
        event_id=f"event_{index}",
        decision_ns=index * 1_000_000_000,
        exit_ns=index * 1_000_000_000 + 500_000_000,
        session_day=day,
        net_pnl=net,
        gross_pnl=net,
        worst_unrealized_pnl=worst,
        best_unrealized_pnl=max(net, 0.0),
        quantity=4,
        mini_equivalent=4.0,
        regime="FROZEN",
    )
    return RoutedTrade(component_id="sleeve_a", market="MES", side=1, event=event)


def _tape(rows: tuple[RoutedTrade, ...]) -> XfaSourceTape:
    events = {"sleeve_a": rows}
    payload = {
        "schema": XFA_SOURCE_TAPE_SCHEMA,
        "campaign_id": "campaign_test",
        "events": {"sleeve_a": [row.to_dict() for row in rows]},
        "eligible_session_days": list(DAYS),
        "event_count": len(rows),
        "component_count": 1,
        "normal_net_pnl": sum(row.event.net_pnl for row in rows),
        "normal_gross_pnl": sum(row.event.gross_pnl for row in rows),
        "source_manifest_sha256": "source-sha",
        "feature_bundle_hashes": {"ES": "feature-sha"},
    }
    return XfaSourceTape(
        schema=XFA_SOURCE_TAPE_SCHEMA,
        campaign_id="campaign_test",
        events=events,
        eligible_session_days=DAYS,
        event_count=len(rows),
        component_count=1,
        normal_net_pnl=float(payload["normal_net_pnl"]),
        normal_gross_pnl=float(payload["normal_gross_pnl"]),
        source_manifest_sha256="source-sha",
        feature_bundle_hashes={"ES": "feature-sha"},
        tape_hash=stable_hash(payload),
    )


def _book() -> BasketPolicy:
    return BasketPolicy(
        policy_id="book_a",
        component_ids=("sleeve_a",),
        archetype="FROZEN_XFA_BOOK",
        maximum_simultaneous_positions=1,
        maximum_mini_equivalent=15,
    )


def _profile() -> FrozenRiskProfile:
    return FrozenRiskProfile(
        profile_id="xfa_profile",
        risk_multiplier=1.0,
        maximum_simultaneous_positions=1,
        maximum_mini_equivalent=15,
    )


def _transition(*, scenario: str = "NORMAL") -> FrozenXfaTransition:
    return FrozenXfaTransition(
        book_id="book_a",
        scenario=scenario,
        combine_start_id="combine_start_0001",
        combine_start_day=20251001,
        xfa_start_day=DAYS[0],
        combine_path_hash="sealed-combine-path-hash",
    )


def _policy(role: FrontierRole, *, risk: float = 0.25, dll: DllScenario = DllScenario.NO_DLL) -> XfaPostPayoutPolicy:
    return next(
        row
        for row in preregistered_post_payout_frontier("book_a")
        if row.role is role
        and row.post_payout_risk_scale == risk
        and row.dll_scenario is dll
    )


def test_preregistered_frontier_is_small_complete_and_deterministic() -> None:
    first = preregistered_post_payout_frontier("book_a")
    second = preregistered_post_payout_frontier("book_a")

    assert len(first) == 24
    assert {row.role for row in first} == set(FrontierRole)
    assert {row.post_payout_risk_scale for row in first} == {0.25, 0.5, 0.75, 1.0}
    assert {row.dll_scenario for row in first} == set(DllScenario)
    assert len({row.fingerprint for row in first}) == 24
    assert [row.fingerprint for row in first] == [row.fingerprint for row in second]


def test_harvest_reduces_risk_only_after_the_first_payout() -> None:
    tape = _tape(tuple(_trade(day, index) for index, day in enumerate(DAYS, 1)))
    result = run_xfa_only_from_transition(
        tape,
        basket=_book(),
        frozen_xfa_profile=_profile(),
        transition=_transition(),
        policy=_policy(FrontierRole.HARVEST),
        horizon_days=len(DAYS),
    )

    assert result.first_payout_day == 3
    assert result.payout_events[0]["gross_payout"] == pytest.approx(450.0)
    assert result.payout_events[0]["trader_net_payout"] == pytest.approx(405.0)
    assert result.payout_events[0]["combine_start_id"] == "combine_start_0001"
    assert result.payout_events[0]["xfa_path"] == "XFA_CONSISTENCY"
    assert result.payout_events[0]["reset_marker"] is True
    assert result.payout_events[0]["mll_before_payout"] == pytest.approx(-3_600.0)
    assert [row["session_risk_scale"] for row in result.daily_ledger[:3]] == [1.0] * 3
    assert result.daily_ledger[3]["session_risk_scale"] == pytest.approx(0.25)
    assert result.daily_ledger[3]["day_pnl"] == pytest.approx(100.0)


def test_balanced_and_longevity_enforce_their_retained_buffers() -> None:
    tape = _tape(tuple(_trade(day, index) for index, day in enumerate(DAYS, 1)))

    balanced = run_xfa_only_from_transition(
        tape,
        basket=_book(),
        frozen_xfa_profile=_profile(),
        transition=_transition(),
        policy=_policy(FrontierRole.BALANCED),
        horizon_days=len(DAYS),
    )
    longevity = run_xfa_only_from_transition(
        tape,
        basket=_book(),
        frozen_xfa_profile=_profile(),
        transition=_transition(),
        policy=_policy(FrontierRole.LONGEVITY),
        horizon_days=len(DAYS),
    )

    assert balanced.first_payout_day == 5
    assert balanced.payout_events[0]["gross_payout"] == pytest.approx(375.0)
    assert balanced.payout_events[0]["post_payout_balance"] == pytest.approx(1125.0)
    assert longevity.first_payout_day == 7
    assert longevity.payout_events[0]["gross_payout"] == pytest.approx(125.0)
    assert longevity.payout_events[0]["post_payout_balance"] == pytest.approx(2175.0)


def test_optional_dll_is_a_separate_scenario_not_an_account_failure() -> None:
    row = _trade(DAYS[0], 1, net=-3_500.0, worst=-3_500.0)
    row = replace(row, event=replace(row.event, quantity=3, mini_equivalent=3.0))
    rows = (row,)
    tape = _tape(rows)
    without_dll = run_xfa_only_from_transition(
        tape,
        basket=_book(),
        frozen_xfa_profile=_profile(),
        transition=_transition(),
        policy=_policy(FrontierRole.HARVEST, dll=DllScenario.NO_DLL),
        horizon_days=1,
    )
    with_dll = run_xfa_only_from_transition(
        tape,
        basket=_book(),
        frozen_xfa_profile=_profile(),
        transition=_transition(),
        policy=_policy(
            FrontierRole.HARVEST,
            dll=DllScenario.OPTIONAL_3000_SESSION_STOP,
        ),
        horizon_days=1,
    )

    assert without_dll.ending_balance == pytest.approx(-3_500.0)
    assert without_dll.dll_trigger_count == 0
    assert with_dll.ending_balance == pytest.approx(-3_000.0)
    assert with_dll.dll_trigger_count == 1
    assert with_dll.terminal is XfaTerminal.SURVIVED_HORIZON


def test_stress_costs_do_not_mutate_the_immutable_source_tape() -> None:
    row = _trade(DAYS[0], 1)
    row = replace(row, event=replace(row.event, gross_pnl=420.0, net_pnl=400.0))
    tape = _tape((row,))
    before = tape.tape_hash

    stressed = run_xfa_only_from_transition(
        tape,
        basket=_book(),
        frozen_xfa_profile=_profile(),
        transition=_transition(scenario="STRESSED"),
        policy=_policy(FrontierRole.HARVEST),
        horizon_days=1,
    )

    # The frozen XFA scaling tier admits three of four nominal contracts, so
    # stressed net is (400 - 10 extra cost) * 3/4.
    assert stressed.ending_balance == pytest.approx(292.5)
    assert tape.tape_hash == before
    assert tape.events["sleeve_a"][0].event.net_pnl == pytest.approx(400.0)
