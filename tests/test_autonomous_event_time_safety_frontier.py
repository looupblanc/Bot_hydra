from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from hydra.economic_evolution.schema import stable_hash
from hydra.execution.v7_cost_model import CostStress
from hydra.execution.v7_cost_model import load_cost_model
from hydra.production import autonomous_event_time_safety_frontier as safety
from hydra.production.autonomous_exact_replay import (
    DEFAULT_RULE_SNAPSHOT,
    _account_config,
    _load_rule_snapshot,
)
from hydra.production.v71_event_time_account_exploration import (
    G4_CANDIDATE_ID,
    G6_CANDIDATE_IDS,
)
from hydra.propfirm.combine_episode import TradePathEvent, run_combine_episode


ROOT = Path(__file__).resolve().parents[1]


def _event(
    day: int,
    *,
    scenario: str = "BASE",
    net: float = 100.0,
    worst: float = -125.0,
    best: float = 150.0,
) -> TradePathEvent:
    decision = day * 86_400_000_000_000 + 1_000
    return TradePathEvent(
        event_id=f"candidate:{day}:{scenario}",
        decision_ns=decision,
        exit_ns=decision + 10_000,
        session_day=day,
        net_pnl=net,
        gross_pnl=net + 10.0,
        worst_unrealized_pnl=worst,
        best_unrealized_pnl=best,
        quantity=1,
        mini_equivalent=1.0,
        regime="TEST",
    )


def _config():
    rules, _receipt = _load_rule_snapshot(ROOT / DEFAULT_RULE_SNAPSHOT)
    return _account_config(rules["50K"])


def test_frontier_is_bounded_whole_micro_and_unique() -> None:
    profiles = safety.frozen_event_time_safety_profiles()
    assert len(profiles) == safety.MAXIMUM_PROFILES == 8
    assert len({row.profile_id for row in profiles}) == len(profiles)
    assert all(1 <= row.nominal_micro_contracts <= 10 for row in profiles)
    assert all(
        row.low_buffer_micro_contracts <= row.nominal_micro_contracts
        for row in profiles
    )


def test_identity_micro_conversion_reconciles_authoritative_episode() -> None:
    days = tuple(range(100, 110))
    source = tuple(_event(day, net=350.0) for day in days)
    transformed, decisions = safety._govern_episode_events(
        source,
        days,
        start_day=100,
        maximum_duration_days=5,
        profile=safety._identity_profile(),
        config=_config(),
        micro_round_turn_cost_usd=0.0,
    )
    original = run_combine_episode(
        source,
        days,
        start_day=100,
        maximum_duration_days=5,
        config=_config(),
        maximum_mini_equivalent=5.0,
    )
    converted = run_combine_episode(
        transformed,
        days,
        start_day=100,
        maximum_duration_days=5,
        config=_config(),
        maximum_mini_equivalent=5.0,
    )
    assert converted.to_dict() == original.to_dict()
    assert decisions["accepted_event_count"] == len(source[:5])
    assert decisions["micro_contract_distribution"] == {"10": 5}


def test_first_quantity_does_not_use_current_or_future_event_outcome() -> None:
    days = (100, 101)
    profile = safety.frozen_event_time_safety_profiles()[4]
    profitable = (_event(100, net=500.0, worst=-10.0, best=700.0), _event(101))
    losing = (_event(100, net=-500.0, worst=-900.0, best=5.0), _event(101))
    left, _ = safety._govern_episode_events(
        profitable,
        days,
        start_day=100,
        maximum_duration_days=2,
        profile=profile,
        config=_config(),
        micro_round_turn_cost_usd=load_cost_model().round_turn_cost(
            "MES", "60m", stress=CostStress.BASE
        ),
    )
    right, _ = safety._govern_episode_events(
        losing,
        days,
        start_day=100,
        maximum_duration_days=2,
        profile=profile,
        config=_config(),
        micro_round_turn_cost_usd=load_cost_model().round_turn_cost(
            "MES", "60m", stress=CostStress.BASE
        ),
    )
    assert left[0].mini_equivalent == right[0].mini_equivalent == 0.8
    assert left[0].quantity == right[0].quantity == 8


def test_micro_conversion_charges_exact_mes_cost_not_scaled_es_commission() -> None:
    source = _event(100, net=87.5, worst=-112.5, best=137.5)
    micro_cost = load_cost_model().round_turn_cost(
        "MES", "60m", stress=CostStress.BASE
    )
    converted = safety._scale_to_micro(
        source,
        10,
        "micro_cost_test",
        micro_round_turn_cost_usd=micro_cost,
    )
    assert converted.gross_pnl == pytest.approx(source.gross_pnl)
    assert converted.net_pnl == pytest.approx(
        source.gross_pnl - 10 * micro_cost
    )
    assert converted.net_pnl != pytest.approx(source.net_pnl)
    assert converted.quantity == 10
    assert converted.mini_equivalent == pytest.approx(1.0)


def test_block_contract_keeps_design_and_heldout_calendars_disjoint() -> None:
    contract = safety._chronological_block_contract(tuple(range(100, 143)))
    design = set(contract["days"]["DESIGN"])
    heldout = set(contract["days"]["HELD_OUT_DEVELOPMENT"])
    assert not design & heldout
    assert design | heldout == set(range(100, 143))
    assert set(contract["block_by_day"].values()) == {"B1", "B2", "B3", "B4"}
    assert contract["receipt"]["start_counts"]["DESIGN"] == {
        "5": 18,
        "10": 13,
        "20": 3,
    }
    assert contract["receipt"]["start_counts"]["HELD_OUT_DEVELOPMENT"] == {
        "5": 17,
        "10": 12,
        "20": 2,
    }


def _synthetic_population(days: tuple[int, ...]) -> dict[str, object]:
    output: dict[str, object] = {}
    for candidate_id in sorted((G4_CANDIDATE_ID, *G6_CANDIDATE_IDS)):
        normal = tuple(
            replace(
                _event(day, scenario="BASE", net=225.0, worst=-300.0, best=300.0),
                event_id=f"{candidate_id}:{day}:BASE",
            )
            for day in days
        )
        stressed = tuple(
            replace(
                row,
                event_id=row.event_id.rsplit(":", 1)[0] + ":STRESS_1_5X",
                net_pnl=row.net_pnl - 15.0,
            )
            for row in normal
        )
        output[candidate_id] = {
            CostStress.BASE.value: normal,
            CostStress.STRESS_1_5X.value: stressed,
        }
    output["_minute"] = object()
    output["_source_audit"] = {"synthetic": True}
    output["_integration"] = {"synthetic": True}
    output["_candidate_metadata"] = {
        candidate_id: {"candidate_id": candidate_id}
        for candidate_id in sorted((G4_CANDIDATE_ID, *G6_CANDIDATE_IDS))
    }
    return output


def test_two_shards_reconcile_and_do_not_promote(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    days = tuple(range(100, 143))
    monkeypatch.setattr(
        safety, "_load_frozen_event_population", lambda _root: _synthetic_population(days)
    )
    monkeypatch.setattr(
        safety,
        "_eligible_days_by_year",
        lambda _minute: {2023: days[:22], 2024: days[22:]},
    )
    shards = [
        safety.build_autonomous_event_time_safety_frontier(
            ROOT, shard_index=index, shard_count=2
        )
        for index in range(2)
    ]
    composite = safety.compose_autonomous_event_time_safety_frontier_shards(shards)
    assert composite["schema"] == safety.COMPOSITE_SCHEMA
    assert composite["counts"]["source_candidate_count"] == 3
    assert composite["counts"]["selected_candidate_count"] == 3
    assert composite["counts"]["authoritative_promotion_count"] == 0
    assert composite["counts"]["xfa_paths_started"] == 0
    assert composite["counts"]["orders"] == 0
    assert [row["candidate_id"] for row in composite["candidate_results"]] == sorted(
        (G4_CANDIDATE_ID, *G6_CANDIDATE_IDS)
    )


def test_compose_fails_closed_on_tampered_shard(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    days = tuple(range(100, 143))
    monkeypatch.setattr(
        safety, "_load_frozen_event_population", lambda _root: _synthetic_population(days)
    )
    monkeypatch.setattr(
        safety,
        "_eligible_days_by_year",
        lambda _minute: {2023: days[:22], 2024: days[22:]},
    )
    shard = safety.build_autonomous_event_time_safety_frontier(ROOT)
    tampered = dict(shard)
    tampered["counts"] = {**shard["counts"], "orders": 1}
    tampered["result_hash"] = stable_hash(
        {key: value for key, value in tampered.items() if key != "result_hash"}
    )
    with pytest.raises(
        safety.AutonomousEventTimeSafetyFrontierError,
        match="read-only event-time safety invariant failed",
    ):
        safety.compose_autonomous_event_time_safety_frontier_shards([tampered])
