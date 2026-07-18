from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest

from hydra.evidence import REQUIRED_DATASETS
from hydra.production.microstructure_hybrid_pilot import (
    PILOT_STATUSES,
    HybridPilotConfig,
    PairedActionOutcome,
    StructuralOpportunityEpisode,
    _evaluate_paired_actions_serial,
    _simulate_scenario,
    decide_hybrid_gate,
    evaluate_paired_actions,
    freeze_action_lattice,
    freeze_and_evaluate_policies,
    load_structural_opportunities,
    run_microstructure_hybrid_pilot,
)
from hydra.production.microstructure_sparse_pilot import SparseStore


def _episode(*, opportunity_id: str = "opp-0") -> StructuralOpportunityEpisode:
    return StructuralOpportunityEpisode(
        opportunity_id=opportunity_id,
        anchor_id="anchor-0",
        anchor_fingerprint="a" * 64,
        mechanism="OPENING_RANGE_EXPANSION",
        market="NQ",
        execution_market="MNQ",
        timeframe="1m",
        session_id="2024-07-08",
        role="DISCOVERY",
        direction=1,
        event_time_ns=900_000_000,
        available_at_ns=1_000_000_000,
        decision_time_ns=1_000_000_000,
        order_submit_time_ns=1_000_000_000,
        earliest_executable_time_ns=1_000_000_000,
        baseline_fill_time_ns=1_000_000_000,
        baseline_exit_time_ns=5_000_000_000,
        raw_fill_price=100.0,
        normal_fill_price=100.25,
        stressed_fill_price=100.375,
        raw_exit_price=101.0,
        stop_price=99.0,
        target_price=101.0,
        maximum_horizon="1",
        quantity=2,
        baseline_normal_net_pnl=100.0,
        baseline_stressed_net_pnl=90.0,
        baseline_normal_minimum_unrealized_pnl=-20.0,
        baseline_stressed_minimum_unrealized_pnl=-25.0,
        baseline_outcome="FAVORABLE_FIRST",
        source_reference_censored=False,
        source_fill_policy_id="CAUSAL_NEXT_TRADABLE_OPEN_V1",
        source_fill_policy_hash="b" * 64,
        feature_fingerprint="c" * 64,
        causal_fingerprint="d" * 64,
        source_event_hash="e" * 64,
    )


def _store(*, contradict: bool = False) -> SparseStore:
    names = (
        "flow_2s",
        "flow_30s",
        "bbo_imbalance",
        "microprice_deviation",
        "spread_ticks",
    )
    times = np.asarray(
        [900_000_000, 1_000_000_000, 1_030_000_000, 3_100_000_000, 5_000_000_000],
        dtype=np.int64,
    )
    flow = -2.0 if contradict else 2.0
    micro = -0.25 if contradict else 0.25
    features = np.asarray(
        [
            [0.5, 1.0, 0.1, 0.05, 1.0],
            [flow, 2.0, 0.5, micro, 1.0],
            [flow, 2.0, 0.5, micro, 1.0],
            [2.0, 2.0, 0.5, 0.25, 1.0],
            [2.0, 2.0, 0.5, 0.25, 1.0],
        ],
        dtype=float,
    )
    return SparseStore(
        feature_names=names,
        feature_values=features,
        feature_hashes=np.asarray([f"feature-{index}" for index in range(len(times))]),
        market=np.asarray(["NQ"] * len(times)),
        contract=np.asarray(["NQU4"] * len(times)),
        session=np.asarray(["2024-07-08"] * len(times)),
        decision_ns=times,
        available_ns=times,
        bid_price=np.asarray([99.75, 100.0, 100.0, 101.75, 101.5]),
        ask_price=np.asarray([100.0, 100.25, 100.25, 102.0, 101.75]),
        bid_size=np.asarray([1, 1, 1, 2, 2]),
        ask_size=np.asarray([1, 1, 1, 2, 2]),
        bid_depth_json=np.asarray(["[[99.75,10]]"] * len(times)),
        ask_depth_json=np.asarray(["[[100.25,10]]"] * len(times)),
        last_trade_price=np.asarray([100.0, 100.0, 100.0, 102.0, 101.75]),
        derived_available_ns={"NQ": np.asarray([1_040_000_000], dtype=np.int64)},
        derived_price={"NQ": np.asarray([100.0])},
        derived_size={"NQ": np.asarray([2], dtype=np.int64)},
        derived_side={"NQ": np.asarray(["A"])},
        roles={"2024-07-08": "DISCOVERY"},
        sessions=("2024-07-08",),
        source_hashes={"feature_matrices": "f" * 64},
    )


def _execution(*, net: float, quantity: int = 1, start: int = 1, end: int = 2) -> dict[str, object]:
    return {
        "fill_status": "AGGRESSIVE_BBO_MICRO" if quantity else "ABSTAINED",
        "quantity": quantity,
        "fill_time_ns": start if quantity else None,
        "exit_time_ns": end if quantity else None,
        "fill_price": 100.0 if quantity else None,
        "exit_price": 101.0 if quantity else None,
        "gross_pnl_usd": net + (1.24 if quantity else 0.0),
        "costs_usd": 1.24 if quantity else 0.0,
        "net_pnl_usd": net,
        "minimum_unrealized_pnl_usd": min(0.0, net),
        "exit_reason": "MAXIMUM_HORIZON" if quantity else "ABSTAIN",
        "quantity_ahead": 0,
        "observed_contra_volume": 0,
    }


def _paired(
    *,
    opportunity_id: str,
    role: str,
    quality: float,
    action_id: str,
    tier: float,
    net: float,
    baseline: float = -1.0,
    mechanism: str = "OPENING_RANGE_EXPANSION",
) -> PairedActionOutcome:
    quantity = 0 if action_id == "A1_ABSTAIN" else 1
    execution = _execution(net=net if quantity else 0.0, quantity=quantity)
    return PairedActionOutcome(
        paired_group_id=f"pair-{opportunity_id}",
        opportunity_id=opportunity_id,
        anchor_id=f"anchor-{opportunity_id}",
        mechanism=mechanism,
        market="NQ",
        execution_market="MNQ",
        quantity_unit="MICRO_CONTRACT",
        execution_book_quantity_unit="MINI_CONTRACT",
        micro_per_mini_ratio=10,
        session_id={
            "DISCOVERY": "2024-07-08",
            "VALIDATION": "2024-07-11",
            "FINAL_DEVELOPMENT": "2024-07-12",
        }[role],
        role=role,
        direction=1,
        action_id=action_id,
        risk_tier=tier,
        promotion_eligible=action_id != "A4_PASSIVE_JOIN",
        passive_side_lane=action_id == "A4_PASSIVE_JOIN",
        causal_quality_score=quality,
        joined_feature_hash="f" * 64,
        joined_decision_time_ns=0,
        feature_join_lag_ns=0,
        normal=execution,
        stressed=execution,
        baseline_normal_net_pnl=baseline,
        baseline_stressed_net_pnl=baseline,
        source_0028_normal_net_pnl=baseline,
        source_0028_stressed_net_pnl=baseline,
        source_0028_normal_fill_price=100.0,
        source_0028_stressed_fill_price=100.0,
        normal_delta_vs_a0_usd=float(execution["net_pnl_usd"]) - baseline,
        stressed_delta_vs_a0_usd=float(execution["net_pnl_usd"]) - baseline,
        normal_delta_vs_source_0028_usd=float(execution["net_pnl_usd"]) - baseline,
        stressed_delta_vs_source_0028_usd=float(execution["net_pnl_usd"]) - baseline,
        development_fill_model_id="CAUSAL_NEXT_TRADABLE_OPEN_V1",
        counterfactual_fill_model_id="0031_CAUSAL_BBO_MICRO_EXECUTION_V1",
        normal_fill_price_delta_vs_a0=0.0 if quantity else None,
        stressed_fill_price_delta_vs_a0=0.0 if quantity else None,
        normal_fill_price_delta_vs_source_0028=0.0 if quantity else None,
        stressed_fill_price_delta_vs_source_0028=0.0 if quantity else None,
        causal_fingerprint="c" * 64,
        outcome_hash=f"hash-{opportunity_id}-{action_id}-{tier}",
    )


def test_status_vocabulary_is_exact_overlay_contract() -> None:
    assert PILOT_STATUSES == (
        "HYBRID_OVERLAY_GREEN",
        "HYBRID_OVERLAY_WEAK",
        "HYBRID_OVERLAY_FALSIFIED",
    )


def test_a0_uses_same_counterfactual_execution_model_and_micro_mini_bridge() -> None:
    outcomes = evaluate_paired_actions((_episode(),), _store(), config=HybridPilotConfig())
    a0 = next(row for row in outcomes if row.action_id == "A0_BASELINE_IMMEDIATE")
    a2 = next(
        row
        for row in outcomes
        if row.action_id == "A2_WAIT_CONFIRM" and row.risk_tier == 1.0
    )
    assert a0.counterfactual_fill_model_id == a2.counterfactual_fill_model_id
    assert a0.baseline_stressed_net_pnl == a0.stressed["net_pnl_usd"]
    assert a0.stressed_delta_vs_a0_usd == pytest.approx(0.0)
    assert a0.source_0028_stressed_net_pnl == 90.0
    assert a0.quantity_unit == "MICRO_CONTRACT"
    assert a0.execution_book_quantity_unit == "MINI_CONTRACT"
    assert a0.micro_per_mini_ratio == 10


def test_two_worker_paired_action_evaluation_is_serially_identical() -> None:
    config = HybridPilotConfig(cpu_worker_count=2)
    episodes = (_episode(opportunity_id="opp-0"), _episode(opportunity_id="opp-1"))
    serial = _evaluate_paired_actions_serial(episodes, _store(), config=config)
    parallel = evaluate_paired_actions(episodes, _store(), config=config)
    assert [row.to_dict() for row in parallel] == [row.to_dict() for row in serial]


def test_a1_is_conditional_and_a5_contradiction_never_enters() -> None:
    config = HybridPilotConfig()
    a5 = next(
        action
        for action in freeze_action_lattice(config)
        if action.action_id == "A5_EARLY_INVALIDATION" and action.risk_tier == 1.0
    )
    execution = _simulate_scenario(
        _episode(), a5, _store(contradict=True), np.arange(5),
        slippage_ticks=config.stressed_adverse_slippage_ticks, config=config,
    )
    assert execution.quantity == 0
    assert execution.fill_time_ns is None
    assert execution.fill_status == "ACTION_NO_FILL"

    outcomes: list[PairedActionOutcome] = []
    opportunities = [
        ("d-low", "DISCOVERY", -2.0),
        ("d-mid", "DISCOVERY", 0.0),
        ("d-high", "DISCOVERY", 2.0),
        ("v-low", "VALIDATION", -3.0),
        ("f-high", "FINAL_DEVELOPMENT", 3.0),
    ]
    for opportunity_id, role, quality in opportunities:
        for action in freeze_action_lattice(config):
            outcomes.append(
                _paired(
                    opportunity_id=opportunity_id,
                    role=role,
                    quality=quality,
                    action_id=action.action_id,
                    tier=action.risk_tier,
                    net=2.0,
                )
            )
    candidates, policies = freeze_and_evaluate_policies(outcomes, config=config)
    assert len(candidates) == len(policies) == 20
    assert all(row["selection_uses_validation_or_final"] is False for row in candidates)
    assert any(
        selected["selected_action_id"] == "A1_ABSTAIN"
        for policy in policies
        for selected in policy["selected_actions"]
    )
    assert any(
        selected["selected_action_id"] != "A1_ABSTAIN"
        for policy in policies
        for selected in policy["selected_actions"]
    )


def test_source_loader_preserves_exact_72_opportunity_denominator(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    population = tmp_path / "population.jsonl"
    population.write_text(
        json.dumps({"candidate_id": "anchor", "candidate": {"market": "NQ"}}) + "\n"
    )
    result = tmp_path / "result.json"
    result.write_text(json.dumps({"economic_results": {"clean_useful_sleeve_ids": ["anchor"]}}))
    event_root = tmp_path / "events"
    event_root.mkdir()
    rows = [
        {"event_id": f"event-{index:02d}", "session_id": f"2024-07-{8 + index % 5:02d}"}
        for index in range(72)
    ]
    (event_root / "anchor.jsonl").write_text("".join(json.dumps(row) + "\n" for row in rows))

    def fake_episode(row: dict[str, object], *_args: object, **_kwargs: object) -> SimpleNamespace:
        index = int(str(row["event_id"]).split("-")[-1])
        return SimpleNamespace(
            opportunity_id=str(row["event_id"]),
            decision_time_ns=index,
            anchor_id="anchor",
        )

    monkeypatch.setattr(
        "hydra.production.microstructure_hybrid_pilot._episode_from_source", fake_episode
    )
    config = HybridPilotConfig(maximum_anchors=1, expected_active_anchors=1)
    episodes, provenance = load_structural_opportunities(
        population, event_root, result, config=config
    )
    assert len(episodes) == provenance["opportunity_count"] == 72
    assert len({row.opportunity_id for row in episodes}) == 72


def _gate_policy(policy_id: str, family: str) -> dict[str, object]:
    return {
        "policy_id": policy_id,
        "mechanism": "A2_WAIT_CONFIRM",
        "benefiting_heldout_anchor_mechanisms": [family],
        "validation_and_final_positive": True,
        "validation_and_final_improve_a0": True,
        "validation_and_final_non_inactive": True,
        "validation_and_final_mll_safe": True,
        "validation_and_final_concentration_safe": True,
        "heldout_denominators_nonzero": True,
        "deployable_l1_l2": True,
        "role_results": {
            "VALIDATION": {"opportunity_count": 18},
            "FINAL_DEVELOPMENT": {"opportunity_count": 24},
        },
    }


def test_gate_requires_mll_domination_activity_deployability_and_structural_families() -> None:
    config = HybridPilotConfig()
    policies = [_gate_policy("p0", "OPENING_RANGE"), _gate_policy("p1", "VWAP_DISPLACEMENT")]
    status, checks = decide_hybrid_gate(policies, config=config)
    assert status == "HYBRID_OVERLAY_GREEN"
    assert checks["distinct_defensible_mechanism_count"] >= 2

    for field in (
        "validation_and_final_non_inactive",
        "validation_and_final_mll_safe",
        "validation_and_final_concentration_safe",
        "deployable_l1_l2",
    ):
        broken = [dict(row) for row in policies]
        broken[0][field] = False
        broken[1][field] = False
        assert decide_hybrid_gate(broken, config=config)[0] != "HYBRID_OVERLAY_GREEN"


def test_run_projects_only_canonical_evidence_bundle_datasets(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    episode = _episode(opportunity_id="0")
    outcome = _paired(
        opportunity_id="0",
        role="DISCOVERY",
        quality=1.0,
        action_id="A0_BASELINE_IMMEDIATE",
        tier=1.0,
        net=2.0,
    )
    policy = {
        "policy_id": "policy-0",
        "policy_fingerprint": "9" * 64,
        "active_action_id": "A0_BASELINE_IMMEDIATE",
        "active_risk_tier": 1.0,
        "quality_threshold": 0.0,
        "selected_actions": [{"outcome_hash": outcome.outcome_hash}],
        "role_results": {
            "VALIDATION": {"stressed_delta_vs_a0_usd": 0.0},
            "FINAL_DEVELOPMENT": {"stressed_delta_vs_a0_usd": 0.0},
        },
    }
    provenance = {
        "population_sha256": "1" * 64,
        "clean_result_sha256": "2" * 64,
        "event_file_sha256": {"anchor-0": "4" * 64},
        "selected_anchor_ids": ["anchor-0"],
        "selected_anchor_count": 1,
        "opportunity_count": 1,
    }
    monkeypatch.setattr(
        "hydra.production.microstructure_hybrid_pilot.load_structural_opportunities",
        lambda *_args, **_kwargs: ((episode,), provenance),
    )
    monkeypatch.setattr(
        "hydra.production.microstructure_hybrid_pilot.load_sparse_source_store",
        lambda *_args, **_kwargs: SimpleNamespace(
            sessions=HybridPilotConfig().selected_sessions,
            source_hashes={"source": "3" * 64},
            decision_ns=np.asarray([episode.decision_time_ns], dtype=np.int64),
        ),
    )
    monkeypatch.setattr(
        "hydra.production.microstructure_hybrid_pilot.evaluate_paired_actions",
        lambda *_args, **_kwargs: (outcome,),
    )
    monkeypatch.setattr(
        "hydra.production.microstructure_hybrid_pilot.freeze_and_evaluate_policies",
        lambda *_args, **_kwargs: ([{"candidate_id": "candidate-0"}], [policy]),
    )
    monkeypatch.setattr(
        "hydra.production.microstructure_hybrid_pilot.decide_hybrid_gate",
        lambda *_args, **_kwargs: (
            "HYBRID_OVERLAY_FALSIFIED",
            {"defensible_policy_count": 0, "defensible_policy_ids": []},
        ),
    )
    result = run_microstructure_hybrid_pilot(
        tmp_path / "source",
        tmp_path / "population",
        tmp_path / "events",
        tmp_path / "result",
        tmp_path / "output",
        config=HybridPilotConfig(expected_active_anchors=1),
    )
    assert set(result["evidence_datasets"]) == set(REQUIRED_DATASETS)
