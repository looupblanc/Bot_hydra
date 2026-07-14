from __future__ import annotations

import json
import random
from datetime import date
from pathlib import Path
from typing import Any, Mapping, Sequence

from hydra.economic_evolution.account_static_parent_basket import (
    StaticParentBasketPolicy,
)
from hydra.mission.economic_evolution_manifest_runtime import (
    EconomicEvolutionManifestRuntime,
    _load_and_verify_generic_account_pair_result,
)
from hydra.selection.selector_crossfit import (
    RecoveredCampaignLedger,
    TemporalBlock,
    TemporalBlockPlan,
)
from hydra.selection.selector_manifest import (
    HARD_REQUIREMENTS,
    PARETO_OBJECTIVES,
    SCHEMA as SELECTOR_MANIFEST_SCHEMA,
)
from hydra.selection.selector_reporting import (
    SELECTOR_PROCEDURE_GREEN,
    SelectorProcedureDecision,
    build_manifest_runtime_compatibility_projection,
    frozen_decision_manifest_policy,
)
from scripts import run_nested_selector_sprint as runner


def _static_policy(
    policy_id: str,
    parent_id: str,
    component_ids: Sequence[str],
) -> StaticParentBasketPolicy:
    components = tuple(component_ids)
    return StaticParentBasketPolicy(
        policy_id=policy_id,
        parent_policy_id=parent_id,
        parent_policy_fingerprint=f"fingerprint::{parent_id}",
        source_parent_ids=(parent_id, f"{parent_id}_peer"),
        component_ids=components,
        retained_added_sleeve_id=components[0],
        mutation_family="STATIC_PARENT_SYNTHESIS",
        failure_target="TEST_ONLY",
        exact_change=(("test", policy_id),),
        expected_effect="Synthetic deterministic test policy.",
        high_risk_units=3,
        daily_loss_guard=1_000.0,
        daily_profit_lock=2_000.0,
        critical_buffer=500.0,
        high_zone_buffer=4_000.0,
        high_zone_remaining_target=8_000.0,
        middle_zone_buffer=2_000.0,
        middle_zone_remaining_target=4_000.0,
        middle_risk_units=2,
        maximum_simultaneous_positions=3,
        maximum_mini_equivalent=15,
        assembly_profile="CONSENSUS_TEST_10",
    )


def test_selector_record_maps_every_preregistered_pareto_objective() -> None:
    source = {
        "variant_id": "policy::RISK_1_25X",
        "policy_id": "policy",
        "component_ids": [f"component_{index}" for index in range(10)],
        "risk_label": "RISK_1_25X",
        "micro_risk_units": 5,
        "design_block_ids": ["B1", "B2", "B3"],
        "heldout_block_id": "B4",
        "design_behavior_fingerprint": "f" * 64,
        "normal_net_usd": 1_200.0,
        "stressed_net_usd": 900.0,
        "mll_breach_rate": 0.02,
        "hard_issue_count": 0,
        "maximum_component_profit_share": 0.31,
        "stress_pass_count": 4,
        "normal_pass_count": 6,
        "stressed_target_progress_median": 0.77,
        "stressed_target_progress_p25": 0.42,
        "consistency_pass_rate": 0.83,
        "maximum_block_profit_share": 0.45,
        "operational_complexity": 10,
        "normal_median_net_usd": 100.0,
        "stressed_median_net_usd": 75.0,
        "positive_temporal_block_count": 3,
        "episode_count": 36,
    }

    record = runner._selector_record(source)

    expected = {
        "stressed_combine_pass_count": 4,
        "normal_combine_pass_count": 6,
        "stressed_median_target_progress": 0.77,
        "lower_quartile_target_progress": 0.42,
        "stressed_net_pnl": 900.0,
        "mll_breach_rate": 0.02,
        "consistency": 0.83,
        "component_concentration": 0.31,
        "temporal_block_concentration": 0.45,
        "operational_simplicity": -105.0,
    }
    assert {
        metric: record[metric] for metric, _ in PARETO_OBJECTIVES
    } == expected
    assert record["design_block_ids"] == ["B1", "B2", "B3"]
    assert record["heldout_block_id"] == "B4"


def test_equal_and_fixed_random_baselines_have_deterministic_membership_and_fingerprint() -> None:
    components = [f"component_{index:02d}" for index in range(16)]
    stats = {
        component_id: {
            "component_id": component_id,
            "event_count": 20 + index,
            "normal_net_usd": 1_000.0 - index,
            "stressed_net_usd": 2_000.0 - index,
        }
        for index, component_id in enumerate(components)
    }
    template = _static_policy("template", "parent", components[:10])

    eligible_first = runner._eligible_component_ids(stats)
    eligible_second = runner._eligible_component_ids(dict(reversed(list(stats.items()))))
    assert eligible_first == eligible_second
    equal_membership = tuple(eligible_first[:10])
    equal_a = runner._synthetic_baseline_policy(
        template, membership=equal_membership, label="FOLD_EQUAL_RISK"
    )
    equal_b = runner._synthetic_baseline_policy(
        template, membership=equal_membership, label="FOLD_EQUAL_RISK"
    )
    assert equal_a == equal_b
    assert equal_a.structural_fingerprint == equal_b.structural_fingerprint

    random_memberships: list[tuple[str, ...]] = []
    for seed in runner.RANDOM_BASELINE_SEEDS:
        first = tuple(random.Random(seed).sample(eligible_first, 10))
        second = tuple(random.Random(seed).sample(eligible_second, 10))
        assert first == second
        random_memberships.append(first)
        policy_a = runner._synthetic_baseline_policy(
            template, membership=first, label=f"FOLD_RANDOM_{seed}"
        )
        policy_b = runner._synthetic_baseline_policy(
            template, membership=second, label=f"FOLD_RANDOM_{seed}"
        )
        assert policy_a.policy_id == policy_b.policy_id
        assert policy_a.structural_fingerprint == policy_b.structural_fingerprint

    assert len(set(random_memberships)) == len(runner.RANDOM_BASELINE_SEEDS)
    assert any(membership != equal_membership for membership in random_memberships)


def test_decision_row_aliases_are_exact_and_optional_safety_fields_are_preserved() -> None:
    metrics = {
        "normal_net_usd": 321.5,
        "stressed_net_usd": 210.25,
        "stressed_target_progress": 0.625,
        "stressed_pass_count": 2,
        "episode_count": 12,
        "mll_breach_count": 1,
        "consistency": 0.875,
        "maximum_component_profit_share": 0.44,
    }

    assert runner._decision_policy_row(metrics) == metrics
    assert runner._decision_policy_row(
        {key: metrics[key] for key in tuple(metrics)[:5]}
    ) == {key: metrics[key] for key in tuple(metrics)[:5]}


def _epoch_day(value: str) -> int:
    return (date.fromisoformat(value) - date(1970, 1, 1)).days


def _temporal_block(index: int, start_date: str) -> TemporalBlock:
    day = _epoch_day(start_date)
    return TemporalBlock(
        block_id=f"V73_B{index}",
        ordinal=index,
        session_days=(day,),
        episode_start_days=(day,),
        embargo_before_days=(),
        embargo_after_days=(),
        start_date=start_date,
        end_date=start_date,
        trading_day_count=1,
        event_count=index,
        signal_markets=("MES",),
        execution_markets=("MES",),
        session_codes=(1,),
        volatility_regime_counts=(
            {"LOW": 1} if index % 2 else {"HIGH": 1}
        ),
        contracts_by_market={"MES": (f"MESH{index}",)},
        roll_transition_dates_by_market={"MES": ()},
        unsafe_roll_session_dates_by_market={"MES": ()},
        contamination_history=(
            {"type": "DEVELOPMENT_REUSE", "independent_confirmation": False},
        ),
    )


def _nested_selector_policy() -> dict[str, Any]:
    return {
        "outer_crossfit": {
            "method": "LEAVE_ONE_BLOCK_OUT",
            "held_out_each_block_exactly_once": True,
            "candidate_evidence_design_only": True,
            "primary_champions_per_fold": 1,
            "maximum_backups_per_fold": 1,
            "no_retuning_after_heldout": True,
            "headline_evidence_heldout_only": True,
        },
        "frozen_horizons": [20, 40, 60, 90, "full"],
        "risk_tiers": [0.75, 1.0, 1.25, 1.5],
        "pareto_objectives": [
            {"metric": metric, "direction": direction}
            for metric, direction in PARETO_OBJECTIVES
        ],
        "selector_execution_policy": runner._selector_execution_policy(),
        "random_selection_seeds": list(runner.RANDOM_BASELINE_SEEDS),
        "decision_thresholds": frozen_decision_manifest_policy(),
        "candidate_bank_policy": runner.frozen_compiler_policy(),
    }


def test_realized_manifest_attests_block_inference_and_dependent_starts() -> None:
    blocks = (
        _temporal_block(1, "2024-01-02"),
        _temporal_block(2, "2024-03-04"),
        _temporal_block(3, "2024-05-06"),
        _temporal_block(4, "2024-07-08"),
    )
    plan = TemporalBlockPlan(
        source_ledger_sha256="a" * 64,
        source_common_session_days_hash="b" * 64,
        contract_map_sha256="c" * 64,
        blocks=blocks,
        embargo_gaps=((), (), ()),
        trailing_unused_days=(),
        plan_hash="d" * 64,
    )
    ledger = RecoveredCampaignLedger(
        manifest_path=Path("/immutable/manifest.json"),
        ledger_path=Path("/immutable/events.jsonl"),
        manifest={"ledger_sha256": "a" * 64},
        runtimes={},
    )
    preregistration = {
        "campaign_id": "hydra_v73_nested_selector_test",
        "nested_selector_policy": _nested_selector_policy(),
    }

    manifest = runner._realized_selector_manifest(
        preregistration, ledger, plan
    )

    assert manifest["schema"] == SELECTOR_MANIFEST_SCHEMA
    assert manifest["block_is_inference_unit"] is True
    assert manifest["selector_frozen_before_heldout"] is True
    assert manifest["manifest_hash"]
    assert manifest["hard_requirements"] == {
        name: True for name in HARD_REQUIREMENTS
    }
    for block in manifest["temporal_blocks"]:
        assert block["inference_unit"] == "TEMPORAL_BLOCK"
        assert block["within_block_starts_independent"] is False
        assert block["overlapping_episode_starts_counted_as_independent"] is False
        assert block["episode_starts_unique_across_blocks"] is True
        assert (
            block["provenance"]["starts_are_dependent_descriptive_replays"]
            is True
        )


def _selector_decision() -> SelectorProcedureDecision:
    return SelectorProcedureDecision(
        status=SELECTOR_PROCEDURE_GREEN,
        metrics={"held_out": "synthetic"},
        checks={"green": {"synthetic": True}, "weak": {}},
        failure_reasons=(),
        thresholds={"minimum_outer_blocks": 4},
    )


def _held_out_fold(index: int) -> dict[str, Any]:
    selector = {
        "normal_net_usd": 100.0,
        "stressed_net_usd": 80.0,
        "stressed_target_progress": 0.80,
        "stressed_pass_count": 1 if index < 3 else 0,
        "episode_count": 12,
        "mll_breach_count": 0,
        "consistency": 0.90,
        "maximum_component_profit_share": 0.40,
    }
    parent = {
        "normal_net_usd": 40.0,
        "stressed_net_usd": 30.0,
        "stressed_target_progress": 0.60,
        "stressed_pass_count": 0,
        "episode_count": 12,
    }
    return {
        "decision_evidence": {"selector": selector, "best_parent": parent}
    }


def test_completed_projection_satisfies_generic_verifier_and_controller_contracts(
    tmp_path: Path,
) -> None:
    decision = _selector_decision()
    projection = build_manifest_runtime_compatibility_projection(
        decision,
        result_schema=runner.RESULT_SCHEMA,
        campaign_id="hydra_v73_nested_selector_test",
        class_id="NESTED_STATIC_BASKET_SELECTOR_PROCEDURE_V1",
        population_manifest_hash="a" * 64,
        compatibility_policy_pair_count=4,
        primary_rolling_combine_episode_count=96,
    )
    held_out = [_held_out_fold(index) for index in range(4)]
    result = runner._complete_runtime_projection(
        projection,
        decision=decision.to_dict(),
        held_out_folds=held_out,
        selector_manifest={"manifest_hash": "b" * 64},
        campaign_audit={"terminal": True},
        final_development={"status": "SYNTHETIC"},
        phase_seconds={"synthetic": 1.0},
        elapsed_seconds=1.0,
        service_state={"active": True},
        budget={"remaining_usd": 37.0},
        q4_status={"q4_access_during_sprint": 0},
        forward_status={"status": "WAITING"},
        next_action="PRESERVE",
    )
    config: dict[str, Any] = {
        "campaign_id": "hydra_v73_nested_selector_test",
        "class_id": "NESTED_STATIC_BASKET_SELECTOR_PROCEDURE_V1",
        "structural_population": {
            "policy_pair_count": 4,
            "policy_manifest_hash": "a" * 64,
        },
        "rolling_episode_policy": {"maximum_starts": 24},
        "runtime_manifest": {
            "engine": "manifest_account_pair_v1",
            "result_schema": runner.RESULT_SCHEMA,
        },
        "multiplicity": {"expected_global_N_trials_after_reservation": 123},
        "data": {"period": ["2023-07-01", "2024-10-01"]},
    }
    path = tmp_path / "selector_result.json"
    path.write_text(json.dumps(result, sort_keys=True), encoding="utf-8")

    verified = _load_and_verify_generic_account_pair_result(path, config)
    complete = EconomicEvolutionManifestRuntime._complete_action(
        None, {}, config, verified
    )
    terminal = EconomicEvolutionManifestRuntime._terminalize(
        None, complete, config, verified, tmp_path
    )

    economics = verified["account_policy_economics"]
    for key in (
        "primary_rolling_combine_episode_count",
        "policies_passing_at_least_one_combine_episode",
        "combine_pass_probability",
        "median_target_progress_distribution",
        "maximum_target_progress",
        "mll_breach_rate_distribution",
        "normal_positive_policy_count",
        "stressed_positive_policy_count",
        "failure_vector_distribution",
        "targeted_mutations_selected",
    ):
        assert key in economics
    assert verified["family_tripwire"]["family_green"] is True
    assert "wall_clock_accounting" in verified
    assert complete["manifest_campaign_state"] == "COMPLETE"
    assert terminal["manifest_campaign_terminal_state"] == (
        "SELECTOR_GREEN_DEVELOPMENT_FINALISTS_FROZEN"
    )
    assert terminal["manifest_campaign_parameter_neighbour_mutation_allowed"] is False
    assert terminal["next_experiment_id"] != (
        "FAILURE_GUIDED_SURVIVOR_MUTATION_MANIFEST"
    )
