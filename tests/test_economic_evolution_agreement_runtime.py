from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

from hydra.governance.proof_registry import (
    load_and_verify,
    multiplicity_trial_count,
)
from hydra.mission.economic_evolution_agreement_runtime import (
    CAMPAIGN_ID,
    EXPECTED_N_TRIALS,
    MULTIPLICITY_DELTA,
    PRIOR_N_TRIALS,
    EconomicEvolutionAgreementRuntime,
    agreement_action_from_result,
)
from hydra.mission.economic_evolution_runtime import EconomicEvolutionRuntimeError
from hydra.research.economic_evolution_agreement_campaign import (
    load_and_verify_agreement_preregistration,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
LIVE_STATE_ROOT = Path(
    "/root/hydra-bot/mission/state/snapshots/"
    "economic_agreement_0008_predeploy_20260713T210612Z"
)


def _state(tmp_path: Path) -> Path:
    state = tmp_path / "mission/state"
    state.mkdir(parents=True)
    shutil.copy2(
        LIVE_STATE_ROOT / "proof_registry.json",
        state / "proof_registry.json",
    )
    return state


def _config() -> dict:
    return {
        "class_id": "DIRECTIONAL_CONTEXT_AGREEMENT_TRADE_VETO_V1",
        "structural_population": {
            "candidate_manifest_hash": (
                "b769f66f8e02e87b93a3fbb46b5ca02fed2353ddfb0100dbfbb08b13e2e296ba"
            ),
            "real_sleeve_count": 44,
            "matched_null_sleeve_count": 44,
            "account_policy_count": 256,
        },
        "funnel": {
            "maximum_account_policy_evaluations": 256,
            "maximum_leave_one_out_controls_per_policy": 4,
        },
        "multiplicity": {
            "prospective_comparisons": 1_600,
            "campaign_specific_inflation": 1.5,
        },
    }


def _predecessor() -> dict:
    return {
        "action_type": "ECONOMIC_EVOLUTION_DENSITY_0007_TOMBSTONED",
        "phase": "4",
        "economic_density_terminal_state": "COMPLETE",
        "economic_density_terminal_verdict": "CLASS_TOMBSTONE_EXACT_GRAMMAR",
        "economic_density_parameter_rescue_allowed": False,
        "economic_density_same_class_relaunch_allowed": False,
        "economic_density_status_inheritance_allowed": False,
        "economic_density_graveyard_class_signature_count": 95,
        "economic_density_graveyard_indexed_object_count": 115_624,
        "next_experiment_id": CAMPAIGN_ID,
        "raw_global_N_trials": PRIOR_N_TRIALS,
        "economic_pre_holdout_ready_count": 0,
        "economic_paper_shadow_ready_count": 0,
    }


def test_agreement_worm_and_population_are_frozen() -> None:
    config = load_and_verify_agreement_preregistration(
        PROJECT_ROOT
        / "config/v7/economic_evolution_directional_agreement_0008_revision_02.json"
    )
    tagged = subprocess.check_output(
        [
            "git",
            "rev-parse",
            "worm/economic-evolution-directional-agreement-0008-revision-02-2026-07-13^{commit}",
        ],
        cwd=PROJECT_ROOT,
        text=True,
    ).strip()
    assert tagged == "a6a3a67132f1db8d8e8a9d2508dcb04ba2edec86"
    assert config["campaign_id"] == CAMPAIGN_ID
    assert config["structural_population"]["real_sleeve_count"] == 44
    assert config["structural_population"]["matched_null_sleeve_count"] == 44
    assert config["multiplicity"]["reserved_delta_trials"] == 2_400
    assert config["source_outcomes_from_0007_used"] is False


def test_agreement_runtime_reserves_once_before_artifacts(tmp_path: Path) -> None:
    state = _state(tmp_path)
    runtime = EconomicEvolutionAgreementRuntime(tmp_path, state)
    first = runtime._ensure_multiplicity_reservation(_config())
    second = runtime._ensure_multiplicity_reservation(_config())
    registry = load_and_verify(state / "proof_registry.json")
    assert first["entry_hash"] == second["entry_hash"]
    assert multiplicity_trial_count(registry) == EXPECTED_N_TRIALS
    assert first["multiplicity"]["delta_trials"] == MULTIPLICITY_DELTA
    assert first["evidence"]["feature_results_seen"] is False
    assert first["evidence"]["source_outcomes_from_0007_used"] is False
    assert first["evidence"]["outbound_orders"] == 0


def test_agreement_runtime_rejects_artifacts_before_reservation(
    tmp_path: Path,
) -> None:
    state = _state(tmp_path)
    runtime = EconomicEvolutionAgreementRuntime(tmp_path, state)
    runtime.output_dir.mkdir(parents=True)
    (runtime.output_dir / "outcome.json").write_text("{}", encoding="utf-8")
    with pytest.raises(EconomicEvolutionRuntimeError, match="before multiplicity"):
        runtime._ensure_multiplicity_reservation(_config())


def test_agreement_runtime_requires_exact_terminal_predecessor(
    tmp_path: Path,
) -> None:
    runtime = EconomicEvolutionAgreementRuntime(tmp_path, tmp_path / "state")
    runtime._verify_predecessor(_predecessor())
    wrong = _predecessor()
    wrong["economic_density_same_class_relaunch_allowed"] = True
    with pytest.raises(EconomicEvolutionRuntimeError, match="predecessor"):
        runtime._verify_predecessor(wrong)


def test_agreement_complete_action_never_promotes_or_authorizes_data() -> None:
    result = {
        "scientific_status": "DEVELOPMENT_ACCOUNT_RESEARCH_CANDIDATES_FOUND",
        "population": {
            "source_count": 22,
            "real_sleeve_count": 44,
            "matched_null_sleeve_count": 44,
            "account_policy_count": 256,
        },
        "account_policy_evaluated_count": 256,
        "account_research_candidate_count": 3,
        "combine_path_diagnostic_count": 1,
        "family_tripwire": {
            "real_pass_count": 12,
            "null_pass_count": 4,
            "NULL_RATIO": 1 / 3,
            "verdict": "GREEN_NULL_ADJUSTED_BASELINE",
            "evidence_strength": "VERT_MINCE",
            "real_exact_replay_missing_count": 0,
            "null_exact_replay_missing_count": 0,
        },
        "component_economics": {
            "real_positive_after_normal_cost_count": 18,
            "real_positive_after_stressed_cost_count": 12,
            "matched_null_component_gate_winner_count": 4,
        },
        "account_policy_economics": {
            "primary_rolling_combine_episode_count": 6_144,
            "policies_passing_at_least_one_combine_episode": 7,
            "combine_pass_probability": {"median": 0.0, "maximum": 0.125},
            "median_target_progress_distribution": {"median": 0.42},
            "maximum_target_progress": 1.0,
            "mll_breach_rate_distribution": {
                "median": 0.0,
                "maximum": 0.25,
            },
            "behaviorally_distinct_policy_count": 21,
            "failure_vector_distribution": {"TARGET_VELOCITY_LOW": 200},
            "targeted_mutations_selected": [
                {
                    "priority": 1,
                    "failure_vector": "TARGET_VELOCITY_LOW",
                    "action": "ADD_COMPLEMENTARY_SESSION_OR_MARKET_SLEEVE",
                }
            ],
        },
        "wall_clock_accounting": {"research_percent": 92.0},
        "next_action": "POWER_AUDIT_BEST_AGREEMENT_ACCOUNT_POLICIES",
    }
    action = agreement_action_from_result(_predecessor(), result)
    assert action["raw_global_N_trials"] == EXPECTED_N_TRIALS
    assert action["economic_agreement_account_research_candidate_count"] == 3
    assert action["economic_agreement_combine_path_diagnostic_count"] == 1
    assert action["economic_agreement_rolling_combine_episode_count"] == 6_144
    assert action["economic_agreement_best_combine_pass_probability"] == 0.125
    assert action["economic_independent_confirmation_queue_eligible_count"] == 0
    assert action["economic_pre_holdout_ready_count"] == 0
    assert action["economic_paper_shadow_ready_count"] == 0
    assert action["new_data_purchase_authorized"] is False
    assert action["protected_holdout_access_authorized"] is False
    assert action["shadow_admission_authorized"] is False
