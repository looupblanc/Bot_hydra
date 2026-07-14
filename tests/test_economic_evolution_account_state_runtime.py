from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

from hydra.governance.proof_registry import (
    MULTIPLICITY_EVENT,
    append_entry,
    load_and_verify,
    multiplicity_trial_count,
)
from hydra.mission.economic_evolution_account_state_runtime import (
    CAMPAIGN_ID,
    EXPECTED_N_TRIALS,
    MULTIPLICITY_DELTA,
    PRIOR_N_TRIALS,
    EconomicEvolutionAccountStateRuntime,
    account_state_action_from_result,
    verify_account_state_freeze,
)
from hydra.mission.economic_evolution_runtime import (
    EconomicEvolutionRuntimeError,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
BASELINE_STATE_ROOT = (
    PROJECT_ROOT
    / "mission/state/snapshots/"
    "economic_role_aware_0010_predeploy_20260713T234209Z"
)


def _state(tmp_path: Path) -> Path:
    state = tmp_path / "mission/state"
    state.mkdir(parents=True)
    shutil.copy2(
        BASELINE_STATE_ROOT / "proof_registry.json",
        state / "proof_registry.json",
    )
    append_entry(
        state / "proof_registry.json",
        {
            "event_id": "role_aware_0010_test_reservation",
            "event_type": MULTIPLICITY_EVENT,
            "recorded_at_utc": "2026-07-13T23:43:15+00:00",
            "status": "RESERVED",
            "scientific_role": "DEVELOPMENT_ONLY",
            "evidence": {
                "campaign_id": (
                    "hydra_economic_evolution_role_aware_account_allocator_0010"
                )
            },
            "multiplicity": {
                "previous_N_trials": PRIOR_N_TRIALS - 3_600,
                "delta_trials": 3_600,
                "cumulative_N_trials": PRIOR_N_TRIALS,
            },
        },
    )
    return state


def _config() -> dict:
    return {
        "class_id": "ACCOUNT_STATE_CONDITIONAL_ROLE_ROUTER_V1",
        "structural_population": {
            "policy_manifest_hash": (
                "da61d2638cb9d0f4e95c8dc7d6fffe73"
                "e8faed9a422f4add95767e9fc6369301"
            ),
            "policy_pair_count": 512,
        },
        "multiplicity": {
            "prospective_comparisons": 2_400,
            "campaign_specific_inflation": 1.5,
        },
    }


def _predecessor() -> dict:
    return {
        "action_type": "ECONOMIC_EVOLUTION_ROLE_AWARE_0010_TOMBSTONED",
        "phase": "4",
        "economic_role_aware_terminal_state": "COMPLETE",
        "economic_role_aware_terminal_verdict": "CLASS_TOMBSTONE_EXACT_GRAMMAR",
        "economic_role_aware_parameter_rescue_allowed": False,
        "economic_role_aware_same_class_relaunch_allowed": False,
        "economic_role_aware_status_inheritance_allowed": False,
        "economic_role_aware_graveyard_class_signature_count": 98,
        "economic_role_aware_graveyard_indexed_object_count": 116_692,
        "next_experiment_id": CAMPAIGN_ID,
        "raw_global_N_trials": PRIOR_N_TRIALS,
        "economic_pre_holdout_ready_count": 0,
        "economic_paper_shadow_ready_count": 0,
    }


def _result() -> dict:
    return {
        "scientific_status": "DEVELOPMENT_ACCOUNT_STATE_ENRICHMENT",
        "population": {
            "component_count": 48,
            "real_policy_count": 512,
            "matched_control_policy_count": 512,
        },
        "policy_pair_evaluated_count": 512,
        "account_research_candidate_count": 7,
        "combine_path_diagnostic_count": 2,
        "family_tripwire": {
            "real_win_count": 160,
            "matched_control_win_count": 64,
            "NULL_RATIO": 0.4,
            "verdict": "GREEN_NULL_ADJUSTED_BASELINE",
            "evidence_strength": "VERT_NET",
        },
        "account_policy_economics": {
            "primary_rolling_combine_episode_count": 12_288,
            "policies_passing_at_least_one_combine_episode": 4,
            "combine_pass_probability": {"median": 0.0, "maximum": 0.125},
            "median_target_progress_distribution": {"median": 0.2},
            "maximum_target_progress": 1.0,
            "mll_breach_rate_distribution": {
                "median": 0.0,
                "maximum": 0.125,
            },
            "stressed_consistency_pass_rate_distribution": {"median": 0.75},
            "normal_positive_policy_count": 400,
            "stressed_positive_policy_count": 300,
            "behaviorally_distinct_policy_count": 500,
            "failure_vector_distribution": {"TARGET_VELOCITY_LOW": 200},
            "targeted_mutations_selected": [
                {
                    "failure_vector": "TARGET_VELOCITY_LOW",
                    "action": "CHANGE_ACCOUNT_REPRESENTATION",
                }
            ],
        },
        "matched_control_economics": {"stressed_positive_policy_count": 200},
        "paired_account_economics": {
            "stressed_median_net_delta_usd": {"median": 150.0},
            "stressed_target_progress_delta": {"median": 0.02},
        },
        "wall_clock_accounting": {"research_percent": 97.0},
        "next_action": "SCALE_ACCOUNT_STATE_ROUTER_SURVIVORS",
    }


def test_account_state_worm_and_implementation_are_frozen() -> None:
    config = verify_account_state_freeze(PROJECT_ROOT)
    tagged = subprocess.check_output(
        [
            "git",
            "rev-parse",
            "worm/economic-evolution-account-state-router-0011-"
            "2026-07-14^{commit}",
        ],
        cwd=PROJECT_ROOT,
        text=True,
    ).strip()
    assert tagged == "f806f071be3fb39a967b80cba0f5027fc1b3221a"
    assert config["campaign_id"] == CAMPAIGN_ID
    assert config["structural_population"]["policy_pair_count"] == 512
    assert config["structural_population"]["past_only_state_inputs"] is True
    assert config["multiplicity"]["reserved_delta_trials"] == 3_600


def test_runtime_reserves_once_before_account_state_outcomes(
    tmp_path: Path,
) -> None:
    state = _state(tmp_path)
    runtime = EconomicEvolutionAccountStateRuntime(tmp_path, state)
    first = runtime._ensure_multiplicity_reservation(_config())
    second = runtime._ensure_multiplicity_reservation(_config())
    registry = load_and_verify(state / "proof_registry.json")
    assert first["entry_hash"] == second["entry_hash"]
    assert multiplicity_trial_count(registry) == EXPECTED_N_TRIALS
    assert first["multiplicity"]["delta_trials"] == MULTIPLICITY_DELTA
    assert first["evidence"]["feature_results_seen"] is False
    assert first["evidence"]["account_results_seen"] is False
    assert first["evidence"]["outbound_orders"] == 0


def test_runtime_rejects_artifacts_before_reservation(tmp_path: Path) -> None:
    state = _state(tmp_path)
    runtime = EconomicEvolutionAccountStateRuntime(tmp_path, state)
    runtime.output_dir.mkdir(parents=True)
    (runtime.output_dir / "outcome.json").write_text("{}", encoding="utf-8")
    with pytest.raises(EconomicEvolutionRuntimeError, match="before multiplicity"):
        runtime._ensure_multiplicity_reservation(_config())


def test_runtime_requires_exact_0010_tombstone(tmp_path: Path) -> None:
    runtime = EconomicEvolutionAccountStateRuntime(tmp_path, tmp_path / "state")
    runtime._verify_predecessor(_predecessor())
    wrong = _predecessor()
    wrong["economic_role_aware_same_class_relaunch_allowed"] = True
    with pytest.raises(EconomicEvolutionRuntimeError, match="predecessor"):
        runtime._verify_predecessor(wrong)


def test_complete_action_remains_development_only() -> None:
    action = account_state_action_from_result(_predecessor(), _result())
    assert action["raw_global_N_trials"] == EXPECTED_N_TRIALS
    assert action["economic_account_state_account_research_candidate_count"] == 7
    assert action["economic_account_state_combine_path_diagnostic_count"] == 2
    assert action["economic_account_state_rolling_combine_episode_count"] == 12_288
    assert action["economic_account_state_best_combine_pass_probability"] == 0.125
    assert action["economic_independent_confirmation_queue_eligible_count"] == 0
    assert action["economic_pre_holdout_ready_count"] == 0
    assert action["economic_paper_shadow_ready_count"] == 0
    assert action["new_data_purchase_authorized"] is False
    assert action["protected_holdout_access_authorized"] is False
    assert action["shadow_admission_authorized"] is False
