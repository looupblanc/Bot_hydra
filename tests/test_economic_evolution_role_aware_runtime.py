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
from hydra.mission.economic_evolution_role_aware_runtime import (
    CAMPAIGN_ID,
    EXPECTED_N_TRIALS,
    MULTIPLICITY_DELTA,
    PRIOR_N_TRIALS,
    EconomicEvolutionRoleAwareRuntime,
    role_aware_action_from_result,
    verify_role_aware_freeze,
)
from hydra.mission.economic_evolution_runtime import (
    EconomicEvolutionRuntimeError,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
BASELINE_STATE_ROOT = (
    Path("/root/hydra-bot")
    / "mission/state/snapshots/"
    "economic_cross_session_0009_predeploy_20260713T220658Z"
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
            "event_id": "cross_session_0009_test_reservation",
            "event_type": MULTIPLICITY_EVENT,
            "recorded_at_utc": "2026-07-13T22:15:00+00:00",
            "status": "RESERVED",
            "scientific_role": "DEVELOPMENT_ONLY",
            "evidence": {
                "campaign_id": (
                    "hydra_economic_evolution_cross_session_account_"
                    "synthesis_0009"
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
        "class_id": "ROLE_AWARE_OPPORTUNITY_POOL_ALLOCATOR_V1",
        "structural_population": {
            "policy_manifest_hash": (
                "f43aa93d75392232cb69e1a768a3856f"
                "1102adc768f5e0d27cfa7ffad347f88a"
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
        "action_type": "ECONOMIC_EVOLUTION_CROSS_SESSION_0009_TOMBSTONED",
        "phase": "4",
        "economic_cross_session_terminal_state": "COMPLETE",
        "economic_cross_session_terminal_verdict": (
            "CLASS_TOMBSTONE_EXACT_GRAMMAR"
        ),
        "economic_cross_session_parameter_rescue_allowed": False,
        "economic_cross_session_same_class_relaunch_allowed": False,
        "economic_cross_session_status_inheritance_allowed": False,
        "economic_cross_session_graveyard_class_signature_count": 97,
        "economic_cross_session_graveyard_indexed_object_count": 116_180,
        "next_experiment_id": CAMPAIGN_ID,
        "raw_global_N_trials": PRIOR_N_TRIALS,
        "economic_pre_holdout_ready_count": 0,
        "economic_paper_shadow_ready_count": 0,
    }


def _result() -> dict:
    return {
        "scientific_status": "DEVELOPMENT_ROLE_AWARE_ENRICHMENT",
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
            "combine_pass_probability": {
                "median": 0.0,
                "maximum": 0.125,
            },
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
                    "action": "ADD_ROLE_DISTINCT_OPPORTUNITY",
                }
            ],
        },
        "matched_control_economics": {
            "stressed_positive_policy_count": 200
        },
        "paired_account_economics": {
            "stressed_median_net_delta_usd": {"median": 150.0},
            "stressed_target_progress_delta": {"median": 0.02},
        },
        "wall_clock_accounting": {"research_percent": 97.0},
        "next_action": "FREEZE_ROLE_AWARE_SURVIVORS_OR_TOMBSTONE_CLASS",
    }


def test_role_aware_worm_and_isolated_implementation_are_frozen() -> None:
    config = verify_role_aware_freeze(PROJECT_ROOT)
    tagged = subprocess.check_output(
        [
            "git",
            "rev-parse",
            "worm/economic-evolution-role-aware-account-0010-"
            "revision-01-2026-07-13^{commit}",
        ],
        cwd=PROJECT_ROOT,
        text=True,
    ).strip()
    assert tagged == "eccc96c9527556b144e3c7cea5ac4cd705d39b87"
    assert config["campaign_id"] == CAMPAIGN_ID
    assert config["structural_population"]["policy_pair_count"] == 512
    assert config["structural_population"]["sleeves_per_policy"] == [6, 7, 8]
    assert config["multiplicity"]["reserved_delta_trials"] == 3_600


def test_runtime_reserves_once_before_role_aware_outcomes(
    tmp_path: Path,
) -> None:
    state = _state(tmp_path)
    runtime = EconomicEvolutionRoleAwareRuntime(tmp_path, state)
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
    runtime = EconomicEvolutionRoleAwareRuntime(tmp_path, state)
    runtime.output_dir.mkdir(parents=True)
    (runtime.output_dir / "outcome.json").write_text("{}", encoding="utf-8")
    with pytest.raises(EconomicEvolutionRuntimeError, match="before multiplicity"):
        runtime._ensure_multiplicity_reservation(_config())


def test_runtime_requires_exact_0009_tombstone(tmp_path: Path) -> None:
    runtime = EconomicEvolutionRoleAwareRuntime(tmp_path, tmp_path / "state")
    runtime._verify_predecessor(_predecessor())
    wrong = _predecessor()
    wrong["economic_cross_session_same_class_relaunch_allowed"] = True
    with pytest.raises(EconomicEvolutionRuntimeError, match="predecessor"):
        runtime._verify_predecessor(wrong)


def test_complete_action_remains_development_only() -> None:
    action = role_aware_action_from_result(_predecessor(), _result())
    assert action["raw_global_N_trials"] == EXPECTED_N_TRIALS
    assert action["economic_role_aware_account_research_candidate_count"] == 7
    assert action["economic_role_aware_combine_path_diagnostic_count"] == 2
    assert action["economic_role_aware_rolling_combine_episode_count"] == 12_288
    assert action["economic_role_aware_best_combine_pass_probability"] == 0.125
    assert action["economic_independent_confirmation_queue_eligible_count"] == 0
    assert action["economic_pre_holdout_ready_count"] == 0
    assert action["economic_paper_shadow_ready_count"] == 0
    assert action["new_data_purchase_authorized"] is False
    assert action["protected_holdout_access_authorized"] is False
    assert action["shadow_admission_authorized"] is False

