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
from hydra.mission.economic_evolution_cross_session_runtime import (
    CAMPAIGN_ID,
    EXPECTED_N_TRIALS,
    MULTIPLICITY_DELTA,
    PRIOR_N_TRIALS,
    EconomicEvolutionCrossSessionRuntime,
    cross_session_action_from_result,
    verify_cross_session_freeze,
)
from hydra.mission.economic_evolution_runtime import EconomicEvolutionRuntimeError


PROJECT_ROOT = Path(__file__).resolve().parents[1]
LIVE_STATE_ROOT = Path("/root/hydra-bot")
BASELINE_STATE_ROOT = (
    LIVE_STATE_ROOT
    / "mission/state/snapshots/"
    "economic_agreement_0008_predeploy_20260713T210612Z"
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
            "event_id": "agreement_0008_test_reservation",
            "event_type": MULTIPLICITY_EVENT,
            "recorded_at_utc": "2026-07-13T21:00:00+00:00",
            "status": "RESERVED",
            "scientific_role": "DEVELOPMENT_ONLY",
            "evidence": {
                "campaign_id": "hydra_economic_evolution_multi_horizon_agreement_0008"
            },
            "multiplicity": {
                "previous_N_trials": PRIOR_N_TRIALS - 2_400,
                "delta_trials": 2_400,
                "cumulative_N_trials": PRIOR_N_TRIALS,
            },
        },
    )
    return state


def _config() -> dict:
    return {
        "class_id": "CROSS_SESSION_ACCOUNT_COMPLEMENTARITY_SYNTHESIS_V1",
        "structural_population": {
            "policy_manifest_hash": (
                "190e175ce829a60321b81194edb5a876f"
                "b8f540c0f270a99b08c7096d89f96d8"
            ),
            "real_policy_count": 512,
            "matched_control_policy_count": 512,
        },
        "multiplicity": {
            "prospective_comparisons": 2_400,
            "campaign_specific_inflation": 1.5,
        },
    }


def _predecessor() -> dict:
    return {
        "action_type": "ECONOMIC_EVOLUTION_AGREEMENT_0008_TOMBSTONED",
        "phase": "4",
        "economic_agreement_terminal_state": "COMPLETE",
        "economic_agreement_terminal_verdict": "CLASS_TOMBSTONE_EXACT_GRAMMAR",
        "economic_agreement_parameter_rescue_allowed": False,
        "economic_agreement_same_class_relaunch_allowed": False,
        "economic_agreement_status_inheritance_allowed": False,
        "economic_agreement_graveyard_class_signature_count": 96,
        "economic_agreement_graveyard_indexed_object_count": 115_668,
        "next_experiment_id": CAMPAIGN_ID,
        "raw_global_N_trials": PRIOR_N_TRIALS,
        "economic_pre_holdout_ready_count": 0,
        "economic_paper_shadow_ready_count": 0,
    }


def test_cross_session_worm_and_implementation_are_frozen() -> None:
    config = verify_cross_session_freeze(PROJECT_ROOT)
    tagged = subprocess.check_output(
        [
            "git",
            "rev-parse",
            "worm/economic-evolution-cross-session-account-0009-2026-07-13^{commit}",
        ],
        cwd=PROJECT_ROOT,
        text=True,
    ).strip()
    assert tagged == "2a96c1bb2af8f6ffb89219315577054c32c2aacd"
    assert config["campaign_id"] == CAMPAIGN_ID
    assert config["structural_population"]["real_policy_count"] == 512
    assert config["structural_population"]["matched_control_policy_count"] == 512
    assert config["multiplicity"]["reserved_delta_trials"] == 3_600
    assert config["structural_population"]["same_class_0008_rescue"] is False


def test_runtime_reserves_once_before_account_outcomes(tmp_path: Path) -> None:
    state = _state(tmp_path)
    runtime = EconomicEvolutionCrossSessionRuntime(tmp_path, state)
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
    runtime = EconomicEvolutionCrossSessionRuntime(tmp_path, state)
    runtime.output_dir.mkdir(parents=True)
    (runtime.output_dir / "outcome.json").write_text("{}", encoding="utf-8")
    with pytest.raises(EconomicEvolutionRuntimeError, match="before multiplicity"):
        runtime._ensure_multiplicity_reservation(_config())


def test_runtime_requires_exact_0008_tombstone(tmp_path: Path) -> None:
    runtime = EconomicEvolutionCrossSessionRuntime(tmp_path, tmp_path / "state")
    runtime._verify_predecessor(_predecessor())
    wrong = _predecessor()
    wrong["economic_agreement_same_class_relaunch_allowed"] = True
    with pytest.raises(EconomicEvolutionRuntimeError, match="predecessor"):
        runtime._verify_predecessor(wrong)


def test_complete_action_never_promotes_or_authorizes_data() -> None:
    result = {
        "scientific_status": "DEVELOPMENT_COMBINE_PATH_CANDIDATES_FOUND",
        "population": {
            "component_count": 43,
            "real_policy_count": 512,
            "matched_control_policy_count": 512,
        },
        "policy_pair_evaluated_count": 512,
        "account_research_candidate_count": 8,
        "combine_path_diagnostic_count": 3,
        "family_tripwire": {
            "real_win_count": 120,
            "matched_control_win_count": 50,
            "NULL_RATIO": 50 / 120,
            "verdict": "GREEN_NULL_ADJUSTED_BASELINE",
            "evidence_strength": "VERT_NET",
        },
        "account_policy_economics": {
            "primary_rolling_combine_episode_count": 12_288,
            "policies_passing_at_least_one_combine_episode": 5,
            "combine_pass_probability": {"median": 0.0, "maximum": 0.125},
            "median_target_progress_distribution": {"median": 0.35},
            "maximum_target_progress": 1.0,
            "mll_breach_rate_distribution": {"median": 0.0, "maximum": 0.2},
            "stressed_positive_policy_count": 200,
            "behaviorally_distinct_policy_count": 180,
            "failure_vector_distribution": {"TARGET_VELOCITY_LOW": 300},
            "targeted_mutations_selected": [
                {
                    "failure_vector": "TARGET_VELOCITY_LOW",
                    "action": "ADD_DISTINCT_SESSION_TARGET_ACCELERATOR",
                }
            ],
        },
        "matched_control_economics": {"stressed_positive_policy_count": 120},
        "paired_account_economics": {
            "stressed_median_net_delta_usd": {"median": 200.0}
        },
        "wall_clock_accounting": {"research_percent": 96.0},
        "next_action": "SCALE_AND_FAILURE_DIRECT_MUTATE_ACCOUNT_SURVIVORS",
    }
    action = cross_session_action_from_result(_predecessor(), result)
    assert action["raw_global_N_trials"] == EXPECTED_N_TRIALS
    assert action["economic_cross_session_account_research_candidate_count"] == 8
    assert action["economic_cross_session_combine_path_diagnostic_count"] == 3
    assert action["economic_cross_session_rolling_combine_episode_count"] == 12_288
    assert action["economic_cross_session_best_combine_pass_probability"] == 0.125
    assert action["economic_independent_confirmation_queue_eligible_count"] == 0
    assert action["economic_pre_holdout_ready_count"] == 0
    assert action["economic_paper_shadow_ready_count"] == 0
    assert action["new_data_purchase_authorized"] is False
    assert action["protected_holdout_access_authorized"] is False
    assert action["shadow_admission_authorized"] is False
