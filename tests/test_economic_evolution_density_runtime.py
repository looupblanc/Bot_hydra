from __future__ import annotations

import json
from pathlib import Path

import pytest

from hydra.governance.proof_registry import (
    GENESIS_HASH,
    MULTIPLICITY_EVENT,
    PROOF_WINDOW_EVENT,
    canonical_hash,
    load_and_verify,
    multiplicity_trial_count,
)
from hydra.mission.economic_evolution_density_runtime import (
    CAMPAIGN_ID,
    EXPECTED_N_TRIALS,
    MULTIPLICITY_DELTA,
    PRIOR_N_TRIALS,
    EconomicEvolutionDensityRuntime,
    density_action_from_result,
)
from hydra.mission.economic_evolution_runtime import EconomicEvolutionRuntimeError


def _entry(payload: dict[str, object], previous: str) -> dict[str, object]:
    value = {**payload, "previous_hash": previous}
    value["entry_hash"] = canonical_hash(value)
    return value


def _proof_registry(path: Path, *, trials: int = PRIOR_N_TRIALS) -> None:
    q4 = _entry(
        {
            "event_id": "q4_burned",
            "event_type": PROOF_WINDOW_EVENT,
            "recorded_at_utc": "2026-07-13T00:00:00+00:00",
            "status": "BURNED",
            "window": {
                "id": "Q4_2024",
                "start": "2024-10-01",
                "end_exclusive": "2025-01-01",
            },
            "evidence": {"q4_access_count": 1},
        },
        GENESIS_HASH,
    )
    counter = _entry(
        {
            "event_id": "prior_trials",
            "event_type": MULTIPLICITY_EVENT,
            "recorded_at_utc": "2026-07-13T00:00:01+00:00",
            "status": "RECORDED",
            "multiplicity": {
                "previous_N_trials": 0,
                "delta_trials": trials,
                "cumulative_N_trials": trials,
            },
        },
        str(q4["entry_hash"]),
    )
    path.parent.mkdir(parents=True)
    path.write_text(
        json.dumps(
            {
                "schema": "hydra_proof_registry_v1",
                "format": "append_only_hash_chain",
                "chain_algorithm": "sha256",
                "entry_count": 2,
                "chain_head": counter["entry_hash"],
                "entries": [q4, counter],
            }
        ),
        encoding="utf-8",
    )


def _config() -> dict:
    return {
        "class_id": "INDEPENDENT_OPPORTUNITY_DENSITY_CONSISTENCY_ASSEMBLY_V1",
        "structural_population": {
            "candidate_manifest_hash": "manifest",
            "real_sleeve_count": 22,
            "matched_null_sleeve_count": 22,
            "account_policy_count": 192,
        },
        "funnel": {
            "maximum_account_policy_evaluations": 160,
            "maximum_leave_one_out_controls_per_policy": 4,
        },
        "multiplicity": {
            "prospective_comparisons": 1250,
            "campaign_specific_inflation": 1.5,
        },
    }


def _predecessor() -> dict:
    return {
        "action_type": "ECONOMIC_EVOLUTION_FAILURE_REVIEW_0006_COMPLETE",
        "phase": "4",
        "economic_failure_review_candidate_status": (
            "FROZEN_DEVELOPMENT_UNDERPOWERED_NO_PROOF"
        ),
        "economic_failure_review_class_status": (
            "CLASS_REFORMULATION_ALLOWED_NEW_IDS_ONLY"
        ),
        "next_experiment_id": CAMPAIGN_ID,
        "economic_pre_holdout_ready_count": 0,
        "economic_paper_shadow_ready_count": 0,
    }


def test_density_runtime_reserves_multiplicity_once_before_artifacts(
    tmp_path: Path,
) -> None:
    state = tmp_path / "mission/state"
    proof = state / "proof_registry.json"
    _proof_registry(proof)
    runtime = EconomicEvolutionDensityRuntime(tmp_path, state)

    first = runtime._ensure_multiplicity_reservation(_config())
    second = runtime._ensure_multiplicity_reservation(_config())
    registry = load_and_verify(proof)

    assert first["entry_hash"] == second["entry_hash"]
    assert multiplicity_trial_count(registry) == EXPECTED_N_TRIALS
    assert len(registry["entries"]) == 3
    assert first["multiplicity"]["delta_trials"] == MULTIPLICITY_DELTA
    assert first["evidence"]["feature_results_seen"] is False
    assert first["evidence"]["outbound_orders"] == 0


def test_density_runtime_rejects_prior_trial_drift(tmp_path: Path) -> None:
    state = tmp_path / "mission/state"
    _proof_registry(state / "proof_registry.json", trials=PRIOR_N_TRIALS + 1)
    runtime = EconomicEvolutionDensityRuntime(tmp_path, state)

    with pytest.raises(EconomicEvolutionRuntimeError, match="predecessor drift"):
        runtime._ensure_multiplicity_reservation(_config())


def test_density_runtime_requires_exact_failure_review_predecessor(
    tmp_path: Path,
) -> None:
    runtime = EconomicEvolutionDensityRuntime(tmp_path, tmp_path / "state")
    runtime._verify_predecessor(_predecessor())
    wrong = _predecessor()
    wrong["economic_failure_review_class_status"] = "PARAMETER_RESCUE_ALLOWED"

    with pytest.raises(EconomicEvolutionRuntimeError, match="predecessor"):
        runtime._verify_predecessor(wrong)


def test_density_complete_action_never_promotes_or_authorizes_data() -> None:
    result = {
        "scientific_status": "DEVELOPMENT_ACCOUNT_RESEARCH_CANDIDATES_FOUND",
        "population": {
            "source_count": 22,
            "real_sleeve_count": 22,
            "matched_null_sleeve_count": 22,
            "account_policy_count": 192,
        },
        "account_policy_evaluated_count": 100,
        "account_research_candidate_count": 3,
        "combine_path_diagnostic_count": 1,
        "family_tripwire": {
            "real_pass_count": 8,
            "null_pass_count": 2,
            "NULL_RATIO": 0.25,
            "verdict": "GREEN_NULL_ADJUSTED_BASELINE",
            "evidence_strength": "VERT_MINCE",
        },
        "next_action": "POWER_AUDIT_BEST_DENSITY_ACCOUNT_POLICIES",
    }
    action = density_action_from_result(_predecessor(), result)

    assert action["raw_global_N_trials"] == EXPECTED_N_TRIALS
    assert action["economic_density_account_research_candidate_count"] == 3
    assert action["economic_density_combine_path_diagnostic_count"] == 1
    assert action["economic_independent_confirmation_queue_eligible_count"] == 0
    assert action["economic_pre_holdout_ready_count"] == 0
    assert action["economic_paper_shadow_ready_count"] == 0
    assert action["new_data_purchase_authorized"] is False
    assert action["protected_holdout_access_authorized"] is False
    assert action["shadow_admission_authorized"] is False


def test_density_snapshot_has_no_writer_or_order_path(tmp_path: Path) -> None:
    runtime = EconomicEvolutionDensityRuntime(tmp_path, tmp_path / "state")
    snapshot = runtime.snapshot()
    assert snapshot["mission_db_writer_count"] == 0
    assert snapshot["registry_writer_count"] == 0
    assert snapshot["broker_connections"] == 0
    assert snapshot["orders"] == 0
