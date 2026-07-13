from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from hydra.economic_evolution.schema import stable_hash
from hydra.governance.proof_registry import (
    GENESIS_HASH,
    MULTIPLICITY_EVENT,
    PROOF_WINDOW_EVENT,
    canonical_hash,
)
from hydra.mission.economic_evolution_failure_runtime import (
    EXPECTED_N_TRIALS,
    REVIEW_ID,
    EconomicEvolutionFailureReviewRuntime,
    _worker_environment,
    failure_review_action_from_result,
    load_and_verify_failure_review_result,
)
from hydra.mission.economic_evolution_runtime import EconomicEvolutionRuntimeError
from hydra.research.economic_evolution_failure_review import REVIEW_SCHEMA


def _entry(payload: dict[str, object], previous: str) -> dict[str, object]:
    value = {**payload, "previous_hash": previous}
    value["entry_hash"] = canonical_hash(value)
    return value


def _proof_registry(path: Path, *, trials: int = EXPECTED_N_TRIALS) -> None:
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
            "event_id": "historical_trials",
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


def _config() -> dict[str, object]:
    return {
        "candidate": {
            "policy_id": "policy_frozen",
            "policy_specification_hash": "policy_hash",
        },
        "next_research_class": {"next_experiment_id": "campaign_0007"},
    }


def _predecessor() -> dict[str, object]:
    return {
        "action_type": "ECONOMIC_EVOLUTION_EXPENSIVE_VALIDATION_0005_COMPLETE",
        "phase": "4",
        "economic_expensive_validation_scientific_status": (
            "EXPENSIVE_VALIDATION_UNDERPOWERED"
        ),
        "economic_expensive_validation_candidate_id": "policy_frozen",
        "economic_independent_confirmation_queue_eligible_count": 0,
    }


def _result() -> dict[str, object]:
    value: dict[str, object] = {
        "schema": REVIEW_SCHEMA,
        "review_id": REVIEW_ID,
        "retrospective_only": True,
        "new_statistical_comparisons_executed": 0,
        "multiplicity_delta": 0,
        "candidate_id": "policy_frozen",
        "candidate_specification_hash": "policy_hash",
        "candidate_exact_status": "FROZEN_DEVELOPMENT_UNDERPOWERED_NO_PROOF",
        "candidate_validated": False,
        "class_status": "CLASS_REFORMULATION_ALLOWED_NEW_IDS_ONLY",
        "dominant_failure": "INSUFFICIENT_STATISTICAL_POWER",
        "ranked_failure_dimensions": [
            "INSUFFICIENT_STATISTICAL_POWER",
            "INSUFFICIENT_TARGET_VELOCITY",
        ],
        "failure_scores": {"INSUFFICIENT_STATISTICAL_POWER": 1.0},
        "observed_evidence": {
            "stress_2x_net_usd": 3000.0,
            "positive_blocks": 3,
            "block_count": 4,
            "consistency_pass_rate_1_5x": 0.25,
            "validator_power": 0.0,
        },
        "decision": {
            "consume_independent_proof": False,
            "reuse_q4": False,
            "purchase_new_data": False,
            "admit_shadow": False,
            "mutate_exact_policy": False,
            "replay_exact_policy_unchanged": False,
            "remove_tombstones": False,
            "inherit_status": False,
            "class_level_reformulation": True,
            "new_ids_required": True,
        },
        "next_experiment_id": "campaign_0007",
        "next_experiment_state": "WORM_PREREGISTRATION_REQUIRED_BEFORE_OUTCOMES",
        "pre_holdout_ready_count": 0,
        "paper_shadow_ready_count": 0,
        "proof_window_consumed": False,
        "q4_access_delta": 0,
        "new_data_purchase_count": 0,
        "broker_connections": 0,
        "orders": 0,
        "outbound_order_capability": False,
    }
    value["result_sha256"] = stable_hash(value)
    return value


def test_failure_review_verifies_proof_without_writing_it(tmp_path: Path) -> None:
    proof = tmp_path / "mission/state/proof_registry.json"
    _proof_registry(proof)
    runtime = EconomicEvolutionFailureReviewRuntime(tmp_path, proof.parent)
    before = hashlib.sha256(proof.read_bytes()).hexdigest()

    runtime._verify_static_protections()

    assert hashlib.sha256(proof.read_bytes()).hexdigest() == before


def test_failure_review_rejects_multiplicity_drift(tmp_path: Path) -> None:
    proof = tmp_path / "mission/state/proof_registry.json"
    _proof_registry(proof, trials=EXPECTED_N_TRIALS + 1)
    runtime = EconomicEvolutionFailureReviewRuntime(tmp_path, proof.parent)

    with pytest.raises(EconomicEvolutionRuntimeError, match="multiplicity"):
        runtime._verify_static_protections()


def test_failure_review_requires_exact_underpowered_predecessor(tmp_path: Path) -> None:
    runtime = EconomicEvolutionFailureReviewRuntime(tmp_path, tmp_path / "state")
    runtime._verify_predecessor(_predecessor(), _config())
    wrong = _predecessor()
    wrong["economic_expensive_validation_scientific_status"] = "SUPPORTED"

    with pytest.raises(EconomicEvolutionRuntimeError, match="predecessor"):
        runtime._verify_predecessor(wrong, _config())


def test_failure_review_result_is_fail_closed_and_selects_new_class(
    tmp_path: Path,
) -> None:
    result_path = tmp_path / "failure_directed_review_result.json"
    result_path.write_text(json.dumps(_result()), encoding="utf-8")
    (tmp_path / "failure_directed_review_report.md").write_text(
        "# report\n\n## CONTRE\n\nselection bias\n", encoding="utf-8"
    )

    result = load_and_verify_failure_review_result(result_path, _config())
    action = failure_review_action_from_result(_predecessor(), result)

    assert action["economic_failure_review_state"] == "COMPLETE"
    assert action["economic_failure_review_multiplicity_delta"] == 0
    assert action["economic_independent_confirmation_queue_eligible_count"] == 0
    assert action["economic_pre_holdout_ready_count"] == 0
    assert action["economic_paper_shadow_ready_count"] == 0
    assert action["new_data_purchase_authorized"] is False
    assert action["protected_holdout_access_authorized"] is False
    assert action["shadow_admission_authorized"] is False
    assert action["next_experiment_id"] == "campaign_0007"


def test_failure_review_result_cannot_mutate_exact_policy(tmp_path: Path) -> None:
    result = _result()
    result["decision"]["mutate_exact_policy"] = True  # type: ignore[index]
    result["result_sha256"] = stable_hash(
        {key: value for key, value in result.items() if key != "result_sha256"}
    )
    result_path = tmp_path / "failure_directed_review_result.json"
    result_path.write_text(json.dumps(result), encoding="utf-8")
    (tmp_path / "failure_directed_review_report.md").write_text(
        "## CONTRE\n", encoding="utf-8"
    )

    with pytest.raises(EconomicEvolutionRuntimeError, match="protected decision"):
        load_and_verify_failure_review_result(result_path, _config())


def test_failure_review_worker_environment_imports_project_root(tmp_path: Path) -> None:
    environment = _worker_environment(tmp_path.resolve())
    assert environment["PYTHONPATH"].split(":")[0] == str(tmp_path.resolve())
