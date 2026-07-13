from __future__ import annotations

import json
from pathlib import Path

import pytest

from hydra.governance.proof_registry import (
    GENESIS_HASH,
    MULTIPLICITY_EVENT,
    PROOF_WINDOW_EVENT,
    burned_window_ids,
    canonical_hash,
    load_and_verify,
    multiplicity_trial_count,
)
from hydra.mission.economic_evolution_information_runtime import (
    MULTIPLICITY_DELTA,
    MULTIPLICITY_EVENT_ID,
    REVIEW_ID,
    EconomicEvolutionInformationRuntime,
    information_review_action_from_result,
    load_and_verify_information_review_result,
)
from hydra.mission.economic_evolution_runtime import EconomicEvolutionRuntimeError
from hydra.research.economic_evolution_information_review import REVIEW_SCHEMA


def _entry(payload: dict[str, object], previous: str) -> dict[str, object]:
    value = {**payload, "previous_hash": previous}
    value["entry_hash"] = canonical_hash(value)
    return value


def _proof_registry(path: Path, *, trials: int = 100) -> None:
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
        "preregistration_hash": "frozen_review",
        "selected_policies": [{"policy_id": f"policy_{index}"} for index in range(5)],
        "multiplicity": {"prospective_diagnostic_comparisons": 50},
    }


def _result(*, eligible: int = 1) -> dict[str, object]:
    return {
        "schema": REVIEW_SCHEMA,
        "review_id": REVIEW_ID,
        "preregistration_hash": "frozen_review",
        "source_campaign_result_sha256": (
            "85d2b600a0ea76d4aaf3ee65dc4a6a77017fd89587708da31321cdbe12705de0"
        ),
        "selected_policy_count": 5,
        "full_available_base_pass_count": 2,
        "full_available_stressed_pass_count": 1,
        "expensive_validation_queue_eligible_count": eligible,
        "expensive_validation_queue_eligible_ids": (
            ["policy_0"] if eligible else []
        ),
        "scientific_status": "DEVELOPMENT_PATH_JUSTIFIES_EXPENSIVE_VALIDATION_QUEUE",
        "development_only": True,
        "validated_policy_count": 0,
        "pre_holdout_ready_count": 0,
        "paper_shadow_ready_count": 0,
        "proof_window_consumed": False,
        "new_data_purchase_count": 0,
        "q4_access_delta": 0,
        "broker_connections": 0,
        "orders": 0,
        "outbound_order_capability": False,
    }


def test_information_review_reservation_is_prospective_and_idempotent(
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / "mission/state"
    registry_path = state_dir / "proof_registry.json"
    _proof_registry(registry_path)
    runtime = EconomicEvolutionInformationRuntime(tmp_path, state_dir)
    before = multiplicity_trial_count(load_and_verify(registry_path))

    first = runtime._ensure_multiplicity_reservation(_config())
    second = runtime._ensure_multiplicity_reservation(_config())
    registry = load_and_verify(registry_path)

    assert first == second
    assert first["event_id"] == MULTIPLICITY_EVENT_ID
    assert multiplicity_trial_count(registry) == before + MULTIPLICITY_DELTA
    assert burned_window_ids(registry) == ("Q4_2024",)
    assert sum(
        row["event_id"] == MULTIPLICITY_EVENT_ID for row in registry["entries"]
    ) == 1


def test_information_review_result_is_fail_closed_and_never_promotes(
    tmp_path: Path,
) -> None:
    path = tmp_path / "result.json"
    result = _result(eligible=1)
    path.write_text(json.dumps(result), encoding="utf-8")

    verified = load_and_verify_information_review_result(path, _config())
    action = information_review_action_from_result(
        {"action_type": "PREDECESSOR_COMPLETE", "phase": "4"}, verified
    )

    assert action["action_type"] == "ECONOMIC_EVOLUTION_INFORMATION_REVIEW_0004_COMPLETE"
    assert action["economic_expensive_validation_queue_eligible_count"] == 1
    assert action["economic_pre_holdout_ready_count"] == 0
    assert action["economic_paper_shadow_ready_count"] == 0
    assert action["next_experiment_state"] == "WORM_PREREGISTRATION_REQUIRED"
    assert action["new_data_purchase_authorized"] is False
    assert action["protected_holdout_access_authorized"] is False
    assert action["shadow_admission_authorized"] is False

    result["validated_policy_count"] = 1
    path.write_text(json.dumps(result), encoding="utf-8")
    with pytest.raises(EconomicEvolutionRuntimeError, match="integrity drift"):
        load_and_verify_information_review_result(path, _config())


def test_information_review_without_eligible_path_pivots_to_representation() -> None:
    action = information_review_action_from_result(
        {"action_type": "PREDECESSOR_COMPLETE", "phase": "4"},
        _result(eligible=0),
    )

    assert action["next_experiment_id"].endswith("representation_review_0005")
    assert action["protected_holdout_access_authorized"] is False
