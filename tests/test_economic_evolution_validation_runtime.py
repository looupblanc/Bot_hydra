from __future__ import annotations

import json
from pathlib import Path

import pytest

from hydra.economic_evolution.schema import stable_hash
from hydra.governance.proof_registry import (
    GENESIS_HASH,
    MULTIPLICITY_EVENT,
    PROOF_WINDOW_EVENT,
    burned_window_ids,
    canonical_hash,
    load_and_verify,
    multiplicity_trial_count,
)
from hydra.mission.economic_evolution_runtime import EconomicEvolutionRuntimeError
from hydra.mission.economic_evolution_validation_runtime import (
    MULTIPLICITY_DELTA,
    MULTIPLICITY_EVENT_ID,
    VALIDATION_ID,
    EconomicEvolutionValidationRuntime,
    expensive_validation_action_from_result,
    load_and_verify_expensive_validation_result,
)
from hydra.validation.economic_evolution_expensive_validation import (
    VALIDATION_SCHEMA,
)


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


def _config(*, trials: int = 100) -> dict[str, object]:
    return {
        "candidate": {
            "policy_id": "policy_frozen",
            "policy_specification_hash": "policy_hash",
        },
        "statistics_policy": {"raw_global_N_trials_at_freeze": trials},
        "decision_policy": {
            "failure_statuses": ["EXPENSIVE_VALIDATION_UNDERPOWERED"],
            "supported_status": (
                "DEVELOPMENT_EXPENSIVE_VALIDATION_SUPPORTED_"
                "INDEPENDENT_CONFIRMATION_REQUIRED"
            ),
        },
    }


def _result(tmp_path: Path, *, passed: bool = False) -> dict[str, object]:
    for filename in (
        "account_profile_results.json",
        "matched_controls.json",
        "statistical_validation.json",
    ):
        (tmp_path / filename).write_text('{"frozen": true}', encoding="utf-8")
    value: dict[str, object] = {
        "schema": VALIDATION_SCHEMA,
        "validation_id": VALIDATION_ID,
        "candidate_id": "policy_frozen",
        "candidate_specification_hash": "policy_hash",
        "development_only": True,
        "validated": False,
        "status_inheritance": False,
        "scientific_status": (
            "DEVELOPMENT_EXPENSIVE_VALIDATION_SUPPORTED_"
            "INDEPENDENT_CONFIRMATION_REQUIRED"
            if passed
            else "EXPENSIVE_VALIDATION_UNDERPOWERED"
        ),
        "all_frozen_gates_passed": passed,
        "gates": {"gate_a": passed, "gate_b": True},
        "profile_results_path": str(tmp_path / "account_profile_results.json"),
        "matched_controls_path": str(tmp_path / "matched_controls.json"),
        "statistical_validation_path": str(tmp_path / "statistical_validation.json"),
        "independent_confirmation_queue_eligible": passed,
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


def _predecessor() -> dict[str, object]:
    return {
        "action_type": "ECONOMIC_EVOLUTION_INFORMATION_REVIEW_0004_COMPLETE",
        "phase": "4",
        "economic_expensive_validation_queue_eligible_count": 1,
        "economic_expensive_validation_queue_eligible_ids": ["policy_frozen"],
    }


def test_validation_reservation_is_prospective_and_idempotent(
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / "mission/state"
    registry_path = state_dir / "proof_registry.json"
    _proof_registry(registry_path)
    runtime = EconomicEvolutionValidationRuntime(tmp_path, state_dir)
    before = multiplicity_trial_count(load_and_verify(registry_path))

    first = runtime._ensure_multiplicity_reservation(_config())
    second = runtime._ensure_multiplicity_reservation(_config())
    registry = load_and_verify(registry_path)

    assert first == second
    assert first["event_id"] == MULTIPLICITY_EVENT_ID
    assert multiplicity_trial_count(registry) == before + MULTIPLICITY_DELTA
    assert burned_window_ids(registry) == ("Q4_2024",)
    assert sum(
        row["event_id"] == MULTIPLICITY_EVENT_ID
        for row in registry["entries"]
    ) == 1


def test_validation_reservation_rejects_trial_count_drift(tmp_path: Path) -> None:
    state_dir = tmp_path / "mission/state"
    _proof_registry(state_dir / "proof_registry.json", trials=101)
    runtime = EconomicEvolutionValidationRuntime(tmp_path, state_dir)

    with pytest.raises(EconomicEvolutionRuntimeError, match="trial-count drift"):
        runtime._ensure_multiplicity_reservation(_config(trials=100))


def test_validation_requires_exact_frozen_predecessor(tmp_path: Path) -> None:
    runtime = EconomicEvolutionValidationRuntime(tmp_path, tmp_path / "state")
    runtime._verify_predecessor(_predecessor(), _config())

    wrong = _predecessor()
    wrong["economic_expensive_validation_queue_eligible_ids"] = ["other"]
    with pytest.raises(EconomicEvolutionRuntimeError, match="predecessor"):
        runtime._verify_predecessor(wrong, _config())


@pytest.mark.parametrize("passed", [False, True])
def test_validation_result_is_fail_closed_and_never_promotes(
    tmp_path: Path, passed: bool
) -> None:
    result_path = tmp_path / "expensive_validation_result.json"
    result_path.write_text(json.dumps(_result(tmp_path, passed=passed)))

    verified = load_and_verify_expensive_validation_result(
        result_path, _config()
    )
    action = expensive_validation_action_from_result(_predecessor(), verified)

    assert action["economic_pre_holdout_ready_count"] == 0
    assert action["economic_paper_shadow_ready_count"] == 0
    assert action["new_data_purchase_authorized"] is False
    assert action["protected_holdout_access_authorized"] is False
    assert action["shadow_admission_authorized"] is False
    if passed:
        assert action["economic_independent_confirmation_queue_eligible_count"] == 1
        assert action["next_experiment_state"].endswith("NO_DATA_ACCESS")
    else:
        assert action["economic_independent_confirmation_queue_eligible_count"] == 0
        assert action["next_experiment_id"].endswith("review_0006")


def test_validation_result_tamper_is_rejected(tmp_path: Path) -> None:
    result_path = tmp_path / "expensive_validation_result.json"
    result = _result(tmp_path, passed=False)
    result["pre_holdout_ready_count"] = 1
    result["result_sha256"] = stable_hash(
        {key: value for key, value in result.items() if key != "result_sha256"}
    )
    result_path.write_text(json.dumps(result))

    with pytest.raises(EconomicEvolutionRuntimeError, match="protected-state"):
        load_and_verify_expensive_validation_result(result_path, _config())
