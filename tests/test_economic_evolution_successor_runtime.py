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
from hydra.mission.economic_evolution_runtime import (
    EconomicEvolutionRuntimeError,
)
from hydra.mission.economic_evolution_successor_runtime import (
    CAMPAIGN_ID,
    CAMPAIGN_RESULT_NAME,
    MULTIPLICITY_DELTA,
    MULTIPLICITY_EVENT_ID,
    EconomicEvolutionSuccessorRuntime,
    load_and_verify_successor_result,
    successor_action_from_result,
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


def _runtime_config() -> dict[str, object]:
    return {
        "preregistration_hash": "frozen",
        "structural_population": {"candidate_manifest_hash": "manifest"},
        "funnel": {
            "raw_proposals": 25_000,
            "maximum_exact_component_replays": 700,
            "incremental_value_evaluations": 400,
            "maximum_component_bank": 100,
            "structural_account_policies": 500,
            "failure_directed_policy_children": 800,
            "exact_account_policy_evaluations": 1_000,
            "rolling_combine_elite_count": 60,
        },
        "multiplicity": {"campaign_specific_inflation": 1.5},
    }


def _successor_result() -> dict[str, object]:
    return {
        "schema": "hydra_economic_evolution_campaign_result_v1",
        "campaign_id": CAMPAIGN_ID,
        "preregistration_hash": "frozen",
        "engine_version": "hydra_economic_evolution_engine_v2",
        "funnel": {
            "raw_structural_proposals": 25_000,
            "unique_sleeves": 22_000,
            "cheap_screen_survivors": 2_000,
            "exact_component_replays": 700,
            "incremental_value_evaluations": 400,
            "micro_edge_useful": 40,
            "component_bank": 100,
            "structural_account_policies": 500,
            "failure_directed_policy_children": 800,
            "exact_account_policies": 1_000,
            "account_policy_research_candidates": 3,
            "combine_path_candidates": 1,
            "rolling_combine_elites": 60,
            "pre_holdout_ready": 0,
            "paper_shadow_ready": 0,
        },
        "rolling_combine": {
            "pass_count": 1,
            "median_target_progress": 0.55,
            "maximum_target_progress": 1.0,
            "median_mll_breach_rate": 0.0,
        },
        "governance": {
            "development_only": True,
            "expensive_validation_executed": False,
            "single_authoritative_mission_writer_preserved": True,
            "protected_holdout_accessed": False,
            "q4_accessed": False,
            "outbound_order_capability": False,
            "broker_connections": 0,
            "orders": 0,
            "status_inheritance": False,
        },
    }


def test_successor_reservation_is_prospective_and_idempotent(
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / "mission/state"
    registry_path = state_dir / "proof_registry.json"
    _proof_registry(registry_path)
    runtime = EconomicEvolutionSuccessorRuntime(tmp_path, state_dir)
    config = _runtime_config()

    first = runtime._ensure_multiplicity_reservation(config)
    second = runtime._ensure_multiplicity_reservation(config)
    registry = load_and_verify(registry_path)

    assert first == second
    assert first["event_id"] == MULTIPLICITY_EVENT_ID
    assert multiplicity_trial_count(registry) == 100 + MULTIPLICITY_DELTA
    assert burned_window_ids(registry) == ("Q4_2024",)
    assert sum(
        row["event_id"] == MULTIPLICITY_EVENT_ID
        for row in registry["entries"]
    ) == 1
    assert not (runtime.output_dir / CAMPAIGN_RESULT_NAME).exists()


def test_successor_result_is_fail_closed_and_action_is_explicit(
    tmp_path: Path,
) -> None:
    config = _runtime_config()
    result = _successor_result()
    path = tmp_path / "result.json"
    path.write_text(json.dumps(result), encoding="utf-8")

    verified = load_and_verify_successor_result(path, config)
    action = successor_action_from_result(
        {"action_type": "PREDECESSOR_COMPLETE", "g12_candidate_count": 24},
        verified,
    )

    assert action["action_type"] == "ECONOMIC_EVOLUTION_CAMPAIGN_0003_COMPLETE"
    assert action["g12_candidate_count"] == 24
    assert action["economic_combine_pass_count"] == 1
    assert action["economic_pre_holdout_ready_count"] == 0
    assert action["economic_paper_shadow_ready_count"] == 0
    assert action["new_data_purchase_authorized"] is False
    assert action["protected_holdout_access_authorized"] is False
    assert action["shadow_admission_authorized"] is False

    result["funnel"]["pre_holdout_ready"] = 1  # type: ignore[index]
    path.write_text(json.dumps(result), encoding="utf-8")
    with pytest.raises(EconomicEvolutionRuntimeError, match="promotion"):
        load_and_verify_successor_result(path, config)


def test_successor_result_rejects_funnel_overrun(tmp_path: Path) -> None:
    config = _runtime_config()
    result = _successor_result()
    result["funnel"]["rolling_combine_elites"] = 61  # type: ignore[index]
    path = tmp_path / "result.json"
    path.write_text(json.dumps(result), encoding="utf-8")

    with pytest.raises(EconomicEvolutionRuntimeError, match="WORM bounds"):
        load_and_verify_successor_result(path, config)


def test_frozen_config_hash_helper_remains_deterministic() -> None:
    payload = _runtime_config()
    assert stable_hash(payload) == stable_hash(json.loads(json.dumps(payload)))
