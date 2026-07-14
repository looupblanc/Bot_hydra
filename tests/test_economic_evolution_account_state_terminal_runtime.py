from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

import hydra.mission.economic_evolution_account_state_terminal_runtime as terminal_module
from hydra.governance.proof_registry import MULTIPLICITY_EVENT, append_entry
from hydra.mission.economic_evolution_account_state_runtime import (
    CAMPAIGN_ID,
    EXPECTED_N_TRIALS,
)
from hydra.mission.economic_evolution_account_state_terminal_runtime import (
    NEXT_CAMPAIGN_ID,
    EconomicEvolutionAccountStateTerminalRuntime,
    load_and_verify_account_state_terminal_verdict,
)
from hydra.mission.economic_evolution_runtime import EconomicEvolutionRuntimeError
from hydra.research.v7_graveyard import (
    ClassTombstone,
    append_class_tombstone,
    audit_graveyard,
    class_feedback,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
BASELINE_STATE_ROOT = (
    PROJECT_ROOT
    / "mission/state/snapshots/"
    "economic_account_state_0011_predeploy_20260714T004311Z"
)


def _predecessor() -> dict[str, object]:
    return {
        "action_type": "ECONOMIC_EVOLUTION_ACCOUNT_STATE_0011_COMPLETE",
        "phase": "4",
        "economic_account_state_campaign_id": CAMPAIGN_ID,
        "economic_account_state_campaign_state": "COMPLETE",
        "economic_account_state_scientific_status": "ARTEFACT_GEOMETRY_ONLY",
        "economic_account_state_tripwire_verdict": "ARTEFACT_GEOMETRY_ONLY",
        "economic_account_state_real_policy_count": 512,
        "economic_account_state_matched_control_count": 512,
        "economic_account_state_policy_pair_evaluated_count": 512,
        "economic_account_state_policies_with_combine_pass_count": 0,
        "raw_global_N_trials": EXPECTED_N_TRIALS,
        "next_experiment_id": (
            "TOMBSTONE_EXACT_0011_AND_CHANGE_ACCOUNT_REPRESENTATION"
        ),
        "economic_pre_holdout_ready_count": 0,
        "economic_paper_shadow_ready_count": 0,
    }


def _frozen_result() -> dict:
    return json.loads(
        (
            PROJECT_ROOT
            / "reports/economic_evolution/account_state_router_0011/"
            "account_state_result.json"
        ).read_text(encoding="utf-8")
    )


def _frozen_verdict() -> dict:
    return json.loads(
        (
            PROJECT_ROOT
            / "WORM/economic-evolution-account-state-router-0011-"
            "verdict-2026-07-14.json"
        ).read_text(encoding="utf-8")
    )


def _runtime_fixture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> EconomicEvolutionAccountStateTerminalRuntime:
    root = tmp_path / "project"
    state = root / "mission/state"
    state.mkdir(parents=True)
    shutil.copy2(BASELINE_STATE_ROOT / "graveyard.db", state / "graveyard.db")
    shutil.copy2(
        BASELINE_STATE_ROOT / "proof_registry.json",
        state / "proof_registry.json",
    )
    append_class_tombstone(
        state / "graveyard.db",
        ClassTombstone(
            mechanism_class="ROLE_AWARE_OPPORTUNITY_POOL_ALLOCATOR_V1",
            regime=(
                "DEVELOPMENT_2023Q3_TO_2024Q3_MULTI_MARKET_MULTI_SESSION_"
                "STATIC_ROLE_ALLOCATION"
            ),
            death_cause="GEOMETRY_ONLY_NULL_RATIO_GTE_0_8",
            candidate_count=512,
            source_scope=(
                "HYDRA_ECONOMIC_EVOLUTION_ROLE_AWARE_ACCOUNT_0010_REAL_POLICIES"
            ),
            evidence_sha256=(
                "8bf2aeda48804d7b8f529c7bc6299450bab815b62dc2d8160612d5b711778033"
            ),
        ),
    )
    append_entry(
        state / "proof_registry.json",
        {
            "event_id": "account_state_0011_test_reservation",
            "event_type": MULTIPLICITY_EVENT,
            "recorded_at_utc": "2026-07-14T00:44:20+00:00",
            "status": "RESERVED",
            "scientific_role": "DEVELOPMENT_ONLY",
            "evidence": {"campaign_id": CAMPAIGN_ID},
            "multiplicity": {
                "previous_N_trials": EXPECTED_N_TRIALS - 3_600,
                "delta_trials": 3_600,
                "cumulative_N_trials": EXPECTED_N_TRIALS,
            },
        },
    )
    monkeypatch.setattr(
        terminal_module,
        "verify_account_state_freeze",
        lambda _root: {},
    )
    monkeypatch.setattr(
        terminal_module,
        "load_and_verify_account_state_result",
        lambda _path, _config: _frozen_result(),
    )
    monkeypatch.setattr(
        terminal_module,
        "load_and_verify_account_state_terminal_verdict",
        lambda _root, *, result: _frozen_verdict(),
    )
    return EconomicEvolutionAccountStateTerminalRuntime(root, state)


def test_terminal_worm_matches_tag_and_frozen_result() -> None:
    verdict = load_and_verify_account_state_terminal_verdict(
        PROJECT_ROOT,
        result=_frozen_result(),
    )
    assert verdict["terminal_decision"]["verdict"] == (
        "CLASS_TOMBSTONE_EXACT_GRAMMAR"
    )
    assert verdict["graveyard_append"]["candidate_count"] == 512
    assert verdict["graveyard_append"]["parameter_level_feedback"] is False


def test_terminal_runtime_appends_once_and_recovers_idempotently(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime_fixture(tmp_path, monkeypatch)
    first = runtime.advance(_predecessor())
    second = runtime.advance(_predecessor())
    audit = audit_graveyard(runtime.graveyard_path)
    matched = [
        row
        for row in class_feedback(runtime.graveyard_path)
        if row["mechanism_class"] == "ACCOUNT_STATE_CONDITIONAL_ROLE_ROUTER_V1"
    ]
    assert first == second
    assert first["action_type"] == (
        "ECONOMIC_EVOLUTION_ACCOUNT_STATE_0011_TOMBSTONED"
    )
    assert first["next_experiment_id"] == NEXT_CAMPAIGN_ID
    assert first["economic_account_state_parameter_rescue_allowed"] is False
    assert first["economic_pre_holdout_ready_count"] == 0
    assert first["economic_paper_shadow_ready_count"] == 0
    assert audit["class_signature_count"] == 99
    assert audit["indexed_object_count"] == 117_204
    assert len(matched) == 1
    assert matched[0]["candidate_count"] == 512
    assert runtime.snapshot()["class_tombstone_present"] is True


def test_terminal_runtime_rejects_nonterminal_predecessor(tmp_path: Path) -> None:
    runtime = EconomicEvolutionAccountStateTerminalRuntime(
        tmp_path,
        tmp_path / "state",
    )
    wrong = _predecessor()
    wrong["economic_account_state_tripwire_verdict"] = (
        "GREEN_NULL_ADJUSTED_BASELINE"
    )
    with pytest.raises(EconomicEvolutionRuntimeError, match="predecessor"):
        runtime._verify_predecessor(wrong)


def test_terminal_action_cannot_promote_or_authorize_data(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    action = _runtime_fixture(tmp_path, monkeypatch).advance(_predecessor())
    assert action["new_data_purchase_authorized"] is False
    assert action["protected_holdout_access_authorized"] is False
    assert action["shadow_admission_authorized"] is False


def test_completed_terminal_allows_later_multiplicity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime_fixture(tmp_path, monkeypatch)
    first = runtime.advance(_predecessor())
    append_entry(
        runtime.state_dir / "proof_registry.json",
        {
            "event_id": "downstream_0012_test_reservation",
            "event_type": MULTIPLICITY_EVENT,
            "recorded_at_utc": "2026-07-14T01:00:00+00:00",
            "status": "RESERVED",
            "scientific_role": "DEVELOPMENT_ONLY",
            "evidence": {"campaign_id": NEXT_CAMPAIGN_ID},
            "multiplicity": {
                "previous_N_trials": EXPECTED_N_TRIALS,
                "delta_trials": 1,
                "cumulative_N_trials": EXPECTED_N_TRIALS + 1,
            },
        },
    )
    assert runtime.advance(_predecessor()) == first
