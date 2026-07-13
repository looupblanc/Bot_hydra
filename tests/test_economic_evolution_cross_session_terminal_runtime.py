from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

import hydra.mission.economic_evolution_cross_session_terminal_runtime as terminal_module
from hydra.governance.proof_registry import MULTIPLICITY_EVENT, append_entry
from hydra.mission.economic_evolution_cross_session_runtime import (
    CAMPAIGN_ID,
    EXPECTED_N_TRIALS,
)
from hydra.mission.economic_evolution_cross_session_terminal_runtime import (
    NEXT_CAMPAIGN_ID,
    EconomicEvolutionCrossSessionTerminalRuntime,
    load_and_verify_cross_session_terminal_verdict,
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
    Path("/root/hydra-bot")
    / "mission/state/snapshots/"
    "economic_cross_session_0009_predeploy_20260713T220658Z"
)


def _predecessor() -> dict[str, object]:
    return {
        "action_type": "ECONOMIC_EVOLUTION_CROSS_SESSION_0009_COMPLETE",
        "phase": "4",
        "economic_cross_session_campaign_id": CAMPAIGN_ID,
        "economic_cross_session_campaign_state": "COMPLETE",
        "economic_cross_session_scientific_status": "ARTEFACT_GEOMETRY_ONLY",
        "economic_cross_session_tripwire_verdict": "ARTEFACT_GEOMETRY_ONLY",
        "economic_cross_session_real_policy_count": 512,
        "economic_cross_session_matched_control_count": 512,
        "economic_cross_session_policy_pair_evaluated_count": 512,
        "economic_cross_session_policies_with_combine_pass_count": 0,
        "raw_global_N_trials": EXPECTED_N_TRIALS,
        "next_experiment_id": (
            "TOMBSTONE_EXACT_0009_AND_PREREGISTER_DISTINCT_0010"
        ),
        "economic_pre_holdout_ready_count": 0,
        "economic_paper_shadow_ready_count": 0,
    }


def _frozen_result() -> dict:
    return json.loads(
        (
            PROJECT_ROOT
            / "reports/economic_evolution/cross_session_account_0009/"
            "cross_session_account_result.json"
        ).read_text(encoding="utf-8")
    )


def _frozen_verdict() -> dict:
    return json.loads(
        (
            PROJECT_ROOT
            / "WORM/economic-evolution-cross-session-account-0009-"
            "verdict-2026-07-13.json"
        ).read_text(encoding="utf-8")
    )


def _runtime_fixture(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> EconomicEvolutionCrossSessionTerminalRuntime:
    root = tmp_path / "project"
    state = root / "mission/state"
    state.mkdir(parents=True)
    shutil.copy2(
        BASELINE_STATE_ROOT / "graveyard.db", state / "graveyard.db"
    )
    append_class_tombstone(
        state / "graveyard.db",
        ClassTombstone(
            mechanism_class="DIRECTIONAL_CONTEXT_AGREEMENT_TRADE_VETO_V1",
            regime=(
                "DEVELOPMENT_2023Q3_TO_2024Q3_MULTI_MARKET_CLOSED_30M_60M"
            ),
            death_cause="GEOMETRY_ONLY_NULL_RATIO_GTE_0_8",
            candidate_count=44,
            source_scope=(
                "HYDRA_ECONOMIC_EVOLUTION_DIRECTIONAL_AGREEMENT_0008_REAL_"
                "COMPONENTS"
            ),
            evidence_sha256=(
                "4ca7fa41f47cd652ed7ad1f1ba23713aa1551660093b104f88c172a11ef17773"
            ),
        ),
    )
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
            "evidence": {"campaign_id": CAMPAIGN_ID},
            "multiplicity": {
                "previous_N_trials": EXPECTED_N_TRIALS - 3_600,
                "delta_trials": 3_600,
                "cumulative_N_trials": EXPECTED_N_TRIALS,
            },
        },
    )
    monkeypatch.setattr(
        terminal_module, "verify_cross_session_freeze", lambda _root: {}
    )
    monkeypatch.setattr(
        terminal_module,
        "load_and_verify_cross_session_result",
        lambda _path, _config: _frozen_result(),
    )
    monkeypatch.setattr(
        terminal_module,
        "load_and_verify_cross_session_terminal_verdict",
        lambda _root, *, result: _frozen_verdict(),
    )
    return EconomicEvolutionCrossSessionTerminalRuntime(root, state)


def test_terminal_worm_matches_tag_and_frozen_result() -> None:
    verdict = load_and_verify_cross_session_terminal_verdict(
        PROJECT_ROOT, result=_frozen_result()
    )
    assert verdict["terminal_decision"]["verdict"] == (
        "CLASS_TOMBSTONE_EXACT_GRAMMAR"
    )
    assert verdict["graveyard_append"]["candidate_count"] == 512
    assert verdict["graveyard_append"]["parameter_level_feedback"] is False


def test_terminal_runtime_appends_once_and_recovers_idempotently(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime = _runtime_fixture(tmp_path, monkeypatch)

    first = runtime.advance(_predecessor())
    second = runtime.advance(_predecessor())
    audit = audit_graveyard(runtime.graveyard_path)
    matched = [
        row
        for row in class_feedback(runtime.graveyard_path)
        if row["mechanism_class"]
        == "CROSS_SESSION_ACCOUNT_COMPLEMENTARITY_SYNTHESIS_V1"
        and row["regime"]
        == (
            "DEVELOPMENT_2023Q3_TO_2024Q3_MULTI_MARKET_MULTI_SESSION_"
            "SHARED_ACCOUNT"
        )
    ]

    assert first == second
    assert first["action_type"] == (
        "ECONOMIC_EVOLUTION_CROSS_SESSION_0009_TOMBSTONED"
    )
    assert first["next_experiment_id"] == NEXT_CAMPAIGN_ID
    assert first["economic_cross_session_parameter_rescue_allowed"] is False
    assert first["economic_pre_holdout_ready_count"] == 0
    assert first["economic_paper_shadow_ready_count"] == 0
    assert audit["class_signature_count"] == 97
    assert audit["indexed_object_count"] == 116_180
    assert len(matched) == 1
    assert matched[0]["candidate_count"] == 512
    assert runtime.snapshot()["class_tombstone_present"] is True


def test_terminal_runtime_rejects_nonterminal_predecessor(tmp_path: Path) -> None:
    runtime = EconomicEvolutionCrossSessionTerminalRuntime(
        tmp_path, tmp_path / "state"
    )
    wrong = _predecessor()
    wrong["economic_cross_session_tripwire_verdict"] = (
        "GREEN_NULL_ADJUSTED_BASELINE"
    )
    with pytest.raises(EconomicEvolutionRuntimeError, match="predecessor"):
        runtime._verify_predecessor(wrong)


def test_terminal_action_cannot_promote_or_authorize_data(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    action = _runtime_fixture(tmp_path, monkeypatch).advance(_predecessor())
    assert action["new_data_purchase_authorized"] is False
    assert action["protected_holdout_access_authorized"] is False
    assert action["shadow_admission_authorized"] is False


def test_completed_terminal_allows_later_campaign_reservation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime = _runtime_fixture(tmp_path, monkeypatch)
    first = runtime.advance(_predecessor())
    append_entry(
        runtime.state_dir / "proof_registry.json",
        {
            "event_id": "downstream_role_allocator_reservation",
            "event_type": MULTIPLICITY_EVENT,
            "recorded_at_utc": "2026-07-13T23:00:00+00:00",
            "status": "RESERVED",
            "scientific_role": "DEVELOPMENT_ONLY",
            "evidence": {"campaign_id": NEXT_CAMPAIGN_ID},
            "multiplicity": {
                "previous_N_trials": EXPECTED_N_TRIALS,
                "delta_trials": 3_600,
                "cumulative_N_trials": EXPECTED_N_TRIALS + 3_600,
            },
        },
    )

    second = runtime.advance(_predecessor())
    assert first == second


def test_completed_terminal_allows_later_class_tombstone(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime = _runtime_fixture(tmp_path, monkeypatch)
    first = runtime.advance(_predecessor())
    append_class_tombstone(
        runtime.graveyard_path,
        ClassTombstone(
            mechanism_class="DOWNSTREAM_ROLE_ALLOCATOR_TEST_CLASS",
            regime="DEVELOPMENT_TEST",
            death_cause="DOWNSTREAM_NULL",
            candidate_count=3,
            source_scope="TEST_ONLY",
            evidence_sha256="b" * 64,
        ),
    )

    second = runtime.advance(_predecessor())

    assert first == second
    audit = audit_graveyard(runtime.graveyard_path)
    assert audit["class_signature_count"] == 98
    assert audit["indexed_object_count"] == 116_183
