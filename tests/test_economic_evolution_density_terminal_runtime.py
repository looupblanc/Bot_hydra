from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

import hydra.mission.economic_evolution_density_terminal_runtime as terminal_module
from hydra.mission.economic_evolution_density_runtime import (
    CAMPAIGN_ID,
    EXPECTED_N_TRIALS,
)
from hydra.mission.economic_evolution_density_terminal_runtime import (
    NEXT_CAMPAIGN_ID,
    EconomicEvolutionDensityTerminalRuntime,
    load_and_verify_density_terminal_verdict,
)
from hydra.mission.economic_evolution_runtime import EconomicEvolutionRuntimeError
from hydra.research.v7_graveyard import audit_graveyard, class_feedback


PROJECT_ROOT = Path(__file__).resolve().parents[1]
LIVE_STATE_ROOT = Path("/root/hydra-bot")


def _predecessor() -> dict[str, object]:
    return {
        "action_type": "ECONOMIC_EVOLUTION_DENSITY_0007_COMPLETE",
        "phase": "4",
        "economic_density_campaign_id": CAMPAIGN_ID,
        "economic_density_campaign_state": "COMPLETE",
        "economic_density_scientific_status": "ARTEFACT_GEOMETRY_ONLY",
        "economic_density_tripwire_verdict": "ARTEFACT_GEOMETRY_ONLY",
        "economic_density_real_component_count": 22,
        "economic_density_matched_null_count": 22,
        "economic_density_account_policy_evaluated_count": 0,
        "raw_global_N_trials": EXPECTED_N_TRIALS,
        "next_experiment_id": "CLASS_TOMBSTONE_AND_NEW_REPRESENTATION",
        "economic_pre_holdout_ready_count": 0,
        "economic_paper_shadow_ready_count": 0,
    }


def _frozen_result() -> dict:
    return json.loads(
        (
            PROJECT_ROOT
            / "reports/economic_evolution/density_diversification_0007/"
            "density_diversification_result.json"
        ).read_text(encoding="utf-8")
    )


def _frozen_verdict() -> dict:
    return json.loads(
        (
            PROJECT_ROOT
            / "WORM/economic-evolution-density-diversification-0007-"
            "verdict-2026-07-13.json"
        ).read_text(encoding="utf-8")
    )


def test_terminal_worm_matches_tag_and_frozen_result() -> None:
    verdict = load_and_verify_density_terminal_verdict(
        PROJECT_ROOT, result=_frozen_result()
    )
    assert verdict["terminal_decision"]["verdict"] == (
        "CLASS_TOMBSTONE_EXACT_GRAMMAR"
    )
    assert verdict["graveyard_append"]["candidate_count"] == 22
    assert verdict["graveyard_append"]["parameter_level_feedback"] is False


def test_terminal_runtime_appends_once_and_recovers_idempotently(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "project"
    state = root / "mission/state"
    state.mkdir(parents=True)
    shutil.copy2(
        LIVE_STATE_ROOT / "mission/state/graveyard.db", state / "graveyard.db"
    )
    shutil.copy2(
        LIVE_STATE_ROOT / "mission/state/proof_registry.json",
        state / "proof_registry.json",
    )
    result = _frozen_result()
    verdict = _frozen_verdict()
    monkeypatch.setattr(terminal_module, "verify_density_freeze", lambda _root: {})
    monkeypatch.setattr(
        terminal_module,
        "load_and_verify_density_result",
        lambda _path, _config: result,
    )
    monkeypatch.setattr(
        terminal_module,
        "load_and_verify_density_terminal_verdict",
        lambda _root, *, result: verdict,
    )
    runtime = EconomicEvolutionDensityTerminalRuntime(root, state)

    first = runtime.advance(_predecessor())
    second = runtime.advance(_predecessor())
    audit = audit_graveyard(state / "graveyard.db")
    matched = [
        row
        for row in class_feedback(state / "graveyard.db")
        if row["mechanism_class"]
        == "INDEPENDENT_OPPORTUNITY_DENSITY_CONSISTENCY_ASSEMBLY_V1"
        and row["regime"] == "DEVELOPMENT_2023Q3_TO_2024Q3_MULTI_MARKET"
    ]

    assert first == second
    assert first["action_type"] == (
        "ECONOMIC_EVOLUTION_DENSITY_0007_TOMBSTONED"
    )
    assert first["next_experiment_id"] == NEXT_CAMPAIGN_ID
    assert first["economic_density_parameter_rescue_allowed"] is False
    assert first["economic_pre_holdout_ready_count"] == 0
    assert first["economic_paper_shadow_ready_count"] == 0
    assert audit["class_signature_count"] == 95
    assert audit["indexed_object_count"] == 115_624
    assert len(matched) == 1
    assert matched[0]["candidate_count"] == 22
    assert runtime.snapshot()["class_tombstone_present"] is True


def test_terminal_runtime_rejects_nonterminal_predecessor(tmp_path: Path) -> None:
    runtime = EconomicEvolutionDensityTerminalRuntime(tmp_path, tmp_path / "state")
    wrong = _predecessor()
    wrong["economic_density_tripwire_verdict"] = "GREEN_NULL_ADJUSTED_BASELINE"
    with pytest.raises(EconomicEvolutionRuntimeError, match="predecessor"):
        runtime._verify_predecessor(wrong)


def test_terminal_action_cannot_promote_or_authorize_data(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "project"
    state = root / "mission/state"
    state.mkdir(parents=True)
    shutil.copy2(
        LIVE_STATE_ROOT / "mission/state/graveyard.db", state / "graveyard.db"
    )
    shutil.copy2(
        LIVE_STATE_ROOT / "mission/state/proof_registry.json",
        state / "proof_registry.json",
    )
    monkeypatch.setattr(terminal_module, "verify_density_freeze", lambda _root: {})
    monkeypatch.setattr(
        terminal_module,
        "load_and_verify_density_result",
        lambda _path, _config: _frozen_result(),
    )
    monkeypatch.setattr(
        terminal_module,
        "load_and_verify_density_terminal_verdict",
        lambda _root, *, result: _frozen_verdict(),
    )
    action = EconomicEvolutionDensityTerminalRuntime(root, state).advance(
        _predecessor()
    )
    assert action["new_data_purchase_authorized"] is False
    assert action["protected_holdout_access_authorized"] is False
    assert action["shadow_admission_authorized"] is False
    assert action["economic_independent_confirmation_queue_eligible_count"] == 0
