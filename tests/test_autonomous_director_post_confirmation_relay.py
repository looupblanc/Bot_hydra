from __future__ import annotations

import json
from pathlib import Path

import pytest

from hydra.production import autonomous_director_runtime as runtime


ROOT = Path(__file__).resolve().parents[1]
EVENT_CONTROL_RESULT = ROOT / (
    "reports/economic_evolution/"
    "autonomous_economic_discovery_director_0035_revision_02/"
    "branch_results/post_source_exhaustion/post_composite/"
    "event_time_matched_controls.json"
)


def _event_result() -> dict:
    return json.loads(EVENT_CONTROL_RESULT.read_text(encoding="utf-8"))


def test_event_time_control_result_is_exactly_reconciled() -> None:
    result = runtime._verify_event_time_matched_controls_result(_event_result())

    assert result["control_verdict"] == "EVENT_TIME_MATCHED_CONTROLS_NOT_DISTINCT"
    assert runtime._event_time_control_counts(result) == {
        "control_count": 3,
        "exact_episode_count": 520,
        "normal_episode_count": 260,
        "stressed_episode_count": 260,
    }


def test_post_confirmation_counts_enter_state_once() -> None:
    manifest = json.loads(
        (ROOT / "config/v7/autonomous_economic_discovery_director_0035.json")
        .read_text(encoding="utf-8")
    )
    state = runtime._state_payload(
        manifest,
        sequence=7,
        state="ROBUSTNESS_ACTIVE",
        stage="EVENT_TIME_MATCHED_CONTROLS_NOT_DISTINCT",
        branch_results={"EVENT_TIME_MATCHED_CONTROLS": _event_result()},
        next_action="CLOSE_EVENT_TIME_CANDIDATE_AND_REALLOCATE_EXPLORATION",
    )

    assert state["combine_episodes_completed"] == 520
    assert state["normal_episodes_completed"] == 260
    assert state["stressed_episodes_completed"] == 260
    assert state["exact_account_replays"] == 4
    assert state["unique_policies_screened"] == 4
    assert state["control_policy_replay_operations"] == 3


def test_frozen_breadth_paths_fail_closed_outside_repository() -> None:
    manifest = {
        "post_confirmation_branch_portfolio": {
            "breadth_contract_path": "/tmp/outside.json",
            "breadth_acquisition_receipt_path": "inside.json",
            "breadth_feature_receipt_path": "inside-feature.json",
            "breadth_result_path": "inside-result.json",
        }
    }

    with pytest.raises(runtime.AutonomousDirectorRuntimeError, match="escapes"):
        runtime._frozen_breadth_manifest_paths(ROOT, manifest)
