from __future__ import annotations

import json
from pathlib import Path

import pytest

from hydra.mission.v7_falsification_controller import (
    V7ControllerConfig,
    V7ControllerIntegrityError,
    V7FalsificationController,
    classify_v7_action,
)
from scripts.run_v7_falsification_mission import main


def _write_tribunal(root: Path, *, verdict: str, selected: list[str]) -> None:
    path = root / "reports/v7/data/d1_candidate_tribunal_result.json"
    path.parent.mkdir(parents=True)
    path.write_text(
        json.dumps(
            {
                "verdict": verdict,
                "selected_shadow_queue_candidate_ids": selected,
            }
        ),
        encoding="utf-8",
    )


def test_classification_waits_for_atomic_tribunal(tmp_path: Path) -> None:
    result = classify_v7_action(tmp_path)

    assert result["action_type"] == "D1_CANDIDATE_TRIBUNAL_PENDING"
    assert result["progressed"] is False


def test_null_tribunal_pivots_at_class_level(tmp_path: Path) -> None:
    _write_tribunal(tmp_path, verdict="NULL", selected=[])

    result = classify_v7_action(tmp_path)

    assert result["action_type"] == "D1_CLASS_TOMBSTONE_REQUIRED"
    assert result["progressed"] is False


def test_green_tribunal_requires_fiche_then_boundary(tmp_path: Path) -> None:
    candidate = "candidate_a"
    _write_tribunal(tmp_path, verdict="GREEN", selected=[candidate])

    missing = classify_v7_action(tmp_path)
    assert missing["action_type"] == "CANDIDATE_FICHE_FREEZE_REQUIRED"

    fiche = tmp_path / "WORM/candidates/candidate_a.json"
    fiche.parent.mkdir(parents=True)
    fiche.write_text("{}", encoding="utf-8")
    no_boundary = classify_v7_action(tmp_path)
    assert no_boundary["action_type"] == "FORWARD_BOUNDARY_MANIFEST_REQUIRED"

    boundary = tmp_path / "mission/state/v7_forward_boundary_manifest.json"
    boundary.parent.mkdir(parents=True)
    boundary.write_text("{}", encoding="utf-8")
    ready = classify_v7_action(tmp_path)
    assert ready["action_type"] == "FORWARD_FEED_READY"


def test_inconsistent_tribunal_fails_closed(tmp_path: Path) -> None:
    _write_tribunal(tmp_path, verdict="GREEN", selected=[])

    with pytest.raises(V7ControllerIntegrityError):
        classify_v7_action(tmp_path)


def test_controller_rejects_live_trading() -> None:
    with pytest.raises(V7ControllerIntegrityError):
        V7FalsificationController(V7ControllerConfig(no_live_trading=False))


def test_runner_uses_non_restarting_integrity_exit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "scripts.run_v7_falsification_mission.parse_args",
        lambda: type(
            "Args",
            (),
            {
                "project_root": ".",
                "state_dir": "mission/state",
                "sleep_seconds": 0.0,
                "checkpoint_every_steps": 25,
                "persistent": True,
                "maximum_steps": 1,
                "no_live_trading": True,
            },
        )(),
    )
    monkeypatch.setattr(
        "scripts.run_v7_falsification_mission.run_v7_controller",
        lambda _config: (_ for _ in ()).throw(V7ControllerIntegrityError("drift")),
    )

    assert main() == 78
