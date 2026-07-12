from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from hydra.governance.cohort_authorization import issue_cohort_authorization
from hydra.mission.controller import (
    AutonomousMissionController,
    DECISION_BRIDGE_V4_PREPARE_EXPERIMENT_ID,
    MissionControllerConfig,
)
from hydra.mission.experiment_queue import experiment_record
from hydra.mission.mission_state import connect_state, mission_paths, set_kv
from hydra.promotion.final_cohort import stable_hash


def _controller(tmp_path: Path):
    paths = mission_paths(str(tmp_path / "state"))
    conn = connect_state(paths)
    controller = AutonomousMissionController(
        MissionControllerConfig(
            mission_id="decision-bridge-test",
            baseline_commit="baseline",
            objective_config="test",
            remaining_databento_budget_usd=70.0,
            persistent=False,
            state_dir=str(paths.state_dir),
            sleep_seconds=0.0,
        )
    )
    return controller, conn


def test_blocker_queues_one_preparation_without_q4_access(tmp_path: Path) -> None:
    controller, conn = _controller(tmp_path)
    try:
        source = (
            tmp_path
            / "reports"
            / "mission_experiments"
            / "evidence_conversion_v3_cohort_0000"
        )
        source.mkdir(parents=True)
        for name in (
            "pre_holdout_cohort_manifest.json",
            "complete_validation.json",
            "behavioral_clusters.json",
        ):
            (source / name).write_text("{}\n", encoding="utf-8")
        assert controller._reconcile_decision_bridge_v4_preparation(conn)
        record = experiment_record(conn, DECISION_BRIDGE_V4_PREPARE_EXPERIMENT_ID)
        assert record is not None and record["status"] == "QUEUED"
        specification = record["specification"]
        assert specification["q4_access_allowed"] is False
        assert specification["paper_shadow_ready_allowed"] is False
        assert specification["parallel_safe"] is False
        assert specification["q4_access_count"] == 0
        assert controller._reconcile_decision_bridge_v4_preparation(conn)
    finally:
        conn.close()

def test_experiment_guard_allows_only_validated_exact_q4_capability(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    controller, conn = _controller(tmp_path)
    try:
        manifest: dict[str, object] = {
            "schema": "hydra_final_q4_cohort_v4",
            "cohort_id": "guard_cohort",
            "candidate_ids": ["a", "b", "c"],
            "candidate_count": 3,
            "candidates": [{"candidate_id": value} for value in ("a", "b", "c")],
            "source_commit": "a" * 40,
            "q4_access_count_before": 0,
            "q4_access_authorized": False,
        }
        manifest["manifest_hash"] = stable_hash(manifest)
        manifest_path = tmp_path / "manifest.json"
        manifest_path.write_text(json.dumps(manifest) + "\n", encoding="utf-8")
        ledger = tmp_path / "access.jsonl"
        issued = issue_cohort_authorization(
            cohort_manifest_path=manifest_path,
            cohort_manifest_sha256=hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
            cohort_manifest_hash=str(manifest["manifest_hash"]),
            source_commit="a" * 40,
            governance_semantic_hash="b" * 64,
            governance_yaml_sha256="c" * 64,
            authorization_root=tmp_path / "auth",
            access_ledger_path=ledger,
        )
        set_kv(conn, "remaining_databento_budget_usd", 70.0)
        monkeypatch.setattr(
            "hydra.mission.controller.check_action_allowed", lambda *args, **kwargs: None
        )
        exact = {
            "experiment_type": "q4_atomic_one_shot",
            "q4_access_allowed": True,
            "q4_one_shot": True,
            "live_or_broker_allowed": False,
            "max_attempts": 1,
            "authorization_token": issued.token,
            "authorization_path": issued.authorization_path,
            "authorization_hash": issued.authorization_hash,
            "authorization_token_id": issued.token_id,
            "cohort_manifest_hash": manifest["manifest_hash"],
            "code_commit": "a" * 40,
            "access_ledger_path": str(ledger),
            "data_cost": 0.1,
            "paid_data_allowed": True,
        }
        controller._check_experiment_allowed(conn, exact)
        with pytest.raises(Exception):
            controller._check_experiment_allowed(
                conn,
                {
                    "experiment_type": "unrelated",
                    "q4_access_allowed": True,
                    "data_cost": 0.0,
                },
            )
    finally:
        conn.close()
