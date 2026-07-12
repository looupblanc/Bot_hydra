from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from hydra.governance.cohort_authorization import issue_cohort_authorization
from hydra.mission.controller import (
    AutonomousMissionController,
    DECISION_BRIDGE_V4_PREPARE_EXPERIMENT_ID,
    DECISION_BRIDGE_V4_Q4_EXPERIMENT_ID,
    MissionControllerConfig,
)
from hydra.mission.experiment_queue import (
    claim_next_experiment,
    complete_experiment,
    enqueue_experiment,
    experiment_record,
)
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


def test_serial_executor_does_not_reject_validated_atomic_q4_twice(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    controller, conn = _controller(tmp_path)
    try:
        enqueue_experiment(
            conn,
            "q4_serial_guard_regression",
            {
                "experiment_type": "q4_atomic_one_shot",
                "priority": 100.0,
                "max_attempts": 1,
                "q4_access_allowed": True,
                "paid_data_allowed": True,
                "live_or_broker_allowed": False,
            },
        )
        monkeypatch.setattr(
            controller, "_check_experiment_allowed", lambda _conn, _row: None
        )
        monkeypatch.setattr(
            controller,
            "_run_experiment_with_heartbeat",
            lambda _conn, _row: {
                "scientific_conclusion": "Q4_SERIAL_GUARD_REGRESSION_PASSED",
                "result_hash": "a" * 64,
            },
        )
        monkeypatch.setattr(
            controller, "_reconcile_completed_experiments", lambda _conn: None
        )

        controller._execute_queued_experiment(conn)

        record = experiment_record(conn, "q4_serial_guard_regression")
        assert record is not None
        assert record["status"] == "COMPLETED"
        assert record["last_error"] is None
    finally:
        conn.close()


@pytest.mark.parametrize(
    ("experiment_id", "experiment_type", "route_name"),
    [
        (
            DECISION_BRIDGE_V4_PREPARE_EXPERIMENT_ID,
            "decision_bridge_v4_prepare",
            "_route_decision_bridge_v4_preparation_result",
        ),
        (
            DECISION_BRIDGE_V4_Q4_EXPERIMENT_ID,
            "q4_atomic_one_shot",
            "_route_decision_bridge_v4_q4_result",
        ),
    ],
)
def test_completed_bridge_types_have_metadata_and_scientific_routes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    experiment_id: str,
    experiment_type: str,
    route_name: str,
) -> None:
    controller, conn = _controller(tmp_path)
    try:
        enqueue_experiment(
            conn,
            experiment_id,
            {"experiment_type": experiment_type, "priority": 1.0},
        )
        claimed = claim_next_experiment(conn)
        assert claimed is not None
        complete_experiment(
            conn,
            experiment_id,
            {"scientific_conclusion": "TEST_ROUTE", "result_hash": "a" * 64},
            claim_token=str(claimed["claim_token"]),
        )
        routed: list[str] = []
        monkeypatch.setattr(
            controller,
            route_name,
            lambda _conn, _result: routed.append(experiment_type),
        )
        controller._reconcile_completed_experiments(conn)
        assert routed == [experiment_type]
    finally:
        conn.close()


def test_preparation_route_enqueues_q4_with_real_mission_db_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    controller, conn = _controller(tmp_path)
    try:
        commit = controller._git_commit()
        manifest: dict[str, object] = {
            "schema": "hydra_final_q4_cohort_v4",
            "cohort_id": "cohort_route_test",
            "candidate_ids": ["a", "b", "c"],
            "candidate_count": 3,
            "candidates": [{"candidate_id": value} for value in ("a", "b", "c")],
            "source_commit": commit,
            "q4_access_count_before": 0,
            "q4_access_authorized": False,
        }
        manifest["manifest_hash"] = stable_hash(manifest)
        output = tmp_path / "prepared"
        output.mkdir()
        manifest_path = output / "manifest.json"
        manifest_path.write_text(json.dumps(manifest) + "\n", encoding="utf-8")
        package_paths = {}
        package_hashes = {}
        for candidate_id in ("a", "b", "c"):
            package_hashes[candidate_id] = f"{candidate_id[0]}" * 64
            package = output / f"{candidate_id}.json"
            package.write_text(
                json.dumps({"package_hash": package_hashes[candidate_id]}) + "\n"
            )
            package_paths[candidate_id] = str(package)
        result: dict[str, object] = {
            "schema": "hydra_decision_bridge_v4_preparation",
            "scientific_conclusion": "FINAL_Q4_COHORT_AND_SHADOW_PACKAGES_FROZEN_Q4_UNOPENED",
            "cohort_id": manifest["cohort_id"],
            "cohort_manifest_path": str(manifest_path),
            "cohort_manifest_sha256": hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
            "cohort_manifest_hash": manifest["manifest_hash"],
            "candidate_ids": ["a", "b", "c"],
            "candidate_count": 3,
            "candidate_roles": {},
            "shadow_package_paths": package_paths,
            "shadow_package_dossier_paths": {},
            "shadow_package_hashes": package_hashes,
            "q4_access_count": 0,
            "q4_access_authorized": False,
            "paper_shadow_ready": 0,
            "report_path": str(output / "report.md"),
            "source_commit": commit,
        }
        result["result_hash"] = stable_hash(result)
        monkeypatch.setattr(
            "hydra.mission.controller.build_q4_data_plan",
            lambda *args, **kwargs: {
                "schema": "test_plan",
                "official_total_estimated_cost_usd": 0.1,
            },
        )
        monkeypatch.setattr(
            "hydra.mission.controller.issue_cohort_authorization",
            lambda **kwargs: SimpleNamespace(
                authorization_path=str(tmp_path / "authorization.json"),
                authorization_hash="1" * 64,
                token="token",
                token_id="token-id",
                token_sha256="2" * 64,
            ),
        )
        monkeypatch.setattr(
            "hydra.mission.controller.governance_semantic_hash", lambda: "3" * 64
        )
        controller._route_decision_bridge_v4_preparation_result(conn, result)
        q4 = experiment_record(conn, DECISION_BRIDGE_V4_Q4_EXPERIMENT_ID)
        assert q4 is not None and q4["status"] == "QUEUED"
        assert q4["specification"]["mission_db_path"] == str(
            controller.paths.db_path
        )
        assert q4["specification"]["q4_access_allowed"] is True
        assert q4["specification"]["max_attempts"] == 1
    finally:
        conn.close()
