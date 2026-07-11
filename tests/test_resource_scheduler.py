from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from hydra.mission.controller import (
    AutonomousMissionController,
    CleanWorkerInterruption,
    MissionControllerConfig,
)
from hydra.mission.experiment_queue import (
    enqueue_experiment,
    experiment_record,
)
from hydra.mission.mission_state import connect_state, mission_paths, set_kv
from hydra.pipelines.resource_scheduler import claim_parallel_batch


def _timed_parallel_worker(experiment: dict[str, object], result_path: str) -> None:
    started = time.time()
    time.sleep(0.40)
    completed = time.time()
    payload = {
        "ok": True,
        "experiment_id": experiment["experiment_id"],
        "specification_hash": experiment["specification_hash"],
        "result": {
            "worker": experiment["experiment_id"],
            "started": started,
            "completed": completed,
        },
    }
    Path(result_path).parent.mkdir(parents=True, exist_ok=True)
    Path(result_path).write_text(json.dumps(payload), encoding="utf-8")


def _controller(tmp_path: Path) -> tuple[AutonomousMissionController, object, object]:
    paths = mission_paths(str(tmp_path / "state"))
    conn = connect_state(paths)
    config = MissionControllerConfig(
        mission_id="parallel-test",
        baseline_commit="test",
        objective_config="test",
        remaining_databento_budget_usd=70.0,
        workers=3,
        persistent=False,
        state_dir=str(paths.state_dir),
        sleep_seconds=0.0,
    )
    return AutonomousMissionController(config), conn, paths


def test_claims_one_parallel_safe_experiment_per_pipeline(tmp_path: Path) -> None:
    controller, conn, _paths = _controller(tmp_path)
    del controller
    try:
        enqueue_experiment(
            conn,
            "discovery-a",
            {
                "experiment_type": "calibration_affected_atom_retest_design",
                "priority": 100,
                "pipeline": "DISCOVERY",
                "parallel_safe": True,
                "writes_data_access_ledger": True,
            },
        )
        enqueue_experiment(
            conn,
            "discovery-b",
            {
                "experiment_type": "calibration_affected_atom_retest_design",
                "priority": 99,
                "pipeline": "DISCOVERY",
                "parallel_safe": True,
            },
        )
        enqueue_experiment(
            conn,
            "promotion",
            {
                "experiment_type": "calibration_affected_atom_retest_execution",
                "priority": 98,
                "pipeline": "PROMOTION",
                "parallel_safe": True,
            },
        )
        enqueue_experiment(
            conn,
            "meta",
            {
                "experiment_type": "calibration_affected_atom_retest_design",
                "priority": 97,
                "pipeline": "META_RESEARCH",
                "parallel_safe": True,
            },
        )

        plan = claim_parallel_batch(conn, claimed_by="test", worker_limit=3)

        assert [row["experiment_id"] for row in plan.experiments] == [
            "discovery-a",
            "promotion",
            "meta",
        ]
        assert len(set(plan.pipelines)) == 3
        assert experiment_record(conn, "discovery-b")["status"] == "QUEUED"
        assert experiment_record(conn, "discovery-b")["attempt_count"] == 0
    finally:
        conn.close()


def test_parallel_batch_allows_only_one_data_access_writer(tmp_path: Path) -> None:
    _controller_instance, conn, _paths = _controller(tmp_path)
    try:
        for experiment_id, pipeline, writer, priority in (
            ("data-a", "DISCOVERY", True, 100),
            ("data-b", "PROMOTION", True, 99),
            ("meta", "META_RESEARCH", False, 98),
        ):
            enqueue_experiment(
                conn,
                experiment_id,
                {
                    "experiment_type": "calibration_affected_atom_retest_design",
                    "priority": priority,
                    "pipeline": pipeline,
                    "parallel_safe": True,
                    "writes_data_access_ledger": writer,
                },
            )

        plan = claim_parallel_batch(conn, claimed_by="test", worker_limit=3)

        assert [row["experiment_id"] for row in plan.experiments] == [
            "data-a",
            "meta",
        ]
        assert experiment_record(conn, "data-b")["status"] == "QUEUED"
    finally:
        conn.close()


def test_workers_have_overlapping_lifetimes_and_isolated_results(tmp_path: Path) -> None:
    controller, conn, _paths = _controller(tmp_path)
    try:
        for experiment_id, pipeline in (
            ("parallel-a", "DISCOVERY"),
            ("parallel-b", "META_RESEARCH"),
        ):
            enqueue_experiment(
                conn,
                experiment_id,
                {
                    "experiment_type": "calibration_affected_atom_retest_design",
                    "priority": 100,
                    "pipeline": pipeline,
                    "parallel_safe": True,
                },
            )
        plan = claim_parallel_batch(conn, claimed_by="test", worker_limit=3)

        outcomes = controller._run_parallel_experiment_workers(
            conn,
            list(plan.experiments),
            worker_entrypoint=_timed_parallel_worker,
        )
        assert set(outcomes) == {"parallel-a", "parallel-b"}
        assert all(row["ok"] for row in outcomes.values())
        assert {row["result"]["worker"] for row in outcomes.values()} == {
            "parallel-a",
            "parallel-b",
        }
        intervals = [row["result"] for row in outcomes.values()]
        assert max(row["started"] for row in intervals) < min(
            row["completed"] for row in intervals
        )
    finally:
        conn.close()


def test_controller_commits_parallel_results_with_one_writer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    controller, conn, _paths = _controller(tmp_path)
    monkeypatch.setattr(
        "hydra.mission.controller.check_action_allowed", lambda *_args, **_kwargs: None
    )
    monkeypatch.setattr(controller, "_tick_shadow_pipeline", lambda _conn: {})
    monkeypatch.setattr(controller, "_reconcile_completed_experiments", lambda _conn: None)
    try:
        set_kv(conn, "current_phase", "PLANNING_NEXT_ACTION")
        for experiment_id, experiment_type, pipeline in (
            (
                "parallel-design",
                "calibration_affected_atom_retest_design",
                "DISCOVERY",
            ),
            (
                "parallel-execution",
                "calibration_affected_atom_retest_execution",
                "PROMOTION",
            ),
        ):
            enqueue_experiment(
                conn,
                experiment_id,
                {
                    "experiment_type": experiment_type,
                    "priority": 100,
                    "pipeline": pipeline,
                    "parallel_safe": True,
                    "q4_access_allowed": False,
                    "paid_data_allowed": False,
                },
            )
        monkeypatch.setattr(
            controller,
            "_run_parallel_experiment_workers",
            lambda _conn, experiments: {
                row["experiment_id"]: {
                    "ok": True,
                    "result": {"scientific_conclusion": row["experiment_id"]},
                }
                for row in experiments
            },
        )

        action, progressed = controller.step(conn)

        assert progressed
        assert action["action_type"] == "RUN_PARALLEL_EXPERIMENT_BATCH"
        assert experiment_record(conn, "parallel-design")["status"] == "COMPLETED"
        assert experiment_record(conn, "parallel-execution")["status"] == "COMPLETED"
        assert json.loads(
            conn.execute(
                "SELECT value FROM kv WHERE key='active_experiments'"
            ).fetchone()[0]
        ) == []
        assert conn.execute(
            "SELECT COUNT(*) FROM experiments WHERE status='RUNNING'"
        ).fetchone()[0] == 0
    finally:
        conn.close()


def test_clean_parallel_interruption_releases_every_claim_without_retry_cost(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    controller, conn, _paths = _controller(tmp_path)
    monkeypatch.setattr(
        "hydra.mission.controller.check_action_allowed", lambda *_args, **_kwargs: None
    )
    monkeypatch.setattr(controller, "_tick_shadow_pipeline", lambda _conn: {})
    try:
        for experiment_id, pipeline in (
            ("clean-a", "DISCOVERY"),
            ("clean-b", "META_RESEARCH"),
        ):
            enqueue_experiment(
                conn,
                experiment_id,
                {
                    "experiment_type": "calibration_affected_atom_retest_design",
                    "priority": 100,
                    "pipeline": pipeline,
                    "parallel_safe": True,
                    "max_attempts": 2,
                },
            )
        monkeypatch.setattr(
            controller,
            "_run_parallel_experiment_workers",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                CleanWorkerInterruption("clean batch stop")
            ),
        )

        controller._execute_queued_experiment_batch(conn)

        for experiment_id in ("clean-a", "clean-b"):
            record = experiment_record(conn, experiment_id)
            assert record["status"] == "QUEUED"
            assert record["attempt_count"] == 0
            assert record["claim_token"] is None
        assert json.loads(
            conn.execute(
                "SELECT value FROM kv WHERE key='active_experiments'"
            ).fetchone()[0]
        ) == []
    finally:
        conn.close()
