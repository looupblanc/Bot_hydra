from __future__ import annotations

import json
import signal
import sqlite3
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from hydra.mission.controller import (
    AutonomousMissionController,
    CleanWorkerInterruption,
    CONTRACT_MAP_REPAIR_EXPERIMENT_ID,
    CROSS_ASSET_DAILY_EXPERIMENT_ID,
    CROSS_ASSET_DAILY_SHADOW_CANDIDATE_ID,
    CROSS_ASSET_DAILY_SHADOW_EXPERIMENT_ID,
    DESIGN_EXPERIMENT_ID,
    ENERGY_METALS_BARRIER_PRIMARY_EXPERIMENT_ID,
    ENERGY_METALS_SESSION_GEOMETRY_EXPERIMENT_ID,
    EXECUTION_EXPERIMENT_ID,
    GC_SESSION_GEOMETRY_FRESH_CHILD_ID,
    GC_SESSION_GEOMETRY_FRESH_EXPERIMENT_ID,
    MissionControllerConfig,
    POST_RETEST_DESIGN_EXPERIMENT_ID,
    POST_RETEST_PILOT_EXPERIMENT_ID,
    SESSION_GEOMETRY_MICRO_CHILD_ID,
    SESSION_GEOMETRY_MICRO_REPAIR_EXPERIMENT_ID,
    SESSION_GEOMETRY_MICRO_SHADOW_EXPERIMENT_ID,
    SESSION_GEOMETRY_PARENT_RESULT_HASH,
    SHADOW_SHARED_ACCOUNT_BASKETS_EXPERIMENT_ID,
    V3_DESIGN_EXPERIMENT_ID,
    V3_EXECUTION_EXPERIMENT_ID,
)
from hydra.mission.experiment_queue import (
    ExperimentSpecificationConflict,
    block_experiment,
    claim_next_experiment,
    complete_experiment,
    enqueue_experiment,
    experiment_counts,
    experiment_record,
    ensure_experiment_schema,
    fail_experiment,
    recover_running_experiments,
)
from hydra.mission.mission_state import (
    connect_state,
    connect_state_readonly,
    get_kv,
    mission_paths,
    set_kv,
)
from hydra.mission.watchdog import HeartbeatStatus, scheduler_health


def _blocking_spawn_worker(_experiment: dict[str, object], _result_path: str) -> None:
    time.sleep(60.0)


def _connection(tmp_path: Path) -> tuple[sqlite3.Connection, object]:
    paths = mission_paths(str(tmp_path / "state"))
    return connect_state(paths), paths


def _config(state_dir: str) -> MissionControllerConfig:
    return MissionControllerConfig(
        mission_id="test_mission",
        baseline_commit="test",
        objective_config="test",
        remaining_databento_budget_usd=77.0,
        persistent=False,
        state_dir=state_dir,
        sleep_seconds=0.0,
    )


def test_enqueue_is_idempotent_and_specification_is_immutable(tmp_path: Path) -> None:
    conn, _paths = _connection(tmp_path)
    try:
        spec = {"experiment_type": "control", "priority": 1.0, "value": 7}
        assert enqueue_experiment(conn, "exp-1", spec)
        assert not enqueue_experiment(conn, "exp-1", dict(spec))
        with pytest.raises(ExperimentSpecificationConflict):
            enqueue_experiment(conn, "exp-1", {**spec, "value": 8})
        assert experiment_counts(conn)["TOTAL"] == 1
    finally:
        conn.close()


def test_claim_complete_preserves_specification_and_result(tmp_path: Path) -> None:
    conn, _paths = _connection(tmp_path)
    try:
        spec = {"experiment_type": "control", "priority": 2.0, "immutable": True}
        enqueue_experiment(conn, "exp-1", spec)
        claimed = claim_next_experiment(conn)
        assert claimed is not None
        assert claimed["experiment_id"] == "exp-1"
        assert claimed["attempt_count"] == 1
        with pytest.raises(RuntimeError):
            complete_experiment(conn, "exp-1", {"passed": True}, claim_token="stale")
        complete_experiment(
            conn,
            "exp-1",
            {"passed": False, "reason": "null"},
            claim_token=str(claimed["claim_token"]),
        )
        row = experiment_record(conn, "exp-1")
        assert row is not None
        assert row["specification"] == spec
        assert row["result"] == {"passed": False, "reason": "null"}
        assert row["status"] == "COMPLETED"
        with pytest.raises(RuntimeError):
            complete_experiment(conn, "exp-1", {"passed": True}, claim_token=str(claimed["claim_token"]))
        assert row["claim_token"] is None
        assert row["claimed_by"] is None
        assert row["lease_expires_at"] is None
    finally:
        conn.close()


def test_failure_allows_two_retries_then_terminates(tmp_path: Path) -> None:
    conn, _paths = _connection(tmp_path)
    try:
        enqueue_experiment(conn, "exp-1", {"experiment_type": "control", "max_attempts": 3})
        expected = ["QUEUED", "QUEUED", "FAILED"]
        for status in expected:
            claimed = claim_next_experiment(conn)
            assert claimed is not None
            assert (
                fail_experiment(
                    conn,
                    "exp-1",
                    "controlled failure",
                    claim_token=str(claimed["claim_token"]),
                )
                == status
            )
        row = experiment_record(conn, "exp-1")
        assert row is not None
        assert row["attempt_count"] == 3
        assert row["status"] == "FAILED"
    finally:
        conn.close()


def test_restart_requeues_running_once_and_respects_attempt_limit(tmp_path: Path) -> None:
    conn, _paths = _connection(tmp_path)
    try:
        enqueue_experiment(conn, "retry", {"experiment_type": "control", "max_attempts": 3})
        enqueue_experiment(conn, "exhausted", {"experiment_type": "control", "max_attempts": 1})
        first = claim_next_experiment(conn)
        second = claim_next_experiment(conn)
        assert {first["experiment_id"], second["experiment_id"]} == {"retry", "exhausted"}
        recovered = recover_running_experiments(conn)
        assert recovered == {"requeued": 1, "failed": 1}
        assert recover_running_experiments(conn) == {"requeued": 0, "failed": 0}
    finally:
        conn.close()


def test_v1_experiment_row_is_additively_backfilled(tmp_path: Path) -> None:
    conn, _paths = _connection(tmp_path)
    try:
        payload = {"experiment_type": "legacy_control", "immutable": 1}
        conn.execute(
            "INSERT INTO experiments(experiment_id,status,payload,updated_at) VALUES (?,?,?,?)",
            ("legacy", "QUEUED", json.dumps(payload), "2026-01-01T00:00:00+00:00"),
        )
        conn.commit()
        ensure_experiment_schema(conn)
        row = experiment_record(conn, "legacy")
        assert row is not None
        assert row["specification"] == payload
        assert row["experiment_type"] == "legacy_control"
        assert row["created_at"] == "2026-01-01T00:00:00+00:00"
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 2
    finally:
        conn.close()


def test_legacy_plan_is_reconciled_exactly_once(tmp_path: Path) -> None:
    conn, paths = _connection(tmp_path)
    controller = AutonomousMissionController(_config(str(paths.state_dir)))
    try:
        set_kv(conn, "bounded_retest_plan_written", True)
        set_kv(
            conn,
            "first_autonomous_experiment_selected",
            {"experiment": "calibration_affected_atom_retest_design", "status": "QUEUED_FOR_NEXT_IMPLEMENTATION_CYCLE"},
        )
        controller._reconcile_legacy_plan(conn)
        controller._reconcile_legacy_plan(conn)
        assert experiment_counts(conn)["QUEUED"] == 1
        row = experiment_record(conn, DESIGN_EXPERIMENT_ID)
        assert row is not None
        assert row["specification"]["q4_access_allowed"] is False
        assert row["specification"]["paid_data_allowed"] is False
    finally:
        conn.close()


def test_empty_mission_becomes_stalled_without_wait_ledger_spam(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("hydra.mission.controller.check_action_allowed", lambda *args, **kwargs: None)
    conn, paths = _connection(tmp_path)
    controller = AutonomousMissionController(_config(str(paths.state_dir)))
    try:
        set_kv(conn, "mission_id", "test")
        set_kv(conn, "validator_calibration_passed", True)
        set_kv(conn, "zero_pass_audited", True)
        set_kv(conn, "bounded_retest_plan_written", True)
        set_kv(conn, "calibration_retest_design_completed", True)
        set_kv(conn, "calibration_retest_execution_plan_written", True)
        set_kv(conn, "current_phase", "PLANNING_NEXT_ACTION")
        first, first_progress = controller.step(conn)
        second, second_progress = controller.step(conn)
        assert not first_progress and not second_progress
        assert first["action_type"] == second["action_type"] == "WAIT"
        assert conn.execute("SELECT COUNT(*) FROM events WHERE event_type='scheduler_stalled'").fetchone()[0] == 1
        assert not paths.decision_ledger.exists()
    finally:
        conn.close()


def test_readonly_connection_cannot_mutate_state(tmp_path: Path) -> None:
    conn, paths = _connection(tmp_path)
    set_kv(conn, "proof", "unchanged")
    conn.close()
    readonly = connect_state_readonly(paths)
    try:
        assert readonly.execute("SELECT value FROM kv WHERE key='proof'").fetchone() == ('"unchanged"',)
        with pytest.raises(sqlite3.OperationalError):
            readonly.execute("INSERT INTO kv VALUES ('x','1','now')")
    finally:
        readonly.close()


def test_scheduler_health_distinguishes_progress_wait_and_stall() -> None:
    now = datetime.now(timezone.utc)
    heartbeat = HeartbeatStatus("heartbeat", True, True, 1.0, {"current_phase": "RUNNING_EXPERIMENT"})
    progressing = scheduler_health(
        heartbeat,
        {
            "governance_passed": True,
            "current_phase": "RUNNING_EXPERIMENT",
            "current_experiment": {"lease_expires_at": (now + timedelta(minutes=3)).isoformat()},
        },
        {"RUNNING": 1, "QUEUED": 0},
        now=now,
    )
    assert progressing["classification"] == "HEALTHY_AND_PROGRESSING"

    waiting = scheduler_health(
        heartbeat,
        {
            "governance_passed": True,
            "current_phase": "IDLE_SCHEDULED",
            "next_wake_at_utc": (now + timedelta(minutes=5)).isoformat(),
            "planned_action_id": "future_control",
        },
        {"RUNNING": 0, "QUEUED": 0},
        now=now,
    )
    assert waiting["classification"] == "HEALTHY_BUT_WAITING_NORMALLY"

    stalled = scheduler_health(
        heartbeat,
        {"governance_passed": True, "current_phase": "SCHEDULER_STALLED"},
        {"RUNNING": 0, "QUEUED": 0},
        now=now,
    )
    assert stalled["classification"] == "ALIVE_BUT_SCHEDULER_STALLED"

    recent_queue = scheduler_health(
        heartbeat,
        {
            "governance_passed": True,
            "service_state": "RUNNING",
            "current_phase": "PLANNING_NEXT_ACTION",
            "last_progress_at_utc": (now - timedelta(seconds=89)).isoformat(),
        },
        {"RUNNING": 0, "QUEUED": 1},
        now=now,
    )
    stale_queue = scheduler_health(
        heartbeat,
        {
            "governance_passed": True,
            "service_state": "RUNNING",
            "current_phase": "PLANNING_NEXT_ACTION",
            "last_progress_at_utc": (now - timedelta(seconds=91)).isoformat(),
        },
        {"RUNNING": 0, "QUEUED": 1},
        now=now,
    )
    assert recent_queue["classification"] == "HEALTHY_AND_PROGRESSING"
    assert stale_queue["classification"] == "ALIVE_BUT_SCHEDULER_STALLED"


def test_queued_experiment_cannot_bypass_governance_guard(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn, paths = _connection(tmp_path)
    controller = AutonomousMissionController(_config(str(paths.state_dir)))
    ran = {"value": False}
    monkeypatch.setattr(
        "hydra.mission.controller.check_action_allowed",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("guard rejected")),
    )
    monkeypatch.setattr(
        controller,
        "_run_experiment_with_heartbeat",
        lambda *_args, **_kwargs: ran.__setitem__("value", True),
    )
    try:
        enqueue_experiment(
            conn,
            "guarded",
            {"experiment_type": "future_unknown_pilot", "q4_access_allowed": False},
        )
        controller.step(conn)
        assert not ran["value"]
        assert experiment_record(conn, "guarded")["status"] == "QUEUED"
        assert json.loads(conn.execute("SELECT value FROM kv WHERE key='current_phase'").fetchone()[0]) == "INTEGRITY_BLOCKED"
    finally:
        conn.close()


def test_blocked_state_is_absorbing_and_retry_clears_current_experiment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn, paths = _connection(tmp_path)
    controller = AutonomousMissionController(_config(str(paths.state_dir)))
    try:
        set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
        action, progressed = controller.step(conn)
        assert not progressed
        assert action["action_type"] == "INTEGRITY_BLOCKED"
        assert json.loads(conn.execute("SELECT value FROM kv WHERE key='current_phase'").fetchone()[0]) == "INTEGRITY_BLOCKED"

        set_kv(conn, "current_phase", "PLANNING_NEXT_ACTION")
        monkeypatch.setattr("hydra.mission.controller.check_action_allowed", lambda *args, **kwargs: None)
        monkeypatch.setattr(
            controller,
            "_run_experiment_with_heartbeat",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("retry me")),
        )
        enqueue_experiment(
            conn,
            "retry-clear",
            {"experiment_type": "calibration_affected_atom_retest_design", "max_attempts": 3},
        )
        controller.step(conn)
        assert experiment_record(conn, "retry-clear")["status"] == "QUEUED"
        assert json.loads(conn.execute("SELECT value FROM kv WHERE key='current_experiment'").fetchone()[0]) is None
    finally:
        conn.close()


def test_blocked_state_is_preserved_across_controller_restart(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn, paths = _connection(tmp_path)
    controller = AutonomousMissionController(_config(str(paths.state_dir)))
    governance = SimpleNamespace(
        manifest_hash="test-hash",
        manifest_path="test-manifest",
        result=SimpleNamespace(
            passed=True,
            details={"cumulative_actual_databento_spend_usd": 0.0, "q4_access_count": 0},
        ),
    )
    monkeypatch.setattr("hydra.mission.controller.initialize_governance_kernel", lambda **_kwargs: governance)
    monkeypatch.setattr(
        "hydra.mission.controller.detect_engineering_capability",
        lambda: SimpleNamespace(to_dict=lambda: {}),
    )
    monkeypatch.setattr("hydra.mission.controller.record_engineering", lambda *_args, **_kwargs: None)
    try:
        set_kv(conn, "mission_id", "test_mission")
        set_kv(conn, "service_state", "RUNNING")
        set_kv(conn, "last_shutdown", "clean")
        set_kv(conn, "current_phase", "ENGINEERING_BLOCKED")
        set_kv(conn, "current_blocker", "MISSING_EXPERIMENT_HANDLER:future_type")
        set_kv(conn, "last_error", "missing handler")
        controller._initialize(conn)
        assert json.loads(conn.execute("SELECT value FROM kv WHERE key='current_phase'").fetchone()[0]) == "ENGINEERING_BLOCKED"
        assert json.loads(conn.execute("SELECT value FROM kv WHERE key='current_blocker'").fetchone()[0]) == (
            "MISSING_EXPERIMENT_HANDLER:future_type"
        )
        assert json.loads(conn.execute("SELECT value FROM kv WHERE key='last_error'").fetchone()[0]) == "missing handler"
    finally:
        conn.close()


def test_missing_handler_is_blocked_before_claim_and_auto_clears_after_install(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import hydra.mission.controller as controller_module

    conn, paths = _connection(tmp_path)
    controller = AutonomousMissionController(_config(str(paths.state_dir)))
    monkeypatch.setattr("hydra.mission.controller.check_action_allowed", lambda *args, **kwargs: None)
    governance = SimpleNamespace(
        manifest_hash="test-hash",
        manifest_path="test-manifest",
        result=SimpleNamespace(
            passed=True,
            details={"cumulative_actual_databento_spend_usd": 0.0, "q4_access_count": 0},
        ),
    )
    try:
        set_kv(conn, "mission_id", "test_mission")
        set_kv(conn, "service_state", "RUNNING")
        set_kv(conn, "last_shutdown", "clean")
        set_kv(conn, "current_phase", "PLANNING_NEXT_ACTION")
        enqueue_experiment(conn, "future", {"experiment_type": "future_pilot", "max_attempts": 3})
        action, progressed = controller.step(conn)
        row = experiment_record(conn, "future")
        assert not progressed
        assert action["action_type"] == "ENGINEERING_BLOCKED"
        assert row is not None and row["status"] == "QUEUED"
        assert row["attempt_count"] == 0

        monkeypatch.setattr(
            controller_module,
            "SUPPORTED_EXPERIMENT_TYPES",
            {*controller_module.SUPPORTED_EXPERIMENT_TYPES, "future_pilot"},
        )
        monkeypatch.setattr("hydra.mission.controller.initialize_governance_kernel", lambda **_kwargs: governance)
        monkeypatch.setattr(
            "hydra.mission.controller.detect_engineering_capability",
            lambda: SimpleNamespace(to_dict=lambda: {}),
        )
        monkeypatch.setattr("hydra.mission.controller.record_engineering", lambda *_args, **_kwargs: None)
        controller._initialize(conn)
        assert json.loads(conn.execute("SELECT value FROM kv WHERE key='current_phase'").fetchone()[0]) == (
            "PLANNING_NEXT_ACTION"
        )
        assert experiment_record(conn, "future")["status"] == "QUEUED"
    finally:
        conn.close()


def test_legacy_missing_handler_block_is_safely_requeued_after_install(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import hydra.mission.controller as controller_module

    conn, paths = _connection(tmp_path)
    controller = AutonomousMissionController(_config(str(paths.state_dir)))
    governance = SimpleNamespace(
        manifest_hash="test-hash",
        manifest_path="test-manifest",
        result=SimpleNamespace(
            passed=True,
            details={"cumulative_actual_databento_spend_usd": 0.0, "q4_access_count": 0},
        ),
    )
    try:
        set_kv(conn, "mission_id", "test_mission")
        set_kv(conn, "service_state", "RUNNING")
        set_kv(conn, "last_shutdown", "clean")
        enqueue_experiment(conn, "legacy-future", {"experiment_type": "future_pilot", "max_attempts": 3})
        claimed = claim_next_experiment(conn)
        assert claimed is not None
        block_experiment(
            conn,
            "legacy-future",
            "No approved handler for experiment type 'future_pilot'.",
            claim_token=str(claimed["claim_token"]),
        )
        set_kv(conn, "current_phase", "ENGINEERING_BLOCKED")
        set_kv(conn, "current_blocker", "MISSING_EXPERIMENT_HANDLER:future_pilot")
        set_kv(conn, "last_error", "missing")
        monkeypatch.setattr(
            controller_module,
            "SUPPORTED_EXPERIMENT_TYPES",
            {*controller_module.SUPPORTED_EXPERIMENT_TYPES, "future_pilot"},
        )
        monkeypatch.setattr("hydra.mission.controller.initialize_governance_kernel", lambda **_kwargs: governance)
        monkeypatch.setattr(
            "hydra.mission.controller.detect_engineering_capability",
            lambda: SimpleNamespace(to_dict=lambda: {}),
        )
        monkeypatch.setattr("hydra.mission.controller.record_engineering", lambda *_args, **_kwargs: None)
        controller._initialize(conn)
        row = experiment_record(conn, "legacy-future")
        assert row is not None and row["status"] == "QUEUED"
        assert row["attempt_count"] == 0
        assert json.loads(conn.execute("SELECT value FROM kv WHERE key='current_phase'").fetchone()[0]) == (
            "PLANNING_NEXT_ACTION"
        )
    finally:
        conn.close()


def test_restart_reconciles_enqueue_before_plan_flag_crash_window(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn, paths = _connection(tmp_path)
    controller = AutonomousMissionController(_config(str(paths.state_dir)))
    governance = SimpleNamespace(
        manifest_hash="test-hash",
        manifest_path="test-manifest",
        result=SimpleNamespace(
            passed=True,
            details={"cumulative_actual_databento_spend_usd": 0.0, "q4_access_count": 0},
        ),
    )
    monkeypatch.setattr("hydra.mission.controller.initialize_governance_kernel", lambda **_kwargs: governance)
    monkeypatch.setattr(
        "hydra.mission.controller.detect_engineering_capability",
        lambda: SimpleNamespace(to_dict=lambda: {}),
    )
    monkeypatch.setattr("hydra.mission.controller.record_engineering", lambda *_args, **_kwargs: None)
    try:
        set_kv(conn, "mission_id", "test_mission")
        set_kv(conn, "service_state", "RUNNING")
        set_kv(conn, "last_shutdown", "clean")
        set_kv(conn, "current_phase", "PLANNING_NEXT_ACTION")
        enqueue_experiment(
            conn,
            DESIGN_EXPERIMENT_ID,
            {"experiment_type": "calibration_affected_atom_retest_design"},
        )
        enqueue_experiment(
            conn,
            EXECUTION_EXPERIMENT_ID,
            {"experiment_type": "calibration_affected_atom_retest_execution"},
        )
        enqueue_experiment(
            conn,
            POST_RETEST_DESIGN_EXPERIMENT_ID,
            {"experiment_type": "post_calibration_retest_research_design"},
        )
        controller._initialize(conn)
        for flag in (
            "bounded_retest_plan_written",
            "calibration_retest_execution_plan_written",
            "post_retest_research_plan_written",
        ):
            assert json.loads(conn.execute("SELECT value FROM kv WHERE key=?", (flag,)).fetchone()[0]) is True
        assert experiment_counts(conn)["TOTAL"] == 3
    finally:
        conn.close()


def test_post_retest_trace_selects_design_then_explicit_engineering_block(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn, paths = _connection(tmp_path)
    controller = AutonomousMissionController(_config(str(paths.state_dir)))
    monkeypatch.setattr("hydra.mission.controller.check_action_allowed", lambda *args, **kwargs: None)
    try:
        set_kv(conn, "mission_id", "test")
        set_kv(conn, "validator_calibration_passed", True)
        set_kv(conn, "zero_pass_audited", True)
        set_kv(conn, "bounded_retest_plan_written", True)
        set_kv(conn, "calibration_retest_design_completed", True)
        set_kv(conn, "calibration_retest_execution_plan_written", True)
        set_kv(conn, "calibration_retest_execution_completed", True)
        set_kv(conn, "current_phase", "PLANNING_NEXT_ACTION")
        enqueue_experiment(
            conn,
            EXECUTION_EXPERIMENT_ID,
            {"experiment_type": "calibration_affected_atom_retest_execution"},
        )
        execution_claim = claim_next_experiment(conn)
        assert execution_claim is not None
        complete_experiment(
            conn,
            EXECUTION_EXPERIMENT_ID,
            {
                "scientific_conclusion": "ZERO_SURVIVAL_PERSISTS_UNDER_CORRECTED_RETEST_PIVOT_RESEARCH_GRAMMAR",
                "result_hash": "frozen-result-hash",
                "artifacts": {"result_json_path": "frozen-result.json"},
                "fully_validated_edge_atoms": 0,
            },
            claim_token=str(execution_claim["claim_token"]),
        )

        action, progressed = controller.step(conn)
        assert progressed and action["action_type"] == "PLAN_POST_RETEST_RESEARCH"
        assert experiment_record(conn, POST_RETEST_DESIGN_EXPERIMENT_ID)["status"] == "QUEUED"

        controller._run_experiment_with_heartbeat = lambda *_args, **_kwargs: {
            "scientific_conclusion": "POST_RETEST_BRANCH_SELECTED:ZERO_SURVIVAL_GEOMETRY_PIVOT",
            "selected_branch": "ZERO_SURVIVAL_GEOMETRY_PIVOT",
            "paths": {"report": "post.md", "engineering_task": "task.json"},
            "pilot_experiment_specification": {
                "experiment_type": "counterfactual_market_state_geometry_pilot",
                "q4_access_allowed": False,
                "paid_data_allowed": False,
            },
        }
        action, progressed = controller.step(conn)
        assert progressed and action["action_type"] == "RUN_QUEUED_EXPERIMENT"
        assert experiment_record(conn, POST_RETEST_DESIGN_EXPERIMENT_ID)["status"] == "COMPLETED"
        pilot = experiment_record(conn, POST_RETEST_PILOT_EXPERIMENT_ID)
        assert pilot is not None and pilot["status"] == "QUEUED" and pilot["attempt_count"] == 0
        assert json.loads(conn.execute("SELECT value FROM kv WHERE key='current_phase'").fetchone()[0]) == (
            "ENGINEERING_BLOCKED"
        )
        blocked_action, blocked_progress = controller.step(conn)
        assert not blocked_progress
        assert blocked_action["action_type"] == "ENGINEERING_BLOCKED"
    finally:
        conn.close()


def test_restart_reconciles_completed_post_design_to_one_queued_pilot_and_block(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn, paths = _connection(tmp_path)
    controller = AutonomousMissionController(_config(str(paths.state_dir)))
    governance = SimpleNamespace(
        manifest_hash="test-hash",
        manifest_path="test-manifest",
        result=SimpleNamespace(
            passed=True,
            details={"cumulative_actual_databento_spend_usd": 0.0, "q4_access_count": 0},
        ),
    )
    monkeypatch.setattr("hydra.mission.controller.initialize_governance_kernel", lambda **_kwargs: governance)
    monkeypatch.setattr(
        "hydra.mission.controller.detect_engineering_capability",
        lambda: SimpleNamespace(to_dict=lambda: {}),
    )
    monkeypatch.setattr("hydra.mission.controller.record_engineering", lambda *_args, **_kwargs: None)
    try:
        set_kv(conn, "mission_id", "test_mission")
        set_kv(conn, "service_state", "RUNNING")
        set_kv(conn, "last_shutdown", "unclean")
        set_kv(conn, "current_phase", "RUNNING_EXPERIMENT")
        enqueue_experiment(
            conn,
            POST_RETEST_DESIGN_EXPERIMENT_ID,
            {"experiment_type": "post_calibration_retest_research_design"},
        )
        claimed = claim_next_experiment(conn)
        assert claimed is not None
        complete_experiment(
            conn,
            POST_RETEST_DESIGN_EXPERIMENT_ID,
            {
                "scientific_conclusion": "POST_RETEST_BRANCH_SELECTED:ZERO_SURVIVAL_GEOMETRY_PIVOT",
                "selected_branch": "ZERO_SURVIVAL_GEOMETRY_PIVOT",
                "paths": {"report": "post.md", "engineering_task": "task.json"},
                "pilot_experiment_specification": {
                    "experiment_type": "counterfactual_market_state_geometry_pilot",
                    "q4_access_allowed": False,
                    "paid_data_allowed": False,
                },
            },
            claim_token=str(claimed["claim_token"]),
        )
        controller._initialize(conn)
        controller._initialize(conn)
        pilot = experiment_record(conn, POST_RETEST_PILOT_EXPERIMENT_ID)
        assert pilot is not None and pilot["status"] == "QUEUED" and pilot["attempt_count"] == 0
        assert experiment_counts(conn)["TOTAL"] == 2
        assert json.loads(conn.execute("SELECT value FROM kv WHERE key='current_phase'").fetchone()[0]) == (
            "ENGINEERING_BLOCKED"
        )
        assert json.loads(conn.execute("SELECT value FROM kv WHERE key='current_blocker'").fetchone()[0]) == (
            "MISSING_EXPERIMENT_HANDLER:counterfactual_market_state_geometry_pilot"
        )
    finally:
        conn.close()


def test_completed_experiment_reconciliation_retries_publication_after_crash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn, paths = _connection(tmp_path)
    controller = AutonomousMissionController(_config(str(paths.state_dir)))
    try:
        enqueue_experiment(
            conn,
            DESIGN_EXPERIMENT_ID,
            {"experiment_type": "calibration_affected_atom_retest_design"},
        )
        claimed = claim_next_experiment(conn)
        assert claimed is not None
        complete_experiment(
            conn,
            DESIGN_EXPERIMENT_ID,
            {
                "scientific_conclusion": "recovered design",
                "paths": {"design": "d.json", "preregistration": "p.json", "report": "r.md"},
            },
            claim_token=str(claimed["claim_token"]),
        )
        original_record_evidence = __import__("hydra.mission.controller", fromlist=["record_evidence"]).record_evidence
        monkeypatch.setattr(
            "hydra.mission.controller.record_evidence",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("ledger unavailable")),
        )
        with pytest.raises(RuntimeError, match="ledger unavailable"):
            controller._reconcile_completed_experiments(conn)
        assert json.loads(
            conn.execute("SELECT value FROM kv WHERE key='calibration_retest_design_completed'").fetchone()[0]
        ) is True
        assert not paths.evidence_ledger.exists()
        monkeypatch.setattr("hydra.mission.controller.record_evidence", original_record_evidence)
        controller._reconcile_completed_experiments(conn)
        controller._reconcile_completed_experiments(conn)
        assert conn.execute("SELECT COUNT(*) FROM events WHERE event_type='completed_experiment_reconciled'").fetchone()[0] == 1
        reconciliation_rows = [
            json.loads(line)
            for line in paths.evidence_ledger.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        assert len([row for row in reconciliation_rows if row.get("reconciliation_id")]) == 1
    finally:
        conn.close()


def test_completed_validator_integrity_pilot_is_reconciled_and_blocks_for_map_repair(
    tmp_path: Path,
) -> None:
    conn, paths = _connection(tmp_path)
    controller = AutonomousMissionController(_config(str(paths.state_dir)))
    try:
        set_kv(conn, "mission_id", "test")
        enqueue_experiment(
            conn,
            POST_RETEST_PILOT_EXPERIMENT_ID,
            {"experiment_type": "validator_integrity_repair_pilot"},
        )
        claimed = claim_next_experiment(conn)
        assert claimed is not None
        complete_experiment(
            conn,
            POST_RETEST_PILOT_EXPERIMENT_ID,
            {
                "scientific_conclusion": "CONTRACT_MAP_DATE_FLATTENING_INTEGRITY_DEFECT_CONFIRMED_NO_CANDIDATE_RERUN",
                "integrity_disposition": "CONTRACT_MAP_REBUILD_REQUIRED",
                "result_hash": "diagnostic-result",
                "report_path": "integrity.md",
                "fully_validated_edge_atoms": 0,
            },
            claim_token=str(claimed["claim_token"]),
        )
        controller._reconcile_completed_experiments(conn)
        controller._reconcile_completed_experiments(conn)
        assert json.loads(
            conn.execute(
                "SELECT value FROM kv WHERE key='validator_integrity_repair_pilot_completed'"
            ).fetchone()[0]
        ) is True
        assert json.loads(conn.execute("SELECT value FROM kv WHERE key='current_phase'").fetchone()[0]) == (
            "INTEGRITY_BLOCKED"
        )
        assert json.loads(conn.execute("SELECT value FROM kv WHERE key='current_blocker'").fetchone()[0]) == (
            "CONTRACT_MAP_REBUILD_REQUIRED"
        )
        evidence = [
            json.loads(line)
            for line in paths.evidence_ledger.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        assert len(
            [row for row in evidence if row.get("experiment_id") == POST_RETEST_PILOT_EXPERIMENT_ID]
        ) == 1
    finally:
        conn.close()


def test_confirmed_map_defect_queues_one_repair_and_completion_remains_fail_closed(
    tmp_path: Path,
) -> None:
    conn, paths = _connection(tmp_path)
    controller = AutonomousMissionController(_config(str(paths.state_dir)))
    try:
        enqueue_experiment(
            conn,
            POST_RETEST_PILOT_EXPERIMENT_ID,
            {"experiment_type": "validator_integrity_repair_pilot"},
        )
        claimed = claim_next_experiment(conn)
        assert claimed is not None
        complete_experiment(
            conn,
            POST_RETEST_PILOT_EXPERIMENT_ID,
            {
                "scientific_conclusion": "CONTRACT_MAP_DATE_FLATTENING_INTEGRITY_DEFECT_CONFIRMED_NO_CANDIDATE_RERUN",
                "integrity_disposition": "CONTRACT_MAP_REBUILD_REQUIRED",
                "result_hash": "pilot-result-hash",
                "artifacts": {"result_json_path": "pilot.json"},
                "contract_map_integrity_audit": {
                    "frozen_contract_map_path": "frozen-map.json",
                    "frozen_contract_map_sha256": "frozen-map-hash",
                    "definition_dbn_path": "definitions.dbn.zst",
                    "definition_dbn_sha256": "definition-hash",
                },
            },
            claim_token=str(claimed["claim_token"]),
        )
        assert controller._reconcile_contract_map_repair(conn)
        assert controller._reconcile_contract_map_repair(conn)
        repair = experiment_record(conn, CONTRACT_MAP_REPAIR_EXPERIMENT_ID)
        assert repair is not None
        assert repair["status"] == "QUEUED"
        assert repair["specification"]["q4_access_allowed"] is False
        assert repair["specification"]["paid_data_allowed"] is False
        assert experiment_counts(conn)["TOTAL"] == 2

        claimed_repair = claim_next_experiment(conn)
        assert claimed_repair is not None
        assert claimed_repair["experiment_id"] == CONTRACT_MAP_REPAIR_EXPERIMENT_ID
        complete_experiment(
            conn,
            CONTRACT_MAP_REPAIR_EXPERIMENT_ID,
            {
                "scientific_conclusion": "DATE_AWARE_EXPLICIT_CONTRACT_MAP_REPAIRED_AND_INTEGRITY_VALIDATED",
                "result_hash": "repair-result-hash",
                "report_path": "repair.md",
                "fully_validated_edge_atoms": 0,
            },
            claim_token=str(claimed_repair["claim_token"]),
        )
        controller._reconcile_completed_experiments(conn)
        assert json.loads(
            conn.execute("SELECT value FROM kv WHERE key='current_phase'").fetchone()[0]
        ) == "INTEGRITY_BLOCKED"
        assert json.loads(
            conn.execute("SELECT value FROM kv WHERE key='current_blocker'").fetchone()[0]
        ) == "FRESH_RETEST_WITH_REPAIRED_MAP_REQUIRED"
    finally:
        conn.close()


def test_clean_stop_restart_reconciles_map_block_before_queueing_repair(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn, paths = _connection(tmp_path)
    controller = AutonomousMissionController(_config(str(paths.state_dir)))
    governance = SimpleNamespace(
        manifest_hash="test-hash",
        manifest_path="test-manifest",
        result=SimpleNamespace(
            passed=True,
            details={"cumulative_actual_databento_spend_usd": 0.0, "q4_access_count": 0},
        ),
    )
    monkeypatch.setattr(
        "hydra.mission.controller.initialize_governance_kernel", lambda **_kwargs: governance
    )
    monkeypatch.setattr(
        "hydra.mission.controller.detect_engineering_capability",
        lambda: SimpleNamespace(to_dict=lambda: {}),
    )
    monkeypatch.setattr("hydra.mission.controller.record_engineering", lambda *_args, **_kwargs: None)
    try:
        set_kv(conn, "mission_id", "test_mission")
        set_kv(conn, "service_state", "STOPPED_CLEANLY")
        set_kv(conn, "last_shutdown", "clean")
        set_kv(conn, "current_phase", "STOPPED_CLEANLY")
        set_kv(conn, "current_blocker", "CONTRACT_MAP_REBUILD_REQUIRED")
        enqueue_experiment(
            conn,
            POST_RETEST_PILOT_EXPERIMENT_ID,
            {"experiment_type": "validator_integrity_repair_pilot"},
        )
        claimed = claim_next_experiment(conn)
        assert claimed is not None
        complete_experiment(
            conn,
            POST_RETEST_PILOT_EXPERIMENT_ID,
            {
                "scientific_conclusion": "CONTRACT_MAP_DATE_FLATTENING_INTEGRITY_DEFECT_CONFIRMED_NO_CANDIDATE_RERUN",
                "integrity_disposition": "CONTRACT_MAP_REBUILD_REQUIRED",
                "result_hash": "pilot-result-hash",
                "artifacts": {"result_json_path": "pilot.json"},
                "contract_map_integrity_audit": {
                    "frozen_contract_map_path": "frozen-map.json",
                    "frozen_contract_map_sha256": "frozen-map-hash",
                    "definition_dbn_path": "definitions.dbn.zst",
                    "definition_dbn_sha256": "definition-hash",
                },
            },
            claim_token=str(claimed["claim_token"]),
        )
        controller._initialize(conn)
        repair = experiment_record(conn, CONTRACT_MAP_REPAIR_EXPERIMENT_ID)
        assert repair is not None and repair["status"] == "QUEUED"
        assert repair["attempt_count"] == 0
        assert experiment_counts(conn)["TOTAL"] == 2
        assert json.loads(
            conn.execute("SELECT value FROM kv WHERE key='current_phase'").fetchone()[0]
        ) == "PLANNING_NEXT_ACTION"
        event = conn.execute(
            "SELECT payload FROM events WHERE event_type='controller_initialized' ORDER BY id DESC LIMIT 1"
        ).fetchone()
        assert event is not None
        assert json.loads(event[0])["contract_map_repair_queued"] is True
    finally:
        conn.close()


def test_clean_restart_queues_one_v3_design_then_one_execution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn, paths = _connection(tmp_path)
    controller = AutonomousMissionController(_config(str(paths.state_dir)))
    governance = SimpleNamespace(
        manifest_hash="test-hash",
        manifest_path="test-manifest",
        result=SimpleNamespace(
            passed=True,
            details={"cumulative_actual_databento_spend_usd": 0.0, "q4_access_count": 0},
        ),
    )
    monkeypatch.setattr(
        "hydra.mission.controller.initialize_governance_kernel", lambda **_kwargs: governance
    )
    monkeypatch.setattr(
        "hydra.mission.controller.detect_engineering_capability",
        lambda: SimpleNamespace(to_dict=lambda: {}),
    )
    monkeypatch.setattr("hydra.mission.controller.record_engineering", lambda *_args, **_kwargs: None)
    try:
        set_kv(conn, "mission_id", "test_mission")
        set_kv(conn, "service_state", "STOPPED_CLEANLY")
        set_kv(conn, "last_shutdown", "clean")
        set_kv(conn, "current_phase", "STOPPED_CLEANLY")
        set_kv(conn, "current_blocker", "FRESH_RETEST_WITH_REPAIRED_MAP_REQUIRED")

        enqueue_experiment(
            conn,
            EXECUTION_EXPERIMENT_ID,
            {"experiment_type": "calibration_affected_atom_retest_execution"},
        )
        invalid_claim = claim_next_experiment(conn)
        assert invalid_claim is not None
        complete_experiment(
            conn,
            EXECUTION_EXPERIMENT_ID,
            {
                "result_hash": "22123708ac5ce71d89a75b73d7f3b5ee03cfd87d48655f5e28e1d828ddb12de9",
                "artifacts": {"result_json_path": "invalid-v2.json"},
            },
            claim_token=str(invalid_claim["claim_token"]),
        )
        enqueue_experiment(
            conn,
            CONTRACT_MAP_REPAIR_EXPERIMENT_ID,
            {"experiment_type": "contract_map_date_aware_repair"},
        )
        repair_claim = claim_next_experiment(conn)
        assert repair_claim is not None
        complete_experiment(
            conn,
            CONTRACT_MAP_REPAIR_EXPERIMENT_ID,
            {
                "result_hash": "a932819f1eb0b72557b39ea867d3e930fd7d9e9dcad3e4cb64e10a0bbe2abb0d",
                "artifacts": {"result_json_path": "repair.json"},
                "repaired_map": {
                    "path": "repaired-map.json",
                    "sha256": "map-sha",
                    "roll_map_hash": "roll-map-hash",
                },
            },
            claim_token=str(repair_claim["claim_token"]),
        )
        set_kv(conn, "calibration_retest_execution_completed", True)
        set_kv(conn, "contract_map_date_aware_repair_completed", True)

        controller._initialize(conn)
        controller._initialize(conn)
        design = experiment_record(conn, V3_DESIGN_EXPERIMENT_ID)
        assert design is not None and design["status"] == "QUEUED"
        assert design["attempt_count"] == 0
        assert design["specification"]["q4_access_allowed"] is False
        assert design["specification"]["market_observation_read_allowed"] is False
        assert experiment_counts(conn)["TOTAL"] == 3
        assert json.loads(
            conn.execute("SELECT value FROM kv WHERE key='current_phase'").fetchone()[0]
        ) == "PLANNING_NEXT_ACTION"

        design_claim = claim_next_experiment(conn)
        assert design_claim is not None
        assert design_claim["experiment_id"] == V3_DESIGN_EXPERIMENT_ID
        complete_experiment(
            conn,
            V3_DESIGN_EXPERIMENT_ID,
            {
                "scientific_conclusion": (
                    "FRESH_V3_RETEST_PREREGISTERED_ON_DATE_AWARE_MAP_NO_EVIDENCE_INHERITED"
                ),
                "design_hash": "v3-design-hash",
                "preregistration": {"preregistration_hash": "v3-prereg-hash"},
                "paths": {"design": "v3-design.json", "preregistration": "v3-prereg.json"},
                "source": {
                    "development_data_manifest": {
                        "contract_map": {
                            "path": "repaired-map.json",
                            "sha256": "map-sha",
                            "roll_map_hash": "roll-map-hash",
                        }
                    }
                },
                "report_path": "v3-design.md",
            },
            claim_token=str(design_claim["claim_token"]),
        )
        controller._reconcile_completed_experiments(conn)
        controller._reconcile_completed_experiments(conn)
        execution = experiment_record(conn, V3_EXECUTION_EXPERIMENT_ID)
        assert execution is not None and execution["status"] == "QUEUED"
        assert execution["attempt_count"] == 0
        assert execution["specification"]["repaired_map_path"] == "repaired-map.json"
        assert experiment_counts(conn)["TOTAL"] == 4
    finally:
        conn.close()


def test_v3_zero_survival_routes_to_geometry_pivot_without_validation(tmp_path: Path) -> None:
    conn, paths = _connection(tmp_path)
    try:
        AutonomousMissionController._route_v3_execution_result(
            conn,
            {
                "scientific_conclusion": (
                    "ZERO_SURVIVAL_PERSISTS_UNDER_CORRECTED_RETEST_PIVOT_RESEARCH_GRAMMAR"
                ),
                "evidence_valid_for_decision_change": True,
                "calibration_sensitive_survivor_count": 0,
            },
        )
        assert json.loads(
            conn.execute("SELECT value FROM kv WHERE key='current_phase'").fetchone()[0]
        ) == "ENGINEERING_BLOCKED"
        assert json.loads(
            conn.execute("SELECT value FROM kv WHERE key='current_blocker'").fetchone()[0]
        ) == "V3_ZERO_SURVIVAL_GEOMETRY_PIVOT_DESIGN_REQUIRED"
        outcome = json.loads(
            conn.execute("SELECT value FROM kv WHERE key='v3_retest_outcome'").fetchone()[0]
        )
        assert outcome["calibration_sensitive_survivor_count"] == 0
    finally:
        conn.close()


def test_watchdog_rejects_failed_service_and_stale_queued_work() -> None:
    now = datetime.now(timezone.utc)
    heartbeat = HeartbeatStatus("heartbeat", True, True, 1.0, {})
    failed = scheduler_health(
        heartbeat,
        {"governance_passed": True, "service_state": "FAILED", "current_phase": "PLANNING_NEXT_ACTION"},
        {"RUNNING": 0, "QUEUED": 0},
        now=now,
    )
    assert failed["classification"] == "SERVICE_FAILED"
    stale = scheduler_health(
        heartbeat,
        {
            "governance_passed": True,
            "service_state": "RUNNING",
            "current_phase": "PLANNING_NEXT_ACTION",
            "last_progress_at_utc": (now - timedelta(hours=2)).isoformat(),
        },
        {"RUNNING": 0, "QUEUED": 1},
        now=now,
        max_queue_age_seconds=90,
    )
    assert stale["classification"] == "ALIVE_BUT_SCHEDULER_STALLED"
    for phase in ("PLANNING_NEXT_ACTION", "RECOVERING", "RETRY_SCHEDULED"):
        stale_transition = scheduler_health(
            heartbeat,
            {
                "governance_passed": True,
                "service_state": "RUNNING",
                "current_phase": phase,
                "last_progress_at_utc": (now - timedelta(hours=2)).isoformat(),
            },
            {"RUNNING": 0, "QUEUED": 0},
            now=now,
            max_transition_age_seconds=90,
        )
        assert stale_transition["classification"] == "ALIVE_BUT_SCHEDULER_STALLED"


def test_clean_worker_interruption_requeues_without_consuming_attempt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn, paths = _connection(tmp_path)
    controller = AutonomousMissionController(_config(str(paths.state_dir)))
    monkeypatch.setattr("hydra.mission.controller.check_action_allowed", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        controller,
        "_run_experiment_with_heartbeat",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(CleanWorkerInterruption("clean stop")),
    )
    try:
        enqueue_experiment(
            conn,
            "clean-stop",
            {"experiment_type": "calibration_affected_atom_retest_design", "max_attempts": 3},
        )
        controller._execute_queued_experiment(conn)
        row = experiment_record(conn, "clean-stop")
        assert row is not None
        assert row["status"] == "QUEUED"
        assert row["attempt_count"] == 0
        assert row["claim_token"] is None
        assert row["claimed_by"] is None
        assert row["lease_expires_at"] is None
        assert json.loads(conn.execute("SELECT value FROM kv WHERE key='current_experiment'").fetchone()[0]) is None
    finally:
        conn.close()


def test_real_spawn_worker_is_terminated_on_clean_signal(tmp_path: Path) -> None:
    conn, paths = _connection(tmp_path)
    controller = AutonomousMissionController(_config(str(paths.state_dir)))
    timer: threading.Timer | None = None
    try:
        enqueue_experiment(
            conn,
            "spawn-stop",
            {"experiment_type": "calibration_affected_atom_retest_design", "max_attempts": 3},
        )
        claimed = claim_next_experiment(conn)
        assert claimed is not None
        timer = threading.Timer(0.2, controller._handle_signal, args=(signal.SIGTERM, None))
        timer.start()
        with pytest.raises(CleanWorkerInterruption):
            controller._run_experiment_with_heartbeat(
                conn,
                claimed,
                worker_entrypoint=_blocking_spawn_worker,
            )
    finally:
        if timer is not None:
            timer.cancel()
            timer.join(timeout=2.0)
        conn.close()


def test_heartbeat_json_is_written_atomically(tmp_path: Path) -> None:
    from hydra.mission.mission_state import write_heartbeat

    paths = mission_paths(str(tmp_path / "state"))
    write_heartbeat(paths, {"mission_id": "test", "cycle_count": 1})
    assert json.loads(paths.heartbeat_path.read_text(encoding="utf-8"))["cycle_count"] == 1
    assert not list(paths.state_dir.glob("*.tmp"))


def test_failed_selection_null_calibration_routes_to_repair_without_name_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn, paths = _connection(tmp_path)
    controller = AutonomousMissionController(_config(str(paths.state_dir)))
    calls: list[bool] = []
    monkeypatch.setattr(
        controller,
        "_reconcile_selection_null_policy_repair",
        lambda _conn: calls.append(True) or True,
    )
    try:
        controller._route_selection_null_power_result(
            conn,
            {
                "scientific_conclusion": "SELECTION_NULL_POLICY_FALSE_POSITIVE_CONTROL_FAILED",
                "maximum_family_false_admission_rate": 0.23,
                "minimum_meaningful_effect_power_n120_plus": 0.80,
                "calibration_passed": False,
            },
        )

        assert calls == [True]
        assert get_kv(conn, "current_blocker") == "SELECTION_NULL_POLICY_REPAIR_REQUIRED"
        assert get_kv(conn, "current_phase") == "ENGINEERING_BLOCKED"
    finally:
        conn.close()


def test_calibrated_single_primary_route_is_not_overwritten_by_legacy_blockers(
    tmp_path: Path,
) -> None:
    conn, paths = _connection(tmp_path)
    controller = AutonomousMissionController(_config(str(paths.state_dir)))
    try:
        controller._route_single_primary_alpha_result(
            conn,
            {
                "calibration_passed": True,
                "selected_alpha": 0.03,
                "prospective_policy_contract": {
                    "promotion_primary_count": 1,
                    "candidate_probability_threshold": 0.03,
                },
            },
        )

        assert get_kv(conn, "current_blocker") == "NEW_SINGLE_PRIMARY_TOURNAMENT_REQUIRED"
        assert get_kv(conn, "foundry_next_planned_action")["action"] == (
            "NEW_SINGLE_PRIMARY_TOURNAMENT_REQUIRED"
        )
    finally:
        conn.close()


def test_falsified_single_primary_is_counted_once_and_routes_to_new_representation(
    tmp_path: Path,
) -> None:
    conn, paths = _connection(tmp_path)
    controller = AutonomousMissionController(_config(str(paths.state_dir)))
    result = {
        "candidate_count": 300,
        "structural_prototypes": 300,
        "round1_survivors": 71,
        "round2_survivors": 14,
        "diagnostic_archive_size": 14,
        "primary_candidate_id": "primary_v3",
        "scientific_conclusion": (
            "SINGLE_PRIMARY_CONTEXT_CONFIRMATION_FALSIFIED_OR_INSUFFICIENT"
        ),
        "promising_candidates": 0,
        "shadow_candidates": 0,
        "candidates": [
            {
                "candidate_id": "primary_v3",
                "status": "RESEARCH_PROTOTYPE",
                "topstep": {"path_candidate": False},
            }
        ],
    }
    try:
        controller._route_single_primary_context_result(conn, result)
        controller._route_single_primary_context_result(conn, result)

        assert get_kv(conn, "current_blocker") == (
            "COUNTERFACTUAL_HAZARD_PRIMARY_REQUIRED"
        )
        assert get_kv(conn, "strategies_killed") == 1
        assert get_kv(conn, "strategy_prototypes_generated") == 300
        assert get_kv(conn, "foundry_killed_candidate_ids") == ["primary_v3"]
    finally:
        conn.close()


def test_counterfactual_no_primary_routes_to_barrier_hazard_without_fake_kill(
    tmp_path: Path,
) -> None:
    conn, paths = _connection(tmp_path)
    controller = AutonomousMissionController(_config(str(paths.state_dir)))
    result = {
        "candidate_count": 96,
        "structural_prototypes": 96,
        "round1_survivors": 13,
        "round2_survivors": 2,
        "diagnostic_archive_size": 1,
        "primary_candidate_id": None,
        "scientific_conclusion": "COUNTERFACTUAL_HAZARD_NO_EARLY_PRIMARY",
        "promising_candidates": 0,
        "shadow_candidates": 0,
        "candidates": [],
    }
    try:
        controller._route_counterfactual_hazard_result(conn, result)
        controller._route_counterfactual_hazard_result(conn, result)

        assert get_kv(conn, "current_blocker") == (
            "DISTRIBUTIONAL_BARRIER_HAZARD_PRIMARY_REQUIRED"
        )
        assert get_kv(conn, "strategies_killed", 0) == 0
        assert get_kv(conn, "strategy_prototypes_generated") == 96
        assert get_kv(conn, "counterfactual_hazard_metrics")[
            "round2_survivors"
        ] == 2
    finally:
        conn.close()


def test_energy_metals_barrier_is_queued_from_ecology_blocker(
    tmp_path: Path,
) -> None:
    conn, paths = _connection(tmp_path)
    controller = AutonomousMissionController(_config(str(paths.state_dir)))
    try:
        set_kv(conn, "current_phase", "ENGINEERING_BLOCKED")
        set_kv(conn, "current_blocker", "ENERGY_METALS_ECOLOGY_SEARCH_REQUIRED")

        assert controller._reconcile_energy_metals_barrier_primary(conn)
        record = experiment_record(conn, ENERGY_METALS_BARRIER_PRIMARY_EXPERIMENT_ID)

        assert record is not None
        assert record["status"] == "QUEUED"
        assert record["experiment_type"] == "energy_metals_barrier_primary"
        assert record["specification"]["q4_access_allowed"] is False
        assert record["specification"]["paid_data_allowed"] is False
        assert record["specification"]["network_allowed"] is False
        assert get_kv(conn, "current_phase") == "PLANNING_NEXT_ACTION"
    finally:
        conn.close()


def test_energy_metals_no_primary_counts_once_and_pivots_representation(
    tmp_path: Path,
) -> None:
    conn, paths = _connection(tmp_path)
    controller = AutonomousMissionController(_config(str(paths.state_dir)))
    result = {
        "candidate_count": 48,
        "structural_prototypes": 48,
        "round1_survivors": 0,
        "round2_survivors": 0,
        "diagnostic_archive_size": 0,
        "primary_candidate_id": None,
        "scientific_conclusion": "ENERGY_METALS_BARRIER_NO_EARLY_PRIMARY",
        "promising_candidates": 0,
        "shadow_candidates": 0,
        "candidates": [],
    }
    try:
        controller._route_energy_metals_barrier_result(conn, result)
        controller._route_energy_metals_barrier_result(conn, result)

        assert get_kv(conn, "current_blocker") == (
            "ENERGY_METALS_SESSION_GEOMETRY_REQUIRED"
        )
        assert get_kv(conn, "strategy_prototypes_generated") == 48
        assert get_kv(conn, "strategies_killed", 0) == 0
    finally:
        conn.close()


def test_session_geometry_is_queued_after_energy_barrier_completion(
    tmp_path: Path,
) -> None:
    conn, paths = _connection(tmp_path)
    controller = AutonomousMissionController(_config(str(paths.state_dir)))
    try:
        enqueue_experiment(
            conn,
            ENERGY_METALS_BARRIER_PRIMARY_EXPERIMENT_ID,
            {"experiment_type": "energy_metals_barrier_primary"},
        )
        claimed = claim_next_experiment(conn)
        assert claimed is not None
        complete_experiment(
            conn,
            ENERGY_METALS_BARRIER_PRIMARY_EXPERIMENT_ID,
            {"scientific_conclusion": "ENERGY_METALS_BARRIER_NO_EARLY_PRIMARY"},
            claim_token=str(claimed["claim_token"]),
        )

        assert controller._reconcile_energy_metals_session_geometry(conn)
        record = experiment_record(
            conn, ENERGY_METALS_SESSION_GEOMETRY_EXPERIMENT_ID
        )

        assert record is not None
        assert record["status"] == "QUEUED"
        assert record["experiment_type"] == (
            "energy_metals_session_geometry_primary"
        )
        assert record["specification"]["q4_access_allowed"] is False
        assert record["specification"]["network_allowed"] is False
    finally:
        conn.close()


def test_promising_session_geometry_counts_once_and_routes_to_replication(
    tmp_path: Path,
) -> None:
    conn, paths = _connection(tmp_path)
    controller = AutonomousMissionController(_config(str(paths.state_dir)))
    result = {
        "candidate_count": 432,
        "structural_prototypes": 432,
        "round1_survivors": 129,
        "round2_survivors": 30,
        "diagnostic_archive_size": 11,
        "primary_candidate_id": "session-primary",
        "scientific_conclusion": (
            "ENERGY_METALS_SESSION_GEOMETRY_PROMISING_BUT_INSUFFICIENT"
        ),
        "promising_candidates": 1,
        "shadow_candidates": 0,
        "candidates": [
            {
                "candidate_id": "session-primary",
                "status": "PROMISING_RESEARCH_CANDIDATE",
                "mechanism_family": "overnight_inventory_transfer",
                "primary_market": "CL",
                "execution_market": "MCL",
                "net_pnl": 1992.0,
                "topstep": {"path_candidate": False},
            }
        ],
    }
    try:
        controller._route_energy_metals_session_geometry_result(conn, result)
        controller._route_energy_metals_session_geometry_result(conn, result)

        assert get_kv(conn, "current_blocker") == (
            "ENERGY_METALS_SESSION_GEOMETRY_REPLICATION_REQUIRED"
        )
        assert get_kv(conn, "strategy_prototypes_generated") == 432
        assert get_kv(conn, "strategies_killed", 0) == 0
    finally:
        conn.close()


def test_synchronized_mcl_repair_is_queued_from_frozen_parent(
    tmp_path: Path,
) -> None:
    conn, paths = _connection(tmp_path)
    controller = AutonomousMissionController(_config(str(paths.state_dir)))
    try:
        enqueue_experiment(
            conn,
            ENERGY_METALS_SESSION_GEOMETRY_EXPERIMENT_ID,
            {"experiment_type": "energy_metals_session_geometry_primary"},
        )
        claimed = claim_next_experiment(conn)
        assert claimed is not None
        complete_experiment(
            conn,
            ENERGY_METALS_SESSION_GEOMETRY_EXPERIMENT_ID,
            {
                "result_hash": SESSION_GEOMETRY_PARENT_RESULT_HASH,
                "scientific_conclusion": (
                    "ENERGY_METALS_SESSION_GEOMETRY_PROMISING_BUT_INSUFFICIENT"
                ),
            },
            claim_token=str(claimed["claim_token"]),
        )

        assert controller._reconcile_session_geometry_micro_repair(conn)
        record = experiment_record(conn, SESSION_GEOMETRY_MICRO_REPAIR_EXPERIMENT_ID)

        assert record is not None
        assert record["status"] == "QUEUED"
        assert record["experiment_type"] == "session_geometry_micro_execution_repair"
        assert record["specification"]["pipeline"] == "PROMOTION_AND_DISCOVERY"
        assert record["specification"]["q4_access_allowed"] is False
        assert record["specification"]["paid_data_allowed"] is False
        assert record["specification"]["network_allowed"] is False
        assert record["specification"]["live_or_broker_allowed"] is False
    finally:
        conn.close()


def test_synchronized_mcl_shadow_candidate_routes_once_to_activation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn, paths = _connection(tmp_path)
    controller = AutonomousMissionController(_config(str(paths.state_dir)))
    monkeypatch.setattr(
        controller, "_reconcile_session_geometry_micro_shadow", lambda _conn: True
    )
    monkeypatch.setattr(controller, "_tick_shadow_pipeline", lambda _conn: {})
    result = {
        "candidate_count": 1,
        "scientific_conclusion": (
            "SYNCHRONIZED_MCL_EXECUTION_SHADOW_CANDIDATE_FOUND"
        ),
        "promising_candidates": 1,
        "shadow_candidates": 1,
        "paper_shadow_ready": 0,
        "candidates": [
            {
                "candidate_id": SESSION_GEOMETRY_MICRO_CHILD_ID,
                "status": "SHADOW_RESEARCH_CANDIDATE",
                "mechanism_family": "overnight_inventory_transfer",
                "primary_market": "CL",
                "execution_market": "MCL",
                "net_pnl": 223.0,
                "admission": {"permits_zero_risk_shadow": True},
                "topstep": {"path_candidate": False},
            }
        ],
    }
    try:
        controller._route_session_geometry_micro_repair_result(conn, result)
        controller._route_session_geometry_micro_repair_result(conn, result)

        assert get_kv(conn, "current_blocker") == (
            "SESSION_GEOMETRY_MICRO_SHADOW_ACTIVATION_REQUIRED"
        )
        assert get_kv(conn, "strategy_prototypes_generated") == 1
        assert get_kv(conn, "shadow_candidates") == 1
        assert get_kv(conn, "paper_shadow_ready_candidates") == 0
        assert get_kv(conn, "strategies_killed", 0) == 0
    finally:
        conn.close()


def test_synchronized_mcl_activation_queue_preserves_frozen_source(
    tmp_path: Path,
) -> None:
    conn, paths = _connection(tmp_path)
    controller = AutonomousMissionController(_config(str(paths.state_dir)))
    source_path = tmp_path / "repair-result.json"
    configuration_path = tmp_path / "shadow-configuration.json"
    source_path.write_text('{"result_hash":"repair-hash"}\n', encoding="utf-8")
    configuration_path.write_text("{}\n", encoding="utf-8")
    result = {
        "result_hash": "repair-hash",
        "scientific_conclusion": (
            "SYNCHRONIZED_MCL_EXECUTION_SHADOW_CANDIDATE_FOUND"
        ),
        "candidates": [
            {
                "candidate_id": SESSION_GEOMETRY_MICRO_CHILD_ID,
                "status": "SHADOW_RESEARCH_CANDIDATE",
                "admission": {"permits_zero_risk_shadow": True},
            }
        ],
        "shadow_configurations": [
            {
                "candidate_id": SESSION_GEOMETRY_MICRO_CHILD_ID,
                "path": str(configuration_path),
                "configuration_hash": "frozen-configuration-hash",
            }
        ],
        "artifacts": {"result_json_path": str(source_path)},
    }
    try:
        enqueue_experiment(
            conn,
            SESSION_GEOMETRY_MICRO_REPAIR_EXPERIMENT_ID,
            {"experiment_type": "session_geometry_micro_execution_repair"},
        )
        claimed = claim_next_experiment(conn)
        assert claimed is not None
        complete_experiment(
            conn,
            SESSION_GEOMETRY_MICRO_REPAIR_EXPERIMENT_ID,
            result,
            claim_token=str(claimed["claim_token"]),
        )

        assert controller._reconcile_session_geometry_micro_shadow(conn)
        record = experiment_record(conn, SESSION_GEOMETRY_MICRO_SHADOW_EXPERIMENT_ID)

        assert record is not None
        assert record["status"] == "QUEUED"
        assert record["experiment_type"] == "session_geometry_micro_shadow_activation"
        assert record["specification"]["candidate_id"] == (
            SESSION_GEOMETRY_MICRO_CHILD_ID
        )
        assert record["specification"]["source_result_hash"] == "repair-hash"
        assert record["specification"]["pipeline"] == "SHADOW"
        assert record["specification"]["q4_access_allowed"] is False
        assert record["specification"]["live_or_broker_allowed"] is False
    finally:
        conn.close()


def test_synchronized_mcl_shadow_activation_is_idempotent_and_zero_order(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from hydra.mission.calibration_retest_execution import _stable_hash

    conn, paths = _connection(tmp_path)
    controller = AutonomousMissionController(_config(str(paths.state_dir)))
    monkeypatch.setattr(controller, "_tick_shadow_pipeline", lambda _conn: {})
    configuration = tmp_path / "mcl-shadow.json"
    configuration.write_text("{}\n", encoding="utf-8")
    manifest = {
        "candidate_id": SESSION_GEOMETRY_MICRO_CHILD_ID,
        "configuration_path": str(configuration),
        "configuration_sha256": "frozen-sha",
        "configuration_hash": "frozen-semantic-hash",
        "stale_data_seconds": 180,
        "outbound_orders_enabled": False,
    }
    manifest["activation_manifest_hash"] = _stable_hash(manifest)
    result = {
        "candidate_id": SESSION_GEOMETRY_MICRO_CHILD_ID,
        "candidate_count": 0,
        "scientific_conclusion": "IMMUTABLE_ZERO_ORDER_SHADOW_ACTIVATED",
        "activation_manifest": manifest,
        "candidates": [
            {
                "candidate_id": SESSION_GEOMETRY_MICRO_CHILD_ID,
                "status": "SHADOW_ACTIVE",
                "mechanism_family": "overnight_inventory_transfer",
                "primary_market": "CL",
                "execution_market": "MCL",
                "net_pnl": 223.0,
                "topstep": {"path_candidate": False},
            }
        ],
    }
    try:
        controller._route_session_geometry_micro_shadow_result(conn, result)
        controller._route_session_geometry_micro_shadow_result(conn, result)

        registry = get_kv(conn, "shadow_active_registry")
        assert list(registry) == [SESSION_GEOMETRY_MICRO_CHILD_ID]
        assert registry[SESSION_GEOMETRY_MICRO_CHILD_ID][
            "outbound_orders_enabled"
        ] is False
        assert get_kv(conn, "shadow_active_candidates") == 1
        assert get_kv(conn, "current_blocker") == (
            "GC_SESSION_GEOMETRY_FRESH_ID_REQUIRED"
        )
        assert get_kv(conn, "paper_shadow_ready_candidates") == 0
    finally:
        conn.close()


def test_fresh_gc_primary_is_queued_after_micro_shadow_activation(
    tmp_path: Path,
) -> None:
    conn, paths = _connection(tmp_path)
    controller = AutonomousMissionController(_config(str(paths.state_dir)))
    try:
        enqueue_experiment(
            conn,
            SESSION_GEOMETRY_MICRO_SHADOW_EXPERIMENT_ID,
            {"experiment_type": "session_geometry_micro_shadow_activation"},
        )
        claimed = claim_next_experiment(conn)
        assert claimed is not None
        complete_experiment(
            conn,
            SESSION_GEOMETRY_MICRO_SHADOW_EXPERIMENT_ID,
            {
                "scientific_conclusion": "IMMUTABLE_ZERO_ORDER_SHADOW_ACTIVATED",
                "candidate_id": SESSION_GEOMETRY_MICRO_CHILD_ID,
            },
            claim_token=str(claimed["claim_token"]),
        )

        assert controller._reconcile_gc_session_geometry_fresh(conn)
        record = experiment_record(conn, GC_SESSION_GEOMETRY_FRESH_EXPERIMENT_ID)

        assert record is not None
        assert record["status"] == "QUEUED"
        assert record["experiment_type"] == "gc_session_geometry_fresh_primary"
        assert record["specification"]["selection_end_exclusive"] == "2024-01-01"
        assert record["specification"]["development_end_exclusive"] == "2024-10-01"
        assert record["specification"]["q4_access_allowed"] is False
        assert record["specification"]["paid_data_allowed"] is False
        assert record["specification"]["network_allowed"] is False
        assert record["specification"]["live_or_broker_allowed"] is False
    finally:
        conn.close()


def test_concentrated_fresh_gc_primary_is_killed_once_and_pivots(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn, paths = _connection(tmp_path)
    controller = AutonomousMissionController(_config(str(paths.state_dir)))
    monkeypatch.setattr(controller, "_tick_shadow_pipeline", lambda _conn: {})
    result = {
        "candidate_count": 1,
        "scientific_conclusion": (
            "GC_SESSION_GEOMETRY_FRESH_PRIMARY_FALSIFIED_OR_INSUFFICIENT"
        ),
        "promising_candidates": 1,
        "shadow_candidates": 0,
        "paper_shadow_ready": 0,
        "candidates": [
            {
                "candidate_id": GC_SESSION_GEOMETRY_FRESH_CHILD_ID,
                "status": "PROMISING_RESEARCH_CANDIDATE",
                "mechanism_family": "overnight_inventory_reversal",
                "primary_market": "GC",
                "execution_market": "MGC",
                "events": 62,
                "net_pnl": 262.0,
                "null_evidence": {"raw_probability": 0.1308},
                "topstep": {"path_candidate": False},
            }
        ],
    }
    try:
        controller._route_gc_session_geometry_fresh_result(conn, result)
        controller._route_gc_session_geometry_fresh_result(conn, result)

        assert get_kv(conn, "current_blocker") == "CROSS_ASSET_DAILY_HORIZON_REQUIRED"
        assert get_kv(conn, "strategy_prototypes_generated") == 1
        assert get_kv(conn, "strategies_killed") == 1
        assert GC_SESSION_GEOMETRY_FRESH_CHILD_ID in get_kv(
            conn, "foundry_killed_candidate_ids"
        )
        assert get_kv(conn, "gc_session_geometry_fresh_metrics") == {
            "candidate_id": GC_SESSION_GEOMETRY_FRESH_CHILD_ID,
            "status": "PROMISING_RESEARCH_CANDIDATE",
            "events": 62,
            "net_pnl": 262.0,
            "null_probability": 0.1308,
            "conclusion": (
                "GC_SESSION_GEOMETRY_FRESH_PRIMARY_FALSIFIED_OR_INSUFFICIENT"
            ),
        }
        assert get_kv(conn, "paper_shadow_ready_candidates") == 0
    finally:
        conn.close()


def test_cross_asset_daily_tournament_is_queued_after_gc_pivot(
    tmp_path: Path,
) -> None:
    conn, paths = _connection(tmp_path)
    controller = AutonomousMissionController(_config(str(paths.state_dir)))
    try:
        enqueue_experiment(
            conn,
            GC_SESSION_GEOMETRY_FRESH_EXPERIMENT_ID,
            {"experiment_type": "gc_session_geometry_fresh_primary"},
        )
        claimed = claim_next_experiment(conn)
        assert claimed is not None
        complete_experiment(
            conn,
            GC_SESSION_GEOMETRY_FRESH_EXPERIMENT_ID,
            {
                "scientific_conclusion": (
                    "GC_SESSION_GEOMETRY_FRESH_PRIMARY_FALSIFIED_OR_INSUFFICIENT"
                )
            },
            claim_token=str(claimed["claim_token"]),
        )

        assert controller._reconcile_cross_asset_daily(conn)
        record = experiment_record(conn, CROSS_ASSET_DAILY_EXPERIMENT_ID)

        assert record is not None
        assert record["status"] == "QUEUED"
        assert record["experiment_type"] == "cross_asset_daily_horizon_primary"
        assert record["specification"]["pipeline"] == "DISCOVERY_AND_PROMOTION"
        assert record["specification"]["selection_end_exclusive"] == "2024-01-01"
        assert record["specification"]["q4_access_allowed"] is False
        assert record["specification"]["paid_data_allowed"] is False
        assert record["specification"]["network_allowed"] is False
        assert record["specification"]["live_or_broker_allowed"] is False
    finally:
        conn.close()


def test_cross_asset_daily_result_kills_weak_elites_once_and_routes_shadow(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn, paths = _connection(tmp_path)
    controller = AutonomousMissionController(_config(str(paths.state_dir)))
    monkeypatch.setattr(
        controller, "_reconcile_cross_asset_daily_shadow", lambda _conn: True
    )
    monkeypatch.setattr(controller, "_tick_shadow_pipeline", lambda _conn: {})
    candidates = []
    for index in range(8):
        status = (
            "SHADOW_RESEARCH_CANDIDATE"
            if index == 0
            else "PROMISING_RESEARCH_CANDIDATE"
            if index == 1
            else "RESEARCH_PROTOTYPE"
        )
        candidates.append(
            {
                "candidate_id": (
                    CROSS_ASSET_DAILY_SHADOW_CANDIDATE_ID
                    if index == 0
                    else f"daily-elite-{index}"
                ),
                "status": status,
                "mechanism_family": f"daily-family-{index % 4}",
                "primary_market": ("YM", "GC", "CL", "RTY")[index % 4],
                "execution_market": ("MYM", "MGC", "MCL", "M2K")[index % 4],
                "net_pnl": 500.0 - index * 100,
                "topstep": {"path_candidate": index == 1},
            }
        )
    result = {
        "candidate_count": 720,
        "structural_prototypes": 720,
        "round1_survivors": 296,
        "round2_survivors": 97,
        "elite_count": 8,
        "scientific_conclusion": "CROSS_ASSET_DAILY_SHADOW_CANDIDATES_FOUND",
        "promising_candidates": 2,
        "shadow_candidates": 1,
        "paper_shadow_ready": 0,
        "topstep_path_candidates": 1,
        "candidates": candidates,
        "selector_audit": {"uses_2024_results": False},
        "elite_candidate_ids": [row["candidate_id"] for row in candidates],
        "negative_controls": ["control-a", "control-b"],
    }
    try:
        controller._route_cross_asset_daily_result(conn, result)
        controller._route_cross_asset_daily_result(conn, result)

        assert get_kv(conn, "current_blocker") == (
            "CROSS_ASSET_DAILY_SHADOW_ACTIVATION_REQUIRED"
        )
        assert get_kv(conn, "strategy_prototypes_generated") == 720
        assert get_kv(conn, "strategies_killed") == 6
        assert get_kv(conn, "promising_candidates") == 2
        assert get_kv(conn, "shadow_candidates") == 1
        assert get_kv(conn, "topstep_path_candidates") == 1
        assert get_kv(conn, "paper_shadow_ready_candidates") == 0
        assert get_kv(conn, "quality_diversity_archive")[
            "negative_controls"
        ] == ["control-a", "control-b"]
    finally:
        conn.close()


def test_cross_asset_daily_shadow_queue_preserves_frozen_candidate(
    tmp_path: Path,
) -> None:
    conn, paths = _connection(tmp_path)
    controller = AutonomousMissionController(_config(str(paths.state_dir)))
    source_path = tmp_path / "daily-result.json"
    configuration_path = tmp_path / "daily-configuration.json"
    source_path.write_text('{"result_hash":"daily-result-hash"}\n', encoding="utf-8")
    configuration_path.write_text("{}\n", encoding="utf-8")
    result = {
        "result_hash": "daily-result-hash",
        "scientific_conclusion": "CROSS_ASSET_DAILY_SHADOW_CANDIDATES_FOUND",
        "candidates": [
            {
                "candidate_id": CROSS_ASSET_DAILY_SHADOW_CANDIDATE_ID,
                "status": "SHADOW_RESEARCH_CANDIDATE",
                "admission": {"permits_zero_risk_shadow": True},
            }
        ],
        "shadow_configurations": [
            {
                "candidate_id": CROSS_ASSET_DAILY_SHADOW_CANDIDATE_ID,
                "path": str(configuration_path),
                "configuration_hash": "daily-configuration-hash",
            }
        ],
        "artifacts": {"result_json_path": str(source_path)},
    }
    try:
        enqueue_experiment(
            conn,
            CROSS_ASSET_DAILY_EXPERIMENT_ID,
            {"experiment_type": "cross_asset_daily_horizon_primary"},
        )
        claimed = claim_next_experiment(conn)
        assert claimed is not None
        complete_experiment(
            conn,
            CROSS_ASSET_DAILY_EXPERIMENT_ID,
            result,
            claim_token=str(claimed["claim_token"]),
        )

        assert controller._reconcile_cross_asset_daily_shadow(conn)
        record = experiment_record(conn, CROSS_ASSET_DAILY_SHADOW_EXPERIMENT_ID)

        assert record is not None
        assert record["status"] == "QUEUED"
        assert record["experiment_type"] == "cross_asset_daily_shadow_activation"
        assert record["specification"]["candidate_id"] == (
            CROSS_ASSET_DAILY_SHADOW_CANDIDATE_ID
        )
        assert record["specification"]["q4_access_allowed"] is False
        assert record["specification"]["live_or_broker_allowed"] is False
    finally:
        conn.close()


def test_cross_asset_daily_shadow_activation_registers_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from hydra.mission.calibration_retest_execution import _stable_hash

    conn, paths = _connection(tmp_path)
    controller = AutonomousMissionController(_config(str(paths.state_dir)))
    monkeypatch.setattr(controller, "_tick_shadow_pipeline", lambda _conn: {})
    configuration = tmp_path / "daily-shadow.json"
    configuration.write_text("{}\n", encoding="utf-8")
    manifest = {
        "candidate_id": CROSS_ASSET_DAILY_SHADOW_CANDIDATE_ID,
        "configuration_path": str(configuration),
        "configuration_sha256": "frozen-sha",
        "configuration_hash": "frozen-configuration-hash",
        "stale_data_seconds": 75,
        "outbound_orders_enabled": False,
    }
    manifest["activation_manifest_hash"] = _stable_hash(manifest)
    result = {
        "candidate_id": CROSS_ASSET_DAILY_SHADOW_CANDIDATE_ID,
        "candidate_count": 0,
        "scientific_conclusion": "IMMUTABLE_ZERO_ORDER_SHADOW_ACTIVATED",
        "activation_manifest": manifest,
        "candidates": [
            {
                "candidate_id": CROSS_ASSET_DAILY_SHADOW_CANDIDATE_ID,
                "status": "SHADOW_ACTIVE",
                "mechanism_family": "daily_direction_transfer",
                "primary_market": "YM",
                "execution_market": "MYM",
                "net_pnl": 513.5,
                "topstep": {"path_candidate": False},
            }
        ],
    }
    try:
        controller._route_cross_asset_daily_shadow_result(conn, result)
        controller._route_cross_asset_daily_shadow_result(conn, result)

        registry = get_kv(conn, "shadow_active_registry")
        assert list(registry) == [CROSS_ASSET_DAILY_SHADOW_CANDIDATE_ID]
        assert registry[CROSS_ASSET_DAILY_SHADOW_CANDIDATE_ID][
            "outbound_orders_enabled"
        ] is False
        assert get_kv(conn, "shadow_active_candidates") == 1
        assert get_kv(conn, "current_blocker") == (
            "PORTFOLIO_BASKET_AND_DISTRIBUTIONAL_SEARCH_REQUIRED"
        )
        assert get_kv(conn, "paper_shadow_ready_candidates") == 0
    finally:
        conn.close()


def test_shared_account_baskets_queue_requires_four_active_shadows(
    tmp_path: Path,
) -> None:
    conn, paths = _connection(tmp_path)
    controller = AutonomousMissionController(_config(str(paths.state_dir)))
    required = {
        "strategy_open_gap_continuation_YM_v1",
        (
            "strategy_barrier_hazard_NQ_signed_extreme_recovery_60_middle_q65_"
            "h30_s100_15m_expansion_v1"
        ),
        SESSION_GEOMETRY_MICRO_CHILD_ID,
        CROSS_ASSET_DAILY_SHADOW_CANDIDATE_ID,
    }
    try:
        enqueue_experiment(
            conn,
            CROSS_ASSET_DAILY_SHADOW_EXPERIMENT_ID,
            {"experiment_type": "cross_asset_daily_shadow_activation"},
        )
        claimed = claim_next_experiment(conn)
        assert claimed is not None
        complete_experiment(
            conn,
            CROSS_ASSET_DAILY_SHADOW_EXPERIMENT_ID,
            {
                "scientific_conclusion": "IMMUTABLE_ZERO_ORDER_SHADOW_ACTIVATED",
                "candidate_id": CROSS_ASSET_DAILY_SHADOW_CANDIDATE_ID,
            },
            claim_token=str(claimed["claim_token"]),
        )
        set_kv(
            conn,
            "shadow_active_registry",
            {candidate_id: {"candidate_id": candidate_id} for candidate_id in required},
        )

        assert controller._reconcile_shadow_shared_account_baskets(conn)
        record = experiment_record(
            conn, SHADOW_SHARED_ACCOUNT_BASKETS_EXPERIMENT_ID
        )

        assert record is not None
        assert record["status"] == "QUEUED"
        assert record["experiment_type"] == "shadow_shared_account_baskets"
        assert record["specification"]["pipeline"] == "PORTFOLIO"
        assert len(record["specification"]["sources"]) == 4
        assert record["specification"]["q4_access_allowed"] is False
        assert record["specification"]["paid_data_allowed"] is False
        assert record["specification"]["live_or_broker_allowed"] is False
    finally:
        conn.close()


def test_three_shared_account_baskets_route_to_new_distributional_search(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn, paths = _connection(tmp_path)
    controller = AutonomousMissionController(_config(str(paths.state_dir)))
    monkeypatch.setattr(controller, "_tick_shadow_pipeline", lambda _conn: {})
    result = {
        "scientific_conclusion": "THREE_EXECUTABLE_SHADOW_BASKETS_FOUND",
        "basket_count": 3,
        "executable_baskets": 3,
        "manifest_hash": "basket-manifest-hash",
        "selected_baskets": [
            {"basket_id": f"basket-{index}", "role": f"role-{index}"}
            for index in range(3)
        ],
        "basket_configurations": [
            {
                "basket_id": f"basket-{index}",
                "role": f"role-{index}",
                "path": f"/tmp/basket-{index}.json",
                "configuration_hash": f"hash-{index}",
            }
            for index in range(3)
        ],
    }
    try:
        controller._route_shadow_shared_account_baskets_result(conn, result)

        assert get_kv(conn, "executable_baskets") == 3
        assert len(get_kv(conn, "shadow_basket_registry")) == 3
        assert all(
            not row["outbound_orders_enabled"]
            for row in get_kv(conn, "shadow_basket_registry").values()
        )
        assert get_kv(conn, "current_blocker") == (
            "DISTRIBUTIONAL_SURVIVAL_HAZARD_SEARCH_REQUIRED"
        )
        assert get_kv(conn, "paper_shadow_ready_candidates", 0) == 0
    finally:
        conn.close()


def test_barrier_shadow_candidate_routes_to_zero_order_activation(
    tmp_path: Path,
) -> None:
    conn, paths = _connection(tmp_path)
    controller = AutonomousMissionController(_config(str(paths.state_dir)))
    candidate_id = "strategy_barrier_hazard_NQ_v1"
    result = {
        "candidate_count": 144,
        "structural_prototypes": 144,
        "round1_survivors": 8,
        "round2_survivors": 4,
        "diagnostic_archive_size": 2,
        "primary_candidate_id": candidate_id,
        "scientific_conclusion": "BARRIER_HAZARD_SHADOW_CANDIDATE_FOUND",
        "promising_candidates": 1,
        "shadow_candidates": 1,
        "candidates": [
            {
                "candidate_id": candidate_id,
                "status": "SHADOW_RESEARCH_CANDIDATE",
                "admission": {"permits_zero_risk_shadow": True},
                "topstep": {"path_candidate": False},
            }
        ],
        "shadow_configurations": [],
    }
    try:
        controller._route_barrier_hazard_result(conn, result)

        assert get_kv(conn, "current_blocker") == (
            "BARRIER_HAZARD_SHADOW_ACTIVATION_REQUIRED"
        )
        assert get_kv(conn, "strategy_prototypes_generated") == 144
        assert get_kv(conn, "barrier_hazard_metrics")["round2_survivors"] == 4
        assert get_kv(conn, "shadow_candidates") == 1
    finally:
        conn.close()
