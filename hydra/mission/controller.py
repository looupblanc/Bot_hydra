from __future__ import annotations

import json
import signal
import time
from dataclasses import asdict, dataclass
from typing import Any

from hydra.calibration.validator_benchmark import benchmark_validator, write_calibration_report
from hydra.governance.kernel import initialize_governance_kernel
from hydra.mission.engineering_runner import detect_engineering_capability
from hydra.mission.experiment_queue import queue_size
from hydra.mission.mission_state import (
    append_event,
    clear_stop,
    connect_state,
    get_kv,
    mission_lock,
    mission_paths,
    set_kv,
    state_snapshot,
    stop_requested,
    write_heartbeat,
)
from hydra.mission.planner import plan_next_action
from hydra.mission.reporting import write_mission_checkpoint, write_mission_summary
from hydra.mission.research_memory import record_decision, record_engineering, record_evidence
from hydra.mission.safety_governor import check_action_allowed
from hydra.utils.time import utc_now_iso


CONTROLLER_VERSION = "autonomous_mission_controller_v1"


@dataclass(frozen=True)
class MissionControllerConfig:
    mission_id: str
    baseline_commit: str
    objective_config: str
    remaining_databento_budget_usd: float
    workers: int = 3
    checkpoint_every_minutes: float = 20.0
    persistent: bool = True
    resume: bool = True
    no_live_trading: bool = True
    state_dir: str = "mission/state"
    sleep_seconds: float = 15.0
    max_cycles: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class AutonomousMissionController:
    def __init__(self, config: MissionControllerConfig) -> None:
        self.config = config
        self.paths = mission_paths(config.state_dir)
        self._shutdown = False

    def run(self) -> int:
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)
        with mission_lock(self.paths):
            conn = connect_state(self.paths)
            try:
                self._initialize(conn)
                cycle_limit = self.config.max_cycles
                cycles_this_process = 0
                while not self._shutdown:
                    if stop_requested(self.paths):
                        self._stop_cleanly(conn, "manual_stop_file")
                        return 0
                    snapshot = state_snapshot(conn)
                    action = plan_next_action(snapshot)
                    check_action_allowed(action, baseline_commit=self.config.baseline_commit, remaining_budget_usd=self.config.remaining_databento_budget_usd)
                    record_decision(self.paths, {"mission_id": self.config.mission_id, "cycle": snapshot.get("cycle_count", 0), "selected_action": action})
                    append_event(conn, "selected_action", action)
                    self._execute_action(conn, action)
                    cycles_this_process += 1
                    set_kv(conn, "cycle_count", int(get_kv(conn, "cycle_count", 0)) + 1)
                    if self._checkpoint_due(conn, action):
                        checkpoint = str(self._checkpoint(conn))
                    else:
                        checkpoint = str(get_kv(conn, "last_successful_checkpoint", ""))
                    heartbeat = self._heartbeat_payload(conn, current_action=action, checkpoint=checkpoint)
                    write_heartbeat(self.paths, heartbeat)
                    if cycle_limit is not None and cycles_this_process >= cycle_limit:
                        set_kv(conn, "last_process_status", "max_cycles_reached")
                        return 0
                    if not self.config.persistent:
                        return 0
                    time.sleep(self.config.sleep_seconds)
                self._stop_cleanly(conn, "signal")
                return 0
            finally:
                conn.close()

    def _initialize(self, conn: Any) -> None:
        self.paths.state_dir.mkdir(parents=True, exist_ok=True)
        clear_stop(self.paths)
        if get_kv(conn, "mission_id") is None:
            set_kv(conn, "mission_id", self.config.mission_id)
            set_kv(conn, "objective_version", self.config.objective_config)
            set_kv(conn, "current_phase", "PHASE_0_GOVERNANCE")
            set_kv(conn, "cycle_count", 0)
            set_kv(conn, "validated_mechanisms", 0)
            set_kv(conn, "validated_strategies", 0)
            set_kv(conn, "executable_baskets", 0)
            set_kv(conn, "remaining_databento_budget_usd", self.config.remaining_databento_budget_usd)
        set_kv(conn, "service_state", "RUNNING")
        set_kv(conn, "stop_reason", None)
        governance = initialize_governance_kernel(
            baseline_commit=self.config.baseline_commit,
            remaining_budget_usd=self.config.remaining_databento_budget_usd,
        )
        set_kv(conn, "governance_manifest_hash", governance.manifest_hash)
        set_kv(conn, "governance_kernel_path", governance.manifest_path)
        set_kv(conn, "governance_passed", governance.result.passed)
        engineering = detect_engineering_capability()
        set_kv(conn, "autonomous_engineering_capability", engineering.to_dict())
        record_engineering(self.paths, {"mission_id": self.config.mission_id, "capability": engineering.to_dict()})
        append_event(conn, "controller_initialized", {"version": CONTROLLER_VERSION, "config": self.config.to_dict()})

    def _execute_action(self, conn: Any, action: dict[str, Any]) -> None:
        action_type = action.get("action_type")
        set_kv(conn, "current_action", action)
        if action_type == "RUN_VALIDATOR_CALIBRATION":
            set_kv(conn, "current_phase", "PHASE_2_VALIDATOR_CALIBRATION")
            result = benchmark_validator(previous_report="reports/edge_atom_lab/edge_atom_lab_20260710T101052+0000_edge_atom_discovery_replication_v1_final_corrected.md")
            report_path = write_calibration_report(result)
            set_kv(conn, "validator_calibration_passed", result.passed)
            set_kv(conn, "validator_calibration_report", str(report_path))
            set_kv(conn, "false_positive_rate", result.false_positive_rate)
            set_kv(conn, "power_on_meaningful_effects", result.power_on_meaningful_effects)
            set_kv(conn, "previous_zero_pass_cause", result.zero_pass_diagnosis.get("cause"))
            record_evidence(self.paths, {"scope": "VALIDATOR_CALIBRATION", "result": result.to_dict(), "report_path": str(report_path)})
            if result.passed:
                set_kv(conn, "milestone", "M1_VALIDATOR_CALIBRATED")
            else:
                set_kv(conn, "current_blocker", "VALIDATOR_CALIBRATION_FAILED")
        elif action_type == "AUDIT_ZERO_PASS_RESULT":
            set_kv(conn, "current_phase", "PHASE_3_REASSESS_OLD_ATOM_BATCH")
            zero_pass = benchmark_validator(previous_report="reports/edge_atom_lab/edge_atom_lab_20260710T101052+0000_edge_atom_discovery_replication_v1_final_corrected.md").zero_pass_diagnosis
            set_kv(conn, "zero_pass_audited", True)
            set_kv(conn, "previous_atom_decisions_affected", zero_pass.get("cause") in {"MULTIPLE_CAUSES_COST_HURDLE_AND_OVERSTRICT_ATTACK_POLICY", "OVERSTRICT_OR_UNCALIBRATED_MANDATORY_ATTACK_POLICY"})
            record_evidence(self.paths, {"scope": "ZERO_PASS_AUDIT", "result": zero_pass})
        elif action_type == "PLAN_BOUNDED_RETESTS":
            set_kv(conn, "current_phase", "PHASE_4_AUTONOMOUS_RESEARCH_PLANNING")
            set_kv(
                conn,
                "first_autonomous_experiment_selected",
                {
                    "experiment": "calibration_affected_atom_retest_design",
                    "status": "QUEUED_FOR_NEXT_IMPLEMENTATION_CYCLE",
                    "reason": action.get("rationale"),
                    "constraints": "new atom IDs, no inherited results, no Q4, bounded by information value",
                },
            )
            set_kv(conn, "bounded_retest_plan_written", True)
        else:
            set_kv(conn, "current_phase", "WAITING_FOR_NEXT_ACTION")

    def _checkpoint_due(self, conn: Any, action: dict[str, Any]) -> bool:
        if action.get("action_type") != "WAIT":
            return True
        last_epoch = get_kv(conn, "last_checkpoint_epoch", None)
        if last_epoch is None:
            return True
        interval_seconds = max(self.config.checkpoint_every_minutes, 0.1) * 60
        return (time.time() - float(last_epoch)) >= interval_seconds

    def _checkpoint(self, conn: Any) -> Any:
        snapshot = state_snapshot(conn)
        payload = {
            "mission_id": self.config.mission_id,
            "checkpoint_at_utc": utc_now_iso(),
            "snapshot": snapshot,
            "queue_size": queue_size(conn),
        }
        path = write_mission_checkpoint(self.config.mission_id, payload)
        write_mission_summary(self.config.mission_id, payload)
        set_kv(conn, "last_successful_checkpoint", str(path))
        set_kv(conn, "last_checkpoint_epoch", time.time())
        return path

    def _heartbeat_payload(self, conn: Any, *, current_action: dict[str, Any], checkpoint: str) -> dict[str, Any]:
        snapshot = state_snapshot(conn)
        return {
            "mission_id": self.config.mission_id,
            "controller_version": CONTROLLER_VERSION,
            "current_phase": snapshot.get("current_phase"),
            "current_action": current_action,
            "cycle_count": snapshot.get("cycle_count", 0),
            "queue_size": queue_size(conn),
            "validated_mechanisms": snapshot.get("validated_mechanisms", 0),
            "validated_strategies": snapshot.get("validated_strategies", 0),
            "executable_baskets": snapshot.get("executable_baskets", 0),
            "current_blocker": snapshot.get("current_blocker"),
            "remaining_databento_budget_usd": self.config.remaining_databento_budget_usd,
            "q4_access_count": 0,
            "latest_checkpoint": checkpoint,
            "latest_commit": self.config.baseline_commit,
            "last_error": snapshot.get("last_error"),
        }

    def _stop_cleanly(self, conn: Any, reason: str) -> None:
        set_kv(conn, "stop_reason", reason)
        set_kv(conn, "service_state", "STOPPED_CLEANLY")
        self._checkpoint(conn)
        write_heartbeat(self.paths, self._heartbeat_payload(conn, current_action={"action_type": "STOPPED", "reason": reason}, checkpoint=str(get_kv(conn, "last_successful_checkpoint"))))

    def _handle_signal(self, _signum: int, _frame: Any) -> None:
        self._shutdown = True


def run_controller(config: MissionControllerConfig) -> int:
    return AutonomousMissionController(config).run()
