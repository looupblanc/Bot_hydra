from __future__ import annotations

import hashlib
import json
import multiprocessing
import os
import signal
import subprocess
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from hydra.calibration.validator_benchmark import benchmark_validator, write_calibration_report
from hydra.governance.kernel import initialize_governance_kernel
from hydra.mission.engineering_runner import detect_engineering_capability
from hydra.mission.experiment_queue import (
    block_experiment,
    claim_next_experiment,
    complete_experiment,
    enqueue_experiment,
    ensure_experiment_schema,
    experiment_counts,
    experiment_record,
    fail_experiment,
    peek_next_experiment,
    queue_size,
    recover_resolved_missing_handler_experiments,
    recover_running_experiments,
    release_experiment_claim_for_shutdown,
    renew_experiment_lease,
)
from hydra.mission.experiment_runner import experiment_worker_entry
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
from hydra.utils.config import project_path
from hydra.utils.time import utc_now_iso


CONTROLLER_VERSION = "autonomous_mission_controller_v2"
DESIGN_EXPERIMENT_ID = "calibration_affected_atom_retest_design_v1"
EXECUTION_EXPERIMENT_ID = "calibration_affected_atom_retest_execution_v1"
POST_RETEST_DESIGN_EXPERIMENT_ID = "post_calibration_retest_research_design_v1"
POST_RETEST_PILOT_EXPERIMENT_ID = "post_calibration_retest_pilot_v1"
CONTRACT_MAP_REPAIR_EXPERIMENT_ID = "contract_map_date_aware_repair_v1"
CONTRACT_MAP_REPAIR_TASK_SHA256 = "92c73632fbff1dcc65de99fdef11b04026189b4033505f82d739f5e7e34216b8"
SUPPORTED_EXPERIMENT_TYPES = {
    "calibration_affected_atom_retest_design",
    "calibration_affected_atom_retest_execution",
    "post_calibration_retest_research_design",
    "validator_integrity_repair_pilot",
    "contract_map_date_aware_repair",
}


class CleanWorkerInterruption(RuntimeError):
    """A controlled stop interrupted research without constituting a failed attempt."""


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
                loops_this_process = 0
                while not self._shutdown:
                    if stop_requested(self.paths):
                        self._stop_cleanly(conn, "manual_stop_file")
                        return 0
                    action, progressed = self.step(conn)
                    loops_this_process += 1
                    if self._checkpoint_due(conn, action, progressed=progressed):
                        checkpoint = str(self._checkpoint(conn))
                    else:
                        checkpoint = str(get_kv(conn, "last_successful_checkpoint", ""))
                    write_heartbeat(self.paths, self._heartbeat_payload(conn, current_action=action, checkpoint=checkpoint))
                    if self._shutdown or stop_requested(self.paths):
                        reason = "signal" if self._shutdown else "manual_stop_file"
                        self._stop_cleanly(conn, reason)
                        return 0
                    if cycle_limit is not None and loops_this_process >= cycle_limit:
                        set_kv(conn, "last_process_status", "max_cycles_reached")
                        set_kv(conn, "last_shutdown", "clean")
                        return 0
                    if not self.config.persistent:
                        set_kv(conn, "last_shutdown", "clean")
                        return 0
                    time.sleep(self.config.sleep_seconds)
                self._stop_cleanly(conn, "signal")
                return 0
            except Exception as exc:
                set_kv(conn, "service_state", "FAILED")
                set_kv(conn, "last_shutdown", "unclean")
                set_kv(conn, "last_error", f"{type(exc).__name__}:{exc}"[:4000])
                write_heartbeat(
                    self.paths,
                    self._heartbeat_payload(
                        conn,
                        current_action={"action_type": "CONTROLLER_FAILED", "reason": str(exc)},
                        checkpoint=str(get_kv(conn, "last_successful_checkpoint", "")),
                    ),
                )
                raise
            finally:
                conn.close()

    def step(self, conn: Any) -> tuple[dict[str, Any], bool]:
        phase = str(get_kv(conn, "current_phase", ""))
        if phase in {"INTEGRITY_BLOCKED", "ENGINEERING_BLOCKED", "EXPERIMENT_BLOCKED"}:
            return {
                "action_id": "blocked_state_requires_external_change",
                "action_type": phase,
                "rationale": str(get_kv(conn, "current_blocker", phase)),
            }, False
        if queue_size(conn) > 0:
            next_experiment = peek_next_experiment(conn)
            if next_experiment is None:
                return {"action_id": "queue_race_retry", "action_type": "RECOVERING"}, False
            experiment_type = str(next_experiment.get("experiment_type") or "")
            try:
                self._check_experiment_allowed(conn, next_experiment)
            except Exception as exc:
                set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
                set_kv(conn, "current_blocker", "EXPERIMENT_GOVERNANCE_GUARD_FAILED")
                set_kv(conn, "last_error", str(exc)[:4000])
                append_event(
                    conn,
                    "experiment_governance_blocked",
                    {"experiment_id": next_experiment.get("experiment_id"), "reason": str(exc)},
                )
                return {
                    "action_id": "experiment_governance_blocked",
                    "action_type": "INTEGRITY_BLOCKED",
                    "rationale": str(exc),
                }, False
            if experiment_type not in SUPPORTED_EXPERIMENT_TYPES:
                blocker = f"MISSING_EXPERIMENT_HANDLER:{experiment_type}"
                first_block = get_kv(conn, "current_blocker") != blocker
                set_kv(conn, "current_phase", "ENGINEERING_BLOCKED")
                set_kv(conn, "current_blocker", blocker)
                set_kv(conn, "last_error", f"No approved handler for experiment type {experiment_type!r}.")
                if first_block:
                    append_event(
                        conn,
                        "experiment_handler_missing_preclaim",
                        {
                            "experiment_id": next_experiment.get("experiment_id"),
                            "experiment_type": experiment_type,
                            "queue_status_preserved": "QUEUED",
                        },
                    )
                return {
                    "action_id": "missing_experiment_handler_preclaim",
                    "action_type": "ENGINEERING_BLOCKED",
                    "rationale": blocker,
                }, False
            action = {
                "action_id": "execute_highest_priority_queued_experiment",
                "action_type": "RUN_QUEUED_EXPERIMENT",
                "data_cost": 0.0,
                "expected_decision_information_gain": 1.0,
                "rationale": "A preregistered experiment is durably queued and is the next executable research action.",
            }
            self._record_action(conn, action)
            self._execute_queued_experiment(conn)
            self._record_progress(conn)
            return action, True

        snapshot = state_snapshot(conn)
        action = plan_next_action(snapshot)
        check_action_allowed(
            action,
            baseline_commit=self.config.baseline_commit,
            remaining_budget_usd=float(get_kv(conn, "remaining_databento_budget_usd", self.config.remaining_databento_budget_usd)),
        )
        if action.get("action_type") == "WAIT":
            first_stall = get_kv(conn, "current_phase") != "SCHEDULER_STALLED"
            set_kv(conn, "current_phase", "SCHEDULER_STALLED")
            set_kv(conn, "current_blocker", "NO_EXECUTABLE_ACTION_OR_SCHEDULER_DEADLINE")
            set_kv(conn, "next_wake_at_utc", None)
            if first_stall:
                append_event(conn, "scheduler_stalled", {"reason": action.get("rationale"), "queue": experiment_counts(conn)})
            return {
                **action,
                "action_id": "scheduler_stalled_no_action",
                "rationale": "Mission incomplete with no executable action or legitimate future scientific deadline.",
            }, False

        self._record_action(conn, action)
        self._execute_action(conn, action)
        self._record_progress(conn)
        return action, True

    def _initialize(self, conn: Any) -> None:
        self.paths.state_dir.mkdir(parents=True, exist_ok=True)
        clear_stop(self.paths)
        ensure_experiment_schema(conn)
        previous_phase = str(get_kv(conn, "current_phase", ""))
        previous_blocker = get_kv(conn, "current_blocker")
        previous_last_error = get_kv(conn, "last_error")
        blocked_phase = previous_phase in {"INTEGRITY_BLOCKED", "ENGINEERING_BLOCKED", "EXPERIMENT_BLOCKED"}
        resolved_missing_handler_type = self._resolved_missing_handler_type(previous_phase, previous_blocker)
        contract_map_repair_required = bool(
            previous_phase == "INTEGRITY_BLOCKED"
            and str(previous_blocker or "") == "CONTRACT_MAP_REBUILD_REQUIRED"
        )
        recovered_missing_handler_rows = 0
        if resolved_missing_handler_type is not None:
            recovered_missing_handler_rows = recover_resolved_missing_handler_experiments(
                conn, resolved_missing_handler_type
            )
        previous_service_state = get_kv(conn, "service_state")
        previous_shutdown = get_kv(conn, "last_shutdown")
        if previous_service_state in {"RUNNING", "FAILED"} and previous_shutdown not in {"clean", None}:
            set_kv(conn, "crash_count", int(get_kv(conn, "crash_count", 0)) + 1)
        elif get_kv(conn, "crash_count") is None:
            set_kv(conn, "crash_count", 0)
        set_kv(conn, "controller_start_count", int(get_kv(conn, "controller_start_count", 0)) + 1)
        set_kv(conn, "last_shutdown", "unclean")
        if get_kv(conn, "mission_id") is None:
            set_kv(conn, "mission_id", self.config.mission_id)
            set_kv(conn, "objective_version", self.config.objective_config)
            set_kv(conn, "current_phase", "PHASE_0_GOVERNANCE")
            set_kv(conn, "cycle_count", 0)
            set_kv(conn, "progress_sequence", 0)
            set_kv(conn, "validated_mechanisms", 0)
            set_kv(conn, "validated_strategies", 0)
            set_kv(conn, "executable_baskets", 0)
        if get_kv(conn, "progress_sequence") is None:
            set_kv(conn, "progress_sequence", 0)
        if get_kv(conn, "last_progress_at_utc") is None:
            set_kv(conn, "last_progress_at_utc", utc_now_iso())
        set_kv(conn, "service_state", "RUNNING")
        set_kv(conn, "stop_reason", None)
        set_kv(conn, "last_error", None)
        set_kv(conn, "current_phase", "RECOVERING")
        governance = initialize_governance_kernel(
            baseline_commit=self.config.baseline_commit,
            remaining_budget_usd=self.config.remaining_databento_budget_usd,
        )
        actual_spend = float(governance.result.details.get("cumulative_actual_databento_spend_usd", 0.0))
        set_kv(conn, "cumulative_databento_spend_usd", actual_spend)
        set_kv(conn, "remaining_databento_budget_usd", max(100.0 - actual_spend, 0.0))
        set_kv(conn, "q4_access_count", int(governance.result.details.get("q4_access_count", 0)))
        set_kv(conn, "governance_manifest_hash", governance.manifest_hash)
        set_kv(conn, "governance_kernel_path", governance.manifest_path)
        set_kv(conn, "governance_passed", governance.result.passed)
        engineering = detect_engineering_capability()
        set_kv(conn, "autonomous_engineering_capability", engineering.to_dict())
        record_engineering(self.paths, {"mission_id": self.config.mission_id, "capability": engineering.to_dict()})
        recovery = recover_running_experiments(conn)
        set_kv(conn, "latest_recovery", recovery)
        if experiment_counts(conn).get("RUNNING", 0) == 0:
            set_kv(conn, "current_experiment", None)
        self._reconcile_planned_experiment_flags(conn)
        self._reconcile_completed_experiments(conn)
        contract_map_repair_required = contract_map_repair_required or bool(
            str(get_kv(conn, "current_phase", "")) == "INTEGRITY_BLOCKED"
            and str(get_kv(conn, "current_blocker") or "") == "CONTRACT_MAP_REBUILD_REQUIRED"
        )
        contract_map_repair_queued = (
            self._reconcile_contract_map_repair(conn) if contract_map_repair_required else False
        )
        self._reconcile_legacy_plan(conn)
        reconciliation_phase = str(get_kv(conn, "current_phase", ""))
        reconciliation_created_block = reconciliation_phase in {
            "INTEGRITY_BLOCKED",
            "ENGINEERING_BLOCKED",
            "EXPERIMENT_BLOCKED",
        }
        if blocked_phase and resolved_missing_handler_type is None and not contract_map_repair_queued:
            set_kv(conn, "current_phase", previous_phase)
            set_kv(conn, "current_blocker", previous_blocker)
            set_kv(conn, "last_error", previous_last_error)
        elif not reconciliation_created_block:
            set_kv(conn, "current_phase", "PLANNING_NEXT_ACTION")
            set_kv(conn, "current_blocker", None)
            set_kv(conn, "last_error", None)
        append_event(
            conn,
            "controller_initialized",
            {
                "version": CONTROLLER_VERSION,
                "config": self.config.to_dict(),
                "recovery": recovery,
                "preserved_blocked_phase": (
                    previous_phase if blocked_phase and resolved_missing_handler_type is None else None
                ),
                "resolved_missing_handler_type": resolved_missing_handler_type,
                "requeued_legacy_missing_handler_rows": recovered_missing_handler_rows,
                "contract_map_repair_queued": contract_map_repair_queued,
                "reconciliation_created_block": reconciliation_phase if reconciliation_created_block else None,
            },
        )

    @staticmethod
    def _resolved_missing_handler_type(previous_phase: str, previous_blocker: Any) -> str | None:
        prefix = "MISSING_EXPERIMENT_HANDLER:"
        blocker = str(previous_blocker or "")
        if previous_phase != "ENGINEERING_BLOCKED" or not blocker.startswith(prefix):
            return None
        experiment_type = blocker[len(prefix) :]
        return experiment_type if experiment_type in SUPPORTED_EXPERIMENT_TYPES else None

    def _reconcile_planned_experiment_flags(self, conn: Any) -> None:
        """Close the enqueue-commit/plan-flag crash window for fixed experiments."""
        for experiment_id, flag in (
            (DESIGN_EXPERIMENT_ID, "bounded_retest_plan_written"),
            (EXECUTION_EXPERIMENT_ID, "calibration_retest_execution_plan_written"),
            (POST_RETEST_DESIGN_EXPERIMENT_ID, "post_retest_research_plan_written"),
            (CONTRACT_MAP_REPAIR_EXPERIMENT_ID, "contract_map_repair_plan_written"),
        ):
            record = experiment_record(conn, experiment_id)
            if record is not None:
                set_kv(conn, flag, True)

    def _reconcile_completed_experiments(self, conn: Any) -> None:
        for experiment_id, experiment_type, completion_flag in (
            (DESIGN_EXPERIMENT_ID, "calibration_affected_atom_retest_design", "calibration_retest_design_completed"),
            (EXECUTION_EXPERIMENT_ID, "calibration_affected_atom_retest_execution", "calibration_retest_execution_completed"),
            (
                POST_RETEST_DESIGN_EXPERIMENT_ID,
                "post_calibration_retest_research_design",
                "post_retest_research_design_completed",
            ),
            (
                POST_RETEST_PILOT_EXPERIMENT_ID,
                "validator_integrity_repair_pilot",
                "validator_integrity_repair_pilot_completed",
            ),
            (
                CONTRACT_MAP_REPAIR_EXPERIMENT_ID,
                "contract_map_date_aware_repair",
                "contract_map_date_aware_repair_completed",
            ),
        ):
            record = experiment_record(conn, experiment_id)
            if record is None or record.get("status") != "COMPLETED":
                continue
            result = record.get("result") or {}
            finding = result.get("scientific_conclusion") or "completed_result_recovered_without_claimed_validation"
            reconciliation_id = (
                f"completed:{experiment_id}:{record.get('specification_hash') or result.get('result_hash') or 'unknown'}"
            )
            compact = {
                "experiment_id": experiment_id,
                "experiment_type": experiment_type,
                "completed_at": record.get("completed_at"),
                "specification_hash": record.get("specification_hash"),
                "scientific_conclusion": finding,
                "report_path": result.get("report_path") or (result.get("paths") or {}).get("report"),
            }
            set_kv(conn, completion_flag, True)
            result_key = {
                "calibration_affected_atom_retest_design": "calibration_retest_design_result",
                "calibration_affected_atom_retest_execution": "calibration_retest_execution_result",
                "post_calibration_retest_research_design": "post_retest_research_design_result",
                "validator_integrity_repair_pilot": "validator_integrity_repair_pilot_result",
                "contract_map_date_aware_repair": "contract_map_date_aware_repair_result",
            }[experiment_type]
            set_kv(conn, result_key, compact)
            set_kv(conn, "latest_completed_experiment", compact)
            set_kv(conn, "latest_scientific_finding", finding)
            set_kv(conn, "current_experiment", None)
            if experiment_type == "calibration_affected_atom_retest_execution":
                validated = int(result.get("fully_validated_edge_atoms", 0))
                if validated > 0:
                    set_kv(conn, "validated_mechanisms", max(int(get_kv(conn, "validated_mechanisms", 0)), validated))
                    set_kv(conn, "milestone", "M2_FIRST_VALIDATED_MECHANISM")
            elif experiment_type == "post_calibration_retest_research_design":
                self._queue_post_retest_pilot(conn, result)
            elif experiment_type == "validator_integrity_repair_pilot":
                disposition = str(result.get("integrity_disposition") or "INTEGRITY_AUDIT_INCONCLUSIVE")
                set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
                set_kv(conn, "current_blocker", disposition)
                set_kv(
                    conn,
                    "last_error",
                    f"Validator-integrity pilot requires controlled resolution: {disposition}",
                )
            elif experiment_type == "contract_map_date_aware_repair":
                set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
                set_kv(conn, "current_blocker", "FRESH_RETEST_WITH_REPAIRED_MAP_REQUIRED")
                set_kv(
                    conn,
                    "last_error",
                    "Repaired map is valid; a separate fresh preregistration with new atom IDs is required.",
                )
            if not self._evidence_reconciliation_exists(reconciliation_id):
                record_evidence(
                    self.paths,
                    {
                        "reconciliation_id": reconciliation_id,
                        "scope": "EXPERIMENT",
                        "experiment_id": experiment_id,
                        "experiment_type": experiment_type,
                        "status": "COMPLETED",
                        "result": result,
                    },
                )
            if not self._event_reconciliation_exists(conn, reconciliation_id):
                append_event(
                    conn,
                    "completed_experiment_reconciled",
                    {**compact, "reconciliation_id": reconciliation_id},
                )

    def _queue_post_retest_pilot(self, conn: Any, result: dict[str, Any]) -> None:
        pilot = result.get("pilot_experiment_specification")
        if not isinstance(pilot, dict) or not pilot.get("experiment_type"):
            raise RuntimeError("Post-retest design did not expose a complete pilot experiment specification.")
        pilot_type = str(pilot["experiment_type"])
        enqueue_experiment(conn, POST_RETEST_PILOT_EXPERIMENT_ID, pilot)
        set_kv(
            conn,
            "post_retest_pilot_selected",
            {
                "experiment_id": POST_RETEST_PILOT_EXPERIMENT_ID,
                "experiment_type": pilot_type,
                "branch": result.get("selected_branch"),
                "engineering_task_path": (result.get("paths") or {}).get("engineering_task"),
                "status": "QUEUED",
            },
        )
        if pilot_type not in SUPPORTED_EXPERIMENT_TYPES:
            set_kv(conn, "current_phase", "ENGINEERING_BLOCKED")
            set_kv(conn, "current_blocker", f"MISSING_EXPERIMENT_HANDLER:{pilot_type}")
            set_kv(
                conn,
                "last_error",
                f"Post-retest branch selected; approved pilot handler {pilot_type!r} must be implemented.",
            )

    def _reconcile_contract_map_repair(self, conn: Any) -> bool:
        existing = experiment_record(conn, CONTRACT_MAP_REPAIR_EXPERIMENT_ID)
        if existing is not None:
            return str(existing.get("status")) in {"QUEUED", "RUNNING"}
        pilot = experiment_record(conn, POST_RETEST_PILOT_EXPERIMENT_ID)
        result = (pilot or {}).get("result") or {}
        if (pilot or {}).get("status") != "COMPLETED" or result.get("integrity_disposition") != (
            "CONTRACT_MAP_REBUILD_REQUIRED"
        ):
            return False
        contract_audit = result.get("contract_map_integrity_audit") or {}
        artifacts = result.get("artifacts") or {}
        task_path = project_path(
            "reports", "engineering", "hydra_contract_map_date_repair_20260710_v2.md"
        )
        if not task_path.is_file() or hashlib.sha256(task_path.read_bytes()).hexdigest() != (
            CONTRACT_MAP_REPAIR_TASK_SHA256
        ):
            set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
            set_kv(conn, "current_blocker", "CONTRACT_MAP_REPAIR_TASK_HASH_MISMATCH")
            set_kv(conn, "last_error", "Immutable contract-map repair task is missing or changed.")
            return False
        specification = {
            "experiment_type": "contract_map_date_aware_repair",
            "priority": 100.0,
            "max_attempts": 3,
            "integrity_pilot_result_path": str(artifacts.get("result_json_path") or ""),
            "integrity_pilot_result_hash": str(result.get("result_hash") or ""),
            "frozen_contract_map_path": str(contract_audit.get("frozen_contract_map_path") or ""),
            "frozen_contract_map_sha256": str(contract_audit.get("frozen_contract_map_sha256") or ""),
            "definition_dbn_path": str(contract_audit.get("definition_dbn_path") or ""),
            "definition_dbn_sha256": str(contract_audit.get("definition_dbn_sha256") or ""),
            "engineering_task_path": str(task_path),
            "engineering_task_sha256": CONTRACT_MAP_REPAIR_TASK_SHA256,
            "code_commit": self._git_commit(),
            "data_role": "CACHED_DEFINITION_METADATA_ONLY",
            "q4_access_allowed": False,
            "paid_data_allowed": False,
            "market_observation_read_allowed": False,
        }
        if not all(
            specification[key]
            for key in (
                "integrity_pilot_result_path",
                "integrity_pilot_result_hash",
                "frozen_contract_map_path",
                "frozen_contract_map_sha256",
                "definition_dbn_path",
                "definition_dbn_sha256",
            )
        ):
            set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
            set_kv(conn, "current_blocker", "CONTRACT_MAP_REPAIR_SOURCE_INCOMPLETE")
            set_kv(conn, "last_error", "Integrity pilot lacks frozen map-repair source metadata.")
            return False
        enqueue_experiment(conn, CONTRACT_MAP_REPAIR_EXPERIMENT_ID, specification)
        set_kv(conn, "contract_map_repair_plan_written", True)
        set_kv(
            conn,
            "current_research_experiment_selected",
            {
                "experiment": CONTRACT_MAP_REPAIR_EXPERIMENT_ID,
                "experiment_type": "contract_map_date_aware_repair",
                "status": "QUEUED",
                "reason": "Confirmed contract-map date-flattening integrity defect.",
            },
        )
        set_kv(conn, "current_phase", "PLANNING_NEXT_ACTION")
        set_kv(conn, "current_blocker", None)
        set_kv(conn, "last_error", None)
        return True

    def _evidence_reconciliation_exists(self, reconciliation_id: str) -> bool:
        path = self.paths.evidence_ledger
        if not path.exists():
            return False
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if payload.get("reconciliation_id") == reconciliation_id:
                return True
        return False

    @staticmethod
    def _event_reconciliation_exists(conn: Any, reconciliation_id: str) -> bool:
        rows = conn.execute("SELECT payload FROM events WHERE event_type='completed_experiment_reconciled'").fetchall()
        for (payload_text,) in rows:
            try:
                payload = json.loads(payload_text)
            except json.JSONDecodeError:
                continue
            if payload.get("reconciliation_id") == reconciliation_id:
                return True
        return False

    def _reconcile_legacy_plan(self, conn: Any) -> None:
        if not get_kv(conn, "bounded_retest_plan_written", False):
            return
        existing = experiment_record(conn, DESIGN_EXPERIMENT_ID)
        created = False
        if existing is None:
            created = enqueue_experiment(conn, DESIGN_EXPERIMENT_ID, self._design_specification())
            existing = experiment_record(conn, DESIGN_EXPERIMENT_ID)
        selected = dict(get_kv(conn, "first_autonomous_experiment_selected", {}) or {})
        selected.update(
            {
                "experiment": DESIGN_EXPERIMENT_ID,
                "experiment_type": "calibration_affected_atom_retest_design",
                "status": "QUEUED" if created else str((existing or {}).get("status", "UNKNOWN")),
                "constraints": "new atom IDs, no inherited results, no Q4, bounded by information value",
            }
        )
        set_kv(conn, "first_autonomous_experiment_selected", selected)

    def _record_action(self, conn: Any, action: dict[str, Any]) -> None:
        snapshot = state_snapshot(conn)
        record_decision(
            self.paths,
            {"mission_id": self.config.mission_id, "cycle": snapshot.get("cycle_count", 0), "selected_action": action},
        )
        append_event(conn, "selected_action", action)
        set_kv(conn, "current_action", action)

    def _record_progress(self, conn: Any) -> None:
        set_kv(conn, "cycle_count", int(get_kv(conn, "cycle_count", 0)) + 1)
        set_kv(conn, "progress_sequence", int(get_kv(conn, "progress_sequence", 0)) + 1)
        set_kv(conn, "last_progress_at_utc", utc_now_iso())
        set_kv(conn, "next_wake_at_utc", self._next_wake_at())

    def _execute_action(self, conn: Any, action: dict[str, Any]) -> None:
        action_type = action.get("action_type")
        if action_type == "RUN_VALIDATOR_CALIBRATION":
            set_kv(conn, "current_phase", "PHASE_2_VALIDATOR_CALIBRATION")
            result = benchmark_validator(
                previous_report="reports/edge_atom_lab/edge_atom_lab_20260710T101052+0000_edge_atom_discovery_replication_v1_final_corrected.md"
            )
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
            zero_pass = benchmark_validator(
                previous_report="reports/edge_atom_lab/edge_atom_lab_20260710T101052+0000_edge_atom_discovery_replication_v1_final_corrected.md"
            ).zero_pass_diagnosis
            set_kv(conn, "zero_pass_audited", True)
            set_kv(
                conn,
                "previous_atom_decisions_affected",
                zero_pass.get("cause")
                in {
                    "MULTIPLE_CAUSES_COST_HURDLE_AND_OVERSTRICT_ATTACK_POLICY",
                    "OVERSTRICT_OR_UNCALIBRATED_MANDATORY_ATTACK_POLICY",
                },
            )
            record_evidence(self.paths, {"scope": "ZERO_PASS_AUDIT", "result": zero_pass})
        elif action_type == "PLAN_BOUNDED_RETESTS":
            set_kv(conn, "current_phase", "PHASE_4_AUTONOMOUS_RESEARCH_PLANNING")
            created = enqueue_experiment(conn, DESIGN_EXPERIMENT_ID, self._design_specification())
            set_kv(
                conn,
                "first_autonomous_experiment_selected",
                {
                    "experiment": DESIGN_EXPERIMENT_ID,
                    "experiment_type": "calibration_affected_atom_retest_design",
                    "status": "QUEUED" if created else "ALREADY_PRESENT",
                    "reason": action.get("rationale"),
                    "constraints": "new atom IDs, no inherited results, no Q4, bounded by information value",
                },
            )
            set_kv(conn, "bounded_retest_plan_written", True)
        elif action_type == "PLAN_CALIBRATION_RETEST_EXECUTION":
            design = experiment_record(conn, DESIGN_EXPERIMENT_ID)
            result = (design or {}).get("result") or {}
            paths = result.get("paths") or result.get("artifacts") or {}
            design_path = paths.get("design") or result.get("design_path")
            preregistration_path = paths.get("preregistration") or result.get("preregistration_path")
            if not design_path or not preregistration_path:
                raise RuntimeError("Completed design did not expose design and preregistration artifact paths.")
            specification = {
                "experiment_type": "calibration_affected_atom_retest_execution",
                "priority": 95.0,
                "max_attempts": 3,
                "design_path": str(design_path),
                "design_preregistration_path": str(preregistration_path),
                "code_commit": self._git_commit(),
                "data_role": "DEVELOPMENT_AND_FALSIFICATION_ONLY",
                "development_end_exclusive": "2024-10-01",
                "q4_access_allowed": False,
                "paid_data_allowed": False,
            }
            created = enqueue_experiment(conn, EXECUTION_EXPERIMENT_ID, specification)
            set_kv(conn, "calibration_retest_execution_plan_written", True)
            set_kv(
                conn,
                "current_research_experiment_selected",
                {"experiment": EXECUTION_EXPERIMENT_ID, "status": "QUEUED" if created else "ALREADY_PRESENT", "reason": action.get("rationale")},
            )
            set_kv(conn, "current_phase", "PHASE_5_CALIBRATION_AFFECTED_RETEST")
        elif action_type == "PLAN_POST_RETEST_RESEARCH":
            execution = experiment_record(conn, EXECUTION_EXPERIMENT_ID)
            result = (execution or {}).get("result") or {}
            artifacts = result.get("artifacts") or {}
            result_path = artifacts.get("result_json_path")
            result_hash = result.get("result_hash")
            if not result_path or not result_hash:
                raise RuntimeError("Completed calibration retest did not expose a frozen result path and hash.")
            specification = {
                "experiment_type": "post_calibration_retest_research_design",
                "priority": 92.0,
                "max_attempts": 3,
                "source_execution_experiment_id": EXECUTION_EXPERIMENT_ID,
                "source_execution_specification_hash": (execution or {}).get("specification_hash"),
                "source_execution_result_path": str(result_path),
                "source_execution_result_hash": str(result_hash),
                "code_commit": self._git_commit(),
                "data_role": "FROZEN_DEVELOPMENT_EVIDENCE_DECISION_ONLY",
                "q4_access_allowed": False,
                "paid_data_allowed": False,
            }
            created = enqueue_experiment(conn, POST_RETEST_DESIGN_EXPERIMENT_ID, specification)
            set_kv(conn, "post_retest_research_plan_written", True)
            set_kv(
                conn,
                "current_research_experiment_selected",
                {
                    "experiment": POST_RETEST_DESIGN_EXPERIMENT_ID,
                    "status": "QUEUED" if created else "ALREADY_PRESENT",
                    "reason": action.get("rationale"),
                },
            )
            set_kv(conn, "current_phase", "PHASE_6_POST_RETEST_RESEARCH_DESIGN")
        else:
            raise RuntimeError(f"Unsupported mission action {action_type!r}.")

    def _execute_queued_experiment(self, conn: Any) -> None:
        experiment = claim_next_experiment(
            conn,
            claimed_by=f"{self.config.mission_id}:{os.getpid()}",
        )
        if experiment is None:
            return
        experiment_id = str(experiment["experiment_id"])
        experiment_type = str(experiment["experiment_type"])
        try:
            self._check_experiment_allowed(conn, experiment)
        except Exception as exc:
            block_experiment(
                conn,
                experiment_id,
                f"governance_guard:{exc}",
                claim_token=str(experiment["claim_token"]),
            )
            set_kv(conn, "current_experiment", None)
            set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
            set_kv(conn, "current_blocker", "EXPERIMENT_GOVERNANCE_GUARD_FAILED")
            set_kv(conn, "last_error", str(exc)[:4000])
            return
        if experiment.get("q4_access_allowed") or experiment.get("paid_data_allowed") or "live" in experiment_type.lower():
            reason = "Experiment specification requests a prohibited Q4, paid-data, or live path."
            block_experiment(conn, experiment_id, reason, claim_token=str(experiment["claim_token"]))
            set_kv(conn, "current_experiment", None)
            set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
            set_kv(conn, "current_blocker", "PROHIBITED_EXPERIMENT_SPECIFICATION")
            set_kv(conn, "last_error", reason)
            return
        set_kv(conn, "current_phase", "RUNNING_EXPERIMENT")
        set_kv(
            conn,
            "current_experiment",
            {
                "experiment_id": experiment_id,
                "experiment_type": experiment_type,
                "specification_hash": experiment.get("specification_hash"),
                "attempt_count": experiment.get("attempt_count"),
                "claim_token": experiment.get("claim_token"),
                "lease_expires_at": experiment.get("lease_expires_at"),
                "started_at_utc": utc_now_iso(),
            },
        )
        set_kv(conn, "last_progress_at_utc", utc_now_iso())
        write_heartbeat(
            self.paths,
            self._heartbeat_payload(
                conn,
                current_action={"action_type": "RUN_QUEUED_EXPERIMENT", "experiment_id": experiment_id},
                checkpoint=str(get_kv(conn, "last_successful_checkpoint", "")),
            ),
        )
        if experiment_type not in SUPPORTED_EXPERIMENT_TYPES:
            exc = RuntimeError(f"No approved handler for experiment type {experiment_type!r}.")
            block_experiment(conn, experiment_id, str(exc), claim_token=str(experiment["claim_token"]))
            set_kv(conn, "current_experiment", None)
            set_kv(conn, "current_phase", "ENGINEERING_BLOCKED")
            set_kv(conn, "current_blocker", f"MISSING_EXPERIMENT_HANDLER:{experiment_type}")
            set_kv(conn, "last_error", str(exc))
            record_evidence(
                self.paths,
                {"scope": "EXPERIMENT", "experiment_id": experiment_id, "status": "BLOCKED", "reason": str(exc)},
            )
            return
        try:
            result = self._run_experiment_with_heartbeat(conn, experiment)
        except CleanWorkerInterruption as exc:
            release_experiment_claim_for_shutdown(
                conn,
                experiment_id,
                claim_token=str(experiment["claim_token"]),
                reason=str(exc),
            )
            set_kv(conn, "current_experiment", None)
            set_kv(conn, "current_phase", "STOPPING")
            set_kv(conn, "current_blocker", None)
            set_kv(conn, "last_error", None)
            append_event(
                conn,
                "experiment_released_for_clean_shutdown",
                {"experiment_id": experiment_id, "reason": str(exc)},
            )
            return
        except Exception as exc:
            status = fail_experiment(
                conn,
                experiment_id,
                f"{type(exc).__name__}:{exc}",
                retryable=True,
                claim_token=str(experiment["claim_token"]),
            )
            set_kv(conn, "current_experiment", None)
            set_kv(conn, "current_phase", "RETRY_SCHEDULED" if status == "QUEUED" else "EXPERIMENT_BLOCKED")
            set_kv(conn, "current_blocker", None if status == "QUEUED" else f"EXPERIMENT_FAILED:{experiment_id}")
            set_kv(conn, "last_error", f"{type(exc).__name__}:{exc}"[:4000])
            record_evidence(
                self.paths,
                {"scope": "EXPERIMENT", "experiment_id": experiment_id, "status": status, "reason": str(exc)},
            )
            return

        complete_experiment(conn, experiment_id, result, claim_token=str(experiment["claim_token"]))
        self._reconcile_completed_experiments(conn)
        if str(get_kv(conn, "current_phase", "")) not in {
            "INTEGRITY_BLOCKED",
            "ENGINEERING_BLOCKED",
            "EXPERIMENT_BLOCKED",
        }:
            set_kv(conn, "current_blocker", None)
            set_kv(conn, "last_error", None)
            set_kv(conn, "current_phase", "PLANNING_NEXT_ACTION")

    def _check_experiment_allowed(self, conn: Any, experiment: dict[str, Any]) -> None:
        experiment_type = str(experiment.get("experiment_type") or "")
        check_action_allowed(
            {
                "action_type": "Q4_ACCESS"
                if experiment.get("q4_access_allowed")
                else ("LIVE_TRADING" if "live" in experiment_type.lower() else "RUN_RESEARCH_EXPERIMENT"),
                "data_cost": float(experiment.get("data_cost", 0.0)),
            },
            baseline_commit=self.config.baseline_commit,
            remaining_budget_usd=float(
                get_kv(conn, "remaining_databento_budget_usd", self.config.remaining_databento_budget_usd)
            ),
        )
        if experiment.get("q4_access_allowed") or experiment.get("paid_data_allowed") or "live" in experiment_type.lower():
            raise RuntimeError("Experiment specification requests a prohibited Q4, paid-data, or live path.")

    def _run_experiment_with_heartbeat(
        self,
        conn: Any,
        experiment: dict[str, Any],
        *,
        worker_entrypoint: Any = experiment_worker_entry,
    ) -> dict[str, Any]:
        experiment_id = str(experiment["experiment_id"])
        specification_hash = str(experiment["specification_hash"])
        result_dir = self.paths.state_dir / "worker_results"
        result_path = result_dir / f"{experiment_id}_{specification_hash[:16]}.json"
        if result_path.exists():
            envelope = json.loads(result_path.read_text(encoding="utf-8"))
            if (
                envelope.get("ok")
                and envelope.get("experiment_id") == experiment_id
                and envelope.get("specification_hash") == specification_hash
            ):
                return dict(envelope["result"])
            result_path.unlink()

        context = multiprocessing.get_context("spawn")
        worker = context.Process(
            target=worker_entrypoint,
            args=(experiment, str(result_path)),
            name=f"hydra-exp-{experiment_id[:32]}",
        )
        worker.start()
        last_lease_renewal = 0.0
        while worker.is_alive():
            worker.join(timeout=min(max(self.config.sleep_seconds, 1.0), 15.0))
            if self._shutdown or stop_requested(self.paths):
                self._signal_worker_tree(worker, signal.SIGTERM)
                worker.join(timeout=10.0)
                if worker.is_alive():
                    self._signal_worker_tree(worker, signal.SIGKILL)
                    worker.join(timeout=5.0)
                raise CleanWorkerInterruption("experiment_worker_interrupted_for_clean_shutdown")
            now_monotonic = time.monotonic()
            if now_monotonic - last_lease_renewal >= 30.0:
                lease = renew_experiment_lease(
                    conn,
                    experiment_id,
                    str(experiment["claim_token"]),
                    lease_seconds=180.0,
                )
                current = dict(get_kv(conn, "current_experiment", {}) or {})
                current.update({"worker_pid": worker.pid, "lease_expires_at": lease})
                set_kv(conn, "current_experiment", current)
                write_heartbeat(
                    self.paths,
                    self._heartbeat_payload(
                        conn,
                        current_action={
                            "action_type": "RUN_QUEUED_EXPERIMENT",
                            "experiment_id": experiment_id,
                            "worker_pid": worker.pid,
                        },
                        checkpoint=str(get_kv(conn, "last_successful_checkpoint", "")),
                    ),
                )
                last_lease_renewal = now_monotonic
        worker.join(timeout=1.0)
        if self._shutdown or stop_requested(self.paths):
            raise CleanWorkerInterruption("experiment_worker_interrupted_for_clean_shutdown")
        if not result_path.exists():
            raise RuntimeError(f"experiment_worker_exited_without_result:exitcode={worker.exitcode}")
        envelope = json.loads(result_path.read_text(encoding="utf-8"))
        if envelope.get("experiment_id") != experiment_id or envelope.get("specification_hash") != specification_hash:
            raise RuntimeError("experiment_worker_result_provenance_mismatch")
        if not envelope.get("ok"):
            raise RuntimeError(
                f"worker:{envelope.get('error_type')}:{envelope.get('error')}\n{envelope.get('traceback', '')}"[:8000]
            )
        return dict(envelope["result"])

    @staticmethod
    def _signal_worker_tree(worker: Any, signal_number: int) -> None:
        """Signal an isolated experiment process group without risking the controller group."""
        try:
            process_group = os.getpgid(int(worker.pid))
            if process_group == int(worker.pid):
                os.killpg(process_group, signal_number)
                return
        except (ProcessLookupError, PermissionError, OSError, TypeError, ValueError):
            return
        if signal_number == signal.SIGKILL:
            worker.kill()
        else:
            worker.terminate()

    def _design_specification(self) -> dict[str, Any]:
        return {
            "experiment_type": "calibration_affected_atom_retest_design",
            "priority": 100.0,
            "max_attempts": 3,
            "historical_report_path": "reports/edge_atom_lab/edge_atom_lab_20260710T101052+0000_edge_atom_discovery_replication_v1_final_corrected.md",
            "historical_preregistration_path": "reports/edge_atom_lab/edge_atom_preregistration_20260710T101052+0000_edge_atom_discovery_replication_v1_final.json",
            "code_commit": self._git_commit(),
            "data_role": "HISTORICAL_DEVELOPMENT_EVIDENCE_ONLY",
            "q4_access_allowed": False,
            "paid_data_allowed": False,
            "selection_rule": "bounded_expected_decision_information_gain_with_positive_negative_and_invariant_controls",
        }

    def _checkpoint_due(self, conn: Any, action: dict[str, Any], *, progressed: bool) -> bool:
        if progressed:
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
            "experiments": experiment_counts(conn),
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
            "scheduler_state": snapshot.get("current_phase"),
            "current_action": current_action,
            "cycle_count": snapshot.get("cycle_count", 0),
            "progress_sequence": snapshot.get("progress_sequence", 0),
            "last_progress_at_utc": snapshot.get("last_progress_at_utc"),
            "next_wake_at_utc": snapshot.get("next_wake_at_utc"),
            "experiment_counts": experiment_counts(conn),
            "queue_size": queue_size(conn),
            "current_experiment": snapshot.get("current_experiment"),
            "latest_completed_experiment": snapshot.get("latest_completed_experiment"),
            "latest_scientific_finding": snapshot.get("latest_scientific_finding"),
            "validated_mechanisms": snapshot.get("validated_mechanisms", 0),
            "validated_strategies": snapshot.get("validated_strategies", 0),
            "executable_baskets": snapshot.get("executable_baskets", 0),
            "current_blocker": snapshot.get("current_blocker"),
            "cumulative_databento_spend_usd": snapshot.get("cumulative_databento_spend_usd"),
            "remaining_databento_budget_usd": snapshot.get("remaining_databento_budget_usd"),
            "q4_access_count": snapshot.get("q4_access_count", 0),
            "latest_checkpoint": checkpoint,
            "latest_commit": self._git_commit(),
            "governance_baseline_commit": self.config.baseline_commit,
            "crash_count": snapshot.get("crash_count", 0),
            "controller_start_count": snapshot.get("controller_start_count", 0),
            "last_error": snapshot.get("last_error"),
        }

    def _stop_cleanly(self, conn: Any, reason: str) -> None:
        set_kv(conn, "stop_reason", reason)
        set_kv(conn, "service_state", "STOPPED_CLEANLY")
        set_kv(conn, "last_shutdown", "clean")
        set_kv(conn, "current_phase", "STOPPED_CLEANLY")
        checkpoint = str(self._checkpoint(conn))
        write_heartbeat(
            self.paths,
            self._heartbeat_payload(conn, current_action={"action_type": "STOPPED", "reason": reason}, checkpoint=checkpoint),
        )

    def _next_wake_at(self) -> str:
        return (datetime.now(timezone.utc) + timedelta(seconds=max(self.config.sleep_seconds, 0.0))).replace(microsecond=0).isoformat()

    @staticmethod
    def _git_commit() -> str:
        try:
            return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True, stderr=subprocess.DEVNULL).strip()
        except Exception:
            return "unknown"

    def _handle_signal(self, _signum: int, _frame: Any) -> None:
        self._shutdown = True


def run_controller(config: MissionControllerConfig) -> int:
    return AutonomousMissionController(config).run()
