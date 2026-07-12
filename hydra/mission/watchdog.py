from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any

from hydra.mission.mission_state import MissionPaths, mission_paths


@dataclass(frozen=True)
class HeartbeatStatus:
    path: str
    exists: bool
    fresh: bool
    age_seconds: float | None
    payload: dict[str, object]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def heartbeat_age_seconds(state_dir: str = "mission/state") -> float | None:
    paths = mission_paths(state_dir)
    if not paths.heartbeat_path.exists():
        return None
    payload = json.loads(paths.heartbeat_path.read_text(encoding="utf-8"))
    stamp = datetime.fromisoformat(payload["heartbeat_at_utc"])
    return (datetime.now(timezone.utc) - stamp).total_seconds()


def heartbeat_is_stale(max_age_seconds: float = 180.0, state_dir: str = "mission/state") -> bool:
    age = heartbeat_age_seconds(state_dir)
    return age is None or age > max_age_seconds


def heartbeat_status(paths: MissionPaths | None = None, *, max_age_seconds: float = 180.0) -> HeartbeatStatus:
    paths = paths or mission_paths()
    if not paths.heartbeat_path.exists():
        return HeartbeatStatus(str(paths.heartbeat_path), False, False, None, {})
    payload = json.loads(paths.heartbeat_path.read_text(encoding="utf-8"))
    stamp = datetime.fromisoformat(str(payload["heartbeat_at_utc"]))
    age = (datetime.now(timezone.utc) - stamp).total_seconds()
    return HeartbeatStatus(str(paths.heartbeat_path), True, age <= max_age_seconds, float(age), payload)


def scheduler_health(
    heartbeat: HeartbeatStatus,
    snapshot: dict[str, Any],
    experiment_counts: dict[str, int],
    *,
    now: datetime | None = None,
    max_queue_age_seconds: float = 90.0,
    max_transition_age_seconds: float = 90.0,
) -> dict[str, Any]:
    now = now or datetime.now(timezone.utc)
    if not heartbeat.exists or not heartbeat.fresh:
        return {"classification": "SERVICE_FAILED", "healthy": False, "reason": "heartbeat_missing_or_stale"}
    if not snapshot.get("governance_passed", False):
        return {"classification": "INTEGRITY_BLOCKED", "healthy": False, "reason": "governance_not_passed"}
    phase = str(snapshot.get("current_phase") or heartbeat.payload.get("current_phase") or "")
    blocker = str(snapshot.get("current_blocker") or heartbeat.payload.get("current_blocker") or "")
    service_state = str(snapshot.get("service_state") or "")
    if service_state == "FAILED":
        return {"classification": "SERVICE_FAILED", "healthy": False, "reason": "controller_reported_failed"}
    if service_state in {"STOPPED", "STOPPED_CLEANLY"} or phase in {"STOPPED", "STOPPED_CLEANLY"}:
        return {"classification": "SERVICE_FAILED", "healthy": False, "reason": "mission_service_stopped"}
    if phase == "ENGINEERING_BLOCKED" or blocker.startswith("MISSING_EXPERIMENT_HANDLER"):
        return {"classification": "ENGINEERING_BLOCKED", "healthy": False, "reason": blocker or phase}
    if phase in {"EXPERIMENT_BLOCKED", "RETRY_EXHAUSTED"} or blocker.startswith("EXPERIMENT_FAILED"):
        return {"classification": "EXPERIMENT_BLOCKED", "healthy": False, "reason": blocker or phase}
    if phase == "INTEGRITY_BLOCKED":
        return {"classification": "INTEGRITY_BLOCKED", "healthy": False, "reason": blocker or phase}
    queued = int(experiment_counts.get("QUEUED", 0))
    running = int(experiment_counts.get("RUNNING", 0))
    last_progress = snapshot.get("last_progress_at_utc") or heartbeat.payload.get("last_progress_at_utc")
    if last_progress:
        try:
            progress_age = (now - datetime.fromisoformat(str(last_progress))).total_seconds()
        except ValueError:
            progress_age = float("inf")
    else:
        progress_age = float("inf")
    current_experiment = snapshot.get("current_experiment") or heartbeat.payload.get("current_experiment") or {}
    lease_value = current_experiment.get("lease_expires_at") if isinstance(current_experiment, dict) else None
    lease_fresh = False
    if lease_value:
        try:
            lease_fresh = datetime.fromisoformat(str(lease_value)) > now
        except ValueError:
            lease_fresh = False
    if running > 0 and not lease_fresh:
        return {
            "classification": "ALIVE_BUT_SCHEDULER_STALLED",
            "healthy": False,
            "reason": "running_experiment_lease_missing_or_expired",
            "progress_age_seconds": progress_age,
        }
    if queued > 0 and running == 0 and progress_age > max_queue_age_seconds:
        return {
            "classification": "ALIVE_BUT_SCHEDULER_STALLED",
            "healthy": False,
            "reason": "queued_or_running_work_without_recent_progress",
            "progress_age_seconds": progress_age,
        }
    if (running > 0 and lease_fresh) or queued > 0:
        return {
            "classification": "HEALTHY_AND_PROGRESSING",
            "healthy": True,
            "reason": "experiment_or_planning_progress_present",
        }
    if phase in {"PLANNING_NEXT_ACTION", "RECOVERING", "RETRY_SCHEDULED"}:
        if progress_age > max_transition_age_seconds:
            return {
                "classification": "ALIVE_BUT_SCHEDULER_STALLED",
                "healthy": False,
                "reason": "transitional_phase_without_recent_progress",
                "progress_age_seconds": progress_age,
            }
        return {
            "classification": "HEALTHY_AND_PROGRESSING",
            "healthy": True,
            "reason": "recent_transitional_progress",
            "progress_age_seconds": progress_age,
        }
    # The heartbeat is the freshest view of a just-selected wait action.  A
    # durable snapshot from an older controller can still contain the prior
    # experiment action until the next state commit, so prefer heartbeat data.
    current_action = heartbeat.payload.get("current_action") or snapshot.get("current_action") or {}
    if not isinstance(current_action, dict):
        current_action = {}
    next_wake = (
        snapshot.get("next_wake_at_utc")
        or heartbeat.payload.get("next_wake_at_utc")
        or current_action.get("next_wake_at_utc")
    )
    planned_action = (
        snapshot.get("planned_action_id")
        or heartbeat.payload.get("planned_action_id")
        or current_action.get("action_id")
    )
    bounded_wait = (
        phase == "WAITING_FOR_NEXT_ACTION"
        and current_action.get("action_type") == "WAIT"
    )
    if (phase == "IDLE_SCHEDULED" or bounded_wait) and next_wake and planned_action:
        try:
            deadline = datetime.fromisoformat(str(next_wake))
        except ValueError:
            deadline = now
        if deadline > now:
            return {
                "classification": "HEALTHY_BUT_WAITING_NORMALLY",
                "healthy": True,
                "reason": "future_deadline_and_planned_action_present",
            }
    if phase == "SCHEDULER_STALLED" or (
        queued == 0 and running == 0 and progress_age > max_transition_age_seconds
    ):
        return {
            "classification": "ALIVE_BUT_SCHEDULER_STALLED",
            "healthy": False,
            "reason": "mission_incomplete_without_queue_or_legitimate_deadline",
            "progress_age_seconds": progress_age,
        }
    return {
        "classification": "HEALTHY_AND_PROGRESSING",
        "healthy": True,
        "reason": "recent_scientific_progress",
        "progress_age_seconds": progress_age,
    }
