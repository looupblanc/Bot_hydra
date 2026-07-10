from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone

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
