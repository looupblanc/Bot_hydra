from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from hydra.utils.config import project_path
from hydra.utils.time import utc_now_iso


def write_mission_checkpoint(mission_id: str, payload: dict[str, Any]) -> Path:
    stamp = utc_now_iso().replace("-", "").replace(":", "").replace("+00:00", "Z")
    path = project_path("reports", "checkpoints", "mission", f"{mission_id}_{stamp}.md")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f"# HYDRA Mission Checkpoint {mission_id}\n\n```json\n"
        + json.dumps(payload, indent=2, sort_keys=True, default=str)
        + "\n```\n",
        encoding="utf-8",
    )
    return path


def write_mission_summary(mission_id: str, payload: dict[str, Any]) -> Path:
    path = project_path("mission", "state", f"{mission_id}_latest_summary.md")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f"# HYDRA Autonomous Mission Summary {mission_id}\n\n"
        "Historical research only. No live trading approval.\n\n"
        "```json\n"
        + json.dumps(payload, indent=2, sort_keys=True, default=str)
        + "\n```\n",
        encoding="utf-8",
    )
    return path
