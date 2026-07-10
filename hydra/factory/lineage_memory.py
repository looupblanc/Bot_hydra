from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from hydra.utils.config import project_path


class LineageMemory:
    def __init__(self, path: str = "reports/gate_aware_remediation/lineage_memory.json") -> None:
        self.path = project_path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.data: dict[str, Any] = {"parents": {}, "policies": {}, "killed": [], "promising": []}
        if self.path.exists():
            self.data.update(json.loads(self.path.read_text(encoding="utf-8")))

    def record_child(self, parent_id: str, child_id: str, policy: str, observed_effect: dict[str, Any]) -> None:
        self.data.setdefault("parents", {}).setdefault(parent_id, []).append(
            {"child_id": child_id, "policy": policy, "observed_effect": observed_effect}
        )
        self.data.setdefault("policies", {}).setdefault(policy, []).append(observed_effect)
        self.flush()

    def freeze_branch(self, branch: str, reason: str) -> None:
        self.data.setdefault("killed", []).append({"branch": branch, "reason": reason})
        self.flush()

    def mark_promising(self, branch: str, reason: str) -> None:
        self.data.setdefault("promising", []).append({"branch": branch, "reason": reason})
        self.flush()

    def flush(self) -> None:
        self.path.write_text(json.dumps(self.data, indent=2, sort_keys=True), encoding="utf-8")

