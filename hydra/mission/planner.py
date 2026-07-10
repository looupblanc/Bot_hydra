from __future__ import annotations

from typing import Any

from hydra.mission.decision_engine import select_best_action


def plan_next_action(state: dict[str, Any]) -> dict[str, Any]:
    return select_best_action(state).to_dict()

