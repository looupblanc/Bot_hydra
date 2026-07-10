from __future__ import annotations

from typing import Any


def detect_plateau(state: dict[str, Any]) -> dict[str, Any]:
    cycles = int(state.get("cycle_count", 0))
    validated = int(state.get("validated_mechanisms", 0))
    repeated_blocker = state.get("current_blocker") == state.get("previous_blocker") and state.get("current_blocker")
    plateau = cycles >= 3 and validated == 0 and bool(repeated_blocker)
    return {
        "plateau": plateau,
        "reason": "repeated_blocker_without_validation" if plateau else "not_plateaued",
        "recommended_pivot_level": 1 if plateau else 0,
    }

