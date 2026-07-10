from __future__ import annotations

from typing import Any


def propose_pivot(plateau: dict[str, Any]) -> dict[str, Any]:
    if not plateau.get("plateau"):
        return {"pivot": "NONE", "reason": "no_plateau_detected"}
    level = int(plateau.get("recommended_pivot_level", 1))
    options = {
        1: "change_experiment_or_null",
        2: "change_feature_target_or_horizon",
        3: "change_representation",
        4: "change_economic_mechanism",
        5: "change_market_ecology",
        6: "change_research_methodology",
        7: "acquire_targeted_data",
        8: "scientific_exhaustion_report",
    }
    return {"pivot": options.get(level, "change_experiment_or_null"), "level": level, "reason": plateau.get("reason")}

