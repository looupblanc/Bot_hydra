from __future__ import annotations

from typing import Any


def classify_representation(evidence: dict[str, Any]) -> str:
    if evidence.get("roll_artifact"):
        return "FALSIFIED"
    if evidence.get("null_beats_effect"):
        return "FALSIFIED"
    if evidence.get("costs_erase_effect"):
        return "FALSIFIED"
    if evidence.get("stable_direction") and evidence.get("positive_after_costs") and evidence.get("periods_with_signal", 0) >= 2:
        return "SURVIVES"
    if evidence.get("periods_with_signal", 0) < 2 or evidence.get("trade_count", 0) < 30:
        return "INSUFFICIENT_EVIDENCE"
    return "PARTIAL_EVIDENCE"
