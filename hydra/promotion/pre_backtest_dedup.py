from __future__ import annotations

from typing import Any


def reject_logical_duplicates(sketches: list[dict[str, Any]], *, max_overlap: float = 0.90) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    accepted: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    seen = set()
    for sketch in sketches:
        logical = sketch.get("logical_fingerprint")
        if logical in seen:
            rejected.append(sketch | {"rejection_reason": "logical_duplicate"})
            continue
        if any(_overlap(sketch, prior) >= max_overlap for prior in accepted):
            rejected.append(sketch | {"rejection_reason": "expected_event_overlap"})
            continue
        seen.add(logical)
        accepted.append(sketch)
    return accepted, rejected


def _overlap(left: dict[str, Any], right: dict[str, Any]) -> float:
    if left.get("event_signature") == right.get("event_signature"):
        return 1.0
    if left.get("direction_signature") == right.get("direction_signature") and left.get("event_count") == right.get("event_count"):
        return 0.95
    return 0.0
