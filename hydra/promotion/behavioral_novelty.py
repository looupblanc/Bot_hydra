from __future__ import annotations

from typing import Any

from hydra.promotion.cluster_calibration import sketch_similarity


def behavioral_novelty_score(candidate: dict[str, Any], existing: list[dict[str, Any]]) -> float:
    if not existing:
        return 1.0
    max_similarity = max(sketch_similarity(candidate, item) for item in existing)
    return round(max(0.0, 1.0 - max_similarity), 6)


def filter_behaviorally_novel(candidates: list[dict[str, Any]], *, threshold: float = 0.15) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    accepted: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for candidate in candidates:
        novelty = behavioral_novelty_score(candidate, accepted)
        row = candidate | {"behavioral_novelty": novelty}
        if novelty < threshold:
            rejected.append(row | {"rejection_reason": "low_behavioral_novelty"})
        else:
            accepted.append(row)
    return accepted, rejected
