from __future__ import annotations

from collections import defaultdict
from typing import Any


def stability_by_month(split_scores: dict[str, float]) -> float:
    if not split_scores:
        return 0.0
    values = list(float(v) for v in split_scores.values())
    return min(values) / max(max(values), 1e-9)


def family_stability(rows: list[dict[str, Any]]) -> dict[str, float]:
    grouped: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get("family") or "unknown")].append(float(row.get("promotion_score") or 0.0))
    return {
        family: (sum(scores) / len(scores) if scores else 0.0)
        for family, scores in grouped.items()
    }

