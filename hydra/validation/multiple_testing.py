from __future__ import annotations

import math
from collections import Counter
from typing import Any


def effective_independent_trials(total_trials: int, duplicate_clusters: int, average_correlation: float = 0.35) -> float:
    if total_trials <= 0:
        return 0.0
    unique = max(duplicate_clusters, 1)
    corr = max(0.0, min(0.99, average_correlation))
    return max(1.0, unique + (total_trials - unique) * (1.0 - corr))


def selection_adjusted_score(raw_score: float, total_trials: int, effective_trials: float) -> float:
    penalty = math.sqrt(max(math.log(max(effective_trials, 1.0)), 0.0)) / max(math.sqrt(max(total_trials, 1)), 1.0)
    return max(0.0, min(1.0, raw_score - penalty))


def family_trial_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    return dict(Counter(str(row.get("family") or "unknown") for row in rows))

