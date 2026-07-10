from __future__ import annotations

from collections import Counter
from typing import Any

from hydra.promotion.gate_distance import failed_gates, gate_distance_summary
from hydra.promotion.gate_policy import classify_failure


def attribute_candidate_failure(row: dict[str, Any]) -> dict[str, Any]:
    failures = failed_gates(row.get("gate_history_json"))
    primary = failures[0] if failures else {}
    reason = row.get("rejection_reason") or primary.get("reason")
    policy = classify_failure(str(reason or ""), str(primary.get("severity") or ""))
    gate_counts = Counter(str(g.get("name", "UNKNOWN")) for g in failures)
    return {
        "primary_reason": reason,
        "policy_classification": policy.classification,
        "recommended_action": policy.action,
        "failed_gate_count": len(failures),
        "failed_gate_counts": dict(gate_counts),
        "gate_distance": gate_distance_summary(row),
    }


def failure_distribution(rows: list[dict[str, Any]], by: str) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for row in rows:
        key = f"{row.get(by) or 'unknown'}::{row.get('rejection_reason') or row.get('validation_status')}"
        counts[key] += 1
    return dict(counts.most_common())

