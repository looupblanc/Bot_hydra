from __future__ import annotations

from collections import defaultdict
from typing import Any


def family_false_discovery_proxy(rows: list[dict[str, Any]]) -> dict[str, dict[str, float]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get("family") or "unknown")].append(row)
    out: dict[str, dict[str, float]] = {}
    for family, members in grouped.items():
        total = len(members)
        strong = sum(1 for row in members if float(row.get("promotion_score") or 0.0) >= 0.65)
        topstep = sum(1 for row in members if row.get("validation_status") == "TOPSTEP_VIABLE")
        intensity_penalty = min(0.75, total / 25000.0)
        signal_rate = (strong + topstep) / max(total, 1)
        out[family] = {
            "trial_count": float(total),
            "strong_count": float(strong),
            "topstep_viable_count": float(topstep),
            "signal_rate": signal_rate,
            "false_discovery_risk_proxy": max(0.0, min(1.0, 1.0 - signal_rate * 10.0 + intensity_penalty)),
        }
    return out

