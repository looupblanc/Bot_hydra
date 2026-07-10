from __future__ import annotations

import json
from typing import Any


def failed_gates(gate_history_json: str | None) -> list[dict[str, Any]]:
    if not gate_history_json:
        return []
    try:
        gates = json.loads(gate_history_json)
    except json.JSONDecodeError:
        return []
    return [gate for gate in gates if not gate.get("passed")]


def gate_distance_summary(row: dict[str, Any]) -> dict[str, Any]:
    failures = failed_gates(row.get("gate_history_json"))
    target_ratio = _clip(float(row.get("net_profit") or 0.0) / 9000.0)
    mll_buffer_ratio = _clip(float(row.get("combine_min_mll_buffer") or 0.0) / 4500.0)
    payout_ratio = _clip(float(row.get("trader_net_payout") or 0.0) / 5000.0)
    return {
        "failed_gate_count": len(failures),
        "failed_gates": [f"{g.get('name')}:{g.get('severity')}:{g.get('reason')}" for g in failures],
        "target_distance": round(1.0 - target_ratio, 6),
        "mll_buffer_distance": round(1.0 - mll_buffer_ratio, 6),
        "payout_distance": round(1.0 - payout_ratio, 6),
        "promotion_distance": round(1.0 - float(row.get("promotion_score") or 0.0), 6),
    }


def _clip(value: float) -> float:
    return max(0.0, min(1.0, value))

