from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class ComponentDefinition:
    component_id: str
    lane: str
    description: str
    expected_sign: int
    column: str


def component_effect(frame: pd.DataFrame, component: ComponentDefinition, forward_col: str = "forward_return") -> dict[str, Any]:
    data = frame[[component.column, forward_col]].replace([np.inf, -np.inf], np.nan).dropna()
    if len(data) < 50:
        return {"component_id": component.component_id, "rows": int(len(data)), "status": "INSUFFICIENT_EVIDENCE", "effect": 0.0}
    signal = np.sign(data[component.column].astype(float)) * int(component.expected_sign)
    forward = data[forward_col].astype(float)
    effect = float((signal * forward).mean())
    stderr = float((signal * forward).std(ddof=1) / max(len(data) ** 0.5, 1.0))
    return {
        "component_id": component.component_id,
        "lane": component.lane,
        "rows": int(len(data)),
        "status": "OK",
        "effect": effect,
        "stderr": stderr,
        "lower_95": effect - 1.96 * stderr,
        "upper_95": effect + 1.96 * stderr,
        "direction_ok": bool(np.sign(effect) == np.sign(component.expected_sign) or abs(effect) < 1e-12),
    }


def false_discovery_adjustment(p_values: list[float]) -> list[float]:
    if not p_values:
        return []
    n = len(p_values)
    order = sorted(range(n), key=lambda i: p_values[i])
    adjusted = [0.0] * n
    running = 1.0
    for rank, idx in reversed(list(enumerate(order, start=1))):
        running = min(running, p_values[idx] * n / rank)
        adjusted[idx] = min(1.0, running)
    return adjusted
