from __future__ import annotations

import hashlib
import json
from itertools import combinations
from typing import Any

import numpy as np
import pandas as pd

from hydra.research.component_attribution import ComponentDefinition, component_effect
from hydra.research.matched_nulls import evaluate_matched_nulls


def ablation_id(lane: str, components: list[str]) -> str:
    raw = json.dumps({"lane": lane, "components": sorted(components)}, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def component_combinations(components: list[ComponentDefinition]) -> list[list[ComponentDefinition]]:
    combos = [[component] for component in components]
    combos.extend([list(items) for items in combinations(components, 2)])
    combos.append(list(components))
    return combos


def evaluate_component_ablation(
    frame: pd.DataFrame,
    components: list[ComponentDefinition],
    *,
    lane: str,
    forward_col: str = "forward_return",
    seed: int = 0,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for combo in component_combinations(components):
        combo_cols = [component.column for component in combo]
        combo_id = ablation_id(lane, combo_cols)
        signal = _combo_signal(frame, combo)
        null_result = evaluate_matched_nulls(frame, signal, frame[forward_col], signal_id=combo_id, seed=seed)
        effects = [component_effect(frame, component, forward_col=forward_col) for component in combo]
        out.append(
            {
                "ablation_id": combo_id,
                "lane": lane,
                "components": combo_cols,
                "component_count": len(combo),
                "component_effects": effects,
                "matched_null": null_result.to_dict(),
                "incremental_value": bool(null_result.beats_all_required and any(abs(item.get("effect", 0.0)) > 0 for item in effects)),
            }
        )
    return out


def remove_component_signal(frame: pd.DataFrame, components: list[ComponentDefinition], removed_column: str) -> pd.Series:
    kept = [component for component in components if component.column != removed_column]
    return _combo_signal(frame, kept)


def _combo_signal(frame: pd.DataFrame, components: list[ComponentDefinition]) -> pd.Series:
    if not components:
        return pd.Series(0, index=frame.index, dtype=int)
    values = []
    for component in components:
        series = pd.to_numeric(frame[component.column], errors="coerce").fillna(0.0)
        values.append(np.sign(series) * int(component.expected_sign))
    stacked = np.vstack([v.to_numpy() for v in values])
    mean_signal = np.nanmean(stacked, axis=0)
    out = pd.Series(0, index=frame.index, dtype=int)
    out.loc[mean_signal > 0.25] = 1
    out.loc[mean_signal < -0.25] = -1
    return out
