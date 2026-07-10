from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class InjectedEdgeSpec:
    edge_id: str
    edge_type: str
    direction: int
    effect_size: float
    event_frequency: float
    horizon: int
    persistence_folds: int
    cross_market: bool = False
    regime_specific: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def inject_edge(frame: pd.DataFrame, spec: InjectedEdgeSpec, *, seed: int = 0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    out = frame.copy()
    event = rng.random(len(out)) < spec.event_frequency
    if spec.regime_specific:
        threshold = out["vol_state"].rolling(200, min_periods=50).median().bfill()
        event &= out["vol_state"] >= threshold
    out[f"signal_{spec.edge_id}"] = event.astype(int) * int(spec.direction)
    future_adjustment = np.zeros(len(out))
    indices = np.flatnonzero(event)
    for idx in indices:
        target = min(idx + spec.horizon, len(out) - 1)
        if spec.edge_type == "mean_shift":
            future_adjustment[target] += spec.direction * spec.effect_size
        elif spec.edge_type == "tail_risk":
            future_adjustment[target] -= abs(spec.effect_size) if spec.direction < 0 else abs(spec.effect_size)
        elif spec.edge_type == "volatility_prediction":
            future_adjustment[target] += rng.choice([-1.0, 1.0]) * abs(spec.effect_size)
        elif spec.edge_type == "path_asymmetry":
            future_adjustment[target] += spec.direction * spec.effect_size * 0.5
    close = out["close"].to_numpy(dtype=float)
    adjusted = close * np.exp(np.cumsum(future_adjustment))
    out["close"] = adjusted
    out["high"] = np.maximum(out["high"].to_numpy(dtype=float), adjusted)
    out["low"] = np.minimum(out["low"].to_numpy(dtype=float), adjusted)
    out[f"known_effect_{spec.edge_id}"] = spec.direction
    return out

