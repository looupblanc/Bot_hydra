from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class ResidualStateConfig:
    z_window: int = 120
    entry_z: float = 1.25
    exit_z: float = 0.25


def residual_zscore(residual: pd.Series, window: int) -> pd.Series:
    mean = residual.rolling(window, min_periods=max(10, window // 3)).mean().shift(1)
    std = residual.rolling(window, min_periods=max(10, window // 3)).std().shift(1)
    return ((residual - mean) / std.replace(0.0, pd.NA)).astype(float)


def classify_residual_state(zscore: pd.Series, config: ResidualStateConfig) -> pd.Series:
    state = pd.Series("neutral", index=zscore.index, dtype=object)
    state[zscore >= config.entry_z] = "left_rich"
    state[zscore <= -config.entry_z] = "left_cheap"
    state[zscore.abs() <= config.exit_z] = "normalized"
    return state
