from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class SyntheticMarketConfig:
    name: str
    rows: int = 5000
    seed: int = 0
    volatility: float = 0.001
    autocorrelation: float = 0.05
    volatility_clustering: float = 0.85
    start: str = "2024-01-01"


def generate_synthetic_market(config: SyntheticMarketConfig) -> pd.DataFrame:
    rng = np.random.default_rng(config.seed)
    vol = np.empty(config.rows)
    vol[0] = config.volatility
    returns = np.empty(config.rows)
    returns[0] = rng.normal(0.0, vol[0])
    for i in range(1, config.rows):
        shock = abs(rng.normal(0.0, config.volatility))
        vol[i] = config.volatility_clustering * vol[i - 1] + (1.0 - config.volatility_clustering) * shock
        returns[i] = config.autocorrelation * returns[i - 1] + rng.normal(0.0, max(vol[i], config.volatility * 0.25))
    close = 100.0 * np.exp(np.cumsum(returns))
    timestamps = pd.date_range(config.start, periods=config.rows, freq="1min", tz="UTC")
    frame = pd.DataFrame(
        {
            "timestamp": timestamps,
            "symbol": config.name,
            "open": close,
            "high": close * (1.0 + np.abs(returns) * 0.5),
            "low": close * (1.0 - np.abs(returns) * 0.5),
            "close": close,
            "volume": rng.integers(50, 500, size=config.rows),
            "base_return": returns,
            "vol_state": vol,
        }
    )
    frame["session_phase"] = frame["timestamp"].dt.hour
    return frame

