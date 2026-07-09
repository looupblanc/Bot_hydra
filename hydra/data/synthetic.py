from __future__ import annotations

import numpy as np
import pandas as pd


def generate_synthetic_ohlcv(symbol: str, timeframe: str, seed: int, bars: int = 1500) -> pd.DataFrame:
    """Regime-switching OHLCV for smoke tests only; not evidence of edge."""
    rng = np.random.default_rng(seed + abs(hash((symbol, timeframe))) % 100_000)
    idx = pd.date_range("2020-01-01", periods=bars, freq="30min" if timeframe == "intraday" else "D")
    regimes = rng.choice([0, 1, 2], size=bars, p=[0.55, 0.25, 0.20])
    vol = np.where(regimes == 0, 0.0025, np.where(regimes == 1, 0.006, 0.011))
    drift = np.where(regimes == 1, 0.00035, np.where(regimes == 2, -0.00015, 0.0))
    shocks = rng.normal(drift, vol)
    jump_mask = rng.random(bars) < 0.015
    shocks[jump_mask] += rng.normal(0, 0.025, jump_mask.sum())
    close = 100 * np.exp(np.cumsum(shocks))
    open_ = np.r_[close[0], close[:-1]]
    spread = np.abs(rng.normal(0.0015, 0.001, bars)) * close
    high = np.maximum(open_, close) + spread
    low = np.minimum(open_, close) - spread
    volume = rng.integers(500, 5000, bars) * (1 + regimes)
    session = np.tile(np.arange(3), int(np.ceil(bars / 3)))[:bars]
    return pd.DataFrame(
        {
            "timestamp": idx,
            "symbol": symbol,
            "timeframe": timeframe,
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
            "session_id": session,
        }
    )
