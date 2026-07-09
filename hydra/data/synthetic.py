from __future__ import annotations

import hashlib

import numpy as np
import pandas as pd


def generate_synthetic_ohlcv(
    symbol: str,
    timeframe: str,
    seed: int,
    bars: int = 1500,
    diagnostic_relaxed: bool = False,
) -> pd.DataFrame:
    """Regime-switching OHLCV for pipeline diagnostics only; not evidence of edge."""
    rng = np.random.default_rng(seed + _stable_offset(symbol, timeframe))
    if diagnostic_relaxed and bars == 1500:
        bars = 3000
    idx = pd.date_range("2020-01-01", periods=bars, freq="30min" if timeframe == "intraday" else "D")
    regimes = _diagnostic_regimes(rng, bars) if diagnostic_relaxed else rng.choice([0, 1, 2], size=bars, p=[0.55, 0.25, 0.20])
    vol = np.where(regimes == 0, 0.0025, np.where(regimes == 1, 0.006, np.where(regimes == 2, 0.011, 0.008)))
    drift = np.where(regimes == 1, 0.00045, np.where(regimes == 2, -0.00035, np.where(regimes == 3, 0.00012, 0.0)))
    shocks = rng.normal(drift, vol)
    if diagnostic_relaxed:
        shocks = _add_diagnostic_structure(rng, shocks, regimes)
        jump_probability = 0.025
        jump_scale = 0.018
    else:
        jump_probability = 0.015
        jump_scale = 0.025
    jump_mask = rng.random(bars) < jump_probability
    shocks[jump_mask] += rng.normal(0, jump_scale, jump_mask.sum())
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


def _stable_offset(symbol: str, timeframe: str) -> int:
    digest = hashlib.sha256(f"{symbol}:{timeframe}".encode("utf-8")).hexdigest()
    return int(digest[:8], 16) % 100_000


def _diagnostic_regimes(rng: np.random.Generator, bars: int) -> np.ndarray:
    regimes: list[int] = []
    cycle = [0, 1, 0, 2, 3]
    while len(regimes) < bars:
        for regime in cycle:
            length = int(rng.integers(35, 120))
            regimes.extend([regime] * length)
            if len(regimes) >= bars:
                break
    return np.array(regimes[:bars], dtype=int)


def _add_diagnostic_structure(rng: np.random.Generator, shocks: np.ndarray, regimes: np.ndarray) -> np.ndarray:
    out = shocks.copy()
    for i in range(1, len(out)):
        if regimes[i] in (1, 2):
            out[i] += 0.18 * out[i - 1]
        elif regimes[i] == 3:
            out[i] -= 0.28 * out[i - 1]
    burst_mask = (regimes == 3) & (rng.random(len(out)) < 0.035)
    out[burst_mask] += rng.normal(0, 0.014, burst_mask.sum())
    return out
