from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class VolatilityShapeConfig:
    short_window: int = 10
    medium_window: int = 40
    long_window: int = 120
    compression_quantile_window: int = 800


def volatility_shape_features(df: pd.DataFrame, config: VolatilityShapeConfig | None = None) -> pd.DataFrame:
    config = config or VolatilityShapeConfig()
    frame = df.copy()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    by_symbol = frame.groupby("symbol", group_keys=False)
    returns = by_symbol["close"].pct_change()
    short = returns.groupby(frame["symbol"]).rolling(config.short_window, min_periods=5).std().reset_index(level=0, drop=True)
    medium = returns.groupby(frame["symbol"]).rolling(config.medium_window, min_periods=10).std().reset_index(level=0, drop=True)
    long = returns.groupby(frame["symbol"]).rolling(config.long_window, min_periods=20).std().reset_index(level=0, drop=True)
    frame["vol_shape_slope"] = (short - medium) / long.replace(0.0, np.nan)
    frame["vol_shape_convexity"] = (short - 2 * medium + long) / long.replace(0.0, np.nan)
    frame["range_asymmetry"] = ((frame["close"] - frame["open"]) / (frame["high"] - frame["low"]).replace(0.0, np.nan)).clip(-2, 2)
    rolling_q = frame.groupby("symbol")["vol_shape_slope"].transform(
        lambda item: item.rolling(config.compression_quantile_window, min_periods=100).quantile(0.25).shift(1)
    )
    frame["compression_state"] = frame["vol_shape_slope"] < rolling_q
    frame["expansion_acceleration"] = frame.groupby("symbol")["vol_shape_slope"].diff()
    frame["feature"] = frame["vol_shape_convexity"].fillna(0.0) + frame["range_asymmetry"].fillna(0.0) * 0.1
    frame["forward_return"] = by_symbol["close"].pct_change(40).shift(-40)
    frame["signal"] = 0
    mask = frame["compression_state"] & (frame["expansion_acceleration"] > 0)
    direction = np.sign(frame.loc[mask, "range_asymmetry"]).replace(0, 1).fillna(0).astype(int)
    frame.loc[mask, "signal"] = direction
    return frame
