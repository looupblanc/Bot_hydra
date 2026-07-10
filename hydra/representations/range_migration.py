from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class RangeMigrationConfig:
    lookback: int = 60
    extreme_fraction: float = 0.20


def range_migration_features(df: pd.DataFrame, config: RangeMigrationConfig | None = None) -> pd.DataFrame:
    config = config or RangeMigrationConfig()
    frame = df.copy()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    grouped = frame.groupby("symbol", group_keys=False)
    rolling_high = grouped["high"].rolling(config.lookback, min_periods=20).max().reset_index(level=0, drop=True).shift(1)
    rolling_low = grouped["low"].rolling(config.lookback, min_periods=20).min().reset_index(level=0, drop=True).shift(1)
    range_width = (rolling_high - rolling_low).replace(0.0, np.nan)
    frame["range_location"] = ((frame["close"] - rolling_low) / range_width).clip(0, 1)
    frame["upper_time_pressure"] = grouped["range_location"].transform(
        lambda item: (item > 1.0 - config.extreme_fraction).rolling(config.lookback, min_periods=20).mean().shift(1)
    )
    frame["lower_time_pressure"] = grouped["range_location"].transform(
        lambda item: (item < config.extreme_fraction).rolling(config.lookback, min_periods=20).mean().shift(1)
    )
    frame["migration_imbalance"] = frame["upper_time_pressure"] - frame["lower_time_pressure"]
    frame["feature"] = frame["migration_imbalance"].fillna(0.0)
    frame["forward_return"] = grouped["close"].pct_change(30).shift(-30)
    frame["signal"] = 0
    continuation = frame["migration_imbalance"].abs() > 0.35
    frame.loc[continuation, "signal"] = np.sign(frame.loc[continuation, "migration_imbalance"]).fillna(0).astype(int)
    return frame
