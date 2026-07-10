from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class OvernightInventoryConfig:
    early_rth_minutes: int = 90
    displacement_threshold: float = 0.003


def overnight_inventory_features(df: pd.DataFrame, config: OvernightInventoryConfig | None = None) -> pd.DataFrame:
    config = config or OvernightInventoryConfig()
    frame = df.copy()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    frame["date"] = frame["timestamp"].dt.date.astype(str)
    frame["minute_utc"] = frame["timestamp"].dt.hour * 60 + frame["timestamp"].dt.minute
    rth_open_utc = 14 * 60 + 30
    frame["is_early_rth"] = (frame["minute_utc"] >= rth_open_utc) & (frame["minute_utc"] < rth_open_utc + config.early_rth_minutes)
    grouped = frame.groupby(["symbol", "date"], sort=True)
    day_open = grouped["open"].transform("first")
    prior_close = frame.groupby("symbol")["close"].shift(1)
    frame["overnight_displacement"] = (day_open - prior_close) / prior_close.replace(0.0, np.nan)
    early_progress = grouped["close"].transform(lambda item: item.iloc[: config.early_rth_minutes].iloc[-1] if len(item) else np.nan)
    frame["early_rth_response"] = (early_progress - day_open) / day_open.replace(0.0, np.nan)
    frame["inventory_pressure"] = frame["overnight_displacement"].rolling(20, min_periods=5).mean().shift(1)
    frame["feature"] = frame["overnight_displacement"].fillna(0.0)
    frame["forward_return"] = frame.groupby("symbol")["close"].pct_change(30).shift(-30)
    frame["signal"] = 0
    continuation = (frame["overnight_displacement"].abs() > config.displacement_threshold) & (frame["early_rth_response"] * frame["overnight_displacement"] > 0)
    rejection = (frame["overnight_displacement"].abs() > config.displacement_threshold) & (frame["early_rth_response"] * frame["overnight_displacement"] < 0)
    frame.loc[continuation, "signal"] = np.sign(frame.loc[continuation, "overnight_displacement"]).fillna(0).astype(int)
    frame.loc[rejection, "signal"] = -np.sign(frame.loc[rejection, "overnight_displacement"]).fillna(0).astype(int)
    return frame
