from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class OpeningAuctionConfig:
    opening_minutes: int = 30
    continuation_minutes: int = 60
    displacement_threshold: float = 0.0025


def opening_auction_features(df: pd.DataFrame, config: OpeningAuctionConfig | None = None) -> pd.DataFrame:
    config = config or OpeningAuctionConfig()
    frame = df.copy()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    frame["date"] = frame["timestamp"].dt.date.astype(str)
    frame["minute_utc"] = frame["timestamp"].dt.hour * 60 + frame["timestamp"].dt.minute
    open_utc = 14 * 60 + 30
    frame["rth_minute"] = frame["minute_utc"] - open_utc
    in_open = (frame["rth_minute"] >= 0) & (frame["rth_minute"] < config.opening_minutes)
    grouped = frame.groupby(["symbol", "date"], sort=True)
    opening_high = frame["high"].where(in_open).groupby([frame["symbol"], frame["date"]]).transform("max")
    opening_low = frame["low"].where(in_open).groupby([frame["symbol"], frame["date"]]).transform("min")
    opening_open = grouped["open"].transform("first")
    opening_mid = (opening_high + opening_low) / 2.0
    frame["opening_displacement"] = (opening_mid - opening_open) / opening_open.replace(0.0, np.nan)
    frame["opening_effort"] = ((opening_high - opening_low) / opening_open.replace(0.0, np.nan)).fillna(0.0)
    frame["path_asymmetry"] = ((frame["close"] - opening_mid) / (opening_high - opening_low).replace(0.0, np.nan)).clip(-3, 3)
    continuation_window = (frame["rth_minute"] >= config.opening_minutes) & (
        frame["rth_minute"] <= config.opening_minutes + config.continuation_minutes
    )
    frame["failed_continuation"] = continuation_window & (
        np.sign(frame["opening_displacement"]) * (frame["close"] - opening_mid) < 0
    )
    frame["feature"] = frame["opening_displacement"].fillna(0.0) * frame["opening_effort"].fillna(0.0)
    frame["forward_return"] = frame.groupby("symbol")["close"].pct_change(30).shift(-30)
    frame["signal"] = 0
    mask = frame["failed_continuation"] & (frame["opening_displacement"].abs() > config.displacement_threshold)
    frame.loc[mask, "signal"] = -np.sign(frame.loc[mask, "opening_displacement"]).fillna(0).astype(int)
    return frame
