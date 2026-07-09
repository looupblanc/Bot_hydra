from __future__ import annotations

import pandas as pd


def add_session_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["session_return"] = out.groupby("session_id")["close"].pct_change().shift(1).fillna(0.0)
    out["session_range"] = ((out.groupby("session_id")["high"].cummax() - out.groupby("session_id")["low"].cummin()) / out["close"]).shift(1).fillna(0.0)
    out["prior_session_return"] = out.groupby("session_id")["session_return"].shift(1).fillna(0.0)
    return out
