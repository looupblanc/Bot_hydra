from __future__ import annotations

import pandas as pd


def add_path_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    ret = out["close"].pct_change()
    out["path_persistence_10"] = ret.rolling(10, min_periods=10).apply(lambda x: abs(x.sum()) / (abs(x).sum() + 1e-12)).shift(1).fillna(0.0)
    out["momentum_20"] = out["close"].pct_change(20).shift(1).fillna(0.0)
    out["momentum_exhaustion"] = (out["momentum_20"] / out["rolling_vol_20"].replace(0, pd.NA)).fillna(0.0)
    return out
