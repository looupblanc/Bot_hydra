from __future__ import annotations

import pandas as pd


def add_volatility_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    ret = out["close"].pct_change()
    out["return"] = ret.fillna(0.0)
    out["rolling_vol_20"] = ret.rolling(20, min_periods=20).std().shift(1).fillna(0.0)
    out["rolling_vol_60"] = ret.rolling(60, min_periods=60).std().shift(1).fillna(0.0)
    vol_q = out["rolling_vol_60"].rolling(200, min_periods=50).quantile(0.65).shift(1)
    out["vol_regime_high"] = (out["rolling_vol_60"] > vol_q.fillna(out["rolling_vol_60"].median())).astype(int)
    out["range_pct"] = ((out["high"] - out["low"]) / out["close"]).shift(1).fillna(0.0)
    out["range_expansion"] = (out["range_pct"] / out["range_pct"].rolling(40, min_periods=20).mean().shift(1)).fillna(1.0)
    out["compression_score"] = (out["rolling_vol_20"] / out["rolling_vol_60"].replace(0, pd.NA)).fillna(1.0)
    return out
