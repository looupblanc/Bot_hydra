from __future__ import annotations

import pandas as pd


def add_regime_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["compression_regime"] = (out["compression_score"] < 0.75).astype(int)
    out["expansion_regime"] = ((out["compression_score"] > 1.15) | (out["range_expansion"] > 1.4)).astype(int)
    out["regime_transition"] = (out["expansion_regime"].diff().abs().shift(1).fillna(0) > 0).astype(int)
    return out
