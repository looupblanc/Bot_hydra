from __future__ import annotations

import pandas as pd

from hydra.features.path_features import add_path_features
from hydra.features.regimes import add_regime_features
from hydra.features.sessions import add_session_features
from hydra.features.volatility import add_volatility_features


def build_market_state(df: pd.DataFrame) -> pd.DataFrame:
    out = add_volatility_features(df)
    out = add_session_features(out)
    out = add_path_features(out)
    out = add_regime_features(out)
    out["state"] = "balanced"
    out.loc[(out["vol_regime_high"] == 1) & (out["expansion_regime"] == 1), "state"] = "high_vol_expansion"
    out.loc[(out["compression_regime"] == 1), "state"] = "compressed"
    out.loc[(out["momentum_exhaustion"].abs() > 2.5), "state"] = "exhausted"
    return out
