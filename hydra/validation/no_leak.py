from __future__ import annotations

import pandas as pd


def audit_no_lookahead(df: pd.DataFrame) -> tuple[bool, str]:
    required = ["rolling_vol_20", "rolling_vol_60", "momentum_20", "range_expansion"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        return False, f"missing_features:{','.join(missing)}"
    if df[required].isna().mean().max() > 0.10:
        return False, "too_many_feature_nans"
    return True, "passed"
