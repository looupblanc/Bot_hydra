from __future__ import annotations

from collections.abc import Callable, Sequence

import numpy as np
import pandas as pd


def audit_no_lookahead(df: pd.DataFrame) -> tuple[bool, str]:
    required = ["rolling_vol_20", "rolling_vol_60", "momentum_20", "range_expansion"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        return False, f"missing_features:{','.join(missing)}"
    if df[required].isna().mean().max() > 0.10:
        return False, "too_many_feature_nans"
    return True, "passed"


def assert_no_future_dependency(
    raw_df: pd.DataFrame,
    feature_builder: Callable[[pd.DataFrame], pd.DataFrame],
    feature_columns: Sequence[str] | None = None,
    cutoffs: Sequence[int] | None = None,
) -> tuple[bool, str]:
    original = feature_builder(raw_df.copy(deep=True)).reset_index(drop=True)
    if cutoffs is None:
        cutoffs = _default_cutoffs(len(raw_df))
    columns = list(feature_columns) if feature_columns else [c for c in original.columns if c not in {"created_at"}]
    for cutoff in cutoffs:
        if cutoff < 0 or cutoff >= len(raw_df) - 1:
            continue
        mutated_raw = _mutate_future_bars(raw_df, cutoff)
        mutated = feature_builder(mutated_raw).reset_index(drop=True)
        comparable = [c for c in columns if c in mutated.columns]
        try:
            pd.testing.assert_frame_equal(
                original.loc[:cutoff, comparable],
                mutated.loc[:cutoff, comparable],
                check_dtype=False,
                check_exact=False,
                atol=1e-10,
                rtol=1e-10,
            )
        except AssertionError as exc:
            return False, f"future_dependency_detected_at_cutoff_{cutoff}: {exc}"
    return True, "passed"


def _default_cutoffs(length: int) -> list[int]:
    if length < 5:
        return []
    candidates = {max(1, length // 4), max(1, length // 2), max(1, (length * 3) // 4)}
    return sorted(c for c in candidates if c < length - 1)


def _mutate_future_bars(raw_df: pd.DataFrame, cutoff: int) -> pd.DataFrame:
    out = raw_df.copy(deep=True).reset_index(drop=True)
    future_idx = out.index[out.index > cutoff]
    if len(future_idx) == 0:
        return out
    factors = np.linspace(1.25, 2.0, len(future_idx))
    for col in ("open", "high", "low", "close"):
        if col in out.columns:
            out.loc[future_idx, col] = pd.to_numeric(out.loc[future_idx, col], errors="raise").to_numpy(dtype=float) * factors
    if "volume" in out.columns:
        out.loc[future_idx, "volume"] = pd.to_numeric(out.loc[future_idx, "volume"], errors="raise").to_numpy(dtype=float) * 3
    if {"open", "high", "low", "close"}.issubset(out.columns):
        out.loc[future_idx, "high"] = out.loc[future_idx, ["open", "high", "close"]].max(axis=1)
        out.loc[future_idx, "low"] = out.loc[future_idx, ["open", "low", "close"]].min(axis=1)
    return out
