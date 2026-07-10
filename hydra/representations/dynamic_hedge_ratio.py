from __future__ import annotations

import numpy as np
import pandas as pd


def rolling_ols_beta(left_returns: pd.Series, right_returns: pd.Series, window: int) -> pd.Series:
    """Past-only beta of left on right using rolling covariance / variance."""
    left = pd.to_numeric(left_returns, errors="coerce")
    right = pd.to_numeric(right_returns, errors="coerce")
    cov = left.rolling(window, min_periods=max(5, window // 3)).cov(right)
    var = right.rolling(window, min_periods=max(5, window // 3)).var()
    return (cov / var.replace(0.0, np.nan)).shift(1).replace([np.inf, -np.inf], np.nan)


def ew_cov_beta(left_returns: pd.Series, right_returns: pd.Series, span: int) -> pd.Series:
    left = pd.to_numeric(left_returns, errors="coerce")
    right = pd.to_numeric(right_returns, errors="coerce")
    left_mean = left.ewm(span=span, adjust=False, min_periods=max(5, span // 3)).mean()
    right_mean = right.ewm(span=span, adjust=False, min_periods=max(5, span // 3)).mean()
    cov = ((left - left_mean) * (right - right_mean)).ewm(span=span, adjust=False, min_periods=max(5, span // 3)).mean()
    var = ((right - right_mean) ** 2).ewm(span=span, adjust=False, min_periods=max(5, span // 3)).mean()
    return (cov / var.replace(0.0, np.nan)).shift(1).replace([np.inf, -np.inf], np.nan)


def robust_rolling_beta(left_returns: pd.Series, right_returns: pd.Series, window: int, clip_sigma: float = 4.0) -> pd.Series:
    left = _winsorize_past(left_returns, window, clip_sigma)
    right = _winsorize_past(right_returns, window, clip_sigma)
    return rolling_ols_beta(left, right, window)


def volatility_normalized_dollar_hedge(
    left_returns: pd.Series,
    right_returns: pd.Series,
    *,
    left_point_value: float,
    right_point_value: float,
    window: int,
) -> pd.Series:
    left_vol = pd.to_numeric(left_returns, errors="coerce").rolling(window, min_periods=max(5, window // 3)).std().shift(1)
    right_vol = pd.to_numeric(right_returns, errors="coerce").rolling(window, min_periods=max(5, window // 3)).std().shift(1)
    beta = (left_vol * left_point_value) / (right_vol * right_point_value).replace(0.0, np.nan)
    return beta.replace([np.inf, -np.inf], np.nan)


def hedge_ratio(
    method: str,
    left_returns: pd.Series,
    right_returns: pd.Series,
    *,
    window: int,
    left_point_value: float,
    right_point_value: float,
) -> pd.Series:
    if method == "rolling_ols":
        return rolling_ols_beta(left_returns, right_returns, window)
    if method == "robust_rolling":
        return robust_rolling_beta(left_returns, right_returns, window)
    if method == "ew_cov":
        return ew_cov_beta(left_returns, right_returns, window)
    if method == "vol_normalized_dollar":
        return volatility_normalized_dollar_hedge(
            left_returns,
            right_returns,
            left_point_value=left_point_value,
            right_point_value=right_point_value,
            window=window,
        )
    raise ValueError(f"Unsupported hedge-ratio method: {method}")


def _winsorize_past(series: pd.Series, window: int, clip_sigma: float) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")
    mean = values.rolling(window, min_periods=max(5, window // 3)).mean().shift(1)
    std = values.rolling(window, min_periods=max(5, window // 3)).std().shift(1)
    lower = mean - clip_sigma * std
    upper = mean + clip_sigma * std
    return values.clip(lower=lower, upper=upper)
