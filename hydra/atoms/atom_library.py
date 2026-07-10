from __future__ import annotations

import numpy as np
import pandas as pd


def add_atom_features(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy().sort_values(["symbol", "timestamp"]).reset_index(drop=True)
    out["timestamp"] = pd.to_datetime(out["timestamp"], utc=True)
    minute = out["timestamp"].dt.hour * 60 + out["timestamp"].dt.minute
    rth_open_minute = 14 * 60 + 30
    out["date"] = out["timestamp"].dt.date.astype(str)
    grouped = out.groupby("symbol", group_keys=False)
    returns = grouped["close"].pct_change()
    rv_short = returns.abs().groupby(out["symbol"]).rolling(30, min_periods=10).mean().reset_index(level=0, drop=True).shift(1)
    rv_long = returns.abs().groupby(out["symbol"]).rolling(180, min_periods=60).mean().reset_index(level=0, drop=True).shift(1)
    high_60 = grouped["high"].rolling(60, min_periods=20).max().reset_index(level=0, drop=True).shift(1)
    low_60 = grouped["low"].rolling(60, min_periods=20).min().reset_index(level=0, drop=True).shift(1)
    center = grouped["close"].rolling(60, min_periods=20).mean().reset_index(level=0, drop=True).shift(1)
    path_30 = out["close"].diff().abs().groupby(out["symbol"]).rolling(30, min_periods=10).sum().reset_index(level=0, drop=True).shift(1)
    disp_30 = grouped["close"].diff(30).shift(1)
    day_group = out.groupby(["symbol", "date"], sort=True)
    day_bar_index = day_group.cumcount()
    day_open = day_group["open"].transform("first")
    prior_close = grouped["close"].shift(1)
    overnight = minute < rth_open_minute
    after_rth_open = minute >= rth_open_minute
    overnight_volume = out["volume"].where(overnight).groupby([out["symbol"], out["date"]]).transform("sum").where(after_rth_open)
    hist_overnight_vol = overnight_volume.groupby(out["symbol"]).rolling(20, min_periods=5).median().reset_index(level=0, drop=True).shift(1)
    overnight_high = out["high"].where(overnight).groupby([out["symbol"], out["date"]]).transform("max").where(after_rth_open)
    overnight_low = out["low"].where(overnight).groupby([out["symbol"], out["date"]]).transform("min").where(after_rth_open)
    opening_close_15 = day_group["close"].transform(lambda item: item.iloc[15] if len(item) > 15 else np.nan)
    opening_close_45 = day_group["close"].transform(lambda item: item.iloc[45] if len(item) > 45 else np.nan)
    open_15 = opening_close_15.where(day_bar_index >= 15)
    open_45 = opening_close_45.where(day_bar_index >= 45)
    width_60 = (high_60 - low_60).replace(0, np.nan)
    location = (out["close"] - low_60) / width_60
    out["overnight_displacement"] = (day_open - prior_close) / prior_close.replace(0, np.nan)
    out["overnight_participation"] = overnight_volume / hist_overnight_vol.replace(0, np.nan)
    out["acceptance_rejection"] = (open_45 - day_open) / (overnight_high - overnight_low).replace(0, np.nan)
    out["prior_value_distance"] = (day_open - center) / width_60
    out["effort_progress_ratio"] = disp_30.abs() / path_30.replace(0, np.nan)
    out["repeated_extension_failure"] = -grouped["close"].diff().groupby(out["symbol"]).rolling(12, min_periods=6).sum().reset_index(level=0, drop=True).shift(1)
    out["directional_pressure_without_progress"] = path_30 - disp_30.abs()
    out["accepted_center_slope"] = center.groupby(out["symbol"]).diff().fillna(0.0)
    out["extreme_dwell"] = ((location > 0.8).astype(float) - (location < 0.2).astype(float)).groupby(out["symbol"]).rolling(60, min_periods=20).mean().reset_index(level=0, drop=True).shift(1)
    out["failed_relocation"] = -(location - 0.5).groupby(out["symbol"]).rolling(30, min_periods=10).mean().reset_index(level=0, drop=True).shift(1)
    out["old_region_reentry"] = -out["failed_relocation"]
    out["rv_short_long_ratio"] = rv_short / rv_long.replace(0, np.nan)
    out["compression_persistence"] = -(rv_short / rv_long.replace(0, np.nan))
    out["failed_expansion"] = -(out["high"] - out["low"]) / width_60
    out["vol_without_displacement"] = rv_short - returns.abs().groupby(out["symbol"]).rolling(30, min_periods=10).mean().reset_index(level=0, drop=True)
    out["open_to_mid_efficiency_shift"] = (open_15 - day_open) / day_open.replace(0, np.nan)
    out["mid_to_late_pressure_decay"] = -grouped["close"].pct_change(90).shift(1)
    out["transition_participation_shift"] = out["volume"].groupby(out["symbol"]).rolling(60, min_periods=20).mean().reset_index(level=0, drop=True).shift(1)
    out["micro_mini_basis_pressure"] = 0.0
    out["same_family_residual_z"] = grouped["close"].pct_change(20).shift(1)
    out["roll_excluded_residual_state"] = out["same_family_residual_z"]
    out["lagged_index_to_metal_response"] = grouped["close"].pct_change(20).shift(1)
    out["lagged_index_to_energy_response"] = grouped["close"].pct_change(20).shift(1)
    out["largecap_smallcap_lagged_residual"] = grouped["close"].pct_change(20).shift(1)
    downside = returns.clip(upper=0).abs().groupby(out["symbol"]).rolling(60, min_periods=20).mean().reset_index(level=0, drop=True).shift(1)
    upside = returns.clip(lower=0).groupby(out["symbol"]).rolling(60, min_periods=20).mean().reset_index(level=0, drop=True).shift(1)
    out["downside_upside_variance_ratio"] = downside / upside.replace(0, np.nan)
    out["tail_cluster_state"] = (returns.abs() > returns.abs().groupby(out["symbol"]).rolling(240, min_periods=60).quantile(0.90).reset_index(level=0, drop=True).shift(1)).astype(float)
    out["recovery_speed_after_loss"] = -returns.clip(upper=0).groupby(out["symbol"]).rolling(20, min_periods=5).sum().reset_index(level=0, drop=True).shift(1)
    out["weekday_state_interaction"] = np.sin(2 * np.pi * out["timestamp"].dt.weekday / 5.0) * np.sign(rv_short.fillna(0))
    out["session_volume_state_interaction"] = np.sin(2 * np.pi * minute / 1440.0) * np.sign(out["volume"] - out["volume"].groupby(out["symbol"]).rolling(60, min_periods=20).median().reset_index(level=0, drop=True).shift(1))
    out["calendar_volatility_interaction"] = out["timestamp"].dt.weekday * rv_short
    out["drawdown_risk_state"] = downside
    out["shared_loss_risk_state"] = downside
    out["tail_avoidance_state"] = out["tail_cluster_state"]
    return out.replace([np.inf, -np.inf], np.nan)


def atom_signal(frame: pd.DataFrame, feature_key: str, expected_direction: int, threshold_name: str) -> pd.Series:
    feature = pd.to_numeric(frame.get(feature_key, pd.Series(index=frame.index, dtype=float)), errors="coerce")
    cleaned = feature.replace([np.inf, -np.inf], np.nan)
    quantile = {"low": 0.55, "moderate": 0.65, "high": 0.75, "extreme": 0.85}.get(threshold_name, 0.65)
    threshold = cleaned.abs().quantile(quantile)
    out = pd.Series(0, index=frame.index, dtype=int)
    if pd.isna(threshold) or threshold == 0:
        return out
    out.loc[cleaned * int(expected_direction) > threshold] = 1
    out.loc[cleaned * int(expected_direction) < -threshold] = -1
    return out


def future_return(frame: pd.DataFrame, horizon_bars: int) -> pd.Series:
    return frame.groupby("symbol")["close"].pct_change(horizon_bars).shift(-horizon_bars)
