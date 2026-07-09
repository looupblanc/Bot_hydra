from __future__ import annotations

import numpy as np
import pandas as pd

from hydra.strategies.dsl import StrategyCandidate


BASE_FAMILIES = [
    "multi_session_momentum_exhaustion",
    "volatility_regime_expansion",
    "regime_compression_breakout",
    "session_exhaustion_reversal",
    "volatility_shift_continuation",
]

TOPSTEP_FAMILIES = [
    "topstep_opening_range_controlled_runner",
    "topstep_prior_level_reclaim_smooth_pnl",
    "topstep_vwap_exhaustion_payout_engine",
    "topstep_volatility_expansion_limited_risk",
    "topstep_nq_es_divergence_controlled",
    "topstep_micro_scaling_mes_mnq",
]

FAMILIES = BASE_FAMILIES


def signal_for_candidate(candidate: StrategyCandidate, df: pd.DataFrame) -> pd.Series:
    p = candidate.parameters
    fam = candidate.family
    sig = pd.Series(0, index=df.index, dtype=float)
    if fam == "multi_session_momentum_exhaustion":
        z = p["exhaustion_z"]
        sig[df["momentum_exhaustion"] > z] = -1
        sig[df["momentum_exhaustion"] < -z] = 1
        sig[(df["path_persistence_10"] < p["max_persistence"]) | (df["vol_regime_high"] == 0)] = 0
    elif fam == "volatility_regime_expansion":
        direction = np.sign(df["momentum_20"]).replace(0, 1)
        sig[(df["vol_regime_high"] == 1) & (df["range_expansion"] > p["range_mult"])] = direction
    elif fam == "regime_compression_breakout":
        direction = np.sign(df["momentum_20"]).replace(0, 1)
        sig[(df["compression_score"] < p["compression_max"]) & (df["range_expansion"] > p["release_mult"])] = direction
    elif fam == "session_exhaustion_reversal":
        sig[(df["session_return"] > p["session_move"]) & (df["range_expansion"] > p["range_mult"])] = -1
        sig[(df["session_return"] < -p["session_move"]) & (df["range_expansion"] > p["range_mult"])] = 1
    elif fam == "volatility_shift_continuation":
        direction = np.sign(df["momentum_20"]).replace(0, 1)
        sig[(df["regime_transition"] == 1) & (df["expansion_regime"] == 1) & (df["path_persistence_10"] > p["persistence_min"])] = direction
    elif fam == "topstep_opening_range_controlled_runner":
        session_minute = _session_minute(df)
        direction = np.sign(df["momentum_20"]).replace(0, 1)
        window = (session_minute >= p["start_minute"]) & (session_minute <= p["end_minute"])
        setup = (df["range_expansion"] > p["range_mult"]) & (df["path_persistence_10"] > p["persistence_min"])
        sig[window & setup & (df["vol_regime_high"] == 1)] = direction
    elif fam == "topstep_prior_level_reclaim_smooth_pnl":
        prior_high = df.groupby("session_id")["high"].cummax().shift(1)
        prior_low = df.groupby("session_id")["low"].cummin().shift(1)
        reclaim_long = (df["close"] > prior_low) & (df["close"].shift(1) < prior_low.shift(1))
        reclaim_short = (df["close"] < prior_high) & (df["close"].shift(1) > prior_high.shift(1))
        calm = df["range_expansion"] < p["max_range_expansion"]
        sig[reclaim_long & calm & (df["momentum_20"] > -p["momentum_floor"])] = 1
        sig[reclaim_short & calm & (df["momentum_20"] < p["momentum_floor"])] = -1
    elif fam == "topstep_vwap_exhaustion_payout_engine":
        proxy_vwap = (df["close"] * df["volume"]).groupby(df["session_id"]).cumsum() / df["volume"].groupby(df["session_id"]).cumsum().replace(0, np.nan)
        distance = (df["close"] - proxy_vwap) / df["close"]
        sig[(distance > p["vwap_distance"]) & (df["momentum_exhaustion"] > p["exhaustion_z"])] = -1
        sig[(distance < -p["vwap_distance"]) & (df["momentum_exhaustion"] < -p["exhaustion_z"])] = 1
        sig[df["range_expansion"] > p["max_range_expansion"]] = 0
    elif fam == "topstep_volatility_expansion_limited_risk":
        session_minute = _session_minute(df)
        direction = np.sign(df["momentum_20"]).replace(0, 1)
        controlled_window = session_minute <= p["latest_minute"]
        setup = (df["vol_regime_high"] == 1) & (df["range_expansion"] > p["range_mult"]) & (df["range_expansion"] < p["max_range_expansion"])
        sig[controlled_window & setup] = direction
    elif fam == "topstep_nq_es_divergence_controlled":
        direction = np.sign(df["momentum_20"]).replace(0, 1)
        divergence_proxy = df["momentum_20"] - df["session_return"]
        sig[(divergence_proxy.abs() > p["divergence_min"]) & (df["range_expansion"] < p["max_range_expansion"])] = direction
    elif fam == "topstep_micro_scaling_mes_mnq":
        direction = np.sign(df["momentum_20"]).replace(0, 1)
        setup = (df["compression_score"] < p["compression_max"]) & (df["path_persistence_10"] > p["persistence_min"])
        sig[setup & (df["range_expansion"] < p["max_range_expansion"])] = direction
    else:
        raise ValueError(f"Forbidden or unknown family: {fam}")
    return sig.shift(1).fillna(0.0)


def _session_minute(df: pd.DataFrame) -> pd.Series:
    timestamps = pd.to_datetime(df["timestamp"], utc=True)
    return timestamps.dt.hour * 60 + timestamps.dt.minute
