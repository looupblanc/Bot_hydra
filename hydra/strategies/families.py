from __future__ import annotations

import numpy as np
import pandas as pd

from hydra.strategies.dsl import StrategyCandidate


FAMILIES = [
    "multi_session_momentum_exhaustion",
    "volatility_regime_expansion",
    "regime_compression_breakout",
    "session_exhaustion_reversal",
    "volatility_shift_continuation",
]


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
    else:
        raise ValueError(f"Forbidden or unknown family: {fam}")
    return sig.shift(1).fillna(0.0)
