from __future__ import annotations

import uuid

import numpy as np

from hydra.strategies.dsl import StrategyCandidate
from hydra.strategies.families import FAMILIES


def generate_candidates(
    count: int,
    symbols: list[str],
    timeframes: list[str],
    seed: int,
    diagnostic_relaxed: bool = False,
) -> list[StrategyCandidate]:
    rng = np.random.default_rng(seed)
    candidates: list[StrategyCandidate] = []
    for _ in range(count):
        family = str(rng.choice(FAMILIES))
        symbol = str(rng.choice(symbols))
        timeframe = str(rng.choice(timeframes))
        params = _params_for_family(family, rng, diagnostic_relaxed)
        risk = {
            "holding_period": int(rng.integers(2, 9) if diagnostic_relaxed else rng.integers(3, 18)),
            "risk_scale": float(rng.uniform(0.25, 0.80) if diagnostic_relaxed else rng.uniform(0.45, 1.25)),
            "max_position": 1,
        }
        candidates.append(
            StrategyCandidate(
                candidate_id=f"cand_{uuid.uuid4().hex[:12]}",
                family=family,
                symbol=symbol,
                timeframe=timeframe,
                parameters=params,
                entry_logic=f"{family}_regime_path_entry",
                exit_logic="time_and_opposite_signal_exit",
                risk_parameters=risk,
            )
        )
    return candidates


def _params_for_family(family: str, rng: np.random.Generator, diagnostic_relaxed: bool = False) -> dict[str, float]:
    if family == "multi_session_momentum_exhaustion":
        if diagnostic_relaxed:
            return {"exhaustion_z": float(rng.uniform(0.8, 2.1)), "max_persistence": float(rng.uniform(0.50, 0.95))}
        return {"exhaustion_z": float(rng.uniform(1.6, 3.5)), "max_persistence": float(rng.uniform(0.25, 0.8))}
    if family == "volatility_regime_expansion":
        if diagnostic_relaxed:
            return {"range_mult": float(rng.uniform(0.75, 1.35))}
        return {"range_mult": float(rng.uniform(1.1, 2.2))}
    if family == "regime_compression_breakout":
        if diagnostic_relaxed:
            return {"compression_max": float(rng.uniform(0.75, 1.20)), "release_mult": float(rng.uniform(0.75, 1.35))}
        return {"compression_max": float(rng.uniform(0.45, 0.95)), "release_mult": float(rng.uniform(1.0, 1.9))}
    if family == "session_exhaustion_reversal":
        if diagnostic_relaxed:
            return {"session_move": float(rng.uniform(0.0005, 0.006)), "range_mult": float(rng.uniform(0.60, 1.30))}
        return {"session_move": float(rng.uniform(0.002, 0.018)), "range_mult": float(rng.uniform(0.8, 1.8))}
    if family == "volatility_shift_continuation":
        if diagnostic_relaxed:
            return {"persistence_min": float(rng.uniform(0.05, 0.45))}
        return {"persistence_min": float(rng.uniform(0.25, 0.75))}
    raise ValueError(f"Unknown family: {family}")
