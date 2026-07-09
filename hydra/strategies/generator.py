from __future__ import annotations

import uuid

import numpy as np

from hydra.strategies.dsl import StrategyCandidate
from hydra.strategies.families import FAMILIES, TOPSTEP_FAMILIES


def generate_candidates(
    count: int,
    symbols: list[str],
    timeframes: list[str],
    seed: int,
    diagnostic_relaxed: bool = False,
    topstep_mode: bool = False,
) -> list[StrategyCandidate]:
    rng = np.random.default_rng(seed)
    candidates: list[StrategyCandidate] = []
    families = TOPSTEP_FAMILIES if topstep_mode else FAMILIES
    for _ in range(count):
        family = str(rng.choice(families))
        symbol_pool = _topstep_symbol_pool(symbols, family) if topstep_mode else symbols
        symbol = str(rng.choice(symbol_pool))
        timeframe = str(rng.choice(timeframes))
        params = _params_for_family(family, rng, diagnostic_relaxed)
        risk = _topstep_risk(symbol, rng) if topstep_mode else {
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
    if family == "topstep_opening_range_controlled_runner":
        return {
            "start_minute": float(rng.integers(13 * 60 + 30, 14 * 60 + 15)),
            "end_minute": float(rng.integers(15 * 60, 17 * 60)),
            "range_mult": float(rng.uniform(1.05, 1.75)),
            "persistence_min": float(rng.uniform(0.35, 0.75)),
        }
    if family == "topstep_prior_level_reclaim_smooth_pnl":
        return {
            "max_range_expansion": float(rng.uniform(1.05, 1.80)),
            "momentum_floor": float(rng.uniform(0.001, 0.008)),
        }
    if family == "topstep_vwap_exhaustion_payout_engine":
        return {
            "vwap_distance": float(rng.uniform(0.0005, 0.0035)),
            "exhaustion_z": float(rng.uniform(1.6, 3.4)),
            "max_range_expansion": float(rng.uniform(1.2, 2.2)),
        }
    if family == "topstep_volatility_expansion_limited_risk":
        return {
            "latest_minute": float(rng.integers(16 * 60, 19 * 60)),
            "range_mult": float(rng.uniform(1.1, 1.9)),
            "max_range_expansion": float(rng.uniform(1.8, 3.2)),
        }
    if family == "topstep_nq_es_divergence_controlled":
        return {
            "divergence_min": float(rng.uniform(0.001, 0.010)),
            "max_range_expansion": float(rng.uniform(1.1, 2.4)),
        }
    if family == "topstep_micro_scaling_mes_mnq":
        return {
            "compression_max": float(rng.uniform(0.55, 1.05)),
            "persistence_min": float(rng.uniform(0.30, 0.80)),
            "max_range_expansion": float(rng.uniform(1.0, 2.0)),
        }
    raise ValueError(f"Unknown family: {family}")


def _topstep_symbol_pool(symbols: list[str], family: str) -> list[str]:
    micros = [s for s in symbols if s in {"MES", "MNQ"}]
    if family == "topstep_micro_scaling_mes_mnq" and micros:
        return micros
    if micros and family in {"topstep_vwap_exhaustion_payout_engine", "topstep_prior_level_reclaim_smooth_pnl"}:
        return micros + symbols
    return symbols


def _topstep_risk(symbol: str, rng: np.random.Generator) -> dict[str, float | int]:
    is_micro = symbol.startswith("M")
    return {
        "holding_period": int(rng.integers(3, 14)),
        "risk_scale": float(rng.uniform(0.35, 1.10) if is_micro else rng.uniform(0.15, 0.55)),
        "max_position": int(rng.integers(1, 4) if is_micro else 1),
        "risk_per_trade": float(rng.choice([150, 250, 350, 500, 750])),
        "stop_distance_ticks": int(rng.choice([8, 12, 16, 20, 28, 36])),
        "internal_daily_stop": float(rng.choice([500, 750, 1000, 1500, 2000])),
        "daily_profit_lock": float(rng.choice([750, 1000, 1500, 2000, 3000])),
    }
