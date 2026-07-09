from __future__ import annotations

import uuid

import numpy as np

from hydra.strategies.dsl import StrategyCandidate


LANES = [
    "topstep_nq_es_divergence_controlled_v2",
    "topstep_opening_range_controlled_runner_v2",
    "topstep_prior_level_reclaim_smooth_pnl_v2",
    "topstep_vwap_exhaustion_payout_engine_v2",
    "topstep_volatility_expansion_limited_risk_v2",
    "topstep_micro_scaling_mes_mnq_v2",
    "payout_cycle_smooth_climber_v1",
    "consistency_safe_runner_v1",
    "near_miss_adaptive_mutator",
    "portfolio_diversification_lane",
    "creative_market_representation_lane",
]

LANE_TO_FAMILY = {
    "topstep_nq_es_divergence_controlled_v2": "topstep_nq_es_divergence_controlled",
    "topstep_opening_range_controlled_runner_v2": "topstep_opening_range_controlled_runner",
    "topstep_prior_level_reclaim_smooth_pnl_v2": "topstep_prior_level_reclaim_smooth_pnl",
    "topstep_vwap_exhaustion_payout_engine_v2": "topstep_vwap_exhaustion_payout_engine",
    "topstep_volatility_expansion_limited_risk_v2": "topstep_volatility_expansion_limited_risk",
    "topstep_micro_scaling_mes_mnq_v2": "topstep_micro_scaling_mes_mnq",
    "payout_cycle_smooth_climber_v1": "topstep_vwap_exhaustion_payout_engine",
    "consistency_safe_runner_v1": "topstep_prior_level_reclaim_smooth_pnl",
    "near_miss_adaptive_mutator": "topstep_nq_es_divergence_controlled",
    "portfolio_diversification_lane": "topstep_opening_range_controlled_runner",
    "creative_market_representation_lane": "topstep_volatility_expansion_limited_risk",
}


def mutate_for_failure(candidate: StrategyCandidate, failure_reason: str | None, seed: int) -> StrategyCandidate:
    rng = np.random.default_rng(seed)
    params = dict(candidate.parameters)
    risk = dict(candidate.risk_parameters)
    reason = failure_reason or ""
    if "target" in reason:
        risk["holding_period"] = int(min(24, int(risk.get("holding_period", 8)) + rng.integers(1, 5)))
        risk["daily_profit_lock"] = float(min(3000, float(risk.get("daily_profit_lock", 1500)) * rng.uniform(1.05, 1.25)))
        risk["risk_scale"] = float(float(risk.get("risk_scale", 1.0)) * rng.uniform(1.03, 1.18))
    elif "mll" in reason:
        risk["risk_scale"] = float(float(risk.get("risk_scale", 1.0)) * rng.uniform(0.55, 0.85))
        risk["internal_daily_stop"] = float(max(500, float(risk.get("internal_daily_stop", 1000)) * rng.uniform(0.65, 0.90)))
        risk["exit_policy"] = "mll_buffer_protection_exit"
    elif "consistency" in reason or "spike" in reason:
        risk["daily_profit_lock"] = float(max(750, float(risk.get("daily_profit_lock", 1500)) * rng.uniform(0.60, 0.85)))
        risk["exit_policy"] = "partial_profit_take_then_runner"
    elif "trade" in reason:
        for key, value in list(params.items()):
            if isinstance(value, (int, float)):
                params[key] = float(value) * rng.uniform(0.85, 1.15)
    elif "payout" in reason:
        risk["daily_profit_lock"] = float(rng.choice([750, 1000, 1500]))
        risk["exit_policy"] = "daily_profit_lock_exit"
    return StrategyCandidate(
        candidate_id=f"cand_{uuid.uuid4().hex[:12]}",
        family=candidate.family,
        symbol=candidate.symbol,
        timeframe=candidate.timeframe,
        parameters=params,
        entry_logic=candidate.entry_logic,
        exit_logic=candidate.exit_logic,
        risk_parameters=risk,
        parent_candidate_id=candidate.candidate_id,
        mutation_type=f"adaptive:{reason[:40]}",
    )
