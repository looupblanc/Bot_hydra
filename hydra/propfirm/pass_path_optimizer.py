from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class PassPathAnalysis:
    diagnosis: str
    recommendation: str
    branch_action: str
    target_velocity_score: float
    mll_safety_score: float
    consistency_score: float
    payout_velocity_score: float


def analyze_pass_path(evaluation: dict[str, Any], profit_target: float = 9000.0, mll: float = 4500.0) -> PassPathAnalysis:
    profit = float(evaluation.get("adjusted_net_profit", 0.0))
    target_velocity = max(0.0, min(1.0, profit / profit_target))
    min_buffer = float(evaluation.get("combine_min_mll_buffer", 0.0))
    mll_safety = max(0.0, min(1.0, min_buffer / mll))
    best_day_pct = float(evaluation.get("combine_best_day_pct_of_total_profit", 0.0))
    consistency = max(0.0, min(1.0, 1.0 - max(best_day_pct - 0.35, 0.0) / 0.30))
    payout_velocity = max(0.0, min(1.0, float(evaluation.get("winning_days_150_count", 0)) / 8.0))
    if evaluation.get("combine_mll_breached"):
        return PassPathAnalysis("too_risky_mll_breach", "reduce sizing, improve exits, add MLL buffer protection and volatility filters", "mutate", target_velocity, mll_safety, consistency, payout_velocity)
    if target_velocity < 0.60 and mll_safety > 0.65:
        return PassPathAnalysis("under_sized_or_too_slow", "increase profit velocity with runner exits, better session selection, or progressive micro sizing", "mutate", target_velocity, mll_safety, consistency, payout_velocity)
    if consistency < 0.70:
        return PassPathAnalysis("too_spiky", "add daily profit lock, reduce spike-day dependence, smooth exits", "mutate", target_velocity, mll_safety, consistency, payout_velocity)
    if not evaluation.get("payout_eligible"):
        return PassPathAnalysis("weak_payout_profile", "improve $150 winning day frequency and post-payout survival margin", "mutate", target_velocity, mll_safety, consistency, payout_velocity)
    if evaluation.get("topstep_passed") and evaluation.get("funded_sim_survived"):
        return PassPathAnalysis("promotion_candidate", "run deeper correlation, sensitivity, and execution-readiness checks", "expand", target_velocity, mll_safety, consistency, payout_velocity)
    return PassPathAnalysis("topstep_near_miss", "mutate only the weakest pass-path dimension", "mutate", target_velocity, mll_safety, consistency, payout_velocity)
