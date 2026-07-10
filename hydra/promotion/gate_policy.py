from __future__ import annotations

from dataclasses import dataclass


HARD_INVALIDATION_REASONS = {
    "lookahead",
    "target_leakage",
    "corrupted_data",
    "duplicate",
    "duplicate_or_near_duplicate_equity_curve",
    "invalid_contract_values",
    "prohibited_session_behavior",
    "maximum_position_violation",
    "impossible_fills",
    "combine_mll_breached",
    "intraday_unrealized_mll_touch_or_breach",
}

REPAIRABLE_REASONS = {
    "combine_target_not_reached",
    "topstep_near_miss_target_velocity",
    "combine_profit_target_not_reached",
    "weak_but_mutatable_economic_profile",
    "viable_only_in_one_split",
    "march_oos_weak",
    "reshuffle_robustness_soft_fail",
    "payout_profile_weak",
    "funded_mll_or_tail_failure",
    "high_correlation_needs_portfolio_role",
    "same_bar_path_requires_higher_resolution_validation",
}


@dataclass(frozen=True)
class GatePolicyDecision:
    classification: str
    action: str
    reason: str


def classify_failure(reason: str | None, severity: str | None = None) -> GatePolicyDecision:
    key = (reason or "").strip()
    if severity == "HARD_FAIL" and key not in REPAIRABLE_REASONS:
        return GatePolicyDecision("HARD_INVALID", "retire_or_rebuild", key or "hard_fail")
    if key in HARD_INVALIDATION_REASONS:
        return GatePolicyDecision("HARD_INVALID", "retire_or_rebuild", key)
    if key in REPAIRABLE_REASONS:
        return GatePolicyDecision("REPAIRABLE_NEAR_MISS", "targeted_mutation_or_retest", key)
    if not key:
        return GatePolicyDecision("WARNING", "review", "no_failure_reason")
    return GatePolicyDecision("WARNING", "review", key)

