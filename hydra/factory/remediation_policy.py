from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class RemediationPolicy:
    name: str
    target_failure: str
    primary_dimension: str
    predicted_effect: str
    complexity_delta: int


POLICIES = [
    RemediationPolicy("target_velocity_runner", "target", "exit_policy", "increase_profit_velocity_without_size_increase", 1),
    RemediationPolicy("mll_buffer_derisk", "mll", "sizing", "increase_mll_buffer_and_reduce_tail_loss", 0),
    RemediationPolicy("consistency_daily_lock", "consistency", "risk_controls", "reduce_best_day_concentration", 1),
    RemediationPolicy("oos_simplify", "oos", "parameters", "reduce_degrees_of_freedom", -1),
    RemediationPolicy("sequence_fragility_smooth", "fragile", "sizing", "reduce_trade_order_dependency", 1),
    RemediationPolicy("payout_frequency", "payout", "session_frequency", "increase_150_winning_day_count", 1),
    RemediationPolicy("portfolio_role_shift", "correlation", "portfolio_role", "improve_uniqueness_or_hedge_role", 0),
]


def choose_policy_for_reason(reason: str | None) -> RemediationPolicy:
    text = (reason or "").lower()
    for policy in POLICIES:
        if policy.target_failure in text:
            return policy
    if "split" in text or "march" in text:
        return next(p for p in POLICIES if p.name == "oos_simplify")
    return POLICIES[0]


def mutation_patch_for_policy(policy: RemediationPolicy, risk: dict[str, Any], params: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    new_risk = dict(risk)
    new_params = dict(params)
    if policy.name == "target_velocity_runner":
        new_risk["exit_policy"] = "partial_profit_take_then_runner"
        new_risk["holding_period"] = min(int(new_risk.get("holding_period", 8)) + 3, 24)
    elif policy.name == "mll_buffer_derisk":
        new_risk["risk_scale"] = max(float(new_risk.get("risk_scale", 1.0)) * 0.80, 0.05)
        new_risk["exit_policy"] = "mll_buffer_protection_exit"
        new_risk["internal_daily_stop"] = min(float(new_risk.get("internal_daily_stop", 1000)), 750.0)
    elif policy.name == "consistency_daily_lock":
        new_risk["daily_profit_lock"] = min(float(new_risk.get("daily_profit_lock", 1500)), 1000.0)
        new_risk["exit_policy"] = "daily_profit_lock_exit"
    elif policy.name == "oos_simplify":
        for key in list(new_params):
            if key.startswith("max_"):
                new_params[key] = float(new_params[key])
        new_risk["holding_period"] = max(4, min(int(new_risk.get("holding_period", 8)), 12))
    elif policy.name == "sequence_fragility_smooth":
        new_risk["risk_scale"] = max(float(new_risk.get("risk_scale", 1.0)) * 0.90, 0.05)
        new_risk["exit_policy"] = "trailing_after_R"
    elif policy.name == "payout_frequency":
        new_risk["holding_period"] = max(3, int(new_risk.get("holding_period", 8)) - 2)
        new_risk["daily_profit_lock"] = min(float(new_risk.get("daily_profit_lock", 1500)), 1500.0)
    elif policy.name == "portfolio_role_shift":
        new_risk["holding_period"] = max(3, int(new_risk.get("holding_period", 8)) + 1)
    return new_risk, new_params

