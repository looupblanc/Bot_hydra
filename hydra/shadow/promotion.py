from __future__ import annotations

import math

from hydra.foundry.status import (
    COMBINE_PASSER_POOL,
    DEFENSIVE_ACCOUNT_POOL,
    OBJECTIVE_POOLS,
    XFA_PAYOUT_POOL,
    EvidenceTier,
)


ACCOUNT_UTILITY_ROLES = frozenset({"DEFENSIVE", "PORTFOLIO_ONLY", "HAZARD"})


def decide_forward_promotion(
    *,
    current_tier: EvidenceTier,
    minimum_forward_signals: int,
    observed_signals: int,
    integrity_incidents: int,
    forward_net_after_costs: float,
    objective_pool: str = COMBINE_PASSER_POOL,
    strategy_role: str = "ALPHA",
    forward_account_utility_delta: float = 0.0,
) -> EvidenceTier:
    if integrity_incidents:
        return EvidenceTier.SHADOW_REJECTED
    if current_tier not in {
        EvidenceTier.SHADOW_RESEARCH_CANDIDATE,
        EvidenceTier.PAPER_SHADOW_READY,
        EvidenceTier.SHADOW_ACTIVE,
    }:
        return current_tier
    if observed_signals < minimum_forward_signals:
        return EvidenceTier.SHADOW_ACTIVE
    pool = str(objective_pool or "").strip().upper()
    role = str(strategy_role or "").strip().upper()
    if pool not in OBJECTIVE_POOLS or not all(
        math.isfinite(float(value))
        for value in (forward_net_after_costs, forward_account_utility_delta)
    ):
        return EvidenceTier.SHADOW_REJECTED
    account_utility_objective = bool(
        pool in {XFA_PAYOUT_POOL, DEFENSIVE_ACCOUNT_POOL}
        or role in ACCOUNT_UTILITY_ROLES
    )
    objective_supported = (
        forward_account_utility_delta > 0.0
        if account_utility_objective
        else forward_net_after_costs > 0.0
    )
    return (
        EvidenceTier.SHADOW_CONFIRMED
        if objective_supported
        else EvidenceTier.SHADOW_REJECTED
    )
