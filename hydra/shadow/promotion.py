from __future__ import annotations

from hydra.foundry.status import EvidenceTier


def decide_forward_promotion(
    *,
    current_tier: EvidenceTier,
    minimum_forward_signals: int,
    observed_signals: int,
    integrity_incidents: int,
    forward_net_after_costs: float,
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
    return (
        EvidenceTier.SHADOW_CONFIRMED
        if forward_net_after_costs > 0
        else EvidenceTier.SHADOW_REJECTED
    )
