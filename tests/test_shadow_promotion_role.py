from __future__ import annotations

from hydra.foundry.status import EvidenceTier
from hydra.shadow.promotion import decide_forward_promotion


def _decision(**changes: object) -> EvidenceTier:
    values: dict[str, object] = {
        "current_tier": EvidenceTier.SHADOW_ACTIVE,
        "minimum_forward_signals": 20,
        "observed_signals": 20,
        "integrity_incidents": 0,
        "forward_net_after_costs": 100.0,
    }
    values.update(changes)
    return decide_forward_promotion(**values)  # type: ignore[arg-type]


def test_alpha_forward_confirmation_remains_net_economic() -> None:
    assert _decision() is EvidenceTier.SHADOW_CONFIRMED
    assert _decision(forward_net_after_costs=-1.0) is EvidenceTier.SHADOW_REJECTED


def test_xfa_and_defensive_forward_confirmation_use_account_utility() -> None:
    assert _decision(
        objective_pool="XFA_PAYOUT_POOL",
        forward_net_after_costs=0.0,
        forward_account_utility_delta=0.2,
    ) is EvidenceTier.SHADOW_CONFIRMED
    assert _decision(
        objective_pool="DEFENSIVE_ACCOUNT_POOL",
        strategy_role="DEFENSIVE",
        forward_net_after_costs=-10.0,
        forward_account_utility_delta=0.3,
    ) is EvidenceTier.SHADOW_CONFIRMED
    assert _decision(
        objective_pool="DEFENSIVE_ACCOUNT_POOL",
        strategy_role="DEFENSIVE",
        forward_account_utility_delta=0.0,
    ) is EvidenceTier.SHADOW_REJECTED


def test_insufficient_forward_sample_stays_active_and_integrity_rejects() -> None:
    assert _decision(observed_signals=19) is EvidenceTier.SHADOW_ACTIVE
    assert _decision(integrity_incidents=1) is EvidenceTier.SHADOW_REJECTED


def test_unknown_pool_and_nonfinite_evidence_fail_closed() -> None:
    assert _decision(objective_pool="UNREGISTERED") is EvidenceTier.SHADOW_REJECTED
    assert _decision(forward_net_after_costs=float("nan")) is EvidenceTier.SHADOW_REJECTED
