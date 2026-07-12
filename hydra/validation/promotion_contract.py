from __future__ import annotations

from dataclasses import dataclass

from hydra.validation.evidence_scope import ComputationMode, EvidenceScope
from hydra.validation.status_provenance import StatusProvenance


VALID_ATOM_STATUSES = {
    "ATOM_VALIDATED",
    "ATOM_ADVERSARIAL_PASS",
    "ATOM_REPLICATED_TEMPORALLY",
    "ATOM_REPLICATED_CROSS_MARKET",
    "ATOM_REPLICATED_CONTRACTUALLY",
}

VALID_STRATEGY_STATUSES = {
    "STRATEGY_VALIDATION_READY",
    "STRATEGY_NULL_PASS",
    "STRATEGY_TEMPORAL_PASS",
    "STRATEGY_COST_PASS",
    "STRATEGY_TOPSTEP_PATH_CANDIDATE",
    "STRATEGY_TOPSTEP_COMPATIBLE",
    "STRATEGY_PORTFOLIO_ONLY",
}


@dataclass(frozen=True)
class PromotionContract:
    source_scope: EvidenceScope
    target_scope: EvidenceScope
    required_statuses: tuple[str, ...]
    policy_version: str


def evidence_can_support_scope(evidence: StatusProvenance, target_scope: EvidenceScope) -> bool:
    if not evidence.passed:
        return False
    if evidence.computation_mode != ComputationMode.FULL:
        return False
    if evidence.scope != target_scope:
        return False
    return True


def require_strategy_ready(evidence: list[StatusProvenance]) -> None:
    statuses = {item.status for item in evidence if evidence_can_support_scope(item, EvidenceScope.STRATEGY_CANDIDATE)}
    missing = {"STRATEGY_NULL_PASS", "STRATEGY_TEMPORAL_PASS", "STRATEGY_COST_PASS"} - statuses
    if missing:
        raise ValueError(f"Strategy candidate missing required full-scope validation: {sorted(missing)}")


def require_atom_validated(evidence: list[StatusProvenance]) -> None:
    statuses = {item.status for item in evidence if evidence_can_support_scope(item, EvidenceScope.EDGE_ATOM)}
    if "ATOM_VALIDATED" not in statuses:
        raise ValueError("Only fully validated Edge Atoms may be assembled into strategies.")


def paper_shadow_ready_after_q4(
    *,
    q4_classification: str,
    shadow_package_complete: bool,
    hard_integrity_issue: bool,
    deterministic_forward_features: bool,
    fail_closed_virtual_execution: bool,
    broker_or_order_capability: bool,
) -> bool:
    """V4 zero-risk paper admission; 2025 is intentionally a later contract."""

    return bool(
        q4_classification == "Q4_LOCKBOX_PASS"
        and shadow_package_complete
        and not hard_integrity_issue
        and deterministic_forward_features
        and fail_closed_virtual_execution
        and not broker_or_order_capability
    )
