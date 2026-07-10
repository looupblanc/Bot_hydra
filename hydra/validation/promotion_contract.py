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
