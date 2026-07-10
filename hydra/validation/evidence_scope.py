from __future__ import annotations

from enum import Enum


class EvidenceScope(str, Enum):
    COMPONENT = "COMPONENT"
    EDGE_ATOM = "EDGE_ATOM"
    REPRESENTATION = "REPRESENTATION"
    STRATEGY_CANDIDATE = "STRATEGY_CANDIDATE"
    PORTFOLIO = "PORTFOLIO"
    ACCOUNT_PATH = "ACCOUNT_PATH"


class ComputationMode(str, Enum):
    FULL = "FULL"
    PROXY = "PROXY"
    INHERITED_INVALID = "INHERITED_INVALID"
    STALE_INVALID = "STALE_INVALID"
    UNKNOWN = "UNKNOWN"


SCOPE_ORDER = {
    EvidenceScope.COMPONENT: 0,
    EvidenceScope.EDGE_ATOM: 1,
    EvidenceScope.REPRESENTATION: 2,
    EvidenceScope.STRATEGY_CANDIDATE: 3,
    EvidenceScope.PORTFOLIO: 4,
    EvidenceScope.ACCOUNT_PATH: 5,
}


def can_promote_scope(source: EvidenceScope, target: EvidenceScope, *, newly_executed_validation: bool) -> bool:
    if source == target:
        return newly_executed_validation
    if SCOPE_ORDER[target] <= SCOPE_ORDER[source]:
        return newly_executed_validation
    return False


def require_full_scope(scope: EvidenceScope, mode: ComputationMode) -> None:
    if mode != ComputationMode.FULL:
        raise ValueError(f"{scope.value} evidence requires FULL computation mode, got {mode.value}.")
