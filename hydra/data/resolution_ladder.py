"""Fail-closed policy for escalating market-data resolution.

The policy is deliberately pure: it performs no acquisition, cache read,
ledger write, Q4 access, or status promotion.  Callers must supply the official
cost estimate and persistent budget state before a paid request can be
authorized by this scientific gate.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import Any


POLICY_VERSION = "progressive_data_resolution_policy_v1"
DEFAULT_FINAL_LOCKBOX_RESERVE_USD = 30.0


class DataResolutionTier(StrEnum):
    OHLCV_1M = "ohlcv-1m"
    OHLCV_1S = "ohlcv-1s"
    TRADES = "trades"
    TBBO = "tbbo"
    MBP_1 = "mbp-1"
    MBO = "mbo"


SERIOUS_RESEARCH_TIERS = frozenset(
    {
        "PROMISING_RESEARCH_CANDIDATE",
        "ROBUST_RESEARCH_CANDIDATE",
        "SHADOW_RESEARCH_CANDIDATE",
        "SHADOW_RESEARCH_ACTIVE",
        "PAPER_SHADOW_READY",
        "SHADOW_ACTIVE",
        "SHADOW_CONFIRMED",
        "TRADING_READY_CANDIDATE",
    }
)
FINALIST_TIERS = frozenset(
    {
        "ROBUST_RESEARCH_CANDIDATE",
        "SHADOW_RESEARCH_CANDIDATE",
        "SHADOW_RESEARCH_ACTIVE",
        "PAPER_SHADOW_READY",
        "SHADOW_ACTIVE",
        "SHADOW_CONFIRMED",
        "TRADING_READY_CANDIDATE",
    }
)


@dataclass(frozen=True)
class ResolutionEvidence:
    """Evidence supplied before escalating beyond canonical 1-minute bars."""

    candidate_id: str
    candidate_tier: str
    serious_bar_level_evidence: bool = False
    event_windows_bounded: bool = False
    event_window_count: int = 0
    intrabar_path_decision_relevant: bool = False
    trade_intensity_hypothesis_preregistered: bool = False
    serious_finalist: bool = False
    execution_ambiguity_decision_relevant: bool = False
    book_dependency_preregistered: bool = False
    simpler_resolution_proven_insufficient: bool = False
    expected_decision_information_gain: float = 0.0


@dataclass(frozen=True)
class ResolutionRequest:
    schema: str
    evidence: ResolutionEvidence
    official_estimated_cost_usd: float | None
    committed_spend_usd: float
    hard_cap_usd: float = 100.0
    minimum_final_lockbox_reserve_usd: float = DEFAULT_FINAL_LOCKBOX_RESERVE_USD
    cache_hit: bool = False


@dataclass(frozen=True)
class ResolutionDecision:
    allowed: bool
    schema: str
    reason: str
    failed_requirements: tuple[str, ...]
    projected_remaining_budget_usd: float
    paid_request: bool
    policy_version: str = POLICY_VERSION
    q4_access_allowed: bool = False
    order_capability: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def decide_resolution_escalation(request: ResolutionRequest) -> ResolutionDecision:
    """Evaluate a data-resolution request without performing any I/O.

    Scientific requirements apply equally to cached and paid high-resolution
    data, because inexpensive access does not remove selection or overfitting
    risk.  Budget requirements apply only when the request is not a cache hit.
    """

    schema = _normalize_schema(request.schema)
    evidence = request.evidence
    failed: list[str] = []

    if schema == DataResolutionTier.MBO.value:
        return _decision(request, schema, ("mbo_prohibited",))
    if schema not in {tier.value for tier in DataResolutionTier}:
        return _decision(request, schema, ("unsupported_resolution_schema",))
    if not evidence.candidate_id.strip():
        failed.append("candidate_id_required")
    if evidence.expected_decision_information_gain <= 0.0:
        failed.append("positive_expected_decision_information_gain_required")

    tier = evidence.candidate_tier.strip().upper()
    bounded = evidence.event_windows_bounded and evidence.event_window_count > 0
    serious = evidence.serious_bar_level_evidence and tier in SERIOUS_RESEARCH_TIERS
    finalist = evidence.serious_finalist and tier in FINALIST_TIERS

    if schema == DataResolutionTier.OHLCV_1S.value:
        if not serious:
            failed.append("serious_bar_level_evidence_required")
        if not bounded:
            failed.append("bounded_event_windows_required")
        if not evidence.intrabar_path_decision_relevant:
            failed.append("intrabar_path_decision_relevance_required")
    elif schema == DataResolutionTier.TRADES.value:
        if not serious:
            failed.append("serious_bar_level_evidence_required")
        if not bounded:
            failed.append("bounded_event_windows_required")
        if not evidence.trade_intensity_hypothesis_preregistered:
            failed.append("trade_intensity_hypothesis_preregistration_required")
    elif schema == DataResolutionTier.TBBO.value:
        if not finalist:
            failed.append("serious_finalist_required")
        if not bounded:
            failed.append("bounded_event_windows_required")
        if not evidence.execution_ambiguity_decision_relevant:
            failed.append("execution_ambiguity_decision_relevance_required")
    elif schema == DataResolutionTier.MBP_1.value:
        if not finalist:
            failed.append("serious_finalist_required")
        if not bounded:
            failed.append("bounded_event_windows_required")
        if not evidence.book_dependency_preregistered:
            failed.append("book_dependency_preregistration_required")
        if not evidence.simpler_resolution_proven_insufficient:
            failed.append("simpler_resolution_insufficiency_required")

    if not request.cache_hit:
        estimate = request.official_estimated_cost_usd
        if estimate is None:
            failed.append("official_cost_estimate_required")
        elif estimate < 0.0:
            failed.append("nonnegative_official_cost_estimate_required")
        elif _projected_remaining(request) < request.minimum_final_lockbox_reserve_usd:
            failed.append("final_lockbox_budget_reserve_breached")

    return _decision(request, schema, tuple(dict.fromkeys(failed)))


def _normalize_schema(schema: str) -> str:
    return str(schema or "").strip().lower().replace("_", "-")


def _projected_remaining(request: ResolutionRequest) -> float:
    estimate = 0.0 if request.cache_hit else float(request.official_estimated_cost_usd or 0.0)
    return float(request.hard_cap_usd - request.committed_spend_usd - estimate)


def _decision(
    request: ResolutionRequest,
    schema: str,
    failed: tuple[str, ...],
) -> ResolutionDecision:
    allowed = not failed
    reason = "resolution_escalation_authorized" if allowed else failed[0]
    return ResolutionDecision(
        allowed=allowed,
        schema=schema,
        reason=reason,
        failed_requirements=failed,
        projected_remaining_budget_usd=_projected_remaining(request),
        paid_request=not request.cache_hit,
    )
