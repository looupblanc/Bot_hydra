from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import Iterable


class EvidenceTier(StrEnum):
    RESEARCH_PROTOTYPE = "RESEARCH_PROTOTYPE"
    RAW_ECONOMIC_SIGNAL = "RAW_ECONOMIC_SIGNAL"
    PROMISING_RESEARCH_CANDIDATE = "PROMISING_RESEARCH_CANDIDATE"
    ROBUST_RESEARCH_CANDIDATE = "ROBUST_RESEARCH_CANDIDATE"
    SHADOW_RESEARCH_CANDIDATE = "SHADOW_RESEARCH_CANDIDATE"
    PAPER_SHADOW_READY = "PAPER_SHADOW_READY"
    SHADOW_ACTIVE = "SHADOW_ACTIVE"
    SHADOW_CONFIRMED = "SHADOW_CONFIRMED"
    SHADOW_REJECTED = "SHADOW_REJECTED"
    TRADING_READY_CANDIDATE = "TRADING_READY_CANDIDATE"
    FUNDED_DEPLOYMENT_ELIGIBLE = "FUNDED_DEPLOYMENT_ELIGIBLE"


FATAL_INVALIDATIONS = frozenset(
    {
        "lookahead",
        "target_leakage",
        "corrupted_data",
        "invalid_contract",
        "invalid_roll",
        "future_higher_timeframe",
        "impossible_fill",
        "wrong_multiplier",
        "prohibited_session",
        "uncontrolled_sizing",
        "uncontrolled_mll",
        "governance_violation",
        "duplicate_strategy",
        "outbound_order_capability",
    }
)


@dataclass(frozen=True)
class ShadowEvidence:
    candidate_id: str
    hard_invalidations: tuple[str, ...] = ()
    data_integrity: bool = False
    no_lookahead: bool = False
    deterministic_signals: bool = False
    net_after_costs: float = 0.0
    supportive_temporal_folds: int = 0
    catastrophic_transfer: bool = False
    candidate_null_pass: bool = False
    null_probability: float = 1.0
    parameter_stable: bool = False
    contract_evidence: bool = False
    account_mll_safe: bool = False
    execution_possible: bool = False
    realtime_features_available: bool = False
    shadow_spec_complete: bool = False
    observability_complete: bool = False
    untouched_holdout_passed: bool = False
    sample_size: int = 0
    uncertainty: str = "unquantified"


@dataclass(frozen=True)
class ShadowAdmissionDecision:
    tier: EvidenceTier
    fatal_reasons: tuple[str, ...]
    missing_requirements: tuple[str, ...]
    uncertainty: str
    permits_zero_risk_shadow: bool
    permits_broker_orders: bool = False

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["tier"] = self.tier.value
        return payload


def decide_shadow_admission(evidence: ShadowEvidence) -> ShadowAdmissionDecision:
    fatal = tuple(sorted(set(evidence.hard_invalidations) & FATAL_INVALIDATIONS))
    if fatal:
        return ShadowAdmissionDecision(
            EvidenceTier.SHADOW_REJECTED, fatal, (), evidence.uncertainty, False
        )
    validity = {
        "data_integrity": evidence.data_integrity,
        "no_lookahead": evidence.no_lookahead,
        "deterministic_signals": evidence.deterministic_signals,
        "execution_possible": evidence.execution_possible,
    }
    missing_validity = tuple(name for name, passed in validity.items() if not passed)
    if missing_validity:
        return ShadowAdmissionDecision(
            EvidenceTier.RESEARCH_PROTOTYPE,
            (),
            missing_validity,
            evidence.uncertainty,
            False,
        )
    if evidence.net_after_costs <= 0:
        return ShadowAdmissionDecision(
            EvidenceTier.RESEARCH_PROTOTYPE,
            (),
            ("positive_net_economics",),
            evidence.uncertainty,
            False,
        )
    if evidence.supportive_temporal_folds < 1 or evidence.catastrophic_transfer:
        return ShadowAdmissionDecision(
            EvidenceTier.RAW_ECONOMIC_SIGNAL,
            (),
            ("defensible_temporal_transfer",),
            evidence.uncertainty,
            False,
        )
    candidate_support = evidence.candidate_null_pass and evidence.null_probability <= 0.20
    if not candidate_support:
        return ShadowAdmissionDecision(
            EvidenceTier.PROMISING_RESEARCH_CANDIDATE,
            (),
            ("candidate_level_null_evidence",),
            evidence.uncertainty,
            False,
        )
    robust = evidence.parameter_stable and evidence.contract_evidence
    if not robust:
        return ShadowAdmissionDecision(
            EvidenceTier.PROMISING_RESEARCH_CANDIDATE,
            (),
            tuple(
                name
                for name, passed in {
                    "parameter_stability": evidence.parameter_stable,
                    "contract_evidence": evidence.contract_evidence,
                }.items()
                if not passed
            ),
            evidence.uncertainty,
            False,
        )
    safe_package = {
        "account_mll_safe": evidence.account_mll_safe,
        "realtime_features_available": evidence.realtime_features_available,
        "shadow_spec_complete": evidence.shadow_spec_complete,
        "observability_complete": evidence.observability_complete,
    }
    missing_package = tuple(name for name, passed in safe_package.items() if not passed)
    if missing_package:
        return ShadowAdmissionDecision(
            EvidenceTier.ROBUST_RESEARCH_CANDIDATE,
            (),
            missing_package,
            evidence.uncertainty,
            False,
        )
    # A safe, fully specified candidate may collect forward evidence even when
    # development evidence remains statistically weak.  Stronger evidence is
    # required for the PAPER_SHADOW_READY label, but neither tier can order.
    tier = (
        EvidenceTier.PAPER_SHADOW_READY
        if evidence.supportive_temporal_folds >= 2
        and evidence.null_probability <= 0.05
        and evidence.sample_size >= 30
        and evidence.untouched_holdout_passed
        else EvidenceTier.SHADOW_RESEARCH_CANDIDATE
    )
    return ShadowAdmissionDecision(tier, (), (), evidence.uncertainty, True)


def calibrate_shadow_policy(
    negative_controls: Iterable[ShadowEvidence],
    weak_real_controls: Iterable[ShadowEvidence],
    strong_controls: Iterable[ShadowEvidence],
) -> dict[str, object]:
    negatives = [decide_shadow_admission(item) for item in negative_controls]
    weak = [decide_shadow_admission(item) for item in weak_real_controls]
    strong = [decide_shadow_admission(item) for item in strong_controls]
    admitted = {EvidenceTier.SHADOW_RESEARCH_CANDIDATE, EvidenceTier.PAPER_SHADOW_READY}
    fpr = sum(item.tier in admitted for item in negatives) / max(len(negatives), 1)
    weak_power = sum(item.tier in admitted for item in weak) / max(len(weak), 1)
    strong_power = sum(item.tier == EvidenceTier.PAPER_SHADOW_READY for item in strong) / max(
        len(strong), 1
    )
    return {
        "negative_control_count": len(negatives),
        "weak_real_control_count": len(weak),
        "strong_control_count": len(strong),
        "false_positive_rate": fpr,
        "weak_real_shadow_admission_power": weak_power,
        "strong_paper_shadow_power": strong_power,
        "passed": fpr <= 0.05 and weak_power >= 0.80 and strong_power >= 0.80,
    }
