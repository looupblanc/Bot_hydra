from __future__ import annotations

import math
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


COMBINE_PASSER_POOL = "COMBINE_PASSER_POOL"
XFA_PAYOUT_POOL = "XFA_PAYOUT_POOL"
DEFENSIVE_ACCOUNT_POOL = "DEFENSIVE_ACCOUNT_POOL"
OBJECTIVE_POOLS = frozenset(
    {COMBINE_PASSER_POOL, XFA_PAYOUT_POOL, DEFENSIVE_ACCOUNT_POOL}
)
ACCOUNT_UTILITY_ROLES = frozenset(
    {"DEFENSIVE", "PORTFOLIO_ONLY", "HAZARD"}
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
    strategy_role: str = "ALPHA"
    objective_pool: str = COMBINE_PASSER_POOL
    account_utility_delta: float = 0.0


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
    objective_pool = str(evidence.objective_pool or "").strip().upper()
    strategy_role = str(evidence.strategy_role or "").strip().upper()
    if objective_pool not in OBJECTIVE_POOLS:
        return ShadowAdmissionDecision(
            EvidenceTier.RESEARCH_PROTOTYPE,
            (),
            ("recognized_objective_pool",),
            evidence.uncertainty,
            False,
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
    account_utility_objective = bool(
        objective_pool in {XFA_PAYOUT_POOL, DEFENSIVE_ACCOUNT_POOL}
        or strategy_role in ACCOUNT_UTILITY_ROLES
    )
    finite_economics = bool(
        math.isfinite(float(evidence.net_after_costs))
        and math.isfinite(float(evidence.account_utility_delta))
    )
    if not finite_economics:
        return ShadowAdmissionDecision(
            EvidenceTier.RESEARCH_PROTOTYPE,
            (),
            ("finite_role_specific_economics",),
            evidence.uncertainty,
            False,
        )
    if account_utility_objective and evidence.account_utility_delta <= 0:
        return ShadowAdmissionDecision(
            EvidenceTier.RESEARCH_PROTOTYPE,
            (),
            ("positive_objective_account_utility",),
            evidence.uncertainty,
            False,
        )
    if not account_utility_objective and evidence.net_after_costs <= 0:
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
    diagnostic_uncertainty = tuple(
        name
        for name, passed in {
            "candidate_level_null_diagnostic_unresolved": (
                evidence.candidate_null_pass and evidence.null_probability <= 0.20
            ),
            "parameter_stability_diagnostic_unresolved": evidence.parameter_stable,
            "contract_transfer_diagnostic_unresolved": evidence.contract_evidence,
        }.items()
        if not passed
    )
    uncertainty = _merge_uncertainty(evidence.uncertainty, diagnostic_uncertainty)
    safe_package = {
        "account_mll_safe": evidence.account_mll_safe,
        "realtime_features_available": evidence.realtime_features_available,
        "shadow_spec_complete": evidence.shadow_spec_complete,
        "observability_complete": evidence.observability_complete,
    }
    missing_package = tuple(name for name, passed in safe_package.items() if not passed)
    if missing_package:
        return ShadowAdmissionDecision(
            (
                EvidenceTier.ROBUST_RESEARCH_CANDIDATE
                if not diagnostic_uncertainty
                else EvidenceTier.PROMISING_RESEARCH_CANDIDATE
            ),
            (),
            missing_package,
            uncertainty,
            False,
        )
    # A safe, fully specified candidate may collect forward evidence even when
    # development evidence remains statistically weak.  Stronger evidence is
    # required for the PAPER_SHADOW_READY label, but neither tier can order.
    tier = (
        EvidenceTier.PAPER_SHADOW_READY
        if evidence.supportive_temporal_folds >= 2
        and evidence.candidate_null_pass
        and evidence.null_probability <= 0.05
        and evidence.sample_size >= 30
        and evidence.untouched_holdout_passed
        and evidence.parameter_stable
        and evidence.contract_evidence
        else EvidenceTier.SHADOW_RESEARCH_CANDIDATE
    )
    return ShadowAdmissionDecision(tier, (), (), uncertainty, True)


def _merge_uncertainty(base: str, diagnostics: tuple[str, ...]) -> str:
    values = [] if not base or base == "unquantified" else [base]
    values.extend(diagnostics)
    return ";".join(dict.fromkeys(values)) or "unquantified"


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
