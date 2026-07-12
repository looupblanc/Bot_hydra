from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class EvidenceGap:
    name: str
    stage: str
    missing: bool
    role_required: bool
    hard_if_missing: bool
    estimated_compute_seconds: float
    estimated_data_cost_usd: float
    decision_change_probability: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def evidence_gaps(
    candidate: Mapping[str, Any],
    exact: Mapping[str, Any] | None,
) -> tuple[EvidenceGap, ...]:
    role = str(candidate.get("role") or "")
    topstep = dict(candidate.get("topstep") or {})
    null = dict(candidate.get("candidate_null") or {})
    contract = dict(candidate.get("contract_transfer") or {})
    delay = dict(candidate.get("delay_resilience") or {})
    neighborhood = dict(candidate.get("parameter_neighborhood") or {})
    exact = dict(exact or {})
    checks = (
        ("exact_temporal_replay", "FULL_ECONOMIC_REPLAY", bool(exact), True, True, 1.0, 0.0, 0.25),
        ("candidate_level_null_suite", "FULL_PROMOTION_VALIDATION", "raw_probability" in null and bool(null.get("method")), True, False, 4.0, 0.0, 0.30),
        ("mini_micro_contract_transfer", "FULL_PROMOTION_VALIDATION", bool(contract), True, False, 1.0, 0.0, 0.18),
        ("delay_stress", "FULL_PROMOTION_VALIDATION", bool(delay), True, False, 0.5, 0.0, 0.12),
        ("elevated_cost_stress", "FULL_ECONOMIC_REPLAY", "cost_stress_1_5x_net" in candidate or "cost_stress_1_5x_net" in exact, True, False, 0.2, 0.0, 0.10),
        ("best_trade_removal", "FULL_PROMOTION_VALIDATION", "best_trade_removal" in candidate, True, False, 0.1, 0.0, 0.12),
        ("best_day_removal", "FULL_PROMOTION_VALIDATION", "best_day_removal" in candidate, True, False, 0.1, 0.0, 0.18),
        ("best_month_removal", "FULL_PROMOTION_VALIDATION", "best_month_removal" in candidate, True, False, 0.1, 0.0, 0.22),
        ("block_bootstrap", "FULL_PROMOTION_VALIDATION", "block_bootstrap" in candidate, True, False, 1.5, 0.0, 0.20),
        ("parameter_neighborhood", "FULL_PROMOTION_VALIDATION", bool(neighborhood), True, False, 1.0, 0.0, 0.10),
        ("intraday_unrealized_mll", "FULL_RISK_REPLAY", bool(candidate.get("intraday_unrealized_mll")), True, True, 6.0, 0.0, 0.32),
        ("combine_path", "FULL_RISK_REPLAY", bool(topstep.get("combine")), role == "COMBINE_PASSER", False, 1.0, 0.0, 0.20),
        ("xfa_standard", "FULL_RISK_REPLAY", bool(topstep.get("xfa_standard")), role == "XFA_PAYOUT", False, 1.0, 0.0, 0.20),
        ("xfa_consistency", "FULL_RISK_REPLAY", bool(topstep.get("xfa_consistency")), role == "XFA_PAYOUT", False, 1.0, 0.0, 0.20),
        ("matched_account_utility_control", "FULL_PROMOTION_VALIDATION", bool(candidate.get("matched_account_utility_control")), role in {"DEFENSIVE", "PORTFOLIO_ONLY"}, False, 8.0, 0.0, 0.38),
        ("shared_account_interaction", "FULL_PROMOTION_VALIDATION", bool(candidate.get("shared_account_interaction")), True, False, 4.0, 0.0, 0.24),
        ("immutable_shadow_package", "SHADOW", bool(candidate.get("immutable_shadow_package")), True, True, 2.0, 0.0, 0.20),
        ("sealed_q4_decision", "HOLDOUT", bool(candidate.get("q4_result")), True, True, 0.0, 0.0, 0.45),
        ("fresh_forward_evidence", "FORWARD", bool(candidate.get("forward_observations")), True, False, 0.0, 0.0, 0.35),
    )
    return tuple(
        EvidenceGap(
            name=name,
            stage=stage,
            missing=not present,
            role_required=required,
            hard_if_missing=hard,
            estimated_compute_seconds=compute,
            estimated_data_cost_usd=data_cost,
            decision_change_probability=decision_probability,
        )
        for name, stage, present, required, hard, compute, data_cost, decision_probability in checks
    )
