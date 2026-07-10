from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from hydra.promotion.export import export_research_config
from hydra.promotion.gates import (
    correlation_gate,
    data_integrity_gate,
    duplicate_gate,
    economic_gate,
    execution_readiness_gate,
    funded_gate,
    monte_carlo_gate,
    no_lookahead_gate,
    oos_gate,
    parameter_sensitivity_gate,
    parameter_zone,
    payout_gate,
    portfolio_interaction_gate,
    strategy_fingerprint,
    topstep_combine_gate,
    walk_forward_gate,
)
from hydra.promotion.readiness import decide_readiness
from hydra.validation.status_provenance import (
    FULL,
    build_promotion_input_fingerprint,
    build_validation_provenance,
    gate_computation_modes,
)


@dataclass(frozen=True)
class PromotionInput:
    candidate: Any
    result: Any
    daily: pd.DataFrame
    topstep_record: dict[str, Any]
    data_validation: dict[str, Any]
    split_scores: dict[str, float]
    leak_ok: bool
    leak_reason: str
    existing_fingerprints: set[str]
    max_correlation: float
    seed: int
    lane: str
    report_tag: str


def run_promotion_pipeline(payload: PromotionInput) -> dict[str, Any]:
    fingerprint = strategy_fingerprint(payload.candidate)
    input_fingerprint = build_promotion_input_fingerprint(
        candidate=payload.candidate,
        metrics=payload.result.metrics,
        topstep_record=payload.topstep_record,
        split_scores=payload.split_scores,
        leak_ok=payload.leak_ok,
        leak_reason=payload.leak_reason,
        data_validation=payload.data_validation,
        max_correlation=payload.max_correlation,
        seed=payload.seed,
    )
    gates = [
        data_integrity_gate(payload.data_validation),
        duplicate_gate(fingerprint, payload.existing_fingerprints),
        no_lookahead_gate(payload.leak_ok, payload.leak_reason),
        economic_gate(payload.result.metrics, payload.daily),
        walk_forward_gate(payload.split_scores),
        oos_gate(payload.split_scores),
        monte_carlo_gate(payload.result, payload.seed),
        parameter_sensitivity_gate(payload.candidate),
        topstep_combine_gate(payload.topstep_record),
        funded_gate(payload.topstep_record),
        payout_gate(payload.topstep_record),
        correlation_gate(payload.max_correlation),
    ]
    economic_score = _gate_score(gates, "ECONOMIC_PROFILE")
    topstep_score = float(payload.topstep_record.get("topstep_score", 0.0))
    preliminary_score = round(0.35 * economic_score + 0.45 * topstep_score + 0.20 * _average_gate_score(gates), 6)
    decision = decide_readiness(gates, preliminary_score, economic_score, topstep_score)
    exported_strategy, exported_risk = None, None
    gates.extend(
        [
            portfolio_interaction_gate(payload.topstep_record, payload.max_correlation),
            execution_readiness_gate(payload.candidate, bool(exported_strategy and exported_risk) or decision.classification != "TRADING_READY_CANDIDATE"),
        ]
    )
    promotion_score = round(0.35 * economic_score + 0.40 * topstep_score + 0.25 * _average_gate_score(gates), 6)
    decision = decide_readiness(gates, promotion_score, economic_score, topstep_score)
    provenance = build_validation_provenance(
        input_fingerprint=input_fingerprint,
        gate_modes={name: mode for name, mode in gate_computation_modes().items() if any(g.name == name for g in gates)},
        notes=[
            "Only FULL evidence may support final promotion.",
            "PROXY gates are allowed for research classification but not final trading-ready promotion without deeper evidence.",
        ],
    )
    if decision.classification == "TRADING_READY_CANDIDATE" and not exported_strategy:
        exported_strategy, exported_risk = export_research_config(payload.candidate, {"classification": decision.classification}, payload.report_tag)
    status = decision.status
    classification = decision.classification
    rejection_reason = decision.rejection_reason
    recommended_action = decision.recommended_action
    branch_action = decision.branch_action
    if classification == "TRADING_READY_CANDIDATE" and provenance.computation_mode != FULL:
        status = "TOPSTEP_VIABLE"
        classification = "EXECUTION_VALIDATION_REQUIRED"
        rejection_reason = "final_promotion_requires_full_validation_evidence"
        recommended_action = "replace_proxy_gates_with_full_trade_and_portfolio_evidence"
        branch_action = "retest"
    return {
        "status": status,
        "classification": classification,
        "rejection_reason": rejection_reason,
        "promotion_stage": decision.promotion_stage,
        "promotion_score": promotion_score,
        "economic_score": economic_score,
        "execution_readiness_score": _gate_score(gates, "EXECUTION_READINESS"),
        "recommended_action": recommended_action,
        "branch_action": branch_action,
        "strategy_fingerprint": fingerprint,
        "parameter_zone": parameter_zone(payload.candidate),
        "research_lane": payload.lane,
        "gate_history": [g.to_dict() for g in gates],
        "config_export_path": exported_strategy,
        "risk_export_path": exported_risk,
        "lineage": {
            "parent_candidate_id": payload.candidate.parent_candidate_id,
            "mutation_type": payload.candidate.mutation_type,
        },
        "validation_provenance": provenance.to_dict(),
        "validation_version": provenance.validation_version,
        "input_fingerprint": provenance.input_fingerprint,
        "validation_computed_at": provenance.computed_at,
        "computation_mode": provenance.computation_mode,
        "evidence_strength": provenance.evidence_strength,
    }


def _gate_score(gates, name: str) -> float:
    for gate in gates:
        if gate.name == name:
            return gate.score
    return 0.0


def _average_gate_score(gates) -> float:
    return sum(g.score for g in gates) / max(len(gates), 1)
