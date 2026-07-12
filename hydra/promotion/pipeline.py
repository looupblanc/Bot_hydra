from __future__ import annotations

from dataclasses import asdict, dataclass, is_dataclass
from inspect import signature
from typing import Any, Mapping

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
from hydra.promotion.readiness import (
    ACCOUNT_PHASE_GATES,
    decide_readiness,
    decision_gates,
    normalize_target_pool,
    required_account_gates,
    required_objective_gates,
)
from hydra.validation import status_provenance as _status_provenance


FULL = _status_provenance.FULL


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
    target_pool: str | None = None


def run_promotion_pipeline(payload: PromotionInput) -> dict[str, Any]:
    target_pool = _resolve_target_pool(payload)
    fingerprint = strategy_fingerprint(payload.candidate)
    input_fingerprint = _build_input_fingerprint(
        candidate=payload.candidate,
        metrics=payload.result.metrics,
        topstep_record=payload.topstep_record,
        split_scores=payload.split_scores,
        leak_ok=payload.leak_ok,
        leak_reason=payload.leak_reason,
        data_validation=payload.data_validation,
        max_correlation=payload.max_correlation,
        seed=payload.seed,
        target_pool=target_pool,
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
    legacy_topstep_score = float(payload.topstep_record.get("topstep_score", 0.0))
    account_objective_score = _account_objective_score(gates, target_pool, legacy_topstep_score)
    preliminary_score = _promotion_score(
        economic_score,
        account_objective_score,
        _average_gate_score(decision_gates(gates, target_pool)),
        target_pool,
        final=False,
    )
    decision = decide_readiness(
        gates,
        preliminary_score,
        economic_score,
        account_objective_score,
        target_pool,
    )
    exported_strategy, exported_risk = None, None
    execution_export_available = bool(exported_strategy and exported_risk) or (
        decision.classification != "TRADING_READY_CANDIDATE"
    )
    gates.extend(
        [
            portfolio_interaction_gate(payload.topstep_record, payload.max_correlation),
            execution_readiness_gate(payload.candidate, execution_export_available),
        ]
    )
    account_objective_score = _account_objective_score(gates, target_pool, legacy_topstep_score)
    scoped_gates = decision_gates(gates, target_pool)
    promotion_score = _promotion_score(
        economic_score,
        account_objective_score,
        _average_gate_score(scoped_gates),
        target_pool,
        final=True,
    )
    decision = decide_readiness(
        gates,
        promotion_score,
        economic_score,
        account_objective_score,
        target_pool,
    )
    provenance = _build_validation_provenance(input_fingerprint, scoped_gates)
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
    required_pool_gates = required_objective_gates(target_pool)
    gate_by_name = {gate.name: gate for gate in gates}
    target_pool_objective_passed = bool(target_pool) and all(
        name in gate_by_name and gate_by_name[name].passed for name in required_pool_gates
    )
    gate_applicability = _gate_applicability(gates, target_pool)
    provenance_payload = provenance.to_dict()
    provenance_payload["target_pool"] = target_pool
    provenance_payload["decision_gate_names"] = [gate.name for gate in scoped_gates]
    return {
        "status": status,
        "classification": classification,
        "rejection_reason": rejection_reason,
        "promotion_stage": decision.promotion_stage,
        "promotion_score": promotion_score,
        "economic_score": economic_score,
        "account_objective_score": account_objective_score,
        "execution_readiness_score": _gate_score(gates, "EXECUTION_READINESS"),
        "recommended_action": recommended_action,
        "branch_action": branch_action,
        "strategy_fingerprint": fingerprint,
        "parameter_zone": parameter_zone(payload.candidate),
        "research_lane": payload.lane,
        "target_pool": target_pool,
        "target_pool_objective_passed": target_pool_objective_passed,
        "required_target_pool_gates": sorted(required_pool_gates),
        "non_target_account_gates": sorted(ACCOUNT_PHASE_GATES - required_account_gates(target_pool)),
        "gate_applicability": gate_applicability,
        "gate_history": [g.to_dict() for g in gates],
        "config_export_path": exported_strategy,
        "risk_export_path": exported_risk,
        "lineage": {
            "parent_candidate_id": payload.candidate.parent_candidate_id,
            "mutation_type": payload.candidate.mutation_type,
        },
        "validation_provenance": provenance_payload,
        "validation_version": provenance.validation_version,
        "input_fingerprint": provenance.input_fingerprint,
        "validation_computed_at": provenance.computed_at,
        "computation_mode": provenance.computation_mode,
        "evidence_strength": _evidence_strength(provenance),
    }


def _gate_score(gates, name: str) -> float:
    for gate in gates:
        if gate.name == name:
            return gate.score
    return 0.0


def _average_gate_score(gates) -> float:
    return sum(g.score for g in gates) / max(len(gates), 1)


def _resolve_target_pool(payload: PromotionInput) -> str | None:
    if payload.target_pool is not None:
        return normalize_target_pool(payload.target_pool)
    candidate = payload.candidate
    if isinstance(candidate, Mapping):
        values = (candidate.get("target_pool"), candidate.get("objective_pool"))
    else:
        values = (getattr(candidate, "target_pool", None), getattr(candidate, "objective_pool", None))
    for value in values:
        if value is not None and str(getattr(value, "value", value)).strip():
            return normalize_target_pool(value)
    return None


def _account_objective_score(gates, target_pool: str | None, legacy_score: float) -> float:
    if target_pool is None:
        return float(legacy_score)
    required = required_objective_gates(target_pool)
    available = [gate.score for gate in gates if gate.name in required]
    if len(available) != len(required):
        return 0.0
    return sum(available) / max(len(available), 1)


def _gate_applicability(gates, target_pool: str | None) -> dict[str, str]:
    required_account = required_account_gates(target_pool)
    required_objective = required_objective_gates(target_pool)
    applicability: dict[str, str] = {}
    for gate in gates:
        if target_pool == "DEFENSIVE_ACCOUNT_POOL" and gate.name == "ECONOMIC_PROFILE":
            applicability[gate.name] = "DIAGNOSTIC_NON_TARGET_ROLE"
        elif gate.name in ACCOUNT_PHASE_GATES and gate.name not in required_account:
            applicability[gate.name] = "DIAGNOSTIC_NON_TARGET_POOL"
        elif target_pool is not None and gate.name in required_objective:
            applicability[gate.name] = "REQUIRED_TARGET_POOL_OBJECTIVE"
        else:
            applicability[gate.name] = "REQUIRED_SCIENTIFIC_OR_INTEGRITY"
    return applicability


def _promotion_score(
    economic_score: float,
    account_objective_score: float,
    average_gate_score: float,
    target_pool: str | None,
    *,
    final: bool,
) -> float:
    if target_pool == "DEFENSIVE_ACCOUNT_POOL":
        # Defensive value is the account objective.  Standalone PnL is still
        # reported as a diagnostic but cannot dominate or hard-kill the role.
        return round(0.65 * account_objective_score + 0.35 * average_gate_score, 6)
    if final:
        return round(
            0.35 * economic_score
            + 0.40 * account_objective_score
            + 0.25 * average_gate_score,
            6,
        )
    return round(
        0.35 * economic_score
        + 0.45 * account_objective_score
        + 0.20 * average_gate_score,
        6,
    )


def _build_input_fingerprint(*, target_pool: str | None, **kwargs: Any) -> str:
    builder = getattr(_status_provenance, "build_promotion_input_fingerprint", None)
    if callable(builder):
        base_fingerprint = str(builder(**kwargs))
    else:
        base_fingerprint = _status_provenance.provenance_hash(
            {
                **{key: value for key, value in kwargs.items() if key != "candidate"},
                "candidate": _candidate_payload(kwargs["candidate"]),
            }
        )
    return _status_provenance.provenance_hash(
        {
            "base_input_fingerprint": base_fingerprint,
            "target_pool_contract": target_pool or "LEGACY_ALL_ACCOUNT_PHASE_GATES",
        }
    )


def _candidate_payload(candidate: Any) -> Any:
    if is_dataclass(candidate):
        return asdict(candidate)
    if isinstance(candidate, Mapping):
        return dict(candidate)
    return {
        key: getattr(candidate, key, None)
        for key in (
            "candidate_id",
            "family",
            "symbol",
            "timeframe",
            "parameters",
            "risk_parameters",
            "parent_candidate_id",
            "mutation_type",
        )
    }


def _build_validation_provenance(input_fingerprint: str, gates) -> Any:
    configured_modes = getattr(_status_provenance, "gate_computation_modes", None)
    configured = dict(configured_modes()) if callable(configured_modes) else {}
    proxy = getattr(_status_provenance, "PROXY", "PROXY")
    gate_modes = {gate.name: configured.get(gate.name, proxy) for gate in gates}
    builder = _status_provenance.build_validation_provenance
    if "notes" in signature(builder).parameters:
        return builder(
            input_fingerprint=input_fingerprint,
            gate_modes=gate_modes,
            notes=[
                "Only FULL evidence may support final promotion.",
                "Non-target account gates are diagnostic and cannot lend status across pools.",
            ],
        )
    return builder(input_fingerprint=input_fingerprint, gate_modes=gate_modes)


def _evidence_strength(provenance: Any) -> float:
    value = getattr(provenance, "evidence_strength", None)
    if value is not None:
        return float(value)
    return 1.0 if provenance.computation_mode == FULL else 0.0
