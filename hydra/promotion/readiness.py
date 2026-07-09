from __future__ import annotations

from dataclasses import dataclass

from hydra.promotion.gates import GateResult, HARD_FAIL, SOFT_FAIL


@dataclass(frozen=True)
class ReadinessDecision:
    classification: str
    status: str
    promotion_stage: str
    rejection_reason: str | None
    recommended_action: str
    branch_action: str


PROMOTION_ORDER = [
    "GENERATED",
    "BACKTESTED",
    "COST_ADJUSTED",
    "NO_LOOKAHEAD_PASSED",
    "WALK_FORWARD_PASSED",
    "OOS_PASSED",
    "MONTE_CARLO_PASSED",
    "PARAMETER_SENSITIVITY_PASSED",
    "TOPSTEP_COMBINE_PASSED",
    "FUNDED_XFA_PASSED",
    "PAYOUT_SURVIVAL_PASSED",
    "CORRELATION_PASSED",
    "PORTFOLIO_INTERACTION_PASSED",
    "EXECUTION_READINESS_PASSED",
    "TRADING_READY_CANDIDATE",
]


def decide_readiness(gates: list[GateResult], promotion_score: float, economic_score: float, topstep_score: float) -> ReadinessDecision:
    hard = [g for g in gates if not g.passed and g.severity == HARD_FAIL]
    soft = [g for g in gates if not g.passed and g.severity == SOFT_FAIL]
    first_failed = (hard or soft or [None])[0]
    passed_names = {g.name for g in gates if g.passed}
    if hard:
        return ReadinessDecision("DEAD_STRATEGY", "DEAD_STRATEGY", _stage_for_passes(passed_names), hard[0].reason, hard[0].recommended_action, "kill")
    if all(g.passed for g in gates) and promotion_score >= 0.78:
        return ReadinessDecision("TRADING_READY_CANDIDATE", "TRADING_READY_CANDIDATE", "TRADING_READY_CANDIDATE", None, "export_for_paper_shadow_research", "expand")
    if topstep_score >= 0.62 and economic_score >= 0.45 and not hard:
        return ReadinessDecision("TOPSTEP_VIABLE", "TOPSTEP_VIABLE", _stage_for_passes(passed_names), first_failed.reason if first_failed else None, "deepen_validation_and_portfolio_test", "expand")
    if topstep_score >= 0.45 and not hard:
        return ReadinessDecision("TOPSTEP_NEAR_MISS", "TOPSTEP_NEAR_MISS", _stage_for_passes(passed_names), first_failed.reason if first_failed else None, "mutate_weak_dimension", "mutate")
    if economic_score >= 0.45:
        return ReadinessDecision("ECONOMICALLY_VIABLE", "ECONOMICALLY_VIABLE", _stage_for_passes(passed_names), first_failed.reason if first_failed else None, "improve_topstep_path", "mutate")
    if soft:
        return ReadinessDecision("PROMISING_NEEDS_MUTATION", "PROMISING_NEEDS_MUTATION", _stage_for_passes(passed_names), soft[0].reason, soft[0].recommended_action, "mutate")
    return ReadinessDecision("DEAD_STRATEGY", "DEAD_STRATEGY", "BACKTESTED", "no_actionable_signal", "kill_branch", "kill")


def _stage_for_passes(passed_names: set[str]) -> str:
    mapping = [
        ("DATA_INTEGRITY", "BACKTESTED"),
        ("NO_LOOKAHEAD", "NO_LOOKAHEAD_PASSED"),
        ("WALK_FORWARD", "WALK_FORWARD_PASSED"),
        ("OOS", "OOS_PASSED"),
        ("MONTE_CARLO", "MONTE_CARLO_PASSED"),
        ("PARAMETER_SENSITIVITY", "PARAMETER_SENSITIVITY_PASSED"),
        ("TOPSTEP_COMBINE", "TOPSTEP_COMBINE_PASSED"),
        ("FUNDED_XFA", "FUNDED_XFA_PASSED"),
        ("PAYOUT_SURVIVAL", "PAYOUT_SURVIVAL_PASSED"),
        ("CORRELATION", "CORRELATION_PASSED"),
        ("PORTFOLIO_INTERACTION", "PORTFOLIO_INTERACTION_PASSED"),
        ("EXECUTION_READINESS", "EXECUTION_READINESS_PASSED"),
    ]
    stage = "GENERATED"
    for gate_name, promotion_stage in mapping:
        if gate_name in passed_names:
            stage = promotion_stage
        else:
            break
    return stage
