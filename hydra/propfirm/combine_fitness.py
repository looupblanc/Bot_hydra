from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from typing import Any, Mapping

from hydra.propfirm.payout_episode import RollingXfaSummary
from hydra.propfirm.rolling_combine import RollingCombineSummary


@dataclass(frozen=True, slots=True)
class FitnessResult:
    objective: str
    score: float
    components: dict[str, float]
    penalties: dict[str, float]
    hard_invalidated: bool
    factory_survivor: bool
    elite: bool
    decision: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class DefensiveAccountEvidence:
    baseline_mll_breach_rate: float
    candidate_mll_breach_rate: float
    baseline_shared_loss_day_rate: float
    candidate_shared_loss_day_rate: float
    baseline_drawdown: float
    candidate_drawdown: float
    baseline_target_velocity: float
    candidate_target_velocity: float
    matched_control_probability: float


def combine_passer_fitness(
    summary: RollingCombineSummary,
    *,
    cost_stress_net_pnl: float,
    complexity: float = 1.0,
    behaviorally_duplicate: bool = False,
    hard_invalidated: bool = False,
) -> FitnessResult:
    net = summary.net_pnl_after_costs_unique_events
    net_economics = _clip01(math.tanh(max(net, 0.0) / 9000.0))
    target_velocity = (
        _clip01(1.0 - float(summary.median_days_to_target) / 60.0)
        if summary.median_days_to_target is not None
        else _clip01(summary.median_target_progress_when_not_passed)
    )
    components = {
        "combine_pass_rate": _clip01(summary.pass_rate),
        "mll_survival_rate": _clip01(1.0 - summary.mll_breach_rate),
        "target_velocity": target_velocity,
        "net_economics": net_economics,
        "mll_buffer": _clip01(summary.minimum_mll_buffer / 4500.0),
        "consistency_margin": _clip01(summary.consistency_pass_rate),
        "opportunity_frequency": _clip01(summary.event_count / 50.0),
        "cost_resilience": _clip01(
            cost_stress_net_pnl / max(abs(net), 1.0) if net > 0 else 0.0
        ),
    }
    penalties = {
        "mll_breach": 0.20 * _clip01(summary.mll_breach_rate),
        "concentration": 0.08
        * _clip01((summary.p90_best_day_concentration - 0.50) / 0.50),
        "complexity": 0.03 * _clip01(complexity / 10.0),
        "behavioral_duplicate": 0.20 if behaviorally_duplicate else 0.0,
        "compliance": 0.30
        * (1.0 - min(summary.contract_limit_compliance_rate, summary.session_compliance_rate)),
    }
    raw = (
        0.30 * components["combine_pass_rate"]
        + 0.22 * components["mll_survival_rate"]
        + 0.12 * components["target_velocity"]
        + 0.12 * components["net_economics"]
        + 0.08 * components["mll_buffer"]
        + 0.07 * components["consistency_margin"]
        + 0.05 * components["opportunity_frequency"]
        + 0.04 * components["cost_resilience"]
        - sum(penalties.values())
    )
    score = 0.0 if hard_invalidated else _clip01(raw)
    survivor = bool(
        not hard_invalidated
        and not behaviorally_duplicate
        and net > 0
        and score >= 0.42
        and summary.mll_breach_rate < 0.50
        and summary.compliance_failure_count == 0
    )
    elite = bool(
        survivor
        and score >= 0.62
        and summary.pass_rate >= 0.10
        and summary.mll_breach_rate <= 0.25
        and summary.consistency_pass_rate >= 0.70
    )
    return FitnessResult(
        objective="COMBINE_PASSER_FITNESS",
        score=round(score, 8),
        components={key: round(value, 8) for key, value in components.items()},
        penalties={key: round(value, 8) for key, value in penalties.items()},
        hard_invalidated=hard_invalidated,
        factory_survivor=survivor,
        elite=elite,
        decision="ELITE" if elite else "KEEP" if survivor else "MUTATE_OR_KILL",
    )


def xfa_payout_fitness(
    summary: RollingXfaSummary,
    *,
    complexity: float = 1.0,
    behaviorally_duplicate: bool = False,
    hard_invalidated: bool = False,
) -> FitnessResult:
    components = {
        "payout_cycles": _clip01(summary.expected_payout_cycles_before_ruin / 2.0),
        "payout_probability": _clip01(summary.payout_probability),
        "mll_survival": _clip01(summary.survival_rate),
        "post_payout_survival": _clip01(summary.post_payout_survival_rate),
        "payout_velocity": (
            _clip01(1.0 - summary.median_first_payout_day / 120.0)
            if summary.median_first_payout_day is not None
            else 0.0
        ),
        "qualifying_day_frequency": _clip01(
            summary.qualifying_day_frequency / 0.35
        ),
        "net_payout": _clip01(summary.median_trader_net_payout / 5000.0),
        "mll_buffer": _clip01(summary.minimum_mll_buffer / 4500.0),
    }
    penalties = {
        "complexity": 0.03 * _clip01(complexity / 10.0),
        "behavioral_duplicate": 0.20 if behaviorally_duplicate else 0.0,
    }
    raw = (
        0.22 * components["payout_cycles"]
        + 0.16 * components["payout_probability"]
        + 0.18 * components["mll_survival"]
        + 0.14 * components["post_payout_survival"]
        + 0.08 * components["payout_velocity"]
        + 0.08 * components["qualifying_day_frequency"]
        + 0.08 * components["net_payout"]
        + 0.06 * components["mll_buffer"]
        - sum(penalties.values())
    )
    score = 0.0 if hard_invalidated else _clip01(raw)
    survivor = bool(
        not hard_invalidated
        and not behaviorally_duplicate
        and summary.survival_rate >= 0.50
        and summary.payout_probability > 0
        and score >= 0.40
    )
    elite = bool(
        survivor
        and score >= 0.62
        and summary.expected_payout_cycles_before_ruin >= 1.0
        and summary.post_payout_survival_rate >= 0.70
    )
    return FitnessResult(
        objective="XFA_PAYOUT_FITNESS",
        score=round(score, 8),
        components={key: round(value, 8) for key, value in components.items()},
        penalties={key: round(value, 8) for key, value in penalties.items()},
        hard_invalidated=hard_invalidated,
        factory_survivor=survivor,
        elite=elite,
        decision="ELITE" if elite else "KEEP" if survivor else "MUTATE_OR_KILL",
    )


def defensive_account_fitness(
    evidence: DefensiveAccountEvidence,
    *,
    complexity: float = 1.0,
    behaviorally_duplicate: bool = False,
    hard_invalidated: bool = False,
) -> FitnessResult:
    components = {
        "mll_breach_reduction": _relative_reduction(
            evidence.baseline_mll_breach_rate, evidence.candidate_mll_breach_rate
        ),
        "shared_loss_day_reduction": _relative_reduction(
            evidence.baseline_shared_loss_day_rate,
            evidence.candidate_shared_loss_day_rate,
        ),
        "drawdown_reduction": _relative_reduction(
            evidence.baseline_drawdown, evidence.candidate_drawdown
        ),
        "target_velocity_retention": _clip01(
            evidence.candidate_target_velocity
            / max(evidence.baseline_target_velocity, 1e-12)
        ),
        "matched_control_evidence": _clip01(
            1.0 - evidence.matched_control_probability
        ),
    }
    penalties = {
        "target_velocity_cost": 0.12
        * _clip01(
            1.0
            - evidence.candidate_target_velocity
            / max(evidence.baseline_target_velocity, 1e-12)
        ),
        "complexity": 0.03 * _clip01(complexity / 10.0),
        "behavioral_duplicate": 0.20 if behaviorally_duplicate else 0.0,
    }
    raw = (
        0.30 * components["mll_breach_reduction"]
        + 0.20 * components["shared_loss_day_reduction"]
        + 0.20 * components["drawdown_reduction"]
        + 0.15 * components["target_velocity_retention"]
        + 0.15 * components["matched_control_evidence"]
        - sum(penalties.values())
    )
    score = 0.0 if hard_invalidated else _clip01(raw)
    survivor = bool(
        not hard_invalidated
        and not behaviorally_duplicate
        and components["mll_breach_reduction"] > 0
        and components["drawdown_reduction"] > 0
        and score >= 0.40
    )
    elite = bool(
        survivor
        and score >= 0.62
        and evidence.matched_control_probability <= 0.10
    )
    return FitnessResult(
        objective="DEFENSIVE_ACCOUNT_FITNESS",
        score=round(score, 8),
        components={key: round(value, 8) for key, value in components.items()},
        penalties={key: round(value, 8) for key, value in penalties.items()},
        hard_invalidated=hard_invalidated,
        factory_survivor=survivor,
        elite=elite,
        decision="ELITE" if elite else "KEEP" if survivor else "MUTATE_OR_KILL",
    )


def diagnose_combine_failure(
    summary: RollingCombineSummary,
    *,
    cost_stress_net_pnl: float,
    hard_invalidated: bool = False,
) -> str:
    if hard_invalidated or summary.compliance_failure_count:
        return "HARD_INVALIDATION"
    if summary.net_pnl_after_costs_unique_events <= 0:
        return "NET_NEGATIVE"
    if summary.mll_breach_rate >= 0.25:
        return "MLL_BREACH"
    if cost_stress_net_pnl <= 0:
        return "COST_FRAGILE"
    if summary.event_count < 8:
        return "INSUFFICIENT_TRADES"
    if summary.consistency_pass_rate < 0.70:
        return "CONSISTENCY_FAILURE"
    if summary.pass_rate <= 0:
        return "TARGET_NOT_REACHED"
    return "NEAR_PASS_OR_ELITE"


def diagnose_xfa_failure(
    summary: RollingXfaSummary,
    *,
    hard_invalidated: bool = False,
) -> str:
    if hard_invalidated:
        return "HARD_INVALIDATION"
    if summary.survival_rate < 0.50:
        return "MLL_BREACH"
    if summary.payout_probability <= 0.0:
        return (
            "INSUFFICIENT_TRADES"
            if sum(row.event_count for row in summary.episodes) < 8
            else "TARGET_NOT_REACHED"
        )
    if summary.expected_payout_cycles_before_ruin < 1.0:
        return "TARGET_NOT_REACHED"
    return "NEAR_PASS_OR_ELITE"


def _relative_reduction(baseline: float, candidate: float) -> float:
    return _clip01((baseline - candidate) / max(abs(baseline), 1e-12))


def _clip01(value: float) -> float:
    if not math.isfinite(value):
        return 0.0
    return float(max(0.0, min(1.0, value)))


__all__ = [
    "DefensiveAccountEvidence",
    "FitnessResult",
    "combine_passer_fitness",
    "defensive_account_fitness",
    "diagnose_combine_failure",
    "diagnose_xfa_failure",
    "xfa_payout_fitness",
]
