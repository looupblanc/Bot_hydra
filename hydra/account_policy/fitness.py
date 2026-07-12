from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from typing import Any

from hydra.account_policy.basket import AccountPolicyRollingSummary


@dataclass(frozen=True, slots=True)
class AccountFitness:
    objective: str
    score: float
    components: dict[str, float]
    penalties: dict[str, float]
    factory_component: bool
    elite: bool
    decision: str
    comparison: dict[str, float]
    hard_invalidated: bool = False
    inherited_status: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def individual_combine_fitness(
    summary: AccountPolicyRollingSummary,
    *,
    positive_net_after_costs: bool,
    complexity: float = 1.0,
) -> AccountFitness:
    return _base_fitness(
        "INDIVIDUAL_COMBINE_FITNESS",
        summary,
        positive_net_after_costs=positive_net_after_costs,
        complexity=complexity,
        elite_requires_improvement=False,
    )


def basket_combine_fitness(
    summary: AccountPolicyRollingSummary,
    *,
    best_component: AccountPolicyRollingSummary,
    positive_net_after_costs: bool,
    complexity: float,
) -> AccountFitness:
    result = _base_fitness(
        "BASKET_COMBINE_FITNESS",
        summary,
        positive_net_after_costs=positive_net_after_costs,
        complexity=complexity,
        elite_requires_improvement=True,
        baseline=best_component,
    )
    return result


def adaptive_controller_fitness(
    summary: AccountPolicyRollingSummary,
    *,
    static_baseline: AccountPolicyRollingSummary,
    random_baseline: AccountPolicyRollingSummary,
    positive_net_after_costs: bool,
    complexity: float,
    paired_evidence: dict[str, float],
) -> AccountFitness:
    pass_delta = summary.pass_rate - static_baseline.pass_rate
    mll_delta = static_baseline.mll_breach_rate - summary.mll_breach_rate
    progress_delta = (
        summary.target_progress_median - static_baseline.target_progress_median
    )
    random_advantage = (
        summary.target_progress_median - random_baseline.target_progress_median
    ) + (summary.pass_rate - random_baseline.pass_rate)
    consistency_delta = (
        summary.consistency_pass_rate - static_baseline.consistency_pass_rate
    )
    components = _components(summary)
    components.update(
        {
            "pass_rate_improvement": _signed01(pass_delta * 4.0),
            "mll_survival_improvement": _signed01(mll_delta * 4.0),
            "target_progress_improvement": _signed01(progress_delta),
            "consistency_improvement": _signed01(consistency_delta),
            "random_router_advantage": _signed01(random_advantage),
        }
    )
    penalties = _penalties(summary, complexity)
    raw = (
        0.21 * components["pass_rate"]
        + 0.16 * components["mll_survival"]
        + 0.10 * components["target_progress"]
        + 0.08 * components["target_velocity"]
        + 0.07 * components["consistency"]
        + 0.10 * max(components["pass_rate_improvement"], 0.0)
        + 0.08 * max(components["mll_survival_improvement"], 0.0)
        + 0.08 * max(components["target_progress_improvement"], 0.0)
        + 0.05 * max(components["consistency_improvement"], 0.0)
        + 0.07 * max(components["random_router_advantage"], 0.0)
        - sum(penalties.values())
    )
    hard = _hard_invalidated(summary)
    score = 0.0 if hard else _clip01(raw)
    improves_static = bool(
        pass_delta > 1e-12
        or progress_delta >= 0.10
        or (mll_delta >= 0.05 and progress_delta >= -0.05)
    )
    beats_random = random_advantage > 0.05
    paired_static_p = float(paired_evidence["static_one_sided_p"])
    paired_random_p = float(paired_evidence["random_one_sided_p"])
    paired_static_delta = float(paired_evidence["static_median_utility_delta"])
    paired_random_delta = float(paired_evidence["random_median_utility_delta"])
    survivor = bool(
        not hard
        and positive_net_after_costs
        and improves_static
        and beats_random
        and paired_static_delta > 0.0
        and paired_random_delta > 0.0
        and paired_static_p <= 0.25
        and paired_random_p <= 0.25
        and summary.effective_block_count >= 4
        and summary.mll_breach_rate <= 0.35
        and score >= 0.40
    )
    elite = bool(
        survivor
        and summary.pass_rate > 0.0
        and pass_delta >= 0.04
        and paired_static_p <= 0.10
        and paired_random_p <= 0.10
        and summary.mll_breach_rate <= 0.25
        and score >= 0.58
    )
    return AccountFitness(
        objective="ADAPTIVE_CONTROLLER_FITNESS",
        score=round(score, 8),
        components=_round(components),
        penalties=_round(penalties),
        factory_component=survivor,
        elite=elite,
        decision="ACCOUNT_CONTROLLER_ELITE" if elite else "KEEP" if survivor else "FREEZE_OR_MUTATE",
        comparison={
            "static_pass_rate_delta": pass_delta,
            "static_mll_breach_rate_delta": mll_delta,
            "static_target_progress_delta": progress_delta,
            "static_consistency_delta": consistency_delta,
            "random_router_advantage": random_advantage,
            **paired_evidence,
        },
        hard_invalidated=hard,
    )


def paired_controller_evidence(
    candidate: list[dict[str, Any]],
    static: list[dict[str, Any]],
    random: list[dict[str, Any]],
) -> dict[str, float]:
    candidate_by_day = {int(row["start_day"]): row for row in candidate}
    static_by_day = {int(row["start_day"]): row for row in static}
    random_by_day = {int(row["start_day"]): row for row in random}
    descriptive_starts = sorted(
        set(candidate_by_day) & set(static_by_day) & set(random_by_day)
    )
    starts = [
        day
        for day in descriptive_starts
        if bool(candidate_by_day[day].get("effective_block", True))
        and bool(static_by_day[day].get("effective_block", True))
        and bool(random_by_day[day].get("effective_block", True))
    ]
    if len(starts) < 4:
        raise ValueError(
            "controller paired evidence needs at least four independent blocks"
        )
    static_deltas = [
        _episode_utility(candidate_by_day[day])
        - _episode_utility(static_by_day[day])
        for day in starts
    ]
    random_deltas = [
        _episode_utility(candidate_by_day[day])
        - _episode_utility(random_by_day[day])
        for day in starts
    ]
    return {
        "paired_start_count": float(len(starts)),
        "descriptive_start_count": float(len(descriptive_starts)),
        "static_median_utility_delta": float(_median(static_deltas)),
        "random_median_utility_delta": float(_median(random_deltas)),
        "static_one_sided_p": _one_sided_sign_p(static_deltas),
        "random_one_sided_p": _one_sided_sign_p(random_deltas),
    }


def _episode_utility(row: dict[str, Any]) -> float:
    return float(
        2.0 * int(bool(row["passed"]))
        - 2.0 * int(bool(row["mll_breached"]))
        + max(-0.5, min(1.2, float(row["target_progress"])))
        + 0.20 * int(bool(row["consistency_ok"]))
        + 0.10 * math.tanh(float(row["net_pnl"]) / 9000.0)
    )


def _one_sided_sign_p(values: list[float]) -> float:
    nonzero = [value for value in values if abs(value) > 1e-12]
    if not nonzero:
        return 1.0
    positives = sum(value > 0.0 for value in nonzero)
    count = len(nonzero)
    return float(
        sum(math.comb(count, index) for index in range(positives, count + 1))
        / (2**count)
    )


def _median(values: list[float]) -> float:
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return float(ordered[middle])
    return float((ordered[middle - 1] + ordered[middle]) / 2.0)


def _base_fitness(
    objective: str,
    summary: AccountPolicyRollingSummary,
    *,
    positive_net_after_costs: bool,
    complexity: float,
    elite_requires_improvement: bool,
    baseline: AccountPolicyRollingSummary | None = None,
) -> AccountFitness:
    components = _components(summary)
    penalties = _penalties(summary, complexity)
    comparison: dict[str, float] = {}
    improvement = True
    if baseline is not None:
        comparison = {
            "pass_rate_delta": summary.pass_rate - baseline.pass_rate,
            "mll_breach_rate_delta": baseline.mll_breach_rate
            - summary.mll_breach_rate,
            "target_progress_delta": summary.target_progress_median
            - baseline.target_progress_median,
            "consistency_delta": summary.consistency_pass_rate
            - baseline.consistency_pass_rate,
        }
        improvement = bool(
            comparison["pass_rate_delta"] > 1e-12
            or comparison["target_progress_delta"] >= 0.10
            or (
                comparison["mll_breach_rate_delta"] >= 0.05
                and comparison["target_progress_delta"] >= -0.05
            )
        )
    raw = (
        0.28 * components["pass_rate"]
        + 0.18 * components["mll_survival"]
        + 0.18 * components["target_progress"]
        + 0.10 * components["target_velocity"]
        + 0.09 * components["consistency"]
        + 0.07 * components["mll_buffer"]
        + 0.05 * components["net_economics"]
        + 0.05 * components["opportunity_density"]
        - sum(penalties.values())
    )
    hard = _hard_invalidated(summary)
    score = 0.0 if hard else _clip01(raw)
    survivor = bool(
        not hard
        and positive_net_after_costs
        and summary.mll_breach_rate <= 0.35
        and summary.target_progress_median > 0.0
        and summary.effective_block_count >= 4
        and score >= 0.36
    )
    elite = bool(
        survivor
        and summary.pass_rate > 0.0
        and summary.mll_breach_rate <= 0.25
        and summary.consistency_pass_rate >= 0.50
        and score >= 0.50
        and (improvement or not elite_requires_improvement)
    )
    label = (
        "INDIVIDUAL_COMBINE_ELITE"
        if elite and objective.startswith("INDIVIDUAL")
        else "ACCOUNT_BASKET_ELITE"
        if elite
        else "FACTORY_COMPONENT"
        if survivor
        else "FREEZE_OR_MUTATE"
    )
    return AccountFitness(
        objective=objective,
        score=round(score, 8),
        components=_round(components),
        penalties=_round(penalties),
        factory_component=survivor,
        elite=elite,
        decision=label,
        comparison=comparison,
        hard_invalidated=hard,
    )


def _components(summary: AccountPolicyRollingSummary) -> dict[str, float]:
    progress = max(
        summary.target_progress_median,
        0.5 * summary.target_progress_p75,
    )
    return {
        "pass_rate": _clip01(summary.pass_rate),
        "mll_survival": _clip01(1.0 - summary.mll_breach_rate),
        "target_progress": _clip01(progress),
        "target_velocity": _clip01(
            1.0 - (summary.projected_days_to_target or 180.0) / 180.0
        ),
        "consistency": _clip01(summary.consistency_pass_rate),
        "mll_buffer": _clip01(summary.minimum_mll_buffer / 4500.0),
        "net_economics": _clip01(
            math.tanh(max(summary.median_episode_net_pnl, 0.0) / 9000.0)
        ),
        "opportunity_density": _clip01(summary.accepted_event_count / 200.0),
    }


def _penalties(
    summary: AccountPolicyRollingSummary, complexity: float
) -> dict[str, float]:
    return {
        "mll_breach": 0.25 * _clip01(summary.mll_breach_rate),
        "conflicts": 0.08 * _clip01(summary.conflict_rate / 0.25),
        "concentration": 0.08
        * _clip01((summary.median_best_day_concentration - 0.50) / 0.50),
        "complexity": 0.04 * _clip01(complexity / 10.0),
        "compliance": 0.35 * int(summary.compliance_failure_count > 0),
    }


def _hard_invalidated(summary: AccountPolicyRollingSummary) -> bool:
    return bool(summary.compliance_failure_count > 0 or summary.minimum_mll_buffer < -4500.0)


def _signed01(value: float) -> float:
    return float(max(-1.0, min(1.0, value))) if math.isfinite(value) else -1.0


def _clip01(value: float) -> float:
    return float(max(0.0, min(1.0, value))) if math.isfinite(value) else 0.0


def _round(values: dict[str, float]) -> dict[str, float]:
    return {key: round(float(value), 8) for key, value in values.items()}


__all__ = [
    "AccountFitness",
    "adaptive_controller_fitness",
    "basket_combine_fitness",
    "individual_combine_fitness",
    "paired_controller_evidence",
]
