"""Power-aware planning for bounded HYDRA research experiments.

The planner is deliberately upstream of strategy evaluation.  It estimates
whether a proposed batch can answer its preregistered question; it never
promotes a strategy and it never interprets a backtest as evidence.
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from statistics import NormalDist
from typing import Any, Literal


POWER_PLAN_VERSION = "hydra_power_plan_v1"
POWER_SUFFICIENT = "POWER_SUFFICIENT"
INSUFFICIENT_BATCH_POWER = "INSUFFICIENT_BATCH_POWER"


class PowerPlanningError(ValueError):
    """The preregistered power request is internally inconsistent."""


@dataclass(frozen=True)
class PowerPlanningRequest:
    """Inputs known before candidate outcomes are read.

    ``observations_per_structure`` is the number of eligible bars/events in the
    requested development slice before the opportunity-frequency filter.
    ``effect_prevalence`` is the prior probability that a generated structure
    belongs to the effect-bearing class.  It is used only to ensure adequate
    structural search coverage, not to claim that an effect exists.
    """

    minimum_useful_effect: float
    outcome_variance: float
    expected_opportunity_frequency: float
    observations_per_structure: int
    available_events: int
    maximum_structures: int
    target_power: float = 0.80
    alpha: float = 0.05
    two_sided: bool = True
    design_effect: float = 1.0
    effect_prevalence: float = 0.05
    search_coverage_probability: float = 0.95
    minimum_effect_bearing_structures: int = 1


@dataclass(frozen=True)
class PowerPlan:
    schema: str
    status: Literal["POWER_SUFFICIENT", "INSUFFICIENT_BATCH_POWER"]
    standardized_minimum_effect: float
    required_events: int
    available_events: int
    expected_events_per_structure: float
    event_limited_structures_required: int
    search_coverage_structures_required: int
    structures_required: int
    maximum_structures: int
    achieved_power_at_available_events: float
    target_power: float
    alpha: float
    limiting_factors: tuple[str, ...]
    recommended_action: str
    assumptions: dict[str, Any]

    @property
    def sufficient(self) -> bool:
        return self.status == POWER_SUFFICIENT

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def plan_experiment_power(request: PowerPlanningRequest) -> PowerPlan:
    """Return a deterministic normal-approximation power plan.

    The event calculation is the conservative one-sample mean formula with a
    preregistered design-effect multiplier.  Search coverage is calculated
    independently so a large number of events from a tiny structural sample
    cannot masquerade as adequate mechanism exploration.
    """

    _validate_request(request)
    standard_deviation = math.sqrt(request.outcome_variance)
    standardized_effect = request.minimum_useful_effect / standard_deviation
    normal = NormalDist()
    tail_alpha = request.alpha / 2.0 if request.two_sided else request.alpha
    z_alpha = normal.inv_cdf(1.0 - tail_alpha)
    z_power = normal.inv_cdf(request.target_power)
    base_required = ((z_alpha + z_power) / standardized_effect) ** 2
    required_events = max(2, math.ceil(base_required * request.design_effect))

    expected_events_per_structure = (
        request.observations_per_structure
        * request.expected_opportunity_frequency
    )
    event_limited_structures = math.ceil(
        required_events / expected_events_per_structure
    )
    coverage_structures = _coverage_structures(
        prevalence=request.effect_prevalence,
        target_probability=request.search_coverage_probability,
        required_successes=request.minimum_effect_bearing_structures,
    )
    structures_required = max(event_limited_structures, coverage_structures)

    usable_events = min(
        request.available_events,
        math.floor(request.maximum_structures * expected_events_per_structure),
    )
    achieved_power = _approximate_power(
        event_count=usable_events,
        standardized_effect=standardized_effect,
        alpha=request.alpha,
        two_sided=request.two_sided,
        design_effect=request.design_effect,
    )
    limiting: list[str] = []
    if request.available_events < required_events:
        limiting.append("AVAILABLE_EVENTS")
    if request.maximum_structures < event_limited_structures:
        limiting.append("EVENTS_PER_STRUCTURE")
    if request.maximum_structures < coverage_structures:
        limiting.append("STRUCTURAL_SEARCH_COVERAGE")

    if limiting:
        status: Literal["POWER_SUFFICIENT", "INSUFFICIENT_BATCH_POWER"] = (
            INSUFFICIENT_BATCH_POWER
        )
        if "AVAILABLE_EVENTS" in limiting:
            action = "EXTEND_DEVELOPMENT_FOLDS_OR_TEST_HIGHER_FREQUENCY_MARKET"
        elif "STRUCTURAL_SEARCH_COVERAGE" in limiting:
            action = "INCREASE_STRUCTURAL_BATCH_OR_DEFER_HYPOTHESIS"
        else:
            action = "INCREASE_CANDIDATE_BATCH_SIZE"
    else:
        status = POWER_SUFFICIENT
        action = "RUN_PREREGISTERED_BATCH"

    return PowerPlan(
        schema=POWER_PLAN_VERSION,
        status=status,
        standardized_minimum_effect=float(standardized_effect),
        required_events=required_events,
        available_events=request.available_events,
        expected_events_per_structure=float(expected_events_per_structure),
        event_limited_structures_required=event_limited_structures,
        search_coverage_structures_required=coverage_structures,
        structures_required=structures_required,
        maximum_structures=request.maximum_structures,
        achieved_power_at_available_events=float(achieved_power),
        target_power=request.target_power,
        alpha=request.alpha,
        limiting_factors=tuple(limiting),
        recommended_action=action,
        assumptions={
            "calculation": "normal_mean_approximation",
            "two_sided": request.two_sided,
            "design_effect": request.design_effect,
            "effect_prevalence": request.effect_prevalence,
            "search_coverage_probability": request.search_coverage_probability,
            "minimum_effect_bearing_structures": (
                request.minimum_effect_bearing_structures
            ),
            "strategy_evidence": False,
            "uses_candidate_outcomes": False,
        },
    )


def _validate_request(request: PowerPlanningRequest) -> None:
    positive_fields = {
        "minimum_useful_effect": request.minimum_useful_effect,
        "outcome_variance": request.outcome_variance,
        "design_effect": request.design_effect,
    }
    for name, value in positive_fields.items():
        if not math.isfinite(value) or value <= 0.0:
            raise PowerPlanningError(f"{name} must be finite and positive")
    probability_fields = {
        "expected_opportunity_frequency": request.expected_opportunity_frequency,
        "target_power": request.target_power,
        "alpha": request.alpha,
        "effect_prevalence": request.effect_prevalence,
        "search_coverage_probability": request.search_coverage_probability,
    }
    for name, value in probability_fields.items():
        if not math.isfinite(value) or not 0.0 < value < 1.0:
            raise PowerPlanningError(f"{name} must be strictly between zero and one")
    for name, value in {
        "observations_per_structure": request.observations_per_structure,
        "minimum_effect_bearing_structures": request.minimum_effect_bearing_structures,
    }.items():
        if int(value) != value or value <= 0:
            raise PowerPlanningError(f"{name} must be a positive integer")
    for name, value in {
        "available_events": request.available_events,
        "maximum_structures": request.maximum_structures,
    }.items():
        if int(value) != value or value < 0:
            raise PowerPlanningError(f"{name} must be a non-negative integer")


def _coverage_structures(
    *, prevalence: float, target_probability: float, required_successes: int
) -> int:
    """Small deterministic binomial search for P(X >= required_successes)."""

    if required_successes == 1:
        return max(
            1,
            math.ceil(
                math.log1p(-target_probability) / math.log1p(-prevalence)
            ),
        )
    structures = max(required_successes, 1)
    while structures < 10_000_000:
        probability_below = 0.0
        for successes in range(required_successes):
            probability_below += math.comb(structures, successes) * (
                prevalence**successes
            ) * ((1.0 - prevalence) ** (structures - successes))
        if 1.0 - probability_below >= target_probability:
            return structures
        structures += 1
    raise PowerPlanningError("structural coverage requirement exceeds safe bound")


def _approximate_power(
    *,
    event_count: int,
    standardized_effect: float,
    alpha: float,
    two_sided: bool,
    design_effect: float,
) -> float:
    if event_count <= 0:
        return 0.0
    normal = NormalDist()
    effective_n = event_count / design_effect
    noncentrality = standardized_effect * math.sqrt(effective_n)
    if two_sided:
        critical = normal.inv_cdf(1.0 - alpha / 2.0)
        power = (
            1.0
            - normal.cdf(critical - noncentrality)
            + normal.cdf(-critical - noncentrality)
        )
    else:
        critical = normal.inv_cdf(1.0 - alpha)
        power = 1.0 - normal.cdf(critical - noncentrality)
    return min(max(float(power), 0.0), 1.0)
