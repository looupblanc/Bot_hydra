from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass, replace
from typing import Any, Mapping

from hydra.research.turbo_exact_replay import spec_to_dict
from hydra.strategies.turbo_batch_fingerprint import structural_fingerprint
from hydra.strategies.turbo_dsl import StrategySpec


@dataclass(frozen=True, slots=True)
class MutationHypothesis:
    parent_candidate_id: str
    parent_lineage_id: str
    diagnosed_failure: str
    mutation_class: str
    exact_change: dict[str, Any]
    expected_effect: str
    mechanism_fingerprint: str
    child_configuration_fingerprint: str
    inherited_status: None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class MutationProposal:
    child: StrategySpec
    hypothesis: MutationHypothesis

    def to_dict(self) -> dict[str, Any]:
        return {
            "child": spec_to_dict(self.child),
            "hypothesis": self.hypothesis.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class MutationOutcome:
    parent_candidate_id: str
    child_candidate_id: str
    diagnosed_failure: str
    expected_effect: str
    parent_fitness: float
    child_fitness: float
    fitness_delta: float
    parent_complexity: float
    child_complexity: float
    complexity_delta: float
    pass_rate_delta: float
    mll_breach_rate_delta: float
    decision: str
    inherited_status: None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def propose_failure_guided_mutation(
    parent: StrategySpec,
    *,
    diagnosed_failure: str,
    rolling_summary: Mapping[str, Any],
) -> MutationProposal | None:
    values = dict(rolling_summary)
    changes: dict[str, Any]
    mutation_class: str
    expected: str
    child = parent
    if diagnosed_failure == "HARD_INVALIDATION":
        return None
    if diagnosed_failure == "NET_NEGATIVE":
        return None
    if diagnosed_failure in {"MLL_BREACH", "CONSISTENCY_FAILURE"}:
        if parent.quantity > 1:
            child = replace(parent, quantity=max(1, parent.quantity - 1))
            changes = {"quantity": [parent.quantity, child.quantity]}
            mutation_class = "ACCOUNT_RISK_COMPRESSION"
            expected = "lower MLL breach and daily concentration"
        else:
            threshold = _away_from_activation(parent.threshold, 0.05)
            child = replace(parent, threshold=threshold)
            changes = {"threshold": [parent.threshold, threshold]}
            mutation_class = "EVENT_QUALITY_TIGHTENING"
            expected = "remove weak events and improve MLL/consistency"
    elif diagnosed_failure == "COST_FRAGILE":
        horizon = _next_horizon(parent.holding_events)
        if horizon == parent.holding_events:
            return None
        child = replace(parent, holding_events=horizon)
        changes = {"holding_events": [parent.holding_events, horizon]}
        mutation_class = "TURNOVER_REDUCTION"
        expected = "lower cost share through a longer executable horizon"
    elif diagnosed_failure == "INSUFFICIENT_TRADES":
        threshold = _toward_activation(parent.threshold, 0.05)
        child = replace(parent, threshold=threshold)
        changes = {"threshold": [parent.threshold, threshold]}
        mutation_class = "OPPORTUNITY_BROADENING"
        expected = "increase opportunity frequency without changing mechanism"
    elif diagnosed_failure in {"TARGET_NOT_REACHED", "NEAR_PASS_OR_ELITE"}:
        progress = max(
            float(values.get("median_target_progress_when_not_passed") or 0.0),
            0.05,
        )
        minimum_buffer = float(values.get("minimum_mll_buffer") or 0.0)
        consumed = max(4500.0 - minimum_buffer, 250.0)
        risk_cap = max(
            parent.quantity,
            math.floor(parent.quantity * 0.80 * 4500.0 / consumed),
        )
        target_quantity = math.ceil(parent.quantity / min(progress, 1.0))
        quantity = min(15, max(parent.quantity + 1, target_quantity), risk_cap)
        if quantity <= parent.quantity:
            threshold = _toward_activation(parent.threshold, 0.025)
            child = replace(parent, threshold=threshold)
            changes = {"threshold": [parent.threshold, threshold]}
            mutation_class = "TARGET_VELOCITY_FREQUENCY"
            expected = "increase target velocity through slightly broader activation"
        else:
            child = replace(parent, quantity=quantity)
            changes = {"quantity": [parent.quantity, quantity]}
            mutation_class = "MLL_BOUNDED_TARGET_SCALING"
            expected = "raise target velocity within a buffer-derived sizing cap"
    else:
        return None

    child = _identify_child(child, parent_ids=(parent.candidate_id,))
    mechanism = structural_fingerprint(parent)
    configuration = configuration_fingerprint(child)
    return MutationProposal(
        child=child,
        hypothesis=MutationHypothesis(
            parent_candidate_id=parent.candidate_id,
            parent_lineage_id=parent.lineage_id,
            diagnosed_failure=diagnosed_failure,
            mutation_class=mutation_class,
            exact_change=changes,
            expected_effect=expected,
            mechanism_fingerprint=mechanism,
            child_configuration_fingerprint=configuration,
        ),
    )


def constrained_crossover(
    left: StrategySpec, right: StrategySpec
) -> MutationProposal | None:
    if (
        left.market != right.market
        or left.family != right.family
        or left.side != right.side
        or left.role != right.role
        or right.context_feature is None
        or (
            left.context_feature,
            left.context_operator,
            left.context_threshold,
        )
        == (
            right.context_feature,
            right.context_operator,
            right.context_threshold,
        )
    ):
        return None
    provisional = replace(
        left,
        context_feature=right.context_feature,
        context_operator=right.context_operator,
        context_threshold=right.context_threshold,
        timeframe=right.timeframe,
    )
    child = _identify_child(
        provisional, parent_ids=(left.candidate_id, right.candidate_id)
    )
    return MutationProposal(
        child=child,
        hypothesis=MutationHypothesis(
            parent_candidate_id=left.candidate_id,
            parent_lineage_id=left.lineage_id,
            diagnosed_failure="COMPATIBLE_COMPONENT_CROSSOVER",
            mutation_class="CONSTRAINED_CONTEXT_CROSSOVER",
            exact_change={
                "context_parent": right.candidate_id,
                "context_feature": right.context_feature,
                "timeframe": right.timeframe,
            },
            expected_effect="retain the primary entry while importing an independently useful context",
            mechanism_fingerprint=structural_fingerprint(child),
            child_configuration_fingerprint=configuration_fingerprint(child),
        ),
    )


def mutation_outcome(
    proposal: MutationProposal,
    *,
    parent_fitness: float,
    child_fitness: float,
    parent_pass_rate: float,
    child_pass_rate: float,
    parent_mll_breach_rate: float,
    child_mll_breach_rate: float,
) -> MutationOutcome:
    parent_complexity = _complexity_from_change({})
    child_complexity = _complexity_from_change(proposal.hypothesis.exact_change)
    delta = child_fitness - parent_fitness
    return MutationOutcome(
        parent_candidate_id=proposal.hypothesis.parent_candidate_id,
        child_candidate_id=proposal.child.candidate_id,
        diagnosed_failure=proposal.hypothesis.diagnosed_failure,
        expected_effect=proposal.hypothesis.expected_effect,
        parent_fitness=parent_fitness,
        child_fitness=child_fitness,
        fitness_delta=delta,
        parent_complexity=parent_complexity,
        child_complexity=child_complexity,
        complexity_delta=child_complexity - parent_complexity,
        pass_rate_delta=child_pass_rate - parent_pass_rate,
        mll_breach_rate_delta=child_mll_breach_rate - parent_mll_breach_rate,
        decision=(
            "KEEP_CHILD"
            if delta > 0.02
            and child_mll_breach_rate <= parent_mll_breach_rate + 1e-12
            else "FREEZE_CHILD_DIAGNOSTIC"
        ),
    )


def configuration_fingerprint(spec: StrategySpec) -> str:
    payload = {
        "mechanism_fingerprint": structural_fingerprint(spec),
        "quantity": spec.quantity,
        "point_value": float(spec.point_value).hex(),
        "round_turn_cost": float(spec.round_turn_cost).hex(),
        "role": spec.role.name,
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _identify_child(
    provisional: StrategySpec, *, parent_ids: tuple[str, ...]
) -> StrategySpec:
    mechanism = structural_fingerprint(provisional)
    configuration = configuration_fingerprint(provisional)
    lineage = hashlib.sha256(
        json.dumps(
            {"parents": sorted(parent_ids), "mechanism": mechanism},
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    return replace(
        provisional,
        candidate_id=f"strategy_v5_{mechanism[:16]}_{configuration[:8]}_v1",
        lineage_id=f"lineage_v5_{lineage[:24]}",
        version=1,
    )


def _toward_activation(value: float, fraction: float) -> float:
    return float(value * (1.0 - fraction))


def _away_from_activation(value: float, fraction: float) -> float:
    return float(value * (1.0 + fraction))


def _next_horizon(value: int) -> int:
    horizons = (5, 15, 30, 60)
    for horizon in horizons:
        if horizon > value:
            return horizon
    return value


def _complexity_from_change(change: Mapping[str, Any]) -> float:
    return float(1 + len(change))


__all__ = [
    "MutationHypothesis",
    "MutationOutcome",
    "MutationProposal",
    "configuration_fingerprint",
    "constrained_crossover",
    "mutation_outcome",
    "propose_failure_guided_mutation",
]
