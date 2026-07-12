from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from typing import Any, Iterable, Mapping, Sequence

from hydra.account_policy.evolution import ComponentRuntime
from hydra.account_policy.schema import stable_hash
from hydra.strategies.turbo_batch_fingerprint import structural_fingerprint
from hydra.strategies.turbo_dsl import ComparisonOperator, StrategySpec


@dataclass(frozen=True, slots=True)
class TargetVelocityHypothesis:
    parent_candidate_id: str
    parent_lineage_id: str
    mutation_class: str
    exact_change: dict[str, Any]
    intended_failure: str
    expected_effect: str
    comparison_policy: str
    inherited_status: None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class TargetVelocityProposal:
    child: StrategySpec
    hypothesis: TargetVelocityHypothesis

    def to_dict(self) -> dict[str, Any]:
        return {
            "child": _spec_payload(self.child),
            "hypothesis": self.hypothesis.to_dict(),
        }


def generate_target_velocity_mutations(
    components: Sequence[ComponentRuntime],
    *,
    generation_index: int,
    maximum: int = 24,
    excluded_fingerprints: Iterable[str] = (),
) -> tuple[TargetVelocityProposal, ...]:
    """Generate one-dimensional structural children without blind size scaling."""

    excluded = set(str(value) for value in excluded_fingerprints)
    ranked = sorted(
        components,
        key=lambda row: (
            -row.descriptor.rolling_pass_rate,
            row.descriptor.rolling_mll_breach_rate,
            -row.descriptor.median_target_progress,
            -row.descriptor.cost_stress_net_pnl,
            row.descriptor.component_id,
        ),
    )
    output: list[TargetVelocityProposal] = []
    for ordinal, component in enumerate(ranked):
        if len(output) >= maximum:
            break
        parent = _spec_from_payload(component.specification)
        proposal = _mutate_one_dimension(
            parent,
            ordinal=ordinal,
            generation_index=generation_index,
        )
        if proposal is None:
            continue
        fingerprint = structural_fingerprint(proposal.child)
        if fingerprint in excluded:
            continue
        excluded.add(fingerprint)
        output.append(proposal)
    return tuple(output)


def evaluate_target_velocity_outcome(
    proposal: TargetVelocityProposal,
    *,
    parent_result: Mapping[str, Any],
    child_result: Mapping[str, Any],
) -> dict[str, Any]:
    parent_rolling = dict(parent_result["rolling_combine"])
    child_rolling = dict(child_result["rolling_combine"])
    parent_starts = tuple(int(value) for value in parent_rolling["episode_start_days"])
    child_starts = tuple(int(value) for value in child_rolling["episode_start_days"])
    if parent_starts != child_starts:
        raise ValueError("parent and child were not compared on identical episode starts")
    parent_path = dict(parent_result["exact_trade_path"])
    child_path = dict(child_result["exact_trade_path"])
    pass_delta = float(child_rolling["pass_rate"]) - float(
        parent_rolling["pass_rate"]
    )
    mll_delta = float(child_rolling["mll_breach_rate"]) - float(
        parent_rolling["mll_breach_rate"]
    )
    progress_delta = float(
        child_rolling["median_target_progress_when_not_passed"]
    ) - float(parent_rolling["median_target_progress_when_not_passed"])
    consistency_delta = float(child_rolling["consistency_pass_rate"]) - float(
        parent_rolling["consistency_pass_rate"]
    )
    event_delta = int(child_path["event_count"]) - int(parent_path["event_count"])
    net_delta = float(child_path["net_pnl"]) - float(parent_path["net_pnl"])
    cost_delta = float(child_path["cost_stress_1_5x_net"]) - float(
        parent_path["cost_stress_1_5x_net"]
    )
    parent_days = parent_rolling.get("median_days_to_target")
    child_days = child_rolling.get("median_days_to_target")
    days_delta = (
        float(child_days) - float(parent_days)
        if parent_days is not None and child_days is not None
        else None
    )
    no_hard_invalidation = bool(
        not child_result.get("hard_invalidation")
        and float(child_path["net_pnl"]) > 0.0
        and float(child_path["cost_stress_1_5x_net"]) > 0.0
    )
    objective_improved = bool(
        pass_delta > 1e-12
        or progress_delta >= 0.05
        or consistency_delta >= 0.10
        or (days_delta is not None and days_delta <= -5.0)
        or (
            event_delta >= max(2, int(parent_path["event_count"] * 0.10))
            and net_delta > 0.0
        )
        or (cost_delta > 0.0 and net_delta > 0.0)
    )
    acceptable_risk = bool(
        float(child_rolling["mll_breach_rate"]) <= 0.35
        and mll_delta <= 0.05 + 1e-12
        and float(child_rolling["minimum_mll_buffer"])
        >= float(parent_rolling["minimum_mll_buffer"]) - 750.0
    )
    decision = (
        "KEEP_CHILD"
        if no_hard_invalidation and objective_improved and acceptable_risk
        else "FREEZE_CHILD_DIAGNOSTIC"
    )
    return {
        "parent_candidate_id": proposal.hypothesis.parent_candidate_id,
        "child_candidate_id": proposal.child.candidate_id,
        "mutation_class": proposal.hypothesis.mutation_class,
        "exact_change": proposal.hypothesis.exact_change,
        "expected_effect": proposal.hypothesis.expected_effect,
        "comparison_episode_start_days": list(parent_starts),
        "pass_rate_delta": pass_delta,
        "mll_breach_rate_delta": mll_delta,
        "target_progress_delta": progress_delta,
        "consistency_delta": consistency_delta,
        "days_to_target_delta": days_delta,
        "event_count_delta": event_delta,
        "net_pnl_delta": net_delta,
        "cost_stress_net_delta": cost_delta,
        "no_hard_invalidation": no_hard_invalidation,
        "objective_improved": objective_improved,
        "acceptable_risk": acceptable_risk,
        "decision": decision,
        "inherited_status": None,
    }


def _mutate_one_dimension(
    parent: StrategySpec,
    *,
    ordinal: int,
    generation_index: int,
) -> TargetVelocityProposal | None:
    use_horizon = (ordinal + generation_index) % 2 == 0
    next_horizon = _next_horizon(parent.holding_events)
    if use_horizon and next_horizon != parent.holding_events:
        provisional = replace(parent, holding_events=next_horizon)
        mutation_class = "TARGET_VELOCITY_HORIZON"
        change = {"holding_events": [parent.holding_events, next_horizon]}
        expected = (
            "increase target progress per accepted opportunity and reduce cost share"
        )
    else:
        threshold = _broaden_threshold(parent)
        if threshold == parent.threshold:
            if next_horizon == parent.holding_events:
                return None
            provisional = replace(parent, holding_events=next_horizon)
            mutation_class = "TARGET_VELOCITY_HORIZON"
            change = {"holding_events": [parent.holding_events, next_horizon]}
            expected = (
                "increase target progress per accepted opportunity and reduce cost share"
            )
        else:
            provisional = replace(parent, threshold=threshold)
            mutation_class = "TARGET_VELOCITY_OPPORTUNITY_DENSITY"
            change = {"threshold": [parent.threshold, threshold]}
            expected = (
                "increase economically related opportunities without increasing size"
            )
    identity = stable_hash(
        {
            "parent": parent.candidate_id,
            "change": change,
            "generation": generation_index,
            "version": "target_velocity_v6",
        }
    )
    child = replace(
        provisional,
        candidate_id=f"strategy_v6_velocity_{identity[:24]}_v1",
        lineage_id=f"lineage_v6_velocity_{stable_hash(parent.lineage_id)[:24]}",
        version=1,
    )
    return TargetVelocityProposal(
        child=child,
        hypothesis=TargetVelocityHypothesis(
            parent_candidate_id=parent.candidate_id,
            parent_lineage_id=parent.lineage_id,
            mutation_class=mutation_class,
            exact_change=change,
            intended_failure="TARGET_VELOCITY_OR_OPPORTUNITY_COVERAGE",
            expected_effect=expected,
            comparison_policy="IDENTICAL_BLOCK_AWARE_EPISODE_STARTS",
        ),
    )


def _broaden_threshold(parent: StrategySpec) -> float:
    value = float(parent.threshold)
    if value == 0.0:
        return value
    if parent.operator in {
        ComparisonOperator.GREATER_THAN,
        ComparisonOperator.GREATER_EQUAL,
    }:
        return value * (0.975 if value > 0.0 else 1.025)
    return value * (1.025 if value > 0.0 else 0.975)


def _next_horizon(value: int) -> int:
    for horizon in (5, 15, 30, 60):
        if horizon > value:
            return horizon
    return value


def _spec_from_payload(value: Mapping[str, Any]) -> StrategySpec:
    from hydra.research.turbo_exact_replay import spec_from_dict

    return spec_from_dict(dict(value))


def _spec_payload(spec: StrategySpec) -> dict[str, Any]:
    from hydra.research.turbo_exact_replay import spec_to_dict

    return spec_to_dict(spec)


__all__ = [
    "TargetVelocityHypothesis",
    "TargetVelocityProposal",
    "evaluate_target_velocity_outcome",
    "generate_target_velocity_mutations",
]
