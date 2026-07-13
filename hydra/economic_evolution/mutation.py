from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from typing import Any, Sequence

from hydra.economic_evolution.schema import (
    AccountPolicyGenome,
    FailureDimension,
    FailureVector,
    SleeveSpec,
    deterministic_id,
)


@dataclass(frozen=True, slots=True)
class DirectedMutation:
    parent_policy_id: str
    child_policy: AccountPolicyGenome | None
    dominant_failure: FailureDimension
    exact_change: dict[str, Any]
    expected_effect: str
    decision: str
    parent_evidence_hash: str
    identical_episode_starts_required: bool = True
    inherited_status: None = None

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["dominant_failure"] = self.dominant_failure.value
        value["child_policy"] = (
            self.child_policy.to_dict() if self.child_policy else None
        )
        return value


def propose_directed_mutation(
    parent: AccountPolicyGenome,
    failure: FailureVector,
    *,
    available_sleeves: Sequence[SleeveSpec],
) -> DirectedMutation:
    if failure.policy_id != parent.policy_id:
        raise ValueError("failure vector does not belong to parent policy")
    if not failure.evaluated_on_identical_parent_child_starts:
        raise ValueError("mutation comparison must freeze identical episode starts")
    dominant = failure.dominant
    if dominant in {
        FailureDimension.NULL_INDISTINGUISHABLE,
        FailureDimension.EXECUTION_INFEASIBILITY,
    }:
        return DirectedMutation(
            parent_policy_id=parent.policy_id,
            child_policy=None,
            dominant_failure=dominant,
            exact_change={},
            expected_effect="No parameter repair is permitted for a mechanism or execution failure.",
            decision="KILL_OR_CHANGE_REPRESENTATION",
            parent_evidence_hash=failure.evidence_hash,
        )

    provisional = parent
    change: dict[str, Any]
    expected: str
    if dominant in {
        FailureDimension.INSUFFICIENT_OPPORTUNITY_COUNT,
        FailureDimension.INSUFFICIENT_TARGET_VELOCITY,
        FailureDimension.LONG_RECOVERY_TIME,
    }:
        replacement = _complementary_sleeve(parent, available_sleeves)
        if replacement is not None and len(parent.sleeve_ids) < 4:
            provisional = replace(
                parent,
                sleeve_ids=(*parent.sleeve_ids, replacement.sleeve_id),
                allocation_units=(*parent.allocation_units, 1),
            )
            change = {"added_sleeve_id": replacement.sleeve_id}
            expected = "Increase independent opportunity density and target velocity."
        else:
            index = min(range(len(parent.allocation_units)), key=parent.allocation_units.__getitem__)
            units = list(parent.allocation_units)
            units[index] = min(4, units[index] + 1)
            provisional = replace(parent, allocation_units=tuple(units))
            change = {"allocation_index": index, "allocation_units": units[index]}
            expected = "Increase bounded allocation to an existing productive sleeve."
    elif dominant in {
        FailureDimension.MLL_BREACH,
        FailureDimension.SEQUENCE_FRAGILITY,
        FailureDimension.HIDDEN_DIRECTIONAL_BETA,
    }:
        if parent.maximum_simultaneous_positions > 1:
            provisional = replace(
                parent,
                maximum_simultaneous_positions=parent.maximum_simultaneous_positions - 1,
            )
            change = {
                "maximum_simultaneous_positions": [
                    parent.maximum_simultaneous_positions,
                    provisional.maximum_simultaneous_positions,
                ]
            }
            expected = "Reduce correlated simultaneous loss exposure."
        else:
            new_budget = _previous_value(
                parent.daily_risk_budget, (500.0, 750.0, 1_250.0, 1_750.0, 2_250.0)
            )
            provisional = replace(parent, daily_risk_budget=new_budget)
            change = {"daily_risk_budget": [parent.daily_risk_budget, new_budget]}
            expected = "Preserve MLL buffer by lowering the frozen daily risk budget."
    elif dominant == FailureDimension.WEAK_COST_MARGIN:
        if len(parent.sleeve_ids) > 2:
            provisional = replace(
                parent,
                sleeve_ids=parent.sleeve_ids[:-1],
                allocation_units=parent.allocation_units[:-1],
                maximum_simultaneous_positions=min(
                    parent.maximum_simultaneous_positions,
                    len(parent.sleeve_ids) - 1,
                ),
            )
            change = {"removed_sleeve_id": parent.sleeve_ids[-1]}
            expected = "Remove a marginal sleeve to reduce churn and redundant costs."
        else:
            provisional = replace(parent, maximum_simultaneous_positions=1)
            change = {"maximum_simultaneous_positions": [parent.maximum_simultaneous_positions, 1]}
            expected = "Reduce overlapping turnover while preserving unchanged signals."
    elif dominant in {
        FailureDimension.CONSISTENCY_RULE_FAILURE,
        FailureDimension.CONCENTRATION,
        FailureDimension.PAYOUT_FRAGILITY,
    }:
        new_lock = _previous_value(
            parent.daily_profit_lock, (1_000.0, 1_500.0, 2_250.0, 3_000.0, 4_500.0)
        )
        provisional = replace(parent, daily_profit_lock=new_lock)
        change = {"daily_profit_lock": [parent.daily_profit_lock, new_lock]}
        expected = "Reduce extreme winning-day concentration and protect payout paths."
    elif dominant == FailureDimension.REDUNDANT_PORTFOLIO_ROLE:
        replacement = _complementary_sleeve(parent, available_sleeves)
        if replacement is None:
            return DirectedMutation(
                parent_policy_id=parent.policy_id,
                child_policy=None,
                dominant_failure=dominant,
                exact_change={},
                expected_effect="No behaviorally distinct replacement is available.",
                decision="FREEZE_LINEAGE",
                parent_evidence_hash=failure.evidence_hash,
            )
        sleeves = (*parent.sleeve_ids[:-1], replacement.sleeve_id)
        provisional = replace(parent, sleeve_ids=sleeves)
        change = {
            "replaced_sleeve_id": parent.sleeve_ids[-1],
            "replacement_sleeve_id": replacement.sleeve_id,
        }
        expected = "Replace a redundant sleeve with a distinct account role."
    else:
        provisional = replace(
            parent,
            loss_streak_throttle_after=max(2, parent.loss_streak_throttle_after - 1),
        )
        change = {
            "loss_streak_throttle_after": [
                parent.loss_streak_throttle_after,
                provisional.loss_streak_throttle_after,
            ]
        }
        expected = "Throttle exposure earlier in unstable temporal states."

    if provisional.structural_fingerprint == parent.structural_fingerprint:
        return DirectedMutation(
            parent_policy_id=parent.policy_id,
            child_policy=None,
            dominant_failure=dominant,
            exact_change=change,
            expected_effect=expected,
            decision="NO_EFFECTIVE_MUTATION_AVAILABLE",
            parent_evidence_hash=failure.evidence_hash,
        )
    child_payload = provisional.structural_payload()
    child = replace(
        provisional,
        policy_id=deterministic_id("account_policy_child", child_payload),
        parent_policy_ids=(parent.policy_id,),
        mutation_target=dominant,
        version=1,
    )
    return DirectedMutation(
        parent_policy_id=parent.policy_id,
        child_policy=child,
        dominant_failure=dominant,
        exact_change=change,
        expected_effect=expected,
        decision="REPLAY_ON_IDENTICAL_STARTS",
        parent_evidence_hash=failure.evidence_hash,
    )


def _complementary_sleeve(
    parent: AccountPolicyGenome, available: Sequence[SleeveSpec]
) -> SleeveSpec | None:
    excluded = set(parent.sleeve_ids)
    candidates = [row for row in available if row.sleeve_id not in excluded]
    if not candidates:
        return None
    candidates.sort(
        key=lambda row: (
            sum(row.market in sleeve_id for sleeve_id in parent.sleeve_ids),
            row.behavioral_fingerprint,
            row.sleeve_id,
        )
    )
    return candidates[0]


def _previous_value(value: float, grid: Sequence[float]) -> float:
    smaller = [candidate for candidate in grid if candidate < value - 1e-12]
    return max(smaller) if smaller else min(grid)


__all__ = ["DirectedMutation", "propose_directed_mutation"]
