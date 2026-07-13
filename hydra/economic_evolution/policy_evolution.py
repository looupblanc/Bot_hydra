from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from typing import Any, Mapping, Sequence

from hydra.economic_evolution.schema import (
    AccountPolicyGenome,
    FailureDimension,
    SleeveSpec,
    deterministic_id,
    stable_hash,
)


@dataclass(frozen=True, slots=True)
class PolicyChildManifest:
    parent_policy_id: str
    child_policy: AccountPolicyGenome
    dominant_failure: FailureDimension
    mutation_kind: str
    exact_change: Mapping[str, Any]
    expected_effect: str
    identical_episode_starts_required: bool = True
    inherited_status: None = None

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["child_policy"] = self.child_policy.to_dict()
        value["dominant_failure"] = self.dominant_failure.value
        value["exact_change"] = dict(self.exact_change)
        return value


@dataclass(frozen=True, slots=True)
class PolicyEvolutionPopulation:
    campaign_id: str
    requested_count: int
    children: tuple[PolicyChildManifest, ...]
    duplicate_rejection_count: int
    no_effect_rejection_count: int
    manifest_hash: str

    def summary(self) -> dict[str, Any]:
        by_kind: dict[str, int] = {}
        for row in self.children:
            by_kind[row.mutation_kind] = by_kind.get(row.mutation_kind, 0) + 1
        return {
            "campaign_id": self.campaign_id,
            "requested_count": self.requested_count,
            "child_count": len(self.children),
            "duplicate_rejection_count": self.duplicate_rejection_count,
            "no_effect_rejection_count": self.no_effect_rejection_count,
            "mutation_kinds": dict(sorted(by_kind.items())),
            "manifest_hash": self.manifest_hash,
        }


def generate_failure_directed_policy_population(
    parents: Sequence[AccountPolicyGenome],
    failures: Mapping[str, FailureDimension],
    sleeves: Sequence[SleeveSpec],
    *,
    campaign_id: str,
    count: int,
) -> PolicyEvolutionPopulation:
    """Generate bounded account-only repairs without changing sleeve signals."""

    if not campaign_id.strip():
        raise ValueError("campaign_id must be non-empty")
    if count < 1:
        raise ValueError("count must be positive")
    ordered_parents = tuple(sorted(parents, key=lambda row: row.policy_id))
    if not ordered_parents:
        raise ValueError("at least one parent policy is required")
    by_id = {row.sleeve_id: row for row in sleeves}
    if any(key not in by_id for row in ordered_parents for key in row.sleeve_ids):
        raise ValueError("parent policy references an absent sleeve")
    if any(row.policy_id not in failures for row in ordered_parents):
        raise ValueError("every parent requires an explicit failure dimension")

    children: list[PolicyChildManifest] = []
    seen = {row.structural_fingerprint for row in ordered_parents}
    duplicates = 0
    no_effect = 0
    maximum_attempts = max(count * 80, 1_000)
    for attempt in range(maximum_attempts):
        if len(children) >= count:
            break
        parent = ordered_parents[
            _choice(campaign_id, attempt, "parent") % len(ordered_parents)
        ]
        failure = failures[parent.policy_id]
        proposal = _mutate(
            parent,
            failure,
            sleeves=sleeves,
            by_id=by_id,
            campaign_id=campaign_id,
            attempt=attempt,
        )
        if proposal is None:
            no_effect += 1
            continue
        provisional, kind, change, expected = proposal
        if provisional.structural_fingerprint in seen:
            duplicates += 1
            continue
        seen.add(provisional.structural_fingerprint)
        payload = {
            "campaign": campaign_id,
            "parent": parent.policy_id,
            "attempt": attempt,
            "mutation_kind": kind,
            "policy": provisional.structural_payload(),
        }
        child = replace(
            provisional,
            policy_id=deterministic_id("account_policy_child", payload),
            parent_policy_ids=(parent.policy_id,),
            mutation_target=failure,
            source_campaign=campaign_id,
            version=parent.version + 1,
        )
        children.append(
            PolicyChildManifest(
                parent_policy_id=parent.policy_id,
                child_policy=child,
                dominant_failure=failure,
                mutation_kind=kind,
                exact_change=change,
                expected_effect=expected,
            )
        )

    manifest_hash = stable_hash(
        [row.child_policy.structural_fingerprint for row in children]
    )
    return PolicyEvolutionPopulation(
        campaign_id=campaign_id,
        requested_count=count,
        children=tuple(children),
        duplicate_rejection_count=duplicates,
        no_effect_rejection_count=no_effect,
        manifest_hash=manifest_hash,
    )


def _mutate(
    parent: AccountPolicyGenome,
    failure: FailureDimension,
    *,
    sleeves: Sequence[SleeveSpec],
    by_id: Mapping[str, SleeveSpec],
    campaign_id: str,
    attempt: int,
) -> tuple[AccountPolicyGenome, str, dict[str, Any], str] | None:
    if failure in {
        FailureDimension.NULL_INDISTINGUISHABLE,
        FailureDimension.EXECUTION_INFEASIBILITY,
    }:
        return None
    mode = _choice(campaign_id, attempt, "repair") % 4
    if failure in {
        FailureDimension.INSUFFICIENT_OPPORTUNITY_COUNT,
        FailureDimension.INSUFFICIENT_TARGET_VELOCITY,
        FailureDimension.LONG_RECOVERY_TIME,
    }:
        if mode == 0 and len(parent.sleeve_ids) < 4:
            candidate = _complementary(
                parent, sleeves, by_id=by_id, campaign_id=campaign_id, attempt=attempt
            )
            if candidate is not None:
                child = replace(
                    parent,
                    sleeve_ids=(*parent.sleeve_ids, candidate.sleeve_id),
                    allocation_units=(*parent.allocation_units, 1),
                )
                return (
                    child,
                    "ADD_COMPLEMENTARY_SLEEVE",
                    {"added_sleeve_id": candidate.sleeve_id},
                    "Increase behaviorally distinct opportunity density.",
                )
        if mode == 1:
            index = _choice(campaign_id, attempt, "allocation") % len(
                parent.allocation_units
            )
            units = list(parent.allocation_units)
            if units[index] < 4:
                units[index] += 1
                child = replace(parent, allocation_units=tuple(units))
                return (
                    child,
                    "BOUNDED_PRODUCTIVE_ALLOCATION",
                    {"allocation_index": index, "allocation_units": units[index]},
                    "Increase target velocity inside an unchanged signal sleeve.",
                )
        if mode == 2 and parent.maximum_simultaneous_positions < min(
            3, len(parent.sleeve_ids)
        ):
            child = replace(
                parent,
                maximum_simultaneous_positions=parent.maximum_simultaneous_positions
                + 1,
            )
            return (
                child,
                "BOUNDED_CONCURRENCY",
                {
                    "maximum_simultaneous_positions": [
                        parent.maximum_simultaneous_positions,
                        child.maximum_simultaneous_positions,
                    ]
                },
                "Increase target velocity while retaining the frozen account cap.",
            )
        candidate = _complementary(
            parent, sleeves, by_id=by_id, campaign_id=campaign_id, attempt=attempt
        )
        if candidate is not None:
            replace_at = _choice(campaign_id, attempt, "replace") % len(
                parent.sleeve_ids
            )
            ids = list(parent.sleeve_ids)
            removed = ids[replace_at]
            ids[replace_at] = candidate.sleeve_id
            child = replace(parent, sleeve_ids=tuple(ids))
            return (
                child,
                "REPLACE_WITH_COMPLEMENTARY_SLEEVE",
                {
                    "removed_sleeve_id": removed,
                    "replacement_sleeve_id": candidate.sleeve_id,
                },
                "Replace inactive coverage with a distinct session, market or role.",
            )
        return None

    if failure in {
        FailureDimension.MLL_BREACH,
        FailureDimension.SEQUENCE_FRAGILITY,
        FailureDimension.HIDDEN_DIRECTIONAL_BETA,
    }:
        if mode % 2 == 0 and parent.maximum_simultaneous_positions > 1:
            child = replace(
                parent,
                maximum_simultaneous_positions=parent.maximum_simultaneous_positions
                - 1,
            )
            return (
                child,
                "REDUCE_CONCURRENCY",
                {"maximum_simultaneous_positions": child.maximum_simultaneous_positions},
                "Reduce correlated simultaneous loss exposure.",
            )
        budget = _bounded_previous(
            parent.daily_risk_budget, (500.0, 750.0, 1_250.0, 1_750.0, 2_250.0)
        )
        if budget == parent.daily_risk_budget:
            return None
        return (
            replace(parent, daily_risk_budget=budget),
            "LOWER_DAILY_RISK_BUDGET",
            {"daily_risk_budget": [parent.daily_risk_budget, budget]},
            "Preserve MLL buffer after adverse account paths.",
        )

    if failure in {
        FailureDimension.CONSISTENCY_RULE_FAILURE,
        FailureDimension.CONCENTRATION,
        FailureDimension.PAYOUT_FRAGILITY,
    }:
        lock = _bounded_previous(
            parent.daily_profit_lock, (1_000.0, 1_500.0, 2_250.0, 3_000.0, 4_500.0)
        )
        if lock == parent.daily_profit_lock:
            return None
        return (
            replace(parent, daily_profit_lock=lock),
            "LOWER_DAILY_PROFIT_LOCK",
            {"daily_profit_lock": [parent.daily_profit_lock, lock]},
            "Reduce extreme-day concentration and preserve payout consistency.",
        )

    if failure == FailureDimension.WEAK_COST_MARGIN and len(parent.sleeve_ids) > 1:
        index = _choice(campaign_id, attempt, "remove") % len(parent.sleeve_ids)
        ids = parent.sleeve_ids[:index] + parent.sleeve_ids[index + 1 :]
        units = parent.allocation_units[:index] + parent.allocation_units[index + 1 :]
        child = replace(
            parent,
            sleeve_ids=ids,
            allocation_units=units,
            maximum_simultaneous_positions=min(
                parent.maximum_simultaneous_positions, len(ids)
            ),
        )
        return (
            child,
            "REMOVE_COSTLY_SLEEVE",
            {"removed_sleeve_id": parent.sleeve_ids[index]},
            "Reduce churn while keeping remaining component signals unchanged.",
        )
    throttle = max(2, parent.loss_streak_throttle_after - 1)
    if throttle == parent.loss_streak_throttle_after:
        return None
    return (
        replace(parent, loss_streak_throttle_after=throttle),
        "EARLIER_LOSS_THROTTLE",
        {
            "loss_streak_throttle_after": [
                parent.loss_streak_throttle_after,
                throttle,
            ]
        },
        "Reduce exposure earlier when temporal transfer is unstable.",
    )


def _complementary(
    parent: AccountPolicyGenome,
    sleeves: Sequence[SleeveSpec],
    *,
    by_id: Mapping[str, SleeveSpec],
    campaign_id: str,
    attempt: int,
) -> SleeveSpec | None:
    current = [by_id[key] for key in parent.sleeve_ids]
    excluded = set(parent.sleeve_ids)
    candidates = [row for row in sleeves if row.sleeve_id not in excluded]
    candidates.sort(
        key=lambda row: (
            sum(row.market == value.market for value in current),
            sum(row.session_code == value.session_code for value in current),
            sum(row.role == value.role for value in current),
            _choice(campaign_id, attempt, row.sleeve_id),
            row.sleeve_id,
        )
    )
    return candidates[0] if candidates else None


def _bounded_previous(value: float, grid: Sequence[float]) -> float:
    lower = [candidate for candidate in grid if candidate < value - 1e-12]
    return max(lower) if lower else value


def _choice(campaign_id: str, attempt: int, name: str) -> int:
    return int(stable_hash([campaign_id, attempt, name])[:16], 16)


__all__ = [
    "PolicyChildManifest",
    "PolicyEvolutionPopulation",
    "generate_failure_directed_policy_population",
]
