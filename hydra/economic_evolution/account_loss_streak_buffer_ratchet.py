from __future__ import annotations

import math
from dataclasses import asdict, dataclass, replace
from itertools import product
from typing import Any, Mapping, Sequence

from hydra.account_policy.router import AccountDecisionState, EntryIntent, RoutingDecision
from hydra.economic_evolution.account_elite_robustness import (
    EliteRobustnessPolicy,
    RobustnessComponent,
    _parent_from_entry,
)
from hydra.economic_evolution.schema import deterministic_id, stable_hash


LOSS_STREAK_BUFFER_RATCHET_CLASS_ID = "LOSS_STREAK_BUFFER_RATCHET_V1"
LOSS_STREAK_BUFFER_RATCHET_POLICY_VERSION = (
    "hydra_loss_streak_buffer_ratchet_policy_v1"
)
RATCHET_MUTATION_QUOTAS = {"LOSS_STREAK_BUFFER_RATCHET": 512}
RATCHET_DEEP_EVALUATION_QUOTAS = {"LOSS_STREAK_BUFFER_RATCHET": 512}


@dataclass(frozen=True, slots=True)
class LossStreakBufferRatchetPolicy(EliteRobustnessPolicy):
    loss_streak_derisk_after: int = 2
    derisked_units: int = 1
    middle_zone_concurrency: int = 2
    minimum_realized_progress_for_high_risk: float = 1_000.0
    daily_gain_derisk_threshold: float = 1_500.0

    def __post_init__(self) -> None:
        EliteRobustnessPolicy.__post_init__(self)
        if self.loss_streak_derisk_after not in {1, 2, 3}:
            raise ValueError("loss-streak threshold escaped frozen bounds")
        if self.derisked_units != 1:
            raise ValueError("loss-streak ratchet must return to one unit")
        if self.middle_zone_concurrency not in {1, 2}:
            raise ValueError("middle-zone concurrency escaped frozen bounds")
        if self.minimum_realized_progress_for_high_risk not in {
            0.0,
            1_000.0,
            2_000.0,
        }:
            raise ValueError("realized-progress ratchet escaped frozen bounds")
        if self.daily_gain_derisk_threshold not in {1_000.0, 2_000.0}:
            raise ValueError("daily-gain ratchet escaped frozen bounds")
        if self.high_risk_units != 4:
            raise ValueError("ratchet class freezes four favorable-state units")

    def structural_payload(self) -> dict[str, Any]:
        payload = EliteRobustnessPolicy.structural_payload(self)
        payload.update(
            {
                "schema": LOSS_STREAK_BUFFER_RATCHET_POLICY_VERSION,
                "loss_streak_derisk_after": self.loss_streak_derisk_after,
                "derisked_units": self.derisked_units,
                "middle_zone_concurrency": self.middle_zone_concurrency,
                "minimum_realized_progress_for_high_risk": float(
                    self.minimum_realized_progress_for_high_risk
                ).hex(),
                "daily_gain_derisk_threshold": float(
                    self.daily_gain_derisk_threshold
                ).hex(),
            }
        )
        return payload


@dataclass(frozen=True, slots=True)
class LossStreakBufferRatchetPair:
    pair_id: str
    parent_policy_id: str
    mutation_family: str
    failure_target: str
    real_policy: LossStreakBufferRatchetPolicy
    matched_control_policy: EliteRobustnessPolicy

    def __post_init__(self) -> None:
        if self.mutation_family != "LOSS_STREAK_BUFFER_RATCHET":
            raise ValueError("ratchet pair has the wrong mutation family")
        if self.real_policy.parent_policy_id != self.parent_policy_id:
            raise ValueError("ratchet child parent drift")
        if self.matched_control_policy.policy_id != self.parent_policy_id:
            raise ValueError("ratchet control must be the unchanged 0018 parent")
        if self.real_policy.component_ids != self.matched_control_policy.component_ids:
            raise ValueError("ratchet comparison must keep identical sleeves")
        if (
            self.real_policy.structural_fingerprint
            == self.matched_control_policy.structural_fingerprint
        ):
            raise ValueError("ratchet child did not change account behavior")

    def to_dict(self) -> dict[str, Any]:
        return {
            "pair_id": self.pair_id,
            "parent_policy_id": self.parent_policy_id,
            "real_policy_id": self.real_policy.policy_id,
            "matched_control_policy_id": self.matched_control_policy.policy_id,
            "mutation_family": self.mutation_family,
            "failure_target": self.failure_target,
            "identical_parent_data": True,
            "identical_component_membership": True,
            "identical_episode_starts": True,
            "identical_cost_scenarios": True,
            "status_inheritance": False,
        }


@dataclass(frozen=True, slots=True)
class LossStreakBufferRatchetPopulation:
    campaign_id: str
    components: tuple[RobustnessComponent, ...]
    proposals: tuple[LossStreakBufferRatchetPolicy, ...]
    screen_rows: tuple[dict[str, Any], ...]
    pairs: tuple[LossStreakBufferRatchetPair, ...]
    duplicate_rejection_count: int
    no_effect_rejection_count: int
    manifest_hash: str

    def summary(self) -> dict[str, Any]:
        return {
            "campaign_id": self.campaign_id,
            "class_id": LOSS_STREAK_BUFFER_RATCHET_CLASS_ID,
            "component_count": len(self.components),
            "proposal_count": len(self.proposals),
            "economically_screened_unique_child_count": len(self.screen_rows),
            "cheap_screen_survivor_count": sum(
                bool(row["cheap_screen_survivor"]) for row in self.screen_rows
            ),
            "real_policy_count": len(self.pairs),
            "matched_control_policy_count": len(self.pairs),
            "structurally_distinct_policy_count": len(
                {row.real_policy.structural_fingerprint for row in self.pairs}
            ),
            "unique_parent_policy_count": len(
                {row.parent_policy_id for row in self.pairs}
            ),
            "duplicate_rejection_count": self.duplicate_rejection_count,
            "no_effect_rejection_count": self.no_effect_rejection_count,
            "mutation_family_counts": {
                "LOSS_STREAK_BUFFER_RATCHET": len(self.proposals)
            },
            "deep_mutation_family_counts": {
                "LOSS_STREAK_BUFFER_RATCHET": len(self.pairs)
            },
            "markets": sorted({row.market for row in self.components}),
            "sessions": sorted({row.session_code for row in self.components}),
            "manifest_hash": self.manifest_hash,
            "new_candidate_ids": True,
            "status_inheritance": False,
            "outcomes_seen_during_generation": False,
            "outbound_order_capability": False,
            "validated": False,
        }


def generate_loss_streak_buffer_ratchet_population(
    elite_manifest: Mapping[str, Any],
    component_rows: Sequence[Mapping[str, Any]],
    *,
    campaign_id: str,
    proposal_count: int = 512,
    deep_pair_count: int = 512,
) -> LossStreakBufferRatchetPopulation:
    if elite_manifest.get("schema") != "hydra_0018_canonical_elite_manifest_v1":
        raise ValueError("ratchet generation requires the canonical 0018 manifest")
    if proposal_count != 512 or deep_pair_count != 512:
        raise ValueError("ratchet population size drift")
    components = tuple(
        RobustnessComponent.from_dict(value)
        for value in sorted(component_rows, key=lambda row: str(row["sleeve_id"]))
    )
    by_id = {row.sleeve_id: row for row in components}
    parents = tuple(
        _parent_from_entry(row)
        for row in sorted(elite_manifest["policies"], key=lambda row: str(row["policy_id"]))
    )
    if any(value not in by_id for row in parents for value in row.component_ids):
        raise ValueError("ratchet parent escaped the component bank")
    profiles = tuple(
        {
            "loss_streak_derisk_after": loss_streak,
            "middle_zone_concurrency": concurrency,
            "minimum_realized_progress_for_high_risk": progress,
            "daily_gain_derisk_threshold": daily_gain,
        }
        for loss_streak, concurrency, progress, daily_gain in product(
            (1, 2, 3),
            (1, 2),
            (0.0, 1_000.0, 2_000.0),
            (1_000.0, 2_000.0),
        )
    )
    proposals: list[LossStreakBufferRatchetPolicy] = []
    seen: set[str] = set()
    cursor = 0
    while len(proposals) < proposal_count:
        parent = parents[cursor % len(parents)]
        profile = profiles[(cursor // len(parents)) % len(profiles)]
        provisional = LossStreakBufferRatchetPolicy(
            **{
                key: value
                for key, value in asdict(parent).items()
                if key
                not in {
                    "policy_id",
                    "mutation_family",
                    "failure_target",
                    "exact_change",
                    "expected_effect",
                    "high_risk_units",
                }
            },
            policy_id="PENDING",
            mutation_family="BUFFER_ACCELERATION",
            failure_target="TARGET_VELOCITY_WITH_SEQUENCE_RISK",
            exact_change=tuple(sorted(profile.items())),
            expected_effect=(
                "Use four units only after frozen realized-progress and buffer "
                "conditions, then derisk after losing sequences or large daily gains."
            ),
            high_risk_units=4,
            **profile,
        )
        policy_id = deterministic_id(
            "loss_streak_buffer_ratchet_child",
            [campaign_id, parent.policy_id, provisional.structural_payload()],
        )
        child = replace(provisional, policy_id=policy_id)
        cursor += 1
        if child.structural_fingerprint in seen:
            continue
        seen.add(child.structural_fingerprint)
        proposals.append(child)
    screen_rows = tuple(_cheap_screen(row, by_id=by_id) for row in proposals)
    if not all(bool(row["cheap_screen_survivor"]) for row in screen_rows):
        raise ValueError("ratchet proposal failed the frozen economic screen")
    parent_by_id = {row.policy_id: row for row in parents}
    pairs = tuple(
        LossStreakBufferRatchetPair(
            pair_id=deterministic_id(
                "loss_streak_buffer_ratchet_pair",
                [campaign_id, row.parent_policy_id, row.structural_fingerprint],
            ),
            parent_policy_id=row.parent_policy_id,
            mutation_family="LOSS_STREAK_BUFFER_RATCHET",
            failure_target=row.failure_target,
            real_policy=row,
            matched_control_policy=parent_by_id[row.parent_policy_id],
        )
        for row in sorted(proposals[:deep_pair_count], key=lambda value: value.policy_id)
    )
    manifest_hash = stable_hash(
        {
            "campaign_id": campaign_id,
            "class_id": LOSS_STREAK_BUFFER_RATCHET_CLASS_ID,
            "source_elite_manifest_hash": elite_manifest["manifest_hash"],
            "proposal_fingerprints": [row.structural_fingerprint for row in proposals],
            "deep_pairs": [
                [row.pair_id, row.real_policy.structural_fingerprint] for row in pairs
            ],
            "profiles": profiles,
            "outcomes_seen_during_generation": False,
        }
    )
    return LossStreakBufferRatchetPopulation(
        campaign_id=campaign_id,
        components=components,
        proposals=tuple(proposals),
        screen_rows=screen_rows,
        pairs=pairs,
        duplicate_rejection_count=0,
        no_effect_rejection_count=0,
        manifest_hash=manifest_hash,
    )


def route_loss_streak_buffer_ratchet_entry(
    intent: EntryIntent,
    state: AccountDecisionState,
    *,
    policy: EliteRobustnessPolicy,
) -> RoutingDecision:
    if not isinstance(policy, LossStreakBufferRatchetPolicy):
        from hydra.economic_evolution.account_elite_robustness import (
            route_elite_robustness_entry,
        )

        return route_elite_robustness_entry(intent, state, policy=policy)
    if intent.component_id not in set(policy.component_ids):
        return _blocked(policy, "COMPONENT_NOT_IN_FROZEN_MEMBERSHIP")
    if state.daily_realized_pnl <= -policy.daily_loss_guard:
        return _blocked(policy, "DAILY_LOSS_GUARD")
    if state.daily_realized_pnl >= policy.daily_profit_lock:
        return _blocked(policy, "DAILY_PROFIT_LOCK")
    if state.mll_buffer <= policy.critical_buffer:
        return _blocked(policy, "CRITICAL_MLL_BUFFER")
    losing_state = state.consecutive_losing_days >= policy.loss_streak_derisk_after
    realized_progress = max(0.0, 9_000.0 - state.remaining_target)
    high_state = bool(
        not losing_state
        and state.daily_realized_pnl < policy.daily_gain_derisk_threshold
        and realized_progress >= policy.minimum_realized_progress_for_high_risk
        and state.mll_buffer >= policy.high_zone_buffer
        and state.remaining_target >= policy.high_zone_remaining_target
    )
    middle_state = bool(
        not losing_state
        and state.mll_buffer >= policy.middle_zone_buffer
        and state.remaining_target >= policy.middle_zone_remaining_target
    )
    concurrency = (
        policy.maximum_simultaneous_positions
        if high_state
        else policy.middle_zone_concurrency
        if middle_state
        else 1
    )
    if len(state.open_exposures) >= concurrency:
        return _blocked(policy, "DYNAMIC_CONCURRENCY_LIMIT")
    if any(
        row.market == intent.market and row.exit_ns > intent.decision_ns
        for row in state.open_exposures
    ):
        return _blocked(policy, "SAME_MARKET_CONFLICT")
    units = (
        policy.derisked_units
        if losing_state or state.daily_realized_pnl >= policy.daily_gain_derisk_threshold
        else policy.high_risk_units
        if high_state
        else policy.middle_risk_units
        if middle_state
        else 1
    )
    quantity = int(intent.base_quantity * units)
    mini = float(intent.base_mini_equivalent * units)
    current = sum(row.mini_equivalent for row in state.open_exposures)
    if current + mini > policy.maximum_mini_equivalent + 1e-12:
        return _blocked(policy, "SHARED_CONTRACT_LIMIT")
    return RoutingDecision(
        True,
        quantity,
        mini,
        f"LOSS_STREAK_BUFFER_RATCHET_UNITS_{units}",
        policy.policy_id,
    )


def _cheap_screen(
    policy: LossStreakBufferRatchetPolicy,
    *,
    by_id: Mapping[str, RobustnessComponent],
) -> dict[str, Any]:
    selected = [by_id[value] for value in policy.component_ids]
    normal = sum(row.net_pnl for row in selected)
    stressed = sum(row.stressed_net_pnl for row in selected)
    events = sum(row.event_count for row in selected)
    positive = [max(0.0, row.stressed_net_pnl) for row in selected]
    concentration = max(positive, default=0.0) / max(sum(positive), 1e-12)
    survivor = bool(normal > 0.0 and stressed > 0.0 and events >= 200 and concentration <= 0.4)
    return {
        "policy_id": policy.policy_id,
        "parent_policy_id": policy.parent_policy_id,
        "structural_fingerprint": policy.structural_fingerprint,
        "mutation_family": "LOSS_STREAK_BUFFER_RATCHET",
        "failure_target": policy.failure_target,
        "approximate_normal_net_usd": normal,
        "approximate_stressed_net_usd": stressed,
        "approximate_event_count": events,
        "maximum_component_share": concentration,
        "economic_screen_score": stressed + 0.2 * normal + 0.25 * events - 500.0 * concentration,
        "cheap_screen_survivor": survivor,
        "rolling_combine_executed": False,
        "validated": False,
    }


def _blocked(
    policy: LossStreakBufferRatchetPolicy, reason: str
) -> RoutingDecision:
    return RoutingDecision(False, 0, 0.0, reason, policy.policy_id)


__all__ = [
    "LOSS_STREAK_BUFFER_RATCHET_CLASS_ID",
    "LOSS_STREAK_BUFFER_RATCHET_POLICY_VERSION",
    "RATCHET_DEEP_EVALUATION_QUOTAS",
    "RATCHET_MUTATION_QUOTAS",
    "LossStreakBufferRatchetPair",
    "LossStreakBufferRatchetPolicy",
    "LossStreakBufferRatchetPopulation",
    "generate_loss_streak_buffer_ratchet_population",
    "route_loss_streak_buffer_ratchet_entry",
]
