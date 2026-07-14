from __future__ import annotations

import statistics
from dataclasses import asdict, dataclass
from typing import Any, Mapping, Sequence

from hydra.account_policy.router import (
    AccountDecisionState,
    EntryIntent,
    RoutingDecision,
)
from hydra.account_policy.schema import AccountPolicyKind
from hydra.economic_evolution.role_aware_account import (
    RoleAwareComponent,
    _select_components,
)
from hydra.economic_evolution.schema import deterministic_id, stable_hash


ACCOUNT_TIMELINE_CLASS_ID = "SLEEVE_VIRTUAL_PERSISTENCE_ROUTER_V1"
ACCOUNT_TIMELINE_POLICY_VERSION = "hydra_sleeve_virtual_persistence_router_v1"
ACCOUNT_TIMELINE_HYPOTHESIS = (
    "Slow market and session regimes make a sleeve's completed virtual "
    "risk-normalized outcomes persist briefly; gating its next opportunity "
    "with its own recent outcome timeline should improve stressed account "
    "expectancy over an identical mapping to another sleeve's timeline."
)
ACCOUNT_TIMELINE_LIMITS: dict[str, Any] = {
    "lookback_completed_outcomes": 6,
    "minimum_completed_outcomes": 4,
    "negative_score_threshold": -0.25,
    "positive_score_threshold": 0.25,
    "positive_score_risk_units": 2,
    "daily_loss_guard": 1_000.0,
    "daily_profit_lock": 1_500.0,
    "critical_buffer": 750.0,
    "maximum_simultaneous_positions": 2,
    "maximum_mini_equivalent": 15,
}


@dataclass(frozen=True, slots=True)
class AccountTimelinePolicy:
    policy_id: str
    component_ids: tuple[str, ...]
    score_source_map: tuple[tuple[str, str], ...]
    lookback_completed_outcomes: int
    minimum_completed_outcomes: int
    negative_score_threshold: float
    positive_score_threshold: float
    positive_score_risk_units: int
    daily_loss_guard: float
    daily_profit_lock: float
    critical_buffer: float
    maximum_simultaneous_positions: int
    maximum_mini_equivalent: int
    policy_version: str = ACCOUNT_TIMELINE_POLICY_VERSION
    outbound_order_capability: bool = False

    def __post_init__(self) -> None:
        if not self.policy_id:
            raise ValueError("account-timeline policy ID is required")
        if not 6 <= len(self.component_ids) <= 8:
            raise ValueError("account-timeline policy requires six to eight sleeves")
        if len(set(self.component_ids)) != len(self.component_ids):
            raise ValueError("account-timeline component IDs must be unique")
        mapping = dict(self.score_source_map)
        if set(mapping) != set(self.component_ids):
            raise ValueError("timeline map must target every component once")
        if set(mapping.values()) != set(self.component_ids):
            raise ValueError("timeline sources must be a component permutation")
        if not 1 <= self.minimum_completed_outcomes <= self.lookback_completed_outcomes:
            raise ValueError("timeline history bounds are inconsistent")
        if self.lookback_completed_outcomes > 12:
            raise ValueError("timeline lookback exceeds the simulator history")
        if not self.negative_score_threshold < 0.0 < self.positive_score_threshold:
            raise ValueError("timeline score thresholds must straddle zero")
        if self.positive_score_risk_units not in {1, 2}:
            raise ValueError("timeline risk units must be one or two")
        if not 0.0 < self.daily_loss_guard <= 3_000.0:
            raise ValueError("daily loss guard is outside frozen bounds")
        if not 0.0 < self.daily_profit_lock <= 4_500.0:
            raise ValueError("daily profit lock is outside frozen bounds")
        if not 0.0 < self.critical_buffer <= 4_500.0:
            raise ValueError("critical buffer is outside frozen bounds")
        if not 1 <= self.maximum_simultaneous_positions <= 3:
            raise ValueError("timeline concurrency is outside frozen bounds")
        if not 1 <= self.maximum_mini_equivalent <= 15:
            raise ValueError("shared contract limit must be in [1,15]")
        if self.outbound_order_capability:
            raise ValueError("account-timeline research cannot submit orders")

    @property
    def score_sources(self) -> dict[str, str]:
        return dict(self.score_source_map)

    @property
    def component_priority(self) -> tuple[str, ...]:
        return self.component_ids

    @property
    def controller_id(self) -> str:
        return self.policy_id

    @property
    def basket_policy_id(self) -> str:
        return f"{self.policy_id}::BASKET"

    @property
    def kind(self) -> AccountPolicyKind:
        return AccountPolicyKind.ADAPTIVE_CONTROLLER

    @property
    def structural_fingerprint(self) -> str:
        return stable_hash(self.to_dict())

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        row["component_ids"] = list(self.component_ids)
        row["score_source_map"] = dict(self.score_source_map)
        row["kind"] = self.kind.value
        row["structural_fingerprint"] = stable_hash(
            {
                key: value
                for key, value in row.items()
                if key != "structural_fingerprint"
            }
        )
        return row


@dataclass(frozen=True, slots=True)
class AccountTimelinePolicyPair:
    pair_id: str
    real_policy: AccountTimelinePolicy
    matched_control_policy: AccountTimelinePolicy
    membership_hash: str

    def __post_init__(self) -> None:
        real = self.real_policy
        control = self.matched_control_policy
        if real.component_ids != control.component_ids:
            raise ValueError("timeline pair must keep ordered membership")
        if real.score_sources != {
            component_id: component_id for component_id in real.component_ids
        }:
            raise ValueError("real timeline policy must use identity history")
        if control.score_sources == real.score_sources:
            raise ValueError("timeline control must permute source identities")
        if sorted(control.score_sources.values()) != sorted(real.component_ids):
            raise ValueError("timeline control must preserve source multiset")
        if _policy_limits(real) != _policy_limits(control):
            raise ValueError("timeline pair must keep every policy limit")

    def to_dict(self) -> dict[str, Any]:
        return {
            "pair_id": self.pair_id,
            "membership_hash": self.membership_hash,
            "real_policy_id": self.real_policy.policy_id,
            "matched_control_policy_id": self.matched_control_policy.policy_id,
            "identical_sleeve_membership": True,
            "identical_component_event_paths": True,
            "identical_completed_shadow_histories": True,
            "same_history_source_multiset": True,
            "same_timeline_thresholds": True,
            "same_account_limits": True,
            "real_score_source_map": dict(self.real_policy.score_source_map),
            "matched_control_score_source_map": dict(
                self.matched_control_policy.score_source_map
            ),
        }


@dataclass(frozen=True, slots=True)
class AccountTimelinePopulation:
    campaign_id: str
    components: tuple[RoleAwareComponent, ...]
    pairs: tuple[AccountTimelinePolicyPair, ...]
    duplicate_rejection_count: int
    manifest_hash: str

    @property
    def real_policies(self) -> tuple[AccountTimelinePolicy, ...]:
        return tuple(row.real_policy for row in self.pairs)

    @property
    def matched_control_policies(self) -> tuple[AccountTimelinePolicy, ...]:
        return tuple(row.matched_control_policy for row in self.pairs)

    def summary(self) -> dict[str, Any]:
        return {
            "campaign_id": self.campaign_id,
            "class_id": ACCOUNT_TIMELINE_CLASS_ID,
            "component_count": len(self.components),
            "real_policy_count": len(self.pairs),
            "matched_control_policy_count": len(self.pairs),
            "markets": sorted({row.sleeve.market for row in self.components}),
            "sessions": sorted(
                {row.sleeve.session_code for row in self.components}
            ),
            "sleeve_counts": _counts(
                len(row.real_policy.component_ids) for row in self.pairs
            ),
            "same_ordered_membership_pair_count": len(self.pairs),
            "same_history_source_multiset_pair_count": len(self.pairs),
            "same_timeline_limits_pair_count": len(self.pairs),
            "unique_membership_count": len(
                {row.membership_hash for row in self.pairs}
            ),
            "duplicate_rejection_count": self.duplicate_rejection_count,
            "manifest_hash": self.manifest_hash,
            "past_only_completed_shadow_outcomes": True,
            "policy_uses_role_labels": False,
            "new_candidate_ids": True,
            "status_inheritance": False,
            "outcomes_seen_during_generation": False,
            "outbound_order_capability": False,
            "validated": False,
        }


def route_account_timeline_entry(
    intent: EntryIntent,
    state: AccountDecisionState,
    *,
    policy: AccountTimelinePolicy,
) -> RoutingDecision:
    if intent.component_id not in policy.score_sources:
        return _blocked(policy, "COMPONENT_NOT_IN_FROZEN_MEMBERSHIP")
    if state.daily_realized_pnl <= -policy.daily_loss_guard:
        return _blocked(policy, "DAILY_LOSS_GUARD")
    if state.daily_realized_pnl >= policy.daily_profit_lock:
        return _blocked(policy, "DAILY_PROFIT_LOCK")
    if state.mll_buffer <= policy.critical_buffer:
        return _blocked(policy, "CRITICAL_MLL_BUFFER")
    if len(state.open_exposures) >= policy.maximum_simultaneous_positions:
        return _blocked(policy, "MAXIMUM_SIMULTANEOUS_POSITIONS")
    if any(
        row.market == intent.market and row.exit_ns > intent.decision_ns
        for row in state.open_exposures
    ):
        return _blocked(policy, "SAME_MARKET_CONFLICT")

    source_id = policy.score_sources[intent.component_id]
    completed = state.shadow_outcome_map.get(source_id, ())
    recent = completed[-policy.lookback_completed_outcomes :]
    units = 1
    reason = "TIMELINE_WARMUP"
    if len(recent) >= policy.minimum_completed_outcomes:
        score = float(statistics.fmean(recent))
        if score <= policy.negative_score_threshold:
            return _blocked(policy, "NEGATIVE_COMPLETED_TIMELINE_VETO")
        if score >= policy.positive_score_threshold:
            units = policy.positive_score_risk_units
            reason = "POSITIVE_COMPLETED_TIMELINE_SCALE"
        else:
            reason = "NEUTRAL_COMPLETED_TIMELINE"
    quantity = int(intent.base_quantity * units)
    mini = float(intent.base_mini_equivalent * units)
    current = sum(row.mini_equivalent for row in state.open_exposures)
    if current + mini > policy.maximum_mini_equivalent + 1e-12:
        return _blocked(policy, "SHARED_CONTRACT_LIMIT")
    return RoutingDecision(True, quantity, mini, reason, policy.policy_id)


def generate_account_timeline_population(
    seed_archive: Mapping[str, Any],
    *,
    campaign_id: str,
    policy_pair_count: int = 512,
    maximum_components: int = 48,
    minimum_component_events: int = 20,
    minimum_markets: int = 3,
    minimum_sessions: int = 3,
) -> AccountTimelinePopulation:
    """Generate identity-history routers and within-membership controls."""

    if not campaign_id.strip():
        raise ValueError("campaign_id must be non-empty")
    if policy_pair_count < 64:
        raise ValueError("account-timeline synthesis requires at least 64 pairs")
    if maximum_components < 16:
        raise ValueError("component bank is too small for timeline routing")
    if seed_archive.get("development_only") is not True:
        raise ValueError("account-timeline generation requires a development seed")
    if seed_archive.get("proof_window_consumed") is not False:
        raise ValueError("proof-consuming seeds cannot drive timeline routing")
    governance = seed_archive.get("governance") or {}
    if governance.get("status_inheritance") is not False:
        raise ValueError("seed status inheritance must be disabled")

    components = _select_components(
        seed_archive,
        maximum_components=maximum_components,
        minimum_component_events=minimum_component_events,
    )
    if len(components) < 16:
        raise ValueError("insufficient positive-net distinct components")
    if len({row.sleeve.market for row in components}) < minimum_markets:
        raise ValueError("timeline routing lacks market coverage")
    if len({row.sleeve.session_code for row in components}) < minimum_sessions:
        raise ValueError("timeline routing lacks session coverage")

    pairs, duplicate_rejections = _generate_pairs(
        components,
        campaign_id=campaign_id,
        count=policy_pair_count,
        minimum_markets=minimum_markets,
        minimum_sessions=minimum_sessions,
    )
    manifest_payload = {
        "schema": "hydra_account_timeline_population_v1",
        "campaign_id": campaign_id,
        "class_id": ACCOUNT_TIMELINE_CLASS_ID,
        "component_behavioral_fingerprints": [
            row.sleeve.behavioral_fingerprint for row in components
        ],
        "pairs": [
            {
                "pair_id": row.pair_id,
                "membership_hash": row.membership_hash,
                "real": row.real_policy.structural_fingerprint,
                "matched_control": row.matched_control_policy.structural_fingerprint,
            }
            for row in pairs
        ],
        "timeline_limits": dict(ACCOUNT_TIMELINE_LIMITS),
        "same_ordered_membership_within_pair": True,
        "same_component_event_paths_within_pair": True,
        "same_completed_shadow_histories_within_pair": True,
        "same_history_source_multiset_within_pair": True,
        "same_timeline_thresholds_within_pair": True,
        "same_account_limits_within_pair": True,
        "control_difference": "DETERMINISTIC_HISTORY_SOURCE_IDENTITY_PERMUTATION",
        "past_only_completed_outcomes": True,
        "policy_uses_role_labels": False,
        "new_candidate_ids": True,
        "status_inheritance": False,
        "same_class_0011_rescue": False,
        "new_market_outcomes_seen_during_generation": False,
        "outbound_order_capability": False,
    }
    return AccountTimelinePopulation(
        campaign_id=campaign_id,
        components=components,
        pairs=pairs,
        duplicate_rejection_count=duplicate_rejections,
        manifest_hash=stable_hash(manifest_payload),
    )


def _generate_pairs(
    components: Sequence[RoleAwareComponent],
    *,
    campaign_id: str,
    count: int,
    minimum_markets: int,
    minimum_sessions: int,
) -> tuple[tuple[AccountTimelinePolicyPair, ...], int]:
    pairs: list[AccountTimelinePolicyPair] = []
    seen_memberships: set[str] = set()
    seen_real: set[str] = set()
    seen_control: set[str] = set()
    duplicate_rejections = 0
    for attempt in range(max(count * 100, 20_000)):
        if len(pairs) == count:
            break
        size = 6 + (attempt % 3)
        ranked = sorted(
            components,
            key=lambda row: (
                stable_hash(
                    [campaign_id, "timeline_membership", attempt, row.sleeve.sleeve_id]
                ),
                row.sleeve.sleeve_id,
            ),
        )
        members = tuple(ranked[:size])
        if not _eligible_membership(
            members,
            minimum_markets=minimum_markets,
            minimum_sessions=minimum_sessions,
        ):
            continue
        membership_hash = stable_hash(
            sorted(row.sleeve.behavioral_fingerprint for row in members)
        )
        if membership_hash in seen_memberships:
            duplicate_rejections += 1
            continue
        ordered = tuple(
            sorted(
                members,
                key=lambda row: (
                    stable_hash(
                        [
                            campaign_id,
                            membership_hash,
                            "shared_component_priority",
                            row.sleeve.sleeve_id,
                        ]
                    ),
                    row.sleeve.sleeve_id,
                ),
            )
        )
        component_ids = tuple(row.sleeve.sleeve_id for row in ordered)
        real_map = tuple((component_id, component_id) for component_id in component_ids)
        control_map = _permuted_source_map(
            component_ids,
            campaign_id=campaign_id,
            membership_hash=membership_hash,
        )
        real = _policy(
            component_ids,
            real_map,
            policy_id=deterministic_id(
                "account_timeline_identity_router",
                {
                    "campaign": campaign_id,
                    "class": ACCOUNT_TIMELINE_CLASS_ID,
                    "membership": membership_hash,
                    "source_map": real_map,
                    "limits": ACCOUNT_TIMELINE_LIMITS,
                },
            ),
        )
        control = _policy(
            component_ids,
            control_map,
            policy_id=deterministic_id(
                "account_timeline_permutation_control",
                {
                    "campaign": campaign_id,
                    "class": ACCOUNT_TIMELINE_CLASS_ID,
                    "membership": membership_hash,
                    "source_map": control_map,
                    "limits": ACCOUNT_TIMELINE_LIMITS,
                },
            ),
        )
        if (
            real.structural_fingerprint in seen_real
            or control.structural_fingerprint in seen_control
            or real.structural_fingerprint == control.structural_fingerprint
        ):
            duplicate_rejections += 1
            continue
        pair = AccountTimelinePolicyPair(
            pair_id=deterministic_id(
                "account_timeline_pair",
                {
                    "campaign": campaign_id,
                    "membership": membership_hash,
                    "real": real.structural_fingerprint,
                    "control": control.structural_fingerprint,
                },
            ),
            real_policy=real,
            matched_control_policy=control,
            membership_hash=membership_hash,
        )
        pairs.append(pair)
        seen_memberships.add(membership_hash)
        seen_real.add(real.structural_fingerprint)
        seen_control.add(control.structural_fingerprint)
    if len(pairs) != count:
        raise RuntimeError(
            f"only {len(pairs)} distinct timeline pairs for {count} requested"
        )
    return tuple(pairs), duplicate_rejections


def _eligible_membership(
    members: Sequence[RoleAwareComponent],
    *,
    minimum_markets: int,
    minimum_sessions: int,
) -> bool:
    market_counts = _counts(row.sleeve.market for row in members)
    return (
        len({row.sleeve.market for row in members}) >= minimum_markets
        and len({row.sleeve.session_code for row in members}) >= minimum_sessions
        and len({row.sleeve.trigger_feature for row in members}) >= 3
        and len({row.sleeve.behavioral_fingerprint for row in members})
        == len(members)
        and max(market_counts.values()) <= 3
    )


def _permuted_source_map(
    component_ids: Sequence[str],
    *,
    campaign_id: str,
    membership_hash: str,
) -> tuple[tuple[str, str], ...]:
    offset = 1 + (
        int(
            stable_hash(
                [campaign_id, membership_hash, "matched_history_rotation"]
            )[:16],
            16,
        )
        % (len(component_ids) - 1)
    )
    rotated = tuple(component_ids[offset:]) + tuple(component_ids[:offset])
    output = tuple(zip(component_ids, rotated, strict=True))
    if any(target == source for target, source in output):
        rotated = tuple(component_ids[1:]) + tuple(component_ids[:1])
        output = tuple(zip(component_ids, rotated, strict=True))
    if any(target == source for target, source in output):
        raise RuntimeError("unable to construct a deranged history-source control")
    return output


def _policy(
    component_ids: tuple[str, ...],
    source_map: tuple[tuple[str, str], ...],
    *,
    policy_id: str,
) -> AccountTimelinePolicy:
    return AccountTimelinePolicy(
        policy_id=policy_id,
        component_ids=component_ids,
        score_source_map=source_map,
        **ACCOUNT_TIMELINE_LIMITS,
    )


def _policy_limits(policy: AccountTimelinePolicy) -> tuple[Any, ...]:
    return (
        policy.lookback_completed_outcomes,
        policy.minimum_completed_outcomes,
        policy.negative_score_threshold,
        policy.positive_score_threshold,
        policy.positive_score_risk_units,
        policy.daily_loss_guard,
        policy.daily_profit_lock,
        policy.critical_buffer,
        policy.maximum_simultaneous_positions,
        policy.maximum_mini_equivalent,
        policy.policy_version,
    )


def _blocked(policy: AccountTimelinePolicy, reason: str) -> RoutingDecision:
    return RoutingDecision(False, 0, 0.0, reason, policy.policy_id)


def _counts(values: Sequence[Any] | Any) -> dict[Any, int]:
    output: dict[Any, int] = {}
    for value in values:
        output[value] = output.get(value, 0) + 1
    return dict(sorted(output.items(), key=lambda row: str(row[0])))


__all__ = [
    "ACCOUNT_TIMELINE_CLASS_ID",
    "ACCOUNT_TIMELINE_HYPOTHESIS",
    "ACCOUNT_TIMELINE_LIMITS",
    "ACCOUNT_TIMELINE_POLICY_VERSION",
    "AccountTimelinePolicy",
    "AccountTimelinePolicyPair",
    "AccountTimelinePopulation",
    "generate_account_timeline_population",
    "route_account_timeline_entry",
]
