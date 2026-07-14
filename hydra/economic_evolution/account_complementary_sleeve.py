from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping

from hydra.account_policy.router import (
    AccountDecisionState,
    EntryIntent,
    RoutingDecision,
)
from hydra.account_policy.schema import AccountPolicyKind
from hydra.economic_evolution.account_coverage_three_zone import (
    CoverageThreeZonePopulation,
    THREE_ZONE_LIMITS,
    generate_coverage_three_zone_population,
    route_coverage_three_zone_entry,
)
from hydra.economic_evolution.role_aware_account import RoleAwareComponent
from hydra.economic_evolution.schema import deterministic_id, stable_hash


COMPLEMENTARY_SLEEVE_CLASS_ID = "GREEN_THREE_ZONE_COMPLEMENTARY_SLEEVE_V1"


@dataclass(frozen=True, slots=True)
class ComplementarySleevePolicy:
    policy_id: str
    parent_policy_id: str
    component_ids: tuple[str, ...]
    added_sleeve_id: str | None
    high_risk_units: int
    daily_loss_guard: float
    daily_profit_lock: float
    critical_buffer: float
    high_zone_buffer: float
    high_zone_remaining_target: float
    middle_zone_buffer: float
    middle_zone_remaining_target: float
    middle_risk_units: int
    maximum_simultaneous_positions: int
    maximum_mini_equivalent: int
    version: int = 1
    inherited_status: None = None

    def __post_init__(self) -> None:
        if not self.policy_id or not self.parent_policy_id:
            raise ValueError("complementary-sleeve identity is required")
        if not 10 <= len(self.component_ids) <= 13:
            raise ValueError("complementary policy requires ten to thirteen sleeves")
        if len(set(self.component_ids)) != len(self.component_ids):
            raise ValueError("complementary sleeves must be unique")
        if self.added_sleeve_id is not None:
            if self.added_sleeve_id not in self.component_ids:
                raise ValueError("added sleeve is absent from real membership")
            if len(self.component_ids) < 11:
                raise ValueError("real complementary policy has no added sleeve")
        if self.high_risk_units != 3:
            raise ValueError("complementary policy freezes three high-zone units")
        for key, expected in THREE_ZONE_LIMITS.items():
            if getattr(self, key) != expected:
                raise ValueError(f"complementary policy {key} drift")
        if self.version != 1 or self.inherited_status is not None:
            raise ValueError("complementary children cannot inherit status")

    @property
    def controller_id(self) -> str:
        return self.policy_id

    @property
    def basket_policy_id(self) -> str:
        return f"{self.policy_id}::BASKET"

    @property
    def component_priority(self) -> tuple[str, ...]:
        return self.component_ids

    @property
    def kind(self) -> AccountPolicyKind:
        return AccountPolicyKind.ADAPTIVE_CONTROLLER

    @property
    def structural_fingerprint(self) -> str:
        return stable_hash(self.structural_payload())

    def structural_payload(self) -> dict[str, Any]:
        return {
            "schema": "hydra_complementary_sleeve_policy_v1",
            "parent_policy_id": self.parent_policy_id,
            "component_ids": list(self.component_ids),
            "added_sleeve_id": self.added_sleeve_id,
            "high_risk_units": self.high_risk_units,
            **dict(THREE_ZONE_LIMITS),
            "version": self.version,
        }

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        row["component_ids"] = list(self.component_ids)
        row["kind"] = self.kind.value
        row["structural_fingerprint"] = self.structural_fingerprint
        return row


@dataclass(frozen=True, slots=True)
class ComplementarySleevePolicyPair:
    pair_id: str
    parent_policy_id: str
    added_sleeve_id: str
    real_policy: ComplementarySleevePolicy
    matched_control_policy: ComplementarySleevePolicy

    def __post_init__(self) -> None:
        real, control = self.real_policy, self.matched_control_policy
        if real.parent_policy_id != self.parent_policy_id:
            raise ValueError("real complementary parent drift")
        if control.parent_policy_id != self.parent_policy_id:
            raise ValueError("control complementary parent drift")
        if real.added_sleeve_id != self.added_sleeve_id:
            raise ValueError("real added-sleeve identity drift")
        if control.added_sleeve_id is not None:
            raise ValueError("leave-one-out control cannot mark an added sleeve")
        if tuple(real.component_ids[:-1]) != control.component_ids:
            raise ValueError("real policy must append exactly one sleeve")
        if real.component_ids[-1] != self.added_sleeve_id:
            raise ValueError("added sleeve must have lowest frozen priority")
        if _limits(real) != _limits(control):
            raise ValueError("complementary pair account limits differ")

    def to_dict(self) -> dict[str, Any]:
        return {
            "pair_id": self.pair_id,
            "parent_policy_id": self.parent_policy_id,
            "real_policy_id": self.real_policy.policy_id,
            "matched_control_policy_id": self.matched_control_policy.policy_id,
            "added_sleeve_id": self.added_sleeve_id,
            "real_component_count": len(self.real_policy.component_ids),
            "control_component_count": len(
                self.matched_control_policy.component_ids
            ),
            "identical_parent_signal_paths": True,
            "control_is_leave_added_sleeve_out_ablation": True,
            "identical_account_limits": True,
            "identical_three_zone_sizing": True,
            "different_membership_only": True,
        }


@dataclass(frozen=True, slots=True)
class ComplementarySleevePopulation:
    campaign_id: str
    parent_campaign_id: str
    parent_population_manifest_hash: str
    components: tuple[RoleAwareComponent, ...]
    pairs: tuple[ComplementarySleevePolicyPair, ...]
    manifest_hash: str

    @property
    def real_policies(self) -> tuple[ComplementarySleevePolicy, ...]:
        return tuple(row.real_policy for row in self.pairs)

    @property
    def matched_control_policies(self) -> tuple[ComplementarySleevePolicy, ...]:
        return tuple(row.matched_control_policy for row in self.pairs)

    def summary(self) -> dict[str, Any]:
        return {
            "campaign_id": self.campaign_id,
            "class_id": COMPLEMENTARY_SLEEVE_CLASS_ID,
            "parent_campaign_id": self.parent_campaign_id,
            "parent_population_manifest_hash": self.parent_population_manifest_hash,
            "component_count": len(self.components),
            "real_policy_count": len(self.pairs),
            "matched_control_policy_count": len(self.pairs),
            "unique_parent_edge_count": len(
                {row.parent_policy_id for row in self.pairs}
            ),
            "structurally_distinct_policy_count": len(
                {row.real_policy.structural_fingerprint for row in self.pairs}
            ),
            "distinct_added_sleeve_count": len(
                {row.added_sleeve_id for row in self.pairs}
            ),
            "duplicate_control_definition_count": 0,
            "markets": sorted({row.sleeve.market for row in self.components}),
            "sessions": sorted(
                {row.sleeve.session_code for row in self.components}
            ),
            "manifest_hash": self.manifest_hash,
            "new_candidate_ids": True,
            "status_inheritance": False,
            "outcomes_seen_during_generation": False,
            "outbound_order_capability": False,
            "validated": False,
        }


def generate_complementary_sleeve_population(
    seed_archive: Mapping[str, Any],
    *,
    campaign_id: str,
    parent_campaign_id: str,
    sizing_parent_campaign_id: str,
    coverage_parent_campaign_id: str,
    policy_pair_count: int = 512,
    maximum_components: int = 48,
    minimum_component_events: int = 20,
) -> ComplementarySleevePopulation:
    parent: CoverageThreeZonePopulation = generate_coverage_three_zone_population(
        seed_archive,
        campaign_id=parent_campaign_id,
        parent_campaign_id=sizing_parent_campaign_id,
        coverage_parent_campaign_id=coverage_parent_campaign_id,
        policy_pair_count=policy_pair_count,
        maximum_components=maximum_components,
        minimum_component_events=minimum_component_events,
    )
    by_id = {row.sleeve.sleeve_id: row for row in parent.components}
    pairs: list[ComplementarySleevePolicyPair] = []
    for source in sorted(parent.pairs, key=lambda row: row.pair_id):
        parent_policy = source.real_policy
        added = _select_complement(parent_policy.component_ids, parent.components)
        real_membership = (*parent_policy.component_ids, added.sleeve.sleeve_id)
        real = _child(
            campaign_id,
            parent_policy.policy_id,
            real_membership,
            added_sleeve_id=added.sleeve.sleeve_id,
            label="real",
        )
        control = _child(
            campaign_id,
            parent_policy.policy_id,
            parent_policy.component_ids,
            added_sleeve_id=None,
            label="control",
        )
        if any(value not in by_id for value in real.component_ids):
            raise ValueError("complementary membership escaped frozen archive")
        pairs.append(
            ComplementarySleevePolicyPair(
                pair_id=deterministic_id(
                    "complementary_sleeve_pair",
                    [campaign_id, parent_policy.policy_id, added.sleeve.sleeve_id],
                ),
                parent_policy_id=parent_policy.policy_id,
                added_sleeve_id=added.sleeve.sleeve_id,
                real_policy=real,
                matched_control_policy=control,
            )
        )
    payload = {
        "schema": "hydra_complementary_sleeve_population_v1",
        "campaign_id": campaign_id,
        "class_id": COMPLEMENTARY_SLEEVE_CLASS_ID,
        "parent_campaign_id": parent_campaign_id,
        "parent_population_manifest_hash": parent.manifest_hash,
        "pairs": [
            {
                "pair_id": row.pair_id,
                "parent_policy_id": row.parent_policy_id,
                "added_sleeve_id": row.added_sleeve_id,
                "real": row.real_policy.structural_fingerprint,
                "control": row.matched_control_policy.structural_fingerprint,
            }
            for row in pairs
        ],
        "selection_order": [
            "new_market_desc",
            "new_session_desc",
            "new_role_desc",
            "event_count_desc",
            "behavioral_fingerprint_asc",
        ],
        "limits": dict(THREE_ZONE_LIMITS),
        "high_risk_units": 3,
        "same_parent_signals": True,
        "control_is_leave_added_sleeve_out": True,
        "new_candidate_ids": True,
        "status_inheritance": False,
        "outcomes_seen_during_generation": False,
        "outbound_order_capability": False,
    }
    return ComplementarySleevePopulation(
        campaign_id=campaign_id,
        parent_campaign_id=parent_campaign_id,
        parent_population_manifest_hash=parent.manifest_hash,
        components=parent.components,
        pairs=tuple(pairs),
        manifest_hash=stable_hash(payload),
    )


def route_complementary_sleeve_entry(
    intent: EntryIntent,
    state: AccountDecisionState,
    *,
    policy: ComplementarySleevePolicy,
) -> RoutingDecision:
    return route_coverage_three_zone_entry(  # type: ignore[arg-type]
        intent,
        state,
        policy=policy,
    )


def _select_complement(
    component_ids: tuple[str, ...],
    components: tuple[RoleAwareComponent, ...],
) -> RoleAwareComponent:
    selected = {row.sleeve.sleeve_id: row for row in components}
    present = [selected[value] for value in component_ids]
    markets = {row.sleeve.market for row in present}
    sessions = {row.sleeve.session_code for row in present}
    roles = {row.sleeve.role for row in present}
    candidates = [
        row for row in components if row.sleeve.sleeve_id not in set(component_ids)
    ]
    if not candidates:
        raise ValueError("no unused complementary sleeve")
    return min(
        candidates,
        key=lambda row: (
            -int(row.sleeve.market not in markets),
            -int(row.sleeve.session_code not in sessions),
            -int(row.sleeve.role not in roles),
            -int(row.event_count),
            row.sleeve.behavioral_fingerprint,
        ),
    )


def _child(
    campaign_id: str,
    parent_policy_id: str,
    component_ids: tuple[str, ...],
    *,
    added_sleeve_id: str | None,
    label: str,
) -> ComplementarySleevePolicy:
    return ComplementarySleevePolicy(
        policy_id=deterministic_id(
            f"complementary_sleeve_{label}",
            [campaign_id, parent_policy_id, added_sleeve_id, label],
        ),
        parent_policy_id=parent_policy_id,
        component_ids=component_ids,
        added_sleeve_id=added_sleeve_id,
        high_risk_units=3,
        **dict(THREE_ZONE_LIMITS),
    )


def _limits(policy: ComplementarySleevePolicy) -> tuple[Any, ...]:
    return (
        policy.high_risk_units,
        *(getattr(policy, key) for key in THREE_ZONE_LIMITS),
    )


__all__ = [
    "COMPLEMENTARY_SLEEVE_CLASS_ID",
    "ComplementarySleevePolicy",
    "ComplementarySleevePolicyPair",
    "ComplementarySleevePopulation",
    "generate_complementary_sleeve_population",
    "route_complementary_sleeve_entry",
]
