from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from hydra.economic_evolution.account_complementary_sleeve import (
    ComplementarySleevePolicy,
    ComplementarySleevePopulation,
    generate_complementary_sleeve_population,
)
from hydra.economic_evolution.account_coverage_three_zone import THREE_ZONE_LIMITS
from hydra.economic_evolution.role_aware_account import RoleAwareComponent
from hydra.economic_evolution.schema import deterministic_id, stable_hash


OPPORTUNITY_REPLACEMENT_CLASS_ID = (
    "GREEN_COMPLEMENTARY_OPPORTUNITY_REPLACEMENT_V1"
)
PARENT_POPULATION_CAMPAIGN_ID = (
    "hydra_economic_evolution_complementary_sleeve_0017"
)
PARENT_POPULATION_MANIFEST_HASH = (
    "6c84ababed3f8c331cbb3e892eca211510e4cc10b3b163a7701395d083835781"
)


@dataclass(frozen=True, slots=True)
class OpportunityReplacementPolicyPair:
    pair_id: str
    parent_policy_id: str
    retained_complementary_sleeve_id: str
    removed_sleeve_id: str
    replacement_sleeve_id: str
    real_policy: ComplementarySleevePolicy
    matched_control_policy: ComplementarySleevePolicy

    def __post_init__(self) -> None:
        real = self.real_policy
        control = self.matched_control_policy
        if real.parent_policy_id != self.parent_policy_id:
            raise ValueError("opportunity-replacement real parent drift")
        if control.parent_policy_id != self.parent_policy_id:
            raise ValueError("opportunity-replacement control parent drift")
        if len(real.component_ids) != len(control.component_ids):
            raise ValueError("replacement must preserve basket breadth")
        if self.removed_sleeve_id not in control.component_ids:
            raise ValueError("removed sleeve is absent from control")
        if self.removed_sleeve_id in real.component_ids:
            raise ValueError("removed sleeve survived in real policy")
        if self.replacement_sleeve_id in control.component_ids:
            raise ValueError("replacement sleeve already existed in control")
        if self.replacement_sleeve_id not in real.component_ids:
            raise ValueError("replacement sleeve is absent from real policy")
        if set(real.component_ids) ^ set(control.component_ids) != {
            self.removed_sleeve_id,
            self.replacement_sleeve_id,
        }:
            raise ValueError("replacement pair changed more than one component")
        if real.component_ids[-1] != self.retained_complementary_sleeve_id:
            raise ValueError("real policy lost the frozen complementary sleeve")
        if control.component_ids[-1] != self.retained_complementary_sleeve_id:
            raise ValueError("control policy lost the frozen complementary sleeve")
        if _limits(real) != _limits(control):
            raise ValueError("replacement pair account limits differ")

    def to_dict(self) -> dict[str, Any]:
        return {
            "pair_id": self.pair_id,
            "parent_policy_id": self.parent_policy_id,
            "real_policy_id": self.real_policy.policy_id,
            "matched_control_policy_id": self.matched_control_policy.policy_id,
            "retained_complementary_sleeve_id": self.retained_complementary_sleeve_id,
            "removed_sleeve_id": self.removed_sleeve_id,
            "replacement_sleeve_id": self.replacement_sleeve_id,
            "component_count": len(self.real_policy.component_ids),
            "same_basket_breadth": True,
            "identical_account_limits": True,
            "one_component_replaced": True,
            "same_exit_and_sizing_semantics": True,
        }


@dataclass(frozen=True, slots=True)
class OpportunityReplacementPopulation:
    campaign_id: str
    parent_campaign_id: str
    parent_population_manifest_hash: str
    components: tuple[RoleAwareComponent, ...]
    pairs: tuple[OpportunityReplacementPolicyPair, ...]
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
            "class_id": OPPORTUNITY_REPLACEMENT_CLASS_ID,
            "parent_campaign_id": self.parent_campaign_id,
            "parent_population_manifest_hash": self.parent_population_manifest_hash,
            "component_count": len(self.components),
            "real_policy_count": len(self.pairs),
            "matched_control_policy_count": len(self.pairs),
            "unique_parent_policy_count": len(
                {row.parent_policy_id for row in self.pairs}
            ),
            "structurally_distinct_policy_count": len(
                {row.real_policy.structural_fingerprint for row in self.pairs}
            ),
            "distinct_removed_sleeve_count": len(
                {row.removed_sleeve_id for row in self.pairs}
            ),
            "distinct_replacement_sleeve_count": len(
                {row.replacement_sleeve_id for row in self.pairs}
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


def generate_opportunity_replacement_population(
    seed_archive: Mapping[str, Any],
    *,
    campaign_id: str,
    parent_campaign_id: str,
    sizing_parent_campaign_id: str,
    coverage_parent_campaign_id: str,
    policy_pair_count: int = 512,
    maximum_components: int = 48,
    minimum_component_events: int = 20,
) -> OpportunityReplacementPopulation:
    parent: ComplementarySleevePopulation = generate_complementary_sleeve_population(
        seed_archive,
        campaign_id=PARENT_POPULATION_CAMPAIGN_ID,
        parent_campaign_id=parent_campaign_id,
        sizing_parent_campaign_id=sizing_parent_campaign_id,
        coverage_parent_campaign_id=coverage_parent_campaign_id,
        policy_pair_count=policy_pair_count,
        maximum_components=maximum_components,
        minimum_component_events=minimum_component_events,
    )
    if parent.manifest_hash != PARENT_POPULATION_MANIFEST_HASH:
        raise ValueError("frozen opportunity parent population drift")
    by_id = {row.sleeve.sleeve_id: row for row in parent.components}
    pairs: list[OpportunityReplacementPolicyPair] = []
    for source in sorted(parent.pairs, key=lambda row: row.pair_id):
        parent_policy = source.real_policy
        removed = min(
            (by_id[value] for value in parent_policy.component_ids[:-1]),
            key=lambda row: (row.event_count, row.sleeve.behavioral_fingerprint),
        )
        replacement = _select_replacement(
            parent_policy.component_ids,
            parent.components,
        )
        real_membership = tuple(
            replacement.sleeve.sleeve_id
            if value == removed.sleeve.sleeve_id
            else value
            for value in parent_policy.component_ids
        )
        real = _child(
            campaign_id,
            parent_policy.policy_id,
            real_membership,
            source.added_sleeve_id,
            label="real",
        )
        control = _child(
            campaign_id,
            parent_policy.policy_id,
            parent_policy.component_ids,
            source.added_sleeve_id,
            label="control",
        )
        pairs.append(
            OpportunityReplacementPolicyPair(
                pair_id=deterministic_id(
                    "opportunity_replacement_pair",
                    [
                        campaign_id,
                        parent_policy.policy_id,
                        removed.sleeve.sleeve_id,
                        replacement.sleeve.sleeve_id,
                    ],
                ),
                parent_policy_id=parent_policy.policy_id,
                retained_complementary_sleeve_id=source.added_sleeve_id,
                removed_sleeve_id=removed.sleeve.sleeve_id,
                replacement_sleeve_id=replacement.sleeve.sleeve_id,
                real_policy=real,
                matched_control_policy=control,
            )
        )
    payload = {
        "schema": "hydra_opportunity_replacement_population_v1",
        "campaign_id": campaign_id,
        "class_id": OPPORTUNITY_REPLACEMENT_CLASS_ID,
        "parent_campaign_id": PARENT_POPULATION_CAMPAIGN_ID,
        "parent_population_manifest_hash": parent.manifest_hash,
        "pairs": [
            {
                "pair_id": row.pair_id,
                "parent_policy_id": row.parent_policy_id,
                "removed_sleeve_id": row.removed_sleeve_id,
                "replacement_sleeve_id": row.replacement_sleeve_id,
                "real": row.real_policy.structural_fingerprint,
                "control": row.matched_control_policy.structural_fingerprint,
            }
            for row in pairs
        ],
        "removal_rule": "LOWEST_EVENT_COUNT_NON_COMPLEMENTARY",
        "replacement_rule": (
            "UNUSED_MARKET_SESSION_ROLE_NOVELTY_THEN_EVENT_COUNT"
        ),
        "same_breadth_exit_sizing_and_account_limits": True,
        "new_candidate_ids": True,
        "status_inheritance": False,
        "outcomes_seen_during_generation": False,
        "outbound_order_capability": False,
    }
    return OpportunityReplacementPopulation(
        campaign_id=campaign_id,
        parent_campaign_id=PARENT_POPULATION_CAMPAIGN_ID,
        parent_population_manifest_hash=parent.manifest_hash,
        components=parent.components,
        pairs=tuple(pairs),
        manifest_hash=stable_hash(payload),
    )


def _select_replacement(
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
        raise ValueError("no unused opportunity-replacement sleeve")
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
    complementary_sleeve_id: str,
    *,
    label: str,
) -> ComplementarySleevePolicy:
    return ComplementarySleevePolicy(
        policy_id=deterministic_id(
            f"opportunity_replacement_{label}",
            [campaign_id, parent_policy_id, component_ids, label],
        ),
        parent_policy_id=parent_policy_id,
        component_ids=component_ids,
        added_sleeve_id=complementary_sleeve_id,
        high_risk_units=3,
        **dict(THREE_ZONE_LIMITS),
    )


def _limits(policy: ComplementarySleevePolicy) -> tuple[Any, ...]:
    return (
        policy.high_risk_units,
        *(getattr(policy, key) for key in THREE_ZONE_LIMITS),
    )


__all__ = [
    "OPPORTUNITY_REPLACEMENT_CLASS_ID",
    "OpportunityReplacementPolicyPair",
    "OpportunityReplacementPopulation",
    "PARENT_POPULATION_CAMPAIGN_ID",
    "PARENT_POPULATION_MANIFEST_HASH",
    "generate_opportunity_replacement_population",
]
