from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any, Mapping, Sequence

from hydra.economic_evolution.account_elite_robustness import (
    EliteRobustnessPolicy,
    RobustnessComponent,
    _parent_from_entry,
)
from hydra.economic_evolution.schema import deterministic_id, stable_hash


CENSORED_HORIZON_CLASS_ID = "FROZEN_0018_CENSORED_HORIZON_DIAGNOSTIC_V1"
CENSORED_HORIZON_POLICY_VERSION = "hydra_0018_censored_horizon_policy_v1"
CENSORED_HORIZON_MUTATION_QUOTAS = {"CENSORED_HORIZON": 49}
CENSORED_HORIZON_DEEP_EVALUATION_QUOTAS = {"CENSORED_HORIZON": 49}
CONTROL_HORIZON_SESSIONS = 60
DIAGNOSTIC_HORIZON_SESSIONS = 90


@dataclass(frozen=True, slots=True)
class CensoredHorizonPair:
    pair_id: str
    parent_policy_id: str
    mutation_family: str
    failure_target: str
    real_policy: EliteRobustnessPolicy
    matched_control_policy: EliteRobustnessPolicy

    def __post_init__(self) -> None:
        if self.mutation_family != "CENSORED_HORIZON":
            raise ValueError("censored-horizon pair has the wrong class")
        if self.real_policy.parent_policy_id != self.parent_policy_id:
            raise ValueError("censored-horizon diagnostic parent drift")
        if self.matched_control_policy.policy_id != self.parent_policy_id:
            raise ValueError("censored-horizon control identity drift")
        if self.real_policy.component_ids != self.matched_control_policy.component_ids:
            raise ValueError("censored-horizon diagnostic changed membership")
        if (
            self.real_policy.structural_fingerprint
            != self.matched_control_policy.structural_fingerprint
        ):
            raise ValueError("censored-horizon diagnostic changed policy behavior")

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
            "identical_policy_behavior": True,
            "identical_episode_starts": True,
            "identical_cost_scenarios": True,
            "control_horizon_sessions": CONTROL_HORIZON_SESSIONS,
            "diagnostic_horizon_sessions": DIAGNOSTIC_HORIZON_SESSIONS,
            "status_inheritance": False,
        }


@dataclass(frozen=True, slots=True)
class CensoredHorizonPopulation:
    campaign_id: str
    components: tuple[RobustnessComponent, ...]
    proposals: tuple[EliteRobustnessPolicy, ...]
    screen_rows: tuple[dict[str, Any], ...]
    pairs: tuple[CensoredHorizonPair, ...]
    duplicate_rejection_count: int
    no_effect_rejection_count: int
    manifest_hash: str

    def summary(self) -> dict[str, Any]:
        return {
            "campaign_id": self.campaign_id,
            "class_id": CENSORED_HORIZON_CLASS_ID,
            "component_count": len(self.components),
            "proposal_count": len(self.proposals),
            "economically_screened_unique_child_count": len(self.screen_rows),
            "cheap_screen_survivor_count": len(self.screen_rows),
            "real_policy_count": len(self.pairs),
            "matched_control_policy_count": len(self.pairs),
            "structurally_distinct_policy_count": len(
                {row.real_policy.structural_fingerprint for row in self.pairs}
            ),
            "unique_parent_policy_count": len(self.pairs),
            "duplicate_rejection_count": self.duplicate_rejection_count,
            "no_effect_rejection_count": self.no_effect_rejection_count,
            "mutation_family_counts": {"CENSORED_HORIZON": len(self.proposals)},
            "deep_mutation_family_counts": {"CENSORED_HORIZON": len(self.pairs)},
            "markets": sorted({row.market for row in self.components}),
            "sessions": sorted({row.session_code for row in self.components}),
            "manifest_hash": self.manifest_hash,
            "control_horizon_sessions": CONTROL_HORIZON_SESSIONS,
            "diagnostic_horizon_sessions": DIAGNOSTIC_HORIZON_SESSIONS,
            "policy_behavior_changed": False,
            "new_candidate_ids": True,
            "status_inheritance": False,
            "outcomes_seen_during_generation": False,
            "outbound_order_capability": False,
            "validated": False,
        }


def generate_censored_horizon_population(
    elite_manifest: Mapping[str, Any],
    component_rows: Sequence[Mapping[str, Any]],
    *,
    campaign_id: str,
    proposal_count: int = 49,
    deep_pair_count: int = 49,
) -> CensoredHorizonPopulation:
    if elite_manifest.get("schema") != "hydra_0018_canonical_elite_manifest_v1":
        raise ValueError("censored-horizon diagnostic requires canonical 0018 elites")
    if proposal_count != 49 or deep_pair_count != 49:
        raise ValueError("censored-horizon cohort size drift")
    components = tuple(
        RobustnessComponent.from_dict(value)
        for value in sorted(component_rows, key=lambda row: str(row["sleeve_id"]))
    )
    by_id = {row.sleeve_id for row in components}
    parents = tuple(
        _parent_from_entry(row)
        for row in sorted(elite_manifest["policies"], key=lambda row: str(row["policy_id"]))
    )
    if len(parents) != 49:
        raise ValueError("canonical 0018 diagnostic cohort drift")
    if any(value not in by_id for row in parents for value in row.component_ids):
        raise ValueError("censored-horizon parent escaped component bank")
    proposals = tuple(
        replace(
            parent,
            policy_id=deterministic_id(
                "censored_horizon_diagnostic",
                [campaign_id, parent.policy_id, DIAGNOSTIC_HORIZON_SESSIONS],
            ),
            exact_change=(("diagnostic_horizon_sessions", 90),),
            expected_effect=(
                "Measure uncensored target attainment without changing the policy."
            ),
        )
        for parent in parents
    )
    screen_rows = tuple(
        {
            "policy_id": child.policy_id,
            "parent_policy_id": parent.policy_id,
            "structural_fingerprint": child.structural_fingerprint,
            "mutation_family": "CENSORED_HORIZON",
            "failure_target": "RESEARCH_HORIZON_CENSORING_VS_TARGET_VELOCITY",
            "cheap_screen_survivor": True,
            "policy_behavior_changed": False,
            "rolling_combine_executed": False,
            "validated": False,
        }
        for child, parent in zip(proposals, parents, strict=True)
    )
    pairs = tuple(
        CensoredHorizonPair(
            pair_id=deterministic_id(
                "censored_horizon_pair", [campaign_id, parent.policy_id]
            ),
            parent_policy_id=parent.policy_id,
            mutation_family="CENSORED_HORIZON",
            failure_target="RESEARCH_HORIZON_CENSORING_VS_TARGET_VELOCITY",
            real_policy=child,
            matched_control_policy=parent,
        )
        for child, parent in zip(proposals, parents, strict=True)
    )
    manifest_hash = stable_hash(
        {
            "campaign_id": campaign_id,
            "class_id": CENSORED_HORIZON_CLASS_ID,
            "source_elite_manifest_hash": elite_manifest["manifest_hash"],
            "pairs": [
                [
                    row.parent_policy_id,
                    row.real_policy.policy_id,
                    row.real_policy.structural_fingerprint,
                ]
                for row in pairs
            ],
            "control_horizon_sessions": CONTROL_HORIZON_SESSIONS,
            "diagnostic_horizon_sessions": DIAGNOSTIC_HORIZON_SESSIONS,
            "policy_behavior_changed": False,
            "outcomes_seen_during_generation": False,
        }
    )
    return CensoredHorizonPopulation(
        campaign_id=campaign_id,
        components=components,
        proposals=proposals,
        screen_rows=screen_rows,
        pairs=pairs,
        duplicate_rejection_count=0,
        no_effect_rejection_count=0,
        manifest_hash=manifest_hash,
    )


__all__ = [
    "CENSORED_HORIZON_CLASS_ID",
    "CENSORED_HORIZON_DEEP_EVALUATION_QUOTAS",
    "CENSORED_HORIZON_MUTATION_QUOTAS",
    "CENSORED_HORIZON_POLICY_VERSION",
    "CONTROL_HORIZON_SESSIONS",
    "DIAGNOSTIC_HORIZON_SESSIONS",
    "CensoredHorizonPair",
    "CensoredHorizonPopulation",
    "generate_censored_horizon_population",
]
