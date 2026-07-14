from __future__ import annotations

import itertools
import math
from collections import Counter
from dataclasses import asdict, dataclass, replace
from typing import Any, Mapping, Sequence

from hydra.account_policy.router import AccountDecisionState, EntryIntent, RoutingDecision
from hydra.account_policy.schema import AccountPolicyKind
from hydra.economic_evolution.account_elite_robustness import (
    EliteRobustnessPolicy,
    RobustnessComponent,
    _parent_from_entry,
    route_elite_robustness_entry,
)
from hydra.economic_evolution.schema import deterministic_id, stable_hash


STATIC_PARENT_BASKET_CLASS_ID = "FROZEN_PARENT_STATIC_ACCOUNT_SYNTHESIS_V1"
STATIC_PARENT_BASKET_POLICY_VERSION = "hydra_static_parent_basket_policy_v1"
STATIC_PARENT_BASKET_MUTATION_QUOTAS = {"STATIC_PARENT_SYNTHESIS": 512}
STATIC_PARENT_BASKET_DEEP_QUOTAS = {"STATIC_PARENT_SYNTHESIS": 128}


@dataclass(frozen=True, slots=True)
class StaticParentBasketPolicy:
    policy_id: str
    parent_policy_id: str
    parent_policy_fingerprint: str
    source_parent_ids: tuple[str, ...]
    component_ids: tuple[str, ...]
    retained_added_sleeve_id: str
    mutation_family: str
    failure_target: str
    exact_change: tuple[tuple[str, Any], ...]
    expected_effect: str
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
    assembly_profile: str
    version: int = 1
    inherited_status: None = None

    def __post_init__(self) -> None:
        if not self.policy_id or not self.parent_policy_id:
            raise ValueError("static parent basket identity is required")
        if not 2 <= len(self.source_parent_ids) <= 4:
            raise ValueError("static synthesis must use two to four parent policies")
        if len(set(self.source_parent_ids)) != len(self.source_parent_ids):
            raise ValueError("static synthesis parent policies must be unique")
        if self.parent_policy_id not in self.source_parent_ids:
            raise ValueError("static synthesis lead must be a source parent")
        if not 10 <= len(self.component_ids) <= 13:
            raise ValueError("static synthesis must contain 10 to 13 frozen sleeves")
        if len(set(self.component_ids)) != len(self.component_ids):
            raise ValueError("static synthesis sleeves must be unique")
        if self.retained_added_sleeve_id not in self.component_ids:
            raise ValueError("lead complementary sleeve must remain present")
        if self.mutation_family != "STATIC_PARENT_SYNTHESIS":
            raise ValueError("unregistered static synthesis family")
        if not self.assembly_profile.startswith("CONSENSUS_"):
            raise ValueError("static synthesis profile escaped preregistered family")
        if self.high_risk_units not in {2, 3, 4}:
            raise ValueError("static synthesis high-risk units escaped parent policy")
        if self.middle_risk_units not in {1, 2, 3}:
            raise ValueError("static synthesis middle-risk units escaped parent policy")
        if not 0 < self.critical_buffer <= self.middle_zone_buffer <= self.high_zone_buffer <= 4_500:
            raise ValueError("static synthesis MLL zones are invalid")
        if not 0 < self.middle_zone_remaining_target <= self.high_zone_remaining_target <= 9_000:
            raise ValueError("static synthesis target zones are invalid")
        if not 1 <= self.maximum_simultaneous_positions <= 3:
            raise ValueError("static synthesis concurrency is invalid")
        if not 1 <= self.maximum_mini_equivalent <= 15:
            raise ValueError("static synthesis contract limit is invalid")
        if self.version != 1 or self.inherited_status is not None:
            raise ValueError("static synthesis cannot inherit status")

    @property
    def controller_id(self) -> str:
        return self.policy_id

    @property
    def basket_policy_id(self) -> str:
        return f"{self.policy_id}::STATIC_BASKET"

    @property
    def component_priority(self) -> tuple[str, ...]:
        return self.component_ids

    @property
    def kind(self) -> AccountPolicyKind:
        return AccountPolicyKind.STATIC_BASKET

    @property
    def structural_fingerprint(self) -> str:
        return stable_hash(self.structural_payload())

    def structural_payload(self) -> dict[str, Any]:
        return {
            "schema": STATIC_PARENT_BASKET_POLICY_VERSION,
            "lead_policy_fingerprint": self.parent_policy_fingerprint,
            "component_priority": list(self.component_ids),
            "high_risk_units": self.high_risk_units,
            "daily_loss_guard": float(self.daily_loss_guard).hex(),
            "daily_profit_lock": float(self.daily_profit_lock).hex(),
            "critical_buffer": float(self.critical_buffer).hex(),
            "high_zone_buffer": float(self.high_zone_buffer).hex(),
            "high_zone_remaining_target": float(self.high_zone_remaining_target).hex(),
            "middle_zone_buffer": float(self.middle_zone_buffer).hex(),
            "middle_zone_remaining_target": float(self.middle_zone_remaining_target).hex(),
            "middle_risk_units": self.middle_risk_units,
            "maximum_simultaneous_positions": self.maximum_simultaneous_positions,
            "maximum_mini_equivalent": self.maximum_mini_equivalent,
            "assembly_profile": self.assembly_profile,
            "version": self.version,
        }

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        row["source_parent_ids"] = list(self.source_parent_ids)
        row["component_ids"] = list(self.component_ids)
        row["exact_change"] = dict(self.exact_change)
        row["kind"] = self.kind.value
        row["structural_fingerprint"] = self.structural_fingerprint
        return row


@dataclass(frozen=True, slots=True)
class StaticParentBasketPair:
    pair_id: str
    parent_policy_id: str
    mutation_family: str
    failure_target: str
    real_policy: StaticParentBasketPolicy
    matched_control_policy: EliteRobustnessPolicy

    def __post_init__(self) -> None:
        if self.mutation_family != "STATIC_PARENT_SYNTHESIS":
            raise ValueError("static synthesis pair family drift")
        if self.real_policy.parent_policy_id != self.parent_policy_id:
            raise ValueError("static synthesis lead drift")
        if self.matched_control_policy.policy_id != self.parent_policy_id:
            raise ValueError("static synthesis control must be the exact lead")
        if self.real_policy.component_ids == self.matched_control_policy.component_ids:
            raise ValueError("static synthesis cannot equal its lead control")

    def to_dict(self) -> dict[str, Any]:
        return {
            "pair_id": self.pair_id,
            "parent_policy_id": self.parent_policy_id,
            "source_parent_ids": list(self.real_policy.source_parent_ids),
            "real_policy_id": self.real_policy.policy_id,
            "matched_control_policy_id": self.matched_control_policy.policy_id,
            "mutation_family": self.mutation_family,
            "failure_target": self.failure_target,
            "identical_parent_data": True,
            "identical_episode_starts": True,
            "identical_cost_scenarios": True,
            "underlying_signals_changed": False,
            "status_inheritance": False,
        }


@dataclass(frozen=True, slots=True)
class StaticParentBasketPopulation:
    campaign_id: str
    components: tuple[RobustnessComponent, ...]
    proposals: tuple[StaticParentBasketPolicy, ...]
    screen_rows: tuple[dict[str, Any], ...]
    pairs: tuple[StaticParentBasketPair, ...]
    duplicate_rejection_count: int
    no_effect_rejection_count: int
    manifest_hash: str

    def summary(self) -> dict[str, Any]:
        return {
            "campaign_id": self.campaign_id,
            "class_id": STATIC_PARENT_BASKET_CLASS_ID,
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
            "mutation_family_counts": {"STATIC_PARENT_SYNTHESIS": len(self.proposals)},
            "deep_mutation_family_counts": {"STATIC_PARENT_SYNTHESIS": len(self.pairs)},
            "markets": sorted({row.market for row in self.components}),
            "sessions": sorted({row.session_code for row in self.components}),
            "manifest_hash": self.manifest_hash,
            "new_candidate_ids": True,
            "status_inheritance": False,
            "outcomes_seen_during_generation": False,
            "underlying_signals_changed": False,
            "outbound_order_capability": False,
            "validated": False,
        }


def generate_static_parent_basket_population(
    elite_manifest: Mapping[str, Any],
    component_rows: Sequence[Mapping[str, Any]],
    *,
    parent_bank: Mapping[str, Any],
    campaign_id: str,
    proposal_count: int = 512,
    deep_pair_count: int = 128,
) -> StaticParentBasketPopulation:
    if elite_manifest.get("schema") != "hydra_0018_canonical_elite_manifest_v1":
        raise ValueError("static synthesis requires the canonical 0018 manifest")
    if parent_bank.get("schema") != "hydra_static_parent_bank_v1":
        raise ValueError("static synthesis requires the frozen parent bank")
    bank_payload = dict(parent_bank)
    bank_hash = bank_payload.pop("bank_hash", None)
    if bank_hash != stable_hash(bank_payload):
        raise ValueError("static parent bank hash drift")
    if proposal_count != 512 or deep_pair_count != 128:
        raise ValueError("static synthesis population size drift")
    components = tuple(
        RobustnessComponent.from_dict(value)
        for value in sorted(component_rows, key=lambda row: str(row["sleeve_id"]))
    )
    by_id = {row.sleeve_id: row for row in components}
    manifest_entries = {
        str(row["policy_id"]): row for row in elite_manifest["policies"]
    }
    selected_ids = tuple(str(value) for value in parent_bank["primary_parent_ids"])
    if len(selected_ids) != 8 or len(set(selected_ids)) != 8:
        raise ValueError("static parent bank must contain eight distinct 0018 parents")
    selected = tuple(_parent_from_entry(manifest_entries[value]) for value in selected_ids)
    expected = parent_bank["primary_parent_fingerprints"]
    if any(
        manifest_entries[row.policy_id]["immutable_policy_fingerprint"]
        != expected[row.policy_id]
        for row in selected
    ):
        raise ValueError("static parent bank fingerprint drift")
    anchor_spec = parent_bank["anchor"]
    anchor_parent = next(
        row for row in selected if row.policy_id == anchor_spec["parent_policy_id"]
    )
    anchor = replace(
        anchor_parent,
        policy_id=str(anchor_spec["policy_id"]),
        mutation_family="BUFFER_ACCELERATION",
        failure_target="INSUFFICIENT_TARGET_VELOCITY",
        exact_change=tuple(sorted(anchor_spec["exact_change"].items())),
        expected_effect="Frozen 0020 development anchor; no inherited status.",
        high_risk_units=int(anchor_spec["exact_change"]["high_risk_units"]),
        critical_buffer=float(anchor_spec["exact_change"]["critical_buffer"]),
    )
    if anchor.structural_fingerprint != anchor_spec["structural_fingerprint"]:
        raise ValueError("static parent bank anchor fingerprint drift")
    parents = (*selected, anchor)
    parent_by_id = {row.policy_id: row for row in parents}
    mutual_exclusion = {
        frozenset(str(value) for value in pair)
        for pair in parent_bank["mutual_exclusion"]
    }
    if any(value not in by_id for row in parents for value in row.component_ids):
        raise ValueError("static synthesis parent escaped the component bank")

    raw: list[StaticParentBasketPolicy] = []
    seen: set[str] = set()
    duplicates = 0
    no_effect = 0
    for size in (2, 3, 4):
        for parent_group in itertools.combinations(parents, size):
            ids = tuple(sorted(row.policy_id for row in parent_group))
            if any(pair.issubset(ids) for pair in mutual_exclusion):
                continue
            lead = max(
                parent_group,
                key=lambda row: (
                    _parent_stressed_progress(row.policy_id, parent_bank, manifest_entries),
                    row.policy_id,
                ),
            )
            for membership_size in (10, 11, 12, 13):
                base_priority = _consensus_priority(
                    parent_group,
                    lead=lead,
                    by_id=by_id,
                    membership_size=membership_size,
                )
                if tuple(base_priority) == tuple(lead.component_ids):
                    no_effect += 1
                    continue
                for rotation in range(4):
                    priority = base_priority[rotation:] + base_priority[:rotation]
                    profile = f"CONSENSUS_{membership_size}_ROTATION_{rotation}"
                    provisional = StaticParentBasketPolicy(
                        policy_id="PENDING",
                        parent_policy_id=lead.policy_id,
                        parent_policy_fingerprint=lead.structural_fingerprint,
                        source_parent_ids=ids,
                        component_ids=priority,
                        retained_added_sleeve_id=lead.retained_added_sleeve_id,
                        mutation_family="STATIC_PARENT_SYNTHESIS",
                        failure_target="INDIVIDUAL_TARGET_VELOCITY_AND_COMPLEMENTARITY",
                        exact_change=(
                            ("assembly_profile", profile),
                            ("source_parent_ids", ids),
                        ),
                        expected_effect=(
                            "Combine frozen parent sleeves by consensus while retaining "
                            "the exact lead account-risk policy."
                        ),
                        high_risk_units=lead.high_risk_units,
                        daily_loss_guard=lead.daily_loss_guard,
                        daily_profit_lock=lead.daily_profit_lock,
                        critical_buffer=lead.critical_buffer,
                        high_zone_buffer=lead.high_zone_buffer,
                        high_zone_remaining_target=lead.high_zone_remaining_target,
                        middle_zone_buffer=lead.middle_zone_buffer,
                        middle_zone_remaining_target=lead.middle_zone_remaining_target,
                        middle_risk_units=lead.middle_risk_units,
                        maximum_simultaneous_positions=lead.maximum_simultaneous_positions,
                        maximum_mini_equivalent=lead.maximum_mini_equivalent,
                        assembly_profile=profile,
                    )
                    child = replace(
                        provisional,
                        policy_id=deterministic_id(
                            "static_parent_basket",
                            [campaign_id, provisional.structural_payload()],
                        ),
                    )
                    if child.structural_fingerprint in seen:
                        duplicates += 1
                        continue
                    seen.add(child.structural_fingerprint)
                    raw.append(child)
    raw.sort(key=lambda row: row.structural_fingerprint)
    if len(raw) < proposal_count:
        raise ValueError(f"static synthesis produced only {len(raw)} unique policies")
    proposals = tuple(raw[:proposal_count])
    screen_rows = tuple(_cheap_screen(row, by_id=by_id) for row in proposals)
    survivors = [row for row in screen_rows if bool(row["cheap_screen_survivor"])]
    if len(survivors) < deep_pair_count:
        raise ValueError("static synthesis has insufficient economic survivors")
    proposal_by_fingerprint = {
        row.structural_fingerprint: row for row in proposals
    }
    selected_rows = _round_robin_lead_selection(survivors, quota=deep_pair_count)
    deep = tuple(
        proposal_by_fingerprint[str(row["structural_fingerprint"])]
        for row in selected_rows
    )
    pairs = tuple(
        StaticParentBasketPair(
            pair_id=deterministic_id(
                "static_parent_basket_pair",
                [campaign_id, row.parent_policy_id, row.structural_fingerprint],
            ),
            parent_policy_id=row.parent_policy_id,
            mutation_family="STATIC_PARENT_SYNTHESIS",
            failure_target=row.failure_target,
            real_policy=row,
            matched_control_policy=parent_by_id[row.parent_policy_id],
        )
        for row in sorted(deep, key=lambda value: value.policy_id)
    )
    manifest_hash = stable_hash(
        {
            "campaign_id": campaign_id,
            "class_id": STATIC_PARENT_BASKET_CLASS_ID,
            "parent_bank_hash": parent_bank["bank_hash"],
            "proposal_fingerprints": [row.structural_fingerprint for row in proposals],
            "screen_decisions": [
                [row["structural_fingerprint"], row["cheap_screen_survivor"]]
                for row in screen_rows
            ],
            "deep_pairs": [
                [row.pair_id, row.real_policy.structural_fingerprint] for row in pairs
            ],
            "bounded_alternatives_per_structure": 4,
            "outcomes_seen_during_generation": False,
        }
    )
    return StaticParentBasketPopulation(
        campaign_id=campaign_id,
        components=components,
        proposals=proposals,
        screen_rows=screen_rows,
        pairs=pairs,
        duplicate_rejection_count=duplicates,
        no_effect_rejection_count=no_effect,
        manifest_hash=manifest_hash,
    )


def route_static_parent_basket_entry(
    intent: EntryIntent,
    state: AccountDecisionState,
    *,
    policy: StaticParentBasketPolicy | EliteRobustnessPolicy,
) -> RoutingDecision:
    return route_elite_robustness_entry(intent, state, policy=policy)  # type: ignore[arg-type]


def _parent_stressed_progress(
    policy_id: str,
    parent_bank: Mapping[str, Any],
    manifest_entries: Mapping[str, Mapping[str, Any]],
) -> float:
    if policy_id == parent_bank["anchor"]["policy_id"]:
        return float(parent_bank["anchor"]["stressed_median_target_progress"])
    return float(manifest_entries[policy_id]["stressed_evidence"]["target_progress_median"])


def _consensus_priority(
    parents: Sequence[EliteRobustnessPolicy],
    *,
    lead: EliteRobustnessPolicy,
    by_id: Mapping[str, RobustnessComponent],
    membership_size: int,
) -> tuple[str, ...]:
    votes = Counter(value for row in parents for value in row.component_ids)
    lead_rank = {value: index for index, value in enumerate(lead.component_ids)}
    union = sorted(
        votes,
        key=lambda value: (
            -votes[value],
            -int(value in lead_rank),
            lead_rank.get(value, 10_000),
            -by_id[value].stressed_net_pnl,
            value,
        ),
    )
    selected = union[:membership_size]
    mandatory = lead.retained_added_sleeve_id
    if mandatory not in selected:
        selected[-1] = mandatory
    return tuple(
        sorted(
            set(selected),
            key=lambda value: (
                -votes[value],
                -int(value in lead_rank),
                lead_rank.get(value, 10_000),
                -by_id[value].stressed_net_pnl,
                value,
            ),
        )
    )


def _cheap_screen(
    policy: StaticParentBasketPolicy,
    *,
    by_id: Mapping[str, RobustnessComponent],
) -> dict[str, Any]:
    selected = [by_id[value] for value in policy.component_ids]
    normal = sum(row.net_pnl for row in selected)
    stressed = sum(row.stressed_net_pnl for row in selected)
    events = sum(row.event_count for row in selected)
    positive = [max(0.0, row.stressed_net_pnl) for row in selected]
    concentration = max(positive, default=0.0) / max(sum(positive), 1e-12)
    survivor = bool(
        normal > 0.0 and stressed > 0.0 and events >= 200 and concentration <= 0.4
    )
    return {
        "policy_id": policy.policy_id,
        "parent_policy_id": policy.parent_policy_id,
        "source_parent_ids": list(policy.source_parent_ids),
        "structural_fingerprint": policy.structural_fingerprint,
        "mutation_family": policy.mutation_family,
        "failure_target": policy.failure_target,
        "assembly_profile": policy.assembly_profile,
        "approximate_normal_net_usd": normal,
        "approximate_stressed_net_usd": stressed,
        "approximate_event_count": events,
        "maximum_component_share": concentration,
        "economic_screen_score": (
            stressed + 0.2 * normal + 0.25 * events - 500.0 * concentration
        ),
        "cheap_screen_survivor": survivor,
        "rolling_combine_executed": False,
        "validated": False,
    }


def _round_robin_lead_selection(
    rows: Sequence[Mapping[str, Any]], *, quota: int
) -> tuple[Mapping[str, Any], ...]:
    by_parent: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        by_parent.setdefault(str(row["parent_policy_id"]), []).append(row)
    for values in by_parent.values():
        values.sort(
            key=lambda row: (
                -float(row["economic_screen_score"]),
                str(row["structural_fingerprint"]),
            )
        )
    output: list[Mapping[str, Any]] = []
    cursor = 0
    parents = sorted(by_parent)
    while len(output) < quota:
        progressed = False
        for parent in parents:
            values = by_parent[parent]
            if cursor < len(values):
                output.append(values[cursor])
                progressed = True
                if len(output) == quota:
                    break
        if not progressed:
            break
        cursor += 1
    if len(output) != quota:
        raise ValueError("static synthesis round-robin selection is incomplete")
    return tuple(output)


__all__ = [
    "STATIC_PARENT_BASKET_CLASS_ID",
    "STATIC_PARENT_BASKET_DEEP_QUOTAS",
    "STATIC_PARENT_BASKET_MUTATION_QUOTAS",
    "STATIC_PARENT_BASKET_POLICY_VERSION",
    "StaticParentBasketPair",
    "StaticParentBasketPolicy",
    "StaticParentBasketPopulation",
    "generate_static_parent_basket_population",
    "route_static_parent_basket_entry",
]
