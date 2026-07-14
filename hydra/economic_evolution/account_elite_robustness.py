from __future__ import annotations

import math
from dataclasses import asdict, dataclass, replace
from typing import Any, Mapping, Sequence

from hydra.account_policy.router import (
    AccountDecisionState,
    EntryIntent,
    RoutingDecision,
)
from hydra.account_policy.schema import AccountPolicyKind
from hydra.economic_evolution.schema import deterministic_id, stable_hash


ELITE_ROBUSTNESS_CLASS_ID = "GREEN_0018_ELITE_ROBUSTNESS_EVOLUTION_V1"
ELITE_ROBUSTNESS_POLICY_VERSION = "hydra_0018_elite_robustness_policy_v1"
MUTATION_QUOTAS: dict[str, int] = {
    "OPPORTUNITY_REPLACEMENT": 5_120,
    "MARKET_SESSION_DIVERSIFIER": 2_048,
    "PRIORITY_REALLOCATION": 2_048,
    "COST_PRUNE": 384,
    "BUFFER_ACCELERATION": 384,
    "PROFIT_SMOOTHER": 256,
}
DEEP_EVALUATION_QUOTAS: dict[str, int] = {
    "OPPORTUNITY_REPLACEMENT": 80,
    "MARKET_SESSION_DIVERSIFIER": 40,
    "PRIORITY_REALLOCATION": 30,
    "BUFFER_ACCELERATION": 25,
    "PROFIT_SMOOTHER": 15,
    "COST_PRUNE": 10,
}


@dataclass(frozen=True, slots=True)
class RobustnessComponent:
    sleeve_id: str
    behavioral_fingerprint: str
    market: str
    session_code: int
    role: str
    event_count: int
    net_pnl: float
    stressed_net_pnl: float

    def __post_init__(self) -> None:
        if not self.sleeve_id or not self.behavioral_fingerprint:
            raise ValueError("robustness component identity is required")
        if self.event_count < 1:
            raise ValueError("robustness component must have events")
        if not all(math.isfinite(value) for value in (self.net_pnl, self.stressed_net_pnl)):
            raise ValueError("robustness component economics must be finite")

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "RobustnessComponent":
        return cls(
            sleeve_id=str(value["sleeve_id"]),
            behavioral_fingerprint=str(value["behavioral_fingerprint"]),
            market=str(value["market"]),
            session_code=int(value["session_code"]),
            role=str(value["role"]),
            event_count=int(value["event_count"]),
            net_pnl=float(value["net_pnl"]),
            stressed_net_pnl=float(value["stress_1_5x_net_pnl"]),
        )


@dataclass(frozen=True, slots=True)
class EliteRobustnessPolicy:
    policy_id: str
    parent_policy_id: str
    parent_policy_fingerprint: str
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
    version: int = 1
    inherited_status: None = None

    def __post_init__(self) -> None:
        if not self.policy_id or not self.parent_policy_id or not self.parent_policy_fingerprint:
            raise ValueError("elite robustness identity is required")
        if not 10 <= len(self.component_ids) <= 13:
            raise ValueError("elite robustness basket must contain 10 to 13 sleeves")
        if len(set(self.component_ids)) != len(self.component_ids):
            raise ValueError("elite robustness sleeves must be unique")
        if self.retained_added_sleeve_id not in self.component_ids:
            raise ValueError("0018 complementary sleeve must remain in the child")
        if self.mutation_family not in {*MUTATION_QUOTAS, "FROZEN_0018_PARENT"}:
            raise ValueError("unregistered elite robustness mutation family")
        if not self.failure_target or not self.expected_effect:
            raise ValueError("mutation diagnosis and expected effect are required")
        if self.high_risk_units not in {2, 3, 4} or self.middle_risk_units not in {1, 2, 3}:
            raise ValueError("elite robustness risk units escaped bounded values")
        if not 0 < self.daily_loss_guard <= 1_000:
            raise ValueError("elite robustness daily loss guard is invalid")
        if not 0 < self.daily_profit_lock <= 3_000:
            raise ValueError("elite robustness daily profit lock is invalid")
        if not 0 < self.critical_buffer <= self.middle_zone_buffer <= self.high_zone_buffer <= 4_500:
            raise ValueError("elite robustness MLL zones are invalid")
        if not 0 < self.middle_zone_remaining_target <= self.high_zone_remaining_target <= 9_000:
            raise ValueError("elite robustness target zones are invalid")
        if not 1 <= self.maximum_simultaneous_positions <= 3:
            raise ValueError("elite robustness concurrency is invalid")
        if not 1 <= self.maximum_mini_equivalent <= 15:
            raise ValueError("elite robustness contract limit is invalid")
        if self.version != 1 or self.inherited_status is not None:
            raise ValueError("elite robustness children cannot inherit status")

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
            "schema": ELITE_ROBUSTNESS_POLICY_VERSION,
            "parent_policy_fingerprint": self.parent_policy_fingerprint,
            "component_ids": list(self.component_ids),
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
            "version": self.version,
        }

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        row["component_ids"] = list(self.component_ids)
        row["exact_change"] = dict(self.exact_change)
        row["kind"] = self.kind.value
        row["structural_fingerprint"] = self.structural_fingerprint
        return row


@dataclass(frozen=True, slots=True)
class EliteRobustnessPolicyPair:
    pair_id: str
    parent_policy_id: str
    mutation_family: str
    failure_target: str
    real_policy: EliteRobustnessPolicy
    matched_control_policy: EliteRobustnessPolicy

    def __post_init__(self) -> None:
        if self.real_policy.parent_policy_id != self.parent_policy_id:
            raise ValueError("elite robustness real parent drift")
        if self.matched_control_policy.policy_id != self.parent_policy_id:
            raise ValueError("elite robustness control must retain 0018 identity")
        if self.matched_control_policy.parent_policy_id != self.parent_policy_id:
            raise ValueError("elite robustness control lineage drift")
        if self.real_policy.structural_fingerprint == self.matched_control_policy.structural_fingerprint:
            raise ValueError("elite robustness child must change account behavior")
        if self.real_policy.retained_added_sleeve_id != self.matched_control_policy.retained_added_sleeve_id:
            raise ValueError("elite robustness pair changed the 0018 complementary sleeve")

    def to_dict(self) -> dict[str, Any]:
        return {
            "pair_id": self.pair_id,
            "parent_policy_id": self.parent_policy_id,
            "real_policy_id": self.real_policy.policy_id,
            "matched_control_policy_id": self.matched_control_policy.policy_id,
            "mutation_family": self.mutation_family,
            "failure_target": self.failure_target,
            "identical_parent_data": True,
            "identical_episode_starts": True,
            "identical_cost_scenarios": True,
            "status_inheritance": False,
        }


@dataclass(frozen=True, slots=True)
class EliteRobustnessPopulation:
    campaign_id: str
    components: tuple[RobustnessComponent, ...]
    proposals: tuple[EliteRobustnessPolicy, ...]
    screen_rows: tuple[dict[str, Any], ...]
    pairs: tuple[EliteRobustnessPolicyPair, ...]
    duplicate_rejection_count: int
    no_effect_rejection_count: int
    manifest_hash: str

    def summary(self) -> dict[str, Any]:
        return {
            "campaign_id": self.campaign_id,
            "class_id": ELITE_ROBUSTNESS_CLASS_ID,
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
            "mutation_family_counts": _counts(
                row.mutation_family for row in self.proposals
            ),
            "deep_mutation_family_counts": _counts(
                row.mutation_family for row in self.pairs
            ),
            "markets": sorted({row.market for row in self.components}),
            "sessions": sorted({row.session_code for row in self.components}),
            "manifest_hash": self.manifest_hash,
            "new_candidate_ids": True,
            "status_inheritance": False,
            "outcomes_seen_during_generation": False,
            "outbound_order_capability": False,
            "validated": False,
        }


def generate_elite_robustness_population(
    elite_manifest: Mapping[str, Any],
    component_rows: Sequence[Mapping[str, Any]],
    *,
    campaign_id: str,
    proposal_count: int = 10_240,
    deep_pair_count: int = 200,
) -> EliteRobustnessPopulation:
    if elite_manifest.get("schema") != "hydra_0018_canonical_elite_manifest_v1":
        raise ValueError("elite robustness requires the canonical 0018 manifest")
    if proposal_count != sum(MUTATION_QUOTAS.values()):
        raise ValueError("elite robustness proposal count must match frozen allocation")
    if deep_pair_count != sum(DEEP_EVALUATION_QUOTAS.values()):
        raise ValueError("elite robustness deep count must match frozen allocation")
    components = tuple(
        RobustnessComponent.from_dict(value)
        for value in sorted(component_rows, key=lambda row: str(row["sleeve_id"]))
    )
    by_id = {row.sleeve_id: row for row in components}
    parent_entries = tuple(
        sorted(elite_manifest["policies"], key=lambda row: str(row["policy_id"]))
    )
    parents = tuple(_parent_from_entry(row) for row in parent_entries)
    evidence = {str(row["policy_id"]): row for row in parent_entries}
    if any(value not in by_id for row in parents for value in row.component_ids):
        raise ValueError("elite robustness parent escaped the component bank")

    proposals: list[EliteRobustnessPolicy] = []
    seen = {row.structural_fingerprint for row in parents}
    duplicates = 0
    no_effect = 0
    for family, quota in MUTATION_QUOTAS.items():
        produced = 0
        attempt = 0
        maximum_attempts = max(quota * 200, 20_000)
        while produced < quota and attempt < maximum_attempts:
            parent = parents[attempt % len(parents)]
            candidate = _mutate_parent(
                parent,
                family=family,
                attempt=attempt // len(parents),
                components=components,
                by_id=by_id,
                parent_evidence=evidence[parent.policy_id],
                campaign_id=campaign_id,
            )
            attempt += 1
            if candidate is None:
                no_effect += 1
                continue
            if candidate.structural_fingerprint in seen:
                duplicates += 1
                continue
            seen.add(candidate.structural_fingerprint)
            proposals.append(candidate)
            produced += 1
        if produced != quota:
            raise ValueError(
                f"elite robustness mutation family {family} produced {produced}/{quota}"
            )
    if len(proposals) != proposal_count:
        raise ValueError("elite robustness proposal generation is incomplete")

    screen_rows = tuple(
        _cheap_screen(row, by_id=by_id, parent_evidence=evidence[row.parent_policy_id])
        for row in proposals
    )
    survivors = [row for row in screen_rows if bool(row["cheap_screen_survivor"])]
    if len(survivors) < 2_048:
        raise ValueError("elite robustness produced fewer than 2,048 economic survivors")
    by_fingerprint = {row.structural_fingerprint: row for row in proposals}
    selected: list[EliteRobustnessPolicy] = []
    for family, quota in DEEP_EVALUATION_QUOTAS.items():
        family_rows = [
            row for row in survivors if str(row["mutation_family"]) == family
        ]
        family_rows.sort(
            key=lambda row: (
                -float(row["economic_screen_score"]),
                str(row["parent_policy_id"]),
                str(row["structural_fingerprint"]),
            )
        )
        chosen = _round_robin_parent_selection(family_rows, quota=quota)
        selected.extend(by_fingerprint[str(row["structural_fingerprint"])] for row in chosen)
    if len(selected) != deep_pair_count:
        raise ValueError("elite robustness deep selection is incomplete")
    parent_by_id = {row.policy_id: row for row in parents}
    pairs = tuple(
        EliteRobustnessPolicyPair(
            pair_id=deterministic_id(
                "elite_robustness_pair",
                [campaign_id, row.parent_policy_id, row.structural_fingerprint],
            ),
            parent_policy_id=row.parent_policy_id,
            mutation_family=row.mutation_family,
            failure_target=row.failure_target,
            real_policy=row,
            matched_control_policy=parent_by_id[row.parent_policy_id],
        )
        for row in sorted(selected, key=lambda value: value.policy_id)
    )
    manifest_hash = stable_hash(
        {
            "campaign_id": campaign_id,
            "source_elite_manifest_hash": elite_manifest["manifest_hash"],
            "proposal_fingerprints": [row.structural_fingerprint for row in proposals],
            "screen_decisions": [
                [row["structural_fingerprint"], row["cheap_screen_survivor"]]
                for row in screen_rows
            ],
            "deep_pairs": [
                [row.pair_id, row.real_policy.structural_fingerprint]
                for row in pairs
            ],
            "mutation_quotas": MUTATION_QUOTAS,
            "deep_quotas": DEEP_EVALUATION_QUOTAS,
            "outcomes_seen_during_generation": False,
        }
    )
    return EliteRobustnessPopulation(
        campaign_id=campaign_id,
        components=components,
        proposals=tuple(proposals),
        screen_rows=screen_rows,
        pairs=pairs,
        duplicate_rejection_count=duplicates,
        no_effect_rejection_count=no_effect,
        manifest_hash=manifest_hash,
    )


def route_elite_robustness_entry(
    intent: EntryIntent,
    state: AccountDecisionState,
    *,
    policy: EliteRobustnessPolicy,
) -> RoutingDecision:
    if intent.component_id not in set(policy.component_ids):
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
    if (
        state.mll_buffer >= policy.high_zone_buffer
        and state.remaining_target >= policy.high_zone_remaining_target
    ):
        zone = "HIGH"
        units = policy.high_risk_units
    elif (
        state.mll_buffer >= policy.middle_zone_buffer
        and state.remaining_target >= policy.middle_zone_remaining_target
    ):
        zone = "MIDDLE"
        units = policy.middle_risk_units
    else:
        zone = "BASE"
        units = 1
    quantity = int(intent.base_quantity * units)
    mini = float(intent.base_mini_equivalent * units)
    current = sum(row.mini_equivalent for row in state.open_exposures)
    if current + mini > policy.maximum_mini_equivalent + 1e-12:
        return _blocked(policy, "SHARED_CONTRACT_LIMIT")
    return RoutingDecision(
        True,
        quantity,
        mini,
        f"{zone}_UNITS_{units}",
        policy.policy_id,
    )


def _parent_from_entry(entry: Mapping[str, Any]) -> EliteRobustnessPolicy:
    source = entry["policy"]
    exact_change: tuple[tuple[str, Any], ...] = (("source", "FROZEN_0018"),)
    return EliteRobustnessPolicy(
        policy_id=str(entry["policy_id"]),
        parent_policy_id=str(entry["policy_id"]),
        parent_policy_fingerprint=str(entry["immutable_policy_fingerprint"]),
        component_ids=tuple(str(value) for value in source["component_ids"]),
        retained_added_sleeve_id=str(entry["added_sleeve_id"]),
        mutation_family="FROZEN_0018_PARENT",
        failure_target="FROZEN_DEVELOPMENT_CONTROL",
        exact_change=exact_change,
        expected_effect="Matched unchanged 0018 parent control.",
        high_risk_units=int(source["high_risk_units"]),
        daily_loss_guard=float(source["daily_loss_guard"]),
        daily_profit_lock=float(source["daily_profit_lock"]),
        critical_buffer=float(source["critical_buffer"]),
        high_zone_buffer=float(source["high_zone_buffer"]),
        high_zone_remaining_target=float(source["high_zone_remaining_target"]),
        middle_zone_buffer=float(source["middle_zone_buffer"]),
        middle_zone_remaining_target=float(source["middle_zone_remaining_target"]),
        middle_risk_units=int(source["middle_risk_units"]),
        maximum_simultaneous_positions=int(source["maximum_simultaneous_positions"]),
        maximum_mini_equivalent=int(source["maximum_mini_equivalent"]),
    )


def _mutate_parent(
    parent: EliteRobustnessPolicy,
    *,
    family: str,
    attempt: int,
    components: Sequence[RobustnessComponent],
    by_id: Mapping[str, RobustnessComponent],
    parent_evidence: Mapping[str, Any],
    campaign_id: str,
) -> EliteRobustnessPolicy | None:
    contribution = {
        str(key): float(value)
        for key, value in parent_evidence["stressed_evidence"][
            "component_contribution"
        ].items()
    }
    removable = sorted(
        (
            value
            for value in parent.component_ids
            if value != parent.retained_added_sleeve_id
        ),
        key=lambda value: (contribution.get(value, 0.0), value),
    )
    unused = [row for row in components if row.sleeve_id not in set(parent.component_ids)]
    unused.sort(
        key=lambda row: (
            -int(row.market not in {by_id[value].market for value in parent.component_ids}),
            -int(row.session_code not in {by_id[value].session_code for value in parent.component_ids}),
            -int(row.role not in {by_id[value].role for value in parent.component_ids}),
            -row.stressed_net_pnl,
            -row.event_count,
            row.behavioral_fingerprint,
        )
    )
    if family in {"OPPORTUNITY_REPLACEMENT", "MARKET_SESSION_DIVERSIFIER"}:
        if not removable or not unused:
            return None
        removed = removable[(attempt // max(1, len(unused))) % len(removable)]
        replacement = unused[attempt % len(unused)]
        ids = [replacement.sleeve_id if value == removed else value for value in parent.component_ids]
        insertion = (attempt // max(1, len(unused) * len(removable))) % len(ids)
        if family == "MARKET_SESSION_DIVERSIFIER":
            ids.remove(replacement.sleeve_id)
            ids.insert(insertion, replacement.sleeve_id)
            failure = "INSUFFICIENT_OPPORTUNITY_COUNT"
            expected = "Add market/session coverage and prioritize it deterministically."
        else:
            # Rotation changes conflict priority while preserving one-for-one breadth.
            ids = ids[insertion:] + ids[:insertion]
            failure = "INSUFFICIENT_TARGET_VELOCITY"
            expected = "Replace a low-contribution sleeve with higher opportunity density."
        change = {
            "removed_sleeve_id": removed,
            "replacement_sleeve_id": replacement.sleeve_id,
            "priority_rotation": insertion,
        }
        provisional = replace(parent, component_ids=tuple(ids))
    elif family == "PRIORITY_REALLOCATION":
        first = attempt % len(parent.component_ids)
        second = (attempt // len(parent.component_ids)) % len(parent.component_ids)
        if first == second:
            return None
        promoted = [parent.component_ids[first], parent.component_ids[second]]
        ids = promoted + [value for value in parent.component_ids if value not in set(promoted)]
        failure = "SEQUENCE_PATH_DEPENDENCY"
        expected = "Give productive sleeves earlier deterministic conflict priority."
        change = {"promoted_component_ids": promoted}
        provisional = replace(parent, component_ids=tuple(ids))
    elif family == "COST_PRUNE":
        if len(parent.component_ids) <= 10 or not removable:
            return None
        removed = removable[attempt % len(removable)]
        ids = tuple(value for value in parent.component_ids if value != removed)
        failure = "ADVERSE_COST_SENSITIVITY"
        expected = "Remove a low-contribution source of turnover and cost."
        change = {"removed_sleeve_id": removed}
        provisional = replace(parent, component_ids=ids)
    elif family == "BUFFER_ACCELERATION":
        profiles = (
            {"high_risk_units": 4},
            {"middle_risk_units": 3},
            {"high_risk_units": 4, "daily_loss_guard": 750.0},
            {"middle_risk_units": 3, "daily_loss_guard": 750.0},
            {"high_risk_units": 2, "middle_risk_units": 3},
            {"high_risk_units": 4, "middle_risk_units": 1},
            {"high_risk_units": 3, "middle_risk_units": 3},
            {"high_risk_units": 2, "daily_loss_guard": 750.0},
            {"high_risk_units": 4, "critical_buffer": 900.0},
            {"middle_risk_units": 3, "critical_buffer": 900.0},
            {"high_risk_units": 4, "high_zone_buffer": 4_000.0},
            {"middle_risk_units": 3, "middle_zone_buffer": 3_250.0},
            {"high_risk_units": 4, "high_zone_remaining_target": 5_000.0},
            {"middle_risk_units": 3, "middle_zone_remaining_target": 2_750.0},
            {
                "high_risk_units": 4,
                "middle_risk_units": 3,
                "daily_loss_guard": 750.0,
            },
            {
                "high_risk_units": 4,
                "middle_risk_units": 3,
                "critical_buffer": 900.0,
            },
        )
        profile = profiles[attempt % len(profiles)]
        failure = "INSUFFICIENT_TARGET_VELOCITY"
        expected = "Deploy bounded extra risk only inside frozen favorable buffer zones."
        change = dict(profile)
        provisional = replace(parent, **profile)
    elif family == "PROFIT_SMOOTHER":
        profiles = (
            {"daily_profit_lock": 1_500.0},
            {"daily_profit_lock": 1_750.0},
            {"daily_profit_lock": 2_000.0},
            {"daily_profit_lock": 1_750.0, "daily_loss_guard": 750.0},
            {"daily_profit_lock": 2_000.0, "daily_loss_guard": 750.0},
            {"daily_profit_lock": 1_500.0, "daily_loss_guard": 750.0},
        )
        profile = profiles[attempt % len(profiles)]
        failure = "EXCESSIVE_PROFIT_CONCENTRATION"
        expected = "Cap exceptional days without changing underlying sleeve signals."
        change = dict(profile)
        provisional = replace(parent, **profile)
    else:
        raise ValueError(f"unknown elite robustness family: {family}")

    payload = provisional.structural_payload()
    policy_id = deterministic_id(
        "elite_robustness_child",
        [campaign_id, parent.policy_id, family, payload],
    )
    return replace(
        provisional,
        policy_id=policy_id,
        mutation_family=family,
        failure_target=failure,
        exact_change=tuple(sorted(change.items())),
        expected_effect=expected,
    )


def _cheap_screen(
    policy: EliteRobustnessPolicy,
    *,
    by_id: Mapping[str, RobustnessComponent],
    parent_evidence: Mapping[str, Any],
) -> dict[str, Any]:
    selected = [by_id[value] for value in policy.component_ids]
    normal = sum(row.net_pnl for row in selected)
    stressed = sum(row.stressed_net_pnl for row in selected)
    events = sum(row.event_count for row in selected)
    markets = len({row.market for row in selected})
    sessions = len({row.session_code for row in selected})
    roles = len({row.role for row in selected})
    positive = [max(0.0, row.stressed_net_pnl) for row in selected]
    concentration = max(positive, default=0.0) / max(sum(positive), 1e-12)
    parent_stress = float(
        parent_evidence["stressed_evidence"]["median_episode_net_pnl"]
    )
    survivor = bool(
        normal > 0.0
        and stressed > 0.0
        and events >= 200
        and concentration <= 0.40
        and policy.maximum_mini_equivalent <= 15
    )
    score = (
        stressed
        + 0.20 * normal
        + 0.25 * events
        + 100.0 * markets
        + 75.0 * sessions
        + 50.0 * roles
        - 500.0 * concentration
        + 0.05 * parent_stress
    )
    return {
        "policy_id": policy.policy_id,
        "parent_policy_id": policy.parent_policy_id,
        "structural_fingerprint": policy.structural_fingerprint,
        "mutation_family": policy.mutation_family,
        "failure_target": policy.failure_target,
        "approximate_normal_net_usd": normal,
        "approximate_stressed_net_usd": stressed,
        "approximate_event_count": events,
        "market_count": markets,
        "session_count": sessions,
        "role_count": roles,
        "maximum_component_share": concentration,
        "economic_screen_score": score,
        "cheap_screen_survivor": survivor,
        "rolling_combine_executed": False,
        "validated": False,
    }


def _round_robin_parent_selection(
    rows: Sequence[Mapping[str, Any]], *, quota: int
) -> list[Mapping[str, Any]]:
    by_parent: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        by_parent.setdefault(str(row["parent_policy_id"]), []).append(row)
    parents = sorted(by_parent)
    output: list[Mapping[str, Any]] = []
    cursor = 0
    while len(output) < quota and parents:
        parent = parents[cursor % len(parents)]
        values = by_parent[parent]
        if values:
            output.append(values.pop(0))
        if not values:
            parents.remove(parent)
            if not parents:
                break
            cursor %= len(parents)
        else:
            cursor += 1
    if len(output) != quota:
        raise ValueError("not enough family survivors for deep quota")
    return output


def _blocked(policy: EliteRobustnessPolicy, reason: str) -> RoutingDecision:
    return RoutingDecision(False, 0, 0.0, reason, policy.policy_id)


def _counts(values: Sequence[str] | Any) -> dict[str, int]:
    output: dict[str, int] = {}
    for value in values:
        output[str(value)] = output.get(str(value), 0) + 1
    return dict(sorted(output.items()))


__all__ = [
    "DEEP_EVALUATION_QUOTAS",
    "ELITE_ROBUSTNESS_CLASS_ID",
    "ELITE_ROBUSTNESS_POLICY_VERSION",
    "EliteRobustnessPolicy",
    "EliteRobustnessPolicyPair",
    "EliteRobustnessPopulation",
    "MUTATION_QUOTAS",
    "RobustnessComponent",
    "generate_elite_robustness_population",
    "route_elite_robustness_entry",
]
