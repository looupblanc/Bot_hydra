from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping

from hydra.account_policy.router import (
    AccountDecisionState,
    EntryIntent,
    RoutingDecision,
)
from hydra.account_policy.schema import AccountPolicyKind
from hydra.economic_evolution.account_coverage_union import (
    CoverageUnionPopulation,
    generate_coverage_union_population,
)
from hydra.economic_evolution.role_aware_account import RoleAwareComponent
from hydra.economic_evolution.schema import deterministic_id, stable_hash


BUFFER_SIZING_CLASS_ID = "GREEN_COVERAGE_UNION_BUFFER_AWARE_SIZING_V1"
BUFFER_SIZING_HYPOTHESIS = (
    "A green diversified coverage union can use two risk units only while "
    "both account buffer and target distance are large, then revert to one "
    "unit, increasing target velocity without changing its signals."
)
BUFFER_SIZING_LIMITS: dict[str, Any] = {
    "daily_loss_guard": 1_000.0,
    "daily_profit_lock": 2_250.0,
    "critical_buffer": 750.0,
    "accelerate_buffer": 3_000.0,
    "accelerate_remaining_target": 2_250.0,
    "maximum_simultaneous_positions": 3,
    "maximum_mini_equivalent": 15,
}


@dataclass(frozen=True, slots=True)
class CoverageSizingPolicy:
    policy_id: str
    parent_policy_id: str
    component_ids: tuple[str, ...]
    accelerate_risk_units: int
    daily_loss_guard: float
    daily_profit_lock: float
    critical_buffer: float
    accelerate_buffer: float
    accelerate_remaining_target: float
    maximum_simultaneous_positions: int
    maximum_mini_equivalent: int
    version: int = 1
    inherited_status: None = None

    def __post_init__(self) -> None:
        if not self.policy_id or not self.parent_policy_id:
            raise ValueError("coverage-sizing identity is required")
        if not 10 <= len(self.component_ids) <= 12:
            raise ValueError("coverage sizing requires ten to twelve sleeves")
        if len(set(self.component_ids)) != len(self.component_ids):
            raise ValueError("coverage-sizing sleeves must be unique")
        if self.accelerate_risk_units not in {1, 2}:
            raise ValueError("coverage sizing supports one or two units")
        for key, expected in BUFFER_SIZING_LIMITS.items():
            if getattr(self, key) != expected:
                raise ValueError(f"coverage-sizing {key} drift")
        if self.version != 1 or self.inherited_status is not None:
            raise ValueError("coverage-sizing children cannot inherit status")

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
            "schema": "hydra_coverage_sizing_policy_v1",
            "parent_policy_id": self.parent_policy_id,
            "component_ids": list(self.component_ids),
            "accelerate_risk_units": self.accelerate_risk_units,
            **dict(BUFFER_SIZING_LIMITS),
            "version": self.version,
        }

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        row["component_ids"] = list(self.component_ids)
        row["kind"] = self.kind.value
        row["structural_fingerprint"] = self.structural_fingerprint
        return row


@dataclass(frozen=True, slots=True)
class CoverageSizingPolicyPair:
    pair_id: str
    parent_policy_id: str
    real_policy: CoverageSizingPolicy
    matched_control_policy: CoverageSizingPolicy

    def __post_init__(self) -> None:
        real, control = self.real_policy, self.matched_control_policy
        if real.parent_policy_id != self.parent_policy_id:
            raise ValueError("real child parent drift")
        if control.parent_policy_id != self.parent_policy_id:
            raise ValueError("control child parent drift")
        if real.component_ids != control.component_ids:
            raise ValueError("coverage-sizing pair membership drift")
        if real.accelerate_risk_units != 2:
            raise ValueError("real coverage-sizing child must use two units")
        if control.accelerate_risk_units != 1:
            raise ValueError("coverage-sizing control must use one unit")
        if _limits(real) != _limits(control):
            raise ValueError("coverage-sizing pair account limits differ")

    def to_dict(self) -> dict[str, Any]:
        return {
            "pair_id": self.pair_id,
            "parent_policy_id": self.parent_policy_id,
            "real_policy_id": self.real_policy.policy_id,
            "matched_control_policy_id": self.matched_control_policy.policy_id,
            "component_count": len(self.real_policy.component_ids),
            "identical_parent_membership": True,
            "identical_component_event_paths": True,
            "identical_account_limits": True,
            "real_accelerate_risk_units": 2,
            "control_risk_units": 1,
            "different_sizing_policy_only": True,
        }


@dataclass(frozen=True, slots=True)
class CoverageSizingPopulation:
    campaign_id: str
    parent_campaign_id: str
    parent_population_manifest_hash: str
    components: tuple[RoleAwareComponent, ...]
    pairs: tuple[CoverageSizingPolicyPair, ...]
    manifest_hash: str

    @property
    def real_policies(self) -> tuple[CoverageSizingPolicy, ...]:
        return tuple(row.real_policy for row in self.pairs)

    @property
    def matched_control_policies(self) -> tuple[CoverageSizingPolicy, ...]:
        return tuple(row.matched_control_policy for row in self.pairs)

    def summary(self) -> dict[str, Any]:
        return {
            "campaign_id": self.campaign_id,
            "class_id": BUFFER_SIZING_CLASS_ID,
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


def generate_coverage_sizing_population(
    seed_archive: Mapping[str, Any],
    *,
    campaign_id: str,
    parent_campaign_id: str,
    policy_pair_count: int = 512,
    maximum_components: int = 48,
    minimum_component_events: int = 20,
) -> CoverageSizingPopulation:
    if not campaign_id.strip() or not parent_campaign_id.strip():
        raise ValueError("campaign identities must be non-empty")
    parent: CoverageUnionPopulation = generate_coverage_union_population(
        seed_archive,
        campaign_id=parent_campaign_id,
        policy_pair_count=policy_pair_count,
        maximum_components=maximum_components,
        minimum_component_events=minimum_component_events,
    )
    pairs: list[CoverageSizingPolicyPair] = []
    for source in sorted(parent.pairs, key=lambda row: row.pair_id):
        parent_policy = source.real_policy
        real = _child(
            campaign_id,
            parent_policy.policy_id,
            parent_policy.component_ids,
            accelerate_risk_units=2,
            label="real",
        )
        control = _child(
            campaign_id,
            parent_policy.policy_id,
            parent_policy.component_ids,
            accelerate_risk_units=1,
            label="control",
        )
        pairs.append(
            CoverageSizingPolicyPair(
                pair_id=deterministic_id(
                    "coverage_sizing_pair",
                    [campaign_id, parent_policy.policy_id],
                ),
                parent_policy_id=parent_policy.policy_id,
                real_policy=real,
                matched_control_policy=control,
            )
        )
    payload = {
        "schema": "hydra_coverage_sizing_population_v1",
        "campaign_id": campaign_id,
        "class_id": BUFFER_SIZING_CLASS_ID,
        "parent_campaign_id": parent_campaign_id,
        "parent_population_manifest_hash": parent.manifest_hash,
        "component_behavioral_fingerprints": [
            row.sleeve.behavioral_fingerprint for row in parent.components
        ],
        "pairs": [
            {
                "pair_id": row.pair_id,
                "parent_policy_id": row.parent_policy_id,
                "real": row.real_policy.structural_fingerprint,
                "control": row.matched_control_policy.structural_fingerprint,
            }
            for row in pairs
        ],
        "limits": dict(BUFFER_SIZING_LIMITS),
        "real_accelerate_risk_units": 2,
        "control_risk_units": 1,
        "same_membership": True,
        "same_signal_paths": True,
        "new_candidate_ids": True,
        "status_inheritance": False,
        "outcomes_seen_during_generation": False,
        "outbound_order_capability": False,
    }
    return CoverageSizingPopulation(
        campaign_id=campaign_id,
        parent_campaign_id=parent_campaign_id,
        parent_population_manifest_hash=parent.manifest_hash,
        components=parent.components,
        pairs=tuple(pairs),
        manifest_hash=stable_hash(payload),
    )


def route_coverage_sizing_entry(
    intent: EntryIntent,
    state: AccountDecisionState,
    *,
    policy: CoverageSizingPolicy,
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
    accelerate = bool(
        state.mll_buffer >= policy.accelerate_buffer
        and state.remaining_target >= policy.accelerate_remaining_target
    )
    units = policy.accelerate_risk_units if accelerate else 1
    quantity = int(intent.base_quantity * units)
    mini = float(intent.base_mini_equivalent * units)
    current = sum(row.mini_equivalent for row in state.open_exposures)
    if current + mini > policy.maximum_mini_equivalent + 1e-12:
        return _blocked(policy, "SHARED_CONTRACT_LIMIT")
    return RoutingDecision(
        True,
        quantity,
        mini,
        f"{'ACCELERATE' if accelerate else 'BASE'}_UNITS_{units}",
        policy.policy_id,
    )


def _child(
    campaign_id: str,
    parent_policy_id: str,
    component_ids: tuple[str, ...],
    *,
    accelerate_risk_units: int,
    label: str,
) -> CoverageSizingPolicy:
    return CoverageSizingPolicy(
        policy_id=deterministic_id(
            f"coverage_sizing_{label}",
            [campaign_id, parent_policy_id, label, accelerate_risk_units],
        ),
        parent_policy_id=parent_policy_id,
        component_ids=component_ids,
        accelerate_risk_units=accelerate_risk_units,
        **dict(BUFFER_SIZING_LIMITS),
    )


def _limits(policy: CoverageSizingPolicy) -> tuple[Any, ...]:
    return tuple(getattr(policy, key) for key in BUFFER_SIZING_LIMITS)


def _blocked(policy: CoverageSizingPolicy, reason: str) -> RoutingDecision:
    return RoutingDecision(False, 0, 0.0, reason, policy.policy_id)


__all__ = [
    "BUFFER_SIZING_CLASS_ID",
    "BUFFER_SIZING_HYPOTHESIS",
    "BUFFER_SIZING_LIMITS",
    "CoverageSizingPolicy",
    "CoverageSizingPolicyPair",
    "CoverageSizingPopulation",
    "generate_coverage_sizing_population",
    "route_coverage_sizing_entry",
]
