from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping

from hydra.account_policy.schema import AccountPolicyKind
from hydra.economic_evolution.account_complementary_sleeve import (
    ComplementarySleevePopulation,
    generate_complementary_sleeve_population,
)
from hydra.economic_evolution.account_coverage_three_zone import THREE_ZONE_LIMITS
from hydra.economic_evolution.role_aware_account import RoleAwareComponent
from hydra.economic_evolution.schema import deterministic_id, stable_hash


PARTIAL_RUNNER_CLASS_ID = "GREEN_COMPLEMENTARY_SLEEVE_PARTIAL_RUNNER_V1"
PARENT_POPULATION_CAMPAIGN_ID = (
    "hydra_economic_evolution_complementary_sleeve_0017"
)
PARENT_POPULATION_MANIFEST_HASH = (
    "6c84ababed3f8c331cbb3e892eca211510e4cc10b3b163a7701395d083835781"
)
PARTIAL_RUNNER_EXIT = "PARTIAL_ONE_SIGMA_TARGET_PLUS_EXACT_TIME_RUNNER"
MATCHED_CONTROL_EXIT = "TWO_LOT_EXACT_TIME_EXIT"
TARGET_VOLATILITY_MULTIPLE = 1.0
TARGET_QUANTITY = 1
RUNNER_QUANTITY = 1
TOTAL_QUANTITY = TARGET_QUANTITY + RUNNER_QUANTITY


@dataclass(frozen=True, slots=True)
class PartialRunnerPolicy:
    policy_id: str
    parent_policy_id: str
    component_ids: tuple[str, ...]
    mutated_sleeve_id: str
    exit_representation: str
    target_volatility_multiple: float
    target_quantity: int
    runner_quantity: int
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
            raise ValueError("partial-runner policy identity is required")
        if not 11 <= len(self.component_ids) <= 13:
            raise ValueError("partial-runner policy requires eleven to thirteen sleeves")
        if len(set(self.component_ids)) != len(self.component_ids):
            raise ValueError("partial-runner sleeves must be unique")
        if self.mutated_sleeve_id not in self.component_ids:
            raise ValueError("mutated sleeve is absent from frozen membership")
        if self.component_ids[-1] != self.mutated_sleeve_id:
            raise ValueError("mutated sleeve must retain lowest frozen priority")
        if self.exit_representation not in {
            PARTIAL_RUNNER_EXIT,
            MATCHED_CONTROL_EXIT,
        }:
            raise ValueError("unknown frozen exit representation")
        if self.target_volatility_multiple != TARGET_VOLATILITY_MULTIPLE:
            raise ValueError("target-volatility multiple drift")
        if (self.target_quantity, self.runner_quantity) != (
            TARGET_QUANTITY,
            RUNNER_QUANTITY,
        ):
            raise ValueError("partial-runner quantity split drift")
        if self.high_risk_units != 3:
            raise ValueError("partial-runner policy freezes three high-zone units")
        for key, expected in THREE_ZONE_LIMITS.items():
            if getattr(self, key) != expected:
                raise ValueError(f"partial-runner policy {key} drift")
        if self.version != 1 or self.inherited_status is not None:
            raise ValueError("partial-runner children cannot inherit status")

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
            "schema": "hydra_partial_runner_policy_v1",
            "parent_policy_id": self.parent_policy_id,
            "component_ids": list(self.component_ids),
            "mutated_sleeve_id": self.mutated_sleeve_id,
            "exit_representation": self.exit_representation,
            "target_volatility_multiple": self.target_volatility_multiple,
            "target_quantity": self.target_quantity,
            "runner_quantity": self.runner_quantity,
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
class PartialRunnerPolicyPair:
    pair_id: str
    parent_policy_id: str
    mutated_sleeve_id: str
    real_policy: PartialRunnerPolicy
    matched_control_policy: PartialRunnerPolicy

    def __post_init__(self) -> None:
        real, control = self.real_policy, self.matched_control_policy
        if real.parent_policy_id != self.parent_policy_id:
            raise ValueError("real partial-runner parent drift")
        if control.parent_policy_id != self.parent_policy_id:
            raise ValueError("control partial-runner parent drift")
        if real.component_ids != control.component_ids:
            raise ValueError("exit comparison must retain identical membership")
        if real.mutated_sleeve_id != self.mutated_sleeve_id:
            raise ValueError("real mutated-sleeve identity drift")
        if control.mutated_sleeve_id != self.mutated_sleeve_id:
            raise ValueError("control mutated-sleeve identity drift")
        if real.exit_representation != PARTIAL_RUNNER_EXIT:
            raise ValueError("real policy must use the frozen partial runner")
        if control.exit_representation != MATCHED_CONTROL_EXIT:
            raise ValueError("control policy must use the two-lot time exit")
        if _limits(real) != _limits(control):
            raise ValueError("partial-runner pair account limits differ")

    def to_dict(self) -> dict[str, Any]:
        return {
            "pair_id": self.pair_id,
            "parent_policy_id": self.parent_policy_id,
            "real_policy_id": self.real_policy.policy_id,
            "matched_control_policy_id": self.matched_control_policy.policy_id,
            "mutated_sleeve_id": self.mutated_sleeve_id,
            "component_count": len(self.real_policy.component_ids),
            "identical_membership": True,
            "identical_signal_paths": True,
            "identical_total_quantity": True,
            "identical_account_limits": True,
            "real_exit_representation": PARTIAL_RUNNER_EXIT,
            "control_exit_representation": MATCHED_CONTROL_EXIT,
        }


@dataclass(frozen=True, slots=True)
class PartialRunnerPopulation:
    campaign_id: str
    parent_campaign_id: str
    parent_population_manifest_hash: str
    components: tuple[RoleAwareComponent, ...]
    pairs: tuple[PartialRunnerPolicyPair, ...]
    manifest_hash: str

    @property
    def real_policies(self) -> tuple[PartialRunnerPolicy, ...]:
        return tuple(row.real_policy for row in self.pairs)

    @property
    def matched_control_policies(self) -> tuple[PartialRunnerPolicy, ...]:
        return tuple(row.matched_control_policy for row in self.pairs)

    def summary(self) -> dict[str, Any]:
        return {
            "campaign_id": self.campaign_id,
            "class_id": PARTIAL_RUNNER_CLASS_ID,
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
            "distinct_mutated_sleeve_count": len(
                {row.mutated_sleeve_id for row in self.pairs}
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


def generate_partial_runner_population(
    seed_archive: Mapping[str, Any],
    *,
    campaign_id: str,
    parent_campaign_id: str,
    sizing_parent_campaign_id: str,
    coverage_parent_campaign_id: str,
    policy_pair_count: int = 512,
    maximum_components: int = 48,
    minimum_component_events: int = 20,
) -> PartialRunnerPopulation:
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
        raise ValueError("frozen complementary-sleeve parent population drift")
    pairs: list[PartialRunnerPolicyPair] = []
    for source in sorted(parent.pairs, key=lambda row: row.pair_id):
        parent_policy = source.real_policy
        real = _child(
            campaign_id,
            parent_policy.policy_id,
            parent_policy.component_ids,
            mutated_sleeve_id=source.added_sleeve_id,
            exit_representation=PARTIAL_RUNNER_EXIT,
            label="real",
        )
        control = _child(
            campaign_id,
            parent_policy.policy_id,
            parent_policy.component_ids,
            mutated_sleeve_id=source.added_sleeve_id,
            exit_representation=MATCHED_CONTROL_EXIT,
            label="control",
        )
        pairs.append(
            PartialRunnerPolicyPair(
                pair_id=deterministic_id(
                    "partial_runner_pair",
                    [campaign_id, parent_policy.policy_id, source.added_sleeve_id],
                ),
                parent_policy_id=parent_policy.policy_id,
                mutated_sleeve_id=source.added_sleeve_id,
                real_policy=real,
                matched_control_policy=control,
            )
        )
    payload = {
        "schema": "hydra_partial_runner_population_v1",
        "campaign_id": campaign_id,
        "class_id": PARTIAL_RUNNER_CLASS_ID,
        "parent_campaign_id": PARENT_POPULATION_CAMPAIGN_ID,
        "parent_population_manifest_hash": parent.manifest_hash,
        "pairs": [
            {
                "pair_id": row.pair_id,
                "parent_policy_id": row.parent_policy_id,
                "mutated_sleeve_id": row.mutated_sleeve_id,
                "real": row.real_policy.structural_fingerprint,
                "control": row.matched_control_policy.structural_fingerprint,
            }
            for row in pairs
        ],
        "target_volatility_multiple": TARGET_VOLATILITY_MULTIPLE,
        "target_quantity": TARGET_QUANTITY,
        "runner_quantity": RUNNER_QUANTITY,
        "target_is_past_volatility_derived": True,
        "runner_retains_exact_parent_time_exit": True,
        "same_signals_membership_and_account_limits": True,
        "new_candidate_ids": True,
        "status_inheritance": False,
        "outcomes_seen_during_generation": False,
        "outbound_order_capability": False,
    }
    return PartialRunnerPopulation(
        campaign_id=campaign_id,
        parent_campaign_id=PARENT_POPULATION_CAMPAIGN_ID,
        parent_population_manifest_hash=parent.manifest_hash,
        components=parent.components,
        pairs=tuple(pairs),
        manifest_hash=stable_hash(payload),
    )


def _child(
    campaign_id: str,
    parent_policy_id: str,
    component_ids: tuple[str, ...],
    *,
    mutated_sleeve_id: str,
    exit_representation: str,
    label: str,
) -> PartialRunnerPolicy:
    return PartialRunnerPolicy(
        policy_id=deterministic_id(
            f"partial_runner_{label}",
            [
                campaign_id,
                parent_policy_id,
                mutated_sleeve_id,
                exit_representation,
            ],
        ),
        parent_policy_id=parent_policy_id,
        component_ids=component_ids,
        mutated_sleeve_id=mutated_sleeve_id,
        exit_representation=exit_representation,
        target_volatility_multiple=TARGET_VOLATILITY_MULTIPLE,
        target_quantity=TARGET_QUANTITY,
        runner_quantity=RUNNER_QUANTITY,
        high_risk_units=3,
        **dict(THREE_ZONE_LIMITS),
    )


def _limits(policy: PartialRunnerPolicy) -> tuple[Any, ...]:
    return (
        policy.high_risk_units,
        policy.target_volatility_multiple,
        policy.target_quantity,
        policy.runner_quantity,
        *(getattr(policy, key) for key in THREE_ZONE_LIMITS),
    )


__all__ = [
    "MATCHED_CONTROL_EXIT",
    "PARTIAL_RUNNER_CLASS_ID",
    "PARTIAL_RUNNER_EXIT",
    "PARENT_POPULATION_CAMPAIGN_ID",
    "PARENT_POPULATION_MANIFEST_HASH",
    "PartialRunnerPolicy",
    "PartialRunnerPolicyPair",
    "PartialRunnerPopulation",
    "RUNNER_QUANTITY",
    "TARGET_QUANTITY",
    "TARGET_VOLATILITY_MULTIPLE",
    "TOTAL_QUANTITY",
    "generate_partial_runner_population",
]
