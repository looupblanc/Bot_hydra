from __future__ import annotations

import hashlib
import itertools
import json
from dataclasses import asdict, dataclass, replace
from typing import Any, Mapping, Sequence

from hydra.account_policy.basket import RoutedTrade
from hydra.propfirm.combine_episode import TradePathEvent
from hydra.validation.v72_component_bank import ComponentEventPaths


V72_POLICY_VERSION = "hydra_account_policy_v7_2_crossfit_v1"
TARGET_VELOCITY_ROLE = "TARGET_VELOCITY"


@dataclass(frozen=True, slots=True)
class V72BasketStructure:
    structure_id: str
    component_ids: tuple[str, ...]
    allocation_profile: str
    structural_hash: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class FrozenRotationBasket:
    basket_id: str
    source_structure_id: str
    source_structural_hash: str
    component_ids: tuple[str, ...]
    allocation_profile: str
    component_risk_units: tuple[tuple[str, int], ...]
    component_priority: tuple[str, ...]
    design_block_ids: tuple[str, ...]
    held_out_block_id: str
    policy_version: str
    basket_hash: str

    @property
    def risk_units(self) -> dict[str, int]:
        return dict(self.component_risk_units)

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        row["component_risk_units"] = dict(self.component_risk_units)
        return row


def generate_static_basket_structures(
    primary_components: Sequence[Mapping[str, Any]],
    *,
    minimum_size: int = 2,
    maximum_size: int = 4,
) -> tuple[V72BasketStructure, ...]:
    components = {
        str(row["candidate_id"]): str(row["role"]) for row in primary_components
    }
    if not minimum_size <= maximum_size:
        raise ValueError("invalid basket size bounds")
    if len(components) < maximum_size:
        raise ValueError("component bank is smaller than maximum basket size")
    rows: list[V72BasketStructure] = []
    for size in range(minimum_size, maximum_size + 1):
        for members in itertools.combinations(sorted(components), size):
            profiles = ["UNIT_EQUAL"]
            if any(components[member] == TARGET_VELOCITY_ROLE for member in members):
                profiles.append("TARGET_VELOCITY_TILT")
            for profile in profiles:
                payload = {
                    "component_ids": list(members),
                    "allocation_profile": profile,
                    "policy_version": V72_POLICY_VERSION,
                    "tilt_selection": (
                        "highest_individual_design_STRESS_1_5X_net_among_TARGET_VELOCITY_components"
                        if profile == "TARGET_VELOCITY_TILT"
                        else "none"
                    ),
                    "priority_selection": (
                        "individual_design_STRESS_1_5X_net_desc_then_candidate_id_asc"
                    ),
                }
                fingerprint = stable_hash(payload)
                rows.append(
                    V72BasketStructure(
                        structure_id=f"v72_static_{fingerprint[:20]}",
                        component_ids=members,
                        allocation_profile=profile,
                        structural_hash=fingerprint,
                    )
                )
    ordered = tuple(sorted(rows, key=lambda row: row.structural_hash))
    if len({row.structural_hash for row in ordered}) != len(ordered):
        raise ValueError("static basket structures are not unique")
    return ordered


def freeze_rotation_basket(
    structure: V72BasketStructure,
    *,
    individual_design_stress_net: Mapping[str, float],
    component_roles: Mapping[str, str],
    design_block_ids: Sequence[str],
    held_out_block_id: str,
) -> FrozenRotationBasket:
    members = structure.component_ids
    missing = sorted(set(members).difference(individual_design_stress_net))
    if missing:
        raise ValueError("missing design-only individual metrics: " + ",".join(missing))
    priority = tuple(
        sorted(
            members,
            key=lambda value: (
                -float(individual_design_stress_net[value]),
                value,
            ),
        )
    )
    risk_units = {member: 1 for member in members}
    if structure.allocation_profile == "TARGET_VELOCITY_TILT":
        candidates = [
            member
            for member in members
            if component_roles[member] == TARGET_VELOCITY_ROLE
        ]
        if not candidates:
            raise ValueError("target-velocity tilt has no eligible component")
        tilted = min(
            candidates,
            key=lambda value: (
                -float(individual_design_stress_net[value]),
                value,
            ),
        )
        risk_units[tilted] = 2
    elif structure.allocation_profile != "UNIT_EQUAL":
        raise ValueError("unsupported V7.2 allocation profile")
    payload = {
        "source_structure_id": structure.structure_id,
        "source_structural_hash": structure.structural_hash,
        "component_ids": list(members),
        "allocation_profile": structure.allocation_profile,
        "component_risk_units": risk_units,
        "component_priority": list(priority),
        "design_block_ids": list(design_block_ids),
        "held_out_block_id": held_out_block_id,
        "policy_version": V72_POLICY_VERSION,
    }
    basket_hash = stable_hash(payload)
    return FrozenRotationBasket(
        basket_id=f"v72_rotation_{basket_hash[:20]}",
        source_structure_id=structure.structure_id,
        source_structural_hash=structure.structural_hash,
        component_ids=members,
        allocation_profile=structure.allocation_profile,
        component_risk_units=tuple(sorted(risk_units.items())),
        component_priority=priority,
        design_block_ids=tuple(str(value) for value in design_block_ids),
        held_out_block_id=str(held_out_block_id),
        policy_version=V72_POLICY_VERSION,
        basket_hash=basket_hash,
    )


def routed_component_events(
    paths: Mapping[str, ComponentEventPaths],
    basket: FrozenRotationBasket,
    *,
    stress: str,
) -> dict[str, tuple[RoutedTrade, ...]]:
    output: dict[str, tuple[RoutedTrade, ...]] = {}
    for candidate_id in basket.component_ids:
        source = paths[candidate_id]
        events = source.base_events if stress == "BASE" else source.stress_events
        units = basket.risk_units[candidate_id]
        output[candidate_id] = tuple(
            RoutedTrade(
                component_id=candidate_id,
                market="ES",
                side=int(side),
                event=scale_trade_path_event(event, units=units),
            )
            for side, event in zip(source.sides, events, strict=True)
        )
    return output


def scale_trade_path_event(event: TradePathEvent, *, units: int) -> TradePathEvent:
    if units not in {1, 2}:
        raise ValueError("V7.2 risk units must be one or two")
    if units == 1:
        return event
    return replace(
        event,
        event_id=f"{event.event_id}:risk_units_{units}",
        net_pnl=float(event.net_pnl * units),
        gross_pnl=float(event.gross_pnl * units),
        worst_unrealized_pnl=float(event.worst_unrealized_pnl * units),
        best_unrealized_pnl=float(event.best_unrealized_pnl * units),
        quantity=int(event.quantity * units),
        mini_equivalent=float(event.mini_equivalent * units),
    )


def stable_hash(payload: Any) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


__all__ = [
    "FrozenRotationBasket",
    "TARGET_VELOCITY_ROLE",
    "V72BasketStructure",
    "V72_POLICY_VERSION",
    "freeze_rotation_basket",
    "generate_static_basket_structures",
    "routed_component_events",
    "scale_trade_path_event",
    "stable_hash",
]
