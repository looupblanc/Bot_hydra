from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import Any, Mapping


SCHEMA_VERSION = "hydra_account_policy_v6"


class AccountPolicyKind(StrEnum):
    INDIVIDUAL = "INDIVIDUAL_STRATEGY"
    STATIC_BASKET = "STATIC_ACCOUNT_BASKET"
    ADAPTIVE_CONTROLLER = "ADAPTIVE_ACCOUNT_CONTROLLER"
    RANDOM_CONTROL = "MATCHED_RANDOM_ROUTER_CONTROL"


class ComponentRole(StrEnum):
    ALPHA_COMPONENT = "ALPHA_COMPONENT"
    TARGET_VELOCITY_COMPONENT = "TARGET_VELOCITY_COMPONENT"
    XFA_PAYOUT_COMPONENT = "XFA_PAYOUT_COMPONENT"
    DEFENSIVE_COMPONENT = "DEFENSIVE_COMPONENT"
    DIVERSIFIER_COMPONENT = "DIVERSIFIER_COMPONENT"
    SESSION_SPECIALIST = "SESSION_SPECIALIST"
    MARKET_SPECIALIST = "MARKET_SPECIALIST"
    RARE_EVENT_COMPONENT = "RARE_EVENT_COMPONENT"


@dataclass(frozen=True, slots=True)
class ComponentDescriptor:
    component_id: str
    specification_hash: str
    market: str
    execution_market: str
    family: str
    timeframe: str
    role: ComponentRole
    behavioral_cluster: str
    source_experiment: str
    source_result_hash: str
    net_pnl_after_costs: float
    cost_stress_net_pnl: float
    event_count: int
    rolling_pass_rate: float
    rolling_mll_breach_rate: float
    median_target_progress: float
    expected_xfa_cycles: float = 0.0
    deterministic_implementation: bool = True
    hard_invalidated: bool = False
    inherited_status: bool = False

    def __post_init__(self) -> None:
        for name in (
            "component_id",
            "specification_hash",
            "market",
            "execution_market",
            "family",
            "timeframe",
            "behavioral_cluster",
            "source_experiment",
            "source_result_hash",
        ):
            if not str(getattr(self, name)).strip():
                raise ValueError(f"{name} must be non-empty")
        if self.event_count < 0:
            raise ValueError("event_count cannot be negative")
        for name in (
            "net_pnl_after_costs",
            "cost_stress_net_pnl",
            "rolling_pass_rate",
            "rolling_mll_breach_rate",
            "median_target_progress",
            "expected_xfa_cycles",
        ):
            if not math.isfinite(float(getattr(self, name))):
                raise ValueError(f"{name} must be finite")
        if not 0.0 <= self.rolling_pass_rate <= 1.0:
            raise ValueError("rolling_pass_rate must be in [0,1]")
        if not 0.0 <= self.rolling_mll_breach_rate <= 1.0:
            raise ValueError("rolling_mll_breach_rate must be in [0,1]")

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        row["role"] = self.role.value
        return row


@dataclass(frozen=True, slots=True)
class BasketPolicy:
    policy_id: str
    component_ids: tuple[str, ...]
    archetype: str
    maximum_simultaneous_positions: int = 4
    maximum_mini_equivalent: int = 15
    conflict_policy: str = "FIXED_PRIORITY_SAME_MARKET_EXCLUSIVE"
    component_priority: tuple[str, ...] = ()
    policy_version: str = SCHEMA_VERSION

    def __post_init__(self) -> None:
        if not self.policy_id:
            raise ValueError("policy_id must be non-empty")
        if not 1 <= len(self.component_ids) <= 5:
            raise ValueError("basket must contain one to five components")
        if len(set(self.component_ids)) != len(self.component_ids):
            raise ValueError("basket components must be unique")
        if self.maximum_simultaneous_positions < 1:
            raise ValueError("maximum_simultaneous_positions must be positive")
        if not 1 <= self.maximum_mini_equivalent <= 15:
            raise ValueError("maximum_mini_equivalent must be in [1,15]")
        priority = self.component_priority or self.component_ids
        if set(priority) != set(self.component_ids):
            raise ValueError("component_priority must contain every component once")

    @property
    def kind(self) -> AccountPolicyKind:
        return (
            AccountPolicyKind.INDIVIDUAL
            if len(self.component_ids) == 1
            else AccountPolicyKind.STATIC_BASKET
        )

    @property
    def fingerprint(self) -> str:
        return self.structural_fingerprint

    @property
    def structural_fingerprint(self) -> str:
        return stable_hash(
            {
                "component_ids": sorted(self.component_ids),
                "archetype": self.archetype,
                "maximum_simultaneous_positions": self.maximum_simultaneous_positions,
                "maximum_mini_equivalent": self.maximum_mini_equivalent,
                "conflict_policy": self.conflict_policy,
                "component_priority": list(
                    self.component_priority or self.component_ids
                ),
                "policy_version": self.policy_version,
            }
        )

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        row["kind"] = self.kind.value
        row["component_ids"] = list(self.component_ids)
        row["component_priority"] = list(
            self.component_priority or self.component_ids
        )
        return row

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "BasketPolicy":
        return cls(
            policy_id=str(value["policy_id"]),
            component_ids=tuple(str(item) for item in value["component_ids"]),
            archetype=str(value["archetype"]),
            maximum_simultaneous_positions=int(
                value.get("maximum_simultaneous_positions") or 4
            ),
            maximum_mini_equivalent=int(
                value.get("maximum_mini_equivalent") or 15
            ),
            conflict_policy=str(
                value.get("conflict_policy")
                or "FIXED_PRIORITY_SAME_MARKET_EXCLUSIVE"
            ),
            component_priority=tuple(
                str(item)
                for item in (
                    value.get("component_priority") or value["component_ids"]
                )
            ),
            policy_version=str(value.get("policy_version") or SCHEMA_VERSION),
        )


@dataclass(frozen=True, slots=True)
class ControllerPolicy:
    controller_id: str
    basket_policy_id: str
    component_priority: tuple[str, ...]
    daily_loss_limit: float
    daily_profit_lock: float
    loss_streak_derisk_after: int
    low_buffer_threshold: float
    critical_buffer_threshold: float
    maximum_simultaneous_positions: int
    maximum_mini_equivalent: int = 15
    allow_regimes: tuple[str, ...] = ()
    routing_policy: str = "FIXED_PRIORITY_PAST_ONLY"
    random_control_seed: int | None = None
    policy_version: str = SCHEMA_VERSION

    def __post_init__(self) -> None:
        if not self.controller_id or not self.basket_policy_id:
            raise ValueError("controller and basket IDs are required")
        if not self.component_priority or len(set(self.component_priority)) != len(
            self.component_priority
        ):
            raise ValueError("component_priority must be non-empty and unique")
        if self.daily_loss_limit <= 0.0 or self.daily_profit_lock <= 0.0:
            raise ValueError("daily guard thresholds must be positive")
        if self.loss_streak_derisk_after < 1:
            raise ValueError("loss streak threshold must be positive")
        if not 0.0 < self.critical_buffer_threshold <= self.low_buffer_threshold:
            raise ValueError("buffer thresholds are not ordered")
        if self.low_buffer_threshold > 4_500.0:
            raise ValueError("low buffer threshold exceeds the account MLL distance")
        if self.maximum_simultaneous_positions < 1:
            raise ValueError("maximum simultaneous positions must be positive")
        if not 1 <= self.maximum_mini_equivalent <= 15:
            raise ValueError("maximum mini equivalent must be in [1,15]")

    @property
    def kind(self) -> AccountPolicyKind:
        return (
            AccountPolicyKind.RANDOM_CONTROL
            if self.random_control_seed is not None
            else AccountPolicyKind.ADAPTIVE_CONTROLLER
        )

    @property
    def fingerprint(self) -> str:
        return stable_hash(self.to_dict())

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        row["kind"] = self.kind.value
        row["component_priority"] = list(self.component_priority)
        row["allow_regimes"] = list(self.allow_regimes)
        return row

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ControllerPolicy":
        return cls(
            controller_id=str(value["controller_id"]),
            basket_policy_id=str(value["basket_policy_id"]),
            component_priority=tuple(
                str(item) for item in value["component_priority"]
            ),
            daily_loss_limit=float(value["daily_loss_limit"]),
            daily_profit_lock=float(value["daily_profit_lock"]),
            loss_streak_derisk_after=int(value["loss_streak_derisk_after"]),
            low_buffer_threshold=float(value["low_buffer_threshold"]),
            critical_buffer_threshold=float(value["critical_buffer_threshold"]),
            maximum_simultaneous_positions=int(
                value["maximum_simultaneous_positions"]
            ),
            maximum_mini_equivalent=int(
                value.get("maximum_mini_equivalent") or 15
            ),
            allow_regimes=tuple(str(item) for item in value.get("allow_regimes", ())),
            routing_policy=str(
                value.get("routing_policy") or "FIXED_PRIORITY_PAST_ONLY"
            ),
            random_control_seed=(
                int(value["random_control_seed"])
                if value.get("random_control_seed") is not None
                else None
            ),
            policy_version=str(value.get("policy_version") or SCHEMA_VERSION),
        )


def stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


__all__ = [
    "AccountPolicyKind",
    "BasketPolicy",
    "ComponentDescriptor",
    "ComponentRole",
    "ControllerPolicy",
    "SCHEMA_VERSION",
    "stable_hash",
]
