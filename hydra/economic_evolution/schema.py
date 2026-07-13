from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import Any, Mapping, Sequence


SCHEMA_VERSION = "hydra_economic_evolution_v1"


class ComponentKind(StrEnum):
    CONTEXT = "CONTEXT"
    TRIGGER = "TRIGGER"
    DIRECTION = "DIRECTION"
    ELIGIBILITY = "ELIGIBILITY"
    SIZING = "SIZING"
    STOP = "STOP"
    TARGET = "TARGET"
    TIME_EXIT = "TIME_EXIT"
    TRADE_VETO = "TRADE_VETO"
    PORTFOLIO_ROLE = "PORTFOLIO_ROLE"
    ACCOUNT_STATE_RESPONSE = "ACCOUNT_STATE_RESPONSE"


class PortType(StrEnum):
    FEATURE_SCALAR = "FEATURE_SCALAR"
    MARKET_STATE = "MARKET_STATE"
    EVENT_TRIGGER = "EVENT_TRIGGER"
    DIRECTION = "DIRECTION"
    ELIGIBILITY = "ELIGIBILITY"
    ACCOUNT_STATE = "ACCOUNT_STATE"
    POSITION_SIZE = "POSITION_SIZE"
    EXIT_POLICY = "EXIT_POLICY"
    PORTFOLIO_ROLE = "PORTFOLIO_ROLE"
    ACCOUNT_ACTION = "ACCOUNT_ACTION"


class EconomicRole(StrEnum):
    PRIMARY_ALPHA = "PRIMARY_ALPHA"
    SECONDARY_ALPHA = "SECONDARY_ALPHA"
    TARGET_ACCELERATOR = "TARGET_ACCELERATOR"
    MLL_STABILIZER = "MLL_STABILIZER"
    CONSISTENCY_SMOOTHER = "CONSISTENCY_SMOOTHER"
    SESSION_DIVERSIFIER = "SESSION_DIVERSIFIER"
    MARKET_DIVERSIFIER = "MARKET_DIVERSIFIER"
    XFA_COMPONENT = "XFA_COMPONENT"
    PAYOUT_STABILIZER = "PAYOUT_STABILIZER"
    DEFENSIVE_SWITCH = "DEFENSIVE_SWITCH"


class FailureDimension(StrEnum):
    INSUFFICIENT_OPPORTUNITY_COUNT = "INSUFFICIENT_OPPORTUNITY_COUNT"
    INSUFFICIENT_TARGET_VELOCITY = "INSUFFICIENT_TARGET_VELOCITY"
    MLL_BREACH = "MLL_BREACH"
    WEAK_COST_MARGIN = "WEAK_COST_MARGIN"
    UNSTABLE_TEMPORAL_TRANSFER = "UNSTABLE_TEMPORAL_TRANSFER"
    HIDDEN_DIRECTIONAL_BETA = "HIDDEN_DIRECTIONAL_BETA"
    CONCENTRATION = "CONCENTRATION"
    SEQUENCE_FRAGILITY = "SEQUENCE_FRAGILITY"
    CONSISTENCY_RULE_FAILURE = "CONSISTENCY_RULE_FAILURE"
    LONG_RECOVERY_TIME = "LONG_RECOVERY_TIME"
    PAYOUT_FRAGILITY = "PAYOUT_FRAGILITY"
    REDUNDANT_PORTFOLIO_ROLE = "REDUNDANT_PORTFOLIO_ROLE"
    NULL_INDISTINGUISHABLE = "NULL_INDISTINGUISHABLE"
    INSUFFICIENT_STATISTICAL_POWER = "INSUFFICIENT_STATISTICAL_POWER"
    EXECUTION_INFEASIBILITY = "EXECUTION_INFEASIBILITY"


@dataclass(frozen=True, slots=True)
class FeatureDependency:
    name: str
    market: str
    timeframe: str
    availability: str = "CLOSED_OR_PAST_ONLY"
    lag_bars: int = 0
    source_version: str = "canonical_feature_bundle_v3"

    def __post_init__(self) -> None:
        for name in ("name", "market", "timeframe", "availability", "source_version"):
            if not str(getattr(self, name)).strip():
                raise ValueError(f"{name} must be non-empty")
        if self.availability != "CLOSED_OR_PAST_ONLY":
            raise ValueError("feature dependency must be closed or past-only")
        if self.lag_bars < 0:
            raise ValueError("feature lag cannot be negative")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


_SIGNATURES: Mapping[
    ComponentKind, tuple[tuple[tuple[PortType, ...], PortType], ...]
] = {
    ComponentKind.CONTEXT: (((PortType.FEATURE_SCALAR,), PortType.MARKET_STATE),),
    ComponentKind.TRIGGER: (
        ((PortType.FEATURE_SCALAR,), PortType.EVENT_TRIGGER),
        ((PortType.FEATURE_SCALAR, PortType.MARKET_STATE), PortType.EVENT_TRIGGER),
    ),
    ComponentKind.DIRECTION: (
        ((PortType.EVENT_TRIGGER,), PortType.DIRECTION),
        ((PortType.EVENT_TRIGGER, PortType.FEATURE_SCALAR), PortType.DIRECTION),
    ),
    ComponentKind.ELIGIBILITY: (
        ((PortType.MARKET_STATE,), PortType.ELIGIBILITY),
        ((PortType.ACCOUNT_STATE,), PortType.ELIGIBILITY),
    ),
    ComponentKind.SIZING: (
        ((PortType.ACCOUNT_STATE,), PortType.POSITION_SIZE),
        ((PortType.ACCOUNT_STATE, PortType.MARKET_STATE), PortType.POSITION_SIZE),
    ),
    ComponentKind.STOP: (
        (
            (PortType.EVENT_TRIGGER, PortType.DIRECTION, PortType.FEATURE_SCALAR),
            PortType.EXIT_POLICY,
        ),
    ),
    ComponentKind.TARGET: (
        (
            (PortType.EVENT_TRIGGER, PortType.DIRECTION, PortType.FEATURE_SCALAR),
            PortType.EXIT_POLICY,
        ),
    ),
    ComponentKind.TIME_EXIT: (
        ((PortType.EVENT_TRIGGER,), PortType.EXIT_POLICY),
    ),
    ComponentKind.TRADE_VETO: (
        ((PortType.MARKET_STATE,), PortType.ELIGIBILITY),
        ((PortType.ACCOUNT_STATE,), PortType.ELIGIBILITY),
    ),
    ComponentKind.PORTFOLIO_ROLE: (
        ((PortType.EVENT_TRIGGER,), PortType.PORTFOLIO_ROLE),
    ),
    ComponentKind.ACCOUNT_STATE_RESPONSE: (
        ((PortType.ACCOUNT_STATE,), PortType.ACCOUNT_ACTION),
    ),
}


Scalar = str | int | float | bool


@dataclass(frozen=True, slots=True)
class ComponentSpec:
    component_id: str
    kind: ComponentKind
    input_types: tuple[PortType, ...]
    output_type: PortType
    mechanism_family: str
    economic_hypothesis: str
    market_scope: tuple[str, ...]
    timeframe: str
    session_scope: str
    role: EconomicRole
    feature_dependencies: tuple[FeatureDependency, ...] = ()
    parameters: tuple[tuple[str, Scalar], ...] = ()
    parent_component_ids: tuple[str, ...] = ()
    failure_target: FailureDimension | None = None
    source_campaign: str = "ECONOMIC_EVOLUTION_ENGINE_V1"
    version: int = 1
    inherited_status: None = None

    def __post_init__(self) -> None:
        for name in (
            "component_id",
            "mechanism_family",
            "economic_hypothesis",
            "timeframe",
            "session_scope",
            "source_campaign",
        ):
            if not str(getattr(self, name)).strip():
                raise ValueError(f"{name} must be non-empty")
        if not self.market_scope or any(not str(value).strip() for value in self.market_scope):
            raise ValueError("market_scope must be non-empty")
        if len(set(self.market_scope)) != len(self.market_scope):
            raise ValueError("market_scope must be unique")
        if len(set(self.parent_component_ids)) != len(self.parent_component_ids):
            raise ValueError("parent_component_ids must be unique")
        if self.version < 1:
            raise ValueError("version must be positive")
        if (self.input_types, self.output_type) not in _SIGNATURES[self.kind]:
            raise ValueError(
                f"invalid interface for {self.kind}: {self.input_types} -> {self.output_type}"
            )
        names = [name for name, _ in self.parameters]
        if len(names) != len(set(names)):
            raise ValueError("component parameter names must be unique")
        if len(names) > 4:
            raise ValueError("a component may expose at most four bounded parameters")
        for name, value in self.parameters:
            if not name:
                raise ValueError("parameter name cannot be empty")
            if isinstance(value, float) and not math.isfinite(value):
                raise ValueError("component parameters must be finite")

    @property
    def structural_fingerprint(self) -> str:
        return stable_hash(self.structural_payload())

    @property
    def semantic_fingerprint(self) -> str:
        payload = self.structural_payload()
        payload.pop("role", None)
        payload.pop("execution_market", None)
        return stable_hash(payload)

    def structural_payload(self) -> dict[str, Any]:
        return {
            "schema": SCHEMA_VERSION,
            "kind": self.kind.value,
            "input_types": [value.value for value in self.input_types],
            "output_type": self.output_type.value,
            "mechanism_family": self.mechanism_family,
            "economic_hypothesis": self.economic_hypothesis,
            "market_scope": sorted(self.market_scope),
            "timeframe": self.timeframe,
            "session_scope": self.session_scope,
            "role": self.role.value,
            "feature_dependencies": [
                row.to_dict()
                for row in sorted(
                    self.feature_dependencies,
                    key=lambda value: (
                        value.market,
                        value.timeframe,
                        value.name,
                        value.lag_bars,
                    ),
                )
            ],
            "parameters": [[name, value] for name, value in sorted(self.parameters)],
            "failure_target": self.failure_target.value if self.failure_target else None,
            "version": self.version,
        }

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["kind"] = self.kind.value
        value["input_types"] = [item.value for item in self.input_types]
        value["output_type"] = self.output_type.value
        value["role"] = self.role.value
        value["feature_dependencies"] = [
            item.to_dict() for item in self.feature_dependencies
        ]
        value["parameters"] = [[name, item] for name, item in self.parameters]
        value["parent_component_ids"] = list(self.parent_component_ids)
        value["market_scope"] = list(self.market_scope)
        value["failure_target"] = (
            self.failure_target.value if self.failure_target else None
        )
        value["structural_fingerprint"] = self.structural_fingerprint
        value["semantic_fingerprint"] = self.semantic_fingerprint
        return value


@dataclass(frozen=True, slots=True)
class SleeveSpec:
    sleeve_id: str
    component_ids: tuple[str, ...]
    market: str
    execution_market: str
    timeframe: str
    session_code: int
    trigger_feature: str
    trigger_operator: str
    trigger_quantile: float
    context_feature: str | None
    context_operator: str | None
    context_quantile: float | None
    side: int
    holding_bars: int
    exit_style: str
    role: EconomicRole
    source_campaign: str
    lineage_id: str
    version: int = 1
    inherited_status: None = None

    def __post_init__(self) -> None:
        for name in (
            "sleeve_id",
            "market",
            "execution_market",
            "timeframe",
            "trigger_feature",
            "trigger_operator",
            "exit_style",
            "source_campaign",
            "lineage_id",
        ):
            if not str(getattr(self, name)).strip():
                raise ValueError(f"{name} must be non-empty")
        if not self.component_ids or len(set(self.component_ids)) != len(self.component_ids):
            raise ValueError("sleeve component IDs must be non-empty and unique")
        if self.trigger_operator not in {"GT", "GE", "LT", "LE"}:
            raise ValueError("unsupported trigger operator")
        if not 0.0 < self.trigger_quantile < 1.0:
            raise ValueError("trigger quantile must be in (0,1)")
        context = (
            self.context_feature,
            self.context_operator,
            self.context_quantile,
        )
        if any(value is not None for value in context) and not all(
            value is not None for value in context
        ):
            raise ValueError("context fields must be supplied together")
        if self.context_operator is not None and self.context_operator not in {
            "GT",
            "GE",
            "LT",
            "LE",
        }:
            raise ValueError("unsupported context operator")
        if self.context_quantile is not None and not 0.0 < self.context_quantile < 1.0:
            raise ValueError("context quantile must be in (0,1)")
        if self.side not in {-1, 1}:
            raise ValueError("side must be -1 or 1")
        if self.holding_bars not in {5, 15, 30, 60}:
            raise ValueError("holding_bars must use a canonical frozen horizon")
        if self.session_code not in {-1, 0, 1, 2}:
            raise ValueError("session code must be all/open/middle/late")
        if self.version < 1:
            raise ValueError("version must be positive")

    @property
    def structural_fingerprint(self) -> str:
        return stable_hash(self.structural_payload(include_role=True))

    @property
    def behavioral_fingerprint(self) -> str:
        return stable_hash(self.structural_payload(include_role=False))

    def structural_payload(self, *, include_role: bool) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "schema": SCHEMA_VERSION,
            "market": self.market,
            "timeframe": self.timeframe,
            "session_code": self.session_code,
            "trigger_feature": self.trigger_feature,
            "trigger_operator": self.trigger_operator,
            "trigger_quantile": float(self.trigger_quantile).hex(),
            "context_feature": self.context_feature,
            "context_operator": self.context_operator,
            "context_quantile": (
                None
                if self.context_quantile is None
                else float(self.context_quantile).hex()
            ),
            "side": self.side,
            "holding_bars": self.holding_bars,
            "exit_style": self.exit_style,
            "version": self.version,
        }
        if include_role:
            payload["role"] = self.role.value
            payload["execution_market"] = self.execution_market
        return payload

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["component_ids"] = list(self.component_ids)
        value["role"] = self.role.value
        value["structural_fingerprint"] = self.structural_fingerprint
        value["behavioral_fingerprint"] = self.behavioral_fingerprint
        return value


@dataclass(frozen=True, slots=True)
class AccountPolicyGenome:
    policy_id: str
    sleeve_ids: tuple[str, ...]
    allocation_units: tuple[int, ...]
    maximum_simultaneous_positions: int
    maximum_mini_equivalent: int
    conflict_policy: str
    daily_risk_budget: float
    daily_profit_lock: float
    low_mll_buffer: float
    critical_mll_buffer: float
    loss_streak_throttle_after: int
    mode: str
    source_campaign: str
    parent_policy_ids: tuple[str, ...] = ()
    mutation_target: FailureDimension | None = None
    version: int = 1
    inherited_status: None = None

    def __post_init__(self) -> None:
        if not self.policy_id or not self.source_campaign:
            raise ValueError("policy and campaign IDs are required")
        if not 1 <= len(self.sleeve_ids) <= 4:
            raise ValueError("an account policy must contain one to four sleeves")
        if len(set(self.sleeve_ids)) != len(self.sleeve_ids):
            raise ValueError("account policy sleeves must be unique")
        if len(self.allocation_units) != len(self.sleeve_ids):
            raise ValueError("allocation units must match sleeves")
        if any(value not in {1, 2, 3, 4} for value in self.allocation_units):
            raise ValueError("allocation units must use the bounded discrete set")
        if not 1 <= self.maximum_simultaneous_positions <= len(self.sleeve_ids):
            raise ValueError("maximum simultaneous positions is inconsistent")
        if not 1 <= self.maximum_mini_equivalent <= 15:
            raise ValueError("maximum mini equivalent must be in [1,15]")
        if self.conflict_policy not in {
            "FIXED_PRIORITY",
            "LOWEST_CORRELATION_FIRST",
            "LOWEST_MLL_USAGE_FIRST",
        }:
            raise ValueError("unsupported conflict policy")
        if not 0.0 < self.daily_risk_budget <= 3_000.0:
            raise ValueError("daily risk budget must be in (0,3000]")
        if not 0.0 < self.daily_profit_lock <= 9_000.0:
            raise ValueError("daily profit lock must be in (0,9000]")
        if not 0.0 < self.critical_mll_buffer <= self.low_mll_buffer <= 4_500.0:
            raise ValueError("MLL buffer thresholds are invalid")
        if self.loss_streak_throttle_after not in {2, 3, 4, 5}:
            raise ValueError("loss streak throttle must use the bounded set")
        if self.mode not in {"COMBINE_RESEARCH", "FUNDED_RESEARCH", "PAYOUT_RESEARCH"}:
            raise ValueError("unsupported account mode")
        if len(set(self.parent_policy_ids)) != len(self.parent_policy_ids):
            raise ValueError("parent policy IDs must be unique")
        if self.version < 1:
            raise ValueError("version must be positive")

    @property
    def structural_fingerprint(self) -> str:
        return stable_hash(self.structural_payload())

    def structural_payload(self) -> dict[str, Any]:
        return {
            "schema": SCHEMA_VERSION,
            "sleeve_ids": list(self.sleeve_ids),
            "allocation_units": list(self.allocation_units),
            "maximum_simultaneous_positions": self.maximum_simultaneous_positions,
            "maximum_mini_equivalent": self.maximum_mini_equivalent,
            "conflict_policy": self.conflict_policy,
            "daily_risk_budget": float(self.daily_risk_budget).hex(),
            "daily_profit_lock": float(self.daily_profit_lock).hex(),
            "low_mll_buffer": float(self.low_mll_buffer).hex(),
            "critical_mll_buffer": float(self.critical_mll_buffer).hex(),
            "loss_streak_throttle_after": self.loss_streak_throttle_after,
            "mode": self.mode,
            "version": self.version,
        }

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["sleeve_ids"] = list(self.sleeve_ids)
        value["allocation_units"] = list(self.allocation_units)
        value["parent_policy_ids"] = list(self.parent_policy_ids)
        value["mutation_target"] = (
            self.mutation_target.value if self.mutation_target else None
        )
        value["structural_fingerprint"] = self.structural_fingerprint
        return value


@dataclass(frozen=True, slots=True)
class FailureVector:
    policy_id: str
    scores: tuple[tuple[FailureDimension, float], ...]
    evidence_hash: str
    evaluated_on_identical_parent_child_starts: bool

    def __post_init__(self) -> None:
        if not self.policy_id or not self.evidence_hash:
            raise ValueError("failure vector requires policy and evidence IDs")
        dimensions = [dimension for dimension, _ in self.scores]
        if not dimensions or len(dimensions) != len(set(dimensions)):
            raise ValueError("failure dimensions must be non-empty and unique")
        for _, value in self.scores:
            if not math.isfinite(value) or not 0.0 <= value <= 1.0:
                raise ValueError("failure scores must be finite in [0,1]")

    @property
    def dominant(self) -> FailureDimension:
        return max(self.scores, key=lambda row: (row[1], row[0].value))[0]

    def score(self, dimension: FailureDimension) -> float:
        return dict(self.scores).get(dimension, 0.0)

    def to_dict(self) -> dict[str, Any]:
        return {
            "policy_id": self.policy_id,
            "scores": {key.value: value for key, value in self.scores},
            "dominant": self.dominant.value,
            "evidence_hash": self.evidence_hash,
            "evaluated_on_identical_parent_child_starts": (
                self.evaluated_on_identical_parent_child_starts
            ),
        }


def stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
    ).hexdigest()


def deterministic_id(prefix: str, payload: Any, *, length: int = 24) -> str:
    if not prefix or length < 8:
        raise ValueError("deterministic ID prefix/length is invalid")
    return f"{prefix}_{stable_hash(payload)[:length]}"


def reject_duplicate_fingerprints(
    values: Sequence[ComponentSpec | SleeveSpec | AccountPolicyGenome],
    *,
    semantic: bool = False,
) -> tuple[tuple[ComponentSpec | SleeveSpec | AccountPolicyGenome, ...], tuple[int, ...]]:
    retained: list[ComponentSpec | SleeveSpec | AccountPolicyGenome] = []
    rejected: list[int] = []
    seen: set[str] = set()
    for index, value in enumerate(values):
        fingerprint = (
            value.semantic_fingerprint
            if semantic and isinstance(value, ComponentSpec)
            else value.behavioral_fingerprint
            if semantic and isinstance(value, SleeveSpec)
            else value.structural_fingerprint
        )
        if fingerprint in seen:
            rejected.append(index)
            continue
        seen.add(fingerprint)
        retained.append(value)
    return tuple(retained), tuple(rejected)


__all__ = [
    "AccountPolicyGenome",
    "ComponentKind",
    "ComponentSpec",
    "EconomicRole",
    "FailureDimension",
    "FailureVector",
    "FeatureDependency",
    "PortType",
    "SCHEMA_VERSION",
    "SleeveSpec",
    "deterministic_id",
    "reject_duplicate_fingerprints",
    "stable_hash",
]
