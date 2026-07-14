from __future__ import annotations

import hashlib
import json
import math
import threading
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from typing import Any, Iterator, Mapping, Sequence

import hydra.account_policy.basket as basket_engine
from hydra.account_policy.router import (
    AccountDecisionState,
    EntryIntent,
    RoutingDecision,
)
from hydra.account_policy.schema import AccountPolicyKind


STATIC_RISK_FRONTIER_VERSION = "hydra_v73_static_integer_micro_risk_frontier_v1"
STATIC_CONFLICT_POLICY = "FIXED_PRIORITY_SAME_MARKET_EXCLUSIVE"


@dataclass(frozen=True, slots=True)
class StaticRiskTier:
    """One preregistered risk level expressed as an integer micro multiplier."""

    label: str
    multiplier: float
    micro_risk_units: int

    def __post_init__(self) -> None:
        if not self.label.endswith("x"):
            raise ValueError("static risk tier label must end in 'x'")
        if not math.isfinite(self.multiplier) or self.multiplier <= 0.0:
            raise ValueError("static risk multiplier must be positive and finite")
        if isinstance(self.micro_risk_units, bool) or self.micro_risk_units < 1:
            raise ValueError("static micro risk units must be a positive integer")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# The tuple order is part of the preregistration.  Four micro units is the
# existing 1.00x reference; no continuous or interpolated level is admissible.
FROZEN_RISK_TIERS: tuple[StaticRiskTier, ...] = (
    StaticRiskTier("0.75x", 0.75, 3),
    StaticRiskTier("1.00x", 1.00, 4),
    StaticRiskTier("1.25x", 1.25, 5),
    StaticRiskTier("1.50x", 1.50, 6),
)
STATIC_RISK_LEVELS: tuple[float, ...] = tuple(
    row.multiplier for row in FROZEN_RISK_TIERS
)
RISK_LEVEL_TO_MICRO_UNITS: Mapping[float, int] = {
    row.multiplier: row.micro_risk_units for row in FROZEN_RISK_TIERS
}
_TIER_BY_LABEL = {row.label: row for row in FROZEN_RISK_TIERS}
_TIER_BY_MULTIPLIER = {row.multiplier: row for row in FROZEN_RISK_TIERS}
_TIER_BY_UNITS = {row.micro_risk_units: row for row in FROZEN_RISK_TIERS}
_ROUTER_PATCH_LOCK = threading.RLock()
_MISSING = object()


def resolve_static_risk_tier(
    value: StaticRiskTier | str | float,
) -> StaticRiskTier:
    """Resolve only an exact frozen level; arbitrary risk values are rejected."""

    if isinstance(value, StaticRiskTier):
        tier = _TIER_BY_LABEL.get(value.label)
        if tier != value:
            raise ValueError("risk tier is not one of the frozen frontier levels")
        return tier
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in _TIER_BY_LABEL:
            return _TIER_BY_LABEL[normalized]
        if normalized.endswith("x"):
            normalized = normalized[:-1]
        try:
            value = float(normalized)
        except ValueError as exc:
            raise ValueError(f"unknown frozen static risk tier: {value!r}") from exc
    numeric = float(value)
    tier = _TIER_BY_MULTIPLIER.get(numeric)
    if tier is None:
        raise ValueError(f"risk level {numeric!r} is outside the frozen frontier")
    return tier


@dataclass(frozen=True, slots=True)
class StaticIntegerMicroPolicy:
    """Adapter consumed by the existing account replay engine.

    Sizing is constant for the whole episode.  Buffer zones and losing streaks
    are deliberately absent from the decision, while the account-level safety
    guards remain frozen from the source basket.
    """

    policy_id: str
    basket_policy_id: str
    component_priority: tuple[str, ...]
    risk_label: str
    micro_risk_units: int
    daily_loss_guard: float
    daily_profit_lock: float
    critical_buffer: float
    maximum_simultaneous_positions: int
    maximum_mini_equivalent: float
    conflict_policy: str = STATIC_CONFLICT_POLICY
    version: str = STATIC_RISK_FRONTIER_VERSION

    def __post_init__(self) -> None:
        if not self.policy_id.strip() or not self.basket_policy_id.strip():
            raise ValueError("static risk policy identity is required")
        if not self.component_priority:
            raise ValueError("static risk policy needs at least one component")
        if len(set(self.component_priority)) != len(self.component_priority):
            raise ValueError("static risk component priority must be unique")
        if any(not str(value).strip() for value in self.component_priority):
            raise ValueError("static risk component IDs must be non-empty")
        tier = resolve_static_risk_tier(self.risk_label)
        if self.micro_risk_units != tier.micro_risk_units:
            raise ValueError("risk label and integer micro units disagree")
        for name in (
            "daily_loss_guard",
            "daily_profit_lock",
            "maximum_mini_equivalent",
        ):
            value = float(getattr(self, name))
            if not math.isfinite(value) or value <= 0.0:
                raise ValueError(f"{name} must be positive and finite")
        if not math.isfinite(float(self.critical_buffer)) or self.critical_buffer < 0.0:
            raise ValueError("critical_buffer must be non-negative and finite")
        if (
            isinstance(self.maximum_simultaneous_positions, bool)
            or self.maximum_simultaneous_positions < 1
        ):
            raise ValueError("maximum simultaneous positions must be positive")
        if self.conflict_policy != STATIC_CONFLICT_POLICY:
            raise ValueError("static frontier requires frozen same-market exclusion")
        if self.version != STATIC_RISK_FRONTIER_VERSION:
            raise ValueError("static frontier policy version drift")

    @property
    def controller_id(self) -> str:
        return self.policy_id

    @property
    def component_ids(self) -> tuple[str, ...]:
        return self.component_priority

    @property
    def risk_multiplier(self) -> float:
        return resolve_static_risk_tier(self.risk_label).multiplier

    @property
    def kind(self) -> AccountPolicyKind:
        return AccountPolicyKind.STATIC_BASKET

    @property
    def dynamic_buffer_sizing(self) -> bool:
        return False

    @property
    def loss_streak_sizing(self) -> bool:
        return False

    @property
    def structural_fingerprint(self) -> str:
        payload = json.dumps(
            self._structural_payload(), sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    def _structural_payload(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "basket_policy_id": self.basket_policy_id,
            "component_priority": list(self.component_priority),
            "risk_label": self.risk_label,
            "micro_risk_units": self.micro_risk_units,
            "daily_loss_guard": float(self.daily_loss_guard).hex(),
            "daily_profit_lock": float(self.daily_profit_lock).hex(),
            "critical_buffer": float(self.critical_buffer).hex(),
            "maximum_simultaneous_positions": self.maximum_simultaneous_positions,
            "maximum_mini_equivalent": float(self.maximum_mini_equivalent).hex(),
            "conflict_policy": self.conflict_policy,
            "dynamic_buffer_sizing": False,
            "loss_streak_sizing": False,
        }

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        row["component_priority"] = list(self.component_priority)
        row["risk_multiplier"] = self.risk_multiplier
        row["kind"] = self.kind.value
        row["dynamic_buffer_sizing"] = False
        row["loss_streak_sizing"] = False
        row["structural_fingerprint"] = self.structural_fingerprint
        return row


def adapt_static_risk_policy(
    source_policy: object | Mapping[str, Any],
    risk_level: StaticRiskTier | str | float,
    *,
    policy_id: str | None = None,
    basket_policy_id: str | None = None,
) -> StaticIntegerMicroPolicy:
    """Freeze a campaign basket into one frontier tier without changing guards."""

    tier = resolve_static_risk_tier(risk_level)
    source_id = str(_source_value(source_policy, "policy_id", "controller_id"))
    components = tuple(
        str(value)
        for value in _source_component_priority(source_policy)
    )
    source_basket_id = _source_value(
        source_policy,
        "basket_policy_id",
        default=f"{source_id}::STATIC_BASKET",
    )
    conflict = str(
        _source_value(
            source_policy,
            "conflict_policy",
            default=STATIC_CONFLICT_POLICY,
        )
    )
    suffix = tier.label.upper().replace(".", "_")
    return StaticIntegerMicroPolicy(
        policy_id=policy_id or f"{source_id}::STATIC_RISK_{suffix}",
        basket_policy_id=basket_policy_id or str(source_basket_id),
        component_priority=components,
        risk_label=tier.label,
        micro_risk_units=tier.micro_risk_units,
        daily_loss_guard=float(
            _source_value(source_policy, "daily_loss_guard", "daily_loss_limit")
        ),
        daily_profit_lock=float(_source_value(source_policy, "daily_profit_lock")),
        critical_buffer=float(
            _source_value(
                source_policy,
                "critical_buffer",
                "critical_buffer_threshold",
            )
        ),
        maximum_simultaneous_positions=int(
            _source_value(source_policy, "maximum_simultaneous_positions")
        ),
        maximum_mini_equivalent=float(
            _source_value(source_policy, "maximum_mini_equivalent")
        ),
        conflict_policy=conflict,
    )


def route_static_integer_micro_entry(
    intent: EntryIntent,
    state: AccountDecisionState,
    *,
    policy: StaticIntegerMicroPolicy,
) -> RoutingDecision:
    """Route one immutable ledger event with constant integer-micro sizing."""

    if intent.component_id not in policy.component_priority:
        return _blocked(policy, "COMPONENT_NOT_IN_FROZEN_MEMBERSHIP")
    if intent.base_quantity < 1 or intent.base_mini_equivalent <= 0.0:
        return _blocked(policy, "INVALID_BASE_RISK_UNIT")
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

    # This is the complete sizing rule.  Neither MLL buffer zones nor loss
    # streak state can alter the frozen integer tier.
    quantity = int(intent.base_quantity * policy.micro_risk_units)
    mini = float(intent.base_mini_equivalent * policy.micro_risk_units)
    current = sum(float(row.mini_equivalent) for row in state.open_exposures)
    if current + mini > policy.maximum_mini_equivalent + 1e-12:
        return _blocked(policy, "SHARED_CONTRACT_LIMIT")
    return RoutingDecision(
        allow=True,
        quantity=quantity,
        mini_equivalent=mini,
        reason=f"STATIC_INTEGER_MICRO_UNITS_{policy.micro_risk_units}",
        policy_id=policy.controller_id,
    )


def equal_risk_integer_units(
    component_ids: Sequence[str],
    *,
    risk_level: StaticRiskTier | str | float = "1.00x",
    per_component_unit_caps: Mapping[str, int] | None = None,
    base_mini_equivalents: Mapping[str, float] | None = None,
    maximum_mini_equivalent: float | None = None,
    maximum_simultaneous_positions: int = 1,
) -> dict[str, int]:
    """Assign the same integer tier to each component, conservatively capped.

    When mini equivalences are supplied, each component is capped to an equal
    share of the shared contract limit at maximum concurrency.  Therefore any
    combination of that many returned allocations remains within the frozen
    mini-equivalent cap.  A zero means the component is not safely executable.
    """

    ordered = tuple(str(value) for value in component_ids)
    if not ordered or any(not value.strip() for value in ordered):
        raise ValueError("equal-risk allocation needs non-empty component IDs")
    if len(set(ordered)) != len(ordered):
        raise ValueError("equal-risk component IDs must be unique")
    if (
        isinstance(maximum_simultaneous_positions, bool)
        or maximum_simultaneous_positions < 1
    ):
        raise ValueError("maximum_simultaneous_positions must be positive")
    if (base_mini_equivalents is None) != (maximum_mini_equivalent is None):
        raise ValueError(
            "base mini equivalences and the shared mini cap must be supplied together"
        )
    tier = resolve_static_risk_tier(risk_level)
    safe_concurrency = min(maximum_simultaneous_positions, len(ordered))
    mini_share: float | None = None
    if maximum_mini_equivalent is not None:
        cap = float(maximum_mini_equivalent)
        if not math.isfinite(cap) or cap <= 0.0:
            raise ValueError("maximum_mini_equivalent must be positive and finite")
        mini_share = cap / safe_concurrency

    output: dict[str, int] = {}
    for component_id in ordered:
        unit_cap = max(_TIER_BY_UNITS)
        if per_component_unit_caps is not None:
            if component_id not in per_component_unit_caps:
                raise ValueError(f"missing integer unit cap for {component_id}")
            raw_cap = per_component_unit_caps[component_id]
            if isinstance(raw_cap, bool) or int(raw_cap) != raw_cap or raw_cap < 0:
                raise ValueError(
                    "per-component unit caps must be non-negative integers"
                )
            unit_cap = min(unit_cap, int(raw_cap))
        if mini_share is not None:
            assert base_mini_equivalents is not None
            if component_id not in base_mini_equivalents:
                raise ValueError(f"missing mini equivalence for {component_id}")
            base_mini = float(base_mini_equivalents[component_id])
            if not math.isfinite(base_mini) or base_mini <= 0.0:
                raise ValueError("base mini equivalences must be positive and finite")
            mini_unit_cap = math.floor((mini_share + 1e-12) / base_mini)
            unit_cap = min(unit_cap, mini_unit_cap)
        output[component_id] = min(tier.micro_risk_units, max(0, unit_cap))
    return output


@contextmanager
def static_risk_router_context() -> Iterator[None]:
    """Install the adapter for existing account replay calls, then restore it."""

    def route_frontier(
        intent: EntryIntent,
        state: AccountDecisionState,
        *,
        policy: StaticIntegerMicroPolicy,
    ) -> RoutingDecision:
        return route_static_integer_micro_entry(intent, state, policy=policy)

    with _ROUTER_PATCH_LOCK:
        prior = basket_engine.route_entry
        basket_engine.route_entry = route_frontier  # type: ignore[assignment]
        try:
            yield
        finally:
            basket_engine.route_entry = prior


def _source_component_priority(
    source: object | Mapping[str, Any],
) -> Sequence[Any]:
    priority = _source_value(source, "component_priority", default=())
    if priority:
        return priority
    return _source_value(source, "component_ids")


def _source_value(
    source: object | Mapping[str, Any],
    *names: str,
    default: object = _MISSING,
) -> Any:
    for name in names:
        if isinstance(source, Mapping) and name in source:
            return source[name]
        if hasattr(source, name):
            return getattr(source, name)
    if default is not _MISSING:
        return default
    joined = " or ".join(names)
    raise ValueError(f"source policy is missing {joined}")


def _blocked(policy: StaticIntegerMicroPolicy, reason: str) -> RoutingDecision:
    return RoutingDecision(False, 0, 0.0, reason, policy.controller_id)


__all__ = [
    "FROZEN_RISK_TIERS",
    "RISK_LEVEL_TO_MICRO_UNITS",
    "STATIC_CONFLICT_POLICY",
    "STATIC_RISK_FRONTIER_VERSION",
    "STATIC_RISK_LEVELS",
    "StaticIntegerMicroPolicy",
    "StaticRiskTier",
    "adapt_static_risk_policy",
    "equal_risk_integer_units",
    "resolve_static_risk_tier",
    "route_static_integer_micro_entry",
    "static_risk_router_context",
]
