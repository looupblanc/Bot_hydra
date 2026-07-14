from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any, Sequence

from hydra.account_policy.schema import ControllerPolicy


@dataclass(frozen=True, slots=True)
class EntryIntent:
    """Information available at the entry decision boundary.

    Future trade PnL, adverse excursion and exit price are deliberately absent.
    The simulator applies those outcomes only after a decision is frozen.
    """

    event_id: str
    component_id: str
    market: str
    side: int
    decision_ns: int
    session_day: int
    regime: str
    base_quantity: int
    base_mini_equivalent: float


@dataclass(frozen=True, slots=True)
class OpenExposure:
    component_id: str
    market: str
    side: int
    mini_equivalent: float
    exit_ns: int


@dataclass(frozen=True, slots=True)
class AccountDecisionState:
    balance: float
    mll_floor: float
    mll_buffer: float
    daily_realized_pnl: float
    consecutive_losing_days: int
    remaining_target: float
    open_exposures: tuple[OpenExposure, ...]
    shadow_component_outcomes: tuple[tuple[str, tuple[float, ...]], ...] = ()

    @property
    def shadow_outcome_map(self) -> dict[str, tuple[float, ...]]:
        """Completed, normalized virtual outcomes available at decision time."""

        return dict(self.shadow_component_outcomes)


@dataclass(frozen=True, slots=True)
class RoutingDecision:
    allow: bool
    quantity: int
    mini_equivalent: float
    reason: str
    policy_id: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "allow": self.allow,
            "quantity": self.quantity,
            "mini_equivalent": self.mini_equivalent,
            "reason": self.reason,
            "policy_id": self.policy_id,
        }


def route_entry(
    intent: EntryIntent,
    state: AccountDecisionState,
    *,
    policy: ControllerPolicy,
) -> RoutingDecision:
    if intent.component_id not in policy.component_priority:
        return _blocked(policy, "COMPONENT_NOT_IN_FROZEN_PRIORITY")
    if policy.allow_regimes and intent.regime not in policy.allow_regimes:
        return _blocked(policy, "REGIME_NOT_ALLOWED")
    if state.daily_realized_pnl <= -policy.daily_loss_limit:
        return _blocked(policy, "DAILY_LOSS_GUARD")
    if state.daily_realized_pnl >= policy.daily_profit_lock:
        return _blocked(policy, "DAILY_PROFIT_LOCK")
    if state.mll_buffer <= policy.critical_buffer_threshold:
        return _blocked(policy, "CRITICAL_MLL_BUFFER")
    if len(state.open_exposures) >= policy.maximum_simultaneous_positions:
        return _blocked(policy, "MAXIMUM_SIMULTANEOUS_POSITIONS")
    if policy.random_control_seed is not None:
        digest = hashlib.sha256(
            f"{policy.random_control_seed}:{intent.event_id}".encode("utf-8")
        ).digest()
        # A deterministic matched routing control.  It never sees the outcome.
        if int.from_bytes(digest[:8], "big") % 2:
            return _blocked(policy, "RANDOM_HASH_CONTROL_BLOCK")
    same_market = [
        exposure
        for exposure in state.open_exposures
        if exposure.market == intent.market and exposure.exit_ns > intent.decision_ns
    ]
    if same_market:
        return _blocked(policy, "SAME_MARKET_CONFLICT")
    scale = 1.0
    reasons: list[str] = []
    if state.mll_buffer <= policy.low_buffer_threshold:
        scale = min(scale, 0.5)
        reasons.append("LOW_MLL_BUFFER_DERISK")
    if state.consecutive_losing_days >= policy.loss_streak_derisk_after:
        scale = min(scale, 0.5)
        reasons.append("LOSS_STREAK_DERISK")
    quantity = int(intent.base_quantity * scale)
    if quantity < 1:
        return _blocked(policy, "INTEGER_SIZING_DERISK_TO_ZERO")
    mini = float(intent.base_mini_equivalent) * quantity / intent.base_quantity
    current_mini = sum(item.mini_equivalent for item in state.open_exposures)
    if current_mini + mini > policy.maximum_mini_equivalent + 1e-12:
        return _blocked(policy, "SHARED_CONTRACT_LIMIT")
    return RoutingDecision(
        allow=True,
        quantity=quantity,
        mini_equivalent=mini,
        reason="+".join(reasons) if reasons else "ALLOWED",
        policy_id=policy.controller_id,
    )


def static_route_entry(
    intent: EntryIntent,
    state: AccountDecisionState,
    *,
    policy_id: str,
    component_priority: Sequence[str],
    maximum_simultaneous_positions: int,
    maximum_mini_equivalent: int,
) -> RoutingDecision:
    if intent.component_id not in component_priority:
        return RoutingDecision(False, 0, 0.0, "COMPONENT_NOT_IN_BASKET", policy_id)
    if len(state.open_exposures) >= maximum_simultaneous_positions:
        return RoutingDecision(False, 0, 0.0, "MAXIMUM_SIMULTANEOUS_POSITIONS", policy_id)
    if any(
        exposure.market == intent.market and exposure.exit_ns > intent.decision_ns
        for exposure in state.open_exposures
    ):
        return RoutingDecision(False, 0, 0.0, "SAME_MARKET_CONFLICT", policy_id)
    current = sum(item.mini_equivalent for item in state.open_exposures)
    if current + intent.base_mini_equivalent > maximum_mini_equivalent + 1e-12:
        return RoutingDecision(False, 0, 0.0, "SHARED_CONTRACT_LIMIT", policy_id)
    return RoutingDecision(
        True,
        intent.base_quantity,
        intent.base_mini_equivalent,
        "ALLOWED",
        policy_id,
    )


def _blocked(policy: ControllerPolicy, reason: str) -> RoutingDecision:
    return RoutingDecision(False, 0, 0.0, reason, policy.controller_id)


__all__ = [
    "AccountDecisionState",
    "EntryIntent",
    "OpenExposure",
    "RoutingDecision",
    "route_entry",
    "static_route_entry",
]
