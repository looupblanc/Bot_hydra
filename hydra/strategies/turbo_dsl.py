from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum
import math


class ComparisonOperator(IntEnum):
    """Compact, serialisable comparison operators understood by the compiler."""

    GREATER_THAN = 1
    GREATER_EQUAL = 2
    LESS_THAN = -1
    LESS_EQUAL = -2


class StrategyRole(IntEnum):
    """Research role, not a validation status."""

    ALPHA = 1
    COMBINE_PASSER = 2
    XFA_PAYOUT = 3
    DEFENSIVE = 4
    PORTFOLIO_ONLY = 5
    HAZARD = 6


@dataclass(frozen=True, slots=True)
class StrategySpec:
    """Small immutable strategy description for the Turbo stage-1 executor.

    The specification deliberately contains only information available before a
    replay.  ``candidate_id`` and ``lineage_id`` are provenance fields and are
    excluded from structural fingerprints; renaming a strategy therefore cannot
    evade duplicate detection.
    """

    candidate_id: str
    lineage_id: str
    family: str
    market: str
    timeframe: str
    feature: str
    operator: ComparisonOperator
    threshold: float
    side: int
    holding_events: int
    point_value: float
    round_turn_cost: float
    role: StrategyRole = StrategyRole.ALPHA
    context_feature: str | None = None
    context_operator: ComparisonOperator | None = None
    context_threshold: float | None = None
    session_code: int = -1
    quantity: int = 1
    version: int = 1

    def __post_init__(self) -> None:
        for name in ("candidate_id", "lineage_id", "family", "market", "timeframe", "feature"):
            if not getattr(self, name).strip():
                raise ValueError(f"{name} must be non-empty")
        if not isinstance(self.operator, ComparisonOperator):
            raise TypeError("operator must be ComparisonOperator")
        if not isinstance(self.role, StrategyRole):
            raise TypeError("role must be StrategyRole")
        if self.side not in {-1, 1}:
            raise ValueError("side must be -1 or 1")
        if self.holding_events <= 0:
            raise ValueError("holding_events must be positive")
        if self.quantity <= 0:
            raise ValueError("quantity must be positive")
        if self.version <= 0:
            raise ValueError("version must be positive")
        if self.session_code < -1:
            raise ValueError("session_code must be -1 (all) or a non-negative code")
        for name in ("threshold", "point_value", "round_turn_cost"):
            if not math.isfinite(float(getattr(self, name))):
                raise ValueError(f"{name} must be finite")
        if self.point_value <= 0.0:
            raise ValueError("point_value must be positive")
        if self.round_turn_cost < 0.0:
            raise ValueError("round_turn_cost cannot be negative")

        context_values = (
            self.context_feature,
            self.context_operator,
            self.context_threshold,
        )
        if any(value is not None for value in context_values) and not all(
            value is not None for value in context_values
        ):
            raise ValueError(
                "context_feature, context_operator and context_threshold must be supplied together"
            )
        if self.context_operator is not None and not isinstance(
            self.context_operator, ComparisonOperator
        ):
            raise TypeError("context_operator must be ComparisonOperator")
        if self.context_threshold is not None and not math.isfinite(
            float(self.context_threshold)
        ):
            raise ValueError("context_threshold must be finite")


__all__ = ["ComparisonOperator", "StrategyRole", "StrategySpec"]
