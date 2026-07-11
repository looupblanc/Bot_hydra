"""Deterministic prior-completed-trade activation guard for virtual shadow research.

The guard is deliberately signal-only.  It owns no market-data connection,
execution adapter, broker credential, or order surface.  A decision observes
only trades that were explicitly recorded as completed before the decision
timestamp; recording the current trade can therefore affect only later events.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping


PRIOR_TRADE_GUARD_VERSION = "prior_completed_trade_equity_guard_v1"


class PriorTradeGuardError(ValueError):
    """The frozen guard contract or restart state is invalid."""


def _canonical_hash(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _utc(value: datetime | str) -> datetime:
    parsed = datetime.fromisoformat(value) if isinstance(value, str) else value
    if parsed.tzinfo is None:
        raise PriorTradeGuardError("Trade-guard timestamps must be timezone-aware")
    return parsed.astimezone(timezone.utc)


@dataclass(frozen=True)
class PriorTradeGuardSpecification:
    trailing_window: int
    minimum_prior_observations: int
    warmup_completed_trades: int
    frozen_threshold: float
    activation_shift_periods: int = 1
    current_event_outcome_used: bool = False
    update_source: str = "COMPLETED_TRADES_ONLY"
    missing_state_policy: str = "FAIL_CLOSED"
    version: str = PRIOR_TRADE_GUARD_VERSION

    def validate(self) -> None:
        if self.version != PRIOR_TRADE_GUARD_VERSION:
            raise PriorTradeGuardError("Unknown prior-trade guard version")
        if self.trailing_window < 1:
            raise PriorTradeGuardError("trailing_window must be positive")
        if not 1 <= self.minimum_prior_observations <= self.trailing_window:
            raise PriorTradeGuardError("Invalid minimum prior-observation count")
        if self.warmup_completed_trades < self.minimum_prior_observations:
            raise PriorTradeGuardError("Warm-up cannot be shorter than minimum observations")
        if not math.isfinite(float(self.frozen_threshold)):
            raise PriorTradeGuardError("The frozen threshold must be finite")
        if self.activation_shift_periods < 1 or self.current_event_outcome_used:
            raise PriorTradeGuardError("The guard must use prior completed trades only")
        if self.update_source != "COMPLETED_TRADES_ONLY":
            raise PriorTradeGuardError("Only completed trades may update the guard")
        if self.missing_state_policy != "FAIL_CLOSED":
            raise PriorTradeGuardError("Missing guard state must fail closed")

    @property
    def specification_hash(self) -> str:
        self.validate()
        return _canonical_hash(asdict(self))

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["specification_hash"] = self.specification_hash
        return payload

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "PriorTradeGuardSpecification":
        payload = {key: item for key, item in dict(value).items() if key != "specification_hash"}
        specification = cls(**payload)
        supplied_hash = value.get("specification_hash")
        if supplied_hash is not None and str(supplied_hash) != specification.specification_hash:
            raise PriorTradeGuardError("Prior-trade guard specification hash drift")
        specification.validate()
        return specification


@dataclass(frozen=True)
class CompletedTrade:
    trade_id: str
    completed_at_utc: str
    net_pnl: float

    @classmethod
    def create(
        cls, *, trade_id: str, completed_at: datetime | str, net_pnl: float
    ) -> "CompletedTrade":
        if not str(trade_id).strip():
            raise PriorTradeGuardError("Completed trade ID is required")
        if not math.isfinite(float(net_pnl)):
            raise PriorTradeGuardError("Completed trade PnL must be finite")
        return cls(
            trade_id=str(trade_id),
            completed_at_utc=_utc(completed_at).isoformat(),
            net_pnl=float(net_pnl),
        )


@dataclass(frozen=True)
class PriorTradeGuardDecision:
    allowed: bool
    reason: str
    decision_at_utc: str
    prior_completed_trade_count: int
    prior_window_net_pnl: float | None
    frozen_threshold: float
    specification_hash: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class PriorTradeGuard:
    """Restart-safe state machine updated only after a trade is complete."""

    def __init__(
        self,
        specification: PriorTradeGuardSpecification,
        completed_trades: Iterable[CompletedTrade] = (),
        *,
        _reconciliation_verified: bool = False,
    ) -> None:
        specification.validate()
        self.specification = specification
        self._completed: list[CompletedTrade] = []
        self._reconciliation_verified = bool(_reconciliation_verified)
        for trade in completed_trades:
            self._append_completed(trade)

    @classmethod
    def initialize_genesis(
        cls, specification: PriorTradeGuardSpecification
    ) -> "PriorTradeGuard":
        """Start a fresh lineage through the same hashed-state reconciliation path."""

        body = {
            "schema": "hydra_prior_trade_guard_state_v1",
            "specification_hash": specification.specification_hash,
            "completed_trades": [],
        }
        body["state_hash"] = _canonical_hash(body)
        return cls.restore(specification, body)

    @property
    def completed_trade_count(self) -> int:
        return len(self._completed)

    def evaluate(self, *, decision_at: datetime | str) -> PriorTradeGuardDecision:
        decision_time = _utc(decision_at)
        if not self._reconciliation_verified:
            return self._decision(
                False,
                "MISSING_RECONCILED_GUARD_STATE_FAIL_CLOSED",
                decision_time,
                None,
            )
        if self._completed:
            last_completed_at = _utc(self._completed[-1].completed_at_utc)
            if last_completed_at > decision_time:
                raise PriorTradeGuardError(
                    "Guard state contains a trade completed after decision"
                )
            if last_completed_at == decision_time:
                return self._decision(
                    False,
                    "NON_PRIOR_COMPLETED_TRADE_FAIL_CLOSED",
                    decision_time,
                    None,
                )
        count = len(self._completed)
        if count < self.specification.warmup_completed_trades:
            return self._decision(
                True,
                "FROZEN_WARMUP_ADMIT",
                decision_time,
                None,
            )
        available = self._completed[-self.specification.trailing_window :]
        if len(available) < self.specification.minimum_prior_observations:
            return self._decision(
                False,
                "INSUFFICIENT_PRIOR_COMPLETED_TRADES_FAIL_CLOSED",
                decision_time,
                None,
            )
        prior_net = float(sum(row.net_pnl for row in available))
        return self._decision(
            prior_net >= self.specification.frozen_threshold,
            "PRIOR_WINDOW_ABOVE_FROZEN_THRESHOLD"
            if prior_net >= self.specification.frozen_threshold
            else "PRIOR_WINDOW_BELOW_FROZEN_THRESHOLD",
            decision_time,
            prior_net,
        )

    def record_completed_trade(
        self,
        *,
        trade_id: str,
        completed_at: datetime | str,
        net_pnl: float,
    ) -> None:
        if not self._reconciliation_verified:
            raise PriorTradeGuardError(
                "Cannot update prior-trade guard before state reconciliation"
            )
        trade = CompletedTrade.create(
            trade_id=trade_id,
            completed_at=completed_at,
            net_pnl=net_pnl,
        )
        self._append_completed(trade)

    def _append_completed(self, trade: CompletedTrade) -> None:
        if any(row.trade_id == trade.trade_id for row in self._completed):
            raise PriorTradeGuardError(f"Duplicate completed trade: {trade.trade_id}")
        if self._completed and _utc(trade.completed_at_utc) < _utc(
            self._completed[-1].completed_at_utc
        ):
            raise PriorTradeGuardError("Completed trades must be recorded chronologically")
        self._completed.append(trade)

    def export_state(self) -> dict[str, Any]:
        if not self._reconciliation_verified:
            raise PriorTradeGuardError(
                "Cannot export an unreconciled prior-trade guard state"
            )
        body = {
            "schema": "hydra_prior_trade_guard_state_v1",
            "specification_hash": self.specification.specification_hash,
            "completed_trades": [asdict(row) for row in self._completed],
        }
        body["state_hash"] = _canonical_hash(body)
        return body

    @classmethod
    def restore(
        cls,
        specification: PriorTradeGuardSpecification,
        state: Mapping[str, Any],
    ) -> "PriorTradeGuard":
        payload = dict(state)
        supplied_hash = str(payload.pop("state_hash", ""))
        if supplied_hash != _canonical_hash(payload):
            raise PriorTradeGuardError("Prior-trade guard restart-state hash drift")
        if payload.get("schema") != "hydra_prior_trade_guard_state_v1":
            raise PriorTradeGuardError("Unknown prior-trade guard restart-state schema")
        if payload.get("specification_hash") != specification.specification_hash:
            raise PriorTradeGuardError("Restart state belongs to another guard specification")
        trades = [
            CompletedTrade.create(
                trade_id=str(row.get("trade_id") or ""),
                completed_at=str(row.get("completed_at_utc") or ""),
                net_pnl=float(row.get("net_pnl")),
            )
            for row in payload.get("completed_trades") or []
        ]
        return cls(specification, trades, _reconciliation_verified=True)

    def _decision(
        self,
        allowed: bool,
        reason: str,
        decision_at: datetime,
        prior_net: float | None,
    ) -> PriorTradeGuardDecision:
        return PriorTradeGuardDecision(
            allowed=bool(allowed),
            reason=reason,
            decision_at_utc=decision_at.isoformat(),
            prior_completed_trade_count=len(self._completed),
            prior_window_net_pnl=prior_net,
            frozen_threshold=float(self.specification.frozen_threshold),
            specification_hash=self.specification.specification_hash,
        )
