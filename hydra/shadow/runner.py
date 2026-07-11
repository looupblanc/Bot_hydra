from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from hydra.shadow.prior_trade_guard import (
    PriorTradeGuard,
    PriorTradeGuardError,
    PriorTradeGuardSpecification,
)
from hydra.shadow.risk_guard import ShadowRiskGuard
from hydra.shadow.signal_bus import ShadowSignal, SignalBus
from hydra.shadow.specification import ShadowSpecification
from hydra.shadow.virtual_execution import VirtualExecution


class ShadowRunner:
    """Fail-closed zero-order-capability shadow runner."""

    def __init__(
        self,
        specification: ShadowSpecification,
        *,
        prior_trade_guard_state_path: str | Path | None = None,
        initialize_prior_trade_guard_genesis: bool = False,
    ) -> None:
        specification.validate()
        self.specification = specification
        self.signal_bus = SignalBus()
        self.risk = ShadowRiskGuard(specification)
        self.execution = VirtualExecution()
        self.events: list[dict[str, Any]] = []
        self._open_virtual_fills: dict[str, datetime] = {}
        self._completed_virtual_fills: set[str] = set()
        self._prior_trade_guard_state_path = (
            Path(prior_trade_guard_state_path)
            if prior_trade_guard_state_path is not None
            else None
        )
        self._prior_trade_guard_audit_path = (
            Path(f"{self._prior_trade_guard_state_path}.audit.jsonl")
            if self._prior_trade_guard_state_path is not None
            else None
        )
        self.prior_trade_guard: PriorTradeGuard | None = None
        self.prior_trade_guard_reconciliation = "NOT_CONFIGURED"
        self._configure_prior_trade_guard(
            initialize_genesis=initialize_prior_trade_guard_genesis
        )

    def process(
        self,
        signal: ShadowSignal,
        *,
        now: datetime,
        latest_data_at: datetime,
        market_price: float,
        proposed_exposure: float,
        session_open: bool,
        simulated_mll: float,
        daily_pnl: float,
        slippage_per_unit: float,
        round_turn_cost: float,
    ) -> dict[str, Any]:
        guard_decision: dict[str, Any] | None = None
        if self.prior_trade_guard is not None:
            try:
                decision = self.prior_trade_guard.evaluate(
                    decision_at=signal.decision_timestamp
                )
                guard_decision = decision.to_dict()
            except PriorTradeGuardError as exc:
                guard_decision = {
                    "allowed": False,
                    "reason": "INVALID_PRIOR_TRADE_GUARD_STATE_FAIL_CLOSED",
                    "decision_at_utc": signal.decision_timestamp.astimezone(
                        timezone.utc
                    ).isoformat(),
                    "error": str(exc),
                }
            self._audit_guard_event(
                "PRIOR_TRADE_GUARD_DECISION",
                at=signal.decision_timestamp,
                signal_id=signal.signal_id,
                decision=guard_decision,
            )
            if not bool(guard_decision.get("allowed")):
                event = {
                    "status": "REJECTED",
                    "reason": str(guard_decision.get("reason")),
                    "signal_id": signal.signal_id,
                    "prior_trade_guard": guard_decision,
                }
                self.events.append(event)
                return event
        if not self.signal_bus.publish(signal):
            event = {"status": "REJECTED", "reason": "duplicate_signal", "signal_id": signal.signal_id}
            self.events.append(event)
            return event
        risk = self.risk.check(
            now=now,
            latest_data_at=latest_data_at,
            proposed_exposure=proposed_exposure,
            session_open=session_open,
            simulated_mll=simulated_mll,
            daily_pnl=daily_pnl,
        )
        if not risk.allowed:
            event = {"status": "REJECTED", "reason": risk.reason, "signal_id": signal.signal_id}
            self.events.append(event)
            return event
        fill = self.execution.fill(
            signal,
            market_price=market_price,
            slippage_per_unit=slippage_per_unit,
            round_turn_cost=round_turn_cost,
        )
        self._open_virtual_fills[signal.signal_id] = signal.decision_timestamp
        event = {
            "status": "VIRTUAL_FILLED",
            "reason": risk.reason,
            "fill": fill.to_dict(),
        }
        if guard_decision is not None:
            event["prior_trade_guard"] = guard_decision
        self.events.append(event)
        return event

    def record_completed_virtual_trade(
        self,
        *,
        signal_id: str,
        completed_at: datetime,
        net_pnl: float,
    ) -> dict[str, Any]:
        """Persist one completed virtual fill as evidence for later decisions only."""

        if self.prior_trade_guard is None:
            raise PriorTradeGuardError("No prior-trade guard is configured")
        if signal_id not in self._open_virtual_fills:
            if signal_id in self._completed_virtual_fills:
                raise PriorTradeGuardError(
                    f"Virtual fill was already completed: {signal_id}"
                )
            raise PriorTradeGuardError(
                f"Cannot update guard without a known open virtual fill: {signal_id}"
            )
        if completed_at.tzinfo is None:
            raise PriorTradeGuardError(
                "Virtual-trade completion timestamp must be timezone-aware"
            )
        fill_at = self._open_virtual_fills[signal_id]
        if completed_at.astimezone(timezone.utc) <= fill_at.astimezone(timezone.utc):
            raise PriorTradeGuardError(
                "Virtual trade must complete strictly after its virtual fill"
            )
        staged_guard = PriorTradeGuard.restore(
            self.prior_trade_guard.specification,
            self.prior_trade_guard.export_state(),
        )
        staged_guard.record_completed_trade(
            trade_id=signal_id,
            completed_at=completed_at,
            net_pnl=net_pnl,
        )
        self._persist_prior_trade_guard_state(staged_guard)
        self.prior_trade_guard = staged_guard
        self._open_virtual_fills.pop(signal_id)
        self._completed_virtual_fills.add(signal_id)
        event = {
            "status": "VIRTUAL_TRADE_COMPLETED",
            "signal_id": signal_id,
            "completed_at_utc": completed_at.astimezone(timezone.utc).isoformat(),
            "net_pnl": float(net_pnl),
            "prior_trade_guard_state_hash": self._current_guard_state_hash(),
        }
        self.events.append(event)
        self._audit_guard_event(
            "VIRTUAL_TRADE_COMPLETED_AND_GUARD_PERSISTED",
            at=completed_at,
            **event,
        )
        return event

    def _configure_prior_trade_guard(self, *, initialize_genesis: bool) -> None:
        guard_payload = self.specification.entry_rules.get("prior_trade_guard")
        if guard_payload is None:
            if self._prior_trade_guard_state_path is not None or initialize_genesis:
                raise PriorTradeGuardError(
                    "Prior-trade guard state was requested for an unguarded specification"
                )
            return
        if not isinstance(guard_payload, Mapping):
            raise PriorTradeGuardError("Prior-trade guard specification must be a mapping")
        guard_specification = PriorTradeGuardSpecification.from_mapping(guard_payload)
        if self._prior_trade_guard_state_path is None:
            self.prior_trade_guard = PriorTradeGuard(guard_specification)
            self.prior_trade_guard_reconciliation = "MISSING_STATE_PATH_FAIL_CLOSED"
            return
        state_path = self._prior_trade_guard_state_path
        if state_path.exists():
            try:
                state = json.loads(state_path.read_text(encoding="utf-8"))
                self.prior_trade_guard = PriorTradeGuard.restore(
                    guard_specification, state
                )
            except (OSError, json.JSONDecodeError, PriorTradeGuardError) as exc:
                self.prior_trade_guard = PriorTradeGuard(guard_specification)
                self.prior_trade_guard_reconciliation = (
                    "INVALID_RESTART_STATE_FAIL_CLOSED"
                )
                self._audit_guard_event(
                    "PRIOR_TRADE_GUARD_RESTORE_REJECTED",
                    at=datetime.now(timezone.utc),
                    error=str(exc),
                )
                return
            self.prior_trade_guard_reconciliation = "HASHED_STATE_RESTORED"
            self._audit_guard_event(
                "PRIOR_TRADE_GUARD_HASHED_STATE_RESTORED",
                at=datetime.now(timezone.utc),
                state_hash=self._current_guard_state_hash(),
            )
            return
        if not initialize_genesis:
            self.prior_trade_guard = PriorTradeGuard(guard_specification)
            self.prior_trade_guard_reconciliation = "MISSING_STATE_FAIL_CLOSED"
            self._audit_guard_event(
                "PRIOR_TRADE_GUARD_STATE_MISSING_FAIL_CLOSED",
                at=datetime.now(timezone.utc),
            )
            return
        self.prior_trade_guard = PriorTradeGuard.initialize_genesis(
            guard_specification
        )
        self._persist_prior_trade_guard_state()
        self.prior_trade_guard_reconciliation = "EXPLICIT_HASHED_GENESIS"
        self._audit_guard_event(
            "PRIOR_TRADE_GUARD_EXPLICIT_HASHED_GENESIS",
            at=datetime.now(timezone.utc),
            state_hash=self._current_guard_state_hash(),
        )

    def _persist_prior_trade_guard_state(
        self, guard: PriorTradeGuard | None = None
    ) -> None:
        state_guard = guard or self.prior_trade_guard
        if state_guard is None or self._prior_trade_guard_state_path is None:
            raise PriorTradeGuardError(
                "Guard persistence path is required before state updates"
            )
        state = state_guard.export_state()
        target = self._prior_trade_guard_state_path
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
        try:
            with temporary.open("w", encoding="utf-8") as handle:
                json.dump(state, handle, indent=2, sort_keys=True)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)

    def _current_guard_state_hash(self) -> str | None:
        if self.prior_trade_guard is None:
            return None
        try:
            return str(self.prior_trade_guard.export_state()["state_hash"])
        except PriorTradeGuardError:
            return None

    def _audit_guard_event(
        self, event_type: str, *, at: datetime, **details: Any
    ) -> None:
        if self._prior_trade_guard_audit_path is None:
            return
        target = self._prior_trade_guard_audit_path
        target.parent.mkdir(parents=True, exist_ok=True)
        previous_hash = "GENESIS"
        if target.exists():
            rows = [
                row
                for row in target.read_text(encoding="utf-8").splitlines()
                if row.strip()
            ]
            if rows:
                try:
                    decoded = [json.loads(row) for row in rows]
                    expected_previous = "GENESIS"
                    for prior_event in decoded:
                        supplied_event_hash = str(prior_event.pop("event_hash"))
                        encoded_prior = json.dumps(
                            prior_event,
                            sort_keys=True,
                            separators=(",", ":"),
                            ensure_ascii=True,
                            allow_nan=False,
                            default=str,
                        ).encode("utf-8")
                        if (
                            prior_event.get("previous_event_hash") != expected_previous
                            or hashlib.sha256(encoded_prior).hexdigest()
                            != supplied_event_hash
                        ):
                            raise ValueError("audit hash-chain drift")
                        expected_previous = supplied_event_hash
                    previous_hash = expected_previous
                except (json.JSONDecodeError, KeyError, TypeError, ValueError):
                    if self.prior_trade_guard is not None:
                        self.prior_trade_guard = PriorTradeGuard(
                            self.prior_trade_guard.specification
                        )
                    self.prior_trade_guard_reconciliation = (
                        "INVALID_AUDIT_CHAIN_FAIL_CLOSED"
                    )
                    return
        event = {
            "schema": "hydra_prior_trade_guard_audit_v1",
            "event": event_type,
            "event_at_utc": at.astimezone(timezone.utc).isoformat(),
            "strategy_id": self.specification.strategy_id,
            "configuration_hash": self.specification.configuration_hash,
            "previous_event_hash": previous_hash,
            "details": details,
        }
        encoded = json.dumps(
            event,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
            default=str,
        ).encode("utf-8")
        event["event_hash"] = hashlib.sha256(encoded).hexdigest()
        with target.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, sort_keys=True, default=str) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
