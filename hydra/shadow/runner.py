from __future__ import annotations

from datetime import datetime
from typing import Any

from hydra.shadow.risk_guard import ShadowRiskGuard
from hydra.shadow.signal_bus import ShadowSignal, SignalBus
from hydra.shadow.specification import ShadowSpecification
from hydra.shadow.virtual_execution import VirtualExecution


class ShadowRunner:
    """Fail-closed zero-order-capability shadow runner."""

    def __init__(self, specification: ShadowSpecification) -> None:
        specification.validate()
        self.specification = specification
        self.signal_bus = SignalBus()
        self.risk = ShadowRiskGuard(specification)
        self.execution = VirtualExecution()
        self.events: list[dict[str, Any]] = []

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
        event = {"status": "VIRTUAL_FILLED", "reason": risk.reason, "fill": fill.to_dict()}
        self.events.append(event)
        return event
