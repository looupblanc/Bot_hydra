from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime

from hydra.shadow.signal_bus import ShadowSignal


@dataclass(frozen=True)
class VirtualFill:
    signal_id: str
    timestamp: datetime
    symbol: str
    side: int
    quantity: int
    theoretical_price: float
    executable_proxy_price: float
    slippage: float
    cost: float
    status: str = "VIRTUAL_ONLY"

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


class VirtualExecution:
    """Pure simulation. Deliberately exposes no broker or order method."""

    def fill(
        self,
        signal: ShadowSignal,
        *,
        market_price: float,
        slippage_per_unit: float,
        round_turn_cost: float,
    ) -> VirtualFill:
        if market_price <= 0 or slippage_per_unit < 0 or round_turn_cost < 0:
            raise ValueError("Invalid virtual execution inputs.")
        executable = market_price + signal.side * slippage_per_unit
        return VirtualFill(
            signal_id=signal.signal_id,
            timestamp=signal.decision_timestamp,
            symbol=signal.symbol,
            side=signal.side,
            quantity=signal.quantity,
            theoretical_price=signal.reference_price,
            executable_proxy_price=executable,
            slippage=(executable - signal.reference_price) * signal.side,
            cost=round_turn_cost * signal.quantity,
        )
