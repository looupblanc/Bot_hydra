from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass(frozen=True)
class ShadowSignal:
    strategy_id: str
    symbol: str
    side: int
    quantity: int
    decision_timestamp: datetime
    feature_timestamp: datetime
    reference_price: float

    @property
    def signal_id(self) -> str:
        raw = "|".join(
            [
                self.strategy_id,
                self.symbol,
                str(self.side),
                str(self.quantity),
                self.decision_timestamp.astimezone(timezone.utc).isoformat(),
            ]
        )
        return hashlib.sha256(raw.encode()).hexdigest()[:24]


class SignalBus:
    def __init__(self) -> None:
        self._seen: set[str] = set()

    def publish(self, signal: ShadowSignal) -> bool:
        if signal.side not in {-1, 1} or signal.quantity <= 0:
            raise ValueError("Shadow signal side/quantity is invalid.")
        if signal.signal_id in self._seen:
            return False
        self._seen.add(signal.signal_id)
        return True
