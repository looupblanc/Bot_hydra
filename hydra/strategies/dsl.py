from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class StrategyCandidate:
    candidate_id: str
    family: str
    symbol: str
    timeframe: str
    parameters: dict[str, Any]
    entry_logic: str
    exit_logic: str
    risk_parameters: dict[str, Any]
    parent_candidate_id: str | None = None
    mutation_type: str | None = None

    def sized(self, risk_scale: float) -> "StrategyCandidate":
        risk = dict(self.risk_parameters)
        risk["risk_scale"] = max(0.05, risk.get("risk_scale", 1.0) * risk_scale)
        return StrategyCandidate(
            candidate_id=f"{self.candidate_id}_rc{risk['risk_scale']:.2f}",
            family=self.family,
            symbol=self.symbol,
            timeframe=self.timeframe,
            parameters=dict(self.parameters),
            entry_logic=self.entry_logic,
            exit_logic=self.exit_logic,
            risk_parameters=risk,
            parent_candidate_id=self.candidate_id,
            mutation_type="risk_compression",
        )
