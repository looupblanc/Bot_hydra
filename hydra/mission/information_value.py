from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class MissionAction:
    action_id: str
    action_type: str
    uncertainty: float
    decision_change_probability: float
    potential_portfolio_value: float
    compute_cost: float
    data_cost: float
    contamination_cost: float
    redundancy_penalty: float
    implementation_risk: float
    rationale: str

    def information_value(self) -> float:
        numerator = self.uncertainty * self.decision_change_probability * max(self.potential_portfolio_value, 0.0)
        denominator = max(
            self.compute_cost + self.data_cost + self.contamination_cost + self.redundancy_penalty + self.implementation_risk,
            1e-9,
        )
        return float(numerator / denominator)

    def to_dict(self) -> dict[str, Any]:
        out = asdict(self)
        out["expected_decision_information_gain"] = self.information_value()
        return out


def rank_actions(actions: list[MissionAction]) -> list[MissionAction]:
    return sorted(actions, key=lambda item: item.information_value(), reverse=True)

