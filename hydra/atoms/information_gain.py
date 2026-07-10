from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


INFO_GAIN_POLICY_VERSION = "edge_atom_information_gain_v1"


@dataclass(frozen=True)
class ExperimentCandidate:
    experiment_id: str
    uncertainty: float
    decision_change_probability: float
    portfolio_value: float
    compute_cost: float
    data_cost: float
    contamination_cost: float
    redundancy_penalty: float
    rationale: str

    def score(self) -> float:
        value = self.uncertainty * self.decision_change_probability * max(self.portfolio_value, 0.0)
        cost = max(self.compute_cost + self.data_cost + self.contamination_cost + self.redundancy_penalty, 1e-9)
        return float(value / cost)

    def to_dict(self) -> dict[str, Any]:
        out = asdict(self)
        out["score"] = self.score()
        out["policy_version"] = INFO_GAIN_POLICY_VERSION
        return out


def rank_experiments(experiments: list[ExperimentCandidate]) -> list[ExperimentCandidate]:
    return sorted(experiments, key=lambda item: item.score(), reverse=True)

