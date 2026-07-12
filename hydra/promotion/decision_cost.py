from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Iterable

from hydra.promotion.evidence_gap import EvidenceGap


@dataclass(frozen=True)
class DecisionCost:
    compute_seconds: float
    data_cost_usd: float
    engineering_effort_units: float
    contamination_cost: float
    expected_experiment_count: int

    @property
    def total_research_cost(self) -> float:
        return max(
            self.compute_seconds / 60.0
            + self.data_cost_usd * 4.0
            + self.engineering_effort_units
            + self.contamination_cost * 10.0
            + self.expected_experiment_count * 0.5,
            0.1,
        )

    def to_dict(self) -> dict[str, Any]:
        return {**asdict(self), "total_research_cost": self.total_research_cost}


def estimate_decision_cost(gaps: Iterable[EvidenceGap]) -> DecisionCost:
    missing = [gap for gap in gaps if gap.missing and gap.role_required]
    return DecisionCost(
        compute_seconds=sum(gap.estimated_compute_seconds for gap in missing),
        data_cost_usd=sum(gap.estimated_data_cost_usd for gap in missing),
        engineering_effort_units=sum(0.6 if gap.hard_if_missing else 0.2 for gap in missing),
        contamination_cost=sum(1.0 for gap in missing if gap.stage == "HOLDOUT"),
        expected_experiment_count=len({gap.stage for gap in missing}),
    )
