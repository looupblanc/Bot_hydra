from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from hydra.promotion.candidate_value import CandidateValue
from hydra.promotion.decision_cost import DecisionCost
from hydra.promotion.evidence_gap import EvidenceGap


@dataclass(frozen=True)
class PromotionPriority:
    expected_account_utility: float
    probability_next_test_changes_decision: float
    economic_distinctness: float
    evidence_debt_cost: float
    priority: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def promotion_priority(
    value: CandidateValue,
    gaps: tuple[EvidenceGap, ...],
    cost: DecisionCost,
    *,
    cluster_size: int,
) -> PromotionPriority:
    relevant = [gap for gap in gaps if gap.missing and gap.role_required]
    decision_probability = max(
        (gap.decision_change_probability for gap in relevant), default=0.05
    )
    distinctness = 1.0 / max(cluster_size, 1) ** 0.5
    score = (
        value.expected_account_utility
        * decision_probability
        * distinctness
        / cost.total_research_cost
    )
    return PromotionPriority(
        expected_account_utility=value.expected_account_utility,
        probability_next_test_changes_decision=decision_probability,
        economic_distinctness=distinctness,
        evidence_debt_cost=cost.total_research_cost,
        priority=score,
    )
