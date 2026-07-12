from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Any, Mapping

from hydra.promotion.candidate_value import candidate_value
from hydra.promotion.decision_cost import estimate_decision_cost
from hydra.promotion.evidence_gap import evidence_gaps
from hydra.promotion.promotion_priority import promotion_priority


@dataclass(frozen=True)
class EvidenceDebtRecord:
    candidate_id: str
    immutable_specification_hash: str
    identity: dict[str, Any]
    potential_value: dict[str, Any]
    missing_evidence: tuple[dict[str, Any], ...]
    estimated_closure_cost: dict[str, Any]
    decision_impact: dict[str, Any]
    priority_components: dict[str, Any]
    evidence_conversion_priority: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_evidence_debt_record(
    candidate: Mapping[str, Any],
    exact: Mapping[str, Any] | None,
    *,
    cluster_id: str,
    cluster_size: int,
) -> EvidenceDebtRecord:
    specification = dict(candidate.get("specification") or {})
    encoded = json.dumps(specification, sort_keys=True, separators=(",", ":"))
    specification_hash = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
    gaps = evidence_gaps(candidate, exact)
    value = candidate_value(candidate)
    cost = estimate_decision_cost(gaps)
    priority = promotion_priority(value, gaps, cost, cluster_size=cluster_size)
    missing = tuple(gap.to_dict() for gap in gaps if gap.missing and gap.role_required)
    identity = {
        "candidate_id": candidate.get("candidate_id"),
        "family": candidate.get("mechanism_family") or candidate.get("family"),
        "lineage": candidate.get("lineage_id"),
        "behavioral_cluster": cluster_id,
        "market": candidate.get("primary_market") or candidate.get("market"),
        "contract": candidate.get("execution_market"),
        "timeframe_profile": candidate.get("timeframe"),
        "session": specification.get("session_code"),
        "strategy_role": candidate.get("role"),
        "current_status": candidate.get("status"),
    }
    return EvidenceDebtRecord(
        candidate_id=str(candidate.get("candidate_id") or ""),
        immutable_specification_hash=specification_hash,
        identity=identity,
        potential_value=value.to_dict(),
        missing_evidence=missing,
        estimated_closure_cost=cost.to_dict(),
        decision_impact={
            "probability_next_test_changes_decision": priority.probability_next_test_changes_decision,
            "probability_pre_holdout_ready": max(0.0, min(value.expected_account_utility * (1.0 - len(missing) / 20.0), 1.0)),
            "probability_shadow_rejection": max(0.0, min(len(missing) / 20.0, 1.0)),
            "expected_account_utility": value.expected_account_utility,
        },
        priority_components=priority.to_dict(),
        evidence_conversion_priority=priority.priority,
    )
