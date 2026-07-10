from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from typing import Any

from hydra.factory.remediation_policy import RemediationPolicy, POLICIES, choose_policy_for_reason, mutation_patch_for_policy
from hydra.strategies.dsl import StrategyCandidate


@dataclass(frozen=True)
class RemediationHypothesis:
    parent_candidate_id: str
    failed_gate: str
    policy: RemediationPolicy
    predicted_effect: str
    child: StrategyCandidate


def child_from_registry_row(row: dict[str, Any], variant: int = 0, policy_name: str | None = None) -> RemediationHypothesis:
    reason = str(row.get("rejection_reason") or "")
    policy = choose_policy_for_reason(reason)
    if policy_name:
        policy = next((item for item in POLICIES if item.name == policy_name), policy)
    risk = _loads(row.get("risk_json"))
    params = _loads(row.get("parameters_json"))
    child_risk, child_params = mutation_patch_for_policy(policy, risk, params, variant=variant)
    child = StrategyCandidate(
        candidate_id=f"rem_{uuid.uuid4().hex[:12]}",
        family=str(row["family"]),
        symbol=str(row["symbol"]),
        timeframe=str(row["timeframe"]),
        parameters=child_params,
        entry_logic=f"{row['family']}_regime_path_entry",
        exit_logic="gate_aware_remediation_exit",
        risk_parameters=child_risk,
        parent_candidate_id=str(row["candidate_id"]),
        mutation_type=policy.name,
    )
    return RemediationHypothesis(
        parent_candidate_id=str(row["candidate_id"]),
        failed_gate=reason,
        policy=policy,
        predicted_effect=policy.predicted_effect,
        child=child,
    )


def _loads(value: str | None) -> dict[str, Any]:
    try:
        return json.loads(value or "{}")
    except json.JSONDecodeError:
        return {}
