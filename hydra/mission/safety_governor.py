from __future__ import annotations

from typing import Any

from hydra.governance.invariants import GovernanceViolation, run_governance_checks


def check_action_allowed(action: dict[str, Any], *, baseline_commit: str, remaining_budget_usd: float) -> None:
    governance = run_governance_checks(baseline_commit=baseline_commit, remaining_budget_usd=remaining_budget_usd)
    if not governance.passed:
        raise GovernanceViolation(str(governance.to_dict()))
    if action.get("data_cost", 0.0) > remaining_budget_usd:
        raise GovernanceViolation("Action would exceed remaining Databento budget.")
    if action.get("action_type") == "LIVE_TRADING":
        raise GovernanceViolation("Live trading is prohibited.")
    if action.get("action_type") == "Q4_ACCESS" and not bool(
        action.get("q4_one_shot_authorization_valid")
    ):
        raise GovernanceViolation(
            "Q4 access requires the validated manifest-bound atomic one-shot capability."
        )
