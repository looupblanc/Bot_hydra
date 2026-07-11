from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import Any


class StrategyRole(StrEnum):
    ALPHA = "ALPHA"
    DEFENSIVE = "DEFENSIVE"
    PORTFOLIO_ONLY = "PORTFOLIO_ONLY"
    RELATIVE_VALUE = "RELATIVE_VALUE"
    HAZARD = "HAZARD"
    EXECUTION_SENSITIVE = "EXECUTION_SENSITIVE"


class AccountObjectivePool(StrEnum):
    COMBINE_PASSER_POOL = "COMBINE_PASSER_POOL"
    XFA_PAYOUT_POOL = "XFA_PAYOUT_POOL"
    DEFENSIVE_ACCOUNT_POOL = "DEFENSIVE_ACCOUNT_POOL"


class MutationClass(StrEnum):
    PRIOR_EQUITY_MLL_GUARD = "PRIOR_EQUITY_MLL_GUARD"
    PRIOR_EQUITY_CONCENTRATION_GUARD = "PRIOR_EQUITY_CONCENTRATION_GUARD"
    PRIOR_EQUITY_TEMPORAL_GUARD = "PRIOR_EQUITY_TEMPORAL_GUARD"
    PRIOR_EQUITY_REGIME_GUARD = "PRIOR_EQUITY_REGIME_GUARD"
    MICRO_EXECUTION_REPAIR_REQUIRED = "MICRO_EXECUTION_REPAIR_REQUIRED"
    AVOIDED_LOSS_POLICY_GUARD = "AVOIDED_LOSS_POLICY_GUARD"
    PAST_ONLY_GAP_STABILITY_BAND = "PAST_ONLY_GAP_STABILITY_BAND"
    MICRO_FIRST_RISK_IMPLEMENTATION = "MICRO_FIRST_RISK_IMPLEMENTATION"


@dataclass(frozen=True)
class MutationHypothesis:
    hypothesis_id: str
    parent_candidate_id: str
    child_candidate_id: str
    mutation_class: str
    strategy_role: str
    objective_pool: str
    exact_change: str
    intended_failure_to_repair: str
    predicted_effect: str
    training_policy: str
    minimum_retained_fraction: float
    status_inheritance_allowed: bool = False
    q4_access_allowed: bool = False
    live_or_broker_allowed: bool = False

    @property
    def hypothesis_hash(self) -> str:
        payload = json.dumps(asdict(self), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def to_record(self) -> dict[str, Any]:
        return {**asdict(self), "hypothesis_hash": self.hypothesis_hash}


def classify_role(candidate: dict[str, Any]) -> StrategyRole:
    family = str(candidate.get("mechanism_family") or "").lower()
    candidate_id = str(candidate.get("candidate_id") or "").lower()
    contract = dict(candidate.get("contract_transfer") or {})
    if "relative" in family or "dispersion" in family or "relative" in candidate_id:
        return StrategyRole.RELATIVE_VALUE
    if "hazard" in family or "risk" in family:
        return StrategyRole.HAZARD
    if "defensive" in family or "overlay" in family:
        return StrategyRole.DEFENSIVE
    if contract and not bool(contract.get("passed", True)):
        return StrategyRole.EXECUTION_SENSITIVE
    return StrategyRole.ALPHA


def assign_objective_pool(candidate: dict[str, Any], role: StrategyRole) -> AccountObjectivePool:
    if role in {StrategyRole.DEFENSIVE, StrategyRole.PORTFOLIO_ONLY, StrategyRole.HAZARD}:
        return AccountObjectivePool.DEFENSIVE_ACCOUNT_POOL
    topstep = dict(candidate.get("topstep") or {})
    if bool(topstep.get("path_candidate")):
        return AccountObjectivePool.COMBINE_PASSER_POOL
    standard = dict(topstep.get("ten_micro_xfa_standard") or {})
    consistency = dict(topstep.get("ten_micro_xfa_consistency") or {})
    if max(
        int(standard.get("payout_cycles_survived") or 0),
        int(consistency.get("payout_cycles_survived") or 0),
    ) > 0:
        return AccountObjectivePool.XFA_PAYOUT_POOL
    return AccountObjectivePool.COMBINE_PASSER_POOL


def choose_mutation_class(candidate: dict[str, Any], role: StrategyRole) -> MutationClass:
    topstep = dict(candidate.get("topstep") or {})
    combine = dict(topstep.get("ten_micro_combine") or {})
    contract = dict(candidate.get("contract_transfer") or {})
    if role == StrategyRole.HAZARD:
        return MutationClass.AVOIDED_LOSS_POLICY_GUARD
    if contract and not bool(contract.get("passed", True)):
        return MutationClass.MICRO_EXECUTION_REPAIR_REQUIRED
    if bool(combine.get("mll_breached")):
        return MutationClass.PRIOR_EQUITY_MLL_GUARD
    if bool(candidate.get("event_dominated")) or bool(
        (candidate.get("concentration") or {}).get("event_dominated")
    ):
        return MutationClass.PRIOR_EQUITY_CONCENTRATION_GUARD
    return MutationClass.PRIOR_EQUITY_TEMPORAL_GUARD
