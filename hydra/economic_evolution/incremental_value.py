from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from typing import Any, Sequence

import numpy as np

from hydra.economic_evolution.schema import EconomicRole


@dataclass(frozen=True, slots=True)
class MatchedAccountObservation:
    start_id: str
    block_id: str
    net_after_costs: float
    stressed_net_after_costs: float
    target_progress: float
    mll_breached: bool
    consistency_ok: bool
    shared_loss_days: int
    conflict_count: int
    total_cost: float

    def __post_init__(self) -> None:
        if not self.start_id or not self.block_id:
            raise ValueError("matched observation requires start and block IDs")
        for value in (
            self.net_after_costs,
            self.stressed_net_after_costs,
            self.target_progress,
            self.total_cost,
        ):
            if not math.isfinite(value):
                raise ValueError("matched observation values must be finite")
        if self.shared_loss_days < 0 or self.conflict_count < 0 or self.total_cost < 0.0:
            raise ValueError("matched observation counts/costs cannot be negative")


@dataclass(frozen=True, slots=True)
class IncrementalValuePolicy:
    minimum_matched_starts: int
    minimum_independent_blocks: int
    minimum_stressed_net_uplift: float
    minimum_target_progress_uplift: float
    minimum_mll_breach_reduction: float
    minimum_consistency_uplift: float
    minimum_shared_loss_day_reduction: float
    maximum_net_sacrifice_for_defensive_role: float
    maximum_cost_increase: float
    minimum_positive_block_fraction: float

    def __post_init__(self) -> None:
        if self.minimum_matched_starts < 2 or self.minimum_independent_blocks < 2:
            raise ValueError("incremental policy requires multiple starts and blocks")
        if not 0.0 <= self.minimum_positive_block_fraction <= 1.0:
            raise ValueError("block fraction must be in [0,1]")
        if self.maximum_net_sacrifice_for_defensive_role < 0.0:
            raise ValueError("defensive net sacrifice cannot be negative")
        if self.maximum_cost_increase < 0.0:
            raise ValueError("maximum cost increase cannot be negative")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class IncrementalValueResult:
    component_id: str
    role: EconomicRole
    matched_start_count: int
    independent_block_count: int
    median_net_uplift: float
    median_stressed_net_uplift: float
    median_target_progress_uplift: float
    mll_breach_reduction: float
    consistency_uplift: float
    median_shared_loss_day_reduction: float
    median_conflict_reduction: float
    median_cost_increase: float
    positive_block_fraction: float
    one_sided_sign_p: float
    status: str
    decision_reason: str
    validated: bool = False
    inherited_status: bool = False

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["role"] = self.role.value
        return value


def evaluate_incremental_value(
    component_id: str,
    role: EconomicRole,
    baseline: Sequence[MatchedAccountObservation],
    included: Sequence[MatchedAccountObservation],
    *,
    policy: IncrementalValuePolicy,
) -> IncrementalValueResult:
    base = {row.start_id: row for row in baseline}
    candidate = {row.start_id: row for row in included}
    if len(base) != len(baseline) or len(candidate) != len(included):
        raise ValueError("matched starts must be unique")
    starts = tuple(sorted(set(base) & set(candidate)))
    if len(starts) != len(base) or len(starts) != len(candidate):
        raise ValueError("baseline and component observations must use identical starts")
    if len(starts) < policy.minimum_matched_starts:
        return _insufficient(component_id, role, len(starts), len({base[x].block_id for x in starts}))
    if any(base[start].block_id != candidate[start].block_id for start in starts):
        raise ValueError("matched starts must preserve block identity")
    blocks = tuple(sorted({base[start].block_id for start in starts}))
    if len(blocks) < policy.minimum_independent_blocks:
        return _insufficient(component_id, role, len(starts), len(blocks))

    net = np.asarray(
        [candidate[start].net_after_costs - base[start].net_after_costs for start in starts]
    )
    stressed = np.asarray(
        [
            candidate[start].stressed_net_after_costs
            - base[start].stressed_net_after_costs
            for start in starts
        ]
    )
    progress = np.asarray(
        [candidate[start].target_progress - base[start].target_progress for start in starts]
    )
    shared_loss = np.asarray(
        [base[start].shared_loss_days - candidate[start].shared_loss_days for start in starts]
    )
    conflict = np.asarray(
        [base[start].conflict_count - candidate[start].conflict_count for start in starts]
    )
    cost = np.asarray(
        [candidate[start].total_cost - base[start].total_cost for start in starts]
    )
    mll_reduction = float(
        np.mean([base[start].mll_breached for start in starts])
        - np.mean([candidate[start].mll_breached for start in starts])
    )
    consistency = float(
        np.mean([candidate[start].consistency_ok for start in starts])
        - np.mean([base[start].consistency_ok for start in starts])
    )
    medians = {
        "net": float(np.median(net)),
        "stressed": float(np.median(stressed)),
        "progress": float(np.median(progress)),
        "shared_loss": float(np.median(shared_loss)),
        "conflict": float(np.median(conflict)),
        "cost": float(np.median(cost)),
    }
    block_values = []
    for block in blocks:
        block_starts = [start for start in starts if base[start].block_id == block]
        indices = [starts.index(start) for start in block_starts]
        block_values.append(
            _block_utility(
                role,
                stressed=stressed[indices],
                shared_loss=shared_loss[indices],
                baseline=[base[start] for start in block_starts],
                included=[candidate[start] for start in block_starts],
                policy=policy,
            )
        )
    positive_blocks = float(np.mean(np.asarray(block_values) > 0.0))
    useful, reason = _role_decision(
        role,
        medians=medians,
        mll_reduction=mll_reduction,
        consistency_uplift=consistency,
        positive_block_fraction=positive_blocks,
        policy=policy,
    )
    return IncrementalValueResult(
        component_id=component_id,
        role=role,
        matched_start_count=len(starts),
        independent_block_count=len(blocks),
        median_net_uplift=medians["net"],
        median_stressed_net_uplift=medians["stressed"],
        median_target_progress_uplift=medians["progress"],
        mll_breach_reduction=mll_reduction,
        consistency_uplift=consistency,
        median_shared_loss_day_reduction=medians["shared_loss"],
        median_conflict_reduction=medians["conflict"],
        median_cost_increase=medians["cost"],
        positive_block_fraction=positive_blocks,
        one_sided_sign_p=_one_sided_sign_p(stressed),
        status="MICRO_EDGE_USEFUL" if useful else "COMPONENT_RESEARCH_ONLY",
        decision_reason=reason,
    )


def _role_decision(
    role: EconomicRole,
    *,
    medians: dict[str, float],
    mll_reduction: float,
    consistency_uplift: float,
    positive_block_fraction: float,
    policy: IncrementalValuePolicy,
) -> tuple[bool, str]:
    stable = positive_block_fraction >= policy.minimum_positive_block_fraction
    cost_ok = medians["cost"] <= policy.maximum_cost_increase
    if role in {EconomicRole.MLL_STABILIZER, EconomicRole.DEFENSIVE_SWITCH}:
        useful = bool(
            stable
            and cost_ok
            and medians["stressed"]
            >= -policy.maximum_net_sacrifice_for_defensive_role
            and (
                mll_reduction >= policy.minimum_mll_breach_reduction
                or medians["shared_loss"]
                >= policy.minimum_shared_loss_day_reduction
            )
        )
        return useful, "DEFENSIVE_ACCOUNT_UPLIFT" if useful else "NO_DEFENSIVE_UPLIFT"
    if role in {EconomicRole.CONSISTENCY_SMOOTHER, EconomicRole.PAYOUT_STABILIZER}:
        useful = bool(
            stable
            and cost_ok
            and medians["stressed"] >= 0.0
            and consistency_uplift >= policy.minimum_consistency_uplift
        )
        return useful, "CONSISTENCY_UPLIFT" if useful else "NO_CONSISTENCY_UPLIFT"
    useful = bool(
        stable
        and cost_ok
        and medians["stressed"] >= policy.minimum_stressed_net_uplift
        and medians["progress"] >= policy.minimum_target_progress_uplift
    )
    return useful, "ECONOMIC_AND_TARGET_UPLIFT" if useful else "NO_INCREMENTAL_ECONOMIC_UPLIFT"


def _insufficient(
    component_id: str, role: EconomicRole, starts: int, blocks: int
) -> IncrementalValueResult:
    return IncrementalValueResult(
        component_id=component_id,
        role=role,
        matched_start_count=starts,
        independent_block_count=blocks,
        median_net_uplift=0.0,
        median_stressed_net_uplift=0.0,
        median_target_progress_uplift=0.0,
        mll_breach_reduction=0.0,
        consistency_uplift=0.0,
        median_shared_loss_day_reduction=0.0,
        median_conflict_reduction=0.0,
        median_cost_increase=0.0,
        positive_block_fraction=0.0,
        one_sided_sign_p=1.0,
        status="COMPONENT_RESEARCH_ONLY",
        decision_reason="INSUFFICIENT_MATCHED_EVIDENCE",
    )


def _one_sided_sign_p(values: np.ndarray) -> float:
    nonzero = values[np.abs(values) > 1e-12]
    if not len(nonzero):
        return 1.0
    positives = int(np.sum(nonzero > 0.0))
    count = len(nonzero)
    return float(
        sum(math.comb(count, index) for index in range(positives, count + 1))
        / (2**count)
    )


def _block_utility(
    role: EconomicRole,
    *,
    stressed: np.ndarray,
    shared_loss: np.ndarray,
    baseline: Sequence[MatchedAccountObservation],
    included: Sequence[MatchedAccountObservation],
    policy: IncrementalValuePolicy,
) -> float:
    if role in {EconomicRole.MLL_STABILIZER, EconomicRole.DEFENSIVE_SWITCH}:
        mll = float(
            np.mean([row.mll_breached for row in baseline])
            - np.mean([row.mll_breached for row in included])
        )
        loss_days = float(np.median(shared_loss))
        net = float(np.median(stressed))
        acceptable_net = net >= -policy.maximum_net_sacrifice_for_defensive_role
        return (mll + 0.1 * loss_days) if acceptable_net else -1.0
    if role in {EconomicRole.CONSISTENCY_SMOOTHER, EconomicRole.PAYOUT_STABILIZER}:
        consistency = float(
            np.mean([row.consistency_ok for row in included])
            - np.mean([row.consistency_ok for row in baseline])
        )
        return consistency + max(float(np.median(stressed)), 0.0) / 10_000.0
    return float(np.median(stressed))


__all__ = [
    "IncrementalValuePolicy",
    "IncrementalValueResult",
    "MatchedAccountObservation",
    "evaluate_incremental_value",
]
