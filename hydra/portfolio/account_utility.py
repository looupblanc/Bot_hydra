from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


ACCOUNT_UTILITY_VERSION = "topstep_account_utility_v1"


@dataclass(frozen=True)
class AccountUtility:
    strategy_id: str
    combine_pass_probability: float
    mll_survival_probability: float
    median_time_to_target_days: float | None
    consistency_probability: float
    xfa_survival_probability: float
    first_payout_probability: float
    repeat_payout_probability: float
    shared_loss_day_penalty: float
    tail_overlap_penalty: float
    execution_cost_penalty: float
    operational_complexity_penalty: float
    expected_utility: float
    policy_version: str = ACCOUNT_UTILITY_VERSION

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def expected_account_utility(
    *,
    strategy_id: str,
    combine_pass_probability: float,
    mll_survival_probability: float,
    consistency_probability: float,
    xfa_survival_probability: float,
    first_payout_probability: float,
    repeat_payout_probability: float,
    shared_loss_day_penalty: float,
    tail_overlap_penalty: float,
    execution_cost_penalty: float,
    operational_complexity_penalty: float,
    median_time_to_target_days: float | None = None,
) -> AccountUtility:
    benefit = (
        0.25 * combine_pass_probability
        + 0.20 * mll_survival_probability
        + 0.15 * consistency_probability
        + 0.15 * xfa_survival_probability
        + 0.15 * first_payout_probability
        + 0.10 * repeat_payout_probability
    )
    penalty = (
        0.20 * shared_loss_day_penalty
        + 0.20 * tail_overlap_penalty
        + 0.15 * execution_cost_penalty
        + 0.10 * operational_complexity_penalty
    )
    return AccountUtility(
        strategy_id=strategy_id,
        combine_pass_probability=float(combine_pass_probability),
        mll_survival_probability=float(mll_survival_probability),
        median_time_to_target_days=median_time_to_target_days,
        consistency_probability=float(consistency_probability),
        xfa_survival_probability=float(xfa_survival_probability),
        first_payout_probability=float(first_payout_probability),
        repeat_payout_probability=float(repeat_payout_probability),
        shared_loss_day_penalty=float(shared_loss_day_penalty),
        tail_overlap_penalty=float(tail_overlap_penalty),
        execution_cost_penalty=float(execution_cost_penalty),
        operational_complexity_penalty=float(operational_complexity_penalty),
        expected_utility=float(benefit - penalty),
    )

