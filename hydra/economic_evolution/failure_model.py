from __future__ import annotations

from typing import Mapping

from hydra.account_policy.basket import AccountPolicyRollingSummary
from hydra.account_policy.schema import stable_hash
from hydra.economic_evolution.schema import FailureDimension, FailureVector


def derive_failure_vector(
    policy_id: str,
    base: AccountPolicyRollingSummary,
    stressed: AccountPolicyRollingSummary,
    *,
    minimum_research_events: int = 24,
    minimum_effective_blocks: int = 4,
    useful_target_progress: float = 0.75,
    maximum_acceptable_mll_breach_rate: float = 0.20,
    temporal_positive_block_fraction: float = 1.0,
    directional_beta_score: float = 0.0,
    redundant_role_score: float = 0.0,
    null_indistinguishable: bool = False,
    execution_infeasible: bool = False,
    expected_payouts: float | None = None,
    evaluated_on_identical_parent_child_starts: bool = True,
) -> FailureVector:
    """Turn an account result into a bounded, actionable failure vector.

    The vector contains no mutation decision and cannot see future outcomes.  It
    merely normalizes already-completed evidence so the mutation engine can
    target the dominant observed failure instead of running blind grids.
    """

    if base.episode_start_days != stressed.episode_start_days:
        raise ValueError("base and stressed evidence must use identical starts")
    if minimum_research_events < 1 or minimum_effective_blocks < 1:
        raise ValueError("failure policy thresholds must be positive")
    if useful_target_progress <= 0.0:
        raise ValueError("useful target progress must be positive")
    opportunity = _clip(
        1.0 - base.accepted_event_count / max(float(minimum_research_events), 1.0)
    )
    target_velocity = _clip(
        1.0 - base.target_progress_median / useful_target_progress
    )
    mll = _clip(
        base.mll_breach_rate / max(maximum_acceptable_mll_breach_rate, 1e-12)
    )
    base_net = base.median_episode_net_pnl
    stressed_net = stressed.median_episode_net_pnl
    if base_net <= 0.0:
        weak_cost = 1.0
    else:
        weak_cost = _clip((base_net - stressed_net) / abs(base_net))
        if stressed_net <= 0.0:
            weak_cost = 1.0
    temporal = _clip(1.0 - temporal_positive_block_fraction)
    concentration = _clip(
        (base.median_best_day_concentration - 0.50) / 0.50
    )
    sequence = _clip(
        max(base.mll_breach_rate, stressed.mll_breach_rate)
        + 0.5 * concentration
    )
    consistency = _clip(1.0 - base.consistency_pass_rate)
    projected = base.projected_days_to_target
    recovery = 1.0 if projected is None else _clip((projected - 30.0) / 90.0)
    payout = (
        0.5
        if expected_payouts is None
        else _clip(1.0 - max(expected_payouts, 0.0))
    )
    power = _clip(
        max(
            1.0 - base.episode_start_count / 24.0,
            1.0 - base.effective_block_count / float(minimum_effective_blocks),
        )
    )
    scores = (
        (FailureDimension.INSUFFICIENT_OPPORTUNITY_COUNT, opportunity),
        (FailureDimension.INSUFFICIENT_TARGET_VELOCITY, target_velocity),
        (FailureDimension.MLL_BREACH, mll),
        (FailureDimension.WEAK_COST_MARGIN, weak_cost),
        (FailureDimension.UNSTABLE_TEMPORAL_TRANSFER, temporal),
        (FailureDimension.HIDDEN_DIRECTIONAL_BETA, _clip(directional_beta_score)),
        (FailureDimension.CONCENTRATION, concentration),
        (FailureDimension.SEQUENCE_FRAGILITY, sequence),
        (FailureDimension.CONSISTENCY_RULE_FAILURE, consistency),
        (FailureDimension.LONG_RECOVERY_TIME, recovery),
        (FailureDimension.PAYOUT_FRAGILITY, payout),
        (FailureDimension.REDUNDANT_PORTFOLIO_ROLE, _clip(redundant_role_score)),
        (
            FailureDimension.NULL_INDISTINGUISHABLE,
            1.0 if null_indistinguishable else 0.0,
        ),
        (FailureDimension.INSUFFICIENT_STATISTICAL_POWER, power),
        (
            FailureDimension.EXECUTION_INFEASIBILITY,
            1.0
            if execution_infeasible
            or base.compliance_failure_count
            or stressed.compliance_failure_count
            else 0.0,
        ),
    )
    evidence_hash = stable_hash(
        {
            "policy_id": policy_id,
            "base": base.to_dict(),
            "stressed": stressed.to_dict(),
            "inputs": {
                "minimum_research_events": minimum_research_events,
                "minimum_effective_blocks": minimum_effective_blocks,
                "useful_target_progress": useful_target_progress,
                "maximum_acceptable_mll_breach_rate": maximum_acceptable_mll_breach_rate,
                "temporal_positive_block_fraction": temporal_positive_block_fraction,
                "directional_beta_score": directional_beta_score,
                "redundant_role_score": redundant_role_score,
                "null_indistinguishable": null_indistinguishable,
                "execution_infeasible": execution_infeasible,
                "expected_payouts": expected_payouts,
            },
        }
    )
    return FailureVector(
        policy_id=policy_id,
        scores=scores,
        evidence_hash=evidence_hash,
        evaluated_on_identical_parent_child_starts=(
            evaluated_on_identical_parent_child_starts
        ),
    )


def failure_scores(vector: FailureVector) -> Mapping[str, float]:
    return {dimension.value: value for dimension, value in vector.scores}


def _clip(value: float) -> float:
    return min(1.0, max(0.0, float(value)))


__all__ = ["derive_failure_vector", "failure_scores"]
