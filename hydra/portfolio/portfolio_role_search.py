"""Role-specific portfolio research over immutable trade ledgers.

This module produces research evidence only.  It intentionally has no registry,
mission, shadow, Q4 or order-writing dependency and can never emit a promotion
status.
"""

from __future__ import annotations

import hashlib
from collections import Counter
from dataclasses import asdict, dataclass
from typing import Any, Iterable, Mapping

import pandas as pd

from hydra.portfolio.account_contribution import (
    AccountContribution,
    AccountReplayConfig,
    MatchedInclusionControlSummary,
    compare_account_contribution,
    matched_random_inclusion_controls,
)
from hydra.portfolio.mll_protection_role import (
    MllProtectionEvaluation,
    evaluate_mll_protection_role,
)
from hydra.portfolio.strategy_role import (
    StrategyPool,
    StrategyRole,
    StrategyRoleClassification,
    classify_strategy_role,
)


PORTFOLIO_ROLE_SEARCH_VERSION = "portfolio_role_search_v1"


@dataclass(frozen=True)
class PortfolioRoleCandidateEvaluation:
    candidate_id: str
    classification: StrategyRoleClassification
    research_status: str
    target_pool: StrategyPool
    optimized_utility_name: str
    optimized_utility_delta: float | None
    contribution: AccountContribution | None
    inclusion_controls: MatchedInclusionControlSummary | None
    protection: MllProtectionEvaluation | None
    hard_risk_violation: bool
    candidate_level_evidence_only: bool = True
    inherited_status: bool = False
    promotion_eligible: bool = False
    shadow_research_active: bool = False
    paper_shadow_ready: bool = False
    policy_version: str = PORTFOLIO_ROLE_SEARCH_VERSION

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        row["target_pool"] = self.target_pool.value
        row["classification"]["role"] = self.classification.role.value
        row["classification"]["target_pool"] = self.classification.target_pool.value
        return row


@dataclass(frozen=True)
class PortfolioRoleSearchResult:
    candidates: tuple[PortfolioRoleCandidateEvaluation, ...]
    pareto_candidate_ids_by_pool: dict[str, tuple[str, ...]]
    status_counts: dict[str, int]
    pool_counts: dict[str, int]
    controls_are_non_operational: bool = True
    inherited_statuses: int = 0
    paper_shadow_ready: int = 0
    policy_version: str = PORTFOLIO_ROLE_SEARCH_VERSION

    def to_dict(self) -> dict[str, Any]:
        return {
            "candidates": [candidate.to_dict() for candidate in self.candidates],
            "pareto_candidate_ids_by_pool": {
                key: list(values)
                for key, values in self.pareto_candidate_ids_by_pool.items()
            },
            "status_counts": dict(self.status_counts),
            "pool_counts": dict(self.pool_counts),
            "controls_are_non_operational": self.controls_are_non_operational,
            "inherited_statuses": self.inherited_statuses,
            "paper_shadow_ready": self.paper_shadow_ready,
            "policy_version": self.policy_version,
        }


def search_portfolio_roles(
    base_ledgers: Mapping[str, pd.DataFrame | Iterable[Mapping[str, Any]]],
    candidate_ledgers: Mapping[str, pd.DataFrame | Iterable[Mapping[str, Any]]],
    specifications: Mapping[str, Mapping[str, Any]],
    *,
    defensive_policies: Mapping[
        str, pd.DataFrame | Iterable[Mapping[str, Any]]
    ]
    | None = None,
    control_count: int = 255,
    seed: int = 0,
    config: AccountReplayConfig | None = None,
) -> PortfolioRoleSearchResult:
    """Evaluate each candidate in one declared economic/account phase pool."""

    policies = defensive_policies or {}
    evaluations: list[PortfolioRoleCandidateEvaluation] = []
    for candidate_id in sorted(candidate_ledgers):
        specification = dict(specifications.get(candidate_id, {}))
        specification.setdefault("candidate_id", candidate_id)
        classification = classify_strategy_role(specification)
        candidate_seed = _candidate_seed(seed, candidate_id)
        if classification.role in {StrategyRole.DEFENSIVE, StrategyRole.HAZARD}:
            if candidate_id not in policies:
                evaluations.append(
                    PortfolioRoleCandidateEvaluation(
                        candidate_id=candidate_id,
                        classification=classification,
                        research_status="PAST_ONLY_DEACTIVATION_POLICY_REQUIRED",
                        target_pool=classification.target_pool,
                        optimized_utility_name="defensive_utility",
                        optimized_utility_delta=None,
                        contribution=None,
                        inclusion_controls=None,
                        protection=None,
                        hard_risk_violation=False,
                    )
                )
                continue
            protection = evaluate_mll_protection_role(
                candidate_id,
                base_ledgers,
                policies[candidate_id],
                control_count=control_count,
                seed=candidate_seed,
                config=config,
            )
            evaluations.append(
                PortfolioRoleCandidateEvaluation(
                    candidate_id=candidate_id,
                    classification=classification,
                    research_status=protection.research_status,
                    target_pool=StrategyPool.DEFENSIVE_ACCOUNT_POOL,
                    optimized_utility_name="defensive_effect_score",
                    optimized_utility_delta=protection.observed_defensive_score,
                    contribution=None,
                    inclusion_controls=None,
                    protection=protection,
                    hard_risk_violation=protection.hard_risk_violation,
                )
            )
            continue

        contribution = compare_account_contribution(
            base_ledgers,
            candidate_id,
            candidate_ledgers[candidate_id],
            target_pool=classification.target_pool,
            config=config,
        )
        controls = matched_random_inclusion_controls(
            base_ledgers,
            candidate_id,
            candidate_ledgers[candidate_id],
            target_pool=classification.target_pool,
            control_count=control_count,
            seed=candidate_seed,
            config=config,
        )
        status = _inclusion_research_status(classification, contribution, controls)
        evaluations.append(
            PortfolioRoleCandidateEvaluation(
                candidate_id=candidate_id,
                classification=classification,
                research_status=status,
                target_pool=classification.target_pool,
                optimized_utility_name=_utility_name(classification.target_pool),
                optimized_utility_delta=contribution.pool_utility_delta,
                contribution=contribution,
                inclusion_controls=controls,
                protection=None,
                hard_risk_violation=contribution.hard_risk_violation,
            )
        )

    ordered = tuple(sorted(evaluations, key=lambda row: row.candidate_id))
    return PortfolioRoleSearchResult(
        candidates=ordered,
        pareto_candidate_ids_by_pool=_pareto_by_pool(ordered),
        status_counts=dict(sorted(Counter(row.research_status for row in ordered).items())),
        pool_counts=dict(sorted(Counter(row.target_pool.value for row in ordered).items())),
    )


def _inclusion_research_status(
    classification: StrategyRoleClassification,
    contribution: AccountContribution,
    controls: MatchedInclusionControlSummary,
) -> str:
    if not contribution.candidate_past_only_verified:
        return "PAST_ONLY_CANDIDATE_PROVENANCE_REQUIRED"
    if contribution.hard_risk_violation:
        return f"{classification.target_pool.value}_HARD_RISK_REJECTED"
    if contribution.pool_utility_delta <= 0.0 or controls.one_sided_p_value > 0.10:
        return f"INSUFFICIENT_{classification.target_pool.value}_EVIDENCE"
    if classification.target_pool is StrategyPool.COMBINE_PASSER_POOL:
        if contribution.net_pnl_delta <= 0.0:
            return "INSUFFICIENT_COMBINE_PASSER_POOL_ECONOMICS"
        return "COMBINE_PASSER_POOL_RESEARCH_CANDIDATE"
    if classification.target_pool is StrategyPool.XFA_PAYOUT_POOL:
        if contribution.combined.payout_cycles_before_ruin <= 0:
            return "INSUFFICIENT_XFA_PAYOUT_PATH_EVIDENCE"
        return "XFA_PAYOUT_POOL_RESEARCH_CANDIDATE"
    return "DEFENSIVE_ACCOUNT_POOL_RESEARCH_CANDIDATE"


def _pareto_by_pool(
    candidates: tuple[PortfolioRoleCandidateEvaluation, ...],
) -> dict[str, tuple[str, ...]]:
    output: dict[str, tuple[str, ...]] = {}
    for pool in StrategyPool:
        rows = [
            row
            for row in candidates
            if row.target_pool is pool
            and row.optimized_utility_delta is not None
            and not row.hard_risk_violation
            and "RESEARCH_CANDIDATE" in row.research_status
        ]
        frontier: list[str] = []
        for row in rows:
            vector = _candidate_vector(row)
            dominated = any(
                other.candidate_id != row.candidate_id
                and _dominates(_candidate_vector(other), vector)
                for other in rows
            )
            if not dominated:
                frontier.append(row.candidate_id)
        output[pool.value] = tuple(sorted(frontier))
    return output


def _candidate_vector(row: PortfolioRoleCandidateEvaluation) -> tuple[float, ...]:
    if row.protection is not None:
        return (
            float(row.protection.observed_defensive_score),
            float(row.protection.min_mll_buffer_delta),
            float(row.protection.maximum_drawdown_reduction),
            float(row.protection.shared_loss_days_reduction),
        )
    assert row.contribution is not None
    return (
        float(row.contribution.pool_utility_delta),
        float(row.contribution.min_mll_buffer_delta),
        float(row.contribution.maximum_drawdown_reduction),
        float(row.contribution.target_velocity_delta),
    )


def _dominates(left: tuple[float, ...], right: tuple[float, ...]) -> bool:
    return all(a >= b for a, b in zip(left, right)) and any(
        a > b for a, b in zip(left, right)
    )


def _utility_name(pool: StrategyPool) -> str:
    return {
        StrategyPool.COMBINE_PASSER_POOL: "combine_utility",
        StrategyPool.XFA_PAYOUT_POOL: "xfa_utility",
        StrategyPool.DEFENSIVE_ACCOUNT_POOL: "defensive_utility",
    }[pool]


def _candidate_seed(seed: int, candidate_id: str) -> int:
    digest = hashlib.sha256(f"{seed}:{candidate_id}".encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big", signed=False)
