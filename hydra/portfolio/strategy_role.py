"""Deterministic, evidence-neutral strategy role classification.

The classifier deliberately does *not* promote a strategy.  It only selects the
role-specific validation contract that downstream research must satisfy.  In
particular, a defensive or portfolio-only strategy is not required to have
large standalone PnL, while an alpha strategy is not allowed to substitute
portfolio diversification for positive economics.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any, Mapping


STRATEGY_ROLE_POLICY_VERSION = "strategy_role_policy_v1"


class StrategyRole(str, Enum):
    """Economic role used to choose a validation contract."""

    ALPHA = "alpha"
    DEFENSIVE = "defensive"
    PORTFOLIO_ONLY = "portfolio_only"
    RELATIVE_VALUE = "relative_value"
    HAZARD = "hazard"
    EXECUTION_SENSITIVE = "execution_sensitive"


class StrategyPool(str, Enum):
    """Account phase in which marginal utility is being researched."""

    COMBINE_PASSER_POOL = "COMBINE_PASSER_POOL"
    XFA_PAYOUT_POOL = "XFA_PAYOUT_POOL"
    DEFENSIVE_ACCOUNT_POOL = "DEFENSIVE_ACCOUNT_POOL"


@dataclass(frozen=True)
class StrategyRoleClassification:
    candidate_id: str
    role: StrategyRole
    target_pool: StrategyPool
    rationale: tuple[str, ...]
    required_evidence: tuple[str, ...]
    classification_source: str
    policy_version: str = STRATEGY_ROLE_POLICY_VERSION
    evidence_status: str = "UNASSESSED"
    inherited_status: bool = False
    promotion_eligible: bool = False

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        row["role"] = self.role.value
        row["target_pool"] = self.target_pool.value
        return row


_REQUIREMENTS: dict[StrategyRole, tuple[str, ...]] = {
    StrategyRole.ALPHA: (
        "positive_net_economics",
        "temporal_transfer",
        "realistic_cost_resilience",
        "candidate_level_nulls",
    ),
    StrategyRole.DEFENSIVE: (
        "shared_drawdown_reduction",
        "shared_mll_survival",
        "shared_loss_day_reduction",
        "matched_random_deactivation_controls",
    ),
    StrategyRole.PORTFOLIO_ONLY: (
        "positive_marginal_account_utility",
        "behavioral_uniqueness",
        "shared_account_replay",
        "matched_random_inclusion_controls",
    ),
    StrategyRole.RELATIVE_VALUE: (
        "past_only_hedge_ratio",
        "integer_executable_hedge",
        "two_leg_costs",
        "legging_stress",
        "marginal_account_utility",
    ),
    StrategyRole.HAZARD: (
        "out_of_sample_calibration",
        "out_of_sample_discrimination",
        "avoided_loss_utility",
        "matched_random_deactivation_controls",
    ),
    StrategyRole.EXECUTION_SENSITIVE: (
        "positive_net_economics",
        "bounded_execution_uncertainty",
        "delay_and_cost_stress",
        "candidate_level_nulls",
    ),
}


_POOL_REQUIREMENTS: dict[StrategyPool, tuple[str, ...]] = {
    StrategyPool.COMBINE_PASSER_POOL: (
        "target_before_mll",
        "time_to_target",
        "consistency_margin",
        "execution_cost_resilience",
        "tail_risk",
    ),
    StrategyPool.XFA_PAYOUT_POOL: (
        "payout_cycles_before_ruin",
        "qualifying_day_frequency",
        "mll_and_post_payout_survival",
        "payout_timing",
    ),
    StrategyPool.DEFENSIVE_ACCOUNT_POOL: (
        "marginal_account_utility",
        "shared_drawdown_reduction",
        "shared_loss_day_reduction",
        "matched_random_controls",
    ),
}


_EXPLICIT_ALIASES: dict[str, StrategyRole] = {
    "alpha": StrategyRole.ALPHA,
    "state_conditioned_alpha": StrategyRole.ALPHA,
    "trend": StrategyRole.ALPHA,
    "reversal": StrategyRole.ALPHA,
    "defensive": StrategyRole.DEFENSIVE,
    "defensive_mll_and_loss_day_control": StrategyRole.DEFENSIVE,
    "risk_off": StrategyRole.DEFENSIVE,
    "portfolio_only": StrategyRole.PORTFOLIO_ONLY,
    "portfolio": StrategyRole.PORTFOLIO_ONLY,
    "diversifier": StrategyRole.PORTFOLIO_ONLY,
    "relative_value": StrategyRole.RELATIVE_VALUE,
    "relative_value_diversifier": StrategyRole.RELATIVE_VALUE,
    "hazard": StrategyRole.HAZARD,
    "defensive_risk_state": StrategyRole.HAZARD,
    "execution_sensitive": StrategyRole.EXECUTION_SENSITIVE,
}


def required_evidence_for_role(role: StrategyRole | str) -> tuple[str, ...]:
    normalized = role if isinstance(role, StrategyRole) else StrategyRole(role)
    return _REQUIREMENTS[normalized]


def required_evidence_for_pool(pool: StrategyPool | str) -> tuple[str, ...]:
    normalized = pool if isinstance(pool, StrategyPool) else StrategyPool(pool)
    return _POOL_REQUIREMENTS[normalized]


def classify_strategy_role(
    specification: Mapping[str, Any],
) -> StrategyRoleClassification:
    """Classify a strategy without consulting its realized performance.

    Explicit semantic fields take precedence.  The fallback inspects structural
    metadata only; PnL, scores and promotion statuses are intentionally ignored
    so classification cannot be tuned after seeing an outcome.
    """

    candidate_id = str(
        specification.get("candidate_id")
        or specification.get("strategy_id")
        or "UNIDENTIFIED_CANDIDATE"
    )
    nested = specification.get("specification")
    nested = nested if isinstance(nested, Mapping) else {}

    explicit_values = (
        specification.get("strategy_role"),
        specification.get("portfolio_role"),
        specification.get("role"),
        nested.get("strategy_role"),
        nested.get("portfolio_role"),
        nested.get("role"),
    )
    target_pool, pool_source = _target_pool(specification, nested)
    for value in explicit_values:
        normalized = _normalize_label(value)
        if normalized in _EXPLICIT_ALIASES:
            role = _EXPLICIT_ALIASES[normalized]
            return _classification(
                candidate_id,
                role,
                target_pool or _default_pool(role),
                (f"explicit_role:{normalized}",),
                f"explicit;{pool_source}" if pool_source else "explicit",
            )

    structural = " ".join(
        _normalize_label(value)
        for value in (
            specification.get("candidate_type"),
            specification.get("mechanism_family"),
            specification.get("hypothesis"),
            nested.get("candidate_type"),
            nested.get("mechanism_family"),
            nested.get("hypothesis"),
        )
        if value is not None
    )
    legs = specification.get("legs", nested.get("legs", ()))
    leg_count = len(legs) if isinstance(legs, (list, tuple)) else 0

    if bool(specification.get("execution_sensitive")) or "execution_sensitive" in structural:
        return _classification(
            candidate_id,
            StrategyRole.EXECUTION_SENSITIVE,
            target_pool or StrategyPool.COMBINE_PASSER_POOL,
            ("structural_execution_sensitivity",),
            "structural",
        )
    if leg_count >= 2 or any(
        token in structural
        for token in ("relative_value", "paired", "spread", "residual", "two_leg")
    ):
        return _classification(
            candidate_id,
            StrategyRole.RELATIVE_VALUE,
            target_pool or StrategyPool.COMBINE_PASSER_POOL,
            (f"structural_leg_count:{leg_count}",),
            "structural",
        )
    if any(token in structural for token in ("hazard", "survival", "tail_probability")):
        return _classification(
            candidate_id,
            StrategyRole.HAZARD,
            target_pool or StrategyPool.DEFENSIVE_ACCOUNT_POOL,
            ("structural_distribution_or_hazard_model",),
            "structural",
        )
    if any(
        token in structural
        for token in ("defensive", "risk_off", "mll_protection", "deactivation")
    ):
        return _classification(
            candidate_id,
            StrategyRole.DEFENSIVE,
            target_pool or StrategyPool.DEFENSIVE_ACCOUNT_POOL,
            ("structural_risk_control",),
            "structural",
        )
    if any(
        token in structural
        for token in ("portfolio_only", "diversifier", "conflict_reduction", "allocator")
    ):
        return _classification(
            candidate_id,
            StrategyRole.PORTFOLIO_ONLY,
            target_pool or StrategyPool.DEFENSIVE_ACCOUNT_POOL,
            ("structural_portfolio_utility",),
            "structural",
        )
    return _classification(
        candidate_id,
        StrategyRole.ALPHA,
        target_pool or StrategyPool.COMBINE_PASSER_POOL,
        ("default_sparse_executable_alpha_contract",),
        "default",
    )


def _classification(
    candidate_id: str,
    role: StrategyRole,
    target_pool: StrategyPool,
    rationale: tuple[str, ...],
    source: str,
) -> StrategyRoleClassification:
    return StrategyRoleClassification(
        candidate_id=candidate_id,
        role=role,
        target_pool=target_pool,
        rationale=rationale,
        required_evidence=tuple(
            dict.fromkeys(
                required_evidence_for_role(role)
                + required_evidence_for_pool(target_pool)
            )
        ),
        classification_source=source,
    )


def _normalize_label(value: Any) -> str:
    return str(value or "").strip().lower().replace("-", "_").replace(" ", "_")


def _default_pool(role: StrategyRole) -> StrategyPool:
    if role in {StrategyRole.DEFENSIVE, StrategyRole.PORTFOLIO_ONLY, StrategyRole.HAZARD}:
        return StrategyPool.DEFENSIVE_ACCOUNT_POOL
    return StrategyPool.COMBINE_PASSER_POOL


def _target_pool(
    specification: Mapping[str, Any], nested: Mapping[str, Any]
) -> tuple[StrategyPool | None, str]:
    aliases = {
        "combine": StrategyPool.COMBINE_PASSER_POOL,
        "combine_passer": StrategyPool.COMBINE_PASSER_POOL,
        "combine_passer_pool": StrategyPool.COMBINE_PASSER_POOL,
        "xfa": StrategyPool.XFA_PAYOUT_POOL,
        "xfa_payout": StrategyPool.XFA_PAYOUT_POOL,
        "xfa_payout_pool": StrategyPool.XFA_PAYOUT_POOL,
        "defensive": StrategyPool.DEFENSIVE_ACCOUNT_POOL,
        "defensive_account": StrategyPool.DEFENSIVE_ACCOUNT_POOL,
        "defensive_account_pool": StrategyPool.DEFENSIVE_ACCOUNT_POOL,
    }
    for raw in (
        specification.get("target_pool"),
        specification.get("account_pool"),
        nested.get("target_pool"),
        nested.get("account_pool"),
    ):
        normalized = _normalize_label(raw)
        if normalized in aliases:
            return aliases[normalized], f"explicit_pool:{normalized}"
    return None, ""
