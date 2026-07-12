from __future__ import annotations

from dataclasses import dataclass

from hydra.promotion.gates import GateResult, HARD_FAIL, SOFT_FAIL


@dataclass(frozen=True)
class ReadinessDecision:
    classification: str
    status: str
    promotion_stage: str
    rejection_reason: str | None
    recommended_action: str
    branch_action: str


PROMOTION_ORDER = [
    "GENERATED",
    "BACKTESTED",
    "COST_ADJUSTED",
    "NO_LOOKAHEAD_PASSED",
    "WALK_FORWARD_PASSED",
    "OOS_PASSED",
    "MONTE_CARLO_PASSED",
    "PARAMETER_SENSITIVITY_PASSED",
    "TOPSTEP_COMBINE_PASSED",
    "FUNDED_XFA_PASSED",
    "PAYOUT_SURVIVAL_PASSED",
    "CORRELATION_PASSED",
    "PORTFOLIO_INTERACTION_PASSED",
    "EXECUTION_READINESS_PASSED",
    "TRADING_READY_CANDIDATE",
]


COMBINE_PASSER_POOL = "COMBINE_PASSER_POOL"
XFA_PAYOUT_POOL = "XFA_PAYOUT_POOL"
DEFENSIVE_ACCOUNT_POOL = "DEFENSIVE_ACCOUNT_POOL"

ACCOUNT_PHASE_GATES = frozenset(
    {
        "TOPSTEP_COMBINE",
        "FUNDED_XFA",
        "PAYOUT_SURVIVAL",
    }
)

_POOL_ACCOUNT_GATES: dict[str, frozenset[str]] = {
    COMBINE_PASSER_POOL: frozenset({"TOPSTEP_COMBINE"}),
    XFA_PAYOUT_POOL: frozenset({"FUNDED_XFA", "PAYOUT_SURVIVAL"}),
    DEFENSIVE_ACCOUNT_POOL: frozenset(),
}

_POOL_OBJECTIVE_GATES: dict[str, frozenset[str]] = {
    COMBINE_PASSER_POOL: frozenset({"TOPSTEP_COMBINE"}),
    XFA_PAYOUT_POOL: frozenset({"FUNDED_XFA", "PAYOUT_SURVIVAL"}),
    DEFENSIVE_ACCOUNT_POOL: frozenset({"PORTFOLIO_INTERACTION"}),
}

_POOL_DIAGNOSTIC_GATES: dict[str, frozenset[str]] = {
    COMBINE_PASSER_POOL: frozenset(),
    XFA_PAYOUT_POOL: frozenset(),
    # Standalone alpha economics cannot hard-kill an account-protection policy;
    # its required objective is marginal portfolio interaction utility.
    DEFENSIVE_ACCOUNT_POOL: frozenset({"ECONOMIC_PROFILE"}),
}

_POOL_ALIASES = {
    "combine": COMBINE_PASSER_POOL,
    "combine_passer": COMBINE_PASSER_POOL,
    "combine_passer_pool": COMBINE_PASSER_POOL,
    "xfa": XFA_PAYOUT_POOL,
    "xfa_payout": XFA_PAYOUT_POOL,
    "xfa_payout_pool": XFA_PAYOUT_POOL,
    "defensive": DEFENSIVE_ACCOUNT_POOL,
    "defensive_account": DEFENSIVE_ACCOUNT_POOL,
    "defensive_account_pool": DEFENSIVE_ACCOUNT_POOL,
}


def normalize_target_pool(target_pool: object | None) -> str | None:
    """Normalize an explicit account objective without inferring it from results."""

    if target_pool is None:
        return None
    raw = getattr(target_pool, "value", target_pool)
    normalized = str(raw).strip()
    if not normalized:
        return None
    canonical = _POOL_ALIASES.get(normalized.lower(), normalized.upper())
    if canonical not in _POOL_ACCOUNT_GATES:
        raise ValueError(f"Unsupported target_pool: {target_pool!r}")
    return canonical


def required_account_gates(target_pool: object | None) -> frozenset[str]:
    """Return legacy all-phase gates or the account gates for one explicit pool."""

    normalized = normalize_target_pool(target_pool)
    return ACCOUNT_PHASE_GATES if normalized is None else _POOL_ACCOUNT_GATES[normalized]


def required_objective_gates(target_pool: object | None) -> frozenset[str]:
    normalized = normalize_target_pool(target_pool)
    return ACCOUNT_PHASE_GATES if normalized is None else _POOL_OBJECTIVE_GATES[normalized]


def decision_gates(gates: list[GateResult], target_pool: object | None = None) -> list[GateResult]:
    """Keep every scientific/integrity gate and only relevant account-phase gates."""

    normalized = normalize_target_pool(target_pool)
    required = required_account_gates(normalized)
    diagnostics = (
        frozenset() if normalized is None else _POOL_DIAGNOSTIC_GATES[normalized]
    )
    return [
        gate
        for gate in gates
        if gate.name not in diagnostics
        and (gate.name not in ACCOUNT_PHASE_GATES or gate.name in required)
    ]


def decide_readiness(
    gates: list[GateResult],
    promotion_score: float,
    economic_score: float,
    topstep_score: float,
    target_pool: object | None = None,
) -> ReadinessDecision:
    normalized_pool = normalize_target_pool(target_pool)
    scoped_gates = decision_gates(gates, normalized_pool)
    hard = [g for g in scoped_gates if not g.passed and g.severity == HARD_FAIL]
    soft = [g for g in scoped_gates if not g.passed and g.severity == SOFT_FAIL]
    first_failed = (hard or soft or [None])[0]
    passed_names = {g.name for g in scoped_gates if g.passed}
    stage = _stage_for_passes(passed_names, normalized_pool)
    objective_passed = _objective_gates_passed(scoped_gates, normalized_pool)
    if hard:
        return ReadinessDecision("DEAD_STRATEGY", "DEAD_STRATEGY", stage, hard[0].reason, hard[0].recommended_action, "kill")
    if normalized_pool is None and all(g.passed for g in scoped_gates) and promotion_score >= 0.78:
        return ReadinessDecision("TRADING_READY_CANDIDATE", "TRADING_READY_CANDIDATE", "TRADING_READY_CANDIDATE", None, "export_for_paper_shadow_research", "expand")
    if normalized_pool is not None and all(g.passed for g in scoped_gates) and promotion_score >= 0.78:
        return ReadinessDecision(
            "TOPSTEP_VIABLE",
            "TOPSTEP_VIABLE",
            stage,
            None,
            "deepen_target_pool_validation_without_cross_pool_status_inheritance",
            "expand",
        )
    if (
        topstep_score >= 0.62
        and economic_score >= 0.45
        and not hard
        and (normalized_pool is None or objective_passed)
    ):
        return ReadinessDecision("TOPSTEP_VIABLE", "TOPSTEP_VIABLE", stage, first_failed.reason if first_failed else None, "deepen_validation_and_portfolio_test", "expand")
    if topstep_score >= 0.45 and not hard:
        return ReadinessDecision("TOPSTEP_NEAR_MISS", "TOPSTEP_NEAR_MISS", stage, first_failed.reason if first_failed else None, "mutate_weak_dimension", "mutate")
    if economic_score >= 0.45:
        return ReadinessDecision("ECONOMICALLY_VIABLE", "ECONOMICALLY_VIABLE", stage, first_failed.reason if first_failed else None, "improve_topstep_path", "mutate")
    if soft:
        return ReadinessDecision("PROMISING_NEEDS_MUTATION", "PROMISING_NEEDS_MUTATION", stage, soft[0].reason, soft[0].recommended_action, "mutate")
    return ReadinessDecision("DEAD_STRATEGY", "DEAD_STRATEGY", "BACKTESTED", "no_actionable_signal", "kill_branch", "kill")


def _objective_gates_passed(gates: list[GateResult], target_pool: str | None) -> bool:
    if target_pool is None:
        return True
    required = required_objective_gates(target_pool)
    by_name = {gate.name: gate for gate in gates}
    return all(name in by_name and by_name[name].passed for name in required)


def _stage_for_passes(passed_names: set[str], target_pool: object | None = None) -> str:
    normalized_pool = normalize_target_pool(target_pool)
    prefix = [
        ("DATA_INTEGRITY", "BACKTESTED"),
        ("NO_LOOKAHEAD", "NO_LOOKAHEAD_PASSED"),
        ("WALK_FORWARD", "WALK_FORWARD_PASSED"),
        ("OOS", "OOS_PASSED"),
        ("MONTE_CARLO", "MONTE_CARLO_PASSED"),
        ("PARAMETER_SENSITIVITY", "PARAMETER_SENSITIVITY_PASSED"),
    ]
    account_mapping = {
        None: [
            ("TOPSTEP_COMBINE", "TOPSTEP_COMBINE_PASSED"),
            ("FUNDED_XFA", "FUNDED_XFA_PASSED"),
            ("PAYOUT_SURVIVAL", "PAYOUT_SURVIVAL_PASSED"),
        ],
        COMBINE_PASSER_POOL: [("TOPSTEP_COMBINE", "TOPSTEP_COMBINE_PASSED")],
        XFA_PAYOUT_POOL: [
            ("FUNDED_XFA", "FUNDED_XFA_PASSED"),
            ("PAYOUT_SURVIVAL", "PAYOUT_SURVIVAL_PASSED"),
        ],
        DEFENSIVE_ACCOUNT_POOL: [],
    }
    suffix = [
        ("CORRELATION", "CORRELATION_PASSED"),
        ("PORTFOLIO_INTERACTION", "PORTFOLIO_INTERACTION_PASSED"),
        ("EXECUTION_READINESS", "EXECUTION_READINESS_PASSED"),
    ]
    mapping = prefix + account_mapping[normalized_pool] + suffix
    stage = "GENERATED"
    for gate_name, promotion_stage in mapping:
        if gate_name in passed_names:
            stage = promotion_stage
        else:
            break
    return stage
