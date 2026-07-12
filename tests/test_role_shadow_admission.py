from __future__ import annotations

from hydra.foundry.status import EvidenceTier, ShadowEvidence, decide_shadow_admission


def _safe_evidence(**updates: object) -> ShadowEvidence:
    values: dict[str, object] = {
        "candidate_id": "role_candidate",
        "data_integrity": True,
        "no_lookahead": True,
        "deterministic_signals": True,
        "net_after_costs": 100.0,
        "supportive_temporal_folds": 2,
        "candidate_null_pass": True,
        "null_probability": 0.03,
        "parameter_stable": True,
        "contract_evidence": True,
        "account_mll_safe": True,
        "execution_possible": True,
        "realtime_features_available": True,
        "shadow_spec_complete": True,
        "observability_complete": True,
        "untouched_holdout_passed": False,
        "sample_size": 50,
    }
    values.update(updates)
    return ShadowEvidence(**values)


def test_default_alpha_contract_remains_backward_compatible() -> None:
    decision = decide_shadow_admission(
        _safe_evidence(untouched_holdout_passed=True)
    )

    assert decision.tier is EvidenceTier.PAPER_SHADOW_READY
    assert decision.permits_zero_risk_shadow
    assert not decision.permits_broker_orders


def test_soft_diagnostics_become_shadow_uncertainty_not_admission_blockers() -> None:
    decision = decide_shadow_admission(
        _safe_evidence(
            parameter_stable=False,
            contract_evidence=False,
            untouched_holdout_passed=True,
            uncertainty="development_only",
        )
    )

    assert decision.tier is EvidenceTier.SHADOW_RESEARCH_CANDIDATE
    assert decision.permits_zero_risk_shadow
    assert decision.missing_requirements == ()
    assert "parameter_stability_diagnostic_unresolved" in decision.uncertainty
    assert "contract_transfer_diagnostic_unresolved" in decision.uncertainty


def test_calibrated_null_shortfall_is_uncertainty_for_zero_order_shadow_only() -> None:
    decision = decide_shadow_admission(
        _safe_evidence(
            candidate_null_pass=False,
            null_probability=0.35,
            untouched_holdout_passed=True,
        )
    )

    assert decision.tier is EvidenceTier.SHADOW_RESEARCH_CANDIDATE
    assert decision.permits_zero_risk_shadow
    assert "candidate_level_null_diagnostic_unresolved" in decision.uncertainty


def test_xfa_pool_can_shadow_with_zero_standalone_pnl_and_positive_account_utility() -> None:
    decision = decide_shadow_admission(
        _safe_evidence(
            strategy_role="ALPHA",
            objective_pool="XFA_PAYOUT_POOL",
            net_after_costs=0.0,
            account_utility_delta=0.15,
            parameter_stable=False,
            contract_evidence=False,
        )
    )

    assert decision.tier is EvidenceTier.SHADOW_RESEARCH_CANDIDATE
    assert decision.permits_zero_risk_shadow


def test_defensive_pool_uses_marginal_account_utility_not_standalone_alpha() -> None:
    decision = decide_shadow_admission(
        _safe_evidence(
            strategy_role="DEFENSIVE",
            objective_pool="DEFENSIVE_ACCOUNT_POOL",
            net_after_costs=0.0,
            account_utility_delta=0.25,
        )
    )

    assert decision.tier is EvidenceTier.SHADOW_RESEARCH_CANDIDATE
    assert decision.permits_zero_risk_shadow


def test_role_pool_without_positive_account_utility_remains_ineligible() -> None:
    decision = decide_shadow_admission(
        _safe_evidence(
            strategy_role="DEFENSIVE",
            objective_pool="DEFENSIVE_ACCOUNT_POOL",
            net_after_costs=500.0,
            account_utility_delta=0.0,
        )
    )

    assert decision.tier is EvidenceTier.RESEARCH_PROTOTYPE
    assert decision.missing_requirements == ("positive_objective_account_utility",)
    assert not decision.permits_zero_risk_shadow


def test_hard_invalidation_still_rejects_role_specific_candidate() -> None:
    decision = decide_shadow_admission(
        _safe_evidence(
            strategy_role="DEFENSIVE",
            objective_pool="DEFENSIVE_ACCOUNT_POOL",
            net_after_costs=0.0,
            account_utility_delta=1.0,
            hard_invalidations=("lookahead",),
        )
    )

    assert decision.tier is EvidenceTier.SHADOW_REJECTED
    assert decision.fatal_reasons == ("lookahead",)
    assert not decision.permits_zero_risk_shadow


def test_role_specific_economics_never_bypass_the_shadow_safety_package() -> None:
    decision = decide_shadow_admission(
        _safe_evidence(
            strategy_role="DEFENSIVE",
            objective_pool="DEFENSIVE_ACCOUNT_POOL",
            net_after_costs=0.0,
            account_utility_delta=1.0,
            account_mll_safe=False,
        )
    )

    assert decision.tier is EvidenceTier.ROBUST_RESEARCH_CANDIDATE
    assert decision.missing_requirements == ("account_mll_safe",)
    assert not decision.permits_zero_risk_shadow


def test_unknown_objective_pool_fails_closed() -> None:
    decision = decide_shadow_admission(
        _safe_evidence(objective_pool="UNREGISTERED_POOL")
    )

    assert decision.tier is EvidenceTier.RESEARCH_PROTOTYPE
    assert decision.missing_requirements == ("recognized_objective_pool",)
    assert not decision.permits_zero_risk_shadow
