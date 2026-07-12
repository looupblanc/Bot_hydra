from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace

import pandas as pd
import pytest

from hydra.promotion import pipeline
from hydra.promotion.gates import GateResult, HARD_FAIL, PASS, SOFT_FAIL
from hydra.promotion.pipeline import PromotionInput
from hydra.promotion.readiness import (
    COMBINE_PASSER_POOL,
    DEFENSIVE_ACCOUNT_POOL,
    XFA_PAYOUT_POOL,
    decide_readiness,
    decision_gates,
    normalize_target_pool,
)


BASE_GATE_NAMES = (
    "DATA_INTEGRITY",
    "NO_LOOKAHEAD",
    "WALK_FORWARD",
    "OOS",
    "MONTE_CARLO",
    "PARAMETER_SENSITIVITY",
)


def _gate(name: str, passed: bool = True, severity: str | None = None) -> GateResult:
    return GateResult(
        name=name,
        passed=passed,
        score=1.0 if passed else 0.0,
        reason="passed" if passed else f"{name.lower()}_failed",
        severity=severity or (PASS if passed else SOFT_FAIL),
        recommended_action="promote" if passed else "retest",
        failure_mode="none" if passed else "retest",
    )


def _readiness_gates(
    *,
    combine: GateResult | None = None,
    funded: GateResult | None = None,
    payout: GateResult | None = None,
    data: GateResult | None = None,
) -> list[GateResult]:
    base = [_gate(name) for name in BASE_GATE_NAMES]
    if data is not None:
        base[0] = data
    return [
        *base,
        combine or _gate("TOPSTEP_COMBINE"),
        funded or _gate("FUNDED_XFA"),
        payout or _gate("PAYOUT_SURVIVAL"),
        _gate("CORRELATION"),
        _gate("PORTFOLIO_INTERACTION"),
        _gate("EXECUTION_READINESS"),
    ]


def test_legacy_readiness_still_requires_all_account_phases() -> None:
    gates = _readiness_gates(
        funded=_gate("FUNDED_XFA", False),
        payout=_gate("PAYOUT_SURVIVAL", False),
    )

    decision = decide_readiness(gates, 0.95, 0.80, 0.80)

    assert decision.classification != "TRADING_READY_CANDIDATE"
    assert {gate.name for gate in decision_gates(gates)} >= {
        "TOPSTEP_COMBINE",
        "FUNDED_XFA",
        "PAYOUT_SURVIVAL",
    }


def test_combine_pool_does_not_require_xfa_or_payout_and_cannot_be_finally_promoted() -> None:
    gates = _readiness_gates(
        funded=_gate("FUNDED_XFA", False),
        payout=_gate("PAYOUT_SURVIVAL", False),
    )

    decision = decide_readiness(gates, 0.95, 0.80, 1.0, COMBINE_PASSER_POOL)

    assert decision.classification == "TOPSTEP_VIABLE"
    assert decision.classification != "TRADING_READY_CANDIDATE"
    assert {gate.name for gate in decision_gates(gates, COMBINE_PASSER_POOL)}.isdisjoint(
        {"FUNDED_XFA", "PAYOUT_SURVIVAL"}
    )


def test_failed_required_pool_gate_cannot_receive_viable_classification_from_score_alone() -> None:
    gates = _readiness_gates(combine=_gate("TOPSTEP_COMBINE", False))

    decision = decide_readiness(gates, 0.95, 0.80, 0.90, COMBINE_PASSER_POOL)

    assert decision.classification != "TOPSTEP_VIABLE"
    assert decision.classification != "TRADING_READY_CANDIDATE"


def test_xfa_pool_ignores_combine_path_but_preserves_hard_integrity() -> None:
    irrelevant_combine_failure = _gate("TOPSTEP_COMBINE", False, HARD_FAIL)
    gates = _readiness_gates(combine=irrelevant_combine_failure)

    scoped = decide_readiness(gates, 0.95, 0.80, 1.0, XFA_PAYOUT_POOL)
    integrity_failed = decide_readiness(
        _readiness_gates(
            combine=irrelevant_combine_failure,
            data=_gate("DATA_INTEGRITY", False, HARD_FAIL),
        ),
        0.95,
        0.80,
        1.0,
        XFA_PAYOUT_POOL,
    )

    assert scoped.classification == "TOPSTEP_VIABLE"
    assert integrity_failed.classification == "DEAD_STRATEGY"
    assert integrity_failed.rejection_reason == "data_integrity_failed"


def test_defensive_pool_treats_account_phase_gates_as_diagnostics() -> None:
    gates = _readiness_gates(
        combine=_gate("TOPSTEP_COMBINE", False, HARD_FAIL),
        funded=_gate("FUNDED_XFA", False),
        payout=_gate("PAYOUT_SURVIVAL", False),
    )
    gates.append(_gate("ECONOMIC_PROFILE", False, HARD_FAIL))

    decision = decide_readiness(gates, 0.95, 0.80, 1.0, DEFENSIVE_ACCOUNT_POOL)

    assert decision.classification == "TOPSTEP_VIABLE"
    assert {gate.name for gate in decision_gates(gates, DEFENSIVE_ACCOUNT_POOL)}.isdisjoint(
        {"ECONOMIC_PROFILE", "TOPSTEP_COMBINE", "FUNDED_XFA", "PAYOUT_SURVIVAL"}
    )


def test_unknown_target_pool_fails_closed() -> None:
    with pytest.raises(ValueError, match="Unsupported target_pool"):
        normalize_target_pool("TYPO_POOL")


def test_pipeline_reports_pool_scope_and_fingerprints_it(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pipeline, "data_integrity_gate", lambda _value: _gate("DATA_INTEGRITY"))
    monkeypatch.setattr(pipeline, "duplicate_gate", lambda *_args: _gate("DUPLICATE_FINGERPRINT"))
    monkeypatch.setattr(pipeline, "no_lookahead_gate", lambda *_args: _gate("NO_LOOKAHEAD"))
    monkeypatch.setattr(pipeline, "economic_gate", lambda *_args: _gate("ECONOMIC_PROFILE"))
    monkeypatch.setattr(pipeline, "walk_forward_gate", lambda *_args: _gate("WALK_FORWARD"))
    monkeypatch.setattr(pipeline, "oos_gate", lambda *_args: _gate("OOS"))
    monkeypatch.setattr(pipeline, "monte_carlo_gate", lambda *_args: _gate("MONTE_CARLO"))
    monkeypatch.setattr(pipeline, "parameter_sensitivity_gate", lambda *_args: _gate("PARAMETER_SENSITIVITY"))
    monkeypatch.setattr(pipeline, "topstep_combine_gate", lambda *_args: _gate("TOPSTEP_COMBINE"))
    monkeypatch.setattr(pipeline, "funded_gate", lambda *_args: _gate("FUNDED_XFA", False))
    monkeypatch.setattr(pipeline, "payout_gate", lambda *_args: _gate("PAYOUT_SURVIVAL", False))
    monkeypatch.setattr(pipeline, "correlation_gate", lambda *_args: _gate("CORRELATION"))
    monkeypatch.setattr(pipeline, "portfolio_interaction_gate", lambda *_args: _gate("PORTFOLIO_INTERACTION"))
    monkeypatch.setattr(pipeline, "execution_readiness_gate", lambda *_args: _gate("EXECUTION_READINESS"))
    monkeypatch.setattr(
        pipeline,
        "export_research_config",
        lambda *_args, **_kwargs: pytest.fail("pool-scoped readiness must not export or promote"),
    )

    candidate = SimpleNamespace(
        candidate_id="pool_test",
        family="test",
        symbol="MES",
        timeframe="1m",
        parameters={"threshold": 1.0},
        risk_parameters={"internal_daily_stop": 100.0, "daily_profit_lock": 200.0, "max_position": 1},
        parent_candidate_id=None,
        mutation_type=None,
    )
    result = SimpleNamespace(metrics={"net_profit": 1_000.0}, trades=[])
    payload = PromotionInput(
        candidate=candidate,
        result=result,
        daily=pd.DataFrame({"pnl": [100.0]}),
        topstep_record={"topstep_score": 1.0},
        data_validation={},
        split_scores={"mar": 1.0},
        leak_ok=True,
        leak_reason="passed",
        existing_fingerprints=set(),
        max_correlation=0.0,
        seed=7,
        lane="test",
        report_tag="target_pool_test",
        target_pool=COMBINE_PASSER_POOL,
    )

    combine = pipeline.run_promotion_pipeline(payload)
    xfa = pipeline.run_promotion_pipeline(replace(payload, target_pool=XFA_PAYOUT_POOL))

    assert combine["classification"] == "TOPSTEP_VIABLE"
    assert combine["target_pool_objective_passed"] is True
    assert combine["required_target_pool_gates"] == ["TOPSTEP_COMBINE"]
    assert combine["gate_applicability"]["FUNDED_XFA"] == "DIAGNOSTIC_NON_TARGET_POOL"
    assert combine["config_export_path"] is None
    assert combine["risk_export_path"] is None
    assert xfa["target_pool_objective_passed"] is False
    assert combine["input_fingerprint"] != xfa["input_fingerprint"]
