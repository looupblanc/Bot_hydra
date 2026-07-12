from __future__ import annotations

from hydra.data.resolution_ladder import (
    POLICY_VERSION,
    ResolutionEvidence,
    ResolutionRequest,
    decide_resolution_escalation,
)


def _evidence(**changes: object) -> ResolutionEvidence:
    values: dict[str, object] = {
        "candidate_id": "strategy_test_v1",
        "candidate_tier": "SHADOW_RESEARCH_CANDIDATE",
        "serious_bar_level_evidence": True,
        "event_windows_bounded": True,
        "event_window_count": 12,
        "intrabar_path_decision_relevant": True,
        "trade_intensity_hypothesis_preregistered": True,
        "serious_finalist": True,
        "execution_ambiguity_decision_relevant": True,
        "book_dependency_preregistered": True,
        "simpler_resolution_proven_insufficient": True,
        "expected_decision_information_gain": 0.7,
    }
    values.update(changes)
    return ResolutionEvidence(**values)  # type: ignore[arg-type]


def _request(schema: str, **changes: object) -> ResolutionRequest:
    values: dict[str, object] = {
        "schema": schema,
        "evidence": _evidence(),
        "official_estimated_cost_usd": 2.0,
        "committed_spend_usd": 27.4,
    }
    values.update(changes)
    return ResolutionRequest(**values)  # type: ignore[arg-type]


def test_one_minute_is_canonical_and_budget_preserving() -> None:
    decision = decide_resolution_escalation(
        _request(
            "ohlcv-1m",
            evidence=_evidence(
                candidate_tier="RESEARCH_PROTOTYPE",
                serious_bar_level_evidence=False,
                event_windows_bounded=False,
                event_window_count=0,
                intrabar_path_decision_relevant=False,
            ),
        )
    )
    assert decision.allowed
    assert decision.projected_remaining_budget_usd == 70.6
    assert decision.policy_version == POLICY_VERSION
    assert not decision.q4_access_allowed
    assert not decision.order_capability


def test_one_second_requires_bounded_decision_relevant_path_evidence() -> None:
    decision = decide_resolution_escalation(
        _request(
            "ohlcv-1s",
            evidence=_evidence(
                serious_bar_level_evidence=False,
                event_windows_bounded=False,
                event_window_count=0,
                intrabar_path_decision_relevant=False,
            ),
        )
    )
    assert not decision.allowed
    assert set(decision.failed_requirements) >= {
        "serious_bar_level_evidence_required",
        "bounded_event_windows_required",
        "intrabar_path_decision_relevance_required",
    }


def test_trades_require_preregistered_trade_intensity_hypothesis() -> None:
    decision = decide_resolution_escalation(
        _request(
            "trades",
            evidence=_evidence(trade_intensity_hypothesis_preregistered=False),
        )
    )
    assert not decision.allowed
    assert decision.reason == "trade_intensity_hypothesis_preregistration_required"


def test_tbbo_is_for_bounded_serious_finalists_only() -> None:
    broad = decide_resolution_escalation(
        _request(
            "tbbo",
            evidence=_evidence(
                candidate_tier="RESEARCH_PROTOTYPE",
                serious_finalist=False,
            ),
        )
    )
    assert not broad.allowed
    assert "serious_finalist_required" in broad.failed_requirements
    assert decide_resolution_escalation(_request("tbbo")).allowed


def test_mbp1_requires_book_dependence_and_simpler_tier_failure() -> None:
    decision = decide_resolution_escalation(
        _request(
            "mbp-1",
            evidence=_evidence(
                book_dependency_preregistered=False,
                simpler_resolution_proven_insufficient=False,
            ),
        )
    )
    assert not decision.allowed
    assert set(decision.failed_requirements) >= {
        "book_dependency_preregistration_required",
        "simpler_resolution_insufficiency_required",
    }


def test_mbo_is_unconditionally_prohibited() -> None:
    decision = decide_resolution_escalation(_request("mbo"))
    assert not decision.allowed
    assert decision.failed_requirements == ("mbo_prohibited",)


def test_paid_request_requires_official_estimate_and_reserve() -> None:
    no_estimate = decide_resolution_escalation(
        _request("ohlcv-1m", official_estimated_cost_usd=None)
    )
    assert not no_estimate.allowed
    assert no_estimate.reason == "official_cost_estimate_required"

    reserve = decide_resolution_escalation(
        _request("ohlcv-1m", official_estimated_cost_usd=43.0)
    )
    assert not reserve.allowed
    assert reserve.reason == "final_lockbox_budget_reserve_breached"


def test_cache_hit_waives_cost_not_scientific_eligibility() -> None:
    cached_bad = decide_resolution_escalation(
        _request(
            "tbbo",
            official_estimated_cost_usd=None,
            cache_hit=True,
            evidence=_evidence(serious_finalist=False),
        )
    )
    assert not cached_bad.allowed
    assert "official_cost_estimate_required" not in cached_bad.failed_requirements
    cached_good = decide_resolution_escalation(
        _request("tbbo", official_estimated_cost_usd=None, cache_hit=True)
    )
    assert cached_good.allowed
    assert not cached_good.paid_request


def test_decision_is_deterministic_and_serializable() -> None:
    request = _request("ohlcv_1s")
    first = decide_resolution_escalation(request)
    second = decide_resolution_escalation(request)
    assert first == second
    assert first.to_dict() == second.to_dict()
    assert first.to_dict()["schema"] == "ohlcv-1s"
