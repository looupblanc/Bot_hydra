from __future__ import annotations

import inspect

import pytest

from hydra.selection.selector_manifest import stable_hash
from hydra.selection.selector_reporting import (
    FROZEN_DECISION_THRESHOLDS,
    REQUIRED_REPORT_SECTIONS,
    SELECTOR_PROCEDURE_FALSIFIED,
    SELECTOR_PROCEDURE_GREEN,
    SELECTOR_PROCEDURE_WEAK,
    SelectorReportingError,
    build_manifest_runtime_compatibility_projection,
    decide_selector_procedure,
    render_selector_report,
)


def _policy(
    *,
    normal: float,
    stressed: float,
    target: float,
    passes: int,
    episodes: int = 10,
) -> dict[str, float | int]:
    return {
        "normal_net_usd": normal,
        "stressed_net_usd": stressed,
        "stressed_target_progress": target,
        "stressed_pass_count": passes,
        "episode_count": episodes,
    }


def _green_folds() -> list[dict[str, object]]:
    folds: list[dict[str, object]] = []
    for index in range(4):
        selector = _policy(
            normal=100.0,
            stressed=100.0,
            target=0.80,
            passes=1 if index < 3 else 0,
        )
        selector.update(
            {
                "mll_breach_count": 1,
                "consistency": 0.75,
                "maximum_component_profit_share": 0.65,
            }
        )
        folds.append(
            {
                "block_id": f"B{index + 1}",
                "selector": selector,
                "best_parent": _policy(
                    normal=90.0,
                    stressed=70.0,
                    target=0.70,
                    passes=0,
                ),
                "equal_risk": _policy(
                    normal=95.0,
                    stressed=80.0,
                    target=0.75,
                    passes=0,
                ),
                "random_selection": [
                    {
                        **_policy(
                            normal=80.0,
                            stressed=60.0,
                            target=0.69,
                            passes=0,
                        ),
                        "seed": 11,
                    },
                    {
                        **_policy(
                            normal=85.0,
                            stressed=65.0,
                            target=0.71,
                            passes=0,
                        ),
                        "seed": 29,
                    },
                ],
            }
        )
    return folds


def test_green_accepts_frozen_safety_boundaries_exactly() -> None:
    decision = decide_selector_procedure(_green_folds())

    assert decision.status == SELECTOR_PROCEDURE_GREEN
    assert decision.metrics["held_out_mll_breach_rate"] == pytest.approx(0.10)
    assert decision.metrics["held_out_consistency"] == pytest.approx(0.75)
    assert decision.metrics["maximum_component_profit_share"] == pytest.approx(0.65)
    assert decision.metrics["selector"]["stressed_pass_count"] == 3
    assert decision.failure_reasons == ()


@pytest.mark.parametrize(
    ("mutation", "expected_status", "failed_check"),
    [
        ("mll", SELECTOR_PROCEDURE_FALSIFIED, "green.mll_within_tolerance"),
        (
            "component_share",
            SELECTOR_PROCEDURE_WEAK,
            "green.component_profit_share_within_limit",
        ),
        (
            "consistency",
            SELECTOR_PROCEDURE_WEAK,
            "green.consistency_meets_minimum",
        ),
        (
            "normal_best_parent",
            SELECTOR_PROCEDURE_WEAK,
            "green.beats_best_parent_normal_net",
        ),
        (
            "pass_concentration",
            SELECTOR_PROCEDURE_WEAK,
            "green.block_pass_share_within_limit",
        ),
    ],
)
def test_green_thresholds_are_not_relaxed(
    mutation: str, expected_status: str, failed_check: str
) -> None:
    folds = _green_folds()
    if mutation == "mll":
        folds[0]["selector"]["episode_count"] = 999
        folds[0]["selector"]["mll_breach_count"] = 100
        for baseline in ("best_parent", "equal_risk"):
            folds[0][baseline]["episode_count"] = 999
        for random_row in folds[0]["random_selection"]:
            random_row["episode_count"] = 999
    elif mutation == "component_share":
        folds[0]["selector"]["maximum_component_profit_share"] = 0.650001
    elif mutation == "consistency":
        for fold in folds:
            fold["selector"]["consistency"] = 0.749999
    elif mutation == "normal_best_parent":
        for fold in folds:
            fold["best_parent"]["normal_net_usd"] = 100.0
    elif mutation == "pass_concentration":
        folds[0]["selector"]["stressed_pass_count"] = 2
        folds[1]["selector"]["stressed_pass_count"] = 1
        folds[2]["selector"]["stressed_pass_count"] = 0

    decision = decide_selector_procedure(folds)

    assert decision.status == expected_status
    assert failed_check in decision.failure_reasons


def test_weak_requires_every_frozen_weak_condition() -> None:
    folds = _green_folds()
    for fold in folds:
        fold["selector"]["stressed_target_progress"] = 0.70

    decision = decide_selector_procedure(folds)

    assert decision.status == SELECTOR_PROCEDURE_FALSIFIED
    assert "weak.beats_best_parent_stressed_target_progress" in decision.failure_reasons


def test_weak_allows_exactly_two_positive_stressed_blocks() -> None:
    folds = _green_folds()
    folds[2]["selector"]["stressed_net_usd"] = -10.0
    folds[3]["selector"]["stressed_net_usd"] = -10.0

    decision = decide_selector_procedure(folds)

    assert decision.status == SELECTOR_PROCEDURE_WEAK
    assert decision.metrics["positive_stressed_block_count"] == 2


def test_green_must_beat_equal_risk_and_median_fixed_random() -> None:
    folds = _green_folds()
    for fold in folds:
        fold["equal_risk"]["stressed_target_progress"] = 0.80
        fold["random_selection"][0]["stressed_pass_count"] = 2
        fold["random_selection"][1]["stressed_pass_count"] = 2

    decision = decide_selector_procedure(folds)

    assert decision.status == SELECTOR_PROCEDURE_WEAK
    assert (
        "green.beats_equal_risk_stressed_target_progress"
        in decision.failure_reasons
    )
    assert "green.passes_not_worse_than_median_random" in decision.failure_reasons


def test_decision_thresholds_are_immutable_and_not_overridable() -> None:
    assert "thresholds" not in inspect.signature(decide_selector_procedure).parameters
    with pytest.raises(TypeError):
        FROZEN_DECISION_THRESHOLDS["maximum_mll_breach_rate"] = 0.11
    with pytest.raises(TypeError):
        decide_selector_procedure(_green_folds(), thresholds={})


@pytest.mark.parametrize("corruption", ["too_few", "duplicate_block", "seed_drift"])
def test_decision_fails_closed_on_invalid_outer_fold_evidence(
    corruption: str,
) -> None:
    folds = _green_folds()
    if corruption == "too_few":
        folds.pop()
    elif corruption == "duplicate_block":
        folds[1]["block_id"] = folds[0]["block_id"]
    elif corruption == "seed_drift":
        folds[1]["random_selection"][0]["seed"] = 777

    with pytest.raises(SelectorReportingError):
        decide_selector_procedure(folds)


def _complete_report_evidence() -> dict[str, object]:
    return {
        key: {"section_key": key, "status": "RECORDED"}
        for key, _ in REQUIRED_REPORT_SECTIONS
    }


def test_report_is_deterministic_and_contains_all_25_sections() -> None:
    evidence = _complete_report_evidence()
    evidence["selector_procedure_decision"] = decide_selector_procedure(
        _green_folds()
    )

    first = render_selector_report(evidence)
    second = render_selector_report(evidence)

    assert first == second
    assert first.count("\n## ") == 25
    for number, (_, title) in enumerate(REQUIRED_REPORT_SECTIONS, start=1):
        assert f"## {number}. {title}" in first
    assert "held-out outer-fold" in first
    assert "SELECTOR_PROCEDURE_GREEN" in first


def test_report_fails_closed_when_a_required_section_is_missing_or_blank() -> None:
    evidence = _complete_report_evidence()
    del evidence["q4_status"]
    with pytest.raises(SelectorReportingError, match="q4_status"):
        render_selector_report(evidence)

    evidence = _complete_report_evidence()
    evidence["final_development_champion"] = None
    with pytest.raises(SelectorReportingError, match="final_development_champion"):
        render_selector_report(evidence)


def test_manifest_runtime_projection_is_compatible_and_explicitly_scoped() -> None:
    decision = decide_selector_procedure(_green_folds())
    projection = build_manifest_runtime_compatibility_projection(
        decision,
        result_schema="hydra_generic_runtime_result_v1",
        campaign_id="0024",
        class_id="nested_selector_v1",
        population_manifest_hash="a" * 64,
        compatibility_policy_pair_count=4,
        primary_rolling_combine_episode_count=40,
    )

    assert projection["population"] == {
        "manifest_hash": "a" * 64,
        "real_policy_count": 4,
        "matched_control_policy_count": 4,
    }
    assert projection["policy_pair_evaluated_count"] == 4
    assert projection["pre_holdout_ready_count"] == 0
    assert projection["paper_shadow_ready_count"] == 0
    assert projection["selector_procedure"]["status"] == SELECTOR_PROCEDURE_GREEN
    assert projection["compatibility_projection"]["independent_confirmation"] is False
    assert (
        projection["compatibility_projection"][
            "family_average_fields_are_selector_evidence"
        ]
        is False
    )
    claimed_hash = projection.pop("result_sha256")
    assert claimed_hash == stable_hash(projection)
