from __future__ import annotations

from hydra.portfolio.marginal_contribution_builder import (
    ExactBookEvaluation,
    GovernorProfile,
    MarginalContributionThresholds,
    SleeveSummary,
    SprintMetrics,
    assess_exact_marginal_contribution,
    build_marginal_book_proposals,
    select_matched_random_members,
)


def _metrics(index: int) -> SprintMetrics:
    return SprintMetrics(
        pass_rate_5d=0.01 * (index % 7),
        pass_rate_10d=0.02 * (index % 6),
        pass_rate_20d=0.03 * (index % 5),
        p25_target_progress=0.05 + index * 0.001,
        mll_survival_rate=0.99,
        consistency_rate=0.95,
        stressed_net=100.0 + index,
    )


def _sleeves(count: int = 16) -> list[SleeveSummary]:
    return [
        SleeveSummary(
            sleeve_id=f"sleeve_{index:03d}",
            qd_cell=f"cell_{index % 8}",
            behavioral_fingerprint=f"fingerprint_{index:03d}",
            metrics=_metrics(index),
        )
        for index in range(count)
    ]


def _profiles() -> tuple[GovernorProfile, ...]:
    return (
        GovernorProfile(
            profile_id="balanced",
            signal_quality_tiers=(0.0, 0.5, 1.0, 1.5, 2.0),
            open_risk_ceiling_fraction=0.25,
            daily_loss_budget_fraction=0.20,
            daily_profit_lock_fraction=0.35,
            maximum_concurrent_sleeves=2,
            target_protection_fraction=0.80,
            same_instrument_conflict_policy="reject_both",
        ),
        GovernorProfile(
            profile_id="concentrated",
            signal_quality_tiers=(0.0, 0.5, 1.0, 1.5, 2.0),
            open_risk_ceiling_fraction=0.35,
            daily_loss_budget_fraction=0.25,
            daily_profit_lock_fraction=0.40,
            maximum_concurrent_sleeves=3,
            target_protection_fraction=0.85,
            same_instrument_conflict_policy="priority",
        ),
    )


def test_builder_generates_requested_unique_diverse_books_deterministically() -> None:
    sleeves = _sleeves()

    first = build_marginal_book_proposals(
        sleeves,
        _profiles(),
        requested_count=1_000,
    )
    second = build_marginal_book_proposals(
        list(reversed(sleeves)),
        tuple(reversed(_profiles())),
        requested_count=1_000,
    )

    assert first == second
    assert len(first) == 1_000
    assert len({item.structural_fingerprint for item in first}) == 1_000
    assert len({item.book_id for item in first}) == 1_000
    assert {len(item.sleeve_ids) for item in first}.issubset({2, 3, 4, 5, 6})
    assert len({item.sleeve_ids[0] for item in first}) > 8
    for proposal in first:
        assert len(proposal.sleeve_ids) <= 6
        assert len(proposal.predecessor_sleeve_ids) == len(proposal.sleeve_ids) - 1
        assert set(proposal.predecessor_sleeve_ids).issubset(proposal.sleeve_ids)
        assert proposal.added_sleeve_id in proposal.sleeve_ids
        assert max(proposal.qd_cells.count(cell) for cell in proposal.qd_cells) <= 2
        assert proposal.governor_profile_id in {"balanced", "concentrated"}


def test_builder_rejects_execution_equivalent_sleeves() -> None:
    sleeves = _sleeves(3)
    duplicate = SleeveSummary(
        sleeve_id="different_id",
        qd_cell="different_cell",
        behavioral_fingerprint=sleeves[0].behavioral_fingerprint,
        metrics=_metrics(99),
    )

    try:
        build_marginal_book_proposals(
            [*sleeves, duplicate], _profiles(), requested_count=1
        )
    except ValueError as exc:
        assert "behavioral fingerprints" in str(exc)
    else:
        raise AssertionError("execution-equivalent sleeves must fail closed")


def _evaluation(
    book_id: str,
    sleeve_ids: tuple[str, ...],
    *,
    pass_5d: float,
    pass_10d: float,
    progress: float,
    mll: float = 0.99,
    consistency: float = 0.95,
) -> ExactBookEvaluation:
    return ExactBookEvaluation(
        book_id=book_id,
        sleeve_ids=sleeve_ids,
        metrics=SprintMetrics(
            pass_rate_5d=pass_5d,
            pass_rate_10d=pass_10d,
            pass_rate_20d=pass_10d,
            p25_target_progress=progress,
            mll_survival_rate=mll,
            consistency_rate=consistency,
            stressed_net=1_000.0,
        ),
    )


def test_exact_marginal_accepts_one_gain_without_material_degradation() -> None:
    predecessor = _evaluation(
        "small", ("a", "b"), pass_5d=0.05, pass_10d=0.10, progress=0.20
    )
    component = _evaluation(
        "best_component", ("a",), pass_5d=0.04, pass_10d=0.11, progress=0.19
    )
    candidate = _evaluation(
        "larger", ("a", "b", "c"), pass_5d=0.06, pass_10d=0.105, progress=0.22
    )

    decision = assess_exact_marginal_contribution(
        candidate,
        predecessor,
        component,
        thresholds=MarginalContributionThresholds(),
    )

    assert decision.accepted
    assert decision.retained_book_id == "larger"
    assert "pass_rate_5d" in decision.improved_metrics
    assert "p25_target_progress" in decision.improved_metrics
    assert decision.material_degradations == ()


def test_exact_marginal_preserves_smaller_book_on_material_degradation() -> None:
    predecessor = _evaluation(
        "small", ("a", "b"), pass_5d=0.08, pass_10d=0.12, progress=0.25
    )
    component = _evaluation(
        "best_component", ("a",), pass_5d=0.07, pass_10d=0.10, progress=0.24
    )
    candidate = _evaluation(
        "larger",
        ("a", "b", "c"),
        pass_5d=0.09,
        pass_10d=0.09,
        progress=0.27,
    )

    decision = assess_exact_marginal_contribution(
        candidate,
        predecessor,
        component,
        thresholds=MarginalContributionThresholds(),
    )

    assert not decision.accepted
    assert decision.retained_book_id == "small"
    assert decision.retained_sleeve_ids == ("a", "b")
    assert "pass_rate_10d" in decision.material_degradations
    assert decision.reason == "material_degradation_preserve_predecessor"


def test_exact_marginal_preserves_smaller_book_when_nothing_improves() -> None:
    predecessor = _evaluation(
        "small", ("a", "b"), pass_5d=0.05, pass_10d=0.10, progress=0.20
    )
    component = _evaluation(
        "best_component", ("a",), pass_5d=0.05, pass_10d=0.10, progress=0.20
    )
    candidate = _evaluation(
        "larger", ("a", "b", "c"), pass_5d=0.05, pass_10d=0.10, progress=0.20
    )

    decision = assess_exact_marginal_contribution(
        candidate,
        predecessor,
        component,
        thresholds=MarginalContributionThresholds(),
    )

    assert not decision.accepted
    assert decision.retained_book_id == "small"
    assert decision.reason == "no_exact_marginal_improvement_preserve_predecessor"


def test_matched_random_selection_is_deterministic_and_cell_matched() -> None:
    sleeves = _sleeves()
    reference = ("sleeve_000", "sleeve_001", "sleeve_002", "sleeve_003")

    first = select_matched_random_members(
        sleeves, reference, deterministic_seed=29
    )
    second = select_matched_random_members(
        list(reversed(sleeves)), reference, deterministic_seed=29
    )

    assert first == second
    assert len(first.sleeve_ids) == len(reference)
    assert set(first.sleeve_ids).isdisjoint(reference)
    assert first.exact_qd_cell_match
    assert first.requested_qd_cells == first.selected_qd_cells
