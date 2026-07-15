from __future__ import annotations

from dataclasses import replace

from hydra.promotion.portfolio_status import (
    BookEvidence,
    PortfolioStatus,
    SleeveEvidence,
    decide_book_statuses,
)


def _sleeve(**changes: object) -> SleeveEvidence:
    value = SleeveEvidence(
        sleeve_id="s1",
        immutable_fingerprint="a" * 64,
        family_id="falsified-family",
        family_verdict="SELECTOR_PROCEDURE_FALSIFIED",
        behavioral_cluster="c1",
        role="TARGET_VELOCITY",
        normal_net_pnl=100.0,
        stressed_net_pnl=50.0,
        mll_breach_rate=0.0,
        event_count=20,
        maximum_single_event_profit_share=0.2,
        complete_trade_ledger=True,
        complete_evidence_bundle=True,
        executable_specification=True,
    )
    return replace(value, **changes)


def _book(**changes: object) -> BookEvidence:
    value = BookEvidence(
        book_pair_id="book-1",
        combine_starts=48,
        combine_evaluable_starts=48,
        normal_combine_passes=5,
        stressed_combine_passes=3,
        pass_block_ids=("B1", "B2"),
        stressed_net_pnl=500.0,
        stressed_economically_defensible=True,
        mll_breach_rate=0.04,
        consistency_acceptable=True,
        maximum_block_profit_share=0.45,
        maximum_sleeve_profit_share=0.45,
        xfa_paths_started=5,
        unique_xfa_start_days=(10, 20, 30),
        payout_eligible_paths=1,
        payout_cycles=1,
        expected_trader_net_payout_per_attempt=20.0,
        post_payout_survival_rate=0.5,
        complete_evidence_bundle=True,
        immutable_books_complete=True,
        forward_no_order_package_complete=True,
    )
    return replace(value, **changes)


def test_family_failure_does_not_erase_sleeve_evidence() -> None:
    assert _sleeve().statuses == (
        PortfolioStatus.SLEEVE_ECONOMICALLY_ELIGIBLE,
        PortfolioStatus.SLEEVE_COMBINE_COMPONENT,
        PortfolioStatus.SLEEVE_XFA_COMPONENT,
    )


def test_hard_candidate_defect_rejects_sleeve() -> None:
    assert _sleeve(hard_execution_or_data_defect=True).statuses == ()


def test_book_graduation_and_payout_stages_are_candidate_level() -> None:
    statuses = decide_book_statuses(_book())
    assert PortfolioStatus.COMBINE_BOOK_GRADUATED in statuses
    assert PortfolioStatus.PAYOUT_PATH_CANDIDATE in statuses
    assert PortfolioStatus.FORWARD_SHADOW_CANDIDATE in statuses
    assert PortfolioStatus.PAPER_SHADOW_READY not in statuses


def test_one_successful_combine_path_cannot_create_payout_candidate() -> None:
    statuses = decide_book_statuses(
        _book(xfa_paths_started=2, unique_xfa_start_days=(10,))
    )

    assert PortfolioStatus.PAYOUT_PATH_CANDIDATE not in statuses


def test_normal_and_stress_versions_of_one_start_are_not_multiple_xfa_paths() -> None:
    statuses = decide_book_statuses(
        _book(
            xfa_paths_started=2,
            unique_xfa_start_days=(10,),
            payout_eligible_paths=2,
            payout_cycles=2,
        )
    )

    assert PortfolioStatus.XFA_BOOK_ACTIVE in statuses
    assert PortfolioStatus.PAYOUT_PATH_CANDIDATE not in statuses


def test_xfa_active_records_transition_without_combine_graduation() -> None:
    statuses = decide_book_statuses(
        _book(
            normal_combine_passes=1,
            stressed_combine_passes=0,
            pass_block_ids=("B1",),
            xfa_paths_started=1,
            unique_xfa_start_days=(10,),
            payout_eligible_paths=0,
            payout_cycles=0,
            expected_trader_net_payout_per_attempt=0.0,
            post_payout_survival_rate=0.0,
        )
    )

    assert PortfolioStatus.COMBINE_BOOK_GRADUATED not in statuses
    assert PortfolioStatus.XFA_BOOK_ACTIVE in statuses


def test_paper_shadow_requires_independent_and_forward_confirmation() -> None:
    evidence = _book(
        independent_confirmation_complete=True,
        forward_confirmation_complete=True,
        paper_shadow_contract_complete=True,
    )
    assert PortfolioStatus.PAPER_SHADOW_READY in decide_book_statuses(evidence)


def test_one_block_dominance_blocks_graduation_without_erasing_candidate() -> None:
    statuses = decide_book_statuses(
        _book(pass_block_ids=("B4",), maximum_block_profit_share=0.9)
    )
    assert statuses[0] is PortfolioStatus.COMBINE_BOOK_CANDIDATE
    assert PortfolioStatus.COMBINE_BOOK_GRADUATED not in statuses
