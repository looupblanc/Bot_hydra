from __future__ import annotations

import json
from pathlib import Path

from hydra.production.operational_candidate_bank_v2 import (
    DEFAULT_OUTPUT_DIR,
    build_bank,
    verify_bank,
)


ROOT = Path(__file__).resolve().parents[1]


def test_bank_reconciles_50_source_to_44_clean_without_erasure() -> None:
    matrix, summary = build_bank(ROOT)
    verify_bank(matrix, summary)

    counts = summary["bank_counts"]
    assert summary["source_reconciliation"]["observed_pass_source_policy_count"] == 50
    assert summary["source_reconciliation"][
        "observed_pass_unique_episode_behavior_hash_count"
    ] == 50
    assert counts["clean_underlying_strategy_count"] == 44
    assert counts["quarantined_preserved_count"] == 6
    assert counts["clean_dynamic_nonclone_account_policy_variant_count"] == 3
    assert counts["usable_research_configuration_count"] == 47
    dynamic_rows = [
        row for row in matrix["rows"]
        if row["record_type"] == "DYNAMIC_ACCOUNT_POLICY_VARIANT"
    ]
    assert all(
        row["nonclone_evidence"]["selected_profile_is_non_identity"]
        and row["nonclone_evidence"]["selected_episode_path_hashes_differ_from_baseline"]
        for row in dynamic_rows
    )


def test_tiers_and_confirmation_failures_are_not_inflated() -> None:
    matrix, summary = build_bank(ROOT)
    assert summary["active_clean_base_tier_counts"] == {
        "E": 22,
        "Q": 19,
        "G": 3,
        "C": 0,
        "F": 0,
    }
    rows = {row["configuration_id"]: row for row in matrix["rows"]}
    strongest = rows["hazard_19327ab34a21d623c654a6cc"]
    assert strongest["evidence_tier"] == "G"
    assert strongest["confirmation"]["status"] == "OVERFIT_CONFIRMATION_FAILURE_BRANCH_CLOSED"
    assert strongest["confirmation"]["tier_c_gate_passed"] is False
    assert all(row["independent_confirmation_claimed"] is False for row in rows.values())


def test_xfa_alternatives_reconcile_and_are_never_added() -> None:
    _, summary = build_bank(ROOT)
    xfa = summary["xfa"]
    assert xfa["combine_transition_count"] == 71
    assert xfa["alternative_path_count"] == 142
    assert xfa["standard"]["path_count"] == 71
    assert xfa["standard"]["first_payout_count"] == 27
    assert xfa["consistency"]["path_count"] == 71
    assert xfa["consistency"]["first_payout_count"] == 20
    assert xfa["standard_and_consistency_are_alternatives"] is True
    assert xfa["sum_standard_and_consistency_ev_allowed"] is False
    frontier = xfa["post_payout_frontier"]
    assert frontier["status"] == "COMPLETE_BOUNDED_XFA_POST_PAYOUT_DEVELOPMENT_DIAGNOSTIC"
    assert frontier["evaluation_count"] == 108
    assert frontier["evaluation_count_by_path"] == {"CONSISTENCY": 60, "STANDARD": 48}
    assert len(frontier["pareto_selected_profiles"]) == 7
    assert frontier["best_expected_value_profile"]["policy_id"] == (
        "hazard_19327ab34a21d623c654a6cc"
    )
    assert frontier["best_expected_value_profile"]["path"] == "CONSISTENCY"
    assert frontier["best_observed_30d_survival_profile"]["policy_id"] == (
        "autonomous_marginal_book_b09b8e7b30f90b34737eb724"
    )


def test_generated_outputs_verify_when_present() -> None:
    matrix_path = ROOT / DEFAULT_OUTPUT_DIR / "lifecycle_matrix.json"
    summary_path = ROOT / DEFAULT_OUTPUT_DIR / "bank_summary.json"
    if not matrix_path.exists() or not summary_path.exists():
        return
    matrix = json.loads(matrix_path.read_text(encoding="utf-8"))
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    verify_bank(matrix, summary)
