from __future__ import annotations

from pathlib import Path

import pytest

from hydra.economic_evolution.schema import stable_hash
from hydra.production import autonomous_graduation_cohort as cohort


ROOT = Path(__file__).resolve().parents[1]


def test_hash_bound_audit_excludes_all_consumed_candidates() -> None:
    result = cohort.audit_autonomous_graduation_cohort(ROOT)
    assert result["status"] == "PASS_HASH_BOUND_COHORT_AUDIT"
    assert result["cohort_size"] == 12
    assert result["confirmation_evaluated"] is False
    assert result["confirmation_partition_reads"] == 0
    assert result["q4_access_count_delta"] == 0
    assert result["orders"] == 0
    assert set(result["consumed_2026_candidate_ids"]) == {
        "hazard_2641d5adb7bfee8dca07de2a",
        "hazard_16a744e747cafb88a7e2c83b",
        "hazard_0a569f580a2540474116636c",
        "hazard_10ffb41856432af08259e32b",
        "hazard_16f0da561bc98f2eb7d2efc4",
    }


def test_preflight_receipt_is_hash_bound_and_read_only() -> None:
    core = {
        "schema": cohort.PREFLIGHT_SCHEMA,
        "status": "PASS_FROZEN_FULL_COVERAGE_PREFLIGHT",
        "manifest_hash": "a" * 64,
        "frozen_candidates": [],
        "confirmation_partition_reads": 0,
        "registry_writes": 0,
        "database_writes": 0,
        "q4_access_count_delta": 0,
        "broker_connections": 0,
        "orders": 0,
    }
    receipt = {**core, "preflight_hash": stable_hash(core)}
    assert cohort.verify_autonomous_graduation_preflight(receipt) == receipt
    receipt["orders"] = 1
    with pytest.raises(cohort.AutonomousGraduationCohortError):
        cohort.verify_autonomous_graduation_preflight(receipt)


def test_full_coverage_filters_without_changing_requested_denominator() -> None:
    value = {
        "calendar": (1, 2, 3, 4, 5, 6),
        "eligible_session_days": frozenset((1, 2, 3, 4, 5, 6)),
        "censored_session_days": frozenset((4,)),
    }
    starts = {
        5: ((1, "B1"), (2, "B2")),
        10: ((1, "B1"),),
        20: ((1, "B1"),),
    }
    result = cohort._coverage_for_prepared(value, starts)
    assert result["5"]["requested_start_count"] == 2
    assert result["5"]["full_coverage_start_count"] == 0
    assert result["5"]["data_censored_start_count"] == 2
    assert result["10"]["requested_start_count"] == 1
    assert result["10"]["full_coverage_start_count"] == 0
    assert result["5"]["within_horizon_non_overlapping"] is True


def test_development_gate_requires_same_horizon_block_diverse_stressed_passes() -> None:
    summary = {
        "episode_count": 12,
        "pass_count": 3,
        "mll_breach_rate": 0.0,
        "net_total_usd": 100.0,
        "by_block": {"B1": {"pass_count": 1}, "B3": {"pass_count": 2}},
    }
    summaries = {
        "NORMAL": {str(h): dict(summary) for h in cohort.HORIZONS},
        "STRESSED_1_5X": {str(h): dict(summary) for h in cohort.HORIZONS},
    }
    gate = {
        "minimum_normal_passes_same_horizon": 3,
        "minimum_stressed_passes_same_horizon": 2,
        "minimum_blocks_with_stressed_passes": 2,
        "maximum_stressed_mll_breach_rate": 0.10,
    }
    concentration = {"cleared": True}
    result = cohort._development_gates(summaries, concentration, gate)
    assert all(all(checks.values()) for checks in result.values())
    summaries["STRESSED_1_5X"]["10"]["by_block"] = {
        "B4": {"pass_count": 3}
    }
    result = cohort._development_gates(summaries, concentration, gate)
    assert result["10"]["multiple_stressed_pass_blocks"] is False


def test_xfa_contract_never_counts_alternative_paths_without_combine_pass() -> None:
    manifest = cohort._load_cohort_manifest(ROOT / cohort.DEFAULT_MANIFEST)
    assert manifest["xfa_contract"]["trigger"] == (
        "UNIQUE_EXACT_TARGET_REACHED_COMBINE_PATH_ONLY"
    )
    assert manifest["xfa_contract"]["paths_are_alternatives"] is True
    assert manifest["xfa_contract"]["sum_expected_values"] is False
    assert manifest["evaluation_contract"]["stage_48_claimed"] is False
    assert manifest["evaluation_contract"]["stage_96_claimed"] is False


def test_xfa_count_sums_only_materialized_alternative_paths() -> None:
    empty = {
        scenario: {
            str(horizon): {"episode_count": 0, "pass_count": 0}
            for horizon in cohort.HORIZONS
        }
        for scenario in cohort.SCENARIOS
    }
    counts = cohort._evidence_counts(
        [{"summaries": empty}],
        [
            {
                "status": "COMPLETE_DIAGNOSTIC_XFA_ALTERNATIVES",
                "alternative_path_count": 2,
            },
            {
                "status": "XFA_CONTINUATION_UNAVAILABLE_FAIL_CLOSED",
                "alternative_path_count": 0,
            },
        ],
        unique_combine_path_count=2,
    )
    assert counts["unique_xfa_transition_record_count"] == 2
    assert counts["ready_xfa_transition_count"] == 1
    assert counts["fail_closed_xfa_transition_count"] == 1
    assert counts["alternative_xfa_path_count"] == 2


def test_cross_horizon_passes_are_labeled_observations_not_unique_paths() -> None:
    summaries = {
        scenario: {
            str(horizon): {"episode_count": 4, "pass_count": 1}
            for horizon in cohort.HORIZONS
        }
        for scenario in cohort.SCENARIOS
    }
    counts = cohort._evidence_counts(
        [{"summaries": summaries}],
        [],
        unique_combine_path_count=2,
    )
    assert counts["combine_episode_horizon_observation_count_non_independent"] == 24
    assert counts["combine_pass_horizon_observation_count_non_independent"] == 6
    assert counts["unique_combine_path_count"] == 2
    assert "combine_pass_count" not in counts
