from __future__ import annotations

from copy import deepcopy

import pytest

from hydra.economic_evolution.schema import stable_hash
from hydra.production import autonomous_director_runtime as runtime


def _relay_results() -> dict[str, dict[str, object]]:
    return {
        "TIER_G_GRADUATION": {
            "counts": {"graduated_development_book_count": 4},
        },
        "TIER_G_XFA_HANDOFF": {
            "counts": {"ready_xfa_transition_count": 20},
        },
        "TIER_G_XFA_DIAGNOSTIC": {
            "counts": {
                "combine_transition_count": 20,
                "alternative_path_count": 40,
                "standard_path_count": 20,
                "consistency_path_count": 20,
                "standard_first_payout_count": 12,
                "consistency_first_payout_count": 9,
                "standard_payout_cycle_count": 18,
                "consistency_payout_cycle_count": 11,
                "standard_post_payout_survival_count": 0,
                "consistency_post_payout_survival_count": 0,
            },
        },
        "CROSS_INDEX_BREADTH": {
            "status": "CROSS_INDEX_BREADTH_TRIPWIRE_WEAK_DEVELOPMENT_ONLY",
            "counts": {
                "primary_candidate_count": 16,
                "control_candidate_count": 48,
                "exact_account_replays": 9_086,
            },
            "gate": {"qualifying_cell_count": 1},
        },
    }


def test_relay_counts_keep_g_xfa_and_breadth_denominators_separate() -> None:
    counts = runtime._relay_evidence_counts(_relay_results())

    assert counts["tier_g_count"] == 4
    assert counts["combine_to_xfa_transition_count"] == 20
    assert counts["xfa_alternative_path_count"] == 40
    assert counts["xfa_standard_first_payout_count"] == 12
    assert counts["xfa_consistency_first_payout_count"] == 9
    assert counts["breadth_exact_account_replay_count"] == 9_086
    assert counts["breadth_qualifying_cell_count"] == 1
    assert counts["tier_c_count"] == 0
    assert counts["tier_f_count"] == 0


def test_relay_counts_fail_closed_on_xfa_denominator_drift() -> None:
    results = _relay_results()
    results["TIER_G_XFA_DIAGNOSTIC"]["counts"][  # type: ignore[index]
        "combine_transition_count"
    ] = 19

    with pytest.raises(
        runtime.AutonomousDirectorRuntimeError,
        match="transition denominators differ",
    ):
        runtime._relay_evidence_counts(results)


def test_state_and_kpis_publish_g_and_diagnostic_xfa_without_c_or_f() -> None:
    results = _relay_results()
    manifest = {
        "campaign_id": "hydra_autonomous_economic_discovery_director_0035",
        "manifest_hash": "a" * 64,
        "source_commit": "b" * 40,
    }
    state = runtime._state_payload(
        manifest,
        sequence=11,
        state="ROBUSTNESS_ACTIVE",
        stage="POST_TIER_G_XFA_AND_BREADTH_RELAYS_COMPLETE",
        branch_results=results,
        next_action="FREEZE_ONE_UNTOUCHED_CONFIRMATION_CONTRACT",
    )
    kpis = runtime._kpis(manifest, state, results, runtime.time.monotonic())

    assert state["authoritative_tier_g_count"] == 4
    assert state["combine_to_xfa_transition_count"] == 20
    assert state["xfa_paths_started"] == 40
    assert state["cross_index_breadth_exact_account_replay_count"] == 9_086
    assert kpis["authoritative_tier_g_count"] == 4
    assert kpis["xfa_standard_path_count"] == 20
    assert kpis["xfa_consistency_path_count"] == 20
    assert kpis["cross_index_breadth_qualifying_cell_count"] == 1
    assert kpis["confirmation_ready_candidates"] == 0


def test_breadth_verifier_accepts_runtime_metadata_outside_economic_hash() -> None:
    core = {
        "schema": runtime.CROSS_INDEX_BREADTH_SCHEMA,
        "status": "CROSS_INDEX_BREADTH_TRIPWIRE_WEAK_DEVELOPMENT_ONLY",
        "evidence_tier": "E_DIAGNOSTIC_DEVELOPMENT",
        "promotion_status": None,
        "gate": {
            "authoritative_promotion_count": 0,
            "xfa_paths_started": 0,
            "independent_confirmation_claimed": False,
        },
        "counts": {
            "authoritative_promotion_count": 0,
            "xfa_paths_started": 0,
            "broker_connections": 0,
            "orders": 0,
            "q4_access_count_delta": 0,
            "data_purchase_count": 0,
            "database_writes": 0,
            "registry_writes": 0,
        },
        "next_action": "PRESERVE_DIAGNOSTIC",
    }
    result = {
        **core,
        "runtime_seconds": 3.5,
        "completed_at_utc": "2026-07-19T00:00:00+00:00",
        "result_hash": stable_hash(core),
    }

    assert runtime._verify_breadth_tripwire_result(result) == result

    unsafe = deepcopy(result)
    unsafe["counts"]["orders"] = 1
    unsafe_core = {
        key: value
        for key, value in unsafe.items()
        if key not in {"result_hash", "runtime_seconds", "completed_at_utc"}
    }
    unsafe["result_hash"] = stable_hash(unsafe_core)
    with pytest.raises(
        runtime.AutonomousDirectorRuntimeError,
        match="attempted a side effect",
    ):
        runtime._verify_breadth_tripwire_result(unsafe)
