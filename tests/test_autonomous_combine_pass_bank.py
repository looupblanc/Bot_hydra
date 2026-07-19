from __future__ import annotations

from copy import deepcopy

import pytest

from hydra.economic_evolution.schema import stable_hash
from hydra.production.autonomous_combine_candidate_bank import (
    SCHEMA as CANDIDATE_BANK_SCHEMA,
)
from hydra.production.autonomous_combine_pass_bank import (
    AutonomousCombinePassBankError,
    build_autonomous_combine_pass_observed_bank,
)
from hydra.production.autonomous_marginal_combine_books import COMPOSITE_SCHEMA


def test_real_capacity_shape_keeps_46_exacts_and_four_marginal_books() -> None:
    exacts = [
        _exact_candidate(
            f"exact-{index:02d}",
            tier_q=index < 24,
            economic_offset=float(index),
        )
        for index in range(46)
    ]
    books = [
        _book(
            f"book-{index:02d}",
            components=["exact-00", "exact-01"],
            behavior_offset=float(index),
        )
        for index in range(4)
    ]

    bank = build_autonomous_combine_pass_observed_bank(
        _candidate_bank(exacts), _marginal_composite(books)
    )

    assert bank["status"].endswith("TARGET_REACHED")
    assert bank["counts"] == {
        "eligible_exact_standalone_count": 46,
        "eligible_marginal_book_count": 4,
        "rejected_non_marginal_pass_book_count": 0,
        "eligible_before_deduplication_count": 50,
        "deduplicated_eligible_count": 50,
        "duplicate_exclusion_count": 0,
        "capacity_exclusion_count": 0,
        "bank_policy_count": 50,
        "shortage_to_minimum_target": 0,
        "tier_e_count": 22,
        "tier_q_count": 28,
        "exact_standalone_count": 46,
        "marginally_accepted_book_count": 4,
        "authoritative_promotion_count": 0,
        "tier_g_count": 0,
        "xfa_paths_started": 0,
    }
    assert all(row["evidence_tier"] in {"E", "Q"} for row in bank["policies"])
    assert all(row["tier_g_claimed"] is False for row in bank["policies"])
    assert all(row["xfa_paths_started"] == 0 for row in bank["policies"])
    assert len(
        {row["fingerprints"]["policy_spec_hash"] for row in bank["policies"]}
    ) == 50
    assert len(
        {row["fingerprints"]["episode_behavior_hash"] for row in bank["policies"]}
    ) == 50

    book = next(
        row
        for row in bank["policies"]
        if row["source_kind"] == "MARGINALLY_ACCEPTED_BOOK"
    )
    assert book["account"] == {"label": "50K", "account_size_usd": 50_000}
    assert book["horizons"]["5"]["overall"]["normal"]["pass_count"] == 1
    assert book["horizons"]["10"]["held_out_development"]["stressed"]["pass_count"] == 1
    assert book["evidence_roles"]["design_blocks"] == ["B1", "B2"]
    assert book["evidence_roles"]["held_out_development_blocks"] == ["B3", "B4"]


def test_passing_book_without_accepted_marginal_value_is_excluded() -> None:
    rejected = _book("rejected-book", marginally_accepted=False)
    bank = build_autonomous_combine_pass_observed_bank(
        _candidate_bank([_exact_candidate("only-exact")]),
        _marginal_composite([rejected]),
    )

    assert bank["counts"]["bank_policy_count"] == 1
    assert bank["counts"]["rejected_non_marginal_pass_book_count"] == 1
    assert bank["counts"]["shortage_to_minimum_target"] == 49
    assert bank["status"].endswith("SHORTAGE")
    assert bank["next_action"] == (
        "REPLENISH_WITH_MATERIALLY_DISTINCT_PASS_OBSERVED_POLICIES"
    )


def test_passes_in_different_horizons_do_not_create_a_paired_book_pass() -> None:
    book = _book("split-pass")
    book["summaries"]["STRESSED_1_5X"]["5"]["pass_count"] = 0
    book["summaries"]["STRESSED_1_5X"]["5"]["pass_rate"] = 0.0
    book["summaries"]["NORMAL"]["10"]["pass_count"] = 0
    book["summaries"]["NORMAL"]["10"]["pass_rate"] = 0.0
    for scenario in ("NORMAL", "STRESSED_1_5X"):
        book["summaries"][scenario]["20"]["pass_count"] = 0
        book["summaries"][scenario]["20"]["pass_rate"] = 0.0
    book = _self_hashed(book)

    bank = build_autonomous_combine_pass_observed_bank(
        _candidate_bank([]), _marginal_composite([book])
    )

    assert bank["counts"]["bank_policy_count"] == 0
    assert bank["counts"]["shortage_to_minimum_target"] == 50


def test_duplicate_episode_behavior_retains_the_tier_q_policy() -> None:
    lower = _exact_candidate("a-tier-e", tier_q=False, behavior="first-receipt")
    higher = _exact_candidate("z-tier-q", tier_q=True, behavior="second-receipt")

    bank = build_autonomous_combine_pass_observed_bank(
        _candidate_bank([lower, higher]), _marginal_composite([])
    )

    assert bank["policy_ids"] == ["z-tier-q"]
    assert bank["counts"]["duplicate_exclusion_count"] == 1
    assert bank["deduplication"]["exclusions"] == [
        {
            "policy_id": "a-tier-e",
            "reason": "DUPLICATE_EPISODE_BEHAVIOR",
            "retained_policy_id": "z-tier-q",
            "duplicate_fingerprint": bank["policies"][0]["fingerprints"][
                "episode_behavior_hash"
            ],
        }
    ]


def test_book_receipt_and_policy_metadata_do_not_define_behavior() -> None:
    candidates = [
        _exact_candidate("component-a"),
        _exact_candidate("component-b"),
    ]
    first = _book(
        "book-a", components=["component-a", "component-b"]
    )
    second = _book(
        "book-b", components=["component-a", "component-b"]
    )

    bank = build_autonomous_combine_pass_observed_bank(
        _candidate_bank(candidates), _marginal_composite([first, second])
    )

    retained_books = [
        row
        for row in bank["policies"]
        if row["source_kind"] == "MARGINALLY_ACCEPTED_BOOK"
    ]
    assert [row["policy_id"] for row in retained_books] == ["book-a"]
    exclusion = next(
        row
        for row in bank["deduplication"]["exclusions"]
        if row["policy_id"] == "book-b"
    )
    assert exclusion["reason"] == "DUPLICATE_EPISODE_BEHAVIOR"


def test_book_tier_q_is_recalculated_and_not_inferred_from_source_label() -> None:
    source_candidate = _exact_candidate("known-q")
    unsupported = _book(
        "unsupported",
        components=["unknown-a", "unknown-b"],
        behavior_offset=1.0,
    )
    supported = _book("supported", components=["known-q"])

    bank = build_autonomous_combine_pass_observed_bank(
        _candidate_bank([source_candidate]),
        _marginal_composite([unsupported, supported]),
    )
    by_id = {row["policy_id"]: row for row in bank["policies"]}

    assert by_id["unsupported"]["evidence_tier"] == "E"
    assert by_id["unsupported"]["tier_q_gate_results"][
        "tier_q_components_only"
    ] is False
    assert by_id["supported"]["evidence_tier"] == "Q"


def test_tier_q_standalone_uses_best_safe_cell_as_primary_evidence() -> None:
    candidate = _exact_candidate("candidate-q")
    observed = deepcopy(candidate["best_observed_pass_cell"])
    observed["horizon_trading_days"] = 5
    observed["normal"]["net_total_usd"] = 99_999.0
    candidate["best_observed_pass_cell"] = observed

    bank = build_autonomous_combine_pass_observed_bank(
        _candidate_bank([candidate]), _marginal_composite([])
    )
    row = bank["policies"][0]

    assert row["evidence_tier"] == "Q"
    assert row["fingerprints"]["primary_evidence_cell"] == "best_safe_cell"
    assert row["horizons"]["10"]["normal_and_stressed_pass_observed"] is True
    assert row["horizons"]["5"]["overall"] is None


def test_hash_drift_and_invalid_capacity_fail_closed() -> None:
    source = _candidate_bank([_exact_candidate("candidate")])
    drifted = deepcopy(source)
    drifted["status"] = "DRIFTED"

    with pytest.raises(AutonomousCombinePassBankError, match="hash mismatch"):
        build_autonomous_combine_pass_observed_bank(
            drifted, _marginal_composite([])
        )
    with pytest.raises(AutonomousCombinePassBankError, match="between 50 and 100"):
        build_autonomous_combine_pass_observed_bank(
            source, _marginal_composite([]), capacity=49
        )


def _exact_candidate(
    candidate_id: str,
    *,
    tier_q: bool = True,
    behavior: str | None = None,
    economic_offset: float = 0.0,
) -> dict[str, object]:
    behavior = behavior or candidate_id
    cell = {
        "candidate_id": candidate_id,
        "account_label": "50K",
        "account_size_usd": 50_000,
        "integer_quantity_tier": 3,
        "risk_governor_mode": "STATIC",
        "horizon_trading_days": 10,
        "full_coverage_start_count": 20,
        "data_censored_start_count": 0,
        "normal": _scenario("normal", behavior, economic_offset),
        "stressed": _scenario("stressed", behavior, economic_offset),
        "cell_hash": f"cell-{candidate_id}",
    }
    return {
        "candidate_id": candidate_id,
        "candidate_fingerprint": f"spec-{candidate_id}",
        "realized_behavioral_fingerprint": f"realized-{behavior}",
        "qd_cell": f"qd-{candidate_id}",
        "source_exact_result_hash": f"exact-result-{candidate_id}",
        "observed_passes": {"normal_and_stressed_same_cell": True},
        "best_observed_pass_cell": cell,
        "best_safe_cell": cell if tier_q else None,
        "tier_q_contract_cleared": tier_q,
        "compact_evidence_bundle": {"bundle_hash": f"bundle-{candidate_id}"},
        "authoritative_promotion_status": None,
    }


def _scenario(
    scenario: str, behavior: str, economic_offset: float
) -> dict[str, object]:
    return {
        "episode_count": 20,
        "pass_count": 2,
        "pass_rate": 0.10,
        "mll_breach_count": 0,
        "mll_breach_rate": 0.0,
        "consistency_compliance_rate": 0.80,
        "net_total_usd": 5_000.0 + economic_offset,
        "net_median_usd": 100.0,
        "target_progress_p25": -0.10,
        "target_progress_median": 0.30,
        "minimum_mll_buffer_usd": 500.0,
        "median_days_to_target": 7.0,
        "episode_path_hash": f"{scenario}-path-{behavior}",
    }


def _book(
    policy_id: str,
    *,
    marginally_accepted: bool = True,
    components: list[str] | None = None,
    behavior_offset: float = 0.0,
) -> dict[str, object]:
    summaries = {
        scenario: {
            str(horizon): _book_scenario(scenario, horizon, behavior_offset)
            for horizon in (5, 10, 20)
        }
        for scenario in ("NORMAL", "STRESSED_1_5X")
    }
    roles = {
        role: deepcopy(summaries)
        for role in ("DESIGN", "HELD_OUT_DEVELOPMENT")
    }
    core = {
        "policy_id": policy_id,
        "policy_spec_hash": f"book-spec-{policy_id}",
        "account_label": "50K",
        "component_ids": components
        or [f"component-a-{policy_id}", f"component-b-{policy_id}"],
        "marginally_accepted": marginally_accepted,
        "marginal_contribution": {"accepted": marginally_accepted},
        "computed_development_tier": "Q_BOOK_DIAGNOSTIC",
        "governor_profile_id": "governor-1",
        "episode_evidence": {
            "record_count": 144,
            "receipt_hash": f"episode-receipt-{policy_id}",
        },
        "completed_episode_count": 144,
        "signal_recomputation_performed": False,
        "quantity_tiers_materialized_before_book_replay": True,
        "additional_quantity_scaling": False,
        "summaries": summaries,
        "summaries_by_role": roles,
        "selection_role_contract": {
            "DESIGN": ["B1", "B2"],
            "HELD_OUT_DEVELOPMENT": ["B3", "B4"],
        },
        "authoritative_promotion_status": None,
        "xfa_paths_started": 0,
        "database_writes": 0,
        "registry_writes": 0,
    }
    return _self_hashed(core)


def _book_scenario(
    scenario: str, horizon: int, behavior_offset: float
) -> dict[str, object]:
    return {
        "episode_count": 20,
        "full_coverage_start_count": 20,
        "data_censored_count": 0,
        "pass_count": 1,
        "pass_rate": 0.05,
        "net_total": 1_000.0 + behavior_offset,
        "net_median": 50.0,
        "mll_breach_count": 0,
        "mll_breach_rate": 0.0,
        "minimum_mll_buffer": 400.0,
        "consistency_rate": 0.75,
        "target_progress_p25": -0.05,
        "target_progress_median": 0.20,
        "median_days_to_target": float(horizon),
        "blocks_with_passes": ["B3", "B4"],
        "episode_path_hash": f"book-{scenario}-{horizon}",
    }


def _candidate_bank(candidates: list[dict[str, object]]) -> dict[str, object]:
    return _self_hashed(
        {
            "schema": CANDIDATE_BANK_SCHEMA,
            "status": "COMPLETE_READ_ONLY_DEVELOPMENT_CLASSIFICATION",
            "candidates": candidates,
            "counts": {
                "authoritative_promotion_count": 0,
                "xfa_paths_started": 0,
            },
            "promotion_status": None,
            "evidence_role": "VIEWED_DEVELOPMENT_ONLY",
            "xfa_paths_started": 0,
            "broker_connections": 0,
            "orders": 0,
        }
    )


def _marginal_composite(books: list[dict[str, object]]) -> dict[str, object]:
    return _self_hashed(
        {
            "schema": COMPOSITE_SCHEMA,
            "status": "COMPLETE_RECONCILED_MARGINAL_COMBINE_BOOK_SHARDS",
            "book_results": books,
            "counts": {
                "authoritative_promotion_count": 0,
                "xfa_paths_started": 0,
                "database_writes": 0,
                "registry_writes": 0,
            },
            "promotion_status": None,
            "evidence_role": "VIEWED_DEVELOPMENT_ONLY",
            "xfa_paths_started": 0,
            "database_writes": 0,
            "registry_writes": 0,
            "broker_connections": 0,
            "orders": 0,
        }
    )


def _self_hashed(core: dict[str, object]) -> dict[str, object]:
    payload = {key: value for key, value in core.items() if key != "result_hash"}
    return {**payload, "result_hash": stable_hash(payload)}
