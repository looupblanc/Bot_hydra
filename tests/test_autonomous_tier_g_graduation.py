from __future__ import annotations

import copy

import pytest

from hydra.economic_evolution.schema import stable_hash
from hydra.production import autonomous_tier_g_controls as controls
from hydra.production import autonomous_tier_g_graduation as graduation


def _summary(
    *,
    passes: int = 2,
    breaches: int = 0,
    net: float = 2_000.0,
    consistency_compliant: int = 6,
    salt: str = "normal",
) -> dict[str, object]:
    episodes = 8
    pass_by_block = {"B1": 0, "B2": 0, "B3": 0, "B4": 0}
    for index in range(passes):
        pass_by_block[f"B{index % 4 + 1}"] += 1
    breach_by_block = {"B1": 0, "B2": 0, "B3": 0, "B4": breaches}
    terminal = {"PASSED": passes, "TIMEOUT": episodes - passes - breaches}
    if breaches:
        terminal["MLL_BREACHED"] = breaches
    by_block = {
        block: {
            "episode_count": 2,
            "pass_count": pass_by_block[block],
            "mll_breach_count": breach_by_block[block],
            "net_total_usd": net / 4.0,
            "target_progress_median": 0.25,
        }
        for block in ("B1", "B2", "B3", "B4")
    }
    return {
        "episode_count": episodes,
        "pass_count": passes,
        "pass_rate": passes / episodes,
        "mll_breach_count": breaches,
        "mll_breach_rate": breaches / episodes,
        "consistency_compliance_rate": consistency_compliant / episodes,
        "net_total_usd": net,
        "net_median_usd": net / episodes,
        "target_progress_p25": 0.10,
        "target_progress_median": 0.40,
        "minimum_mll_buffer_usd": -10.0 if breaches else 500.0,
        "median_days_to_target": 12.0,
        "terminal_distribution": terminal,
        "by_block": by_block,
        "episode_path_hash": stable_hash({"scenario": salt}),
        "requested_quantity_total": 80,
        "admitted_quantity_total": 80,
        "size_reduced_count": 0,
        "risk_or_contract_rejection_count": 0,
        "silent_size_reduction": False,
    }


def _ledger_scenario(salt: str) -> dict[str, object]:
    return {
        "record_count": 100,
        "unique_event_count": 100,
        "unique_trade_count": 100,
        "unique_session_day_count": 20,
        "positive_profit_total_usd": 4_000.0,
        "net_total_usd": 2_000.0,
        "maximum_single_day_profit_share": 0.20,
        "maximum_single_trade_profit_share": 0.05,
        "maximum_single_event_profit_share": 0.05,
        "maximum_single_trade_loss_share": 0.08,
        "maximum_single_day_loss_share": 0.20,
        "ledger_hash": stable_hash({"ledger": salt}),
        "event_inventory_hash": stable_hash({"events": salt}),
    }


def _concentration() -> dict[str, object]:
    normal = _ledger_scenario("normal")
    stressed = _ledger_scenario("stressed")
    core: dict[str, object] = {
        "denominator": "UNIQUE_COMPLETED_CAUSAL_TRAJECTORY_LEDGER",
        "event_trade_mapping": "ONE_COMPLETED_TRAJECTORY_PER_EVENT",
        "rolling_start_multiplicity": 0,
        "maximum_allowed_profit_share": 0.50,
        "normal": normal,
        "stressed": stressed,
        "worst_case_maximums": {
            "maximum_single_day_profit_share": 0.20,
            "maximum_single_trade_profit_share": 0.05,
            "maximum_single_event_profit_share": 0.05,
        },
        "cleared": True,
    }
    return {**core, "concentration_hash": stable_hash(core)}


def _candidate(
    candidate_id: str = "hazard-ready",
    *,
    ready: bool = True,
    breaches: int = 0,
    consistency_compliant: int = 6,
) -> dict[str, object]:
    normal = _summary(
        breaches=breaches,
        consistency_compliant=consistency_compliant,
        salt=f"{candidate_id}:normal",
    )
    stressed = _summary(
        breaches=breaches,
        consistency_compliant=consistency_compliant,
        net=1_800.0,
        salt=f"{candidate_id}:stressed",
    )
    synthetic_controls: list[dict[str, object]] = []
    synthetic_stressed_base = 400.0 if ready else 2_000.0
    for index, offset in enumerate((25, 50, 75), start=1):
        control_core: dict[str, object] = {
            "control_id": f"CIRCULAR_SHIFT_{index:02d}_OFFSET_{offset}",
            "control_type": "CIRCULAR_OUTCOME_PATH_SHIFT_SYNTHETIC_NULL",
            "synthetic_non_deployable": True,
            "may_promote_candidate": False,
            "offset": offset,
            "event_count": 100,
            "exposure_count_matched": True,
            "normal": _summary(
                passes=1,
                net=500.0 + index,
                salt=f"control-{index}:normal",
            ),
            "stressed": _summary(
                passes=1,
                net=synthetic_stressed_base + index,
                salt=f"control-{index}:stressed",
            ),
            "normal_control_ledger_hash": stable_hash(
                {"control": index, "scenario": "normal"}
            ),
            "stressed_control_ledger_hash": stable_hash(
                {"control": index, "scenario": "stressed"}
            ),
        }
        synthetic_controls.append(
            {**control_core, "control_hash": stable_hash(control_core)}
        )
    comparison = {
        "observed_stressed_pass_count": 2,
        "median_synthetic_stressed_pass_count": 1.0,
        "observed_stressed_net_usd": 1_800.0,
        "median_synthetic_stressed_net_usd": synthetic_stressed_base + 2.0,
        "pass_count_not_worse_than_median_synthetic": True,
        "stressed_net_not_worse_than_median_synthetic": ready,
        "interpretation": (
            "SYNTHETIC_TEMPORAL_ALIGNMENT_CONTROL_ONLY_NOT_ALPHA_CONFIRMATION"
        ),
    }
    concentration = _concentration()
    account_policy = {
        "policy_id": candidate_id,
        "maximum_concurrent_sleeves": 1,
        "risk_governor_mode": "CONTRACT_ONLY_UNIFORM_SCALE",
    }
    markets = ["NQ"]
    final_core: dict[str, object] = {
        "schema": "hydra_tier_g_final_development_evidence_v1",
        "candidate_id": candidate_id,
        "candidate_fingerprint": "1" * 64,
        "behavioral_fingerprint": "2" * 64,
        "qd_cell": "test_qd_cell",
        "source_exact_result_hash": "3" * 64,
        "source_manifest_hash": "4" * 64,
        "frozen_grid_hash": "5" * 64,
        "official_rule_snapshot_hash": "6" * 64,
        "source_candidate_result_hash": "7" * 64,
        "selected_cell_hash": "8" * 64,
        "account_label": "50K",
        "account_size_usd": 50_000,
        "market_inventory_hash": stable_hash(markets),
        "frozen_account_policy_hash": stable_hash(account_policy),
        "policy_sleeve_inventory_hash": stable_hash([candidate_id]),
        "source_event_receipt": {
            "relative_path": f"events/{candidate_id}.jsonl.gz",
            "record_count": 100,
            "sha256": "9" * 64,
            "uncompressed_sha256": "a" * 64,
        },
        "normal_unique_ledger_hash": concentration["normal"]["ledger_hash"],
        "stressed_unique_ledger_hash": concentration["stressed"]["ledger_hash"],
        "identity_normal_episode_path_hash": normal["episode_path_hash"],
        "identity_stressed_episode_path_hash": stressed["episode_path_hash"],
        "concentration_hash": concentration["concentration_hash"],
        "control_hashes": [row["control_hash"] for row in synthetic_controls],
        "control_comparison_hash": stable_hash(comparison),
        "source_block_concentration_hash": "b" * 64,
        "frozen_gate_thresholds": {
            "maximum_day_trade_event_profit_share": 0.50,
            "maximum_block_pass_share": 0.75,
            "minimum_normal_passes": 2,
            "minimum_stressed_passes": 2,
            "minimum_temporal_contexts": 2,
            "synthetic_control_comparison": "NOT_WORSE_THAN_MEDIAN",
        },
        "evidence_role": "VIEWED_FINAL_DEVELOPMENT_ONLY",
        "independent_confirmation_claimed": False,
    }
    final = {
        **final_core,
        "final_development_evidence_hash": stable_hash(final_core),
    }
    concentration_receipt = controls._concentration_receipt(
        candidate_id=candidate_id,
        source_exact_result_hash="3" * 64,
        concentration=concentration,
        final_development_evidence_hash=final["final_development_evidence_hash"],
    )
    gates = {
        "tier_q_source": True,
        "identity_best_parent_reconciled": True,
        "multiple_normal_and_stressed_passes": True,
        "multiple_temporal_contexts": True,
        "block_concentration_le_75pct": True,
        "unique_ledger_day_trade_event_concentration_le_50pct": True,
        "synthetic_controls_complete_and_exposure_matched": True,
        "not_worse_than_median_synthetic_pass_count": True,
        "not_worse_than_median_synthetic_stressed_net": ready,
        "final_development_evidence_hash_complete": True,
    }
    core: dict[str, object] = {
        "candidate_id": candidate_id,
        "candidate_fingerprint": "1" * 64,
        "behavioral_fingerprint": "2" * 64,
        "qd_cell": "test_qd_cell",
        "account_label": "50K",
        "account_size_usd": 50_000,
        "markets": markets,
        "market_inventory_hash": stable_hash(markets),
        "frozen_account_policy": account_policy,
        "frozen_account_policy_hash": stable_hash(account_policy),
        "complete_account_policy": True,
        "policy_sleeve_ids": [candidate_id],
        "selected_horizon_trading_days": 20,
        "selected_cell_hash": "8" * 64,
        "source_exact_result_hash": "3" * 64,
        "identity_best_parent": {
            "control_role": "IDENTITY_AND_STANDALONE_BEST_PARENT",
            "reconciled": True,
            "normal": normal,
            "stressed": stressed,
        },
        "unique_ledger_concentration": concentration,
        "synthetic_controls": synthetic_controls,
        "control_comparison": comparison,
        "final_development_evidence": final,
        "concentration_receipt": concentration_receipt,
        "g_control_gate_results": gates,
        "g_control_ready": ready,
        "computed_development_tier": (
            "G_CONTROL_READY" if ready else "Q_CONTROL_EVALUATED"
        ),
        "authoritative_promotion_status": None,
        "independent_confirmation_claimed": False,
        "exact_account_replay_count": 64,
        "xfa_paths_started": 0,
        "registry_writes": 0,
        "database_writes": 0,
        "broker_connections": 0,
        "orders": 0,
    }
    return {**core, "result_hash": stable_hash(core)}


def _composite(*candidates: dict[str, object]) -> dict[str, object]:
    rows = sorted(candidates, key=lambda row: str(row["candidate_id"]))
    ready = [str(row["candidate_id"]) for row in rows if row["g_control_ready"]]
    core: dict[str, object] = {
        "schema": controls.COMPOSITE_SCHEMA,
        "status": "COMPLETE_RECONCILED_TIER_G_CONTROL_SHARDS",
        "source_candidate_bank_hash": "c" * 64,
        "source_exact_composite_hash": "d" * 64,
        "source_manifest_hash": "e" * 64,
        "frozen_grid": {"grid_hash": "f" * 64},
        "official_rule_snapshot": {"parsed_rule_hash": "0" * 64},
        "source_bank_receipt": {"receipt_hash": "1" * 64},
        "control_contract": {
            "candidate_maximum": 5,
            "candidate_selection": "TEST",
            "unique_ledger_denominator": "TEST",
            "maximum_day_trade_event_profit_share": 0.50,
            "control_type": "CIRCULAR_OUTCOME_PATH_SHIFT_SYNTHETIC_NULL",
            "control_shift_count": 3,
            "control_is_deployable": False,
            "control_can_promote": False,
            "identity_best_parent_required": True,
        },
        "shard_receipts": [],
        "candidate_results": rows,
        "concentration_receipts": {
            str(row["candidate_id"]): row["concentration_receipt"] for row in rows
        },
        "counts": {
            "source_candidate_count": len(rows),
            "selected_candidate_count": len(rows),
            "exact_account_replay_count": sum(
                int(row["exact_account_replay_count"]) for row in rows
            ),
            "synthetic_control_count": len(rows) * 3,
            "g_control_ready_count": len(ready),
            "authoritative_promotion_count": 0,
            "xfa_paths_started": 0,
            "registry_writes": 0,
            "database_writes": 0,
            "q4_access_count_delta": 0,
            "data_purchase_count": 0,
            "broker_connections": 0,
            "orders": 0,
        },
        "candidate_ids": {"g_control_ready": ready},
        "evidence_role": "VIEWED_FINAL_DEVELOPMENT_ONLY",
        "promotion_status": None,
        "independent_confirmation_claimed": False,
        "next_action": "TEST",
    }
    return {**core, "result_hash": stable_hash(core)}


def _rehash(payload: dict[str, object], field: str = "result_hash") -> None:
    core = {key: value for key, value in payload.items() if key != field}
    payload[field] = stable_hash(core)


def test_ready_single_sleeve_becomes_a_development_book_only() -> None:
    source = _composite(_candidate())
    result = graduation.build_graduated_development_books(source)

    assert result == graduation.build_graduated_development_books(source)
    graduation.verify_tier_g_development_graduation(result)
    assert result["candidate_ids"] == {
        "graduated_development_books": ["hazard-ready"],
        "retained_tier_q": [],
    }
    receipt = result["graduated_development_books"][0]
    assert receipt["graduation_status"] == "GRADUATED_DEVELOPMENT_BOOK"
    assert receipt["evidence_tier"] == "G"
    assert receipt["complete_account_policy"] is True
    assert receipt["sleeve_count"] == 1
    assert receipt["sleeve_ids"] == ["hazard-ready"]
    assert receipt["markets"] == ["NQ"]
    assert receipt["account_size_usd"] == 50_000
    assert receipt["independent_confirmation_claimed"] is False
    assert receipt["xfa_status"] == "NOT_STARTED"
    assert receipt["xfa_book_hash"] is None
    assert receipt["xfa_profile_hash"] is None
    assert receipt["orders"] == receipt["database_writes"] == 0


def test_control_failed_candidate_remains_tier_q_without_receipt() -> None:
    source = _composite(_candidate(ready=False))
    result = graduation.build_graduated_development_books(source)

    assert result["graduated_development_books"] == []
    assert result["candidate_ids"]["retained_tier_q"] == ["hazard-ready"]
    assert "SOURCE_G_CONTROL_READY" in result["not_graduated"][0]["reason_codes"]


def test_mll_or_consistency_failure_cannot_graduate_a_control_ready_row() -> None:
    mll = graduation.build_graduated_development_books(
        _composite(_candidate(candidate_id="mll", breaches=1))
    )
    consistency = graduation.build_graduated_development_books(
        _composite(_candidate(candidate_id="consistency", consistency_compliant=1))
    )

    assert mll["graduated_development_books"] == []
    assert "MLL_CLEARED" in mll["not_graduated"][0]["reason_codes"]
    assert consistency["graduated_development_books"] == []
    assert "CONSISTENCY_CLEARED" in consistency["not_graduated"][0]["reason_codes"]


def test_rehashed_final_development_tamper_still_fails_closed() -> None:
    source = _composite(_candidate())
    candidate = source["candidate_results"][0]
    candidate["final_development_evidence"]["normal_unique_ledger_hash"] = "0" * 64
    _rehash(candidate)
    source["concentration_receipts"][candidate["candidate_id"]] = candidate[
        "concentration_receipt"
    ]
    _rehash(source)

    with pytest.raises(
        graduation.AutonomousTierGGraduationError,
        match="final-development evidence hash drift",
    ):
        graduation.build_graduated_development_books(source)


def test_rehashed_control_comparison_tamper_is_recomputed_and_rejected() -> None:
    source = _composite(_candidate())
    candidate = source["candidate_results"][0]
    candidate["control_comparison"]["median_synthetic_stressed_net_usd"] = -999.0
    _rehash(candidate)
    _rehash(source)

    with pytest.raises(
        graduation.AutonomousTierGGraduationError,
        match="comparison does not reconcile",
    ):
        graduation.build_graduated_development_books(source)


def test_source_side_effect_is_rejected_even_when_outer_hash_is_recomputed() -> None:
    source = _composite(_candidate())
    source["counts"]["orders"] = 1
    _rehash(source)

    with pytest.raises(
        graduation.AutonomousTierGGraduationError,
        match="forbidden side effect: orders",
    ):
        graduation.build_graduated_development_books(source)


def test_receipt_market_tamper_is_rejected_after_rehash() -> None:
    result = graduation.build_graduated_development_books(
        _composite(_candidate())
    )
    receipt = copy.deepcopy(result["graduated_development_books"][0])
    receipt["markets"] = ["ES"]
    _rehash(receipt)

    with pytest.raises(
        graduation.AutonomousTierGGraduationError,
        match="market/account-policy binding",
    ):
        graduation.verify_graduated_development_book_receipt(receipt)
