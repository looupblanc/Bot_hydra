from __future__ import annotations

from copy import deepcopy

import pytest

from hydra.economic_evolution.schema import stable_hash
from hydra.production.autonomous_combine_candidate_bank import (
    CONCENTRATION_RECEIPT_SCHEMA,
    AutonomousCombineCandidateBankError,
    build_autonomous_combine_candidate_bank,
)
from hydra.production.autonomous_exact_replay import EXACT_REPLAY_SCHEMA


def test_tier_q_does_not_require_concentration() -> None:
    candidate_ids = [f"candidate-{index:02d}" for index in range(32)]
    passing = _candidate(candidate_ids[0], cell=_cell(block_passes={"B4": 3}))
    exact = _exact_result(
        candidate_ids,
        [passing, *(_candidate(value) for value in candidate_ids[1:])],
    )

    bank = build_autonomous_combine_candidate_bank(exact)
    row = next(
        value for value in bank["candidates"] if value["candidate_id"] == candidate_ids[0]
    )

    assert bank["counts"]["candidate_with_normal_and_stressed_pass_count"] == 1
    assert bank["counts"]["tier_q_contract_cleared_count"] == 1
    assert bank["counts"]["g_ready_count"] == 0
    assert row["tier_q_contract_cleared"] is True
    assert row["computed_development_tier"] == "Q"
    assert row["concentration_diagnostic"]["block_concentration_cleared"] is False
    assert row["concentration_diagnostic"]["tier_q_gate"] is False
    assert row["g_ready"] is False
    assert row["authoritative_promotion_status"] is None
    assert bank["promotion_status"] is None
    assert bank["counts"]["authoritative_promotion_count"] == 0
    assert bank["counts"]["xfa_paths_started"] == 0


def test_incomplete_compact_bundle_prevents_tier_q() -> None:
    candidate_ids = [f"candidate-{index:02d}" for index in range(32)]
    passing = _candidate(candidate_ids[0], cell=_cell(block_passes={"B1": 1, "B2": 1}))
    passing["source_event_evidence"].pop("uncompressed_sha256")
    exact = _exact_result(
        candidate_ids,
        [passing, *(_candidate(value) for value in candidate_ids[1:])],
    )

    bank = build_autonomous_combine_candidate_bank(exact)
    row = next(
        value for value in bank["candidates"] if value["candidate_id"] == candidate_ids[0]
    )

    assert row["compact_evidence_bundle"]["complete"] is False
    assert row["tier_q_gate_results"]["compact_evidence_bundle_complete"] is False
    assert row["tier_q_contract_cleared"] is False
    assert row["classification_status"] == (
        "OBSERVED_COMBINE_PASS_DEVELOPMENT_NOT_TIER_Q"
    )


def test_g_ready_requires_complete_concentration_and_final_development_receipt() -> None:
    candidate_ids = [f"candidate-{index:02d}" for index in range(32)]
    candidate = _candidate(
        candidate_ids[0],
        cell=_cell(normal_passes=2, stressed_passes=2, block_passes={"B1": 1, "B2": 1}),
    )
    exact = _exact_result(
        candidate_ids,
        [candidate, *(_candidate(value) for value in candidate_ids[1:])],
    )
    receipt_core = {
        "schema": CONCENTRATION_RECEIPT_SCHEMA,
        "candidate_id": candidate_ids[0],
        "source_exact_result_hash": exact["result_hash"],
        "maximum_single_day_profit_share": 0.20,
        "maximum_single_trade_profit_share": 0.20,
        "maximum_single_event_profit_share": 0.20,
        "final_development_evidence_hash": "final-development-hash",
    }
    receipt = {**receipt_core, "receipt_hash": stable_hash(receipt_core)}

    bank = build_autonomous_combine_candidate_bank(
        exact,
        concentration_receipts={candidate_ids[0]: receipt},
    )
    row = next(
        value for value in bank["candidates"] if value["candidate_id"] == candidate_ids[0]
    )

    assert row["tier_q_contract_cleared"] is True
    assert row["g_ready"] is True
    assert row["classification_status"] == (
        "G_READY_CLASSIFICATION_AWAITING_AUTHORITATIVE_WRITER"
    )
    assert bank["counts"]["g_ready_count"] == 1
    assert bank["counts"]["authoritative_promotion_count"] == 0


def test_duplicate_candidate_ids_fail_closed() -> None:
    candidate_ids = [f"candidate-{index:02d}" for index in range(32)]
    candidate_ids[-1] = candidate_ids[0]
    rows = [_candidate(value) for value in candidate_ids]
    exact = _exact_result(candidate_ids, rows)

    with pytest.raises(
        (AutonomousCombineCandidateBankError, RuntimeError),
        match="duplicate|more than one|differ",
    ):
        build_autonomous_combine_candidate_bank(exact)


def test_forbidden_xfa_counter_fails_closed() -> None:
    candidate_ids = [f"candidate-{index:02d}" for index in range(32)]
    exact = _exact_result(candidate_ids, [_candidate(value) for value in candidate_ids])
    changed = deepcopy(exact)
    changed["counters"]["xfa_paths_started"] = 1
    changed.pop("result_hash")
    changed["result_hash"] = stable_hash(changed)

    with pytest.raises(RuntimeError, match="forbidden counter"):
        build_autonomous_combine_candidate_bank(changed)


def _candidate(
    candidate_id: str,
    *,
    cell: dict[str, object] | None = None,
) -> dict[str, object]:
    return {
        "candidate_id": candidate_id,
        "candidate_fingerprint": f"candidate-fingerprint-{candidate_id}",
        "realized_behavioral_fingerprint": f"behavior-{candidate_id}",
        "qd_cell": f"qd-{candidate_id}",
        "source_event_evidence": {
            "sha256": f"compressed-{candidate_id}",
            "uncompressed_sha256": f"content-{candidate_id}",
            "record_count": 20,
        },
        "candidate_result_hash": f"result-{candidate_id}",
        "session_contract": {"event_violation_count": 0},
        "frontier": [] if cell is None else [{**cell, "candidate_id": candidate_id}],
        "promotion_status": None,
        "evidence_tier": "E_DIAGNOSTIC_DEVELOPMENT",
    }


def _cell(
    *,
    normal_passes: int = 3,
    stressed_passes: int = 3,
    block_passes: dict[str, int],
) -> dict[str, object]:
    return {
        "account_label": "50K",
        "account_size_usd": 50_000,
        "integer_quantity_tier": 3,
        "risk_governor_mode": "CAUSAL_STATIC_STOP_RISK_GOVERNOR",
        "horizon_trading_days": 10,
        "full_coverage_start_count": 20,
        "data_censored_start_count": 0,
        "legally_executable": True,
        "account_rule_compliant": True,
        "hard_compliance_failure_count": 0,
        "normal": _scenario("normal", normal_passes, block_passes),
        "stressed": _scenario("stressed", stressed_passes, block_passes),
    }


def _scenario(
    scenario: str, passes: int, block_passes: dict[str, int]
) -> dict[str, object]:
    return {
        "episode_count": 20,
        "pass_count": passes,
        "pass_rate": passes / 20,
        "mll_breach_count": 1,
        "mll_breach_rate": 0.05,
        "consistency_compliance_rate": 0.80,
        "net_total_usd": 5_000.0,
        "net_median_usd": 100.0,
        "target_progress_p25": -0.10,
        "target_progress_median": 0.25,
        "minimum_mll_buffer_usd": 500.0,
        "median_days_to_target": 5.0,
        "episode_path_hash": f"{scenario}-episode-path-hash",
        "by_block": {
            block: {"pass_count": count}
            for block, count in block_passes.items()
        },
    }


def _exact_result(
    candidate_ids: list[str], candidates: list[dict[str, object]]
) -> dict[str, object]:
    counters = {
        "source_bank_entry_count": len(candidate_ids),
        "source_unique_candidate_count": len(candidate_ids),
        "qd_selected_candidate_count": len(candidate_ids),
        "canonical_candidates_reconstructed": len(candidate_ids),
        "canonical_event_records_reconstructed": len(candidate_ids) * 20,
        "legal_account_horizon_cells": sum(
            len(value["frontier"]) for value in candidates
        ),
        "contract_illegal_account_horizon_cells": 0,
        "candidate_horizon_full_coverage_start_count": 20,
        "candidate_horizon_data_censored_start_count": 0,
        "exact_account_replays": 40,
        "exact_normal_account_replays": 20,
        "exact_stressed_account_replays": 20,
        "summary_scaled_episode_screens": 0,
        "promotion_count": 0,
        "xfa_paths_started": 0,
        "data_purchase_count": 0,
        "q4_access_count_delta": 0,
        "broker_connections": 0,
        "orders": 0,
    }
    core = {
        "schema": EXACT_REPLAY_SCHEMA,
        "status": "COMPLETE_EXACT_CAUSAL_ACCOUNT_SIZE_RACE",
        "branch_id": "EXACT_0029_ACCOUNT_SIZE_RACE",
        "source_campaign_id": "hydra_fast_pass_factory_0029",
        "source_manifest": {
            "manifest_hash": "manifest-hash",
            "file_sha256": "manifest-file-sha",
        },
        "source_banks": {"entry_count": len(candidate_ids), "files": []},
        "official_rule_snapshot": {"parsed_rule_hash": "rule-hash"},
        "frozen_grid": {"grid_hash": "grid-hash"},
        "selection": {
            "offset": 0,
            "selected_count": len(candidate_ids),
            "selected_candidate_ids": candidate_ids,
            "outcome_roles": "VIEWED_DEVELOPMENT_ONLY",
        },
        "results": candidates,
        "best_exact_frontier_point": None,
        "counters": counters,
        "evidence_tier": "E",
        "promotion_status": None,
        "result_hash_excludes_runtime_telemetry": True,
    }
    return {**core, "result_hash": stable_hash(core)}
