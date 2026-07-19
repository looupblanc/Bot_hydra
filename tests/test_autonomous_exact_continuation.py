from __future__ import annotations

import pickle
from pathlib import Path

import pytest

from hydra.economic_evolution.schema import stable_hash
from hydra.production import autonomous_exact_continuation as continuation
from hydra.production.autonomous_exact_replay import EXACT_REPLAY_SCHEMA


def _bank_entry(index: int) -> dict[str, object]:
    candidate_id = f"candidate-{index:03d}"
    return {
        "candidate_id": candidate_id,
        "candidate": {
            "market": ("NQ", "YM", "CL", "GC")[index % 4],
            "mechanism": f"MECHANISM-{index % 7}",
        },
        "candidate_fingerprint": f"candidate-fingerprint-{index}",
        "realized_behavioral_fingerprint": f"behavior-{index}",
        "qd_cell": f"qd-{index}",
        "stressed_full_net": float(1_000 - index),
        "normal_full_net": float(1_100 - index),
        "stressed_design_net": float(500 - index),
        "positive_stressed_block_count": 2,
        "completed_event_count": 1,
        "exact_hashes": {"decision_hash": f"decision-{index}"},
        "event_evidence": {"relative_path": f"candidate-{index}.jsonl.gz"},
        "eligible_session_days": [1],
        "_source_wave": 1,
    }


def _exact_result(
    candidate_ids: list[str],
    *,
    offset: int,
    results: list[dict[str, object]] | None = None,
    source_unique_count: int | None = None,
) -> dict[str, object]:
    counters = {
        "source_bank_entry_count": source_unique_count or len(candidate_ids),
        "source_unique_candidate_count": source_unique_count or len(candidate_ids),
        "qd_selected_candidate_count": len(candidate_ids),
        "canonical_candidates_reconstructed": len(candidate_ids),
        "canonical_event_records_reconstructed": len(candidate_ids) * 2,
        "legal_account_horizon_cells": len(candidate_ids) * 72,
        "contract_illegal_account_horizon_cells": 0,
        "candidate_horizon_full_coverage_start_count": len(candidate_ids) * 3,
        "candidate_horizon_data_censored_start_count": 0,
        "exact_account_replays": len(candidate_ids) * 12,
        "exact_normal_account_replays": len(candidate_ids) * 6,
        "exact_stressed_account_replays": len(candidate_ids) * 6,
        "summary_scaled_episode_screens": 0,
        "promotion_count": 0,
        "xfa_paths_started": 0,
        "data_purchase_count": 0,
        "q4_access_count_delta": 0,
        "broker_connections": 0,
        "orders": 0,
    }
    core: dict[str, object] = {
        "schema": EXACT_REPLAY_SCHEMA,
        "status": "COMPLETE_EXACT_CAUSAL_ACCOUNT_SIZE_RACE",
        "source_campaign_id": "hydra_fast_pass_factory_0029",
        "source_manifest": {
            "manifest_hash": "manifest-hash",
            "file_sha256": "manifest-sha",
        },
        "source_banks": {"entry_count": source_unique_count or len(candidate_ids)},
        "official_rule_snapshot": {"parsed_rule_hash": "rules-hash"},
        "frozen_grid": {"grid_hash": "grid-hash"},
        "selection": {
            "offset": offset,
            "selected_count": len(candidate_ids),
            "selected_candidate_ids": candidate_ids,
            "outcome_roles": "VIEWED_DEVELOPMENT_ONLY",
        },
        "results": results or [],
        "best_exact_frontier_point": None,
        "counters": counters,
        "evidence_tier": "E",
        "promotion_status": None,
        "result_hash_excludes_runtime_telemetry": True,
    }
    return {**core, "result_hash": stable_hash(core)}


def _inventory(candidate_ids: list[str]) -> dict[str, object]:
    core: dict[str, object] = {
        "source_campaign_id": "hydra_fast_pass_factory_0029",
        "source_bank_entry_count": len(candidate_ids),
        "source_unique_candidate_count": len(candidate_ids),
        "source_qd_replayable_candidate_count": len(candidate_ids),
        "source_qd_excluded_candidate_count": 0,
        "sealed_initial_exact_candidate_count": 32,
        "remaining_exact_candidate_count": len(candidate_ids) - 32,
        "sealed_initial_candidate_ids": candidate_ids[:32],
        "remaining_candidate_ids": candidate_ids[32:],
        "ordered_replayable_candidate_ids_hash": stable_hash(candidate_ids),
        "source_bank_files": [],
    }
    return {**core, "source_inventory_hash": stable_hash(core)}


def _continuation_wrapper(
    exact: dict[str, object], inventory: dict[str, object]
) -> dict[str, object]:
    core: dict[str, object] = {
        "schema": continuation.CONTINUATION_SCHEMA,
        "status": "COMPLETE_READ_ONLY_EXACT_CONTINUATION_COHORT",
        "cohort_offset": exact["selection"]["offset"],  # type: ignore[index]
        "cohort_maximum": exact["selection"]["selected_count"],  # type: ignore[index]
        "candidate_ids": exact["selection"]["selected_candidate_ids"],  # type: ignore[index]
        "source_inventory": inventory,
        "exact_result": exact,
        "read_only_worker": True,
        "evidence_role": "VIEWED_DEVELOPMENT_ONLY",
        "evidence_tier": "E",
        "promotion_status": None,
        "xfa_paths_started": 0,
        "data_purchase_count": 0,
        "q4_access_count_delta": 0,
        "broker_connections": 0,
        "orders": 0,
        "result_hash_excludes_runtime_telemetry": True,
    }
    return {**core, "result_hash": stable_hash(core)}


def test_plan_uses_two_disjoint_cohorts_after_initial_32(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    entries = [_bank_entry(index) for index in range(80)]
    monkeypatch.setattr(
        continuation,
        "_load_banks",
        lambda _root: (
            entries,
            {
                "entry_count": 80,
                "unique_candidate_count": 80,
                "files": [],
            },
        ),
    )

    plan = continuation.plan_remaining_0029_exact_jobs(
        tmp_path,
        cohort_size=16,
        lane_count=2,
    )

    assert [job["cohort_offset"] for job in plan["jobs"]] == [32, 48]
    first, second = (set(job["expected_candidate_ids"]) for job in plan["jobs"])
    assert first.isdisjoint(second)
    assert len(first) == len(second) == 16
    assert plan["source_inventory"]["source_qd_replayable_candidate_count"] == 80
    assert plan["source_bank_exhausted"] is False

    exhausted = continuation.plan_remaining_0029_exact_jobs(
        tmp_path,
        completed_cohort_offsets=(32, 48, 64),
        cohort_size=16,
        lane_count=2,
    )
    assert exhausted["jobs"] == []
    assert exhausted["source_bank_exhausted"] is True


def test_worker_is_pickle_safe_reuses_exact_worker_and_stays_diagnostic(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    candidate_ids = [f"candidate-{index:03d}" for index in range(34)]
    inventory = _inventory(candidate_ids)
    exact = _exact_result(candidate_ids[32:], offset=32, source_unique_count=34)
    monkeypatch.setattr(
        continuation, "inspect_remaining_0029_source", lambda _root: inventory
    )
    observed: list[dict[str, object]] = []

    def fake_worker(payload: dict[str, object]) -> dict[str, object]:
        observed.append(payload)
        return exact

    monkeypatch.setattr(continuation, "exact_0029_account_size_worker", fake_worker)
    payload = {
        "root": str(tmp_path),
        "cohort_offset": 32,
        "cohort_maximum": 2,
        "expected_candidate_ids": candidate_ids[32:],
        "source_inventory_hash": inventory["source_inventory_hash"],
        "integer_tiers": [1, 2, 3, 4],
    }

    assert pickle.loads(pickle.dumps(continuation.remaining_0029_exact_worker)).__name__ == (
        "remaining_0029_exact_worker"
    )
    result = continuation.remaining_0029_exact_worker(payload)

    assert observed[0]["cohort_offset"] == 32
    assert result["candidate_ids"] == candidate_ids[32:]
    assert result["promotion_status"] is None
    assert result["xfa_paths_started"] == 0
    assert result["orders"] == 0


def test_composer_exposes_unique_candidate_denominators_and_exhaustion() -> None:
    candidate_ids = [f"candidate-{index:03d}" for index in range(36)]
    inventory = _inventory(candidate_ids)
    initial = _exact_result(candidate_ids[:32], offset=0, source_unique_count=36)
    first = _continuation_wrapper(
        _exact_result(candidate_ids[32:34], offset=32, source_unique_count=36),
        inventory,
    )
    second = _continuation_wrapper(
        _exact_result(candidate_ids[34:], offset=34, source_unique_count=36),
        inventory,
    )

    partial = continuation.compose_remaining_0029_exact_results(initial, [first])
    assert partial["source_bank_exhausted"] is False
    assert partial["aggregate_counters"]["exact_candidate_count"] == 34
    assert partial["remaining_candidate_count"] == 2

    complete = continuation.compose_remaining_0029_exact_results(
        initial, [first, second]
    )
    assert complete["source_bank_exhausted"] is True
    assert complete["aggregate_counters"]["exact_candidate_count"] == 36
    assert complete["remaining_candidate_count"] == 0
    assert complete["aggregate_counters"]["promotion_count"] == 0
    assert complete["promotion_status"] is None

    with pytest.raises(
        continuation.AutonomousExactContinuationError, match="more than one"
    ):
        continuation.compose_remaining_0029_exact_results(initial, [first, first])


def test_hazard_19327_remains_q_pending_on_uncleared_concentration() -> None:
    candidate = {
        "candidate_id": continuation.QUALIFICATION_CANDIDATE_ID,
        "candidate_fingerprint": "candidate-fingerprint",
        "realized_behavioral_fingerprint": "behavior-fingerprint",
        "qd_cell": "qd-cell",
        "source_event_evidence": {
            "sha256": "compressed-sha",
            "uncompressed_sha256": "content-sha",
            "record_count": 469,
        },
        "candidate_result_hash": "candidate-result-hash",
        "session_contract": {"event_violation_count": 0},
        "frontier": [
            _qualification_cell(5, 44, 3, {"B1": 0, "B2": 0, "B3": 0, "B4": 3}),
            _qualification_cell(10, 20, 2, {"B1": 0, "B2": 0, "B3": 0, "B4": 2}),
            _qualification_cell(20, 8, 4, {"B1": 1, "B2": 1, "B3": 1, "B4": 1}),
        ],
    }
    initial_ids = [f"candidate-{index:03d}" for index in range(31)] + [
        continuation.QUALIFICATION_CANDIDATE_ID
    ]
    exact = _exact_result(
        initial_ids,
        offset=0,
        results=[candidate],
        source_unique_count=180,
    )

    audit = continuation.audit_hazard_19327_tier_q(exact)

    assert audit["qualification_status"] == "Q_PENDING"
    assert audit["current_evidence_tier"] == "E"
    assert audit["promotion_status"] is None
    assert audit["retuning_performed"] is False
    assert audit["compact_evidence_bundle"]["complete"] is True
    assert audit["horizon_results"]["5"]["maximum_stressed_pass_block_share"] == 1.0
    assert audit["horizon_results"]["10"]["block_concentration_cleared"] is False
    assert audit["horizon_results"]["20"]["block_concentration_cleared"] is True
    assert audit["tier_q_gate_results"]["concentration_cleared"] is False
    assert "concentration_cleared" in audit["pending_fields"]


def _qualification_cell(
    horizon: int,
    episodes: int,
    passes: int,
    block_passes: dict[str, int],
) -> dict[str, object]:
    def scenario(name: str) -> dict[str, object]:
        return {
            "episode_count": episodes,
            "pass_count": passes,
            "pass_rate": passes / episodes,
            "mll_breach_count": 0,
            "mll_breach_rate": 0.0,
            "consistency_compliance_rate": 0.75,
            "net_total_usd": 1_000.0,
            "net_median_usd": 100.0,
            "target_progress_p25": -0.1,
            "target_progress_median": 0.2,
            "minimum_mll_buffer_usd": 1_000.0,
            "median_days_to_target": 4.0,
            "episode_path_hash": f"{name}-{horizon}-path-hash",
            "by_block": {
                block: {"pass_count": count}
                for block, count in block_passes.items()
            },
        }

    return {
        "account_label": continuation.QUALIFICATION_ACCOUNT_LABEL,
        "integer_quantity_tier": continuation.QUALIFICATION_INTEGER_TIER,
        "risk_governor_mode": continuation.QUALIFICATION_GOVERNOR,
        "horizon_trading_days": horizon,
        "full_coverage_start_count": episodes,
        "data_censored_start_count": 0,
        "legally_executable": True,
        "account_rule_compliant": True,
        "hard_compliance_failure_count": 0,
        "normal": scenario("normal"),
        "stressed": scenario("stressed"),
    }
