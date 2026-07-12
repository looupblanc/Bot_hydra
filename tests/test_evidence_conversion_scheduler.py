from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from hydra.mission.controller import (
    AutonomousMissionController,
    EVIDENCE_CONVERSION_V3_INITIAL_EXPERIMENT_ID,
    MissionControllerConfig,
)
from hydra.mission.evidence_conversion_scheduler import (
    EVIDENCE_CONVERSION_ALLOCATION,
    EvidenceConversionContractError,
    FrozenEvidenceSources,
    candidate_bank_manifest,
    is_turbo_structural_exhaustion,
    validate_evidence_conversion_result,
)
from hydra.mission.experiment_queue import (
    claim_next_experiment,
    complete_experiment,
    enqueue_experiment,
    experiment_record,
)
from hydra.mission.mission_state import connect_state, get_kv, mission_paths, set_kv


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _controller(tmp_path: Path) -> tuple[AutonomousMissionController, object]:
    paths = mission_paths(str(tmp_path / "state"))
    conn = connect_state(paths)
    return (
        AutonomousMissionController(
            MissionControllerConfig(
                mission_id="evidence-conversion-test",
                baseline_commit="test",
                objective_config="test",
                remaining_databento_budget_usd=70.0,
                persistent=False,
                state_dir=str(paths.state_dir),
                sleep_seconds=0.0,
            )
        ),
        conn,
    )


def _completed_promotion(conn: object, tmp_path: Path, *, index: int = 0) -> None:
    exact_path = tmp_path / f"exact_{index}.jsonl"
    exact_path.write_text(
        json.dumps(
            {
                "candidate_id": f"candidate_{index}",
                "event_net_pnl": [1.0, -0.5],
                "event_gross_pnl": [2.0, 0.5],
                "event_session_days": [19358, 19359],
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    semantic_hash = str(index + 1) * 64
    result_path = tmp_path / f"promotion_{index}.json"
    result_path.write_text(
        json.dumps(
            {
                "schema": "hydra_turbo_promotion_batch_v1",
                "result_hash": semantic_hash,
                "candidates": [{"candidate_id": f"candidate_{index}"}],
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    experiment_id = f"turbo_promotion_batch_{index:04d}"
    enqueue_experiment(
        conn,
        experiment_id,
        {
            "experiment_type": "turbo_promotion_batch",
            "exact_results_path": str(exact_path),
            "exact_results_sha256": _sha(exact_path),
        },
    )
    claimed = claim_next_experiment(conn)
    assert claimed is not None and claimed["experiment_id"] == experiment_id
    complete_experiment(
        conn,
        experiment_id,
        {
            "result_hash": semantic_hash,
            "artifacts": {"result_path": str(result_path)},
            "candidates": [],
        },
        claim_token=str(claimed["claim_token"]),
    )


def _conversion_result(
    tmp_path: Path, cohort_id: str, *, debt: int = 1
) -> dict[str, object]:
    payload: dict[str, object] = {
        "schema": "hydra_evidence_conversion_foundry_v3",
        "cohort_id": cohort_id,
        "candidates_before_clustering": 3,
        "behavioral_clusters": 2,
        "representative_count": 2,
        "role_distribution": {"COMBINE_PASSER_POOL": 1, "XFA_PAYOUT_POOL": 1},
        "evidence_debt_queue_count": debt,
        "evidence_debt_inventory_count": 2 + debt,
        "full_economic_replay_count": 2,
        "full_risk_replay_count": 2,
        "full_promotion_validation_count": 2,
        "promotion_decisions_count": 2,
        "complete_validation_candidate_ids": ["candidate_0", "candidate_shadow"],
        "status_counts": {
            "PROMOTION_FAILED": 0,
            "SHADOW_RESEARCH_ONLY": 1,
            "PRE_HOLDOUT_READY": 1,
        },
        "pre_holdout_candidate_ids": ["candidate_0"],
        "q4_access_count": 0,
        "paper_shadow_ready": 0,
        "scientific_conclusion": "EVIDENCE_CONVERSION_PROGRESS",
        "candidate_count": 2,
        "candidates": [
            {
                "candidate_id": "candidate_0",
                "status": "PRE_HOLDOUT_READY",
                "role": "COMBINE_PASSER",
            },
            {
                "candidate_id": "candidate_shadow",
                "status": "SHADOW_RESEARCH_ONLY",
                "role": "XFA_PAYOUT",
            },
        ],
    }
    payload["result_hash"] = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()
    artifact_names = {
        "evidence_debt_queue_path": "debt.json",
        "behavioral_clusters_path": "clusters.json",
        "representatives_path": "representatives.json",
        "complete_validation_path": "validation.json",
        "result_path": "result.json",
        "report_path": "report.md",
    }
    artifacts: dict[str, str | None] = {}
    artifact_hashes: dict[str, str | None] = {}
    for key, name in artifact_names.items():
        path = tmp_path / f"{cohort_id}_{name}"
        if key == "result_path":
            path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
        else:
            path.write_text(f"{key}\n", encoding="utf-8")
        artifacts[key] = str(path)
        artifact_hashes[key] = _sha(path)
    artifacts["pre_holdout_manifest_path"] = None
    artifact_hashes["pre_holdout_manifest_path"] = None
    return {
        **payload,
        "artifacts": artifacts,
        "artifact_sha256s": artifact_hashes,
        "report_path": artifacts["report_path"],
    }


def test_structural_exhaustion_is_distinct_from_transient_worker_failure() -> None:
    assert is_turbo_structural_exhaustion(
        "INSUFFICIENT_STRUCTURAL_DIVERSITY: family/lineage caps permit only 3920"
    )
    assert is_turbo_structural_exhaustion(
        "Only 490 unique structures are available for 5990 requested."
    )
    assert not is_turbo_structural_exhaustion("worker commit differs")


def test_source_freeze_and_candidate_manifest_are_deterministic() -> None:
    sources = FrozenEvidenceSources.build(
        promotion_sources=[
            {"experiment_id": "p1", "path": "/p/one", "sha256": "1" * 64},
            {"experiment_id": "p0", "path": "/p/zero", "sha256": "0" * 64},
        ],
        exact_sources=[
            {"experiment_id": "p1", "path": "/e/one", "sha256": "3" * 64},
            {"experiment_id": "p0", "path": "/e/zero", "sha256": "2" * 64},
        ],
    )
    left = candidate_bank_manifest(
        {"b": {"status": "RAW_ECONOMIC_SIGNAL"}, "a": {"status": "PROMISING_RESEARCH_CANDIDATE"}},
        cohort_id="evidence_conversion_v3_cohort_0000",
        code_commit="c" * 40,
        sources=sources,
    )
    right = candidate_bank_manifest(
        {"a": {"status": "PROMISING_RESEARCH_CANDIDATE"}, "b": {"status": "RAW_ECONOMIC_SIGNAL"}},
        cohort_id="evidence_conversion_v3_cohort_0000",
        code_commit="c" * 40,
        sources=sources,
    )
    assert left == right
    assert [row["candidate_id"] for row in left["candidates"]] == ["a"]
    assert left["source_candidate_bank_count"] == 2
    assert left["excluded_status_counts"] == {"RAW_ECONOMIC_SIGNAL": 1}
    assert left["q4_access_allowed"] is False


def test_result_contract_is_role_specific_pre_holdout_only(tmp_path: Path) -> None:
    result = _conversion_result(tmp_path, "evidence_conversion_v3_cohort_0000")
    validate_evidence_conversion_result(result)
    result["paper_shadow_ready"] = 1
    with pytest.raises(EvidenceConversionContractError, match="PAPER_SHADOW_READY"):
        validate_evidence_conversion_result(result)


def test_result_contract_rejects_semantic_or_artifact_forgery(tmp_path: Path) -> None:
    semantic = _conversion_result(tmp_path, "evidence_conversion_v3_cohort_0000")
    semantic["scientific_conclusion"] = "FORGED"
    with pytest.raises(EvidenceConversionContractError, match="semantic result hash"):
        validate_evidence_conversion_result(semantic)

    artifact = _conversion_result(tmp_path, "evidence_conversion_v3_cohort_0001")
    Path(str(artifact["artifacts"]["complete_validation_path"])).write_text(
        "tampered\n", encoding="utf-8"
    )
    with pytest.raises(EvidenceConversionContractError, match="artifact hash mismatch"):
        validate_evidence_conversion_result(artifact)


def test_controller_freezes_all_sources_bank_and_allocation(tmp_path: Path) -> None:
    controller, conn = _controller(tmp_path)
    try:
        _completed_promotion(conn, tmp_path, index=0)
        _completed_promotion(conn, tmp_path, index=1)
        set_kv(
            conn,
            "foundry_candidate_bank",
            {
                "candidate_0": {"status": "PROMISING_RESEARCH_CANDIDATE"},
                "candidate_1": {"status": "SHADOW_RESEARCH_CANDIDATE"},
                "candidate_unresolved": {"status": "RAW_ECONOMIC_SIGNAL"},
            },
        )

        assert controller._reconcile_evidence_conversion_v3(conn, cohort_index=0)
        record = experiment_record(conn, EVIDENCE_CONVERSION_V3_INITIAL_EXPERIMENT_ID)
        assert record is not None and record["status"] == "QUEUED"
        specification = record["specification"]
        assert specification["priority"] == 260.0
        assert specification["allocation"] == EVIDENCE_CONVERSION_ALLOCATION
        assert specification["max_representatives"] == 40
        assert specification["max_complete_validation"] == 20
        assert len(specification["source_result_paths"]) == 2
        assert len(specification["source_exact_result_paths"]) == 2
        assert specification["q4_access_allowed"] is False
        assert specification["paid_data_allowed"] is False
        assert specification["network_allowed"] is False
        assert specification["live_or_broker_allowed"] is False
        manifest_path = Path(specification["candidate_bank_manifest_path"])
        assert _sha(manifest_path) == specification["candidate_bank_manifest_sha256"]
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        assert manifest["candidate_count"] == 2
        assert manifest["source_candidate_bank_count"] == 3
        assert manifest["excluded_status_counts"] == {"RAW_ECONOMIC_SIGNAL": 1}
        assert len(manifest["source_experiment_ids"]) == 2
        assert specification["contract_map_sha256"] == _sha(
            Path(specification["contract_map_path"])
        )
    finally:
        conn.close()


def test_completed_cohort_routes_debt_to_next_frozen_cohort(tmp_path: Path) -> None:
    controller, conn = _controller(tmp_path)
    try:
        _completed_promotion(conn, tmp_path)
        set_kv(
            conn,
            "foundry_candidate_bank",
            {"candidate_0": {"status": "PROMISING_RESEARCH_CANDIDATE"}},
        )
        assert controller._reconcile_evidence_conversion_v3(conn, cohort_index=0)
        controller._route_evidence_conversion_v3_result(
            conn,
            _conversion_result(tmp_path, EVIDENCE_CONVERSION_V3_INITIAL_EXPERIMENT_ID),
            EVIDENCE_CONVERSION_V3_INITIAL_EXPERIMENT_ID,
        )
        followup = experiment_record(conn, "evidence_conversion_v3_cohort_0001")
        assert followup is not None and followup["status"] == "QUEUED"
        assert followup["specification"]["previously_decided_candidate_ids"] == [
            "candidate_0",
            "candidate_shadow",
        ]
        assert get_kv(conn, "current_phase") == "PLANNING_NEXT_ACTION"
        assert get_kv(conn, "pre_holdout_candidate_ids") == ["candidate_0"]
        assert get_kv(conn, "paper_shadow_ready", 0) == 0
        assert get_kv(conn, "promising_candidates") == 2
        assert get_kv(conn, "shadow_candidates") == 2
        # Crash-window reconciliation is idempotent: the same completed cohort
        # may repair its accumulator, but cannot allocate cohort 0002.
        controller._route_evidence_conversion_v3_result(
            conn,
            _conversion_result(tmp_path, EVIDENCE_CONVERSION_V3_INITIAL_EXPERIMENT_ID),
            EVIDENCE_CONVERSION_V3_INITIAL_EXPERIMENT_ID,
        )
        assert experiment_record(conn, "evidence_conversion_v3_cohort_0002") is None
    finally:
        conn.close()


def test_turbo_exhaustion_skips_retries_and_pivots_to_conversion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    controller, conn = _controller(tmp_path)
    try:
        _completed_promotion(conn, tmp_path)
        set_kv(
            conn,
            "foundry_candidate_bank",
            {"candidate_0": {"status": "PROMISING_RESEARCH_CANDIDATE"}},
        )
        turbo_id = "turbo_foundry_v2_epoch_0020"
        enqueue_experiment(
            conn,
            turbo_id,
            {
                "experiment_type": "turbo_foundry_v2_epoch",
                "priority": 200.0,
                "max_attempts": 3,
                "q4_access_allowed": False,
                "paid_data_allowed": False,
            },
        )

        def exhausted(_conn: object, _experiment: dict[str, object]) -> dict[str, object]:
            raise RuntimeError(
                "INSUFFICIENT_STRUCTURAL_DIVERSITY: family/lineage caps permit only 47"
            )

        monkeypatch.setattr(controller, "_check_experiment_allowed", lambda *_: None)
        monkeypatch.setattr(controller, "_run_experiment_with_heartbeat", exhausted)
        controller._execute_queued_experiment(conn)

        failed = experiment_record(conn, turbo_id)
        assert failed is not None and failed["status"] == "FAILED"
        assert failed["attempt_count"] == 1
        conversion = experiment_record(
            conn, EVIDENCE_CONVERSION_V3_INITIAL_EXPERIMENT_ID
        )
        assert conversion is not None and conversion["status"] == "QUEUED"
        assert get_kv(conn, "current_phase") == "PLANNING_NEXT_ACTION"
        assert get_kv(conn, "current_blocker") is None
        assert get_kv(conn, "turbo_discovery_grammar_status") == (
            "DISCOVERY_GRAMMAR_CAPACITY_EXHAUSTED_NONFATAL"
        )
        assert experiment_record(conn, "turbo_foundry_v2_epoch_0021") is None
    finally:
        conn.close()
