from __future__ import annotations

import hashlib
from pathlib import Path

from hydra.mission.controller import (
    AutonomousMissionController,
    FORWARD_SHADOW_FEED_AUDIT_EXPERIMENT_ID,
    MissionControllerConfig,
    PORTFOLIO_ROLE_RESEARCH_EXPERIMENT_ID,
    POST_MUTATION_CHILD_SHADOW_ACTIVATION_EXPERIMENT_ID,
    POST_MUTATION_META_ALLOCATION_EXPERIMENT_ID,
    POST_MUTATION_SHADOW_ADMISSION_EXPERIMENT_ID,
    POST_MUTATION_SUCCESSIVE_HALVING_EXPERIMENT_ID,
    PROMISING_LINEAGE_MUTATION_EXPERIMENT_ID,
    SHADOW_SHARED_ACCOUNT_BASKETS_EXPERIMENT_ID,
)
from hydra.mission.experiment_queue import (
    claim_next_experiment,
    complete_experiment,
    enqueue_experiment,
    experiment_record,
)
from hydra.mission.mission_state import connect_state, get_kv, mission_paths, set_kv


def _controller(tmp_path: Path) -> tuple[AutonomousMissionController, object, object]:
    paths = mission_paths(str(tmp_path / "state"))
    conn = connect_state(paths)
    controller = AutonomousMissionController(
        MissionControllerConfig(
            mission_id="portfolio-mutation-test",
            baseline_commit="test",
            objective_config="test",
            remaining_databento_budget_usd=72.0,
            persistent=False,
            state_dir=str(paths.state_dir),
            sleep_seconds=0.0,
        )
    )
    return controller, conn, paths


def _seed_completed_basket(conn: object) -> None:
    sources = [
        {
            "candidate_id": f"active_shadow_{index}",
            "result_path": f"result_{index}.json",
            "result_sha256": str(index) * 64,
            "result_hash": str(index + 1) * 64,
            "ledger_path": f"ledger_{index}.jsonl",
            "ledger_sha256": str(index + 2) * 64,
        }
        for index in range(4)
    ]
    enqueue_experiment(
        conn,
        SHADOW_SHARED_ACCOUNT_BASKETS_EXPERIMENT_ID,
        {"experiment_type": "shadow_shared_account_baskets", "sources": sources},
    )
    claimed = claim_next_experiment(conn)
    assert claimed is not None
    complete_experiment(
        conn,
        SHADOW_SHARED_ACCOUNT_BASKETS_EXPERIMENT_ID,
        {"scientific_conclusion": "THREE_EXECUTABLE_SHADOW_BASKETS_FOUND"},
        claim_token=str(claimed["claim_token"]),
    )


def test_blocker_recovery_queues_three_distinct_parallel_pipelines(tmp_path: Path) -> None:
    controller, conn, _paths = _controller(tmp_path)
    try:
        _seed_completed_basket(conn)
        set_kv(conn, "current_phase", "ENGINEERING_BLOCKED")
        set_kv(
            conn,
            "current_blocker",
            "PORTFOLIO_ROLE_AND_PROMISING_LINEAGE_MUTATION_REQUIRED",
        )

        assert controller._reconcile_portfolio_mutation_campaign(conn)
        records = [
            experiment_record(conn, experiment_id)
            for experiment_id in (
                PROMISING_LINEAGE_MUTATION_EXPERIMENT_ID,
                PORTFOLIO_ROLE_RESEARCH_EXPERIMENT_ID,
                FORWARD_SHADOW_FEED_AUDIT_EXPERIMENT_ID,
            )
        ]
        assert all(record is not None and record["status"] == "QUEUED" for record in records)
        assert len({record["specification"]["pipeline"] for record in records}) == 3
        assert all(record["specification"]["parallel_safe"] is True for record in records)
        assert all(record["specification"]["q4_access_allowed"] is False for record in records)
        assert all(record["specification"]["paid_data_allowed"] is False for record in records)
        assert all(record["specification"]["network_allowed"] is False for record in records)
        assert all(record["specification"]["live_or_broker_allowed"] is False for record in records)
        assert get_kv(conn, "current_phase") == "PLANNING_NEXT_ACTION"
        assert get_kv(conn, "current_blocker") is None
    finally:
        conn.close()


def test_stalled_completed_campaign_queues_successive_halving(
    tmp_path: Path,
) -> None:
    controller, conn, _paths = _controller(tmp_path)
    try:
        result_path = tmp_path / "mutation_result.json"
        ledger_path = tmp_path / "mutation_ledger.jsonl"
        result_path.write_text("{}\n", encoding="utf-8")
        ledger_path.write_text("{}\n", encoding="utf-8")
        enqueue_experiment(
            conn,
            PROMISING_LINEAGE_MUTATION_EXPERIMENT_ID,
            {"experiment_type": "promising_lineage_mutation"},
        )
        claimed = claim_next_experiment(conn)
        assert claimed is not None
        complete_experiment(
            conn,
            PROMISING_LINEAGE_MUTATION_EXPERIMENT_ID,
            {
                "q4_access_count": 0,
                "order_capability": False,
                "result_hash": "frozen-result",
                "artifacts": {
                    "result_json_path": str(result_path),
                    "result_json_sha256": hashlib.sha256(
                        result_path.read_bytes()
                    ).hexdigest(),
                    "trade_ledger_path": str(ledger_path),
                    "trade_ledger_sha256": hashlib.sha256(
                        ledger_path.read_bytes()
                    ).hexdigest(),
                },
            },
            claim_token=str(claimed["claim_token"]),
        )
        set_kv(conn, "portfolio_mutation_campaign_completed", True)
        set_kv(conn, "current_phase", "SCHEDULER_STALLED")

        assert controller._reconcile_post_mutation_successive_halving(conn)
        record = experiment_record(
            conn, POST_MUTATION_SUCCESSIVE_HALVING_EXPERIMENT_ID
        )
        assert record is not None and record["status"] == "QUEUED"
        assert record["specification"]["pipeline"] == "PROMOTION"
        assert record["specification"]["parallel_safe"] is True
        assert record["specification"]["q4_access_allowed"] is False
        assert record["specification"]["network_allowed"] is False
        assert record["specification"]["live_or_broker_allowed"] is False
        assert get_kv(conn, "current_phase") == "PLANNING_NEXT_ACTION"
    finally:
        conn.close()


def test_completed_campaign_queues_next_meta_action_without_feed_blocking(
    tmp_path: Path,
) -> None:
    controller, conn, _paths = _controller(tmp_path)
    try:
        _seed_completed_basket(conn)
        assert controller._reconcile_portfolio_mutation_campaign(conn)
        results = {
            PROMISING_LINEAGE_MUTATION_EXPERIMENT_ID: {
                "candidate_count": 18,
                "candidates": [],
                "parent_count": 16,
                "mutation_hypothesis_count": 18,
                "primary_child_count": 16,
                "ym_versioned_hypotheses": 3,
                "accepted_research_prototypes": 10,
                "scientific_conclusion": "TARGETED_MUTATIONS_CREATED_FORWARD_EVIDENCE_REQUIRED",
            },
            PORTFOLIO_ROLE_RESEARCH_EXPERIMENT_ID: {
                "candidate_count": 5,
                "portfolio_role_candidates_generated": 5,
                "defensive_role_candidates_generated": 3,
                "research_candidate_count": 1,
                "scientific_conclusion": "PORTFOLIO_ROLE_RESEARCH_CANDIDATES_FOUND",
            },
            FORWARD_SHADOW_FEED_AUDIT_EXPERIMENT_ID: {
                "scientific_conclusion": "FORWARD_DATA_SOURCE_REQUIRED",
                "status": "SOURCE_REQUIRED",
                "required_roots": ["MYM"],
                "candidate_heartbeats_published": 0,
                "network_requests": 0,
                "outbound_orders": 0,
            },
        }
        for experiment_id in (
            PROMISING_LINEAGE_MUTATION_EXPERIMENT_ID,
            PORTFOLIO_ROLE_RESEARCH_EXPERIMENT_ID,
            FORWARD_SHADOW_FEED_AUDIT_EXPERIMENT_ID,
        ):
            claimed = claim_next_experiment(conn)
            assert claimed is not None
            # Priority order is deterministic and matches the tuple above.
            assert claimed["experiment_id"] == experiment_id
            complete_experiment(
                conn,
                experiment_id,
                results[experiment_id],
                claim_token=str(claimed["claim_token"]),
            )
        controller._route_promising_lineage_mutation_result(
            conn, results[PROMISING_LINEAGE_MUTATION_EXPERIMENT_ID]
        )
        controller._route_portfolio_role_research_result(
            conn, results[PORTFOLIO_ROLE_RESEARCH_EXPERIMENT_ID]
        )
        controller._route_forward_shadow_feed_audit_result(
            conn, results[FORWARD_SHADOW_FEED_AUDIT_EXPERIMENT_ID]
        )

        assert get_kv(conn, "portfolio_mutation_campaign_completed") is True
        assert get_kv(conn, "forward_data_blocker") == "FORWARD_DATA_SOURCE_REQUIRED"
        assert get_kv(conn, "current_blocker") is None
        next_record = experiment_record(conn, POST_MUTATION_META_ALLOCATION_EXPERIMENT_ID)
        assert next_record is not None and next_record["status"] == "QUEUED"
        assert next_record["specification"]["q4_access_allowed"] is False
        assert next_record["specification"]["live_or_broker_allowed"] is False
    finally:
        conn.close()


def test_completed_halving_queues_frozen_shadow_admission(tmp_path: Path) -> None:
    controller, conn, _paths = _controller(tmp_path)
    try:
        result_path = tmp_path / "halving_result.json"
        manifest_path = tmp_path / "elite_manifest.json"
        evidence_path = tmp_path / "candidate_evidence.jsonl"
        result_path.write_text("{}\n", encoding="utf-8")
        manifest_path.write_text('{"manifest_hash":"frozen-manifest"}\n', encoding="utf-8")
        evidence_path.write_text("{}\n", encoding="utf-8")
        enqueue_experiment(
            conn,
            POST_MUTATION_SUCCESSIVE_HALVING_EXPERIMENT_ID,
            {"experiment_type": "post_mutation_successive_halving"},
        )
        claimed = claim_next_experiment(conn)
        assert claimed is not None
        complete_experiment(
            conn,
            POST_MUTATION_SUCCESSIVE_HALVING_EXPERIMENT_ID,
            {
                "result_hash": "frozen-halving",
                "artifacts": {
                    "result": {
                        "path": str(result_path),
                        "sha256": hashlib.sha256(result_path.read_bytes()).hexdigest(),
                    },
                    "elite_manifest": {
                        "path": str(manifest_path),
                        "sha256": hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
                    },
                    "candidate_evidence": {
                        "path": str(evidence_path),
                        "sha256": hashlib.sha256(evidence_path.read_bytes()).hexdigest(),
                    },
                },
            },
            claim_token=str(claimed["claim_token"]),
        )

        assert controller._reconcile_post_mutation_shadow_admission(conn)
        record = experiment_record(conn, POST_MUTATION_SHADOW_ADMISSION_EXPERIMENT_ID)
        assert record is not None and record["status"] == "QUEUED"
        specification = record["specification"]
        assert specification["pipeline"] == "PROMOTION"
        assert specification["q4_access_allowed"] is False
        assert specification["network_allowed"] is False
        assert specification["live_or_broker_allowed"] is False
        assert specification["parent_source_result_path"].endswith(
            "equity_open_gap_continuation_result.json"
        )
        assert len(specification["parent_source_result_sha256"]) == 64
        assert get_kv(conn, "current_phase") == "PLANNING_NEXT_ACTION"
    finally:
        conn.close()


def test_admitted_child_queues_generic_fail_closed_activation(tmp_path: Path) -> None:
    controller, conn, _paths = _controller(tmp_path)
    try:
        result_path = tmp_path / "admission_result.json"
        configuration_path = tmp_path / "child_shadow.json"
        result_path.write_text("{}\n", encoding="utf-8")
        configuration_path.write_text("{}\n", encoding="utf-8")
        configuration_sha = hashlib.sha256(configuration_path.read_bytes()).hexdigest()
        enqueue_experiment(
            conn,
            POST_MUTATION_SHADOW_ADMISSION_EXPERIMENT_ID,
            {"experiment_type": "post_mutation_shadow_admission"},
        )
        claimed = claim_next_experiment(conn)
        assert claimed is not None
        complete_experiment(
            conn,
            POST_MUTATION_SHADOW_ADMISSION_EXPERIMENT_ID,
            {
                "scientific_conclusion": (
                    "ONE_POST_MUTATION_SHADOW_RESEARCH_CANDIDATE_ADMITTED"
                ),
                "result_hash": "frozen-admission",
                "candidates": [{"candidate_id": "child_v1"}],
                "shadow_configurations": [
                    {
                        "path": str(configuration_path),
                        "sha256": configuration_sha,
                        "configuration_hash": "semantic-child-hash",
                    }
                ],
                "artifacts": {
                    "result": {
                        "path": str(result_path),
                        "sha256": hashlib.sha256(result_path.read_bytes()).hexdigest(),
                    }
                },
            },
            claim_token=str(claimed["claim_token"]),
        )

        assert controller._reconcile_post_mutation_child_shadow_activation(conn)
        record = experiment_record(
            conn, POST_MUTATION_CHILD_SHADOW_ACTIVATION_EXPERIMENT_ID
        )
        assert record is not None and record["status"] == "QUEUED"
        specification = record["specification"]
        assert specification["experiment_type"] == "immutable_shadow_activation"
        assert specification["pipeline"] == "SHADOW"
        assert specification["q4_access_allowed"] is False
        assert specification["network_allowed"] is False
        assert specification["live_or_broker_allowed"] is False
        assert any(
            path.endswith("hydra/shadow/prior_trade_guard.py")
            for path in specification["code_surface_paths"]
        )
    finally:
        conn.close()
