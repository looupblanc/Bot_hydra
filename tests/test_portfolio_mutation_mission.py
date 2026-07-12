from __future__ import annotations

import hashlib
from pathlib import Path

from hydra.mission.controller import (
    AutonomousMissionController,
    FORWARD_SHADOW_FEED_AUDIT_EXPERIMENT_ID,
    EQUITY_PRECLOSE_INVENTORY_DISPERSION_EXPERIMENT_ID,
    MINI_MICRO_PARTICIPATION_DIVERGENCE_EXPERIMENT_ID,
    MissionControllerConfig,
    PORTFOLIO_ROLE_RESEARCH_EXPERIMENT_ID,
    POST_MUTATION_CHILD_SHADOW_ACTIVATION_EXPERIMENT_ID,
    POST_MUTATION_META_ALLOCATION_EXPERIMENT_ID,
    POST_MUTATION_SHADOW_ADMISSION_EXPERIMENT_ID,
    POST_MUTATION_SUCCESSIVE_HALVING_EXPERIMENT_ID,
    PROMISING_LINEAGE_MUTATION_EXPERIMENT_ID,
    ROLE_CONDITIONED_STRUCTURAL_EPOCH_EXPERIMENT_ID,
    SHADOW_SHARED_ACCOUNT_BASKETS_EXPERIMENT_ID,
    TURBO_FOUNDRY_V2_INITIAL_EXPERIMENT_ID,
)
from hydra.mission.experiment_queue import (
    claim_next_experiment,
    complete_experiment,
    enqueue_experiment,
    experiment_record,
    fail_experiment,
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


def test_child_activation_queues_role_conditioned_epoch_without_idle_gap(
    tmp_path: Path,
) -> None:
    controller, conn, _paths = _controller(tmp_path)
    try:
        def frozen(name: str, content: str = "{}\n") -> tuple[Path, str]:
            path = tmp_path / name
            path.write_text(content, encoding="utf-8")
            return path, hashlib.sha256(path.read_bytes()).hexdigest()

        mutation_result, mutation_result_sha = frozen("mutation.json")
        mutation_ledger, mutation_ledger_sha = frozen("mutation.jsonl")
        halving_result, halving_result_sha = frozen("halving.json")
        halving_evidence, halving_evidence_sha = frozen("halving.jsonl")
        halving_manifest, halving_manifest_sha = frozen(
            "manifest.json", '{"manifest_hash":"manifest-semantic-hash"}\n'
        )
        portfolio_result, portfolio_result_sha = frozen("portfolio.json")
        meta_result, _meta_result_sha = frozen("meta.json")
        completed = [
            (
                PROMISING_LINEAGE_MUTATION_EXPERIMENT_ID,
                "promising_lineage_mutation",
                {
                    "result_hash": "mutation-hash",
                    "q4_access_count": 0,
                    "network_requests": 0,
                    "order_capability": False,
                    "artifacts": {
                        "result_json_path": str(mutation_result),
                        "result_json_sha256": mutation_result_sha,
                        "trade_ledger_path": str(mutation_ledger),
                        "trade_ledger_sha256": mutation_ledger_sha,
                    },
                },
            ),
            (
                POST_MUTATION_SUCCESSIVE_HALVING_EXPERIMENT_ID,
                "post_mutation_successive_halving",
                {
                    "result_hash": "halving-hash",
                    "q4_access_count": 0,
                    "network_requests": 0,
                    "order_capability": False,
                    "artifacts": {
                        "result": {
                            "path": str(halving_result),
                            "sha256": halving_result_sha,
                        },
                        "candidate_evidence": {
                            "path": str(halving_evidence),
                            "sha256": halving_evidence_sha,
                        },
                        "elite_manifest": {
                            "path": str(halving_manifest),
                            "sha256": halving_manifest_sha,
                        },
                    },
                },
            ),
            (
                PORTFOLIO_ROLE_RESEARCH_EXPERIMENT_ID,
                "portfolio_role_research",
                {
                    "result_hash": "portfolio-hash",
                    "q4_access_count": 0,
                    "network_requests": 0,
                    "outbound_orders": 0,
                    "artifacts": {
                        "result_json_path": str(portfolio_result),
                        "result_json_sha256": portfolio_result_sha,
                    },
                },
            ),
            (
                POST_MUTATION_META_ALLOCATION_EXPERIMENT_ID,
                "meta_failure_allocation",
                {
                    "result_hash": "meta-hash",
                    "governance": {"q4_access_count": 0},
                    "artifacts": {"result_json_path": str(meta_result)},
                },
            ),
            (
                POST_MUTATION_CHILD_SHADOW_ACTIVATION_EXPERIMENT_ID,
                "immutable_shadow_activation",
                {
                    "scientific_conclusion": "IMMUTABLE_ZERO_ORDER_SHADOW_ACTIVATED",
                    "paper_shadow_ready": 0,
                    "governance": {"outbound_order_capability": False},
                },
            ),
        ]
        for experiment_id, experiment_type, result in completed:
            enqueue_experiment(
                conn, experiment_id, {"experiment_type": experiment_type}
            )
            claimed = claim_next_experiment(conn)
            assert claimed is not None and claimed["experiment_id"] == experiment_id
            complete_experiment(
                conn,
                experiment_id,
                result,
                claim_token=str(claimed["claim_token"]),
            )

        assert controller._reconcile_role_conditioned_structural_epoch(conn)
        record = experiment_record(
            conn, ROLE_CONDITIONED_STRUCTURAL_EPOCH_EXPERIMENT_ID
        )
        assert record is not None and record["status"] == "QUEUED"
        specification = record["specification"]
        assert specification["pipeline"] == "DISCOVERY_AND_PORTFOLIO"
        assert specification["parallel_safe"] is True
        assert specification["q4_access_allowed"] is False
        assert specification["network_allowed"] is False
        assert specification["live_or_broker_allowed"] is False
        assert specification["halving_manifest_hash"] == "manifest-semantic-hash"
        assert get_kv(conn, "current_phase") == "PLANNING_NEXT_ACTION"
        assert get_kv(conn, "current_blocker") is None
    finally:
        conn.close()


def test_negative_role_epoch_queues_distinct_preclose_pivot(tmp_path: Path) -> None:
    controller, conn, _paths = _controller(tmp_path)
    try:
        enqueue_experiment(
            conn,
            ROLE_CONDITIONED_STRUCTURAL_EPOCH_EXPERIMENT_ID,
            {"experiment_type": "role_conditioned_structural_epoch"},
        )
        claimed = claim_next_experiment(conn)
        assert claimed is not None
        complete_experiment(
            conn,
            ROLE_CONDITIONED_STRUCTURAL_EPOCH_EXPERIMENT_ID,
            {
                "scientific_conclusion": (
                    "ROLE_CONDITIONED_ACCOUNT_POLICY_EVIDENCE_INSUFFICIENT"
                ),
                "promising_candidates": 0,
                "q4_access_count": 0,
                "order_capability": False,
                "result_hash": "negative-role-epoch",
            },
            claim_token=str(claimed["claim_token"]),
        )

        assert controller._reconcile_equity_preclose_inventory_dispersion(conn)
        record = experiment_record(
            conn, EQUITY_PRECLOSE_INVENTORY_DISPERSION_EXPERIMENT_ID
        )
        assert record is not None and record["status"] == "QUEUED"
        specification = record["specification"]
        assert specification["pipeline"] == "DISCOVERY"
        assert specification["writes_data_access_ledger"] is True
        assert len(specification["core_data_paths"]) == 5
        assert len(specification["core_data_sha256s"]) == 5
        assert specification["q4_access_allowed"] is False
        assert specification["paid_data_allowed"] is False
        assert specification["network_allowed"] is False
        assert specification["live_or_broker_allowed"] is False
        assert get_kv(conn, "current_phase") == "PLANNING_NEXT_ACTION"
        assert get_kv(conn, "current_blocker") is None
    finally:
        conn.close()


def test_negative_preclose_queues_distinct_mini_micro_participation_pivot(
    tmp_path: Path,
) -> None:
    controller, conn, _paths = _controller(tmp_path)
    try:
        enqueue_experiment(
            conn,
            EQUITY_PRECLOSE_INVENTORY_DISPERSION_EXPERIMENT_ID,
            {"experiment_type": "equity_preclose_inventory_dispersion"},
        )
        claimed = claim_next_experiment(conn)
        assert claimed is not None
        complete_experiment(
            conn,
            EQUITY_PRECLOSE_INVENTORY_DISPERSION_EXPERIMENT_ID,
            {
                "scientific_conclusion": (
                    "PRECLOSE_PRIMARY_INSUFFICIENT_PIVOT_MARKET_ECOLOGY"
                ),
                "promising_candidates": 0,
                "q4_access_count": 0,
                "order_capability": False,
                "result_hash": "a" * 64,
            },
            claim_token=str(claimed["claim_token"]),
        )

        assert controller._reconcile_mini_micro_participation_divergence(conn)
        record = experiment_record(
            conn, MINI_MICRO_PARTICIPATION_DIVERGENCE_EXPERIMENT_ID
        )
        assert record is not None and record["status"] == "QUEUED"
        specification = record["specification"]
        assert specification["experiment_type"] == "mini_micro_participation_divergence"
        assert specification["pipeline"] == "DISCOVERY"
        assert len(specification["core_data_paths"]) == 5
        assert specification["source_preclose_result_hash"] == "a" * 64
        assert specification["q4_access_allowed"] is False
        assert specification["paid_data_allowed"] is False
        assert specification["network_allowed"] is False
        assert specification["live_or_broker_allowed"] is False
        assert get_kv(conn, "current_phase") == "PLANNING_NEXT_ACTION"
        assert get_kv(conn, "current_blocker") is None
    finally:
        conn.close()


def test_negative_mini_micro_result_freezes_family_idempotently(tmp_path: Path) -> None:
    controller, conn, _paths = _controller(tmp_path)
    result = {
        "scientific_conclusion": (
            "MINI_MICRO_PARTICIPATION_PRIMARY_INSUFFICIENT_PIVOT_MECHANISM"
        ),
        "candidate_count": 96,
        "structural_prototypes": 96,
        "stage1_survivors": 5,
        "frozen_elite_count": 3,
        "promising_candidates": 0,
        "status_counts": {"INSUFFICIENT_EVIDENCE": 3},
        "candidates": [],
        "q4_access_count": 0,
        "paper_shadow_ready": 0,
    }
    try:
        controller._route_mini_micro_participation_divergence_result(conn, result)
        controller._route_mini_micro_participation_divergence_result(conn, result)
        assert get_kv(conn, "current_phase") == "ENGINEERING_BLOCKED"
        assert get_kv(conn, "current_blocker") == "DISTINCT_MECHANISM_OR_FORWARD_DATA_REQUIRED"
        assert get_kv(conn, "lineages_frozen") == 1
        assert get_kv(conn, "foundry_frozen_lineage_ids") == [
            "MINI_MICRO_PARTICIPATION_DIVERGENCE_PRIMARY_V1"
        ]
    finally:
        conn.close()


def test_turbo_v2_resolves_distinct_mechanism_block_and_queues_single_epoch(
    tmp_path: Path,
) -> None:
    controller, conn, _paths = _controller(tmp_path)
    try:
        set_kv(conn, "current_phase", "ENGINEERING_BLOCKED")
        set_kv(conn, "current_blocker", "DISTINCT_MECHANISM_OR_FORWARD_DATA_REQUIRED")

        assert controller._reconcile_turbo_foundry_v2(conn, batch_index=0)
        assert controller._reconcile_turbo_foundry_v2(conn, batch_index=0)
        record = experiment_record(conn, TURBO_FOUNDRY_V2_INITIAL_EXPERIMENT_ID)
        assert record is not None and record["status"] == "QUEUED"
        specification = record["specification"]
        assert specification["experiment_type"] == "turbo_foundry_v2_epoch"
        assert specification["pipeline"] == "DISCOVERY"
        assert specification["parallel_safe"] is False
        assert specification["worker_count"] == 3
        assert specification["q4_access_allowed"] is False
        assert specification["paid_data_allowed"] is False
        assert specification["network_allowed"] is False
        assert specification["live_or_broker_allowed"] is False
        assert get_kv(conn, "current_phase") == "PLANNING_NEXT_ACTION"
        assert get_kv(conn, "current_blocker") is None
    finally:
        conn.close()


def test_completed_turbo_epoch_routes_candidates_and_queues_next_epoch(
    tmp_path: Path,
) -> None:
    controller, conn, _paths = _controller(tmp_path)
    try:
        source_result_path = tmp_path / "turbo_result.json"
        exact_results_path = tmp_path / "turbo_exact_results.jsonl"
        source_result_path.write_text("{}\n", encoding="utf-8")
        exact_results_path.write_text("{}\n", encoding="utf-8")
        assert controller._reconcile_turbo_foundry_v2(conn, batch_index=0)
        claimed = claim_next_experiment(conn)
        assert claimed is not None
        complete_experiment(
            conn,
            TURBO_FOUNDRY_V2_INITIAL_EXPERIMENT_ID,
            {
                "schema": "hydra_turbo_foundry_v2_epoch_v1",
                "batch_index": 0,
                "scientific_conclusion": "TURBO_V2_PROMOTION_CANDIDATES_FOUND",
                "candidate_count": 5_990,
                "structural_prototypes": 5_990,
                "stage0_valid": 5_990,
                "stage1_survivors": 100,
                "exact_replays": 80,
                "promotion_candidates_queued": 4,
                "promising_candidates": 4,
                "paper_shadow_ready": 0,
                "candidates": [
                    {
                        "candidate_id": "turbo_promising_1",
                        "status": "PROMISING_RESEARCH_CANDIDATE",
                        "mechanism_family": "test_family",
                        "primary_market": "ES",
                        "execution_market": "MES",
                        "role": "COMBINE_PASSER",
                        "objective_pool": "COMBINE_PASSER_POOL",
                        "net_pnl": 123.0,
                    }
                ],
                "performance": {"stage1_candidates_per_second": 500.0},
                "feature_store": {"cache_hits": 6},
                "meta_screen": {"status": "TRAINED_REGISTRY_OOS_ALLOCATION_ONLY"},
                "governance": {"q4_access_count_delta": 0},
                "report_path": "turbo_report.md",
                "promotion_candidate_ids": ["turbo_promising_1"],
                "artifacts": {
                    "result_path": str(source_result_path),
                    "exact_results_path": str(exact_results_path),
                },
                "result_hash": "a" * 64,
            },
            claim_token=str(claimed["claim_token"]),
        )

        controller._reconcile_completed_turbo_epochs(conn)
        next_record = experiment_record(conn, "turbo_foundry_v2_epoch_0001")
        promotion_record = experiment_record(conn, "turbo_promotion_batch_0000")
        assert next_record is not None and next_record["status"] == "QUEUED"
        assert promotion_record is not None and promotion_record["status"] == "QUEUED"
        assert get_kv(conn, "current_phase") == "PLANNING_NEXT_ACTION"
        assert get_kv(conn, "current_blocker") is None
        assert get_kv(conn, "promising_candidates") == 1
        assert get_kv(conn, "turbo_foundry_v2_latest_metrics")[
            "promotion_candidates_queued"
        ] == 4
    finally:
        conn.close()


def test_completed_historical_route_is_not_replayed_after_reconciliation_event(
    tmp_path: Path,
) -> None:
    controller, conn, _paths = _controller(tmp_path)
    try:
        enqueue_experiment(
            conn,
            MINI_MICRO_PARTICIPATION_DIVERGENCE_EXPERIMENT_ID,
            {"experiment_type": "mini_micro_participation_divergence"},
        )
        claimed = claim_next_experiment(conn)
        assert claimed is not None
        complete_experiment(
            conn,
            MINI_MICRO_PARTICIPATION_DIVERGENCE_EXPERIMENT_ID,
            {
                "scientific_conclusion": (
                    "MINI_MICRO_PARTICIPATION_PRIMARY_INSUFFICIENT_PIVOT_MECHANISM"
                ),
                "candidate_count": 96,
                "promising_candidates": 0,
                "candidates": [],
                "result_hash": "b" * 64,
            },
            claim_token=str(claimed["claim_token"]),
        )
        controller._reconcile_completed_experiments(conn)
        assert get_kv(conn, "current_blocker") == "DISTINCT_MECHANISM_OR_FORWARD_DATA_REQUIRED"

        set_kv(conn, "current_phase", "PLANNING_NEXT_ACTION")
        set_kv(conn, "current_blocker", None)
        controller._reconcile_completed_experiments(conn)
        assert get_kv(conn, "current_phase") == "PLANNING_NEXT_ACTION"
        assert get_kv(conn, "current_blocker") is None
    finally:
        conn.close()


def test_turbo_commit_drift_recovery_advances_to_fresh_batch_id(tmp_path: Path) -> None:
    controller, conn, _paths = _controller(tmp_path)
    try:
        for index in range(2):
            experiment_id = f"turbo_foundry_v2_epoch_{index:04d}"
            enqueue_experiment(
                conn,
                experiment_id,
                {"experiment_type": "turbo_foundry_v2_epoch", "batch_index": index},
            )
            claimed = claim_next_experiment(conn)
            assert claimed is not None
            complete_experiment(
                conn,
                experiment_id,
                {
                    "batch_index": index,
                    "scientific_conclusion": f"TURBO_{index}_COMPLETE",
                    "result_hash": str(index) * 64,
                },
                claim_token=str(claimed["claim_token"]),
            )
        failed_id = "turbo_foundry_v2_epoch_0002"
        enqueue_experiment(
            conn,
            failed_id,
            {
                "experiment_type": "turbo_foundry_v2_epoch",
                "batch_index": 2,
                "max_attempts": 1,
            },
        )
        failed_claim = claim_next_experiment(conn)
        assert failed_claim is not None
        assert (
            fail_experiment(
                conn,
                failed_id,
                "worker commit differs",
                retryable=True,
                claim_token=str(failed_claim["claim_token"]),
            )
            == "FAILED"
        )

        assert controller._next_turbo_batch_index(conn) == 3
        assert controller._pending_or_next_turbo_batch_index(conn) == 3
        controller._refresh_latest_completed_experiment_metadata(conn)
        assert get_kv(conn, "latest_completed_experiment")["experiment_id"] == (
            "turbo_foundry_v2_epoch_0001"
        )
    finally:
        conn.close()


def test_turbo_stale_failed_blocker_reuses_pending_epoch(tmp_path: Path) -> None:
    controller, conn, _paths = _controller(tmp_path)
    try:
        failed_id = "turbo_foundry_v2_epoch_0004"
        enqueue_experiment(
            conn,
            failed_id,
            {
                "experiment_type": "turbo_foundry_v2_epoch",
                "batch_index": 4,
                "max_attempts": 1,
            },
        )
        failed_claim = claim_next_experiment(conn)
        assert failed_claim is not None
        assert (
            fail_experiment(
                conn,
                failed_id,
                "Quality-diversity caps yielded too few structures",
                retryable=True,
                claim_token=str(failed_claim["claim_token"]),
            )
            == "FAILED"
        )

        pending_id = "turbo_foundry_v2_epoch_0005"
        enqueue_experiment(
            conn,
            pending_id,
            {
                "experiment_type": "turbo_foundry_v2_epoch",
                "batch_index": 5,
            },
        )
        assert controller._pending_or_next_turbo_batch_index(conn) == 5

        pending_claim = claim_next_experiment(conn)
        assert pending_claim is not None
        assert pending_claim["experiment_id"] == pending_id
        assert controller._pending_or_next_turbo_batch_index(conn) == 5

        set_kv(conn, "current_phase", "EXPERIMENT_BLOCKED")
        set_kv(conn, "current_blocker", f"EXPERIMENT_FAILED:{failed_id}")
        resumed_index = controller._pending_or_next_turbo_batch_index(conn)
        assert controller._reconcile_turbo_foundry_v2(
            conn, batch_index=resumed_index
        )

        assert experiment_record(conn, failed_id)["status"] == "FAILED"
        assert experiment_record(conn, pending_id)["status"] == "RUNNING"
        assert experiment_record(conn, "turbo_foundry_v2_epoch_0006") is None
        assert get_kv(conn, "current_phase") == "PLANNING_NEXT_ACTION"
        assert get_kv(conn, "current_blocker") is None
    finally:
        conn.close()
