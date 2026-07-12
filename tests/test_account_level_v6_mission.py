from __future__ import annotations

from pathlib import Path

import pytest

from hydra.mission.controller import (
    ACCOUNT_LEVEL_V6_INITIAL_EXPERIMENT_ID,
    AutonomousMissionController,
    COMBINE_FIRST_V5_INITIAL_EXPERIMENT_ID,
    MissionControllerConfig,
)
from hydra.mission.experiment_queue import (
    claim_next_experiment,
    complete_experiment,
    ensure_experiment_schema,
    experiment_record,
)
from hydra.mission.mission_state import connect_state, get_kv, mission_paths, set_kv


def _controller(tmp_path: Path) -> tuple[AutonomousMissionController, object]:
    paths = mission_paths(str(tmp_path / "state"))
    conn = connect_state(paths)
    ensure_experiment_schema(conn)
    controller = AutonomousMissionController(
        MissionControllerConfig(
            mission_id="account-v6-test",
            baseline_commit="baseline",
            objective_config="test",
            remaining_databento_budget_usd=70.0,
            persistent=False,
            state_dir=str(paths.state_dir),
            sleep_seconds=0.0,
            workers=3,
        )
    )
    return controller, conn


def _enable_v6(conn: object) -> None:
    set_kv(conn, "current_phase", "PLANNING_NEXT_ACTION")
    set_kv(conn, "q4_access_count", 1)
    set_kv(conn, "decision_bridge_v4_status", "Q4_ONE_SHOT_COMMITTED")
    set_kv(conn, "combine_first_v5_completed_generations", 6)


def test_v6_policy_gate_requires_completed_v5_and_committed_prior_q4(
    tmp_path: Path,
) -> None:
    controller, conn = _controller(tmp_path)
    try:
        set_kv(conn, "current_phase", "PLANNING_NEXT_ACTION")
        set_kv(conn, "q4_access_count", 1)
        set_kv(conn, "decision_bridge_v4_status", "Q4_ONE_SHOT_COMMITTED")
        set_kv(conn, "combine_first_v5_completed_generations", 2)
        assert not controller._account_level_v6_should_run(conn)
        set_kv(conn, "combine_first_v5_completed_generations", 3)
        assert controller._account_level_v6_should_run(conn)
        set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
        assert not controller._account_level_v6_should_run(conn)
    finally:
        conn.close()


def test_v6_freezes_queued_v5_generation_and_preserves_elites(
    tmp_path: Path,
) -> None:
    controller, conn = _controller(tmp_path)
    try:
        assert controller._reconcile_combine_first_v5(conn, epoch_index=0)
        controller._freeze_saturated_v5_grammar(conn)
        record = experiment_record(conn, COMBINE_FIRST_V5_INITIAL_EXPERIMENT_ID)
        assert record is not None and record["status"] == "BLOCKED"
        assert (
            get_kv(conn, "combine_first_v5_grammar_status")
            == "V5_GRAMMAR_SATURATED_KEEP_ELITES_ONLY"
        )
        migration = get_kv(conn, "account_level_v6_migration")
        assert migration["v5_components_preserved"] is True
        assert migration["q4_reuse_authorized"] is False
    finally:
        conn.close()


def test_v6_reconcile_queues_one_powered_development_generation(
    tmp_path: Path,
) -> None:
    controller, conn = _controller(tmp_path)
    try:
        _enable_v6(conn)
        assert controller._reconcile_account_level_v6(conn, generation_index=0)
        assert controller._reconcile_account_level_v6(conn, generation_index=0)
        record = experiment_record(conn, ACCOUNT_LEVEL_V6_INITIAL_EXPERIMENT_ID)
        assert record is not None and record["status"] == "QUEUED"
        specification = record["specification"]
        assert specification["experiment_type"] == "account_level_evolution_v6"
        assert specification["grammar_count"] == 480
        assert specification["basket_count"] == 600
        assert specification["target_velocity_mutation_limit"] == 24
        assert specification["screening_starts"] == 24
        assert specification["promotion_starts"] == 48
        assert specification["worker_count"] == 3
        assert specification["q4_access_allowed"] is False
        assert specification["q4_reuse_prohibited"] is True
        assert specification["paid_data_allowed"] is False
        assert specification["network_allowed"] is False
        assert specification["live_or_broker_allowed"] is False
        assert specification["outbound_order_capability"] is False
        assert get_kv(conn, "account_level_v6_queues")["shadow_queue"] == (
            "WAITING_FOR_FRESH_FORWARD_DATA"
        )
    finally:
        conn.close()


def test_v6_guard_rejects_scope_or_power_expansion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    controller, conn = _controller(tmp_path)
    try:
        _enable_v6(conn)
        assert controller._reconcile_account_level_v6(conn, generation_index=0)
        record = experiment_record(conn, ACCOUNT_LEVEL_V6_INITIAL_EXPERIMENT_ID)
        assert record is not None
        exact = dict(record["specification"])
        monkeypatch.setattr(
            "hydra.mission.controller.check_action_allowed",
            lambda *args, **kwargs: None,
        )
        controller._check_experiment_allowed(conn, exact)
        for changed in (
            {"q4_access_allowed": True},
            {"q4_reuse_prohibited": False},
            {"paid_data_allowed": True},
            {"network_allowed": True},
            {"live_or_broker_allowed": True},
            {"outbound_order_capability": True},
            {"development_end_exclusive": "2025-01-01"},
            {"screening_starts": 23},
            {"promotion_starts": 47},
            {"basket_count": 199},
            {"target_velocity_mutation_limit": 7},
        ):
            with pytest.raises(RuntimeError, match="minimum-power scope"):
                controller._check_experiment_allowed(conn, {**exact, **changed})
    finally:
        conn.close()


def test_completed_v6_generation_is_accounted_once_and_refills_all_queues(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    controller, conn = _controller(tmp_path)
    try:
        _enable_v6(conn)
        monkeypatch.setattr(controller, "_tick_shadow_pipeline", lambda _conn: {})
        assert controller._reconcile_account_level_v6(conn, generation_index=0)
        claimed = claim_next_experiment(conn)
        assert claimed is not None
        result = {
            "schema": "hydra_account_level_evolution_v6_epoch_v1",
            "generation_index": 0,
            "scientific_conclusion": "ACCOUNT_LEVEL_ELITES_FOUND",
            "component_bank": {
                "total_components": 36,
                "behavioral_clusters": 30,
            },
            "new_mechanism_grammar": {
                "structures_generated": 480,
                "accepted_components": 2,
                "novelty_yield": 2 / 480,
            },
            "populations": {
                "individuals_evaluated": 30,
                "baskets_evaluated": 600,
                "controllers_evaluated": 160,
                "promotion_objects_evaluated": 12,
                "total_rolling_combine_episodes": 20_000,
            },
            "individuals": {"pass_rate": {"max": 0.0833}},
            "baskets": {"pass_rate": {"max": 0.50}},
            "controllers": {"pass_rate": {"max": 0.55}},
            "promotion": {"pass_rate": {"max": 0.45}},
            "policy_improvements": {"baskets_improving_pass_rate": 100},
            "archive": {"occupied_niches": 50},
            "performance": {"worker_count": 3},
            "individual_combine_elites": [],
            "account_basket_elites": ["basket-a", "basket-b"],
            "account_controller_elites": ["controller-a"],
            "xfa_payout_elites": ["component-xfa"],
            "persistent_queues": {
                "individual_evolution_queue": "COMPLETED_AND_REFILL_PENDING",
                "basket_evolution_queue": "COMPLETED_AND_REFILL_PENDING",
                "controller_evolution_queue": "COMPLETED_AND_REFILL_PENDING",
                "new_grammar_queue": "COMPLETED_AND_REFILL_PENDING",
                "xfa_queue": "COMPONENT_EVIDENCE_PRESERVED",
                "shadow_queue": "WAITING_FOR_FRESH_FORWARD_DATA",
            },
            "report_path": "account_v6_report.md",
            "result_hash": "a" * 64,
            "paper_shadow_ready": 0,
            "governance": {
                "q4_access_count_delta": 0,
                "network_requests": 0,
                "incremental_databento_spend_usd": 0.0,
                "outbound_order_capability": False,
            },
        }
        complete_experiment(
            conn,
            ACCOUNT_LEVEL_V6_INITIAL_EXPERIMENT_ID,
            result,
            claim_token=str(claimed["claim_token"]),
        )

        controller._reconcile_completed_account_level_v6_generations(conn)
        controller._reconcile_completed_account_level_v6_generations(conn)

        assert get_kv(conn, "account_level_v6_completed_generations") == 1
        assert get_kv(conn, "account_level_v6_total_rolling_episodes") == 20_000
        assert get_kv(conn, "account_policy_elite_count") == 3
        assert get_kv(conn, "account_level_v6_elite_archive_count") == 4
        assert get_kv(conn, "paper_shadow_ready_candidates", 0) == 0
        assert get_kv(conn, "account_level_v6_accounted_generations") == [
            ACCOUNT_LEVEL_V6_INITIAL_EXPERIMENT_ID
        ]
        next_record = experiment_record(
            conn, "account_level_evolution_v6_generation_0001"
        )
        assert next_record is not None and next_record["status"] == "QUEUED"
        assert get_kv(conn, "current_blocker") is None
    finally:
        conn.close()
