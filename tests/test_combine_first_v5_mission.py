from __future__ import annotations

from pathlib import Path

import pytest

from hydra.mission.controller import (
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


@pytest.fixture(autouse=True)
def _freeze_external_q4_ledger_state(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep these controller unit tests independent of the live access ledger.

    Each test constructs the intended one-shot Q4 state in its temporary
    mission database.  Operational append-only forward records in the shared
    repository ledger must not leak into this isolated unit-test fixture.
    """

    monkeypatch.setattr("hydra.mission.controller.q4_access_count", lambda: 1)


def _controller(tmp_path: Path) -> tuple[AutonomousMissionController, object]:
    paths = mission_paths(str(tmp_path / "state"))
    conn = connect_state(paths)
    ensure_experiment_schema(conn)
    controller = AutonomousMissionController(
        MissionControllerConfig(
            mission_id="combine-v5-test",
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


def test_v5_policy_gate_requires_committed_q4_and_no_unresolved_failure(
    tmp_path: Path,
) -> None:
    controller, conn = _controller(tmp_path)
    try:
        set_kv(conn, "current_phase", "PLANNING_NEXT_ACTION")
        set_kv(conn, "q4_access_count", 1)
        assert not controller._combine_first_v5_should_run(conn)
        set_kv(conn, "decision_bridge_v4_status", "Q4_ONE_SHOT_COMMITTED")
        assert controller._combine_first_v5_should_run(conn)
        set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
        assert not controller._combine_first_v5_should_run(conn)
    finally:
        conn.close()


def test_v5_reconcile_queues_one_development_only_epoch(tmp_path: Path) -> None:
    controller, conn = _controller(tmp_path)
    try:
        assert controller._reconcile_combine_first_v5(conn, epoch_index=0)
        assert controller._reconcile_combine_first_v5(conn, epoch_index=0)
        record = experiment_record(conn, COMBINE_FIRST_V5_INITIAL_EXPERIMENT_ID)
        assert record is not None and record["status"] == "QUEUED"
        specification = record["specification"]
        assert specification["experiment_type"] == "combine_first_evolution_v5"
        assert specification["proposal_count"] == 5_000
        assert specification["exact_limit"] == 200
        assert specification["mutation_limit"] == 60
        assert specification["maximum_episode_starts"] == 24
        assert specification["q4_access_allowed"] is False
        assert specification["q4_reuse_prohibited"] is True
        assert specification["paid_data_allowed"] is False
        assert specification["network_allowed"] is False
        assert specification["live_or_broker_allowed"] is False
        assert specification["outbound_order_capability"] is False
    finally:
        conn.close()


def test_v5_guard_rejects_any_protected_scope_expansion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    controller, conn = _controller(tmp_path)
    try:
        assert controller._reconcile_combine_first_v5(conn, epoch_index=0)
        record = experiment_record(conn, COMBINE_FIRST_V5_INITIAL_EXPERIMENT_ID)
        assert record is not None
        exact = dict(record["specification"])
        monkeypatch.setattr(
            "hydra.mission.controller.check_action_allowed",
            lambda *args, **kwargs: None,
        )
        controller._check_experiment_allowed(conn, exact)
        for changed in (
            {"q4_access_allowed": True},
            {"paid_data_allowed": True},
            {"network_allowed": True},
            {"live_or_broker_allowed": True},
            {"outbound_order_capability": True},
            {"development_end_exclusive": "2025-01-01"},
        ):
            with pytest.raises(RuntimeError, match="development-only scope"):
                controller._check_experiment_allowed(conn, {**exact, **changed})
    finally:
        conn.close()


def test_completed_v5_epoch_is_accounted_once_and_queues_next(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    controller, conn = _controller(tmp_path)
    try:
        monkeypatch.setattr(controller, "_tick_shadow_pipeline", lambda _conn: {})
        assert controller._reconcile_combine_first_v5(conn, epoch_index=0)
        claimed = claim_next_experiment(conn)
        assert claimed is not None
        result = {
            "schema": "hydra_combine_first_evolution_v5_epoch_v1",
            "epoch_index": 0,
            "candidate_count": 1_000,
            "structural_proposals": 1_000,
            "fast_screens": 1_000,
            "fast_screen_survivors": 20,
            "rolling_candidates_evaluated": 12,
            "mutation_children_evaluated": 6,
            "rolling_episode_count": 108,
            "factory_survivor_count": 1,
            "combine_elite_count": 1,
            "xfa_candidate_count": 0,
            "defensive_candidate_count": 0,
            "mutation_success_count": 1,
            "mutation_success_rate": 1.0 / 6.0,
            "combine_pass_rate_distribution": {"count": 18, "median": 0.1},
            "mll_breach_rate_distribution": {"count": 18, "median": 0.0},
            "payout_cycle_distribution": {"count": 18, "median": 0.0},
            "performance": {"rolling_candidates_per_hour": 100.0},
            "archive": {"candidate_count": 1},
            "scientific_conclusion": "COMBINE_FIRST_ELITES_FOUND",
            "interpretation_boundary": "development evidence only",
            "report_path": "combine_v5_report.md",
            "result_hash": "a" * 64,
            "paper_shadow_ready": 0,
            "governance": {
                "q4_access_count_delta": 0,
                "network_requests": 0,
                "incremental_databento_spend_usd": 0.0,
                "outbound_order_capability": False,
            },
            "candidates": [
                {
                    "candidate_id": "combine-elite-1",
                    "status": "PROMISING_RESEARCH_CANDIDATE",
                    "mechanism_family": "market_state_geometry",
                    "primary_market": "NQ",
                    "execution_market": "MNQ",
                    "role": "COMBINE_PASSER",
                    "objective_pool": "COMBINE_PASSER_POOL",
                    "net_pnl": 10_000.0,
                    "topstep_path_candidate": True,
                }
            ],
        }
        complete_experiment(
            conn,
            COMBINE_FIRST_V5_INITIAL_EXPERIMENT_ID,
            result,
            claim_token=str(claimed["claim_token"]),
        )

        controller._reconcile_completed_combine_first_v5_epochs(conn)
        controller._reconcile_completed_combine_first_v5_epochs(conn)

        assert get_kv(conn, "combine_first_v5_completed_generations") == 1
        assert get_kv(conn, "combine_first_v5_total_rolling_episodes") == 108
        assert get_kv(conn, "strategy_prototypes_generated") == 1_000
        assert get_kv(conn, "topstep_path_candidates") == 1
        assert get_kv(conn, "combine_first_v5_accounted_epochs") == [
            COMBINE_FIRST_V5_INITIAL_EXPERIMENT_ID
        ]
        next_record = experiment_record(
            conn, "combine_first_evolution_v5_epoch_0001"
        )
        assert next_record is not None and next_record["status"] == "QUEUED"
        assert get_kv(conn, "current_blocker") is None
    finally:
        conn.close()
