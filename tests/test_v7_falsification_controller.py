from __future__ import annotations

import json
from pathlib import Path

import pytest

from hydra.mission.experiment_queue import experiment_counts
from hydra.mission.mission_state import (
    connect_state_readonly,
    mission_paths,
    state_snapshot,
)
from hydra.mission.watchdog import heartbeat_status, scheduler_health
from hydra.mission.v7_falsification_controller import (
    CONTROLLER_SCHEMA,
    V7ControllerConfig,
    V7ControllerIntegrityError,
    V7FalsificationController,
    _classify_v71_power_aware_action,
    classify_v7_action,
)
from scripts.run_v7_falsification_mission import main


def _write_tribunal(root: Path, *, verdict: str, selected: list[str]) -> None:
    path = root / "reports/v7/data/d1_candidate_tribunal_result.json"
    path.parent.mkdir(parents=True)
    path.write_text(
        json.dumps(
            {
                "verdict": verdict,
                "selected_shadow_queue_candidate_ids": selected,
            }
        ),
        encoding="utf-8",
    )


def test_classification_waits_for_atomic_tribunal(tmp_path: Path) -> None:
    result = classify_v7_action(tmp_path)

    assert result["action_type"] == "D1_CANDIDATE_TRIBUNAL_PENDING"
    assert result["progressed"] is False


def test_null_tribunal_pivots_at_class_level(tmp_path: Path) -> None:
    _write_tribunal(tmp_path, verdict="NULL", selected=[])

    result = classify_v7_action(tmp_path)

    assert result["action_type"] == "D1_CLASS_TOMBSTONE_REQUIRED"
    assert result["progressed"] is False


def test_green_tribunal_requires_fiche_then_boundary(tmp_path: Path) -> None:
    candidate = "candidate_a"
    _write_tribunal(tmp_path, verdict="GREEN", selected=[candidate])

    missing = classify_v7_action(tmp_path)
    assert missing["action_type"] == "CANDIDATE_FICHE_FREEZE_REQUIRED"

    fiche = tmp_path / "WORM/candidates/candidate_a.json"
    fiche.parent.mkdir(parents=True)
    fiche.write_text("{}", encoding="utf-8")
    no_boundary = classify_v7_action(tmp_path)
    assert no_boundary["action_type"] == "FORWARD_BOUNDARY_MANIFEST_REQUIRED"

    boundary = tmp_path / "mission/state/v7_forward_boundary_manifest.json"
    boundary.parent.mkdir(parents=True)
    boundary.write_text("{}", encoding="utf-8")
    ready = classify_v7_action(tmp_path)
    assert ready["action_type"] == "FORWARD_FEED_READY"


def test_inconsistent_tribunal_fails_closed(tmp_path: Path) -> None:
    _write_tribunal(tmp_path, verdict="GREEN", selected=[])

    with pytest.raises(V7ControllerIntegrityError):
        classify_v7_action(tmp_path)


def test_v71_controller_selects_next_power_aware_grammar(tmp_path: Path) -> None:
    policy = tmp_path / "WORM/v7.1-hierarchical-validation-policy-2026-07-12.json"
    policy.parent.mkdir(parents=True)
    policy.write_text("{}", encoding="utf-8")
    artifacts = {
        "reports/v7_1/calibration/v71_power_audit_result.json": {"verdict": "RED"},
        "reports/v7_1/calibration/v71_power_sample_extension_result.json": {
            "verdict": "GREEN",
            "minimum_required_event_count": 320,
        },
        "reports/v7_1/discovery/v71_signal_manifest.json": {"candidate_count": 256},
        "reports/v7_1/discovery/v71_development_funnel_result.json": {
            "walk_forward_positive_count": 11,
            "powered_walk_forward_candidate_count": 0,
        },
        "reports/v7_1/forensics/v71_mechanism_forensics_result.json": {
            "MINI_MICRO_DIVERGENCE": {"mechanism": "MECHANISM_CONFIRMED_DEAD"}
        },
    }
    for relative, payload in artifacts.items():
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload), encoding="utf-8")

    result = classify_v7_action(tmp_path)

    assert result["action_type"] == "V71_OPPORTUNITY_DENSITY_GRAMMAR_REQUIRED"
    assert result["walk_forward_positive_count"] == 11
    assert result["minimum_powered_events"] == 320
    assert result["new_data_purchase_authorized"] is False


def test_v71_controller_recognizes_frozen_g2_confirmation_queue(
    tmp_path: Path,
) -> None:
    policy = tmp_path / "WORM/v7.1-hierarchical-validation-policy-2026-07-12.json"
    policy.parent.mkdir(parents=True)
    policy.write_text("{}", encoding="utf-8")
    artifacts = {
        "reports/v7_1/calibration/v71_power_audit_result.json": {"verdict": "RED"},
        "reports/v7_1/calibration/v71_power_sample_extension_result.json": {
            "verdict": "GREEN",
            "minimum_required_event_count": 320,
        },
        "reports/v7_1/discovery/v71_signal_manifest.json": {"candidate_count": 256},
        "reports/v7_1/discovery/v71_development_funnel_result.json": {
            "walk_forward_positive_count": 11,
            "powered_walk_forward_candidate_count": 0,
        },
        "reports/v7_1/forensics/v71_mechanism_forensics_result.json": {
            "MINI_MICRO_DIVERGENCE": {"mechanism": "MECHANISM_CONFIRMED_DEAD"}
        },
        "WORM/v7.1-opportunity-density-grammar-0002-2026-07-12.json": {},
        "reports/v7_1/discovery_0002/v71_opportunity_density_signal_manifest.json": {
            "candidate_count": 128
        },
        "reports/v7_1/discovery_0002/v71_opportunity_density_funnel_result.json": {
            "raw_global_N_trials": 262356,
            "walk_forward_positive_count": 3,
            "powered_walk_forward_candidate_count": 0,
        },
        "reports/v7_1/discovery_0002/v71_opportunity_density_tripwire_result.json": {
            "verdict": "GREEN_NULL_ADJUSTED_BASELINE",
            "NULL_RATIO": 0.75,
            "evidence_strength": "VERT_NET",
        },
        "WORM/v7.1-independent-confirmation-queue-0001-2026-07-12.json": {
            "queue_status": "QUEUED_NO_DATA_PURCHASE_AUTHORIZED_IN_V7_1",
            "candidates": [
                {"candidate_id": "a"},
                {"candidate_id": "b"},
                {"candidate_id": "c"},
            ],
        },
    }
    for relative, payload in artifacts.items():
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload), encoding="utf-8")
    hashes = {
        "reports/v7_1/discovery_0002/v71_opportunity_density_signal_manifest.json": "c90a2321fc66e114d65dd533d077ec04308ae714369e28b82f5d9e996dd7fa24",
        "reports/v7_1/discovery_0002/v71_opportunity_density_funnel_result.json": "2a45c4da55875f90438cd6cb19f1ce79ec8de7d934f7a442e78000364aff5897",
        "reports/v7_1/discovery_0002/v71_opportunity_density_tripwire_result.json": "dddabdad7e828e84bbee974dc47432a1a90b2a1989d26a44d48bf88cef91cbb2",
    }
    original_sha = __import__(
        "hydra.mission.v7_falsification_controller",
        fromlist=["_sha256"],
    )._sha256

    def fake_sha(path: Path) -> str:
        relative = str(path.relative_to(tmp_path))
        return hashes.get(relative, original_sha(path))

    from unittest.mock import patch

    with patch(
        "hydra.mission.v7_falsification_controller._sha256",
        side_effect=fake_sha,
    ):
        result = classify_v7_action(tmp_path)

    assert result["action_type"] == "V71_CONFIRMATION_QUEUE_FROZEN_DISCOVERY_CONTINUES"
    assert result["confirmation_candidate_count"] == 3
    assert result["new_data_purchase_authorized"] is False
    assert result["shadow_admission_authorized"] is False


def test_v71_power_aware_integrated_action_reports_g9_falsification() -> None:
    result = _classify_v71_power_aware_action(
        Path.cwd(), prior_positive=11, g2_positive=3
    )

    assert result["action_type"] == "V71_G9_FORMULATIONS_FALSIFIED_TRIPWIRE_UNDERPOWERED"
    assert result["walk_forward_positive_count"] == 24
    assert result["powered_candidate_count"] == 0
    assert result["rolling_combine_promotions"] == 0
    assert result["g6_candidate_count"] == 6
    assert result["g6_walk_forward_positive_count"] == 2
    assert result["g6_tripwire_verdict"] == "GREEN_NULL_ADJUSTED_BASELINE"
    assert result["g5_cemetery_candidate_count"] == 12
    assert result["confirmation_queue_underpowered_count"] == 16
    assert result["evidence_reconciliation_accounted_count"] == 24
    assert result["evidence_reconciliation_unaccounted_count"] == 0
    assert result["underpowered_combine_selected_count"] == 5
    assert result["underpowered_combine_episode_start_count"] == 24
    assert result["underpowered_combine_effective_block_count"] == 4
    assert result["underpowered_combine_candidate_pass_count"] == 0
    assert result["underpowered_combine_basket_pass_count"] == 0
    assert result["underpowered_combine_validated_count"] == 0
    assert result["g7_candidate_count"] == 6
    assert result["g7_signal_count"] == 1889
    assert result["g7_stage1_survivor_count"] == 0
    assert result["g7_walk_forward_positive_count"] == 0
    assert result["g7_tripwire_verdict"] == "ARTEFACT_GEOMETRY_ONLY"
    assert result["g7_real_pass_count"] == "5/120"
    assert result["g7_null_pass_count"] == "17/360"
    assert result["g7_cemetery_candidate_count"] == 6
    assert result["g8_feature_row_count"] == 17200
    assert result["g8_candidate_count"] == 6
    assert result["g8_signal_count"] == 2182
    assert result["g8_stage1_survivor_count"] == 2
    assert result["g8_walk_forward_positive_count"] == 2
    assert result["g8_tripwire_verdict"] == "GREEN_NULL_ADJUSTED_BASELINE"
    assert result["g8_tripwire_evidence_strength"] == "VERT_MINCE"
    assert result["g8_power_status_counts"] == {"WF_POSITIVE_BUT_FRAGILE": 2}
    assert result["g8_powered_candidate_count"] == 0
    assert result["g9_feature_row_count"] == 17200
    assert result["g9_candidate_count"] == 4
    assert result["g9_signal_count"] == 1573
    assert result["g9_stage1_survivor_count"] == 0
    assert result["g9_walk_forward_positive_count"] == 0
    assert result["g9_formulation_falsified_count"] == 4
    assert result["g9_tripwire_verdict"] == "BLOCKED_UNDERPOWERED"
    assert result["g9_NULL_RATIO"] is None
    assert result["g9_real_pass_count"] == "0/80"
    assert result["g9_null_pass_count"] == "9/240"
    assert result["g9_power_audit_executed"] is False
    assert result["confirmation_queue_fragile_retired_count"] == 6
    assert result["evidence_reconciliation_accounted_count"] == 24
    assert result["evidence_reconciliation_unaccounted_count"] == 0
    assert result["broad_D1_generation_authorized"] is False
    assert result["new_data_purchase_authorized"] is False
    assert result["shadow_admission_authorized"] is False


def test_v72_integrated_action_records_leakage_safe_crossfit_null() -> None:
    result = classify_v7_action(Path.cwd())

    assert CONTROLLER_SCHEMA == "hydra_v7_2_pareto_crossfit_controller_v1"
    assert result["action_type"] == (
        "V72_STATIC_BASKET_CROSS_FIT_NULL_DISTINCT_MECHANISM_PIVOT"
    )
    assert result["v72_static_structure_count"] == 1009
    assert result["v72_design_episode_count"] == 24216
    assert result["v72_cross_fit_rotation_count"] == 4
    assert result["v72_held_out_basket_evaluation_count"] == 12
    assert result["v72_held_out_pass_count"] == 1
    assert result["v72_held_out_mll_breach_count"] == 7
    assert result["v72_parent_dominated_count"] == 9
    assert result["v72_cross_fit_survivor_count"] == 0
    assert result["v72_risk_overlay_executed_count"] == 0
    assert result["v72_promotion_to_48_starts_count"] == 0
    assert result["new_data_purchase_authorized"] is False
    assert result["protected_holdout_access_authorized"] is False
    assert result["shadow_admission_authorized"] is False


def test_controller_rejects_live_trading() -> None:
    with pytest.raises(V7ControllerIntegrityError):
        V7FalsificationController(V7ControllerConfig(no_live_trading=False))


def test_controller_migrates_fresh_mission_schema_before_first_step(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    controller = V7FalsificationController(
        V7ControllerConfig(
            project_root=str(tmp_path),
            state_dir="mission/state",
            sleep_seconds=0.0,
            checkpoint_every_steps=25,
            persistent=True,
            maximum_steps=1,
        )
    )
    monkeypatch.setattr(controller, "_verify_constitution", lambda: "# MISSION HYDRA V7\n")
    monkeypatch.setattr(
        "hydra.mission.v7_falsification_controller.load_and_verify",
        lambda _path: {"entries": []},
    )
    monkeypatch.setattr(
        "hydra.mission.v7_falsification_controller.classify_v7_action",
        lambda _root: {
            "action_type": "V71_TEST_CONTINUATION",
            "phase": "4",
            "progressed": True,
            "reason": "deterministic fresh-state smoke",
        },
    )

    assert controller.run() == 0

    paths = mission_paths(str(tmp_path / "mission/state"))
    import sqlite3

    conn = sqlite3.connect(paths.db_path)
    try:
        columns = {
            str(row[1]) for row in conn.execute("PRAGMA table_info(experiments)")
        }
        row = conn.execute(
            "SELECT status,experiment_type FROM experiments"
        ).fetchone()
    finally:
        conn.close()
    assert {"experiment_type", "completed_at", "claim_token"} <= columns
    assert row == ("COMPLETED", "v7_falsification_perpetual")
    heartbeat = json.loads(paths.heartbeat_path.read_text(encoding="utf-8"))
    assert heartbeat["outbound_orders"] == 0


def test_running_v7_controller_renews_watchdog_lease(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    controller = V7FalsificationController(
        V7ControllerConfig(
            project_root=str(tmp_path),
            state_dir="mission/state",
            sleep_seconds=0.0,
            checkpoint_every_steps=25,
        )
    )
    monkeypatch.setattr(controller, "_verify_constitution", lambda: "# MISSION HYDRA V7\n")
    monkeypatch.setattr(
        "hydra.mission.v7_falsification_controller.load_and_verify",
        lambda _path: {"entries": []},
    )
    monkeypatch.setattr(
        "hydra.mission.v7_falsification_controller.classify_v7_action",
        lambda _root: {
            "action_type": "V71_TEST_CONTINUATION",
            "phase": "4",
            "progressed": True,
            "reason": "deterministic lease smoke",
        },
    )
    from hydra.mission.mission_state import connect_state

    conn = connect_state(controller.paths)
    try:
        controller._initialize(conn)
        controller._step(conn)
    finally:
        conn.close()

    readonly = connect_state_readonly(controller.paths)
    try:
        snapshot = state_snapshot(readonly)
        counts = experiment_counts(readonly)
    finally:
        readonly.close()
    health = scheduler_health(
        heartbeat_status(controller.paths), snapshot, counts
    )
    assert health["classification"] == "HEALTHY_AND_PROGRESSING"
    assert snapshot["current_experiment"]["claimed_by"] == (
        "v7_2_pareto_crossfit_controller"
    )
    assert snapshot["broker_order_capability"] is False
    heartbeat = heartbeat_status(controller.paths).payload
    assert heartbeat["remaining_databento_budget_usd"] == 125.0
    assert heartbeat["q4_access_count"] == 0


def test_runner_uses_non_restarting_integrity_exit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "scripts.run_v7_falsification_mission.parse_args",
        lambda: type(
            "Args",
            (),
            {
                "project_root": ".",
                "state_dir": "mission/state",
                "sleep_seconds": 0.0,
                "checkpoint_every_steps": 25,
                "persistent": True,
                "maximum_steps": 1,
                "no_live_trading": True,
            },
        )(),
    )
    monkeypatch.setattr(
        "scripts.run_v7_falsification_mission.run_v7_controller",
        lambda _config: (_ for _ in ()).throw(V7ControllerIntegrityError("drift")),
    )

    assert main() == 78
