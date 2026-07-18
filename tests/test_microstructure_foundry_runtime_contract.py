from __future__ import annotations

import json
from pathlib import Path

from hydra.economic_evolution.schema import stable_hash
from hydra.mission.economic_evolution_manifest_runtime import (
    EconomicEvolutionManifestRuntime,
)
from hydra.production.microstructure_foundry_manifest import CAMPAIGN_ID
from hydra.production.microstructure_foundry_pilot import FoundryPilotConfig
from hydra.production.microstructure_foundry_runtime import (
    KPI_SCHEMA,
    RESULT_SCHEMA,
    STATE_SCHEMA,
    _build_terminal_result,
    _pilot_config,
    _wait_for_acquisition_receipt,
    _write_state,
)


def _manifest() -> dict[str, object]:
    return {
        "schema": "hydra_economic_production_manifest_v1",
        "campaign_id": CAMPAIGN_ID,
        "campaign_mode": "MICROSTRUCTURE_ORDER_FLOW_FOUNDRY",
        "manifest_hash": "a" * 64,
        "source_commit": "c" * 40,
        "runtime": {
            "engine": "production_kernel_v1",
            "output_dir": (
                "reports/economic_evolution/"
                "microstructure_order_flow_foundry_0031"
            ),
            "result_name": "economic_production_result.json",
        },
        "evidence_bundle": {"destination": "data/cache/evidence_bundles"},
        "multiplicity": {
            "expected_global_N_trials_after_reservation": 1_000_000,
        },
        "market_selection": {
            "selected_markets": ["NQ", "YM"],
            "explicit_contracts": ["NQU4", "YMU4"],
        },
        "account_rule_snapshot": {
            "profit_target_usd": 9_000.0,
            "maximum_loss_limit_usd": 4_500.0,
            "best_day_consistency_fraction": 0.5,
            "costs_and_slippage": {"stressed_multiplier": 1.5},
        },
        "bounded_pilot": {
            "minimum_candidates": 20,
            "maximum_candidates": 40,
            "green_gate": {"minimum_useful_mechanism_families": 3},
        },
        "terminal_baseline_0029": {
            "best_ohlcv_baseline": {
                "stressed_target_progress_pct": 8.097,
            }
        },
    }


def _pilot() -> dict[str, object]:
    metrics = {
        "exact_replay_count": 24,
        "control_replay_count": 72,
        "normal_episode_count": 360,
        "stressed_episode_count": 360,
        "positive_stressed_count": 7,
        "normal_pass_candidate_count": 3,
        "stressed_pass_candidate_count": 2,
        "normal_p5_pass_rate_best": 0.10,
        "normal_p5_pass_rate_median": 0.01,
        "stressed_p5_pass_rate_best": 0.05,
        "stressed_p5_pass_rate_median": 0.0,
        "stressed_p5_target_progress_best_pct": 15.0,
        "stressed_p5_target_progress_population_median_pct": 2.0,
        "mll_breach_rate_min": 0.0,
        "mll_breach_rate_max": 0.05,
        "near_pass_count": 2,
        "economic_cpu_to_wall_ratio": 0.85,
        "exact_replays_per_hour": 2_400.0,
        "combine_episodes_per_hour": 72_000.0,
    }
    return {
        "campaign_id": CAMPAIGN_ID,
        "pilot_status": "MICROSTRUCTURE_PILOT_WEAK",
        "decision_report": {
            "production_kpis": metrics,
            "target_velocity_uplift_ratio": 1.85,
            "useful_mechanism_families": [
                "ABSORPTION_REVERSAL",
                "INITIATIVE_CONTINUATION",
            ],
            "teacher_counts": {"ABSORPTION": 11, "DEPLETION": 5},
            "students": [{"tier": "L1"}, {"tier": "L2"}],
            "green_checks": {
                "material_target_velocity_uplift_over_ohlcv": True,
                "three_distinct_useful_mechanism_families": False,
            },
            "candidates": [
                {"sleeve_id": "sleeve-a", "serious": True},
                {"sleeve_id": "sleeve-b", "serious": True},
            ],
        },
        "runtime_kpis": {"event_count": 123_456},
        "compact_outputs": {"event_count": 123_456},
    }


def _evidence_receipt() -> dict[str, object]:
    return {
        "contract": "HYDRA_EVIDENCE_BUNDLE_V1",
        "schema_version": 1,
        "campaign_id": CAMPAIGN_ID,
        "evidence_status": "FRESH_DEVELOPMENT_EVIDENCE",
        "reconstruction_flag": False,
        "bundle_path": "/tmp/0031.evidence-v1",
        "manifest_path": "/tmp/0031.evidence-v1/manifest.json",
        "manifest_sha256": "d" * 64,
        "bundle_content_sha256": "e" * 64,
        "dataset_row_counts": {"episodes": 720},
    }


def test_live_state_and_kpis_are_adoptable_by_stable_v17(tmp_path: Path) -> None:
    manifest = _manifest()
    output = (
        tmp_path
        / "reports/economic_evolution/microstructure_order_flow_foundry_0031"
    )
    state = _write_state(
        output,
        manifest,
        state="STARTING",
        stage="MICROSTRUCTURE_ACQUISITION_AWAITING_EXECUTION",
        next_action="WAIT_FOR_MANIFEST_BOUND_ACQUISITION_RECEIPT",
    )

    assert state["schema"] == STATE_SCHEMA
    runtime = EconomicEvolutionManifestRuntime(tmp_path, tmp_path / "mission/state")
    assert runtime._production_resume_state(manifest, output, required=True)[
        "runner_pid"
    ] == state["runner_pid"]
    kpis = runtime._load_production_kpis(manifest, output)
    assert kpis is not None
    assert kpis["schema"] == KPI_SCHEMA
    assert kpis["workers"] == {"compute": 3, "evidence_writer": 1}


def test_terminal_result_maps_without_reinterpretation_in_stable_v17(
    tmp_path: Path,
) -> None:
    manifest = _manifest()
    pilot = _pilot()
    result = _build_terminal_result(
        manifest=manifest,
        pilot=pilot,
        evidence_receipt=_evidence_receipt(),
        decision="MICROSTRUCTURE_PILOT_WEAK",
        cost_report={"cost_matrix_hash": "f" * 64},
        acquisition={
            "requests": [
                {"request": {"schema": "mbo", "session_count": 5}}
            ]
        },
        actual_spend_usd=3.25,
        remaining_budget_usd=33.90,
    )

    assert result["schema"] == RESULT_SCHEMA
    assert result["result_hash"] == stable_hash(
        {key: value for key, value in result.items() if key != "result_hash"}
    )
    runtime = EconomicEvolutionManifestRuntime(tmp_path, tmp_path / "mission/state")
    action = runtime._production_complete_action({}, manifest, result)
    assert action["manifest_campaign_scientific_status"] == (
        "MICROSTRUCTURE_PILOT_WEAK"
    )
    assert action["manifest_campaign_exact_account_replays"] == 24
    assert action["manifest_campaign_rolling_combine_episode_count"] == 720
    assert action["manifest_campaign_stressed_positive_policy_count"] == 7
    runtime._verify_production_successor_recommendation(
        result["autonomous_next_action"]
    )


def test_pilot_configuration_binds_source_and_acquisition_provenance() -> None:
    config = _pilot_config(
        _manifest(),
        FoundryPilotConfig,
        acquisition={"receipt_hash": "b" * 64},
    )

    assert config.source_commit == "c" * 40
    assert config.acquisition_receipt_hash == "b" * 64
    assert config.manifest_hash == "a" * 64


def test_acquisition_wait_keeps_same_worker_alive_and_heartbeats(
    tmp_path: Path,
) -> None:
    manifest = _manifest()
    output = (
        tmp_path
        / "reports/economic_evolution/microstructure_order_flow_foundry_0031"
    )
    receipt = output / "microstructure_acquisition_receipt.json"
    clock_values = iter((0.0, 1.0))
    sleep_calls: list[float] = []

    def sleep(seconds: float) -> None:
        sleep_calls.append(seconds)
        receipt.parent.mkdir(parents=True, exist_ok=True)
        receipt.write_text(json.dumps({"arrived": True}), encoding="utf-8")

    _wait_for_acquisition_receipt(
        output,
        manifest,
        cost_report={
            "acquisition_plan": {
                "projected_incremental_spend_usd": 3.25,
                "projected_remaining_usd": 33.90,
            }
        },
        receipt_path=receipt,
        poll_interval_seconds=1.0,
        heartbeat_interval_seconds=1.0,
        sleep=sleep,
        monotonic=lambda: next(clock_values),
    )

    assert sleep_calls == [1.0]
    state = json.loads((output / "production_state.json").read_text())
    assert state["state"] == "STARTING"
    assert state["receipt_poll_count"] == 1
    assert state["runner_pid"] > 0
