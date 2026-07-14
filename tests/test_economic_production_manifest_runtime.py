from __future__ import annotations

import json
import hashlib
import shutil
from pathlib import Path
from typing import Any

import pytest

from hydra.economic_evolution.schema import stable_hash
from hydra.mission.economic_evolution_manifest_runtime import (
    EconomicEvolutionManifestRuntime,
    EconomicEvolutionRuntimeError,
    PRODUCTION_ENGINE,
    PRODUCTION_KPI_NAME,
    PRODUCTION_KPI_SCHEMA,
    PRODUCTION_STATE_NAME,
    PRODUCTION_STATE_SCHEMA,
)
from hydra.research.v7_graveyard import class_feedback


def _write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n")


def _config(root: Path) -> dict[str, Any]:
    runner = root / "scripts/run_economic_production_manifest.py"
    runner.parent.mkdir(parents=True, exist_ok=True)
    runner.write_text("raise SystemExit(0)\n")
    manifest = root / "config/v7/economic_production.json"
    manifest.parent.mkdir(parents=True, exist_ok=True)
    manifest.write_text("{}\n")
    return {
        "schema": "hydra_economic_production_manifest_v1",
        "campaign_id": "hydra_economic_production_test",
        "class_id": "ECONOMIC_PRODUCTION_TEST_V1",
        "source_commit": "c" * 40,
        "manifest_hash": "a" * 64,
        "_runtime_preregistration_path": str(manifest),
        "runtime": {
            "engine": PRODUCTION_ENGINE,
            "runner": "scripts/run_economic_production_manifest.py",
            "output_dir": "reports/economic_evolution/economic_production_test",
            "result_name": "economic_production_result.json",
        },
        "evidence_bundle": {"destination": "data/cache/evidence_bundles"},
        "multiplicity": {
            "reserved_delta_trials": 30_000,
            "expected_global_N_trials_after_reservation": 522_099,
        },
    }


def _state(config: dict[str, Any], **overrides: Any) -> dict[str, Any]:
    value: dict[str, Any] = {
        "schema": PRODUCTION_STATE_SCHEMA,
        "campaign_id": config["campaign_id"],
        "manifest_hash": config["manifest_hash"],
        "source_commit": config["source_commit"],
        "state": "FAST_SCREEN_COMPLETE",
        "stage": "STAGE_1_FAST_SCREEN",
        "checkpoint_sequence": 4,
        "started_at_utc": "2026-07-14T12:00:00Z",
        "updated_at_utc": "2026-07-14T12:05:00Z",
        "runner_pid": 4242,
        "worker_count": 3,
        "evidence_writer_count": 1,
        "policies_proposed": 20_000,
        "unique_policies_screened": 4_096,
        "exact_account_replays": 0,
        "combine_episodes_completed": 0,
        "next_action": "EXACT_ACCOUNT_REPLAY",
        "last_completed_policy_id": "policy-4095",
        "evidence_staging_path": (
            "data/cache/evidence_bundles/"
            ".hydra_economic_production_test.evidence-v1.staging"
        ),
        "evidence_final_path": (
            "data/cache/evidence_bundles/"
            "hydra_economic_production_test.evidence-v1"
        ),
        "broker_connections": 0,
        "orders": 0,
        "q4_access_count_delta": 0,
        "data_purchase_count": 0,
    }
    value.update(overrides)
    value["state_hash"] = stable_hash(value)
    return value


def _kpis(config: dict[str, Any], **overrides: Any) -> dict[str, Any]:
    value: dict[str, Any] = {
        "schema": PRODUCTION_KPI_SCHEMA,
        "campaign_id": config["campaign_id"],
        "manifest_hash": config["manifest_hash"],
        "source_commit": config["source_commit"],
        "checkpoint_sequence": 4,
        "updated_at_utc": "2026-07-14T12:05:00Z",
        "state": "FAST_SCREEN_COMPLETE",
        "policies_proposed": 20_000,
        "unique_policies_screened": 4_096,
        "exact_account_replays": 0,
        "combine_episodes_completed": 0,
        "normal_episodes_completed": 0,
        "stressed_episodes_completed": 0,
        "positive_stressed_net_candidates": 0,
        "candidates_with_normal_pass": 0,
        "candidates_with_stressed_pass": 0,
        "best_normal_pass_rate": 0.0,
        "best_stressed_pass_rate": 0.0,
        "median_normal_pass_rate": 0.0,
        "median_stressed_pass_rate": 0.0,
        "near_pass_count": 0,
        "candidates_promoted_96": 0,
        "confirmation_ready_candidates": 0,
        "duplicate_rejection_rate": 0.75,
        "cache_hit_rate": 1.0,
        "economic_research_wall_clock_fraction": 0.90,
        "cpu_utilization_fraction": 0.80,
        "rates_per_hour": {
            "policies_proposed": 40_000.0,
            "unique_policies_screened": 8_192.0,
            "exact_account_replays": 0.0,
            "combine_episodes": 0.0,
        },
        "workers": {"compute": 3, "evidence_writer": 1},
        "matched_controls_status": "PENDING_STAGE_4_NOT_EXECUTED",
        "null_status": "PENDING_STAGE_4_NOT_EXECUTED",
        "admin_overhead_alert": False,
        "broker_connections": 0,
        "orders": 0,
        "q4_access_count_delta": 0,
        "data_purchase_count": 0,
    }
    value.update(overrides)
    value["kpi_hash"] = stable_hash(value)
    return value


def _terminal_result(config: dict[str, Any]) -> dict[str, Any]:
    kpis = _kpis(
        config,
        exact_account_replays=512,
        combine_episodes_completed=49_152,
        normal_episodes_completed=24_576,
        stressed_episodes_completed=24_576,
        positive_stressed_net_candidates=137,
        candidates_with_normal_pass=42,
        candidates_with_stressed_pass=21,
        best_normal_pass_rate=0.1875,
        best_stressed_pass_rate=0.125,
        median_normal_pass_rate=0.0417,
        median_stressed_pass_rate=0.0208,
        near_pass_count=19,
    )
    production_kpis = {
        field: kpis[field]
        for field in (
            "rates_per_hour",
            "economic_research_wall_clock_fraction",
            "cpu_utilization_fraction",
            "workers",
            "duplicate_rejection_rate",
            "cache_hit_rate",
        )
    }
    return {
        "status": "COMPLETE",
        "scientific_status": "DEVELOPMENT_WAVE_COMPLETE",
        "kpis": kpis,
        "economic_results": {
            "schema": "hydra_production_campaign_summary_v1",
            "campaign_id": config["campaign_id"],
            "candidate_count": 4,
            "positive_stressed_net_count": 3,
            "normal_pass_candidate_count": 2,
            "stressed_pass_candidate_count": 1,
            "confirmation_ready_candidate_ids": ["policy-final"],
            "development_only": True,
            "independently_confirmed": False,
            "production_counters": {
                "serious_exact_account_replays": 512,
                "predeclared_control_policy_replays": 348,
                "combine_episodes_completed": 49_152,
                "normal_episodes_completed": 24_576,
                "stressed_episodes_completed": 24_576,
            },
            "production_kpis": production_kpis,
            "economic_frontier": {
                "candidate_count": 4,
                "normal_pass_fraction_best": 0.1875,
                "normal_pass_fraction_median": 0.09375,
                "stressed_pass_fraction_best": 0.125,
                "stressed_pass_fraction_median": 0.0625,
                "stressed_target_progress_median_best": 1.11,
                "stressed_target_progress_median_population": 0.72,
                "stressed_mll_breach_rate_minimum": 0.0,
                "stressed_mll_breach_rate_maximum": 0.04,
                "positive_stressed_net_count": 3,
            },
        },
        "successive_halving": {"stage_decisions": []},
        "failure_vectors": {"counts": {}},
        "matched_controls": {"status": "EXECUTED"},
        "evidence_bundle": {
            "evidence_status": "FRESH_DEVELOPMENT_EVIDENCE",
            "bundle_content_sha256": "d" * 64,
        },
        "q4_access_delta": 0,
        "new_data_purchase_count": 0,
        "broker_connections": 0,
        "orders": 0,
        "result_hash": "e" * 64,
    }


def test_production_checkpoint_is_preserved_and_resumable(tmp_path: Path) -> None:
    config = _config(tmp_path)
    runtime = EconomicEvolutionManifestRuntime(tmp_path, tmp_path / "mission/state")
    output, result = runtime._paths(config)
    state_path = output / PRODUCTION_STATE_NAME
    _write_json(state_path, _state(config))

    runtime._quarantine_incomplete_attempt(config, output, result)

    assert state_path.is_file()
    assert not (tmp_path / "reports/economic_evolution/quarantine").exists()
    assert runtime._production_resume_state(config, output)["checkpoint_sequence"] == 4


@pytest.mark.parametrize(
    "component_state",
    ["COMPONENT_LEDGER_COMPLETE", "COMPONENT_LEDGER_COMPILED", "FINALIZING"],
)
def test_component_ledger_checkpoint_names_are_resumable(
    tmp_path: Path, component_state: str
) -> None:
    config = _config(tmp_path)
    runtime = EconomicEvolutionManifestRuntime(tmp_path, tmp_path / "mission/state")
    output, _ = runtime._paths(config)
    _write_json(
        output / PRODUCTION_STATE_NAME,
        _state(config, state=component_state),
    )

    assert runtime._production_resume_state(config, output)["state"] == component_state


def test_production_complete_without_atomic_result_fails_closed(tmp_path: Path) -> None:
    config = _config(tmp_path)
    runtime = EconomicEvolutionManifestRuntime(tmp_path, tmp_path / "mission/state")
    output, result = runtime._paths(config)
    _write_json(output / PRODUCTION_STATE_NAME, _state(config, state="COMPLETE"))

    with pytest.raises(EconomicEvolutionRuntimeError, match="terminal result is missing"):
        runtime._quarantine_incomplete_attempt(config, output, result)


def test_production_live_kpis_enforce_hash_workers_and_safety(tmp_path: Path) -> None:
    config = _config(tmp_path)
    runtime = EconomicEvolutionManifestRuntime(tmp_path, tmp_path / "mission/state")
    output, _ = runtime._paths(config)
    path = output / PRODUCTION_KPI_NAME
    _write_json(path, _kpis(config))
    assert runtime._load_production_kpis(config, output)["workers"] == {
        "compute": 3,
        "evidence_writer": 1,
    }

    unsafe = _kpis(config)
    unsafe["orders"] = 1
    unsafe["kpi_hash"] = stable_hash(
        {key: value for key, value in unsafe.items() if key != "kpi_hash"}
    )
    _write_json(path, unsafe)
    with pytest.raises(EconomicEvolutionRuntimeError, match="orders"):
        runtime._load_production_kpis(config, output)


def test_production_deployment_requires_source_worm_head_ancestry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _config(tmp_path)
    runtime = EconomicEvolutionManifestRuntime(tmp_path, tmp_path / "mission/state")
    calls: list[tuple[str, str]] = []

    monkeypatch.setattr(
        "hydra.mission.economic_evolution_manifest_runtime.subprocess.check_output",
        lambda *args, **kwargs: "d" * 40 + "\n",
    )

    class _Completed:
        returncode = 0

    def _run(command: list[str], **kwargs: Any) -> _Completed:
        calls.append((command[-2], command[-1]))
        return _Completed()

    monkeypatch.setattr(
        "hydra.mission.economic_evolution_manifest_runtime.subprocess.run",
        _run,
    )
    runtime._verify_production_deployment_ancestry(config, "b" * 40)

    assert calls == [
        (config["source_commit"], "b" * 40),
        ("b" * 40, "d" * 40),
    ]


def test_production_deployment_rejects_unrelated_live_head(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _config(tmp_path)
    runtime = EconomicEvolutionManifestRuntime(tmp_path, tmp_path / "mission/state")
    monkeypatch.setattr(
        "hydra.mission.economic_evolution_manifest_runtime.subprocess.check_output",
        lambda *args, **kwargs: "d" * 40 + "\n",
    )
    return_codes = iter((0, 1))

    class _Completed:
        def __init__(self, returncode: int) -> None:
            self.returncode = returncode

    monkeypatch.setattr(
        "hydra.mission.economic_evolution_manifest_runtime.subprocess.run",
        lambda *args, **kwargs: _Completed(next(return_codes)),
    )

    with pytest.raises(EconomicEvolutionRuntimeError, match="live HEAD"):
        runtime._verify_production_deployment_ancestry(config, "b" * 40)


def test_production_worker_command_uses_manifest_and_resumes_in_place(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _config(tmp_path)
    runtime = EconomicEvolutionManifestRuntime(tmp_path, tmp_path / "mission/state")
    output, _ = runtime._paths(config)
    calls: list[list[str]] = []

    class _FakeProcess:
        pid = 8811
        returncode = None

        def poll(self) -> None:
            return None

    def _popen(command: list[str], **_: Any) -> _FakeProcess:
        calls.append(command)
        return _FakeProcess()

    monkeypatch.setattr(
        "hydra.mission.economic_evolution_manifest_runtime.subprocess.Popen",
        _popen,
    )
    runtime._start_worker(config, output)

    assert calls and "--manifest" in calls[0]
    assert "--output-dir" not in calls[0]
    assert "--preregistration" not in calls[0]
    assert calls[0][calls[0].index("--manifest") + 1] == config[
        "_runtime_preregistration_path"
    ]
    assert runtime.snapshot()["production_research_worker_count"] == 3
    assert runtime.snapshot()["production_evidence_writer_count"] == 1


def test_verified_production_resume_does_not_consume_retry_budget(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _config(tmp_path)
    runtime = EconomicEvolutionManifestRuntime(tmp_path, tmp_path / "mission/state")
    output, _ = runtime._paths(config)
    _write_json(output / PRODUCTION_STATE_NAME, _state(config))

    class _FakeProcess:
        pid = 8812
        returncode = None

        def poll(self) -> None:
            return None

    monkeypatch.setattr(
        "hydra.mission.economic_evolution_manifest_runtime.subprocess.Popen",
        lambda *args, **kwargs: _FakeProcess(),
    )
    runtime._start_worker(config, output)

    assert runtime._attempts == {}
    assert (output / PRODUCTION_STATE_NAME).is_file()


def test_identical_no_result_worker_exits_stop_relaunch_loop(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    runtime = EconomicEvolutionManifestRuntime(tmp_path, tmp_path / "mission/state")
    output, _ = runtime._paths(config)
    _write_json(output / PRODUCTION_STATE_NAME, _state(config))
    runtime._active_config = config
    runtime._active_campaign_id = str(config["campaign_id"])

    assert (
        runtime._record_production_no_result_exit(
            config, output, worker_exit_code=0
        )
        is False
    )
    assert (
        runtime._record_production_no_result_exit(
            config, output, worker_exit_code=0
        )
        is True
    )
    persisted = json.loads(runtime.runtime_state_path.read_text())
    guard = next(iter(persisted["production_no_result_exits"].values()))
    assert guard["identical_exit_count"] == 2

    _write_json(
        output / PRODUCTION_STATE_NAME,
        _state(
            config,
            exact_account_replays=2,
            combine_episodes_completed=192,
            last_completed_policy_id="policy-0002",
        ),
    )
    assert (
        runtime._record_production_no_result_exit(
            config, output, worker_exit_code=0
        )
        is False
    )


def test_disabled_queue_restart_persists_idle_without_worker(
    tmp_path: Path,
) -> None:
    queue = {
        "schema": "hydra_manifest_campaign_queue_v1",
        "runtime_policy": {
            "reload_queue_each_controller_step": True,
            "controller_source_change_for_new_manifest": False,
            "single_active_campaign": True,
            "single_authoritative_mission_writer": True,
        },
        "governance": {
            "q4_access_allowed": False,
            "new_data_purchase_allowed": False,
            "broker_or_orders_allowed": False,
            "proof_window_consumption_allowed": False,
        },
        "entries": [
            {
                "ordinal": 1,
                "campaign_id": "disabled-production-campaign",
                "enabled": False,
            }
        ],
    }
    queue["queue_hash"] = stable_hash(queue)
    _write_json(
        tmp_path / "config/v7/economic_evolution_production_queue.json",
        queue,
    )
    state_dir = tmp_path / "mission/state"
    runtime_state_path = state_dir / "economic_evolution_manifest_runtime.json"
    _write_json(
        runtime_state_path,
        {
            "schema": "hydra_manifest_campaign_runtime_v1",
            "state": "RUNNING",
            "campaign_id": "disabled-production-campaign",
            "attempts": {"completed-revision": 1},
            "production_no_result_exits": {},
            "production_successor_handoffs": [],
            "worker_pid": 999_999,
            "worker_exit_code": None,
            "engine": PRODUCTION_ENGINE,
            "production_state_path": "/stale/production_state.json",
            "production_kpi_path": "/stale/production_kpis.json",
        },
    )

    runtime = EconomicEvolutionManifestRuntime(tmp_path, state_dir)
    action = runtime.advance({"action_type": "TERMINAL_PREDECESSOR"})

    persisted = json.loads(runtime_state_path.read_text())
    assert action["manifest_campaign_runtime_state"] == "MANIFEST_QUEUE_EMPTY"
    assert persisted["state"] == "IDLE"
    assert persisted["campaign_id"] is None
    assert persisted["worker_pid"] is None
    assert persisted["engine"] is None
    assert persisted["production_state_path"] is None
    assert persisted["production_kpi_path"] is None
    assert persisted["attempts"] == {"completed-revision": 1}
    snapshot = runtime.snapshot()
    assert snapshot["state"] == "IDLE"
    assert snapshot["active_campaign_id"] is None
    assert snapshot["worker_pid"] is None
    assert snapshot["production_research_worker_count"] == 0
    assert snapshot["production_evidence_writer_count"] == 0


def test_production_running_action_surfaces_live_economic_counters(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    runtime = EconomicEvolutionManifestRuntime(tmp_path, tmp_path / "mission/state")
    output, _ = runtime._paths(config)
    _write_json(output / PRODUCTION_STATE_NAME, _state(config))
    _write_json(output / PRODUCTION_KPI_NAME, _kpis(config))
    reservation = {"multiplicity": {"delta_trials": 30_000}}

    action = runtime._running_action({}, config, reservation)

    assert action["action_type"] == "MANIFEST_ECONOMIC_PRODUCTION_RUNNING"
    assert action["manifest_campaign_policies_proposed"] == 20_000
    assert action["manifest_campaign_unique_policies_screened"] == 4_096
    assert action["manifest_campaign_worker_count"] == 3
    assert action["manifest_campaign_evidence_writer_count"] == 1
    assert action["orders"] == 0
    assert action["q4_access_delta"] == 0


def test_production_complete_maps_nested_campaign_summary_truth(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    runtime = EconomicEvolutionManifestRuntime(tmp_path, tmp_path / "mission/state")
    result = _terminal_result(config)

    action = runtime._production_complete_action({}, config, result)

    assert action["manifest_campaign_scientific_status"] == (
        "DEVELOPMENT_WAVE_COMPLETE"
    )
    assert action["manifest_campaign_exact_account_replays"] == 512
    assert action["manifest_campaign_predeclared_control_policy_replays"] == 348
    assert action["manifest_campaign_rolling_combine_episode_count"] == 49_152
    assert action["manifest_campaign_normal_episode_count"] == 24_576
    assert action["manifest_campaign_stressed_episode_count"] == 24_576
    assert action["manifest_campaign_stressed_positive_policy_count"] == 3
    assert action["manifest_campaign_policies_with_normal_pass_count"] == 2
    assert action["manifest_campaign_policies_with_stressed_pass_count"] == 1
    assert action["manifest_campaign_best_normal_pass_rate"] == 0.1875
    assert action["manifest_campaign_median_stressed_pass_rate"] == 0.0625
    assert action["manifest_campaign_target_progress_frontier"] == {
        "stressed_target_progress_median_best": 1.11,
        "stressed_target_progress_median_population": 0.72,
    }
    assert action["manifest_campaign_mll_frontier"] == {
        "stressed_mll_breach_rate_minimum": 0.0,
        "stressed_mll_breach_rate_maximum": 0.04,
    }
    assert action["manifest_campaign_economic_frontier"]["candidate_count"] == 4
    assert action["manifest_campaign_near_pass_count"] == 19


def test_production_complete_rejects_partial_nested_summary_instead_of_zeros(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    runtime = EconomicEvolutionManifestRuntime(tmp_path, tmp_path / "mission/state")
    result = _terminal_result(config)
    del result["economic_results"]["economic_frontier"]

    with pytest.raises(
        EconomicEvolutionRuntimeError, match="partial nested terminal payload"
    ):
        runtime._production_complete_action({}, config, result)


def test_production_complete_requires_scientific_status_not_terminal_status(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    runtime = EconomicEvolutionManifestRuntime(tmp_path, tmp_path / "mission/state")
    result = _terminal_result(config)
    del result["scientific_status"]

    with pytest.raises(EconomicEvolutionRuntimeError, match="scientific_status"):
        runtime._production_complete_action({}, config, result)


def test_production_complete_accepts_explicit_empty_economic_frontier(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    runtime = EconomicEvolutionManifestRuntime(tmp_path, tmp_path / "mission/state")
    result = _terminal_result(config)
    summary = result["economic_results"]
    summary.update(
        candidate_count=0,
        positive_stressed_net_count=0,
        normal_pass_candidate_count=0,
        stressed_pass_candidate_count=0,
        confirmation_ready_candidate_ids=[],
        economic_frontier={"candidate_count": 0, "positive_stressed_net_count": 0},
    )

    action = runtime._production_complete_action({}, config, result)

    assert action["manifest_campaign_economic_frontier"] == {
        "candidate_count": 0,
        "positive_stressed_net_count": 0,
        "normal_pass_fraction_best": None,
        "normal_pass_fraction_median": None,
        "stressed_pass_fraction_best": None,
        "stressed_pass_fraction_median": None,
        "stressed_target_progress_median_best": None,
        "stressed_target_progress_median_population": None,
        "stressed_mll_breach_rate_minimum": None,
        "stressed_mll_breach_rate_maximum": None,
    }
    assert action["manifest_campaign_best_normal_pass_rate"] is None


def test_production_terminalization_does_not_require_legacy_tripwire(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    runtime = EconomicEvolutionManifestRuntime(tmp_path, tmp_path / "mission/state")
    result = {
        "evidence_bundle": {
            "evidence_status": "FRESH_DEVELOPMENT_EVIDENCE",
            "bundle_content_sha256": "d" * 64,
        },
        "autonomous_next_action": {
            "action": "QUEUE_FAILURE_GUIDED_WAVE",
            "manifest_required": True,
            "mechanism_targets": ["TARGET_TOO_SLOW"],
            "reason": "advance survivors",
            "q4_access_authorized": False,
            "new_data_purchase_authorized": False,
        },
        "result_hash": "e" * 64,
    }

    action = runtime._terminalize({}, config, result, tmp_path)

    assert action["manifest_campaign_terminal_state"] == (
        "PRODUCTION_EVIDENCE_BUNDLE_COMPLETE"
    )
    assert action["next_experiment_id"] == "QUEUE_FAILURE_GUIDED_WAVE"
    assert action["next_experiment_state"] == "WORM_MANIFEST_REQUIRED"
    assert action["manifest_campaign_independently_confirmed"] is False
    assert runtime.snapshot()["production_successor_handoff_count"] == 1

    repeated = runtime._terminalize({}, config, result, tmp_path)
    assert repeated["manifest_campaign_successor_handoff_id"] == action[
        "manifest_campaign_successor_handoff_id"
    ]
    assert runtime.snapshot()["production_successor_handoff_count"] == 1


def test_no_survivor_tombstones_exact_class_and_requires_worm_successor(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    state_dir = tmp_path / "mission/state"
    state_dir.mkdir(parents=True)
    shutil.copy2(Path("mission/state/graveyard.db"), state_dir / "graveyard.db")
    output_dir = tmp_path / "reports/economic_evolution/production-test"
    output_dir.mkdir(parents=True)
    result = _terminal_result(config)
    summary = result["economic_results"]
    frontier = {
        **summary["economic_frontier"],
        "candidate_count": 256,
        "normal_pass_fraction_best": 0.0,
        "normal_pass_fraction_median": 0.0,
        "stressed_pass_fraction_best": 0.0,
        "stressed_pass_fraction_median": 0.0,
        "stressed_target_progress_median_best": 0.0,
        "stressed_target_progress_median_population": 0.0,
        "stressed_mll_breach_rate_minimum": 0.0,
        "stressed_mll_breach_rate_maximum": 0.0,
        "positive_stressed_net_count": 0,
    }
    summary.update(
        candidate_count=256,
        positive_stressed_net_count=0,
        normal_pass_candidate_count=0,
        stressed_pass_candidate_count=0,
        confirmation_ready_candidate_ids=[],
        economic_frontier=frontier,
    )
    result["successive_halving"] = {
        "stage_decisions": [
            {
                "stage": "STAGE_3_ROLLING_COMBINE",
                "input_count": 256,
                "output_count": 0,
                "selected_policy_ids": [],
            }
        ]
    }
    result["autonomous_next_action"] = {
        "action": "QUEUE_MATERIALLY_DISTINCT_MECHANISM_MANIFEST",
        "manifest_required": True,
        "q4_access_authorized": False,
        "new_data_purchase_authorized": False,
    }
    runtime = EconomicEvolutionManifestRuntime(tmp_path, state_dir)

    action = runtime._terminalize({}, config, result, output_dir)

    assert action["manifest_campaign_terminal_state"] == (
        "PRODUCTION_EVIDENCE_COMPLETE_EXACT_CLASS_TOMBSTONED"
    )
    assert action["manifest_campaign_same_class_relaunch_allowed"] is False
    assert action["next_experiment_state"] == "WORM_MANIFEST_REQUIRED"
    rows = [
        row
        for row in class_feedback(state_dir / "graveyard.db")
        if row["mechanism_class"] == config["class_id"]
    ]
    assert rows == [
        {
            "mechanism_class": config["class_id"],
            "regime": "DEVELOPMENT_MANIFEST_DRIVEN_COMPLETE_EVIDENCE",
            "death_cause": "NO_SUCCESSIVE_HALVING_SURVIVOR",
            "candidate_count": 512,
        }
    ]

    resumed = EconomicEvolutionManifestRuntime(tmp_path, state_dir)
    repeated = resumed._terminalize({}, config, result, output_dir)
    assert repeated["manifest_campaign_tombstone_signature_hash"] == action[
        "manifest_campaign_tombstone_signature_hash"
    ]
    assert resumed.snapshot()["production_successor_handoff_count"] == 1

def test_production_complete_requires_evidence_receipt(tmp_path: Path) -> None:
    config = _config(tmp_path)
    runtime = EconomicEvolutionManifestRuntime(tmp_path, tmp_path / "mission/state")
    with pytest.raises(EconomicEvolutionRuntimeError, match="EvidenceBundle receipt"):
        runtime._require_production_terminal_evidence(config, {})


def test_production_terminal_evidence_uses_deep_relational_verification(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _config(tmp_path)
    runtime = EconomicEvolutionManifestRuntime(tmp_path, tmp_path / "mission/state")
    bundle = (
        tmp_path
        / "data/cache/evidence_bundles"
        / "hydra_economic_production_test.evidence-v1"
    )
    manifest_path = bundle / "evidence_bundle_manifest.json"
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text("{}\n")
    manifest_sha = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    calls: list[dict[str, Any]] = []

    def _verify(path: Path, **kwargs: Any) -> dict[str, Any]:
        calls.append({"path": path, **kwargs})
        return {
            "bundle_content_sha256": "b" * 64,
            "evidence_status": "FRESH_DEVELOPMENT_EVIDENCE",
            "reconstruction_flag": False,
            "dataset_row_counts": {"episodes": 96},
        }

    monkeypatch.setattr(
        "hydra.mission.economic_evolution_manifest_runtime."
        "require_complete_evidence_bundle",
        _verify,
    )
    result = {
        "evidence_bundle": {
            "contract": "HYDRA_EVIDENCE_BUNDLE_V1",
            "schema_version": 1,
            "campaign_id": config["campaign_id"],
            "bundle_path": str(bundle),
            "manifest_path": str(manifest_path),
            "manifest_sha256": manifest_sha,
            "bundle_content_sha256": "b" * 64,
            "evidence_status": "FRESH_DEVELOPMENT_EVIDENCE",
            "reconstruction_flag": False,
            "dataset_row_counts": {"episodes": 96},
        },
        "evidence_verification_manifest_sha256": manifest_sha,
    }

    runtime._require_production_terminal_evidence(config, result)

    assert calls == [
        {
            "path": bundle,
            "campaign_id": config["campaign_id"],
            "deep": True,
        }
    ]
