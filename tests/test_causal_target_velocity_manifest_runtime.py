from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from hydra.economic_evolution.schema import stable_hash
from hydra.evidence import REQUIRED_DATASETS
from hydra.mission.economic_evolution_manifest_runtime import (
    EconomicEvolutionManifestRuntime,
)
from hydra.production.causal_target_velocity_manifest import (
    CAUSAL_TARGET_VELOCITY_ENGINE,
    CausalTargetVelocityManifestError,
    load_and_validate_causal_target_velocity_manifest,
)


def _write(path: Path, value: str) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _fixture(tmp_path: Path) -> tuple[Path, dict[str, Any]]:
    _write(tmp_path / "MISSION_CONTRACT.md", "test\n")
    implementation = {
        name: _write(tmp_path / name, f"# {name}\n")
        for name in (
            "scripts/run_causal_target_velocity_manifest.py",
            "hydra/production/causal_target_velocity_manifest.py",
            "hydra/mission/economic_evolution_manifest_runtime.py",
        )
    }
    source_path = tmp_path / "reports/economic_evolution/causal_salvage/result.json"
    source_sha = _write(source_path, "{}\n")
    manifest: dict[str, Any] = {
        "schema": "hydra_causal_target_velocity_manifest_v1",
        "campaign_id": "hydra_causal_target_velocity_0028",
        "class_id": (
            "TARGET_BEFORE_ADVERSE_EXCURSION_HAZARD_OPPORTUNITY_DENSITY_V1"
        ),
        "development_only": True,
        "source_commit": "a" * 40,
        "economic_hypothesis": "Causal hazard and density improve target velocity.",
        "implementation_files": implementation,
        "terminal_predecessors": {
            "operating_package_v1": "RETRACTED_DEVELOPMENT_EVIDENCE_CONTAMINATED",
            "causal_salvage_0027": "CAUSAL_SALVAGE_GATE_FALSIFIED",
            "retry_former_six_books_allowed": False,
            "inherit_economic_or_xfa_status": False,
        },
        "clean_causal_baseline": {
            "sleeves_evaluated": 18,
            "positive_stressed_sleeves_at_90d": 17,
            "normal_combine_passes": 0,
            "stressed_combine_passes": 0,
            "former_book_count": 6,
            "former_book_passes_all_horizons": 0,
            "best_former_book_full_horizon_stressed_median_target_progress": 0.0656,
            "observed_mll_breaches": 0,
            "stressed_median_minimum_mll_buffer_usd": 4396.70,
            "worst_observed_minimum_mll_buffer_usd": 1869.73,
            "former_b4_pass_concentration_disappeared": True,
            "all_former_economic_and_xfa_statuses_withdrawn": True,
            "phase_b_closed": True,
            "xfa_closed": True,
            "forward_closed": True,
            "component_status": "LOW_VELOCITY_CAUSAL_COMPONENTS",
            "promotion_status": None,
            "sources": {
                "causal_salvage_result": {
                    "path": source_path.relative_to(tmp_path).as_posix(),
                    "file_sha256": source_sha,
                }
            },
        },
        "causal_event_contract": {
            "kernel": "SHARED_CAUSAL_DECISION_KERNEL",
            "fill_policy": "CAUSAL_NEXT_TRADABLE_OPEN_V1",
            "availability_rule": "AVAILABLE_AT_LTE_DECISION_TIME",
            "future_outcomes_are_labels_only": True,
            "missing_future_coverage": "CENSORED_FUTURE_COVERAGE",
            "missing_future_suppresses_signal": False,
            "batch_streaming_decision_equality_required": True,
            "persist_timestamps": [
                "event_time",
                "available_at",
                "decision_time",
                "earliest_executable_time",
                "fill_time",
            ],
            "forbidden_decision_inputs": [
                "FUTURE_LABEL_AVAILABILITY",
                "NEGATIVE_SHIFT",
                "NEXT_BAR_AT_EARLIER_TIMESTAMP",
                "FUTURE_CONTINUITY",
                "CENTERED_ROLLING",
                "BACKWARD_FILL_ACROSS_DECISION_TIME",
            ],
        },
        "risk_frontier_preflight": {
            "normalized_levels": [0.75, 1.0, 1.25, 1.5],
            "executable_micro_quantities": [3, 4, 5, 6],
            "executable_reference_micro_quantity": 4,
            "positive_sleeve_count": 17,
            "diagnostic_former_book_count": 6,
            "maximum_campaign_compute_fraction": 0.10,
            "neighbor_multiplier_tuning_allowed": False,
            "proceed_to_discovery_after_failure": True,
            "failure_status": "RISK_SCALE_ONLY_FALSIFIED",
            "success_gate": {
                "minimum_normal_passes_out_of_48": 3,
                "minimum_stressed_passes_out_of_48": 2,
                "positive_stressed_net_required": True,
                "maximum_mll_breach_rate": 0.10,
            },
        },
        "target_before_adverse_labels": {
            "favorable_levels_r": [0.5, 1.0, 1.5, 2.0],
            "adverse_levels_r": [0.5, 0.75, 1.0],
            "horizons": ["5m", "15m", "30m", "60m", "SESSION", "OVERNIGHT"],
            "unrestricted_grid_allowed": False,
            "outcome_only": True,
            "censored_state": "CENSORED_FUTURE_COVERAGE",
        },
        "search_space": {
            "markets": ["CL", "ES", "NQ", "RTY", "YM", "GC"],
            "timeframes": ["1m", "5m", "15m", "30m", "60m"],
            "sessions": ["OVERNIGHT", "SESSION_OPEN", "INTRADAY", "CLOSE"],
            "mechanisms": [
                "VOLATILITY_EXPANSION",
                "PARTICIPATION_OPPORTUNITY_DENSITY",
                "MULTI_TIMEFRAME_ALIGNMENT",
                "CROSS_ASSET_STATE",
                "REVERSAL_AFTER_EXHAUSTION",
                "SESSION_SPECIALIZED_MECHANISM",
            ],
            "cached_data_only": True,
            "executable_sleeve_required": True,
            "parameter_variants_independent": False,
            "cemetery_resurrection_allowed": False,
            "proposal_count": 20_000,
            "unique_event_screen_minimum": 4_096,
            "structural_deduplication": True,
            "semantic_deduplication": True,
            "behavioral_deduplication": True,
        },
        "successive_halving": {
            "stage0_proposals_minimum": 20_000,
            "stage1_unique_candidates_minimum": 4_096,
            "stage1_survivor_maximum": 1_024,
            "stage2_exact_sleeve_replay_maximum": 1_024,
            "stage2_survivor_maximum": 256,
            "stage3_rolling_combine_maximum": 256,
            "stage3_survivor_maximum": 64,
            "stage4_account_assembly_maximum": 64,
            "stage4_survivor_maximum": 16,
            "stage5_96_start_maximum": 16,
            "stage5_192_start_maximum": 4,
            "minimum_useful_sleeves_before_account_assembly": 4,
            "mutation_or_retuning_between_stages": False,
            "xfa_before_clean_combine_survivors": False,
        },
        "evaluation_coverage": {
            "headline_horizon_trading_days": 90,
            "headline_requires_complete_horizon": True,
            "role_frozen_before_run": True,
            "coverage_rule_enters_strategy_decision": False,
            "overlapping_starts_role": "SECONDARY_DIAGNOSTIC_ONLY",
            "manufacture_independence_when_under_48": False,
            "reported_states": [
                "FULL_COVERAGE",
                "DATA_CENSORED",
                "HARD_FAILURE",
                "TARGET_REACHED",
                "MLL_BREACHED",
            ],
        },
        "temporal_blocks": {
            "blocks": [
                {"block_id": "B1"},
                {"block_id": "B2"},
                {"block_id": "B3"},
                {"block_id": "B4"},
            ]
        },
        "matched_controls": {
            "types": [
                "RANDOM_EVENT_TIMING",
                "DIRECTION_FLIPPED",
                "SESSION_MATCHED_NULL",
                "CLEAN_LOW_VELOCITY_SLEEVE",
            ],
            "match_dimensions": [
                "MARKET",
                "SESSION",
                "TIMEFRAME",
                "OPPORTUNITY_COUNT",
                "ACTIVE_DURATION",
                "AVERAGE_EXPOSURE",
                "COST_LEVEL",
            ],
            "full_suite_after_cheap_screen_only": True,
        },
        "economic_objective": {
            "ranking": "TRANSPARENT_PARETO_FRONTIER",
            "raw_aggregate_pnl_primary": False,
            "inactivity_rewarded": False,
            "opportunity_density_required": True,
        },
        "promotion_gates": {
            "sleeve_to_account_assembly": {
                "positive_normal_and_stressed_economics": True,
                "higher_target_velocity_than_clean_baseline": True,
                "minimum_positive_blocks": 2,
                "hard_causal_defect_allowed": False,
            },
            "book_48_to_96": {
                "minimum_normal_passes": 3,
                "minimum_stressed_passes": 2,
                "positive_stressed_net_required": True,
                "maximum_mll_breach_rate": 0.10,
                "minimum_contributing_blocks": 2,
            },
            "final_development": {
                "minimum_normal_pass_rate": 0.10,
                "minimum_stressed_pass_rate": 0.05,
                "development_only": True,
                "live_execution_allowed": False,
            },
        },
        "failure_guided_mutation": {
            "classifications": [
                "TOO_FEW_OPPORTUNITIES",
                "LOW_FAVORABLE_BEFORE_ADVERSE_RATE",
                "COST_FRAGILITY",
                "TARGET_TOO_SLOW",
                "MLL_EXCESS",
                "CONSISTENCY_FAILURE",
                "TEMPORAL_INSTABILITY",
                "MARKET_CONCENTRATION",
                "DUPLICATE_BEHAVIOR",
            ],
            "blind_whole_strategy_mutation": False,
        },
        "compute_allocation": {
            "economic_compute_minimum": 0.85,
            "integrity_persistence_maximum": 0.10,
            "reporting_maximum": 0.05,
        },
        "compute": {
            "worker_count": 3,
            "asynchronous_evidence_writer_count": 1,
            "compute_workers_read_only": True,
        },
        "data": {
            "role": "DEVELOPMENT_ONLY_Q4_EXCLUDED",
            "cached_data_only": True,
            "new_purchase_allowed": False,
            "q4_access_allowed": False,
        },
        "costs": {
            "normal_multiplier": 1.0,
            "stressed_multiplier": 1.5,
            "frozen_causal_cost_model": True,
        },
        "account_parameters": {
            "profit_target": 9000.0,
            "maximum_loss_limit": 4500.0,
            "maximum_mini_equivalent": 15,
        },
        "governance": {
            "q4_access_allowed": False,
            "new_data_purchase_allowed": False,
            "broker_connection_allowed": False,
            "orders_allowed": False,
            "live_trading_allowed": False,
            "status_inheritance_allowed": False,
            "former_book_retry_allowed": False,
            "single_authoritative_mission_writer": True,
            "single_persistent_controller": True,
        },
        "evidence_bundle": {
            "destination": "data/cache/evidence_bundles",
            "lightweight_manifest_path": (
                "reports/economic_evolution/causal_target_velocity_0028/"
                "evidence_bundle_receipt.json"
            ),
            "required_datasets": list(REQUIRED_DATASETS),
            "required_for_campaign_complete": True,
            "atomic_finalize": True,
            "summary_only_complete_allowed": False,
            "large_files_git_tracked": False,
            "reconstruction_flag": False,
        },
        "runtime": {
            "engine": CAUSAL_TARGET_VELOCITY_ENGINE,
            "runner": "scripts/run_causal_target_velocity_manifest.py",
            "output_dir": "reports/economic_evolution/causal_target_velocity_0028",
            "result_name": "economic_production_result.json",
            "result_schema": "hydra_economic_production_result_v1",
            "controller_source_change_required": False,
            "worker_count": 3,
            "asynchronous_evidence_writer_count": 1,
            "resume_from_checkpoint": True,
        },
        "multiplicity": {
            "prior_global_N_trials": 100,
            "prospective_comparisons": 20_000,
            "reserved_delta_trials": 20_000,
            "expected_global_N_trials_after_reservation": 20_100,
            "reservation_required_before_outcome_access": True,
            "proof_window_consumed": False,
        },
    }
    manifest["manifest_hash"] = stable_hash(manifest)
    path = tmp_path / "config/v7/causal_target_velocity_0028.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, sort_keys=True, indent=2) + "\n")
    return path, manifest


def test_valid_0028_manifest_is_production_like_and_frozen(tmp_path: Path) -> None:
    path, expected = _fixture(tmp_path)

    actual = load_and_validate_causal_target_velocity_manifest(path)

    assert actual == expected
    assert actual["runtime"]["engine"] == CAUSAL_TARGET_VELOCITY_ENGINE
    assert actual["clean_causal_baseline"]["promotion_status"] is None


def test_0028_manifest_rejects_causal_contract_drift(tmp_path: Path) -> None:
    path, manifest = _fixture(tmp_path)
    manifest["causal_event_contract"]["future_outcomes_are_labels_only"] = False
    manifest.pop("manifest_hash")
    manifest["manifest_hash"] = stable_hash(manifest)
    path.write_text(json.dumps(manifest, sort_keys=True, indent=2) + "\n")

    with pytest.raises(CausalTargetVelocityManifestError, match="causal event"):
        load_and_validate_causal_target_velocity_manifest(path)


def test_0028_worker_dispatch_uses_single_production_like_writer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path, manifest = _fixture(tmp_path)
    config = dict(manifest)
    config["_runtime_preregistration_path"] = str(path)
    runtime = EconomicEvolutionManifestRuntime(tmp_path, tmp_path / "mission/state")
    output, _ = runtime._paths(config)
    calls: list[list[str]] = []

    class _FakeProcess:
        pid = 8828
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

    assert calls[0][calls[0].index("--manifest") + 1] == str(path)
    assert "--preregistration" not in calls[0]
    assert runtime.snapshot()["production_research_worker_count"] == 3
    assert runtime.snapshot()["production_evidence_writer_count"] == 1
    assert runtime.snapshot()["authoritative_mission_writer_count"] == 1
