from __future__ import annotations

import hashlib
import json
import sys
import types
from pathlib import Path
from typing import Any

import pytest

from hydra.economic_evolution.schema import stable_hash
from hydra.evidence import REQUIRED_DATASETS
from hydra.production.fast_pass_manifest import (
    FAST_PASS_CAMPAIGN_MODE,
    FAST_PASS_CLASS_ID,
    FAST_PASS_RUNTIME_VERSION,
    FastPassManifestError,
    validate_fast_pass_manifest,
)
from hydra.production.manifest import (
    ProductionManifestError,
    load_and_validate_production_manifest,
)
from hydra.propfirm.combine_to_xfa import official_rule_snapshot_2026_07_15


ROOT = Path(__file__).resolve().parents[1]


def _write(path: Path, value: str) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _rehash(path: Path, manifest: dict[str, Any]) -> None:
    manifest.pop("manifest_hash", None)
    manifest["manifest_hash"] = stable_hash(manifest)
    path.write_text(json.dumps(manifest, sort_keys=True, indent=2) + "\n")


def _fixture(tmp_path: Path) -> tuple[Path, dict[str, Any]]:
    official_rules = official_rule_snapshot_2026_07_15()
    quality_tiers = [0.0, 0.5, 1.0, 1.5, 2.0]
    governor_profiles = [
        {
            "profile_id": "fast_pass_governor_01",
            "signal_quality_tiers": quality_tiers,
            "open_risk_ceiling_fraction": 0.25,
            "daily_loss_budget_fraction": 0.25,
            "daily_profit_lock_fraction": 0.50,
            "maximum_concurrent_sleeves": 1,
            "target_protection_fraction": 0.80,
            "same_instrument_conflict_policy": "priority",
        },
        {
            "profile_id": "fast_pass_governor_02",
            "signal_quality_tiers": quality_tiers,
            "open_risk_ceiling_fraction": 0.50,
            "daily_loss_budget_fraction": 0.50,
            "daily_profit_lock_fraction": 0.75,
            "maximum_concurrent_sleeves": 2,
            "target_protection_fraction": 0.90,
            "same_instrument_conflict_policy": "priority",
        },
        {
            "profile_id": "fast_pass_governor_03",
            "signal_quality_tiers": quality_tiers,
            "open_risk_ceiling_fraction": 0.75,
            "daily_loss_budget_fraction": 0.25,
            "daily_profit_lock_fraction": 0.50,
            "maximum_concurrent_sleeves": 3,
            "target_protection_fraction": 0.80,
            "same_instrument_conflict_policy": "priority",
        },
        {
            "profile_id": "fast_pass_governor_04",
            "signal_quality_tiers": quality_tiers,
            "open_risk_ceiling_fraction": 0.25,
            "daily_loss_budget_fraction": 0.50,
            "daily_profit_lock_fraction": 0.75,
            "maximum_concurrent_sleeves": 4,
            "target_protection_fraction": 0.90,
            "same_instrument_conflict_policy": "priority",
        },
    ]
    _write(tmp_path / "MISSION_CONTRACT.md", "fixture\n")
    implementation = {
        name: _write(tmp_path / name, f"# {name}\n")
        for name in (
            "hydra/account_policy/active_risk_pool.py",
            "hydra/account_policy/causal_active_pool_replay.py",
            "hydra/evidence/causal_target_velocity_adapter.py",
            "hydra/portfolio/marginal_contribution_builder.py",
            "hydra/production/fast_pass_manifest.py",
            "hydra/production/fast_pass_runtime.py",
            "hydra/production/fast_pass_runtime_helpers.py",
            "hydra/production/manifest.py",
            "hydra/production/runtime.py",
            "hydra/research/causal_sleeve_replay.py",
            "hydra/research/causal_target_velocity.py",
            "scripts/run_economic_production_manifest.py",
        )
    }
    terminal_path = tmp_path / "reports/economic_evolution/0028/result.json"
    terminal_sha = _write(terminal_path, "{}\n")
    starts = {
        "5": [
            {"session_day": 19_500, "temporal_block": "B1"},
            {"session_day": 19_505, "temporal_block": "B2"},
        ],
        "10": [
            {"session_day": 19_500, "temporal_block": "B1"},
            {"session_day": 19_510, "temporal_block": "B2"},
        ],
        "20": [
            {"session_day": 19_500, "temporal_block": "B1"},
            {"session_day": 19_520, "temporal_block": "B2"},
        ],
    }
    manifest: dict[str, Any] = {
        "schema": "hydra_economic_production_manifest_v1",
        "campaign_id": "hydra_fast_pass_factory_0029",
        "campaign_mode": FAST_PASS_CAMPAIGN_MODE,
        "class_id": FAST_PASS_CLASS_ID,
        "policy_classes": [FAST_PASS_CLASS_ID],
        "development_only": True,
        "created_at_utc": "2026-07-17T10:00:00Z",
        "source_commit": "a" * 40,
        "economic_hypothesis": "Causal quality diversity can improve five-day passage.",
        "implementation_files": implementation,
        "terminal_baseline_0028": {
            "status": "CAUSAL_TARGET_VELOCITY_INCONCLUSIVE_COVERAGE_LIMITED",
            "risk_scale_status": "RISK_SCALE_ONLY_FALSIFIED",
            "preserved_sleeve_count": 47,
            "exact_causal_sleeve_replays": 206,
            "stage2_stressed_positive_sleeves": 178,
            "normal_combine_passes_90d": 0,
            "stressed_combine_passes_90d": 0,
            "stage3_median_stressed_target_progress": 0.01,
            "observed_mll_breaches": 0,
            "fully_covered_stage4_starts": 11,
            "active_assembly_degraded_best_sleeves": True,
            "retry_or_retune_allowed": False,
            "reference_bank_status": "LOW_VELOCITY_CAUSAL_REFERENCE_BANK",
            "promotion_status": None,
            "xfa_path_exists": False,
            "forward_package_exists": False,
            "sources": {
                "terminal_result": {
                    "path": terminal_path.relative_to(tmp_path).as_posix(),
                    "file_sha256": terminal_sha,
                }
            },
        },
        "runtime": {
            "engine": "production_kernel_v1",
            "runner": "scripts/run_economic_production_manifest.py",
            "fast_pass_runtime_version": FAST_PASS_RUNTIME_VERSION,
            "output_dir": "reports/economic_evolution/fast_pass_factory_0029",
            "result_name": "economic_production_result.json",
            "result_schema": "hydra_economic_production_result_v1",
            "controller_source_change_required": False,
            "resume_from_checkpoint": True,
            "worker_count": 3,
            "asynchronous_evidence_writer_count": 1,
        },
        "data": {
            "role": "DEVELOPMENT_ONLY_Q4_EXCLUDED",
            "cached_features_only_initial_waves": True,
            "q4_access_allowed": False,
            "new_purchase_allowed_initial_waves": False,
            "feature_source_fingerprint": "b" * 64,
            "contract_map_sha256": "c" * 64,
        },
        "governance": {
            "live_trading_allowed": False,
            "broker_connection_allowed": False,
            "orders_allowed": False,
            "q4_access_allowed": False,
            "new_mission_allowed": False,
            "new_service_allowed": False,
            "new_database_allowed": False,
            "new_registry_writer_allowed": False,
            "controller_version_change_required": False,
            "causality_weakening_allowed": False,
            "promotion_gate_lowering_after_results_allowed": False,
            "xfa_before_combine_graduates_allowed": False,
            "q4_access_count_delta": 0,
        },
        "budget": {
            "actual_spend_usd": 62.8529,
            "hard_cap_usd": 100.0,
            "remaining_usd": 37.1471,
            "protected_reserve_usd": 25.0,
            "new_data_purchase_count": 0,
        },
        "markets": ["CL", "ES", "GC", "NQ", "RTY", "YM"],
        "reference_bank": {
            "status": "LOW_VELOCITY_CAUSAL_REFERENCE_BANK",
            "preserved_sleeve_count": 47,
            "prior_stage1_candidate_count": 4_096,
            "promotion_status": None,
            "baseline_or_parent_role_only": True,
        },
        "temporal_blocks": {
            "blocks": [
                {"block_id": "B1", "start": "2023-07-01", "end": "2023-09-30"},
                {"block_id": "B2", "start": "2023-10-01", "end": "2023-12-31"},
                {"block_id": "B3", "start": "2024-01-01", "end": "2024-03-31"},
                {"block_id": "B4", "start": "2024-04-01", "end": "2024-09-30"},
            ],
            "overlapping_starts_independent": False,
        },
        "account_rule_snapshot": {
            "account_size_usd": 150_000,
            "profit_target_usd": 9_000,
            "maximum_loss_limit_usd": 4_500,
            "best_day_consistency_fraction": 0.5,
            "maximum_mini_contracts": 15,
            "maximum_micro_contracts": 150,
            "derive_risk_limits_from_snapshot": True,
            "hardcode_alternate_account_in_strategy": False,
            "rule_snapshot_version": official_rules.rule_version,
            "rule_snapshot_hash": official_rules.fingerprint,
            "official_snapshot": official_rules.to_dict(),
            "session_close_rule": "FLAT_AT_SESSION_CLOSE",
            "optional_dll": {"enabled": False},
            "costs_and_slippage": {"normal": 1.0, "stressed": 1.5},
        },
        "account_speed_objective": {
            "primary_trading_days": 5,
            "target_profit_per_day_usd": 1_800,
            "target_to_mll_ratio": 2.0,
            "five_days_is_optimization_not_promotion_fabrication": True,
        },
        "evaluation_grid": {
            "primary_headline_days": [5],
            "secondary_headline_days": [10, 20],
            "diagnostic_rolling_days": [5, 10, 20],
            "late_stage_survival_days": [40, 60, 90],
            "headline_non_overlapping": True,
            "headline_full_coverage_required": True,
            "starts_and_roles_frozen_before_outcomes": True,
            "overlapping_starts_independent": False,
            "states": [
                "FULL_COVERAGE",
                "TARGET_REACHED",
                "MLL_BREACHED",
                "HARD_RULE_FAILURE",
                "DATA_CENSORED",
            ],
            "headline_starts": starts,
            "block_roles": {
                "B1": "DESIGN",
                "B2": "DESIGN",
                "B3": "HELD_OUT_DEVELOPMENT",
                "B4": "HELD_OUT_DEVELOPMENT",
            },
            "grid_hash": "e" * 64,
        },
        "bank_architecture": {
            "causal_executable_capacity": 150,
            "fast_5d_capacity": 50,
            "balanced_10d_capacity": 50,
            "robust_20d_capacity": 50,
            "graduated_minimum_target": 10,
            "graduated_maximum_target": 20,
            "forward_minimum_target": 3,
            "forward_maximum_target": 5,
        },
        "quality_diversity": {
            "cell_dimensions": [
                "MARKET",
                "SESSION",
                "TIMEFRAME",
                "HOLDING_HORIZON",
                "MECHANISM_FAMILY",
                "DIRECTION_PROFILE",
                "OPPORTUNITY_DENSITY",
                "RISK_PROFILE",
                "TRADE_FREQUENCY",
                "MLL_CONSUMPTION_PROFILE",
            ],
            "persist_pairwise_overlap": ["SIGNAL", "TRADE", "DAILY_PNL", "LOSS_DAY"],
            "maximum_close_parameter_variants_per_cell": 3,
            "exact_trade_path_clone_allowed": False,
            "execution_equivalent_clone_allowed": False,
            "novel_capacity_fraction_minimum": 0.20,
            "weak_archive_replacement_fraction_minimum": 0.10,
            "weak_archive_replacement_fraction_maximum": 0.20,
        },
        "research_lanes": {
            "enabled": ["SYMBOLIC_RULE", "HAZARD_MODEL", "EVENT_TIME", "CROSS_ASSET"],
            "hazard_model": {
                "allowed_models": [
                    "REGULARIZED_LOGISTIC_REGRESSION",
                    "SHALLOW_DECISION_TREE",
                    "MONOTONIC_GRADIENT_BOOSTED_TREE",
                    "DISCRETE_COMPETING_RISK",
                ],
                "strict_cross_fit_required": True,
                "future_labels_in_inference_features": False,
                "availability_rule": "AVAILABLE_AT_LTE_DECISION_TIME",
                "executable_export_required": True,
                "deterministic_hash_required": True,
            },
            "event_time": {
                "representations": [
                    "VOLUME_BARS",
                    "DOLLAR_BARS",
                    "IMBALANCE_BARS",
                    "VOLATILITY_EVENT_BARS",
                    "SESSION_EVENT_BARS",
                ]
            },
        },
        "risk_governor": {
            "signal_quality_tiers": quality_tiers,
            "maximum_concurrent_sleeves": [1, 2, 3, 4],
            "open_risk_ceiling_mll_buffer_fractions": [0.25, 0.5, 0.75],
            "daily_loss_budget_mll_fractions": [0.25, 0.5],
            "daily_profit_lock_consistency_fractions": [0.5, 0.75],
            "inactive_sleeves_reserve_risk": False,
            "contract_limits_are_hard": True,
            "continuous_optimization_allowed": False,
            "future_episode_outcome_adaptation_allowed": False,
            "freeze_before_held_out_evaluation": True,
            "frozen_profiles": governor_profiles,
            "frozen_profiles_hash": stable_hash(governor_profiles),
            "executable_quality_quantization": (
                "CAUSAL_EVENT_IDENTITY_HASH_INTEGER_MICRO_QUANTIZATION_V1"
            ),
        },
        "marginal_contribution_book_builder": {
            "minimum_exact_books_when_sleeves_sufficient": 1_000,
            "maximum_sleeves_per_book": 6,
            "start_with_one_high_velocity_sleeve": True,
            "held_out_development_evidence_required": True,
            "addition_improvement_metrics": [
                "FIVE_DAY_PASS_RATE",
                "TEN_DAY_PASS_RATE",
                "LOWER_QUARTILE_TARGET_PROGRESS",
                "MLL_SURVIVAL",
                "CONSISTENCY",
            ],
            "material_degradation_allowed": False,
            "remove_negative_marginal_contributors": True,
            "baselines": [
                "BEST_COMPONENT_SLEEVE",
                "PRECEDING_SMALLER_BOOK",
                "EQUAL_RISK_POOLING",
                "EXPOSURE_MATCHED_RANDOM_ASSEMBLY",
            ],
            "larger_book_automatic_advantage": False,
            "search_method": "PARETO_BEAM",
        },
        "waves": {
            "proposals_per_wave": 50_000,
            "unique_event_screens_per_wave": 10_000,
            "stage2_exact_sleeve_replay_maximum": 2_500,
            "stage3_causal_executable_bank_maximum": 150,
            "stage4_exact_books_minimum_when_sleeves_sufficient": 1_000,
            "stage5_fast_5d_book_maximum": 150,
            "stage5_balanced_10d_book_maximum": 100,
            "stage5_robust_20d_book_maximum": 50,
            "stage6_graduated_book_minimum_target": 10,
            "stage6_graduated_book_maximum_target": 20,
            "stage6_forward_package_minimum_target": 3,
            "stage6_forward_package_maximum_target": 5,
            "complete_ohlcv_waves_before_representation_decision": 2,
            "continuous_waves": True,
            "failure_guided_generation": True,
            "reuse_qualified_candidates_across_waves": True,
            "restart_bank_from_zero": False,
            "xfa_before_combine_graduates": False,
        },
        "progressive_controls": {
            "LEVEL_0": ["DUPLICATE_CHECK", "CAUSAL_CHECK"],
            "LEVEL_1": ["DIRECTION_FLIP", "RANDOM_TIMING", "SESSION_MATCHED_NULL"],
            "LEVEL_2": [
                "EXPOSURE_MATCHED_RANDOM",
                "BEST_PARENT",
                "MARGINAL_CONTRIBUTION",
            ],
            "LEVEL_3": [
                "TEMPORAL_CROSSFIT",
                "COMPLETE_MATCHED_CONTROLS",
                "BLOCK_CONCENTRATION",
                "EVIDENCE_BUNDLE_RECONCILIATION",
            ],
            "level1_top_fraction": 0.10,
            "universal_full_controls_allowed": False,
            "worm_receipts_for_weak_candidates": False,
            "one_batch_archive_per_wave": True,
            "exposure_match_tolerances": {
                "mean_daily_contract_utilization_absolute": 0.05,
                "maximum_mini_equivalent_mean_relative": 0.25,
                "accepted_event_count_relative": 0.25,
            },
        },
        "promotion_gates": {
            "causal_executable_bank": {
                "complete_causal_execution_required": True,
                "positive_stressed_development_blocks_minimum": 2,
                "defensive_role_exception_requires_demonstration": True,
                "behavioral_uniqueness_required": True,
            },
            "fast_5d_bank": {
                "full_coverage_normal_passes_minimum": 1,
                "positive_stressed_five_day_economics_required": True,
                "single_trade_domination_allowed": False,
            },
            "graduated_book": {
                "normal_5d_pass_rate_minimum": 0.05,
                "stressed_5d_pass_rate_minimum": 0.02,
                "normal_10d_pass_rate_minimum": 0.10,
                "stressed_10d_pass_rate_minimum": 0.05,
                "mll_breach_rate_maximum": 0.10,
                "independent_blocks_with_passes_minimum": 2,
                "positive_stressed_economics_required": True,
                "consistency_compliance_required": True,
                "single_sleeve_or_day_domination_allowed": False,
            },
            "strong_sprint_book": {
                "normal_5d_pass_rate_minimum": 0.10,
                "stressed_5d_pass_rate_minimum": 0.05,
                "positive_lower_quartile_target_progress_required": True,
            },
            "lower_after_observing_results": False,
            "development_only": True,
        },
        "microstructure_escalation": {
            "complete_ohlcv_waves_before_decision": 2,
            "trigger_fast_5d_bank_below": 20,
            "trigger_when_no_stressed_five_day_pass": True,
            "exact_cost_estimate_required_before_purchase": True,
            "minimum_budget_reserve_usd": 25.0,
            "maximum_initial_spend_usd": 10.0,
            "broad_purchase_allowed": False,
            "q4_access_allowed": False,
            "candidate_schemas": ["TRADES", "MBP_1", "TOP_OF_BOOK", "AGGRESSOR_SIDE_FLOW"],
        },
        "compute_allocation": {
            "economic_wall_clock_minimum": 0.90,
            "integrity_persistence_maximum": 0.07,
            "reporting_maximum": 0.03,
            "aggregate_cpu_target_minimum": 0.80,
            "cores_reserved_for_os_and_writer": 1,
            "full_regression_per_wave": False,
            "idle_wait_for_user_input": False,
        },
        "evidence_bundle": {
            "required_for_campaign_complete": True,
            "atomic_finalize": True,
            "summary_only_complete_allowed": False,
            "required_datasets": list(REQUIRED_DATASETS),
            "large_files_git_tracked": False,
            "reconstruction_flag": False,
            "one_batch_archive_per_wave": True,
            "destination": "data/cache/evidence_bundles",
            "lightweight_manifest_path": (
                "reports/economic_evolution/fast_pass_factory_0029/"
                "evidence_bundle_receipt.json"
            ),
        },
        "multiplicity": {
            "prior_global_N_trials": 612_109,
            "prospective_comparisons": 100_000,
            "campaign_specific_inflation": 1.5,
            "reserved_delta_trials": 150_000,
            "expected_global_N_trials_after_reservation": 762_109,
            "reservation_required_before_outcome_access": True,
            "proof_window_consumed": False,
        },
    }
    path = tmp_path / "config/v7/fast_pass_factory_0029.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    _rehash(path, manifest)
    return path, manifest


def test_fast_pass_manifest_is_accepted_by_stable_production_schema(
    tmp_path: Path,
) -> None:
    path, expected = _fixture(tmp_path)

    assert load_and_validate_production_manifest(path) == expected
    validate_fast_pass_manifest(expected, manifest_path=path)


def test_terminal_validation_recovery_revision_is_valid_and_finalization_only() -> None:
    path = ROOT / "config/v7/fast_pass_factory_0029_revision_05.json"

    manifest = load_and_validate_production_manifest(path)

    assert manifest["revision_id"] == "hydra_fast_pass_factory_0029_revision_05"
    assert manifest["technical_repair"]["resume_scope"] == (
        "REUSE_COMPLETE_STAGING_FINALIZATION_ONLY"
    )
    assert manifest["technical_repair"]["terminal_economic_evidence_recomputed"] is False


@pytest.mark.parametrize(
    ("section", "field", "value", "message"),
    (
        ("account_rule_snapshot", "profit_target_usd", 8_999, "account snapshot"),
        ("evaluation_grid", "primary_headline_days", [10], "evaluation-window"),
        ("waves", "proposals_per_wave", 49_999, "wave scale"),
        ("governance", "orders_allowed", True, "unsafe fast-pass authority"),
        ("promotion_gates", "lower_after_observing_results", True, "promotion gates"),
    ),
)
def test_fast_pass_manifest_rejects_scientific_or_safety_drift(
    tmp_path: Path,
    section: str,
    field: str,
    value: object,
    message: str,
) -> None:
    path, manifest = _fixture(tmp_path)
    manifest[section][field] = value
    _rehash(path, manifest)

    with pytest.raises(ProductionManifestError, match=message):
        load_and_validate_production_manifest(path)


def test_fast_pass_manifest_rejects_path_escape_and_multiplicity_drift(
    tmp_path: Path,
) -> None:
    path, manifest = _fixture(tmp_path)
    manifest["implementation_files"]["../outside.py"] = "f" * 64
    _rehash(path, manifest)
    with pytest.raises(ProductionManifestError, match="path escapes root"):
        load_and_validate_production_manifest(path)

    path, manifest = _fixture(tmp_path)
    manifest["multiplicity"]["expected_global_N_trials_after_reservation"] += 1
    _rehash(path, manifest)
    with pytest.raises(ProductionManifestError, match="multiplicity arithmetic"):
        load_and_validate_production_manifest(path)


def test_stable_runner_dispatches_fast_pass_mode(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path, _manifest = _fixture(tmp_path)
    calls: list[tuple[object, ...]] = []

    def _run(*args: object, **kwargs: object) -> dict[str, object]:
        calls.append((args, kwargs))
        return {"status": "FAST_PASS_DISPATCHED"}

    module = types.ModuleType("hydra.production.fast_pass_runtime")
    module.run_fast_pass_manifest = _run  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, module.__name__, module)
    from hydra.production.runtime import run_production_manifest

    result = run_production_manifest(
        path,
        contract_map_path=tmp_path / "contract.json",
        cache_root=tmp_path / "cache",
    )

    assert result == {"status": "FAST_PASS_DISPATCHED"}
    assert calls and calls[0][0][0] == path
