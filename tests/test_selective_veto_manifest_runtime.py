from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from hydra.economic_evolution.schema import stable_hash
from hydra.evidence import REQUIRED_COMPACT_OUTPUTS, REQUIRED_DATASETS
from hydra.production.manifest import load_and_validate_production_manifest
from hydra.production.selective_veto_manifest import (
    ACCOUNT_RULE_SNAPSHOTS,
    CAMPAIGN_ID,
    CAMPAIGN_MODE,
    CLASS_ID,
    LONG_SAMPLE_DECISIONS,
    MANIFEST_SCHEMA,
    MODEL_CLASSES,
    PRIMARY_ACTIONS,
    PRIMARY_SEED_ID,
    RUNTIME_VERSION,
    SECONDARY_SEED_ID,
    SEED_DECISIONS,
    SEED_STATUS,
    SelectiveVetoManifestError,
    validate_selective_veto_manifest,
)
from hydra.production.selective_veto_runtime import (
    SelectiveVetoRuntimeError,
    _next_recommendation,
    _validate_campaign_result,
    _write_state,
)


def _write(path: Path, content: bytes) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return hashlib.sha256(content).hexdigest()


def _rehash(manifest: dict[str, Any]) -> None:
    manifest.pop("manifest_hash", None)
    manifest["manifest_hash"] = stable_hash(manifest)


def _fixture(tmp_path: Path) -> tuple[Path, dict[str, Any]]:
    path = tmp_path / "config/v7/selective_order_flow_veto_expansion_0034.json"
    implementation_files = {
        name: _write(tmp_path / name, name.encode())
        for name in (
            "hydra/production/selective_veto_manifest.py",
            "hydra/production/selective_veto_runtime.py",
            "hydra/production/selective_veto_seed_audit.py",
            "hydra/production/selective_veto_pilot.py",
            "hydra/production/selective_veto_metadata.py",
            "hydra/production/manifest.py",
            "hydra/production/runtime.py",
            "scripts/run_economic_production_manifest.py",
        )
    }
    sources: dict[str, dict[str, str]] = {}
    for name in (
        "authoritative_result",
        "evidence_bundle_receipt",
        "evidence_bundle_manifest",
        "decision_report",
        "pilot_summary",
        "selective_veto_terminal_receipt",
        "selective_veto_boundary_verdict",
    ):
        source_path = tmp_path / f"sources/0033/{name}.json"
        sources[name] = {
            "path": source_path.relative_to(tmp_path).as_posix(),
            "sha256": _write(source_path, name.encode()),
        }
    seeds = [
        {
            "policy_id": PRIMARY_SEED_ID,
            "policy_fingerprint": "1" * 64,
            "status": SEED_STATUS,
            "immutable": True,
            "mutation_allowed": False,
            "status_inheritance_allowed": False,
            "promotion_status": None,
            "validation_normal_net_usd": 793.82,
            "validation_stressed_net_usd": 776.32,
            "final_development_normal_net_usd": 1324.78,
            "final_development_stressed_net_usd": 1314.78,
            "validation_paired_stressed_uplift_usd": 1120.58,
            "final_development_paired_stressed_uplift_usd": 499.28,
            "validation_abstention_rate": 0.5556,
            "final_development_abstention_rate": 0.2083,
            "minimum_mll_buffer_usd": 4141.30,
            "deployability_tier": "L1_DEPLOYABLE",
        },
        {
            "policy_id": SECONDARY_SEED_ID,
            "policy_fingerprint": "2" * 64,
            "status": SEED_STATUS,
            "immutable": True,
            "mutation_allowed": False,
            "status_inheritance_allowed": False,
            "promotion_status": None,
            "validation_normal_net_usd": 972.70,
            "validation_stressed_net_usd": 930.70,
            "final_development_normal_net_usd": 1077.46,
            "final_development_stressed_net_usd": 1046.96,
            "validation_paired_stressed_uplift_usd": 1274.96,
            "final_development_paired_stressed_uplift_usd": 231.46,
            "validation_abstention_rate": 0.5556,
            "final_development_abstention_rate": 0.2083,
            "minimum_mll_buffer_usd": 3922.08,
            "deployability_tier": "L1_DEPLOYABLE_OR_L2_DEPLOYABLE",
        },
    ]
    manifest: dict[str, Any] = {
        "schema": MANIFEST_SCHEMA,
        "campaign_mode": CAMPAIGN_MODE,
        "campaign_id": CAMPAIGN_ID,
        "class_id": CLASS_ID,
        "policy_classes": [CLASS_ID],
        "development_only": True,
        "created_at_utc": "2026-07-18T18:00:00Z",
        "source_commit": "a" * 40,
        "economic_hypothesis": "Order flow can veto weak causal structural trades.",
        "implementation_files": implementation_files,
        "terminal_source_0033": {
            "campaign_id": "hydra_hybrid_structural_alpha_order_flow_0033",
            "terminal_status": "HYBRID_OVERLAY_WEAK",
            "reuse_mode": "IMMUTABLE_READ_ONLY",
            "broad_refinement_resume_allowed": False,
            "timing_alpha_established": False,
            "execution_alpha_established": False,
            "standalone_microstructure_alpha_falsified": True,
            "status_inheritance_allowed": False,
            "outcomes_are_development_only": True,
            "source_hashes": sources,
        },
        "frozen_seed_policies": seeds,
        "runtime": {
            "engine": "production_kernel_v1",
            "runner": "scripts/run_economic_production_manifest.py",
            "selective_veto_runtime_version": RUNTIME_VERSION,
            "output_dir": "reports/economic_evolution/selective_order_flow_veto_expansion_0034",
            "result_schema": "hydra_economic_production_result_v1",
            "result_name": "economic_production_result.json",
            "controller_source_change_required": False,
            "resume_from_checkpoint": True,
            "orchestrator_count": 1,
            "worker_count": 2,
            "asynchronous_evidence_writer_count": 1,
        },
        "compute_contract": {
            "vps_cpu_core_count": 3,
            "orchestrator_count": 1,
            "cpu_worker_count": 2,
            "authoritative_writer_count": 1,
            "cpu_workers_read_only": True,
            "single_writer_atomic_commits": True,
            "oversubscription_allowed": False,
            "target_cpu_utilization_min": 0.80,
            "target_cpu_utilization_max": 0.95,
            "economic_wall_clock_minimum": 0.90,
            "thread_limits": {
                "OMP_NUM_THREADS": "1",
                "MKL_NUM_THREADS": "1",
                "OPENBLAS_NUM_THREADS": "1",
                "NUMEXPR_NUM_THREADS": "1",
            },
        },
        "primary_action_contract": {
            "actions": list(PRIMARY_ACTIONS),
            "risk_levels": [0.0, 1.0, 1.5],
            "structural_direction_immutable": True,
            "structural_entry_exit_stop_target_immutable": True,
            "a2_timing_allowed": False,
            "a3_execution_variant_allowed": False,
            "a4_passive_allowed": False,
            "a5_early_invalidation_allowed": False,
            "direction_reversal_allowed": False,
            "new_structural_direction_allowed": False,
        },
        "seed_robustness_audit": {
            "attribution_dimensions": [
                "market",
                "structural_anchor_family",
                "session",
                "individual_opportunity",
                "abstention",
                "risk_1_5x",
                "all_in_costs",
            ],
            "leave_one_opportunity_out": True,
            "top_trade_removal_counts": [1, 2, 3],
            "leave_one_anchor_family_out": True,
            "cost_stress_multipliers": [1.0, 1.25],
            "feature_dependency_tiers": [
                "TRADES_ONLY",
                "TBBO",
                "MBP_1",
                "MBO_TEACHER_ONLY",
            ],
            "account_sizes_usd": [50_000, 100_000, 150_000],
            "account_horizons_days": [5, 10],
            "allowed_decisions": list(SEED_DECISIONS),
            "no_purchase_before_decision": True,
            "best_trade_removal_positive_stressed_required": True,
            "minimum_distinct_context_count": 2,
            "maximum_single_opportunity_profit_share": 0.25,
            "hard_data_or_deployability_defect_allowed": False,
            "thresholds_may_change_after_results": False,
        },
        "structural_anchor_universe": {
            "source": "EXISTING_CACHED_CAUSAL_OHLCV_AND_STRUCTURAL_FEATURES",
            "families": ["OPENING_RANGE", "FAILED_BREAKOUT"],
            "microstructure_outcomes_used_for_generation": False,
            "causal_generation_required": True,
            "behavioral_deduplication_required": True,
            "temporal_deduplication_required": True,
            "neighboring_bar_duplicates_allowed": False,
            "anchor_fields_complete": True,
        },
        "anchor_conditioned_windows": {
            "pre_decision_lookback_seconds": 120,
            "post_decision_safety_seconds": 60,
            "deterministic_warmup_included": True,
            "overlapping_windows_merged": True,
            "full_session_request_default": False,
            "holding_period_microstructure_required": False,
            "cached_market_data_used_for_post_entry_outcomes": True,
        },
        "targeted_cost_policy": {
            "schemas": ["trades", "tbbo", "mbp-1"],
            "window_counts": [100, 250, 500, 1000],
            "official_databento_cost_estimate_required": True,
            "one_and_two_market_estimates_required": True,
            "chronological_role_costs_required": True,
            "no_new_mbo_purchase": True,
            "purchase_before_seed_gate_allowed": False,
            "silent_purchase_allowed": False,
            "manifest_bound_purchase_counter_required": True,
            "unmanifested_purchase_count_must_be_zero": True,
            "ledger_before_after_hash_required": True,
            "current_remaining_budget_usd": 28.498462508622012,
            "minimum_budget_reserve_usd": 20.0,
            "maximum_incremental_spend_usd": 8.0,
        },
        "chronological_roles": {
            "discovery_fraction": 0.60,
            "validation_fraction": 0.20,
            "final_development_fraction": 0.20,
            "random_temporal_mixing_allowed": False,
            "roles_frozen_before_download": True,
            "final_development_includes_all_eligible_anchors": True,
            "final_development_outcomes_visible_before_policy_freeze": False,
            "final_development_is_independent_confirmation": False,
        },
        "selective_policy_distillation": {
            "actions": list(PRIMARY_ACTIONS),
            "model_classes": list(MODEL_CLASSES),
            "maximum_production_features": 8,
            "maximum_thresholds": 3,
            "direction_generation_allowed": False,
            "deterministic_versioned_output": True,
            "objective": "LOWER_CONFIDENCE_BOUND_OF_PAIRED_STRESSED_UPLIFT",
            "minimum_trade_coverage": 0.20,
            "maximum_abstention": 0.80,
            "exact_mll_required": True,
            "consistency_required": True,
            "single_opportunity_domination_allowed": False,
            "raw_in_sample_net_is_primary": False,
        },
        "paired_long_sample_evaluation": {
            "baseline_action": "BASELINE_IMMEDIATE_CAUSAL_STRUCTURAL_TRADE",
            "selective_actions": list(PRIMARY_ACTIONS),
            "identical_structural_direction_stop_target_exit": True,
            "identical_causal_fill_and_account_rules": True,
            "paired_metrics": [
                "normal_net",
                "stressed_net",
                "baseline_normal_net",
                "baseline_stressed_net",
                "paired_normal_uplift",
                "paired_stressed_uplift",
                "entry_cost",
                "mae",
                "mfe",
                "stop_rate",
                "target_rate",
                "holding_duration",
                "target_contribution",
                "mll_contribution",
            ],
            "report_market_family_session_block": True,
            "unpaired_opportunity_sets_primary": False,
            "unique_structural_opportunity_required": True,
            "aggregate_to_event_reconciliation_required": True,
        },
        "sequential_evidence_policy": {
            "additional_session_checkpoints": [5, 10, 15],
            "maximum_available_within_budget_checkpoint": True,
            "allowed_decisions": [
                "SUCCESS_EVIDENCE_SUFFICIENT",
                "CONTINUE_ACQUISITION",
                "FUTILITY_STOP",
            ],
            "evidence_available_up_to_checkpoint_only": True,
            "continuous_retuning_allowed": False,
            "success_requires_frozen_gate": True,
            "futility_requires_negative_seed_and_policy_uplift": True,
        },
        "diagnostic_forward": {
            "status": "SELECTIVE_VETO_DIAGNOSTIC_FORWARD",
            "policy_ids": [PRIMARY_SEED_ID, SECONDARY_SEED_ID],
            "activation_requires_authorized_research_feed": True,
            "append_only": True,
            "zero_order": True,
            "parameter_changes_allowed": False,
            "economic_promotion_allowed": False,
            "paper_shadow_ready_claim_allowed": False,
            "broker_connection_allowed": False,
            "orders_allowed": False,
            "activation_after_manifest_freeze_required": True,
            "feed_authorization_receipt_required": True,
            "raw_event_fingerprints_required": True,
            "seed_fingerprint_match_required": True,
            "falsified_seed_activation_allowed": False,
        },
        "account_rule_snapshots": {
            label: dict(snapshot)
            for label, snapshot in ACCOUNT_RULE_SNAPSHOTS.items()
        },
        "account_speed_gate": {
            "account_sizes_usd": [50_000, 100_000, 150_000],
            "horizons_days": [5, 10],
            "allowed_decisions": list(LONG_SAMPLE_DECISIONS),
            "positive_stressed_validation_required": True,
            "positive_stressed_final_development_required": True,
            "positive_paired_uplift_validation_required": True,
            "positive_paired_uplift_final_development_required": True,
            "minimum_distinct_family_or_context_count": 2,
            "single_trade_domination_allowed": False,
            "maximum_single_trade_positive_profit_fraction": 0.25,
            "mll_within_frozen_tolerance_required": True,
            "maximum_mll_breach_rate": 0.10,
            "consistency_required": True,
            "complete_stressed_p5_or_p10_pass_or_material_progress_required": True,
            "minimum_material_stressed_target_progress_uplift": 0.05,
            "material_progress_required_in_validation_and_final_development": True,
            "normal_and_stressed_scenarios_required": True,
            "full_coverage_denominators_required": True,
            "account_rule_snapshot_hash_required": True,
            "select_fastest_viable_account_size": True,
            "thresholds_may_change_after_results": False,
            "development_only": True,
        },
        "multiplicity": {
            "prior_global_N_trials": 100,
            "prospective_comparisons": 10,
            "reserved_delta_trials": 10,
            "expected_global_N_trials_after_reservation": 110,
            "campaign_specific_inflation": 1.5,
            "reservation_required_before_outcome_access": True,
            "proof_window_consumed": False,
        },
        "evidence_bundle": {
            "required": True,
            "atomic_single_writer_finalization": True,
            "summary_only_complete_allowed": False,
            "evidence_status": "FRESH_DEVELOPMENT_EVIDENCE",
            "reconstruction_flag": False,
            "destination": "data/cache/evidence_bundles",
            "required_datasets": list(REQUIRED_DATASETS),
            "exact_account_replay_required": True,
            "sentinel_economic_records_allowed": False,
            "paired_evidence_reconciliation_required": True,
            "normal_stressed_episode_pairing_required": True,
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
            "status_inheritance_allowed": False,
            "threshold_lowering_after_results_allowed": False,
            "broad_research_framework_allowed": False,
            "xfa_work_allowed": False,
            "new_data_purchase_allowed": True,
            "purchase_only_after_seed_gate": True,
            "maximum_incremental_spend_usd": 8.0,
            "minimum_budget_reserve_usd": 20.0,
        },
    }
    _rehash(manifest)
    return path, manifest


def test_valid_0034_manifest_and_generic_dispatch(tmp_path: Path) -> None:
    path, manifest = _fixture(tmp_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest), encoding="utf-8")

    validate_selective_veto_manifest(manifest, manifest_path=path)
    loaded = load_and_validate_production_manifest(path)

    assert loaded["campaign_mode"] == CAMPAIGN_MODE
    assert tuple(loaded["primary_action_contract"]["actions"]) == PRIMARY_ACTIONS


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        (("primary_action_contract", "a2_timing_allowed"), True, "action lattice"),
        (("targeted_cost_policy", "maximum_incremental_spend_usd"), 8.01, "window/cost"),
        (("targeted_cost_policy", "minimum_budget_reserve_usd"), 19.99, "window/cost"),
        (("diagnostic_forward", "orders_allowed"), True, "diagnostic-forward"),
        (("compute_contract", "cpu_worker_count"), 3, "topology"),
    ],
)
def test_0034_manifest_rejects_frozen_contract_drift(
    tmp_path: Path, field: tuple[str, str], value: Any, message: str
) -> None:
    path, manifest = _fixture(tmp_path)
    manifest[field[0]][field[1]] = value
    _rehash(manifest)

    with pytest.raises(SelectiveVetoManifestError, match=message):
        validate_selective_veto_manifest(manifest, manifest_path=path)


def test_0034_manifest_rejects_seed_evidence_mutation(tmp_path: Path) -> None:
    path, manifest = _fixture(tmp_path)
    manifest["frozen_seed_policies"][0]["validation_stressed_net_usd"] += 0.01
    _rehash(manifest)

    with pytest.raises(SelectiveVetoManifestError, match="reported evidence drift"):
        validate_selective_veto_manifest(manifest, manifest_path=path)


def _minimal_evidence_datasets() -> dict[str, list[dict[str, Any]]]:
    campaign_id = CAMPAIGN_ID
    component_id = "component_0034_fixture"
    policy_id = "policy_0034_fixture"
    entry_time = "2026-07-01T14:31:00Z"
    exit_time = "2026-07-01T14:32:00Z"
    datasets: dict[str, list[dict[str, Any]]] = {
        "component_signals": [
            {
                "campaign_id": campaign_id,
                "component_id": component_id,
                "signal_id": "signal_fixture",
                "event_time": entry_time,
                "market": "NQ",
                "contract": "NQU6",
                "timeframe": "1m",
                "signal": "BUY",
                "sizing": 1.0,
                "stop": 99.0,
                "target": 102.0,
                "veto": False,
                "component_role": "SELECTIVE_VETO",
            }
        ],
        "component_entries": [
            {
                "campaign_id": campaign_id,
                "component_id": component_id,
                "trade_id": "trade_fixture",
                "entry_time": entry_time,
                "market": "NQ",
                "contract": "NQU6",
                "side": "BUY",
                "quantity": 1,
                "entry_price": 100.0,
                "sizing": 1.0,
                "stop_price": 99.0,
                "target_price": 102.0,
            }
        ],
        "component_exits": [
            {
                "campaign_id": campaign_id,
                "component_id": component_id,
                "trade_id": "trade_fixture",
                "exit_time": exit_time,
                "exit_price": 101.0,
                "exit_reason": "TIME_STOP",
            }
        ],
        "component_trades": [
            {
                "campaign_id": campaign_id,
                "component_id": component_id,
                "trade_id": "trade_fixture",
                "entry_time": entry_time,
                "exit_time": exit_time,
                "market": "NQ",
                "contract": "NQU6",
                "side": "BUY",
                "quantity": 1,
                "entry_price": 100.0,
                "exit_price": 101.0,
                "gross_pnl": 20.0,
                "costs": 2.0,
                "net_pnl": 18.0,
            }
        ],
        "account_policy_membership": [
            {
                "campaign_id": campaign_id,
                "policy_id": policy_id,
                "component_id": component_id,
                "risk_allocation": 1.0,
                "component_role": "SELECTIVE_VETO",
            }
        ],
        "account_daily_paths": [],
        "episodes": [],
        "provenance": [
            {
                "campaign_id": campaign_id,
                "validator_version": "fixture-v1",
                "replay_version": "fixture-v1",
                "market_data_role": "DEVELOPMENT_FIXTURE",
                "access_ledger_sha256": "a" * 64,
                "reconstruction_flag": False,
                "immutable_checksums": {"fixture": "b" * 64},
                "recorded_at_utc": "2026-07-18T18:00:00Z",
            }
        ],
    }
    for scenario, costs, net in (("NORMAL", 2.0, 18.0), ("STRESSED_1_5X", 3.0, 17.0)):
        episode_id = "episode_fixture"
        datasets["account_daily_paths"].append(
            {
                "campaign_id": campaign_id,
                "policy_id": policy_id,
                "episode_id": episode_id,
                "trading_day": "2026-07-01",
                "cost_scenario": scenario,
                "horizon": "P5",
                "realized_pnl": net,
                "unrealized_pnl": 0.0,
                "daily_pnl": net,
                "equity": net,
                "mll": -4500.0,
                "mll_buffer": 4500.0 + net,
                "minimum_mll_buffer": 4500.0,
                "consistency": 1.0,
                "target_progress": net / 9000.0,
                "costs": costs,
                "conflicts": [],
                "consistency_ok": True,
                "exposure": {"NQ": 0.0},
                "component_attribution": {component_id: net},
            }
        )
        datasets["episodes"].append(
            {
                "campaign_id": campaign_id,
                "policy_id": policy_id,
                "episode_id": episode_id,
                "episode_start": entry_time,
                "horizon": "P5",
                "temporal_block": "B1",
                "duration_trading_days": 1,
                "target_reached": False,
                "mll_breached": False,
                "censored_state": True,
                "cost_scenario": scenario,
                "costs": costs,
                "net_pnl": net,
                "target_progress": net / 9000.0,
                "minimum_mll_buffer": 4500.0,
                "consistency_ok": True,
                "days_to_target": None,
                "failure_vector": {"DATA_CENSORED": 1},
                "terminal_state": "DATA_CENSORED",
            }
        )
    return datasets


def _campaign(manifest: dict[str, Any], *, seed_decision: str) -> dict[str, Any]:
    audit_row = {
        "leave_one_opportunity_out": {},
        "top_trade_removal": {},
        "leave_one_anchor_family_out": {},
        "cost_stress": {},
        "feature_dependencies": {},
        "account_size_matrix": {},
        "market_attribution": {},
        "anchor_family_attribution": {},
    }
    rows = []
    for schema in ("trades", "tbbo", "mbp-1"):
        for count in (100, 250, 500, 1000):
            for market_count in (1, 2):
                rows.append(
                    {
                        "schema": schema,
                        "anchor_window_count": count,
                        "merged_window_duration_seconds": count * 180,
                        "estimated_records": count,
                        "estimated_bytes": count * 100,
                        "estimated_cost_usd": 1.0,
                        "feature_coverage": "L1",
                        "market_count": market_count,
                        "estimate_fingerprint": f"{schema}-{count}-{market_count}",
                    }
                )
    falsified = seed_decision == "SELECTIVE_VETO_SEED_FALSIFIED"
    return {
        "seed_audit": {
            "decision": seed_decision,
            "completed_before_cost_estimation": True,
            "completed_before_purchase": True,
            "actual_spend_usd": 0.0,
            "policies": {
                PRIMARY_SEED_ID: dict(audit_row),
                SECONDARY_SEED_ID: dict(audit_row),
            },
        },
        "anchor_universe": {
            "anchors_generated": 0 if falsified else 1000,
            "merged_windows_estimated": 0 if falsified else 750,
        },
        "window_cost_matrix": {
            "status": "NOT_RUN_SEED_FALSIFIED" if falsified else "OFFICIAL_COST_MATRIX_COMPLETE",
            "official_metadata_get_cost_used": not falsified,
            "full_session_matrix_reused_as_final": False,
            "rows": [] if falsified else rows,
            "chronological_role_costs": (
                {}
                if falsified
                else {"DISCOVERY": {}, "VALIDATION": {}, "FINAL_DEVELOPMENT": {}}
            ),
        },
        "acquisition": {
            "purchase_performed": False,
            "actual_spend_usd": 0.0,
            "prior_budget_usd": 28.498462508622012,
            "remaining_budget_usd": 28.498462508622012,
            "q4_accessed": False,
            "broker_connections": 0,
            "orders": 0,
            "manifest_bound_data_purchase_count": 0,
            "unmanifested_data_purchase_count": 0,
        },
        "long_sample": {
            "status": (
                "NOT_RUN_SEED_FALSIFIED"
                if falsified
                else "NOT_STARTED_NO_AFFORDABLE_SAMPLE"
            ),
            "decision": (
                "LONG_SAMPLE_SELECTIVE_OVERLAY_FALSIFIED"
                if falsified
                else "LONG_SAMPLE_SELECTIVE_OVERLAY_WEAK"
            ),
            "policy_frozen_before_final_development": False,
        },
        "diagnostic_forward": {
            "status": (
                "NOT_STARTED_SEED_FALSIFIED"
                if falsified
                else "NOT_STARTED_NO_AUTHORIZED_RESEARCH_FEED"
            ),
            "broker_connections": 0,
            "orders": 0,
            "parameter_changes": 0,
            "economic_promotion_allowed": False,
            "paper_shadow_ready": False,
        },
        "evidence_identity": {
            "campaign_id": CAMPAIGN_ID,
            "manifest_hash": manifest["manifest_hash"],
            "source_commit": manifest["source_commit"],
        },
        "evidence_datasets": _minimal_evidence_datasets(),
        "compact_outputs": {name: {"status": "COMPLETE"} for name in REQUIRED_COMPACT_OUTPUTS},
        "production_kpis": {},
        "runtime_metrics": {
            "elapsed_seconds": 1.0,
            "aggregate_cpu_utilization": 0.8,
            "economic_wall_clock_fraction": 0.9,
        },
    }


def test_runtime_accepts_robust_seed_then_cost_matrix_without_purchase(
    tmp_path: Path,
) -> None:
    _, manifest = _fixture(tmp_path)
    campaign = _campaign(manifest, seed_decision="SELECTIVE_VETO_SEED_ROBUST")

    scientific = _validate_campaign_result(manifest, campaign)

    assert scientific["seed_decision"] == "SELECTIVE_VETO_SEED_ROBUST"
    assert scientific["actual_spend_usd"] == 0.0


def test_runtime_falsified_seed_stops_cost_and_acquisition(tmp_path: Path) -> None:
    _, manifest = _fixture(tmp_path)
    campaign = _campaign(manifest, seed_decision="SELECTIVE_VETO_SEED_FALSIFIED")
    campaign["window_cost_matrix"]["status"] = "OFFICIAL_COST_MATRIX_COMPLETE"

    with pytest.raises(SelectiveVetoRuntimeError, match="did not stop"):
        _validate_campaign_result(manifest, campaign)


def test_runtime_rejects_purchase_over_cap_or_reserve(tmp_path: Path) -> None:
    _, manifest = _fixture(tmp_path)
    campaign = _campaign(manifest, seed_decision="SELECTIVE_VETO_SEED_ROBUST")
    campaign["acquisition"].update(
        {
            "purchase_performed": True,
            "actual_spend_usd": 8.25,
            "remaining_budget_usd": 20.248462508622012,
        }
    )

    with pytest.raises(SelectiveVetoRuntimeError, match="budget or safety"):
        _validate_campaign_result(manifest, campaign)


def test_runtime_rejects_forward_order_or_parameter_change(tmp_path: Path) -> None:
    _, manifest = _fixture(tmp_path)
    campaign = _campaign(manifest, seed_decision="SELECTIVE_VETO_SEED_ROBUST")
    campaign["diagnostic_forward"]["orders"] = 1

    with pytest.raises(SelectiveVetoRuntimeError, match="diagnostic-forward"):
        _validate_campaign_result(manifest, campaign)


def test_0034_manifest_rejects_account_snapshot_provenance_drift(
    tmp_path: Path,
) -> None:
    path, manifest = _fixture(tmp_path)
    manifest["account_rule_snapshots"]["50K"]["official_source_verified"] = True
    _rehash(manifest)

    with pytest.raises(SelectiveVetoManifestError, match="snapshot provenance"):
        validate_selective_veto_manifest(manifest, manifest_path=path)


def test_runtime_rejects_incomplete_cost_grid(tmp_path: Path) -> None:
    _, manifest = _fixture(tmp_path)
    campaign = _campaign(manifest, seed_decision="SELECTIVE_VETO_SEED_ROBUST")
    campaign["window_cost_matrix"]["rows"].pop()

    with pytest.raises(SelectiveVetoRuntimeError, match="grid coverage"):
        _validate_campaign_result(manifest, campaign)


def test_runtime_rejects_empty_or_sentinel_economic_evidence(tmp_path: Path) -> None:
    _, manifest = _fixture(tmp_path)
    campaign = _campaign(manifest, seed_decision="SELECTIVE_VETO_SEED_ROBUST")
    campaign["evidence_datasets"]["component_trades"] = []

    with pytest.raises(SelectiveVetoRuntimeError, match="dataset is empty"):
        _validate_campaign_result(manifest, campaign)

    campaign = _campaign(manifest, seed_decision="SELECTIVE_VETO_SEED_ROBUST")
    campaign["evidence_datasets"]["component_signals"][0][
        "component_role"
    ] = "DIAGNOSTIC_ONLY"
    with pytest.raises(SelectiveVetoRuntimeError, match="sentinel"):
        _validate_campaign_result(manifest, campaign)


def test_runtime_rejects_unmanifested_purchase_counter(tmp_path: Path) -> None:
    _, manifest = _fixture(tmp_path)
    campaign = _campaign(manifest, seed_decision="SELECTIVE_VETO_SEED_ROBUST")
    campaign["acquisition"]["unmanifested_data_purchase_count"] = 1

    with pytest.raises(SelectiveVetoRuntimeError, match="budget or safety"):
        _validate_campaign_result(manifest, campaign)


def test_runtime_live_state_uses_generic_controller_topology(tmp_path: Path) -> None:
    _, manifest = _fixture(tmp_path)
    output = tmp_path / "runtime-output"
    _write_state(
        output,
        manifest,
        state="STARTING",
        stage="SEED_AUDIT",
        next_action="RUN_SEED_AUDIT",
    )
    state = json.loads((output / "production_state.json").read_text())
    kpis = json.loads((output / "production_kpis.json").read_text())

    assert state["policies_proposed"] == 0
    assert state["combine_episodes_completed"] == 0
    assert kpis["workers"] == {"compute": 2, "evidence_writer": 1}
    assert kpis["rates_per_hour"]["combine_episodes"] == 0.0
    assert (
        _next_recommendation("LONG_SAMPLE_SELECTIVE_OVERLAY_WEAK")[
            "recommendation"
        ]["new_data_purchase_authorized"]
        is False
    )


def test_runtime_active_forward_requires_post_freeze_provenance(tmp_path: Path) -> None:
    _, manifest = _fixture(tmp_path)
    campaign = _campaign(manifest, seed_decision="SELECTIVE_VETO_SEED_ROBUST")
    campaign["diagnostic_forward"].update(
        {
            "status": "SELECTIVE_VETO_DIAGNOSTIC_FORWARD",
            "authorized_research_feed": True,
            "policy_ids": [PRIMARY_SEED_ID, SECONDARY_SEED_ID],
            "append_only": True,
            "zero_order": True,
            "first_event_time_utc": "2026-07-18T18:01:00Z",
            "last_event_time_utc": "2026-07-18T18:02:00Z",
            "raw_event_fingerprints": ["f" * 64],
            "raw_event_count": 1,
            "policy_fingerprints": {
                PRIMARY_SEED_ID: "1" * 64,
                SECONDARY_SEED_ID: "2" * 64,
            },
        }
    )

    with pytest.raises(SelectiveVetoRuntimeError, match="provenance drift"):
        _validate_campaign_result(manifest, campaign)
