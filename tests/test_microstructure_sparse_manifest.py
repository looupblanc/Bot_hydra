from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from hydra.economic_evolution.schema import stable_hash
from hydra.evidence import REQUIRED_DATASETS
from hydra.production.microstructure_sparse_manifest import (
    ACCOUNT_HORIZONS_DAYS,
    CAMPAIGN_ID,
    CAMPAIGN_MODE,
    CLASS_ID,
    EDGE_TO_COST_RATIOS,
    EXIT_TYPES,
    GATE_DECISIONS,
    HOLDING_HORIZONS_SECONDS,
    RUNTIME_VERSION,
    SparseManifestError,
    TRADE_BUDGETS_PER_SESSION,
    validate_microstructure_sparse_manifest,
)


def _write(path: Path, content: bytes = b"fixture\n") -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return hashlib.sha256(content).hexdigest()


def _rehash(manifest: dict[str, Any]) -> None:
    manifest.pop("manifest_hash", None)
    manifest["manifest_hash"] = stable_hash(manifest)


def _set(manifest: dict[str, Any], path: tuple[str, ...], value: Any) -> None:
    target: dict[str, Any] = manifest
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    _rehash(manifest)


def _fixture(tmp_path: Path) -> tuple[Path, dict[str, Any], dict[str, Path]]:
    manifest_path = tmp_path / "config/v7/microstructure_sparse_0032.json"
    implementation_files = {
        relative: _write(tmp_path / relative, relative.encode())
        for relative in (
            "hydra/production/microstructure_sparse_manifest.py",
            "hydra/production/microstructure_sparse_runtime.py",
            "scripts/run_economic_production_manifest.py",
        )
    }
    source_paths: dict[str, Path] = {}
    source_hashes: dict[str, dict[str, str]] = {}
    for label in (
        "authoritative_result",
        "evidence_bundle_receipt",
        "event_store_receipt",
        "raw_dbn",
        "derived_events",
        "feature_matrices",
        "outcome_labels",
        "signals",
        "trades",
        "episodes",
    ):
        path = tmp_path / f"data/source_0031/{label}.bin"
        source_paths[label] = path
        source_hashes[label] = {
            "path": path.relative_to(tmp_path).as_posix(),
            "sha256": _write(path, f"0031:{label}\n".encode()),
        }

    manifest: dict[str, Any] = {
        "schema": "hydra_economic_production_manifest_v1",
        "campaign_mode": CAMPAIGN_MODE,
        "campaign_id": CAMPAIGN_ID,
        "class_id": CLASS_ID,
        "policy_classes": [CLASS_ID],
        "development_only": True,
        "created_at_utc": "2026-07-18T11:00:00Z",
        "source_commit": "a" * 40,
        "economic_hypothesis": (
            "Sparse, causally selected order-flow states can retain enough net "
            "edge after costs to improve account target velocity."
        ),
        "implementation_files": implementation_files,
        "terminal_baseline_0031": {
            "campaign_id": "hydra_microstructure_order_flow_foundry_0031",
            "terminal_status": "MICROSTRUCTURE_PILOT_FALSIFIED",
            "manifest_hash": "b" * 64,
            "authoritative_bundle_content_sha256": "c" * 64,
            "account_terminal_recovery_receipt_hash": "d" * 64,
            "candidate_count": 24,
            "exact_replays": 24,
            "matched_control_replays": 72,
            "combine_episodes": 720,
            "normal_episodes": 360,
            "stressed_episodes": 360,
            "positive_stressed_candidates": 0,
            "normal_pass_candidates": 0,
            "stressed_pass_candidates": 0,
            "teacher_event_count": 61_240,
            "events_processed": 92_127_026,
            "full_coverage_episodes": 5,
            "data_censored_episodes": 94,
            "mll_breached_episodes": 621,
            "actual_spend_usd": 8.648637887836,
            "remaining_budget_usd": 28.498462508622012,
            "mass_scale_started": False,
            "xfa_paths": 0,
            "retry_or_retune_allowed": False,
            "status_inheritance_allowed": False,
        },
        "source_store": {
            "source_campaign_id": "hydra_microstructure_order_flow_foundry_0031",
            "reuse_mode": "IMMUTABLE_READ_ONLY",
            "raw_rewrite_allowed": False,
            "source_feature_recomputation_allowed": False,
            "outcome_labels_physically_separate": True,
            "source_status_inheritance_allowed": False,
            "source_hashes": source_hashes,
        },
        "runtime": {
            "engine": "production_kernel_v1",
            "runner": "scripts/run_economic_production_manifest.py",
            "sparse_runtime_version": RUNTIME_VERSION,
            "output_dir": (
                "reports/economic_evolution/"
                "microstructure_sparse_alpha_distillation_0032"
            ),
            "result_name": "economic_production_result.json",
            "result_schema": "hydra_economic_production_result_v1",
            "controller_source_change_required": False,
            "resume_from_checkpoint": True,
            "orchestrator_count": 1,
            "worker_count": 2,
            "asynchronous_evidence_writer_count": 1,
        },
        "compute_contract": {
            "orchestrator_count": 1,
            "cpu_worker_count": 2,
            "authoritative_writer_count": 1,
            "cpu_workers_read_only": True,
            "single_writer_atomic_commits": True,
            "oversubscription_allowed": False,
        },
        "forensic_bridge": {
            "required_before_sparse_outcomes": True,
            "source_signal_trade_episode_reconciliation": True,
            "gross_cost_net_arithmetic_reconciliation": True,
            "account_terminal_precedence_preserved": True,
            "post_mll_path_truncation_preserved": True,
            "source_features_signals_trades_mutable": False,
            "source_result_reinterpretation_allowed": False,
            "bridge_is_new_economic_evidence": False,
        },
        "opportunity_episode_contract": {
            "availability_rule": "available_at<=decision_time",
            "future_outcomes_are_labels_only": True,
            "future_label_availability_in_eligibility": False,
            "negative_shift_in_decision_code": False,
            "one_decision_per_opportunity": True,
            "duplicate_opportunities_allowed": False,
            "missing_future_coverage_status": "CENSORED_FUTURE_COVERAGE",
            "censored_in_headline_denominator": False,
            "chronological_roles_frozen_before_outcomes": True,
        },
        "finite_state_engine": {
            "states": [
                "FLAT",
                "ARMED",
                "ENTRY_PENDING",
                "OPEN",
                "EXIT_PENDING",
                "COOLDOWN",
            ],
            "single_authoritative_step": True,
            "batch_streaming_decision_equality": True,
            "event_time_sequence_required": True,
            "restart_resume_idempotent": True,
            "duplicate_event_idempotent": True,
            "future_state_access_allowed": False,
        },
        "meta_labeling": {
            "teacher_source": "CAMPAIGN_0031_MBO_OUTCOME_LABELS",
            "student_source": "CAMPAIGN_0031_CAUSAL_FEATURE_MATRICES",
            "model_classes": [
                "REGULARIZED_LOGISTIC_REGRESSION",
                "SHALLOW_DECISION_TREE",
                "MONOTONIC_GRADIENT_BOOSTING",
            ],
            "chronological_cross_fit": True,
            "random_temporal_mixing_allowed": False,
            "teacher_fields_at_inference_allowed": False,
            "classification_accuracy_alone_promotable": False,
            "economic_utility_required": True,
        },
        "execution_model": {
            "decision_after_completed_event": True,
            "earliest_fill_after_decision": True,
            "touch_implies_fill": False,
            "partial_fills_modeled": True,
            "available_depth_enforced": True,
            "fees_and_slippage_frozen": True,
            "normal_cost_multiplier": 1.0,
            "stressed_cost_multiplier": 1.5,
            "sub_millisecond_latency_arbitrage_allowed": False,
        },
        "sparse_policy_frontier": {
            "edge_to_cost_ratios": list(EDGE_TO_COST_RATIOS),
            "trade_budgets_per_session": list(TRADE_BUDGETS_PER_SESSION),
            "max_strategies": 30,
            "continuous_threshold_optimization_allowed": False,
            "parameter_clone_admission_allowed": False,
            "frontier_frozen_before_outcomes": True,
        },
        "holding_exit_frontier": {
            "horizons_seconds": list(HOLDING_HORIZONS_SECONDS),
            "exit_types": list(EXIT_TYPES),
            "event_state_reset_is_causal": True,
            "unrestricted_exit_search_allowed": False,
        },
        "account_evaluation": {
            "horizons_days": list(ACCOUNT_HORIZONS_DAYS),
            "cost_scenarios": ["NORMAL", "STRESSED_1_5X"],
            "full_coverage_only_headline": True,
            "overlapping_windows_independent": False,
            "xfa_enabled": False,
        },
        "account_size_frontier": {
            "account_sizes_usd": [50_000, 100_000, 150_000],
            "official_rule_snapshot_per_size_required": True,
            "legal_contract_limits_required": True,
            "selected_after_final_outcomes": False,
        },
        "development_gate": {
            "allowed_decisions": list(GATE_DECISIONS),
            "thresholds_may_change_after_results": False,
            "development_only": True,
            "independent_confirmation_claim_allowed": False,
            "green_requirements": {
                "material_target_velocity_uplift": True,
                "positive_stressed_economics": True,
                "two_behaviorally_distinct_mechanism_families": True,
                "positive_net_validation_and_final_development": True,
                "no_mll_breach_frozen_risk_profile": True,
                "median_trades_per_session_maximum": 12,
                "single_event_domination_allowed": False,
                "acceptable_mll_and_consistency": True,
                "final_development_evidence": True,
                "deployable_causal_strategy": True,
            },
            "weak_requires_information_uplift": True,
            "falsified_when_no_material_uplift": True,
        },
        "conditional_extension": {
            "enabled": True,
            "trigger_decisions": ["SPARSE_PILOT_GREEN"],
            "maximum_incremental_spend_usd": 3.25,
            "minimum_budget_reserve_usd": 25.0,
            "current_remaining_budget_usd": 28.498462508622012,
            "maximum_extension_count": 1,
            "official_cost_estimate_required_before_purchase": True,
            "automatic_purchase_allowed": False,
            "broad_historical_purchase_allowed": False,
            "q4_access_allowed": False,
        },
        "multiplicity": {
            "prior_global_N_trials": 762_169,
            "prospective_comparisons": 30,
            "campaign_specific_inflation": 1.5,
            "reserved_delta_trials": 45,
            "expected_global_N_trials_after_reservation": 762_214,
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
            "xfa_before_clean_combine_survivors_allowed": False,
        },
    }
    _rehash(manifest)
    return manifest_path, manifest, source_paths


def test_valid_sparse_manifest_freezes_complete_0032_contract(
    tmp_path: Path,
) -> None:
    path, manifest, _ = _fixture(tmp_path)

    validate_microstructure_sparse_manifest(manifest, manifest_path=path)

    assert manifest["runtime"]["worker_count"] == 2
    assert manifest["compute_contract"] == {
        "orchestrator_count": 1,
        "cpu_worker_count": 2,
        "authoritative_writer_count": 1,
        "cpu_workers_read_only": True,
        "single_writer_atomic_commits": True,
        "oversubscription_allowed": False,
    }
    assert tuple(manifest["development_gate"]["allowed_decisions"]) == (
        "SPARSE_PILOT_GREEN",
        "SPARSE_PILOT_WEAK",
        "SPARSE_PILOT_FALSIFIED",
    )


def test_sparse_manifest_rejects_semantic_hash_drift(tmp_path: Path) -> None:
    path, manifest, _ = _fixture(tmp_path)
    manifest["economic_hypothesis"] = "changed after freeze"

    with pytest.raises(SparseManifestError, match="semantic hash drift"):
        validate_microstructure_sparse_manifest(manifest, manifest_path=path)


def test_sparse_manifest_rejects_0031_source_byte_drift(tmp_path: Path) -> None:
    path, manifest, sources = _fixture(tmp_path)
    sources["trades"].write_bytes(b"mutated source evidence\n")

    with pytest.raises(SparseManifestError, match="source checksum drift: trades"):
        validate_microstructure_sparse_manifest(manifest, manifest_path=path)


@pytest.mark.parametrize(
    ("path", "value", "message"),
    [
        (("terminal_baseline_0031", "positive_stressed_candidates"), 1, "baseline"),
        (("runtime", "orchestrator_count"), 2, "topology"),
        (("runtime", "worker_count"), 3, "topology"),
        (("compute_contract", "authoritative_writer_count"), 2, "topology"),
        (
            ("sparse_policy_frontier", "edge_to_cost_ratios"),
            [1.25, 1.5, 2.0, 2.5],
            "frontier",
        ),
        (
            ("sparse_policy_frontier", "trade_budgets_per_session"),
            [2, 4, 8, 16],
            "frontier",
        ),
        (("sparse_policy_frontier", "max_strategies"), 31, "frontier"),
        (("holding_exit_frontier", "horizons_seconds"), [30, 120, 300], "frontier"),
        (("account_evaluation", "horizons_days"), [5, 10, 30], "frontier"),
        (
            ("development_gate", "allowed_decisions"),
            ["SPARSE_PILOT_GREEN", "SPARSE_PILOT_WEAK"],
            "decision gate",
        ),
        (("conditional_extension", "maximum_incremental_spend_usd"), 3.26, "extension"),
        (("conditional_extension", "minimum_budget_reserve_usd"), 24.99, "extension"),
        (("governance", "q4_access_allowed"), True, "governance"),
        (("governance", "broker_connection_allowed"), True, "governance"),
        (("governance", "orders_allowed"), True, "governance"),
    ],
)
def test_sparse_manifest_rejects_frozen_contract_drift(
    tmp_path: Path,
    path: tuple[str, ...],
    value: Any,
    message: str,
) -> None:
    manifest_path, manifest, _ = _fixture(tmp_path)
    _set(manifest, path, value)

    with pytest.raises(SparseManifestError, match=message):
        validate_microstructure_sparse_manifest(manifest, manifest_path=manifest_path)


def test_sparse_manifest_extension_must_preserve_25_dollar_reserve(
    tmp_path: Path,
) -> None:
    path, manifest, _ = _fixture(tmp_path)
    _set(manifest, ("conditional_extension", "current_remaining_budget_usd"), 28.24)

    with pytest.raises(SparseManifestError, match="conditional data extension"):
        validate_microstructure_sparse_manifest(manifest, manifest_path=path)


def test_sparse_manifest_multiplicity_is_reserved_before_outcomes(
    tmp_path: Path,
) -> None:
    path, manifest, _ = _fixture(tmp_path)
    _set(
        manifest,
        ("multiplicity", "expected_global_N_trials_after_reservation"),
        762_213,
    )

    with pytest.raises(SparseManifestError, match="multiplicity reservation"):
        validate_microstructure_sparse_manifest(manifest, manifest_path=path)


def test_sparse_manifest_rejects_source_path_escape(tmp_path: Path) -> None:
    path, manifest, _ = _fixture(tmp_path)
    _set(
        manifest,
        ("source_store", "source_hashes", "raw_dbn", "path"),
        "../../outside.dbn",
    )

    with pytest.raises(SparseManifestError, match="path escapes project root"):
        validate_microstructure_sparse_manifest(manifest, manifest_path=path)
