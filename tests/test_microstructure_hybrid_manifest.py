from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import pytest

from hydra.economic_evolution.schema import stable_hash
from hydra.evidence import REQUIRED_DATASETS
from hydra.production.microstructure_hybrid_manifest import (
    ACTION_IDS,
    ACTION_TYPES,
    ANCHOR_MARKETS,
    ANCHOR_SESSION_DATES,
    CAMPAIGN_ID,
    CAMPAIGN_MODE,
    CHRONOLOGICAL_ROLE_COUNTS,
    CLASS_ID,
    FROZEN_STRUCTURAL_ANCHOR_COUNT,
    HybridManifestError,
    MANIFEST_SCHEMA,
    MAXIMUM_HYBRID_POLICIES,
    MAXIMUM_STRUCTURAL_ANCHORS,
    PILOT_DECISIONS,
    RISK_LEVELS,
    RUNTIME_VERSION,
    validate_microstructure_hybrid_manifest,
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


def _source_descriptors(
    tmp_path: Path, *, directory: str, labels: tuple[str, ...]
) -> tuple[dict[str, dict[str, str]], dict[str, Path]]:
    descriptors: dict[str, dict[str, str]] = {}
    paths: dict[str, Path] = {}
    for label in labels:
        path = tmp_path / f"data/{directory}/{label}.bin"
        paths[label] = path
        descriptors[label] = {
            "path": path.relative_to(tmp_path).as_posix(),
            "sha256": _write(path, f"{directory}:{label}\n".encode()),
        }
    return descriptors, paths


def _fixture(
    tmp_path: Path,
) -> tuple[Path, dict[str, Any], dict[str, Path]]:
    manifest_path = tmp_path / "config/v7/microstructure_hybrid_0033.json"
    implementation_files = {
        relative: _write(tmp_path / relative, relative.encode())
        for relative in (
            "hydra/production/microstructure_hybrid_manifest.py",
            "hydra/production/microstructure_hybrid_pilot.py",
            "hydra/production/microstructure_hybrid_runtime.py",
            "hydra/production/manifest.py",
            "hydra/production/runtime.py",
            "scripts/run_economic_production_manifest.py",
        )
    }
    source_0032, paths_0032 = _source_descriptors(
        tmp_path,
        directory="source_0032",
        labels=(
            "authoritative_result",
            "evidence_bundle_receipt",
            "decision_report",
            "opportunity_episodes",
            "opportunity_outcomes",
        ),
    )
    source_0031, paths_0031 = _source_descriptors(
        tmp_path,
        directory="source_0031",
        labels=(
            "event_store_receipt",
            "raw_dbn",
            "book_snapshots",
            "derived_events",
            "feature_matrices",
            "outcome_labels",
        ),
    )
    source_0028, paths_0028 = _source_descriptors(
        tmp_path,
        directory="source_0028",
        labels=("source_result", "candidate_population"),
    )

    anchor_ids = tuple(
        f"hazard_{value:024x}" for value in range(FROZEN_STRUCTURAL_ANCHOR_COUNT)
    )
    event_ledgers: dict[str, dict[str, Any]] = {}
    ledger_paths: dict[str, Path] = {}
    for ordinal, candidate_id in enumerate(anchor_ids):
        path = tmp_path / f"data/source_0028/stage2_event_evidence/{candidate_id}.jsonl"
        ledger_paths[candidate_id] = path
        event_ledgers[candidate_id] = {
            "candidate_id": candidate_id,
            "market": ANCHOR_MARKETS[ordinal % len(ANCHOR_MARKETS)],
            "active_session_dates": [
                ANCHOR_SESSION_DATES[ordinal % len(ANCHOR_SESSION_DATES)]
            ],
            "event_count_in_window": ordinal + 1,
            "path": path.relative_to(tmp_path).as_posix(),
            "sha256": _write(path, f"event-ledger:{candidate_id}\n".encode()),
        }

    actions = [
        {
            "action_id": action_id,
            "action_type": action_type,
            "description": f"Frozen 0033 paired action {action_type}.",
            "side_lane_only": action_id == "A4",
        }
        for action_id, action_type in zip(ACTION_IDS, ACTION_TYPES, strict=True)
    ]

    manifest: dict[str, Any] = {
        "schema": MANIFEST_SCHEMA,
        "campaign_mode": CAMPAIGN_MODE,
        "campaign_id": CAMPAIGN_ID,
        "class_id": CLASS_ID,
        "policy_classes": [CLASS_ID],
        "development_only": True,
        "created_at_utc": "2026-07-18T12:00:00Z",
        "source_commit": "a" * 40,
        "economic_hypothesis": (
            "Causal microstructure actions can improve clean structural "
            "opportunity economics without changing the structural anchors."
        ),
        "implementation_files": implementation_files,
        "terminal_source_0032": {
            "campaign_id": "hydra_microstructure_sparse_alpha_distillation_0032",
            "terminal_status": "SPARSE_PILOT_WEAK",
            "reuse_mode": "IMMUTABLE_READ_ONLY",
            "retry_or_retune_allowed": False,
            "status_inheritance_allowed": False,
            "outcomes_are_development_only": True,
            "source_hashes": source_0032,
        },
        "immutable_source_store_0031": {
            "campaign_id": "hydra_microstructure_order_flow_foundry_0031",
            "terminal_status": "MICROSTRUCTURE_PILOT_FALSIFIED",
            "store_status": "BOOK_STATE_RECONSTRUCTION_GREEN",
            "reuse_mode": "IMMUTABLE_READ_ONLY",
            "raw_rewrite_allowed": False,
            "source_feature_recomputation_allowed": False,
            "outcome_labels_physically_separate": True,
            "status_inheritance_allowed": False,
            "source_hashes": source_0031,
        },
        "clean_structural_anchors_0028": {
            "campaign_id": "hydra_causal_target_velocity_0028",
            "terminal_status": "CAUSAL_TARGET_VELOCITY_INCONCLUSIVE_COVERAGE_LIMITED",
            "component_status": "LOW_VELOCITY_CAUSAL_REFERENCE_BANK",
            "reuse_mode": "IMMUTABLE_READ_ONLY",
            "maximum_anchor_count": MAXIMUM_STRUCTURAL_ANCHORS,
            "frozen_anchor_count": FROZEN_STRUCTURAL_ANCHOR_COUNT,
            "anchor_ids": list(anchor_ids),
            "markets": list(ANCHOR_MARKETS),
            "coverage_session_dates": list(ANCHOR_SESSION_DATES),
            "anchor_selection_frozen_before_hybrid_outcomes": True,
            "promotion_status_inheritance_allowed": False,
            "source_hashes": source_0028,
            "event_ledgers": event_ledgers,
        },
        "runtime": {
            "engine": "production_kernel_v1",
            "runner": "scripts/run_economic_production_manifest.py",
            "hybrid_runtime_version": RUNTIME_VERSION,
            "output_dir": (
                "reports/economic_evolution/"
                "hybrid_structural_alpha_order_flow_0033"
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
        "structural_opportunity_episode_contract": {
            "schema": "hydra_structural_opportunity_episode_v1",
            "availability_rule": "available_at<=decision_time",
            "persist_timestamps": [
                "event_time",
                "available_at",
                "decision_time",
                "order_submit_time",
                "earliest_executable_time",
                "fill_time",
            ],
            "future_outcomes_are_labels_only": True,
            "outcome_labels_physically_separate": True,
            "post_confirmation_episode_fields_in_decision_allowed": False,
            "future_label_availability_in_eligibility": False,
            "one_decision_per_episode": True,
            "duplicate_episode_ids_allowed": False,
            "batch_streaming_decision_equality_required": True,
            "missing_future_coverage_status": "CENSORED_FUTURE_COVERAGE",
            "censored_suppresses_decision": False,
        },
        "paired_action_frontier": {
            "action_ids": list(ACTION_IDS),
            "actions": actions,
            "risk_levels": list(RISK_LEVELS),
            "maximum_policy_count": MAXIMUM_HYBRID_POLICIES,
            "paired_on_identical_episode": True,
            "paired_on_identical_start_and_costs": True,
            "action_selected_from_decision_time_features_only": True,
            "abstention_has_zero_exposure_and_cost": True,
            "continuous_risk_optimization_allowed": False,
            "neighbor_action_generation_allowed": False,
            "frontier_frozen_before_outcomes": True,
        },
        "chronological_roles": {
            "discovery_sessions": CHRONOLOGICAL_ROLE_COUNTS[0],
            "validation_sessions": CHRONOLOGICAL_ROLE_COUNTS[1],
            "final_development_sessions": CHRONOLOGICAL_ROLE_COUNTS[2],
            "random_temporal_mixing_allowed": False,
            "roles_frozen_before_outcomes": True,
            "validation_or_final_used_for_thresholds": False,
            "final_development_is_independent_confirmation": False,
        },
        "development_gate": {
            "allowed_decisions": list(PILOT_DECISIONS),
            "thresholds_may_change_after_results": False,
            "development_only": True,
            "independent_confirmation_claim_allowed": False,
            "positive_stressed_economics_required": True,
            "material_uplift_over_structural_anchor_required": True,
            "acceptable_mll_and_consistency_required": True,
            "final_development_evidence_required": True,
        },
        "conditional_extension": {
            "enabled_before_qualified_gate": False,
            "trigger_decisions": [
                "HYBRID_OVERLAY_GREEN",
                "HYBRID_OVERLAY_WEAK",
            ],
            "weak_qualification_required": True,
            "weak_qualification": {
                "positive_paired_uplift_validation": True,
                "positive_paired_uplift_final_development": True,
                "minimum_near_break_even_stressed_strategy_count": 1,
            },
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
            "prior_global_N_trials": 762_214,
            "prospective_comparisons": MAXIMUM_HYBRID_POLICIES,
            "campaign_specific_inflation": 1.5,
            "reserved_delta_trials": 30,
            "expected_global_N_trials_after_reservation": 762_244,
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
        },
    }
    _rehash(manifest)
    paths = {
        **{f"0032:{key}": value for key, value in paths_0032.items()},
        **{f"0031:{key}": value for key, value in paths_0031.items()},
        **{f"0028:{key}": value for key, value in paths_0028.items()},
        **{f"ledger:{key}": value for key, value in ledger_paths.items()},
    }
    return manifest_path, manifest, paths


def test_valid_hybrid_manifest_freezes_complete_0033_contract(
    tmp_path: Path,
) -> None:
    path, manifest, _ = _fixture(tmp_path)

    validate_microstructure_hybrid_manifest(manifest, manifest_path=path)

    assert tuple(manifest["paired_action_frontier"]["action_ids"]) == ACTION_IDS
    assert tuple(manifest["paired_action_frontier"]["risk_levels"]) == RISK_LEVELS
    assert len(manifest["clean_structural_anchors_0028"]["anchor_ids"]) == 22
    assert manifest["runtime"]["worker_count"] == 2
    assert manifest["compute_contract"]["authoritative_writer_count"] == 1


def test_hybrid_manifest_rejects_semantic_hash_drift(tmp_path: Path) -> None:
    path, manifest, _ = _fixture(tmp_path)
    manifest["economic_hypothesis"] = "post-freeze mutation"

    with pytest.raises(HybridManifestError, match="semantic hash drift"):
        validate_microstructure_hybrid_manifest(manifest, manifest_path=path)


def test_hybrid_manifest_rejects_implementation_hash_drift(tmp_path: Path) -> None:
    path, manifest, _ = _fixture(tmp_path)
    target = tmp_path / "hydra/production/microstructure_hybrid_runtime.py"
    target.write_bytes(b"mutated implementation\n")

    with pytest.raises(HybridManifestError, match="implementation checksum drift"):
        validate_microstructure_hybrid_manifest(manifest, manifest_path=path)


def test_hybrid_manifest_rejects_immutable_source_drift(tmp_path: Path) -> None:
    path, manifest, paths = _fixture(tmp_path)
    paths["0031:feature_matrices"].write_bytes(b"mutated store\n")

    with pytest.raises(HybridManifestError, match="0031 store source checksum drift"):
        validate_microstructure_hybrid_manifest(manifest, manifest_path=path)


def test_hybrid_manifest_requires_exact_22_anchor_ledgers(tmp_path: Path) -> None:
    path, manifest, _ = _fixture(tmp_path)
    candidate_id = manifest["clean_structural_anchors_0028"]["anchor_ids"].pop()
    manifest["clean_structural_anchors_0028"]["event_ledgers"].pop(candidate_id)
    _rehash(manifest)

    with pytest.raises(HybridManifestError, match="anchor contract drift"):
        validate_microstructure_hybrid_manifest(manifest, manifest_path=path)


def test_hybrid_manifest_rejects_anchor_ledger_drift(tmp_path: Path) -> None:
    path, manifest, paths = _fixture(tmp_path)
    candidate_id = manifest["clean_structural_anchors_0028"]["anchor_ids"][0]
    paths[f"ledger:{candidate_id}"].write_bytes(b"mutated event ledger\n")

    with pytest.raises(HybridManifestError, match="event-ledger checksum drift"):
        validate_microstructure_hybrid_manifest(manifest, manifest_path=path)


@pytest.mark.parametrize(
    ("path", "value", "message"),
    [
        (("terminal_source_0032", "terminal_status"), "SPARSE_PILOT_GREEN", "0032 source"),
        (("immutable_source_store_0031", "reuse_mode"), "MUTABLE", "0031 store"),
        (("clean_structural_anchors_0028", "maximum_anchor_count"), 25, "anchor contract"),
        (("runtime", "worker_count"), 3, "topology"),
        (("compute_contract", "authoritative_writer_count"), 2, "topology"),
        (("paired_action_frontier", "action_ids"), ["A0", "A1"], "action/risk"),
        (("paired_action_frontier", "risk_levels"), [0.5, 1.0, 2.0], "action/risk"),
        (("paired_action_frontier", "maximum_policy_count"), 21, "action/risk"),
        (("chronological_roles", "discovery_sessions"), 4, "chronological role"),
        (("conditional_extension", "enabled_before_qualified_gate"), True, "extension"),
        (("conditional_extension", "maximum_incremental_spend_usd"), 3.26, "extension"),
        (("conditional_extension", "minimum_budget_reserve_usd"), 24.99, "extension"),
        (("governance", "q4_access_allowed"), True, "governance"),
        (("governance", "broker_connection_allowed"), True, "governance"),
        (("governance", "orders_allowed"), True, "governance"),
    ],
)
def test_hybrid_manifest_rejects_frozen_contract_drift(
    tmp_path: Path,
    path: tuple[str, ...],
    value: Any,
    message: str,
) -> None:
    manifest_path, manifest, _ = _fixture(tmp_path)
    _set(manifest, path, value)

    with pytest.raises(HybridManifestError, match=message):
        validate_microstructure_hybrid_manifest(
            manifest, manifest_path=manifest_path
        )


def test_hybrid_manifest_requires_exact_action_meanings(tmp_path: Path) -> None:
    path, manifest, _ = _fixture(tmp_path)
    manifest["paired_action_frontier"]["actions"][4]["action_type"] = (
        "MARKET_IMMEDIATE"
    )
    _rehash(manifest)

    with pytest.raises(HybridManifestError, match="paired action/risk"):
        validate_microstructure_hybrid_manifest(manifest, manifest_path=path)


def test_hybrid_manifest_keeps_passive_join_in_side_lane(tmp_path: Path) -> None:
    path, manifest, _ = _fixture(tmp_path)
    manifest["paired_action_frontier"]["actions"][4]["side_lane_only"] = False
    _rehash(manifest)

    with pytest.raises(HybridManifestError, match="paired action/risk"):
        validate_microstructure_hybrid_manifest(manifest, manifest_path=path)


def test_hybrid_manifest_extension_must_preserve_25_dollar_reserve(
    tmp_path: Path,
) -> None:
    path, manifest, _ = _fixture(tmp_path)
    _set(
        manifest,
        ("conditional_extension", "current_remaining_budget_usd"),
        28.24,
    )

    with pytest.raises(HybridManifestError, match="conditional data extension"):
        validate_microstructure_hybrid_manifest(manifest, manifest_path=path)


def test_hybrid_manifest_rejects_source_path_escape(tmp_path: Path) -> None:
    path, manifest, _ = _fixture(tmp_path)
    _set(
        manifest,
        (
            "immutable_source_store_0031",
            "source_hashes",
            "raw_dbn",
            "path",
        ),
        "../../outside.dbn",
    )

    with pytest.raises(HybridManifestError, match="path escapes project root"):
        validate_microstructure_hybrid_manifest(manifest, manifest_path=path)


def test_hybrid_manifest_reserves_all_20_policies_before_outcomes(
    tmp_path: Path,
) -> None:
    path, manifest, _ = _fixture(tmp_path)
    _set(
        manifest,
        ("multiplicity", "expected_global_N_trials_after_reservation"),
        762_243,
    )

    with pytest.raises(HybridManifestError, match="multiplicity reservation"):
        validate_microstructure_hybrid_manifest(manifest, manifest_path=path)
