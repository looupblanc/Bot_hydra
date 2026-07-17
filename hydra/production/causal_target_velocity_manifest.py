"""Frozen manifest contract for causal target-velocity campaign 0028.

This module is deliberately validation-only.  It gives the existing V17
manifest runtime a narrow, immutable contract for campaign 0028 without
embedding campaign logic in the controller or authorizing a second writer.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from pathlib import Path
from typing import Any, Mapping

from hydra.economic_evolution.schema import stable_hash
from hydra.evidence import REQUIRED_DATASETS


CAUSAL_TARGET_VELOCITY_MANIFEST_SCHEMA = (
    "hydra_causal_target_velocity_manifest_v1"
)
CAUSAL_TARGET_VELOCITY_ENGINE = "causal_target_velocity_v1"
CAUSAL_TARGET_VELOCITY_CAMPAIGN_ID = "hydra_causal_target_velocity_0028"
CAUSAL_TARGET_VELOCITY_CLASS_ID = (
    "TARGET_BEFORE_ADVERSE_EXCURSION_HAZARD_OPPORTUNITY_DENSITY_V1"
)
CAUSAL_TARGET_VELOCITY_RESULT_SCHEMA = "hydra_economic_production_result_v1"

_HEX_40_OR_64 = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
_ALLOWED_MARKETS = frozenset({"CL", "ES", "NQ", "RTY", "YM", "GC"})
_REQUIRED_TIMEFRAMES = frozenset({"1m", "5m", "15m", "30m", "60m"})
_REQUIRED_SESSIONS = frozenset(
    {"OVERNIGHT", "SESSION_OPEN", "INTRADAY", "CLOSE"}
)
_REQUIRED_MECHANISMS = frozenset(
    {
        "VOLATILITY_EXPANSION",
        "PARTICIPATION_OPPORTUNITY_DENSITY",
        "MULTI_TIMEFRAME_ALIGNMENT",
        "CROSS_ASSET_STATE",
        "REVERSAL_AFTER_EXHAUSTION",
        "SESSION_SPECIALIZED_MECHANISM",
    }
)
_REQUIRED_FAILURE_CLASSES = frozenset(
    {
        "TOO_FEW_OPPORTUNITIES",
        "LOW_FAVORABLE_BEFORE_ADVERSE_RATE",
        "COST_FRAGILITY",
        "TARGET_TOO_SLOW",
        "MLL_EXCESS",
        "CONSISTENCY_FAILURE",
        "TEMPORAL_INSTABILITY",
        "MARKET_CONCENTRATION",
        "DUPLICATE_BEHAVIOR",
    }
)


class CausalTargetVelocityManifestError(RuntimeError):
    """The 0028 manifest is unsafe, incomplete, or no longer immutable."""


def load_and_validate_causal_target_velocity_manifest(
    path: str | Path,
) -> dict[str, Any]:
    """Load and fail-closed validate the one authorized 0028 manifest."""

    resolved = Path(path).resolve()
    manifest = _load_json(resolved)
    claimed = str(manifest.get("manifest_hash") or "")
    payload = dict(manifest)
    payload.pop("manifest_hash", None)
    if not claimed or stable_hash(payload) != claimed:
        raise CausalTargetVelocityManifestError("0028 manifest hash drift")
    if (
        manifest.get("schema") != CAUSAL_TARGET_VELOCITY_MANIFEST_SCHEMA
        or manifest.get("campaign_id") != CAUSAL_TARGET_VELOCITY_CAMPAIGN_ID
        or manifest.get("class_id") != CAUSAL_TARGET_VELOCITY_CLASS_ID
        or manifest.get("development_only") is not True
        or not _HEX_40_OR_64.fullmatch(str(manifest.get("source_commit") or ""))
        or not str(manifest.get("economic_hypothesis") or "").strip()
    ):
        raise CausalTargetVelocityManifestError("0028 identity declaration drift")

    root = _project_root(resolved)
    _validate_implementation_files(manifest, root)
    _validate_terminal_baseline(manifest, root)
    _validate_technical_repair(manifest, root)
    _validate_causal_contract(manifest)
    _validate_risk_preflight(manifest)
    _validate_hazard_labels(manifest)
    _validate_search_space(manifest)
    _validate_scale_and_coverage(manifest)
    _validate_controls_objective_and_gates(manifest)
    _validate_compute_data_and_safety(manifest)
    _validate_evidence_contract(manifest)
    _validate_runtime(manifest)
    _validate_multiplicity(manifest)
    return manifest


def _validate_implementation_files(
    manifest: Mapping[str, Any], root: Path
) -> None:
    implementation = _mapping(manifest, "implementation_files")
    runner = "scripts/run_causal_target_velocity_manifest.py"
    required = {
        runner,
        "hydra/production/causal_target_velocity_manifest.py",
        "hydra/mission/economic_evolution_manifest_runtime.py",
    }
    if not required.issubset(implementation):
        raise CausalTargetVelocityManifestError(
            "0028 implementation checksum closure is incomplete"
        )
    for relative, expected in implementation.items():
        candidate = (root / str(relative)).resolve()
        try:
            candidate.relative_to(root)
        except ValueError as exc:
            raise CausalTargetVelocityManifestError(
                "0028 implementation path escapes project root"
            ) from exc
        if _sha256(candidate) != str(expected):
            raise CausalTargetVelocityManifestError(
                f"0028 implementation checksum drift: {relative}"
            )


def _validate_terminal_baseline(
    manifest: Mapping[str, Any], root: Path
) -> None:
    terminal = _mapping(manifest, "terminal_predecessors")
    if (
        terminal.get("operating_package_v1")
        != "RETRACTED_DEVELOPMENT_EVIDENCE_CONTAMINATED"
        or terminal.get("causal_salvage_0027")
        != "CAUSAL_SALVAGE_GATE_FALSIFIED"
        or terminal.get("retry_former_six_books_allowed") is not False
        or terminal.get("inherit_economic_or_xfa_status") is not False
    ):
        raise CausalTargetVelocityManifestError("0028 terminal predecessor drift")

    baseline = _mapping(manifest, "clean_causal_baseline")
    exact = {
        "sleeves_evaluated": 18,
        "positive_stressed_sleeves_at_90d": 17,
        "normal_combine_passes": 0,
        "stressed_combine_passes": 0,
        "former_book_count": 6,
        "former_book_passes_all_horizons": 0,
        "observed_mll_breaches": 0,
    }
    if any(int(baseline.get(key, -1)) != value for key, value in exact.items()):
        raise CausalTargetVelocityManifestError("0028 clean baseline count drift")
    if (
        not _close(
            baseline.get(
                "best_former_book_full_horizon_stressed_median_target_progress"
            ),
            0.0656,
            5e-4,
        )
        or not _close(
            baseline.get("stressed_median_minimum_mll_buffer_usd"),
            4_396.70,
            0.02,
        )
        or not _close(
            baseline.get("worst_observed_minimum_mll_buffer_usd"),
            1_869.73,
            0.02,
        )
        or baseline.get("former_b4_pass_concentration_disappeared") is not True
        or baseline.get("all_former_economic_and_xfa_statuses_withdrawn") is not True
        or baseline.get("phase_b_closed") is not True
        or baseline.get("xfa_closed") is not True
        or baseline.get("forward_closed") is not True
        or baseline.get("component_status") != "LOW_VELOCITY_CAUSAL_COMPONENTS"
        or baseline.get("promotion_status") is not None
    ):
        raise CausalTargetVelocityManifestError("0028 clean baseline metric drift")
    _validate_hashed_sources(
        _mapping(baseline, "sources"),
        root,
        label="clean baseline",
    )


def _validate_causal_contract(manifest: Mapping[str, Any]) -> None:
    causal = _mapping(manifest, "causal_event_contract")
    timestamps = tuple(str(value) for value in causal.get("persist_timestamps") or ())
    required_timestamps = (
        "event_time",
        "available_at",
        "decision_time",
        "earliest_executable_time",
        "fill_time",
    )
    forbidden = set(str(value) for value in causal.get("forbidden_decision_inputs") or ())
    if (
        causal.get("kernel") != "SHARED_CAUSAL_DECISION_KERNEL"
        or causal.get("fill_policy") != "CAUSAL_NEXT_TRADABLE_OPEN_V1"
        or causal.get("availability_rule") != "AVAILABLE_AT_LTE_DECISION_TIME"
        or causal.get("future_outcomes_are_labels_only") is not True
        or causal.get("missing_future_coverage") != "CENSORED_FUTURE_COVERAGE"
        or causal.get("missing_future_suppresses_signal") is not False
        or causal.get("batch_streaming_decision_equality_required") is not True
        or timestamps != required_timestamps
        or not {
            "FUTURE_LABEL_AVAILABILITY",
            "NEGATIVE_SHIFT",
            "NEXT_BAR_AT_EARLIER_TIMESTAMP",
            "FUTURE_CONTINUITY",
            "CENTERED_ROLLING",
            "BACKWARD_FILL_ACROSS_DECISION_TIME",
        }.issubset(forbidden)
    ):
        raise CausalTargetVelocityManifestError("0028 causal event contract drift")


def _validate_risk_preflight(manifest: Mapping[str, Any]) -> None:
    risk = _mapping(manifest, "risk_frontier_preflight")
    gate = _mapping(risk, "success_gate")
    if (
        tuple(float(value) for value in risk.get("normalized_levels") or ())
        != (0.75, 1.0, 1.25, 1.5)
        or tuple(int(value) for value in risk.get("executable_micro_quantities") or ())
        != (3, 4, 5, 6)
        or int(risk.get("executable_reference_micro_quantity", -1)) != 4
        or int(risk.get("positive_sleeve_count", -1)) != 17
        or int(risk.get("diagnostic_former_book_count", -1)) != 6
        or float(risk.get("maximum_campaign_compute_fraction", 2.0)) > 0.10
        or risk.get("neighbor_multiplier_tuning_allowed") is not False
        or risk.get("proceed_to_discovery_after_failure") is not True
        or int(gate.get("minimum_normal_passes_out_of_48", -1)) != 3
        or int(gate.get("minimum_stressed_passes_out_of_48", -1)) != 2
        or gate.get("positive_stressed_net_required") is not True
        or not _close(gate.get("maximum_mll_breach_rate"), 0.10)
        or risk.get("failure_status") != "RISK_SCALE_ONLY_FALSIFIED"
    ):
        raise CausalTargetVelocityManifestError("0028 risk preflight drift")


def _validate_hazard_labels(manifest: Mapping[str, Any]) -> None:
    labels = _mapping(manifest, "target_before_adverse_labels")
    if (
        tuple(float(value) for value in labels.get("favorable_levels_r") or ())
        != (0.5, 1.0, 1.5, 2.0)
        or tuple(float(value) for value in labels.get("adverse_levels_r") or ())
        != (0.5, 0.75, 1.0)
        or tuple(str(value) for value in labels.get("horizons") or ())
        != ("5m", "15m", "30m", "60m", "SESSION", "OVERNIGHT")
        or labels.get("unrestricted_grid_allowed") is not False
        or labels.get("outcome_only") is not True
        or labels.get("censored_state") != "CENSORED_FUTURE_COVERAGE"
    ):
        raise CausalTargetVelocityManifestError("0028 hazard-label family drift")


def _validate_search_space(manifest: Mapping[str, Any]) -> None:
    search = _mapping(manifest, "search_space")
    markets = set(str(value) for value in search.get("markets") or ())
    reference_map = {
        str(key): str(value)
        for key, value in _mapping(search, "cross_asset_reference_map").items()
    }
    timeframes = set(str(value) for value in search.get("timeframes") or ())
    sessions = set(str(value) for value in search.get("sessions") or ())
    mechanisms = set(str(value) for value in search.get("mechanisms") or ())
    if (
        not markets
        or not markets.issubset(_ALLOWED_MARKETS)
        or set(reference_map) != markets
        or not set(reference_map.values()).issubset(markets)
        or any(market == reference for market, reference in reference_map.items())
        or not _REQUIRED_TIMEFRAMES.issubset(timeframes)
        or not _REQUIRED_SESSIONS.issubset(sessions)
        or mechanisms != _REQUIRED_MECHANISMS
        or search.get("cached_data_only") is not True
        or search.get("executable_sleeve_required") is not True
        or search.get("parameter_variants_independent") is not False
        or search.get("cemetery_resurrection_allowed") is not False
        or int(search.get("proposal_count", -1)) < 20_000
        or int(search.get("unique_event_screen_minimum", -1)) < 4_096
        or search.get("structural_deduplication") is not True
        or search.get("semantic_deduplication") is not True
        or search.get("behavioral_deduplication") is not True
    ):
        raise CausalTargetVelocityManifestError("0028 causal search-space drift")


def _validate_scale_and_coverage(manifest: Mapping[str, Any]) -> None:
    halving = _mapping(manifest, "successive_halving")
    expected = {
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
    }
    if any(int(halving.get(key, -1)) != value for key, value in expected.items()):
        raise CausalTargetVelocityManifestError("0028 successive-halving drift")
    if (
        int(halving.get("minimum_useful_sleeves_before_account_assembly", -1)) != 4
        or halving.get("mutation_or_retuning_between_stages") is not False
        or halving.get("xfa_before_clean_combine_survivors") is not False
    ):
        raise CausalTargetVelocityManifestError("0028 stage transition drift")

    coverage = _mapping(manifest, "evaluation_coverage")
    states = set(str(value) for value in coverage.get("reported_states") or ())
    if (
        int(coverage.get("headline_horizon_trading_days", -1)) != 90
        or coverage.get("headline_requires_complete_horizon") is not True
        or coverage.get("role_frozen_before_run") is not True
        or coverage.get("coverage_rule_enters_strategy_decision") is not False
        or coverage.get("overlapping_starts_role") != "SECONDARY_DIAGNOSTIC_ONLY"
        or coverage.get("manufacture_independence_when_under_48") is not False
        or not {
            "FULL_COVERAGE",
            "DATA_CENSORED",
            "HARD_FAILURE",
            "TARGET_REACHED",
            "MLL_BREACHED",
        }.issubset(states)
    ):
        raise CausalTargetVelocityManifestError("0028 evaluation coverage drift")
    blocks = list(_mapping(manifest, "temporal_blocks").get("blocks") or ())
    block_ids = {str(row.get("block_id") or "") for row in blocks if isinstance(row, Mapping)}
    if len(blocks) < 4 or len(block_ids) != len(blocks) or "" in block_ids:
        raise CausalTargetVelocityManifestError("0028 temporal-block contract drift")


def _validate_controls_objective_and_gates(manifest: Mapping[str, Any]) -> None:
    controls = _mapping(manifest, "matched_controls")
    strict_null_types = {
        "RANDOM_EVENT_TIMING",
        "DIRECTION_FLIPPED",
        "SESSION_MATCHED_NULL",
    }
    strict_dimensions = {
        "MARKET",
        "SESSION",
        "TIMEFRAME",
        "OPPORTUNITY_COUNT",
        "ACTIVE_DURATION",
        "AVERAGE_EXPOSURE",
        "COST_LEVEL",
    }
    clean_baseline = _mapping(
        controls, "clean_low_velocity_baseline_contract"
    )
    if (
        set(str(value) for value in controls.get("types") or ())
        != strict_null_types | {"CLEAN_LOW_VELOCITY_SLEEVE"}
        or "match_dimensions" in controls
        or set(
            str(value)
            for value in controls.get("strict_matched_null_types") or ()
        )
        != strict_null_types
        or set(
            str(value)
            for value in controls.get(
                "strict_matched_null_match_dimensions"
            )
            or ()
        )
        != strict_dimensions
        or clean_baseline.get("type") != "CLEAN_LOW_VELOCITY_SLEEVE"
        or clean_baseline.get("role")
        != "SAME_START_NEAREST_CELL_ECONOMIC_BASELINE_NOT_MATCHED_NULL"
        or clean_baseline.get("limitations_explicit") is not True
        or clean_baseline.get("strict_seven_dimension_match_required")
        is not False
        or clean_baseline.get("positive_stressed_velocity_delta_required")
        is not True
        or controls.get("full_suite_after_cheap_screen_only") is not True
    ):
        raise CausalTargetVelocityManifestError("0028 matched-control drift")

    objective = _mapping(manifest, "economic_objective")
    if (
        objective.get("ranking") != "TRANSPARENT_PARETO_FRONTIER"
        or objective.get("raw_aggregate_pnl_primary") is not False
        or objective.get("inactivity_rewarded") is not False
        or objective.get("opportunity_density_required") is not True
    ):
        raise CausalTargetVelocityManifestError("0028 economic objective drift")

    sleeve = _mapping(_mapping(manifest, "promotion_gates"), "sleeve_to_account_assembly")
    book = _mapping(_mapping(manifest, "promotion_gates"), "book_48_to_96")
    finalist = _mapping(_mapping(manifest, "promotion_gates"), "final_development")
    if (
        sleeve.get("positive_normal_and_stressed_economics") is not True
        or sleeve.get("higher_target_velocity_than_clean_baseline") is not True
        or int(sleeve.get("minimum_positive_blocks", -1)) != 2
        or sleeve.get("hard_causal_defect_allowed") is not False
        or int(book.get("minimum_normal_passes", -1)) != 3
        or int(book.get("minimum_stressed_passes", -1)) != 2
        or book.get("positive_stressed_net_required") is not True
        or not _close(book.get("maximum_mll_breach_rate"), 0.10)
        or int(book.get("minimum_contributing_blocks", -1)) != 2
        or not _close(finalist.get("minimum_normal_pass_rate"), 0.10)
        or not _close(finalist.get("minimum_stressed_pass_rate"), 0.05)
        or finalist.get("development_only") is not True
        or finalist.get("live_execution_allowed") is not False
    ):
        raise CausalTargetVelocityManifestError("0028 promotion-gate drift")

    failures = set(
        str(value)
        for value in _mapping(manifest, "failure_guided_mutation").get(
            "classifications"
        )
        or ()
    )
    if (
        failures != _REQUIRED_FAILURE_CLASSES
        or _mapping(manifest, "failure_guided_mutation").get(
            "blind_whole_strategy_mutation"
        )
        is not False
    ):
        raise CausalTargetVelocityManifestError("0028 failure taxonomy drift")


def _validate_compute_data_and_safety(manifest: Mapping[str, Any]) -> None:
    allocation = _mapping(manifest, "compute_allocation")
    compute = _mapping(manifest, "compute")
    data = _mapping(manifest, "data")
    governance = _mapping(manifest, "governance")
    costs = _mapping(manifest, "costs")
    account = _mapping(manifest, "account_parameters")
    if (
        float(allocation.get("economic_compute_minimum", -1.0)) < 0.85
        or float(allocation.get("integrity_persistence_maximum", 2.0)) > 0.10
        or float(allocation.get("reporting_maximum", 2.0)) > 0.05
        or int(compute.get("worker_count", -1)) != 3
        or int(compute.get("asynchronous_evidence_writer_count", -1)) != 1
        or compute.get("compute_workers_read_only") is not True
        or data.get("role") != "DEVELOPMENT_ONLY_Q4_EXCLUDED"
        or data.get("cached_data_only") is not True
        or data.get("new_purchase_allowed") is not False
        or data.get("q4_access_allowed") is not False
        or float(costs.get("normal_multiplier", 0.0)) != 1.0
        or float(costs.get("stressed_multiplier", 0.0)) != 1.5
        or costs.get("frozen_causal_cost_model") is not True
        or float(account.get("profit_target", 0.0)) != 9_000.0
        or float(account.get("maximum_loss_limit", 0.0)) != 4_500.0
        or int(account.get("maximum_mini_equivalent", 0)) != 15
    ):
        raise CausalTargetVelocityManifestError("0028 compute/data/account drift")
    forbidden = (
        "q4_access_allowed",
        "new_data_purchase_allowed",
        "broker_connection_allowed",
        "orders_allowed",
        "live_trading_allowed",
        "status_inheritance_allowed",
        "former_book_retry_allowed",
    )
    if (
        any(governance.get(key) is not False for key in forbidden)
        or governance.get("single_authoritative_mission_writer") is not True
        or governance.get("single_persistent_controller") is not True
    ):
        raise CausalTargetVelocityManifestError("0028 governance drift")


def _validate_evidence_contract(manifest: Mapping[str, Any]) -> None:
    evidence = _mapping(manifest, "evidence_bundle")
    expected_prefix = (
        "reports/economic_evolution/causal_target_velocity_0028_revision_01/"
        if _is_technical_kpi_revision(manifest)
        else "reports/economic_evolution/causal_target_velocity_0028/"
    )
    if (
        set(str(value) for value in evidence.get("required_datasets") or ())
        != set(REQUIRED_DATASETS)
        or evidence.get("required_for_campaign_complete") is not True
        or evidence.get("atomic_finalize") is not True
        or evidence.get("summary_only_complete_allowed") is not False
        or evidence.get("large_files_git_tracked") is not False
        or evidence.get("reconstruction_flag") is not False
        or evidence.get("destination") != "data/cache/evidence_bundles"
        or not str(evidence.get("lightweight_manifest_path") or "").startswith(
            expected_prefix
        )
    ):
        raise CausalTargetVelocityManifestError("0028 EvidenceBundle contract drift")


def _validate_runtime(manifest: Mapping[str, Any]) -> None:
    runtime = _mapping(manifest, "runtime")
    expected_output = (
        "reports/economic_evolution/causal_target_velocity_0028_revision_01"
        if _is_technical_kpi_revision(manifest)
        else "reports/economic_evolution/causal_target_velocity_0028"
    )
    if (
        runtime.get("engine") != CAUSAL_TARGET_VELOCITY_ENGINE
        or runtime.get("runner") != "scripts/run_causal_target_velocity_manifest.py"
        or runtime.get("output_dir") != expected_output
        or runtime.get("result_name") != "economic_production_result.json"
        or runtime.get("result_schema") != CAUSAL_TARGET_VELOCITY_RESULT_SCHEMA
        or runtime.get("controller_source_change_required") is not False
        or int(runtime.get("worker_count", -1)) != 3
        or int(runtime.get("asynchronous_evidence_writer_count", -1)) != 1
        or runtime.get("resume_from_checkpoint") is not True
    ):
        raise CausalTargetVelocityManifestError("0028 runtime declaration drift")


def _is_technical_kpi_revision(manifest: Mapping[str, Any]) -> bool:
    repair = manifest.get("technical_repair")
    return bool(
        manifest.get("revision_id")
        == "hydra_causal_target_velocity_0028_revision_01"
        and isinstance(repair, Mapping)
        and repair.get("classification")
        == "TECHNICAL_STAGE3_KPI_INVALID_ROW_AGGREGATION_DEFECT"
        and repair.get("economic_semantics_changed") is False
        and repair.get("scientific_hypothesis_changed") is False
        and repair.get("population_or_selection_changed") is False
        and repair.get("risk_threshold_or_control_changed") is False
        and repair.get("completed_evidence_recomputed") is False
        and repair.get("completed_stage3_batch_reused_unchanged") is True
        and repair.get("new_multiplicity_reservation_required") is False
        and repair.get("supersedes_output_dir")
        == "reports/economic_evolution/causal_target_velocity_0028"
        and repair.get("revision_output_dir")
        == "reports/economic_evolution/causal_target_velocity_0028_revision_01"
        and repair.get("preserved_preflight_path")
        == (
            "reports/economic_evolution/causal_target_velocity_0028/"
            "preflight/risk_frontier_preflight_result.json"
        )
    )


def _validate_technical_repair(
    manifest: Mapping[str, Any], root: Path
) -> None:
    repair = manifest.get("technical_repair")
    if repair is None:
        if manifest.get("revision_id") is not None:
            raise CausalTargetVelocityManifestError(
                "0028 revision lacks a technical-repair contract"
            )
        return
    if not _is_technical_kpi_revision(manifest) or not isinstance(repair, Mapping):
        raise CausalTargetVelocityManifestError("0028 technical repair drift")
    receipt_ref = repair.get("repair_receipt")
    if not isinstance(receipt_ref, Mapping):
        raise CausalTargetVelocityManifestError("0028 repair receipt missing")
    receipt_path = (root / str(receipt_ref.get("path") or "")).resolve()
    try:
        receipt_path.relative_to(root)
    except ValueError as exc:
        raise CausalTargetVelocityManifestError(
            "0028 repair receipt escapes root"
        ) from exc
    receipt = _load_json(receipt_path)
    claimed = receipt.get("repair_record_hash")
    receipt_core = dict(receipt)
    receipt_core.pop("repair_record_hash", None)
    if (
        repair.get("supersedes_manifest_hash")
        != "23b24c50c2f71a6bce87fe88b22df7ab2a0177700a0c618924911c35c405c27d"
        or repair.get("supersedes_manifest_file_sha256")
        != "2329999422838d63210345f11d274c9b03986113e3eeca889a17ac086033880d"
        or repair.get("repair_commit") != manifest.get("source_commit")
        or receipt_ref.get("file_sha256") != _sha256(receipt_path)
        or receipt_ref.get("repair_record_hash") != claimed
        or claimed != stable_hash(receipt_core)
        or receipt.get("classification") != repair.get("classification")
        or receipt.get("scientific_status")
        != "NO_ECONOMIC_SEMANTICS_CHANGE"
        or receipt.get("multiplicity", {}).get("multiplicity_delta") != 0
    ):
        raise CausalTargetVelocityManifestError("0028 repair provenance drift")


def _validate_multiplicity(manifest: Mapping[str, Any]) -> None:
    multiplicity = _mapping(manifest, "multiplicity")
    prior = int(multiplicity.get("prior_global_N_trials", -1))
    delta = int(multiplicity.get("reserved_delta_trials", -1))
    expected = int(multiplicity.get("expected_global_N_trials_after_reservation", -1))
    if (
        prior < 0
        or delta < 20_000
        or prior + delta != expected
        or int(multiplicity.get("prospective_comparisons", -1)) < 20_000
        or multiplicity.get("reservation_required_before_outcome_access") is not True
        or multiplicity.get("proof_window_consumed") is not False
    ):
        raise CausalTargetVelocityManifestError("0028 multiplicity drift")


def _validate_hashed_sources(
    sources: Mapping[str, Any], root: Path, *, label: str
) -> None:
    if not sources:
        raise CausalTargetVelocityManifestError(f"0028 {label} sources missing")
    for name, raw in sources.items():
        if not isinstance(raw, Mapping):
            raise CausalTargetVelocityManifestError(
                f"0028 {label} source invalid: {name}"
            )
        relative = str(raw.get("path") or "")
        expected = str(raw.get("file_sha256") or "")
        candidate = (root / relative).resolve()
        try:
            candidate.relative_to(root)
        except ValueError as exc:
            raise CausalTargetVelocityManifestError(
                f"0028 {label} source escapes root: {name}"
            ) from exc
        if _sha256(candidate) != expected:
            raise CausalTargetVelocityManifestError(
                f"0028 {label} source checksum drift: {name}"
            )


def _project_root(path: Path) -> Path:
    for parent in (path.parent, *path.parents):
        if (parent / "MISSION_CONTRACT.md").is_file():
            return parent
    raise CausalTargetVelocityManifestError("project root not found")


def _mapping(value: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    item = value.get(key)
    if not isinstance(item, Mapping):
        raise CausalTargetVelocityManifestError(f"0028 mapping missing: {key}")
    return item


def _close(value: Any, expected: float, tolerance: float = 1e-12) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(float(value))
        and abs(float(value) - expected) <= tolerance
    )


def _load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CausalTargetVelocityManifestError(f"invalid JSON: {path}") from exc
    if not isinstance(value, dict):
        raise CausalTargetVelocityManifestError(f"expected JSON object: {path}")
    return value


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1 << 20), b""):
                digest.update(chunk)
    except OSError as exc:
        raise CausalTargetVelocityManifestError(
            f"0028 referenced file missing: {path}"
        ) from exc
    return digest.hexdigest()


__all__ = [
    "CAUSAL_TARGET_VELOCITY_CAMPAIGN_ID",
    "CAUSAL_TARGET_VELOCITY_CLASS_ID",
    "CAUSAL_TARGET_VELOCITY_ENGINE",
    "CAUSAL_TARGET_VELOCITY_MANIFEST_SCHEMA",
    "CAUSAL_TARGET_VELOCITY_RESULT_SCHEMA",
    "CausalTargetVelocityManifestError",
    "load_and_validate_causal_target_velocity_manifest",
]
