"""Fail-closed manifest contract for HYDRA FAST-PASS FACTORY 0029.

The fast-pass factory is a campaign mode of the existing stable production
kernel.  This module validates the scientific and operational envelope only;
it neither reserves multiplicity nor writes mission state.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from hydra.evidence import REQUIRED_DATASETS
from hydra.economic_evolution.schema import stable_hash
from hydra.propfirm.combine_to_xfa import official_rule_snapshot_2026_07_15


FAST_PASS_CAMPAIGN_MODE = "FAST_PASS_FACTORY"
FAST_PASS_CAMPAIGN_ID = "hydra_fast_pass_factory_0029"
FAST_PASS_CLASS_ID = "FIVE_DAY_COMBINE_QUALITY_DIVERSITY_MARGINAL_BOOK_V1"
FAST_PASS_RUNTIME_VERSION = "hydra_fast_pass_factory_runtime_v1"
FAST_PASS_TECHNICAL_REVISION_ID = "hydra_fast_pass_factory_0029_revision_01"
FAST_PASS_TECHNICAL_REVISION_02_ID = "hydra_fast_pass_factory_0029_revision_02"
FAST_PASS_TECHNICAL_REVISION_03_ID = "hydra_fast_pass_factory_0029_revision_03"
FAST_PASS_ORIGINAL_MANIFEST_HASH = (
    "47465e3c7ee39c76660fb57b83db709c799d11ba22b1a49b9cac01dd437a31ec"
)
FAST_PASS_ORIGINAL_MANIFEST_FILE_SHA256 = (
    "6ea725cf5538efa50c8b0e8ab5be8b80fdea1674f0f8e08fb556284e849f531c"
)
FAST_PASS_WINDOWS = (5, 10, 20)
FAST_PASS_REQUIRED_IMPLEMENTATION_FILES = frozenset(
    {
        "hydra/production/fast_pass_manifest.py",
        "hydra/production/fast_pass_runtime.py",
        "hydra/production/fast_pass_runtime_helpers.py",
        "hydra/production/manifest.py",
        "hydra/production/runtime.py",
        "hydra/portfolio/marginal_contribution_builder.py",
        "hydra/research/causal_target_velocity.py",
        "hydra/research/causal_sleeve_replay.py",
        "hydra/account_policy/causal_active_pool_replay.py",
        "hydra/account_policy/active_risk_pool.py",
        "hydra/evidence/causal_target_velocity_adapter.py",
        "scripts/run_economic_production_manifest.py",
    }
)

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_GIT_SHA = re.compile(r"^[0-9a-f]{40}$")
_COVERAGE_STATES = frozenset(
    {
        "FULL_COVERAGE",
        "TARGET_REACHED",
        "MLL_BREACHED",
        "HARD_RULE_FAILURE",
        "DATA_CENSORED",
    }
)
_QD_DIMENSIONS = frozenset(
    {
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
    }
)
_QD_OVERLAPS = frozenset(
    {"SIGNAL", "TRADE", "DAILY_PNL", "LOSS_DAY"}
)
_RESEARCH_LANES = frozenset(
    {"SYMBOLIC_RULE", "HAZARD_MODEL", "EVENT_TIME", "CROSS_ASSET"}
)
_ALLOWED_HAZARD_MODELS = frozenset(
    {
        "REGULARIZED_LOGISTIC_REGRESSION",
        "SHALLOW_DECISION_TREE",
        "MONOTONIC_GRADIENT_BOOSTED_TREE",
        "DISCRETE_COMPETING_RISK",
    }
)
_PROGRESSIVE_CONTROLS = {
    "LEVEL_0": frozenset({"DUPLICATE_CHECK", "CAUSAL_CHECK"}),
    "LEVEL_1": frozenset(
        {"DIRECTION_FLIP", "RANDOM_TIMING", "SESSION_MATCHED_NULL"}
    ),
    "LEVEL_2": frozenset(
        {
            "EXPOSURE_MATCHED_RANDOM",
            "BEST_PARENT",
            "MARGINAL_CONTRIBUTION",
        }
    ),
    "LEVEL_3": frozenset(
        {
            "TEMPORAL_CROSSFIT",
            "COMPLETE_MATCHED_CONTROLS",
            "BLOCK_CONCENTRATION",
            "EVIDENCE_BUNDLE_RECONCILIATION",
        }
    ),
}


class FastPassManifestError(RuntimeError):
    """The 0029 manifest is incomplete, unsafe, or no longer immutable."""


def validate_fast_pass_manifest(
    manifest: Mapping[str, Any], *, manifest_path: str | Path
) -> None:
    """Validate the frozen 0029 contract without mutating durable state."""

    path = Path(manifest_path).resolve()
    root = _project_root(path)
    _validate_identity(manifest)
    _validate_implementation(manifest, root)
    _validate_technical_repair(manifest, root)
    _validate_terminal_baseline(manifest, root)
    _validate_runtime(manifest, root)
    _validate_data_and_governance(manifest)
    _validate_population_inputs(manifest)
    _validate_account_snapshot(manifest)
    _validate_evaluation_grid(manifest)
    _validate_bank_and_diversity(manifest)
    _validate_research_lanes(manifest)
    _validate_risk_governor(manifest)
    _validate_book_builder(manifest)
    _validate_waves_and_halving(manifest)
    _validate_controls(manifest)
    _validate_promotion(manifest)
    _validate_microstructure(manifest)
    _validate_compute(manifest)
    _validate_evidence(manifest)
    _validate_multiplicity(manifest)


def _validate_identity(manifest: Mapping[str, Any]) -> None:
    campaign_id = str(manifest.get("campaign_id") or "")
    created = str(manifest.get("created_at_utc") or "")
    if (
        manifest.get("schema") != "hydra_economic_production_manifest_v1"
        or campaign_id != FAST_PASS_CAMPAIGN_ID
        or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", campaign_id)
        or manifest.get("campaign_mode") != FAST_PASS_CAMPAIGN_MODE
        or manifest.get("class_id") != FAST_PASS_CLASS_ID
        or tuple(manifest.get("policy_classes") or ()) != (FAST_PASS_CLASS_ID,)
        or manifest.get("development_only") is not True
        or not _GIT_SHA.fullmatch(str(manifest.get("source_commit") or ""))
        or not created
        or not str(manifest.get("economic_hypothesis") or "").strip()
    ):
        raise FastPassManifestError("fast-pass campaign identity drift")
    try:
        frozen_at = datetime.fromisoformat(created.replace("Z", "+00:00"))
    except ValueError as exc:
        raise FastPassManifestError("fast-pass freeze timestamp is invalid") from exc
    if frozen_at.tzinfo is None:
        raise FastPassManifestError("fast-pass freeze timestamp must be UTC-aware")


def _validate_implementation(manifest: Mapping[str, Any], root: Path) -> None:
    implementation = _mapping(manifest, "implementation_files")
    declared = {str(value) for value in implementation}
    missing = FAST_PASS_REQUIRED_IMPLEMENTATION_FILES - declared
    if missing:
        raise FastPassManifestError(
            "fast-pass implementation checksum closure is incomplete: "
            + ", ".join(sorted(missing))
        )
    for raw_relative, raw_claimed in implementation.items():
        relative = str(raw_relative)
        if not relative or Path(relative).is_absolute():
            raise FastPassManifestError("fast-pass implementation path is unsafe")
        target = (root / relative).resolve()
        if target == root or root not in target.parents:
            raise FastPassManifestError(
                f"fast-pass implementation path escapes root: {relative}"
            )
        claimed = str(raw_claimed or "")
        if not _SHA256.fullmatch(claimed) or _sha256(target) != claimed:
            raise FastPassManifestError(
                f"fast-pass implementation checksum drift: {relative}"
            )


def _validate_terminal_baseline(
    manifest: Mapping[str, Any], root: Path
) -> None:
    baseline = _mapping(manifest, "terminal_baseline_0028")
    if (
        baseline.get("status")
        != "CAUSAL_TARGET_VELOCITY_INCONCLUSIVE_COVERAGE_LIMITED"
        or baseline.get("risk_scale_status") != "RISK_SCALE_ONLY_FALSIFIED"
        or int(baseline.get("preserved_sleeve_count", -1)) != 47
        or int(baseline.get("exact_causal_sleeve_replays", -1)) != 206
        or int(baseline.get("stage2_stressed_positive_sleeves", -1)) != 178
        or int(baseline.get("normal_combine_passes_90d", -1)) != 0
        or int(baseline.get("stressed_combine_passes_90d", -1)) != 0
        or int(baseline.get("observed_mll_breaches", -1)) != 0
        or int(baseline.get("fully_covered_stage4_starts", -1)) != 11
        or baseline.get("active_assembly_degraded_best_sleeves") is not True
        or baseline.get("retry_or_retune_allowed") is not False
        or baseline.get("reference_bank_status")
        != "LOW_VELOCITY_CAUSAL_REFERENCE_BANK"
        or baseline.get("promotion_status") is not None
        or baseline.get("xfa_path_exists") is not False
        or baseline.get("forward_package_exists") is not False
    ):
        raise FastPassManifestError("fast-pass terminal 0028 baseline drift")
    progress = float(baseline.get("stage3_median_stressed_target_progress", -1.0))
    if not math.isfinite(progress) or not math.isclose(
        progress, 0.01, abs_tol=0.002
    ):
        raise FastPassManifestError("fast-pass 0028 target-progress baseline drift")
    _validate_hashed_references(_mapping(baseline, "sources"), root)


def _validate_runtime(manifest: Mapping[str, Any], root: Path) -> None:
    runtime = _mapping(manifest, "runtime")
    output = (root / str(runtime.get("output_dir") or "")).resolve()
    result_name = str(runtime.get("result_name") or "")
    if (
        runtime.get("engine") != "production_kernel_v1"
        or runtime.get("runner") != "scripts/run_economic_production_manifest.py"
        or runtime.get("fast_pass_runtime_version") != FAST_PASS_RUNTIME_VERSION
        or runtime.get("result_schema") != "hydra_economic_production_result_v1"
        or output != root / _technical_revision_output(manifest)
        or Path(result_name).name != result_name
        or result_name != "economic_production_result.json"
        or runtime.get("controller_source_change_required") is not False
        or runtime.get("resume_from_checkpoint") is not True
        or int(runtime.get("worker_count", -1)) != 3
        or int(runtime.get("asynchronous_evidence_writer_count", -1)) != 1
    ):
        raise FastPassManifestError("stable fast-pass runtime declaration drift")


def _validate_data_and_governance(manifest: Mapping[str, Any]) -> None:
    data = _mapping(manifest, "data")
    if (
        data.get("role") != "DEVELOPMENT_ONLY_Q4_EXCLUDED"
        or data.get("cached_features_only_initial_waves") is not True
        or data.get("q4_access_allowed") is not False
        or data.get("new_purchase_allowed_initial_waves") is not False
        or not _SHA256.fullmatch(str(data.get("feature_source_fingerprint") or ""))
        or not _SHA256.fullmatch(str(data.get("contract_map_sha256") or ""))
    ):
        raise FastPassManifestError("fast-pass development-data contract drift")
    governance = _mapping(manifest, "governance")
    for key in (
        "live_trading_allowed",
        "broker_connection_allowed",
        "orders_allowed",
        "q4_access_allowed",
        "new_mission_allowed",
        "new_service_allowed",
        "new_database_allowed",
        "new_registry_writer_allowed",
        "controller_version_change_required",
        "causality_weakening_allowed",
        "promotion_gate_lowering_after_results_allowed",
        "xfa_before_combine_graduates_allowed",
    ):
        if governance.get(key) is not False:
            raise FastPassManifestError(f"unsafe fast-pass authority: {key}")

    budget = _mapping(manifest, "budget")
    try:
        spent = float(budget["actual_spend_usd"])
        cap = float(budget["hard_cap_usd"])
        remaining = float(budget["remaining_usd"])
    except (KeyError, TypeError, ValueError) as exc:
        raise FastPassManifestError("invalid fast-pass budget declaration") from exc
    if (
        not all(math.isfinite(value) and value >= 0.0 for value in (spent, cap, remaining))
        or not math.isclose(spent + remaining, cap, abs_tol=1e-8)
        or int(budget.get("new_data_purchase_count", -1)) != 0
        or float(budget.get("protected_reserve_usd", -1.0)) < 25.0
    ):
        raise FastPassManifestError("fast-pass protected-budget drift")


def _validate_population_inputs(manifest: Mapping[str, Any]) -> None:
    allowed_markets = {"CL", "ES", "GC", "NQ", "RTY", "YM"}
    markets = tuple(str(value) for value in manifest.get("markets") or ())
    if (
        len(markets) < 3
        or len(set(markets)) != len(markets)
        or not set(markets).issubset(allowed_markets)
    ):
        raise FastPassManifestError("fast-pass cached-market universe drift")
    reference = _mapping(manifest, "reference_bank")
    if (
        reference.get("status") != "LOW_VELOCITY_CAUSAL_REFERENCE_BANK"
        or int(reference.get("preserved_sleeve_count", -1)) != 47
        or int(reference.get("prior_stage1_candidate_count", -1)) < 4_096
        or reference.get("promotion_status") is not None
        or reference.get("baseline_or_parent_role_only") is not True
    ):
        raise FastPassManifestError("fast-pass reference-bank drift")
    temporal = _mapping(manifest, "temporal_blocks")
    blocks = list(temporal.get("blocks") or ())
    block_ids = [str(row.get("block_id") or "") for row in blocks if isinstance(row, Mapping)]
    if (
        len(blocks) < 4
        or len(block_ids) != len(blocks)
        or len(set(block_ids)) != len(block_ids)
        or "" in block_ids
        or temporal.get("overlapping_starts_independent") is not False
        or any(
            not str(row.get("start") or "") or not str(row.get("end") or "")
            for row in blocks
        )
    ):
        raise FastPassManifestError("fast-pass temporal-block contract drift")


def _validate_account_snapshot(manifest: Mapping[str, Any]) -> None:
    account = _mapping(manifest, "account_rule_snapshot")
    official = official_rule_snapshot_2026_07_15()
    exact = {
        "account_size_usd": 150_000.0,
        "profit_target_usd": 9_000.0,
        "maximum_loss_limit_usd": 4_500.0,
        "best_day_consistency_fraction": 0.50,
        "maximum_mini_contracts": 15,
        "maximum_micro_contracts": 150,
    }
    for key, expected in exact.items():
        actual = account.get(key)
        if not isinstance(actual, (int, float)) or isinstance(actual, bool) or not math.isclose(
            float(actual), expected
        ):
            raise FastPassManifestError(f"fast-pass account snapshot drift: {key}")
    if (
        account.get("derive_risk_limits_from_snapshot") is not True
        or account.get("hardcode_alternate_account_in_strategy") is not False
        or not str(account.get("rule_snapshot_version") or "").strip()
        or not _SHA256.fullmatch(str(account.get("rule_snapshot_hash") or ""))
        or not str(account.get("session_close_rule") or "").strip()
        or not isinstance(account.get("optional_dll"), Mapping)
        or not isinstance(account.get("costs_and_slippage"), Mapping)
        or account.get("rule_snapshot_version") != official.rule_version
        or account.get("rule_snapshot_hash") != official.fingerprint
        or account.get("official_snapshot") != official.to_dict()
    ):
        raise FastPassManifestError("fast-pass account rule provenance drift")
    speed = _mapping(manifest, "account_speed_objective")
    if (
        int(speed.get("primary_trading_days", -1)) != 5
        or not math.isclose(float(speed.get("target_profit_per_day_usd", -1.0)), 1800.0)
        or not math.isclose(float(speed.get("target_to_mll_ratio", -1.0)), 2.0)
        or speed.get("five_days_is_optimization_not_promotion_fabrication") is not True
    ):
        raise FastPassManifestError("fast-pass account-speed objective drift")


def _validate_evaluation_grid(manifest: Mapping[str, Any]) -> None:
    windows = _mapping(manifest, "evaluation_grid")
    if (
        tuple(int(value) for value in windows.get("primary_headline_days") or ())
        != (5,)
        or tuple(int(value) for value in windows.get("secondary_headline_days") or ())
        != (10, 20)
        or tuple(int(value) for value in windows.get("diagnostic_rolling_days") or ())
        != FAST_PASS_WINDOWS
        or tuple(int(value) for value in windows.get("late_stage_survival_days") or ())
        != (40, 60, 90)
        or windows.get("headline_non_overlapping") is not True
        or windows.get("headline_full_coverage_required") is not True
        or windows.get("starts_and_roles_frozen_before_outcomes") is not True
        or windows.get("overlapping_starts_independent") is not False
        or set(str(value) for value in windows.get("states") or ())
        != _COVERAGE_STATES
    ):
        raise FastPassManifestError("fast-pass evaluation-window contract drift")
    starts = _mapping(windows, "headline_starts")
    if dict(windows.get("block_roles") or {}) != {
        "B1": "DESIGN",
        "B2": "DESIGN",
        "B3": "HELD_OUT_DEVELOPMENT",
        "B4": "HELD_OUT_DEVELOPMENT",
    }:
        raise FastPassManifestError("fast-pass frozen block-role drift")
    for horizon in FAST_PASS_WINDOWS:
        rows = starts.get(str(horizon))
        if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes)) or not rows:
            raise FastPassManifestError(
                f"fast-pass {horizon}d headline starts are missing"
            )
        identities: set[tuple[int, str]] = set()
        for raw in rows:
            if not isinstance(raw, Mapping):
                raise FastPassManifestError("fast-pass headline start is invalid")
            try:
                identity = (int(raw["session_day"]), str(raw["temporal_block"]))
            except (KeyError, TypeError, ValueError) as exc:
                raise FastPassManifestError(
                    "fast-pass headline start identity is invalid"
                ) from exc
            if identity in identities or not identity[1]:
                raise FastPassManifestError(
                    "fast-pass headline starts are not unique"
                )
            identities.add(identity)
    if not _SHA256.fullmatch(str(windows.get("grid_hash") or "")):
        raise FastPassManifestError("fast-pass frozen coverage-grid hash drift")


def _validate_bank_and_diversity(manifest: Mapping[str, Any]) -> None:
    banks = _mapping(manifest, "bank_architecture")
    expected = {
        "causal_executable_capacity": 150,
        "fast_5d_capacity": 50,
        "balanced_10d_capacity": 50,
        "robust_20d_capacity": 50,
        "graduated_minimum_target": 10,
        "graduated_maximum_target": 20,
        "forward_minimum_target": 3,
        "forward_maximum_target": 5,
    }
    if any(int(banks.get(key, -1)) != value for key, value in expected.items()):
        raise FastPassManifestError("fast-pass bank-capacity contract drift")
    qd = _mapping(manifest, "quality_diversity")
    if (
        set(str(value) for value in qd.get("cell_dimensions") or ())
        != _QD_DIMENSIONS
        or set(str(value) for value in qd.get("persist_pairwise_overlap") or ())
        != _QD_OVERLAPS
        or int(qd.get("maximum_close_parameter_variants_per_cell", -1)) != 3
        or qd.get("exact_trade_path_clone_allowed") is not False
        or qd.get("execution_equivalent_clone_allowed") is not False
        or float(qd.get("novel_capacity_fraction_minimum", -1.0)) < 0.20
        or not 0.10
        <= float(qd.get("weak_archive_replacement_fraction_minimum", -1.0))
        <= float(qd.get("weak_archive_replacement_fraction_maximum", -1.0))
        <= 0.20
    ):
        raise FastPassManifestError("fast-pass quality-diversity contract drift")


def _validate_research_lanes(manifest: Mapping[str, Any]) -> None:
    lanes = _mapping(manifest, "research_lanes")
    if set(str(value) for value in lanes.get("enabled") or ()) != _RESEARCH_LANES:
        raise FastPassManifestError("fast-pass research lanes drift")
    hazard = _mapping(lanes, "hazard_model")
    if (
        set(str(value) for value in hazard.get("allowed_models") or ())
        != _ALLOWED_HAZARD_MODELS
        or hazard.get("strict_cross_fit_required") is not True
        or hazard.get("future_labels_in_inference_features") is not False
        or hazard.get("availability_rule") != "AVAILABLE_AT_LTE_DECISION_TIME"
        or hazard.get("executable_export_required") is not True
        or hazard.get("deterministic_hash_required") is not True
    ):
        raise FastPassManifestError("fast-pass hazard-model contract drift")
    event = _mapping(lanes, "event_time")
    if not {
        "VOLUME_BARS",
        "DOLLAR_BARS",
        "IMBALANCE_BARS",
        "VOLATILITY_EVENT_BARS",
        "SESSION_EVENT_BARS",
    }.issubset(set(str(value) for value in event.get("representations") or ())):
        raise FastPassManifestError("fast-pass event-time lane drift")


def _validate_risk_governor(manifest: Mapping[str, Any]) -> None:
    risk = _mapping(manifest, "risk_governor")
    if (
        tuple(float(value) for value in risk.get("signal_quality_tiers") or ())
        != (0.0, 0.5, 1.0, 1.5, 2.0)
        or tuple(int(value) for value in risk.get("maximum_concurrent_sleeves") or ())
        != (1, 2, 3, 4)
        or not _small_positive_frontier(risk.get("open_risk_ceiling_mll_buffer_fractions"))
        or not _small_positive_frontier(risk.get("daily_loss_budget_mll_fractions"))
        or not _small_positive_frontier(risk.get("daily_profit_lock_consistency_fractions"))
        or risk.get("inactive_sleeves_reserve_risk") is not False
        or risk.get("contract_limits_are_hard") is not True
        or risk.get("continuous_optimization_allowed") is not False
        or risk.get("future_episode_outcome_adaptation_allowed") is not False
        or risk.get("freeze_before_held_out_evaluation") is not True
    ):
        raise FastPassManifestError("fast-pass bounded risk-governor drift")
    profiles = list(risk.get("frozen_profiles") or ())
    if (
        len(profiles) != 4
        or len({str(row.get("profile_id") or "") for row in profiles}) != 4
        or {int(row.get("maximum_concurrent_sleeves", 0)) for row in profiles}
        != {1, 2, 3, 4}
        or any(
            tuple(float(value) for value in row.get("signal_quality_tiers") or ())
            != (0.0, 0.5, 1.0, 1.5, 2.0)
            or row.get("same_instrument_conflict_policy") != "priority"
            for row in profiles
        )
        or str(risk.get("frozen_profiles_hash") or "") != stable_hash(profiles)
        or risk.get("executable_quality_quantization")
        != "CAUSAL_EVENT_IDENTITY_HASH_INTEGER_MICRO_QUANTIZATION_V1"
    ):
        raise FastPassManifestError("fast-pass governor profiles are not frozen")


def _validate_book_builder(manifest: Mapping[str, Any]) -> None:
    builder = _mapping(manifest, "marginal_contribution_book_builder")
    required_improvements = {
        "FIVE_DAY_PASS_RATE",
        "TEN_DAY_PASS_RATE",
        "LOWER_QUARTILE_TARGET_PROGRESS",
        "MLL_SURVIVAL",
        "CONSISTENCY",
    }
    required_baselines = {
        "BEST_COMPONENT_SLEEVE",
        "PRECEDING_SMALLER_BOOK",
        "EQUAL_RISK_POOLING",
        "EXPOSURE_MATCHED_RANDOM_ASSEMBLY",
    }
    if (
        int(builder.get("minimum_exact_books_when_sleeves_sufficient", -1)) < 1_000
        or int(builder.get("maximum_sleeves_per_book", -1)) != 6
        or builder.get("start_with_one_high_velocity_sleeve") is not True
        or builder.get("held_out_development_evidence_required") is not True
        or set(str(value) for value in builder.get("addition_improvement_metrics") or ())
        != required_improvements
        or builder.get("material_degradation_allowed") is not False
        or builder.get("remove_negative_marginal_contributors") is not True
        or set(str(value) for value in builder.get("baselines") or ())
        != required_baselines
        or builder.get("larger_book_automatic_advantage") is not False
        or str(builder.get("search_method") or "")
        not in {"CONSTRAINED_BEAM", "PARETO_BEAM", "MIXED_INTEGER_PRECOMPUTED"}
    ):
        raise FastPassManifestError("fast-pass marginal book-builder drift")


def _validate_waves_and_halving(manifest: Mapping[str, Any]) -> None:
    waves = _mapping(manifest, "waves")
    exact = {
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
    }
    if any(int(waves.get(key, -1)) != value for key, value in exact.items()):
        raise FastPassManifestError("fast-pass wave scale drift")
    if (
        waves.get("continuous_waves") is not True
        or waves.get("failure_guided_generation") is not True
        or waves.get("reuse_qualified_candidates_across_waves") is not True
        or waves.get("restart_bank_from_zero") is not False
        or waves.get("xfa_before_combine_graduates") is not False
    ):
        raise FastPassManifestError("fast-pass wave lifecycle drift")


def _validate_controls(manifest: Mapping[str, Any]) -> None:
    controls = _mapping(manifest, "progressive_controls")
    for level, required in _PROGRESSIVE_CONTROLS.items():
        if set(str(value) for value in controls.get(level) or ()) != required:
            raise FastPassManifestError(f"fast-pass {level} controls drift")
    if (
        not math.isclose(float(controls.get("level1_top_fraction", -1.0)), 0.10)
        or controls.get("universal_full_controls_allowed") is not False
        or controls.get("worm_receipts_for_weak_candidates") is not False
        or controls.get("one_batch_archive_per_wave") is not True
    ):
        raise FastPassManifestError("fast-pass progressive-control policy drift")
    tolerances = _mapping(controls, "exposure_match_tolerances")
    expected = {
        "mean_daily_contract_utilization_absolute": 0.05,
        "maximum_mini_equivalent_mean_relative": 0.25,
        "accepted_event_count_relative": 0.25,
    }
    if any(
        not math.isclose(float(tolerances.get(key, -1.0)), value)
        for key, value in expected.items()
    ):
        raise FastPassManifestError("fast-pass exposure-match tolerance drift")


def _validate_promotion(manifest: Mapping[str, Any]) -> None:
    promotion = _mapping(manifest, "promotion_gates")
    executable = _mapping(promotion, "causal_executable_bank")
    fast = _mapping(promotion, "fast_5d_bank")
    graduated = _mapping(promotion, "graduated_book")
    strong = _mapping(promotion, "strong_sprint_book")
    if (
        executable.get("complete_causal_execution_required") is not True
        or int(executable.get("positive_stressed_development_blocks_minimum", -1)) != 2
        or executable.get("defensive_role_exception_requires_demonstration") is not True
        or executable.get("behavioral_uniqueness_required") is not True
        or int(fast.get("full_coverage_normal_passes_minimum", -1)) != 1
        or fast.get("positive_stressed_five_day_economics_required") is not True
        or fast.get("single_trade_domination_allowed") is not False
        or not math.isclose(float(graduated.get("normal_5d_pass_rate_minimum", -1)), 0.05)
        or not math.isclose(float(graduated.get("stressed_5d_pass_rate_minimum", -1)), 0.02)
        or not math.isclose(float(graduated.get("normal_10d_pass_rate_minimum", -1)), 0.10)
        or not math.isclose(float(graduated.get("stressed_10d_pass_rate_minimum", -1)), 0.05)
        or not math.isclose(float(graduated.get("mll_breach_rate_maximum", -1)), 0.10)
        or int(graduated.get("independent_blocks_with_passes_minimum", -1)) != 2
        or graduated.get("positive_stressed_economics_required") is not True
        or graduated.get("consistency_compliance_required") is not True
        or graduated.get("single_sleeve_or_day_domination_allowed") is not False
        or not math.isclose(float(strong.get("normal_5d_pass_rate_minimum", -1)), 0.10)
        or not math.isclose(float(strong.get("stressed_5d_pass_rate_minimum", -1)), 0.05)
        or strong.get("positive_lower_quartile_target_progress_required") is not True
        or promotion.get("lower_after_observing_results") is not False
        or promotion.get("development_only") is not True
    ):
        raise FastPassManifestError("fast-pass frozen promotion gates drift")


def _validate_microstructure(manifest: Mapping[str, Any]) -> None:
    escalation = _mapping(manifest, "microstructure_escalation")
    if (
        int(escalation.get("complete_ohlcv_waves_before_decision", -1)) != 2
        or int(escalation.get("trigger_fast_5d_bank_below", -1)) != 20
        or escalation.get("trigger_when_no_stressed_five_day_pass") is not True
        or escalation.get("exact_cost_estimate_required_before_purchase") is not True
        or float(escalation.get("minimum_budget_reserve_usd", -1.0)) != 25.0
        or float(escalation.get("maximum_initial_spend_usd", -1.0)) != 10.0
        or escalation.get("broad_purchase_allowed") is not False
        or escalation.get("q4_access_allowed") is not False
        or not {"TRADES", "MBP_1", "TOP_OF_BOOK", "AGGRESSOR_SIDE_FLOW"}.issubset(
            set(str(value) for value in escalation.get("candidate_schemas") or ())
        )
    ):
        raise FastPassManifestError("fast-pass microstructure escalation drift")


def _validate_compute(manifest: Mapping[str, Any]) -> None:
    compute = _mapping(manifest, "compute_allocation")
    if (
        float(compute.get("economic_wall_clock_minimum", -1.0)) < 0.90
        or float(compute.get("integrity_persistence_maximum", 2.0)) > 0.07
        or float(compute.get("reporting_maximum", 2.0)) > 0.03
        or float(compute.get("aggregate_cpu_target_minimum", -1.0)) < 0.80
        or int(compute.get("cores_reserved_for_os_and_writer", -1)) != 1
        or compute.get("full_regression_per_wave") is not False
        or compute.get("idle_wait_for_user_input") is not False
    ):
        raise FastPassManifestError("fast-pass compute allocation drift")


def _validate_evidence(manifest: Mapping[str, Any]) -> None:
    evidence = _mapping(manifest, "evidence_bundle")
    receipt = str(evidence.get("lightweight_manifest_path") or "")
    expected_output = _technical_revision_output(manifest)
    if (
        evidence.get("required_for_campaign_complete") is not True
        or evidence.get("atomic_finalize") is not True
        or evidence.get("summary_only_complete_allowed") is not False
        or set(str(value) for value in evidence.get("required_datasets") or ())
        != set(REQUIRED_DATASETS)
        or evidence.get("large_files_git_tracked") is not False
        or evidence.get("reconstruction_flag") is not False
        or evidence.get("one_batch_archive_per_wave") is not True
        or str(evidence.get("destination") or "") != "data/cache/evidence_bundles"
        or not receipt.startswith(f"{expected_output}/")
        or not receipt.endswith("/evidence_bundle_receipt.json")
    ):
        raise FastPassManifestError("fast-pass EvidenceBundle contract drift")


def _technical_revision_output(manifest: Mapping[str, Any]) -> str:
    revision_id = manifest.get("revision_id")
    if revision_id is None:
        return "reports/economic_evolution/fast_pass_factory_0029"
    outputs = {
        FAST_PASS_TECHNICAL_REVISION_ID:
            "reports/economic_evolution/fast_pass_factory_0029_revision_01",
        FAST_PASS_TECHNICAL_REVISION_02_ID:
            "reports/economic_evolution/fast_pass_factory_0029_revision_02",
        FAST_PASS_TECHNICAL_REVISION_03_ID:
            "reports/economic_evolution/fast_pass_factory_0029_revision_03",
    }
    try:
        return outputs[str(revision_id)]
    except KeyError as exc:
        raise FastPassManifestError(
            "unknown fast-pass technical revision"
        ) from exc


def _validate_technical_repair(
    manifest: Mapping[str, Any], root: Path
) -> None:
    repair = manifest.get("technical_repair")
    revision_id = manifest.get("revision_id")
    if revision_id is None:
        if repair is not None:
            raise FastPassManifestError("unexpected fast-pass technical repair")
        return
    if revision_id not in {
        FAST_PASS_TECHNICAL_REVISION_ID,
        FAST_PASS_TECHNICAL_REVISION_02_ID,
        FAST_PASS_TECHNICAL_REVISION_03_ID,
    } or not isinstance(repair, Mapping):
        raise FastPassManifestError("fast-pass technical repair contract missing")
    receipt_ref = repair.get("repair_receipt")
    if not isinstance(receipt_ref, Mapping):
        raise FastPassManifestError("fast-pass technical repair receipt missing")
    relative = Path(str(receipt_ref.get("path") or ""))
    if relative.is_absolute() or ".." in relative.parts:
        raise FastPassManifestError("fast-pass repair receipt path is unsafe")
    receipt_path = (root / relative).resolve()
    if root not in receipt_path.parents or not receipt_path.is_file():
        raise FastPassManifestError("fast-pass repair receipt is missing")
    receipt = _load_json(receipt_path)
    claimed = str(receipt.get("repair_record_hash") or "")
    core = dict(receipt)
    core.pop("repair_record_hash", None)
    revision_contracts = {
        FAST_PASS_TECHNICAL_REVISION_ID: {
            "classification": "TECHNICAL_SCENARIO_SUFFIX_QUALITY_IDENTITY_DEFECT",
            "scientific_status":
                "RESTORES_FROZEN_SCENARIO_NEUTRAL_QUALITY_SEMANTICS",
            "resume_scope":
                "REUSE_SEALED_STAGE0_STAGE1_STAGE2_BEGIN_SPRINT_EVALUATION",
            "supersedes_manifest_hash": FAST_PASS_ORIGINAL_MANIFEST_HASH,
            "supersedes_manifest_file_sha256":
                FAST_PASS_ORIGINAL_MANIFEST_FILE_SHA256,
            "supersedes_output_dir":
                "reports/economic_evolution/fast_pass_factory_0029",
            "revision_output_dir":
                "reports/economic_evolution/fast_pass_factory_0029_revision_01",
        },
        FAST_PASS_TECHNICAL_REVISION_02_ID: {
            "classification": "TECHNICAL_BLOCK_SUMMARY_SCHEMA_READER_DEFECT",
            "scientific_status":
                "RESTORES_CANONICAL_BY_BLOCK_CONTROL_AGGREGATION",
            "resume_scope":
                "REUSE_SEALED_STAGE0_STAGE1_STAGE2_SPRINT_AND_CONTROLS_AGGREGATE_ONLY",
            "supersedes_manifest_hash":
                "4e6a2feff9ea16ce866330e760ad05afd48b79f6aef207e16fadb1d9f2c6c882",
            "supersedes_manifest_file_sha256":
                "a10a667055da7a62bec2e5718b14823c2fdfeb7b2640e924bafca28d2dcbc2f4",
            "supersedes_output_dir":
                "reports/economic_evolution/fast_pass_factory_0029_revision_01",
            "revision_output_dir":
                "reports/economic_evolution/fast_pass_factory_0029_revision_02",
        },
        FAST_PASS_TECHNICAL_REVISION_03_ID: {
            "classification":
                "TECHNICAL_DIVERSITY_DECISION_FIELD_ADAPTER_DEFECT",
            "scientific_status":
                "RESTORES_CANONICAL_DECISION_TIME_DIVERSITY_AUDIT",
            "resume_scope":
                "REUSE_SEALED_ALL_ECONOMIC_EVIDENCE_FINALIZATION_ONLY",
            "supersedes_manifest_hash":
                "5907af0925ce8958200a96e5497edce2ccc7fda22d3b8e39da70cbc384fbccd5",
            "supersedes_manifest_file_sha256":
                "4d402cd3484bb17b0585260f2c58c856647baef61f453b81184a73f2bf9f5e7c",
            "supersedes_output_dir":
                "reports/economic_evolution/fast_pass_factory_0029_revision_02",
            "revision_output_dir":
                "reports/economic_evolution/fast_pass_factory_0029_revision_03",
        },
    }
    contract = revision_contracts[str(revision_id)]
    expected_flags = (
        repair.get("classification") == contract["classification"]
        and repair.get("scientific_hypothesis_changed") is False
        and repair.get("candidate_population_or_selection_changed") is False
        and repair.get("stage0_stage1_stage2_evidence_recomputed") is False
        and repair.get("completed_stage0_stage1_stage2_reused_unchanged") is True
        and repair.get("risk_threshold_or_control_changed") is False
        and repair.get("new_multiplicity_reservation_required") is False
        and repair.get("resume_scope") == contract["resume_scope"]
        and repair.get("supersedes_manifest_hash")
        == contract["supersedes_manifest_hash"]
        and repair.get("supersedes_manifest_file_sha256")
        == contract["supersedes_manifest_file_sha256"]
        and repair.get("supersedes_output_dir")
        == contract["supersedes_output_dir"]
        and repair.get("revision_output_dir")
        == contract["revision_output_dir"]
        and repair.get("repair_commit") == manifest.get("source_commit")
    )
    if revision_id == FAST_PASS_TECHNICAL_REVISION_02_ID:
        expected_flags = bool(
            expected_flags
            and repair.get("sprint_or_control_evidence_recomputed") is False
            and repair.get("completed_sprint_and_controls_reused_unchanged") is True
        )
    if revision_id == FAST_PASS_TECHNICAL_REVISION_03_ID:
        expected_flags = bool(
            expected_flags
            and repair.get("sprint_or_control_evidence_recomputed") is False
            and repair.get("completed_sprint_and_controls_reused_unchanged") is True
            and repair.get("terminal_economic_evidence_recomputed") is False
            and repair.get("completed_all_economic_evidence_reused_unchanged")
            is True
        )
    if (
        not expected_flags
        or receipt_ref.get("file_sha256") != _sha256(receipt_path)
        or receipt_ref.get("repair_record_hash") != claimed
        or not _SHA256.fullmatch(claimed)
        or stable_hash(core) != claimed
        or receipt.get("campaign_id") != FAST_PASS_CAMPAIGN_ID
        or receipt.get("classification") != repair.get("classification")
        or receipt.get("scientific_status") != contract["scientific_status"]
        or (receipt.get("code_repair") or {}).get("repair_commit")
        != manifest.get("source_commit")
        or receipt.get("multiplicity", {}).get("multiplicity_delta") != 0
    ):
        raise FastPassManifestError("fast-pass technical repair provenance drift")
    if revision_id in {
        FAST_PASS_TECHNICAL_REVISION_02_ID,
        FAST_PASS_TECHNICAL_REVISION_03_ID,
    }:
        _validate_repair_semantic_allowlist(manifest, repair, root)


def _validate_repair_semantic_allowlist(
    manifest: Mapping[str, Any], repair: Mapping[str, Any], root: Path
) -> None:
    relative = Path(str(repair.get("supersedes_manifest_path") or ""))
    if relative.is_absolute() or ".." in relative.parts:
        raise FastPassManifestError("fast-pass superseded manifest path is unsafe")
    source_path = (root / relative).resolve()
    if root not in source_path.parents or not source_path.is_file():
        raise FastPassManifestError("fast-pass superseded manifest is missing")
    if _sha256(source_path) != repair.get("supersedes_manifest_file_sha256"):
        raise FastPassManifestError("fast-pass superseded manifest checksum drift")
    prior = dict(_load_json(source_path))
    current = dict(manifest)
    for payload in (prior, current):
        payload.pop("manifest_hash", None)
        payload.pop("source_commit", None)
        payload.pop("revision_id", None)
        payload.pop("technical_repair", None)
        payload.pop("implementation_files", None)
        runtime = dict(payload["runtime"])
        runtime.pop("output_dir", None)
        payload["runtime"] = runtime
        evidence = dict(payload["evidence_bundle"])
        evidence.pop("lightweight_manifest_path", None)
        payload["evidence_bundle"] = evidence
    if current != prior:
        raise FastPassManifestError(
            "fast-pass technical revision changed frozen economics"
        )


def _validate_multiplicity(manifest: Mapping[str, Any]) -> None:
    value = _mapping(manifest, "multiplicity")
    try:
        prior = int(value["prior_global_N_trials"])
        prospective = int(value["prospective_comparisons"])
        delta = int(value["reserved_delta_trials"])
        expected = int(value["expected_global_N_trials_after_reservation"])
        inflation = float(value["campaign_specific_inflation"])
    except (KeyError, TypeError, ValueError) as exc:
        raise FastPassManifestError("invalid fast-pass multiplicity declaration") from exc
    if (
        min(prior, prospective, delta, expected) < 0
        or prospective < 50_000
        or inflation < 1.0
        or delta != int(prospective * inflation)
        or expected != prior + delta
        or value.get("reservation_required_before_outcome_access") is not True
        or value.get("proof_window_consumed") is not False
    ):
        raise FastPassManifestError("fast-pass multiplicity arithmetic drift")


def _validate_hashed_references(references: Mapping[str, Any], root: Path) -> None:
    if not references:
        raise FastPassManifestError("fast-pass terminal baseline sources missing")
    for name, raw in references.items():
        if not isinstance(raw, Mapping):
            raise FastPassManifestError(f"invalid fast-pass source reference: {name}")
        relative = str(raw.get("path") or "")
        target = (root / relative).resolve()
        if target == root or root not in target.parents:
            raise FastPassManifestError(f"fast-pass source path escapes root: {name}")
        expected = str(raw.get("file_sha256") or "")
        if not _SHA256.fullmatch(expected) or _sha256(target) != expected:
            raise FastPassManifestError(f"fast-pass source checksum drift: {name}")


def _small_positive_frontier(raw: Any) -> bool:
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        return False
    try:
        values = tuple(float(value) for value in raw)
    except (TypeError, ValueError):
        return False
    return 1 <= len(values) <= 6 and all(
        math.isfinite(value) and 0.0 < value <= 1.0 for value in values
    ) and tuple(sorted(set(values))) == values


def _project_root(path: Path) -> Path:
    for parent in (path.parent, *path.parents):
        if (parent / "MISSION_CONTRACT.md").is_file():
            return parent
    raise FastPassManifestError("fast-pass project root not found")


def _mapping(value: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    item = value.get(key)
    if not isinstance(item, Mapping):
        raise FastPassManifestError(f"fast-pass manifest requires object: {key}")
    return item


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1 << 20), b""):
                digest.update(chunk)
    except OSError as exc:
        raise FastPassManifestError(f"cannot hash fast-pass file: {path}") from exc
    return digest.hexdigest()


def _load_json(path: Path) -> Mapping[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise FastPassManifestError(
            f"cannot load fast-pass JSON receipt: {path}"
        ) from exc
    if not isinstance(payload, Mapping):
        raise FastPassManifestError(
            f"fast-pass JSON receipt is not an object: {path}"
        )
    return payload


__all__ = [
    "FAST_PASS_CAMPAIGN_ID",
    "FAST_PASS_CAMPAIGN_MODE",
    "FAST_PASS_CLASS_ID",
    "FAST_PASS_REQUIRED_IMPLEMENTATION_FILES",
    "FAST_PASS_RUNTIME_VERSION",
    "FAST_PASS_WINDOWS",
    "FastPassManifestError",
    "validate_fast_pass_manifest",
]
