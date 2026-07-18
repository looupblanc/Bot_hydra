"""Fail-closed manifest contract for HYDRA campaign 0034.

Campaign 0034 is a bounded continuation of the two diagnostic abstention
policies sealed by campaign 0033.  The structural direction and trade contract
remain authoritative; the only production actions are abstain, trade at the
nominal risk, or trade at 1.5 times nominal risk.  This module is deliberately
non-mutating.  The existing controller owns multiplicity reservation, data
acquisition, evidence finalisation, and all mission-state writes.
"""

from __future__ import annotations

import hashlib
import math
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from hydra.economic_evolution.schema import stable_hash
from hydra.evidence import REQUIRED_DATASETS


MANIFEST_SCHEMA = "hydra_economic_production_manifest_v1"
CAMPAIGN_MODE = "SELECTIVE_ORDER_FLOW_VETO_EXPANSION"
CAMPAIGN_ID = "hydra_selective_order_flow_veto_expansion_0034"
CLASS_ID = "SELECTIVE_ORDER_FLOW_VETO_EXPANSION_V1"
RUNTIME_VERSION = "hydra_selective_order_flow_veto_runtime_v1"

PRIMARY_SEED_ID = "hybrid_0033_01_f0345ecb99af8c25"
SECONDARY_SEED_ID = "hybrid_0033_07_5f93891cf737e51a"
SEED_IDS = (PRIMARY_SEED_ID, SECONDARY_SEED_ID)
SEED_STATUS = "SELECTIVE_VETO_DIAGNOSTIC_SEED"
PRIMARY_ACTIONS = ("ABSTAIN", "TRADE_1X", "TRADE_1_5X")
RISK_LEVELS = (0.0, 1.0, 1.5)
SEED_DECISIONS = (
    "SELECTIVE_VETO_SEED_ROBUST",
    "SELECTIVE_VETO_SEED_FRAGILE",
    "SELECTIVE_VETO_SEED_FALSIFIED",
)
LONG_SAMPLE_DECISIONS = (
    "LONG_SAMPLE_SELECTIVE_OVERLAY_GREEN",
    "LONG_SAMPLE_SELECTIVE_OVERLAY_WEAK",
    "LONG_SAMPLE_SELECTIVE_OVERLAY_FALSIFIED",
)
SEQUENTIAL_DECISIONS = (
    "SUCCESS_EVIDENCE_SUFFICIENT",
    "CONTINUE_ACQUISITION",
    "FUTILITY_STOP",
)
ACCOUNT_SIZES_USD = (50_000, 100_000, 150_000)
MICROSTRUCTURE_SCHEMAS = ("trades", "tbbo", "mbp-1")
WINDOW_COUNTS = (100, 250, 500, 1_000)
MODEL_CLASSES = (
    "REGULARIZED_LOGISTIC_REGRESSION",
    "SHALLOW_MONOTONIC_TREE",
    "MONOTONIC_GRADIENT_BOOSTING",
    "TRANSPARENT_POLICY_TREE",
)
FEATURE_TIERS = ("TRADES_ONLY", "TBBO", "MBP_1", "MBO_TEACHER_ONLY")
MATERIAL_STRESSED_TARGET_PROGRESS_UPLIFT_MINIMUM = 0.05
STRUCTURAL_FAMILIES = (
    "CROSS_MARKET_DIVERGENCE",
    "COMPRESSION_TO_EXPANSION",
    "MULTI_TIMEFRAME_CONTINUATION",
    "FAILED_BREAKOUT",
    "SESSION_TRANSITION",
    "OPENING_RANGE",
)
OFFICIAL_RULE_SOURCE_URLS = (
    "https://help.topstep.com/en/articles/8284197-trading-combine-parameters",
    "https://help.topstep.com/en/articles/8284204-what-is-the-maximum-loss-limit",
    "https://help.topstep.com/en/articles/8284208-consistency-at-topstep",
)

# The three sizes were reconciled against the same official Topstep pages on
# 2026-07-18 before any 0034 long-sample outcome or data purchase was observed.
ACCOUNT_RULE_SNAPSHOTS: Mapping[str, Mapping[str, Any]] = {
    "50K": {
        "snapshot_id": "topstep_50k_2026-07-18_official_no_optional_dll_v1",
        "snapshot_sha256": "cb135983710b5c62755d8f38b1c9c283f90f403ee1a239ca8a670e5af505268f",
        "provenance_class": "OFFICIAL_VERSIONED_RULE_SNAPSHOT",
        "official_source_verified": True,
        "official_source_urls": list(OFFICIAL_RULE_SOURCE_URLS),
        "verified_at_utc": "2026-07-18T22:15:00Z",
        "account_size_usd": 50_000,
        "profit_target_usd": 3_000,
        "maximum_loss_limit_usd": 2_000,
        "maximum_mini_contracts": 5,
        "maximum_micro_contracts": 50,
        "consistency_limit": 0.50,
        "minimum_pass_days": 2,
        "session_close_required": True,
        "no_daily_loss_limit": True,
        "use_optional_daily_loss_limit": False,
        "mll_mode": "EOD_LEVEL_RT_BREACH",
    },
    "100K": {
        "snapshot_id": "topstep_100k_2026-07-18_official_no_optional_dll_v1",
        "snapshot_sha256": "dd75379f2d378e657c1530cee50b8687252919897a8e2a6d072ca00313138f0c",
        "provenance_class": "OFFICIAL_VERSIONED_RULE_SNAPSHOT",
        "official_source_verified": True,
        "official_source_urls": list(OFFICIAL_RULE_SOURCE_URLS),
        "verified_at_utc": "2026-07-18T22:15:00Z",
        "account_size_usd": 100_000,
        "profit_target_usd": 6_000,
        "maximum_loss_limit_usd": 3_000,
        "maximum_mini_contracts": 10,
        "maximum_micro_contracts": 100,
        "consistency_limit": 0.50,
        "minimum_pass_days": 2,
        "session_close_required": True,
        "no_daily_loss_limit": True,
        "use_optional_daily_loss_limit": False,
        "mll_mode": "EOD_LEVEL_RT_BREACH",
    },
    "150K": {
        "snapshot_id": "topstep_150k_2026-07-18_official_no_optional_dll_v1",
        "snapshot_sha256": "d777dd84c6cc2848d983ee4ee3d8df8836674e23d518962b15ed59d261ca9fce",
        "provenance_class": "OFFICIAL_VERSIONED_RULE_SNAPSHOT",
        "official_source_verified": True,
        "official_source_urls": list(OFFICIAL_RULE_SOURCE_URLS),
        "verified_at_utc": "2026-07-18T22:15:00Z",
        "account_size_usd": 150_000,
        "profit_target_usd": 9_000,
        "maximum_loss_limit_usd": 4_500,
        "maximum_mini_contracts": 15,
        "maximum_micro_contracts": 150,
        "consistency_limit": 0.50,
        "minimum_pass_days": 2,
        "session_close_required": True,
        "no_daily_loss_limit": True,
        "use_optional_daily_loss_limit": False,
        "mll_mode": "EOD_LEVEL_RT_BREACH",
    },
}

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_GIT_SHA = re.compile(r"^[0-9a-f]{40}$")
_REQUIRED_IMPLEMENTATION_FILES = frozenset(
    {
        "hydra/production/selective_veto_manifest.py",
        "hydra/production/selective_veto_runtime.py",
        "hydra/production/selective_veto_seed_audit.py",
        "hydra/production/selective_veto_pilot.py",
        "hydra/production/selective_veto_metadata.py",
        "hydra/production/manifest.py",
        "hydra/production/runtime.py",
        "scripts/run_economic_production_manifest.py",
    }
)
_REQUIRED_0033_SOURCES = frozenset(
    {
        "authoritative_result",
        "evidence_bundle_receipt",
        "evidence_bundle_manifest",
        "decision_report",
        "pilot_summary",
        "selective_veto_terminal_receipt",
        "selective_veto_boundary_verdict",
    }
)
_ALLOWED_STRUCTURAL_FAMILIES = frozenset(
    {
        "OVERNIGHT_INVENTORY",
        "OPENING_GAP_CONTINUATION_REJECTION",
        "OPENING_RANGE",
        "COMPRESSION_TO_EXPANSION",
        "MULTI_TIMEFRAME_CONTINUATION",
        "FAILED_BREAKOUT",
        "ANCHORED_VWAP_DISPLACEMENT",
        "CROSS_MARKET_DIVERGENCE",
        "SESSION_TRANSITION",
    }
)


class SelectiveVetoManifestError(RuntimeError):
    """The 0034 scientific or operational preregistration is unsafe."""


def validate_selective_veto_manifest(
    manifest: Mapping[str, Any], *, manifest_path: str | Path
) -> None:
    """Validate the full immutable 0034 contract without side effects."""

    path = Path(manifest_path).resolve()
    root = _project_root(path)
    _identity(manifest)
    _implementation(manifest, root)
    _source_0033(manifest, root)
    _frozen_seeds(manifest)
    _runtime_compute(manifest, root)
    _primary_actions(manifest)
    _seed_audit(manifest)
    _structural_universe(manifest)
    _window_and_cost_contract(manifest)
    _chronological_roles(manifest)
    _distillation(manifest)
    _paired_evaluation(manifest)
    _sequential_policy(manifest)
    _diagnostic_forward(manifest)
    _account_rule_snapshots(manifest)
    _account_speed_gate(manifest)
    _multiplicity(manifest)
    _evidence_and_governance(manifest, root)


def _identity(manifest: Mapping[str, Any]) -> None:
    try:
        created = datetime.fromisoformat(
            str(manifest.get("created_at_utc") or "").replace("Z", "+00:00")
        )
    except ValueError as exc:
        raise SelectiveVetoManifestError("0034 freeze timestamp is invalid") from exc
    claimed = str(manifest.get("manifest_hash") or "")
    payload = dict(manifest)
    payload.pop("manifest_hash", None)
    if (
        manifest.get("schema") != MANIFEST_SCHEMA
        or manifest.get("campaign_mode") != CAMPAIGN_MODE
        or manifest.get("campaign_id") != CAMPAIGN_ID
        or manifest.get("class_id") != CLASS_ID
        or _tuple(manifest.get("policy_classes")) != (CLASS_ID,)
        or manifest.get("development_only") is not True
        or created.tzinfo is None
        or not _GIT_SHA.fullmatch(str(manifest.get("source_commit") or ""))
        or not str(manifest.get("economic_hypothesis") or "").strip()
        or not _SHA256.fullmatch(claimed)
        or stable_hash(payload) != claimed
    ):
        raise SelectiveVetoManifestError("0034 identity or semantic hash drift")


def _implementation(manifest: Mapping[str, Any], root: Path) -> None:
    files = _mapping(manifest, "implementation_files")
    if not _REQUIRED_IMPLEMENTATION_FILES <= {str(key) for key in files}:
        raise SelectiveVetoManifestError("0034 implementation closure is incomplete")
    for relative, claimed_raw in files.items():
        target = _project_file(root, relative, "implementation")
        claimed = str(claimed_raw or "")
        if not _SHA256.fullmatch(claimed) or _sha256(target) != claimed:
            raise SelectiveVetoManifestError(
                f"0034 implementation checksum drift: {relative}"
            )


def _source_0033(manifest: Mapping[str, Any], root: Path) -> None:
    source = _mapping(manifest, "terminal_source_0033")
    if (
        source.get("campaign_id")
        != "hydra_hybrid_structural_alpha_order_flow_0033"
        or source.get("terminal_status") != "HYBRID_OVERLAY_WEAK"
        or source.get("reuse_mode") != "IMMUTABLE_READ_ONLY"
        or source.get("broad_refinement_resume_allowed") is not False
        or source.get("timing_alpha_established") is not False
        or source.get("execution_alpha_established") is not False
        or source.get("standalone_microstructure_alpha_falsified") is not True
        or source.get("status_inheritance_allowed") is not False
        or source.get("outcomes_are_development_only") is not True
    ):
        raise SelectiveVetoManifestError("0034 terminal 0033 source drift")
    _hashed_sources(
        source,
        root=root,
        required=_REQUIRED_0033_SOURCES,
        label="0033 source",
    )


def _frozen_seeds(manifest: Mapping[str, Any]) -> None:
    seeds = manifest.get("frozen_seed_policies")
    if not isinstance(seeds, Sequence) or isinstance(seeds, (str, bytes)):
        raise SelectiveVetoManifestError("0034 frozen seed declarations are absent")
    rows = [row for row in seeds if isinstance(row, Mapping)]
    if len(rows) != 2 or tuple(str(row.get("policy_id")) for row in rows) != SEED_IDS:
        raise SelectiveVetoManifestError("0034 frozen seed identities drift")
    expected = {
        PRIMARY_SEED_ID: {
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
        SECONDARY_SEED_ID: {
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
    }
    for row in rows:
        policy_id = str(row["policy_id"])
        values = expected[policy_id]
        if (
            row.get("status") != SEED_STATUS
            or row.get("immutable") is not True
            or row.get("mutation_allowed") is not False
            or row.get("status_inheritance_allowed") is not False
            or row.get("promotion_status") is not None
            or not _SHA256.fullmatch(str(row.get("policy_fingerprint") or ""))
            or str(row.get("deployability_tier")) != values["deployability_tier"]
        ):
            raise SelectiveVetoManifestError("0034 frozen seed status drift")
        for key, expected_value in values.items():
            if key == "deployability_tier":
                continue
            if not _close(row.get(key), expected_value, tolerance=5e-4):
                raise SelectiveVetoManifestError(
                    f"0034 reported evidence drift for {policy_id}: {key}"
                )


def _runtime_compute(manifest: Mapping[str, Any], root: Path) -> None:
    runtime = _mapping(manifest, "runtime")
    compute = _mapping(manifest, "compute_contract")
    output = (root / str(runtime.get("output_dir") or "")).resolve()
    allowed = (root / "reports/economic_evolution").resolve()
    if (
        runtime.get("engine") != "production_kernel_v1"
        or runtime.get("runner") != "scripts/run_economic_production_manifest.py"
        or runtime.get("selective_veto_runtime_version") != RUNTIME_VERSION
        or runtime.get("result_schema") != "hydra_economic_production_result_v1"
        or runtime.get("result_name") != "economic_production_result.json"
        or runtime.get("controller_source_change_required") is not False
        or runtime.get("resume_from_checkpoint") is not True
        or _integer(runtime.get("orchestrator_count")) != 1
        or _integer(runtime.get("worker_count")) != 2
        or _integer(runtime.get("asynchronous_evidence_writer_count")) != 1
        or output == allowed
        or allowed not in output.parents
        or _integer(compute.get("vps_cpu_core_count")) != 3
        or _integer(compute.get("orchestrator_count")) != 1
        or _integer(compute.get("cpu_worker_count")) != 2
        or _integer(compute.get("authoritative_writer_count")) != 1
        or compute.get("cpu_workers_read_only") is not True
        or compute.get("single_writer_atomic_commits") is not True
        or compute.get("oversubscription_allowed") is not False
        or not _close(compute.get("target_cpu_utilization_min"), 0.80)
        or not _close(compute.get("target_cpu_utilization_max"), 0.95)
        or not _close(compute.get("economic_wall_clock_minimum"), 0.90)
    ):
        raise SelectiveVetoManifestError("0034 runtime/three-core topology drift")
    thread_limits = _mapping(compute, "thread_limits")
    required = ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS", "NUMEXPR_NUM_THREADS")
    if any(str(thread_limits.get(name)) != "1" for name in required):
        raise SelectiveVetoManifestError("0034 CPU library thread limits drift")


def _primary_actions(manifest: Mapping[str, Any]) -> None:
    contract = _mapping(manifest, "primary_action_contract")
    if (
        _tuple(contract.get("actions")) != PRIMARY_ACTIONS
        or _float_tuple(contract.get("risk_levels")) != RISK_LEVELS
        or contract.get("structural_direction_immutable") is not True
        or contract.get("structural_entry_exit_stop_target_immutable") is not True
        or contract.get("a2_timing_allowed") is not False
        or contract.get("a3_execution_variant_allowed") is not False
        or contract.get("a4_passive_allowed") is not False
        or contract.get("a5_early_invalidation_allowed") is not False
        or contract.get("direction_reversal_allowed") is not False
        or contract.get("new_structural_direction_allowed") is not False
    ):
        raise SelectiveVetoManifestError("0034 primary action lattice drift")


def _seed_audit(manifest: Mapping[str, Any]) -> None:
    audit = _mapping(manifest, "seed_robustness_audit")
    required_attribution = {
        "market",
        "structural_anchor_family",
        "session",
        "individual_opportunity",
        "abstention",
        "risk_1_5x",
        "all_in_costs",
    }
    if (
        set(_tuple(audit.get("attribution_dimensions"))) != required_attribution
        or audit.get("leave_one_opportunity_out") is not True
        or _integer_tuple(audit.get("top_trade_removal_counts")) != (1, 2, 3)
        or audit.get("leave_one_anchor_family_out") is not True
        or _float_tuple(audit.get("cost_stress_multipliers")) != (1.0, 1.25)
        or _tuple(audit.get("feature_dependency_tiers")) != FEATURE_TIERS
        or _integer_tuple(audit.get("account_sizes_usd")) != ACCOUNT_SIZES_USD
        or _integer_tuple(audit.get("account_horizons_days")) != (5, 10)
        or _tuple(audit.get("allowed_decisions")) != SEED_DECISIONS
        or audit.get("no_purchase_before_decision") is not True
        or audit.get("best_trade_removal_positive_stressed_required") is not True
        or _integer(audit.get("minimum_distinct_context_count")) != 2
        or not _close(audit.get("maximum_single_opportunity_profit_share"), 0.25)
        or audit.get("hard_data_or_deployability_defect_allowed") is not False
        or audit.get("thresholds_may_change_after_results") is not False
    ):
        raise SelectiveVetoManifestError("0034 seed robustness audit drift")


def _structural_universe(manifest: Mapping[str, Any]) -> None:
    universe = _mapping(manifest, "structural_anchor_universe")
    families = _tuple(universe.get("families"))
    if (
        families != STRUCTURAL_FAMILIES
        or not set(families) <= _ALLOWED_STRUCTURAL_FAMILIES
        or universe.get("source") != "EXISTING_CACHED_CAUSAL_OHLCV_AND_STRUCTURAL_FEATURES"
        or universe.get("microstructure_outcomes_used_for_generation") is not False
        or universe.get("causal_generation_required") is not True
        or universe.get("behavioral_deduplication_required") is not True
        or universe.get("temporal_deduplication_required") is not True
        or universe.get("neighboring_bar_duplicates_allowed") is not False
        or universe.get("anchor_fields_complete") is not True
    ):
        raise SelectiveVetoManifestError("0034 structural-anchor universe drift")


def _window_and_cost_contract(manifest: Mapping[str, Any]) -> None:
    windows = _mapping(manifest, "anchor_conditioned_windows")
    cost = _mapping(manifest, "targeted_cost_policy")
    if (
        _integer(windows.get("pre_decision_lookback_seconds")) != 120
        or _integer(windows.get("post_decision_safety_seconds")) not in {30, 60}
        or windows.get("deterministic_warmup_included") is not True
        or windows.get("overlapping_windows_merged") is not True
        or windows.get("full_session_request_default") is not False
        or windows.get("holding_period_microstructure_required") is not False
        or windows.get("cached_market_data_used_for_post_entry_outcomes") is not True
        or _tuple(cost.get("schemas")) != MICROSTRUCTURE_SCHEMAS
        or _integer_tuple(cost.get("window_counts")) != WINDOW_COUNTS
        or cost.get("official_databento_cost_estimate_required") is not True
        or cost.get("one_and_two_market_estimates_required") is not True
        or cost.get("chronological_role_costs_required") is not True
        or cost.get("no_new_mbo_purchase") is not True
        or cost.get("purchase_before_seed_gate_allowed") is not False
        or cost.get("silent_purchase_allowed") is not False
        or cost.get("manifest_bound_purchase_counter_required") is not True
        or cost.get("unmanifested_purchase_count_must_be_zero") is not True
        or cost.get("ledger_before_after_hash_required") is not True
        or not _close(cost.get("current_remaining_budget_usd"), 28.498462508622012)
        or not _close(cost.get("minimum_budget_reserve_usd"), 20.0)
        or not _close(cost.get("maximum_incremental_spend_usd"), 8.0)
    ):
        raise SelectiveVetoManifestError("0034 window/cost contract drift")
    if _finite(cost.get("current_remaining_budget_usd")) - _finite(
        cost.get("maximum_incremental_spend_usd")
    ) < _finite(cost.get("minimum_budget_reserve_usd")):
        raise SelectiveVetoManifestError("0034 targeted purchase cannot preserve reserve")


def _chronological_roles(manifest: Mapping[str, Any]) -> None:
    roles = _mapping(manifest, "chronological_roles")
    if (
        not _close(roles.get("discovery_fraction"), 0.60)
        or not _close(roles.get("validation_fraction"), 0.20)
        or not _close(roles.get("final_development_fraction"), 0.20)
        or roles.get("random_temporal_mixing_allowed") is not False
        or roles.get("roles_frozen_before_download") is not True
        or roles.get("final_development_includes_all_eligible_anchors") is not True
        or roles.get("final_development_outcomes_visible_before_policy_freeze") is not False
        or roles.get("final_development_is_independent_confirmation") is not False
    ):
        raise SelectiveVetoManifestError("0034 chronological role contract drift")


def _distillation(manifest: Mapping[str, Any]) -> None:
    policy = _mapping(manifest, "selective_policy_distillation")
    if (
        _tuple(policy.get("actions")) != PRIMARY_ACTIONS
        or _tuple(policy.get("model_classes")) != MODEL_CLASSES
        or _integer(policy.get("maximum_production_features")) > 8
        or _integer(policy.get("maximum_thresholds")) > 3
        or policy.get("direction_generation_allowed") is not False
        or policy.get("deterministic_versioned_output") is not True
        or policy.get("objective")
        != "LOWER_CONFIDENCE_BOUND_OF_PAIRED_STRESSED_UPLIFT"
        or not _close(policy.get("minimum_trade_coverage"), 0.20)
        or not _close(policy.get("maximum_abstention"), 0.80)
        or policy.get("exact_mll_required") is not True
        or policy.get("consistency_required") is not True
        or policy.get("single_opportunity_domination_allowed") is not False
        or policy.get("raw_in_sample_net_is_primary") is not False
    ):
        raise SelectiveVetoManifestError("0034 selective-policy contract drift")


def _paired_evaluation(manifest: Mapping[str, Any]) -> None:
    paired = _mapping(manifest, "paired_long_sample_evaluation")
    required = {
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
    }
    if (
        paired.get("baseline_action") != "BASELINE_IMMEDIATE_CAUSAL_STRUCTURAL_TRADE"
        or _tuple(paired.get("selective_actions")) != PRIMARY_ACTIONS
        or paired.get("identical_structural_direction_stop_target_exit") is not True
        or paired.get("identical_causal_fill_and_account_rules") is not True
        or set(_tuple(paired.get("paired_metrics"))) != required
        or paired.get("report_market_family_session_block") is not True
        or paired.get("unpaired_opportunity_sets_primary") is not False
        or paired.get("unique_structural_opportunity_required") is not True
        or paired.get("aggregate_to_event_reconciliation_required") is not True
    ):
        raise SelectiveVetoManifestError("0034 paired evaluation contract drift")


def _sequential_policy(manifest: Mapping[str, Any]) -> None:
    policy = _mapping(manifest, "sequential_evidence_policy")
    if (
        _integer_tuple(policy.get("additional_session_checkpoints")) != (5, 10, 15)
        or policy.get("maximum_available_within_budget_checkpoint") is not True
        or _tuple(policy.get("allowed_decisions")) != SEQUENTIAL_DECISIONS
        or policy.get("evidence_available_up_to_checkpoint_only") is not True
        or policy.get("continuous_retuning_allowed") is not False
        or policy.get("success_requires_frozen_gate") is not True
        or policy.get("futility_requires_negative_seed_and_policy_uplift") is not True
    ):
        raise SelectiveVetoManifestError("0034 sequential evidence policy drift")


def _diagnostic_forward(manifest: Mapping[str, Any]) -> None:
    forward = _mapping(manifest, "diagnostic_forward")
    if (
        forward.get("status") != "SELECTIVE_VETO_DIAGNOSTIC_FORWARD"
        or _tuple(forward.get("policy_ids")) != SEED_IDS
        or forward.get("activation_requires_authorized_research_feed") is not True
        or forward.get("append_only") is not True
        or forward.get("zero_order") is not True
        or forward.get("parameter_changes_allowed") is not False
        or forward.get("economic_promotion_allowed") is not False
        or forward.get("paper_shadow_ready_claim_allowed") is not False
        or forward.get("broker_connection_allowed") is not False
        or forward.get("orders_allowed") is not False
        or forward.get("activation_after_manifest_freeze_required") is not True
        or forward.get("feed_authorization_receipt_required") is not True
        or forward.get("raw_event_fingerprints_required") is not True
        or forward.get("seed_fingerprint_match_required") is not True
        or forward.get("falsified_seed_activation_allowed") is not False
    ):
        raise SelectiveVetoManifestError("0034 diagnostic-forward contract drift")


def _account_rule_snapshots(manifest: Mapping[str, Any]) -> None:
    snapshots = _mapping(manifest, "account_rule_snapshots")
    if set(str(key) for key in snapshots) != set(ACCOUNT_RULE_SNAPSHOTS):
        raise SelectiveVetoManifestError("0034 account-rule snapshot inventory drift")
    for label, expected in ACCOUNT_RULE_SNAPSHOTS.items():
        row = snapshots.get(label)
        payload = dict(row) if isinstance(row, Mapping) else {}
        claimed = payload.pop("snapshot_sha256", None)
        if (
            not isinstance(row, Mapping)
            or dict(row) != dict(expected)
            or claimed != stable_hash(payload)
        ):
            raise SelectiveVetoManifestError(
                f"0034 {label} account-rule snapshot provenance drift"
            )


def _account_speed_gate(manifest: Mapping[str, Any]) -> None:
    gate = _mapping(manifest, "account_speed_gate")
    if (
        _integer_tuple(gate.get("account_sizes_usd")) != ACCOUNT_SIZES_USD
        or _integer_tuple(gate.get("horizons_days")) != (5, 10)
        or _tuple(gate.get("allowed_decisions")) != LONG_SAMPLE_DECISIONS
        or gate.get("positive_stressed_validation_required") is not True
        or gate.get("positive_stressed_final_development_required") is not True
        or gate.get("positive_paired_uplift_validation_required") is not True
        or gate.get("positive_paired_uplift_final_development_required") is not True
        or _integer(gate.get("minimum_distinct_family_or_context_count")) != 2
        or gate.get("single_trade_domination_allowed") is not False
        or not _close(gate.get("maximum_single_trade_positive_profit_fraction"), 0.25)
        or gate.get("mll_within_frozen_tolerance_required") is not True
        or not _close(gate.get("maximum_mll_breach_rate"), 0.10)
        or gate.get("consistency_required") is not True
        or gate.get("complete_stressed_p5_or_p10_pass_or_material_progress_required") is not True
        or not _close(
            gate.get("minimum_material_stressed_target_progress_uplift"),
            MATERIAL_STRESSED_TARGET_PROGRESS_UPLIFT_MINIMUM,
        )
        or gate.get("material_progress_required_in_validation_and_final_development")
        is not True
        or gate.get("normal_and_stressed_scenarios_required") is not True
        or gate.get("full_coverage_denominators_required") is not True
        or gate.get("account_rule_snapshot_hash_required") is not True
        or gate.get("select_fastest_viable_account_size") is not True
        or gate.get("thresholds_may_change_after_results") is not False
        or gate.get("development_only") is not True
    ):
        raise SelectiveVetoManifestError("0034 account-speed gate drift")


def _multiplicity(manifest: Mapping[str, Any]) -> None:
    value = _mapping(manifest, "multiplicity")
    prior = _integer(value.get("prior_global_N_trials"))
    comparisons = _integer(value.get("prospective_comparisons"))
    reserved = _integer(value.get("reserved_delta_trials"))
    expected = _integer(value.get("expected_global_N_trials_after_reservation"))
    if (
        prior < 0
        or comparisons < 2
        or reserved < comparisons
        or expected != prior + reserved
        or not _close(value.get("campaign_specific_inflation"), 1.5)
        or value.get("reservation_required_before_outcome_access") is not True
        or value.get("proof_window_consumed") is not False
    ):
        raise SelectiveVetoManifestError("0034 multiplicity reservation drift")


def _evidence_and_governance(manifest: Mapping[str, Any], root: Path) -> None:
    evidence = _mapping(manifest, "evidence_bundle")
    governance = _mapping(manifest, "governance")
    destination = (root / str(evidence.get("destination") or "")).resolve()
    allowed = (root / "data/cache/evidence_bundles").resolve()
    if (
        evidence.get("required") is not True
        or evidence.get("atomic_single_writer_finalization") is not True
        or evidence.get("summary_only_complete_allowed") is not False
        or evidence.get("evidence_status") != "FRESH_DEVELOPMENT_EVIDENCE"
        or evidence.get("reconstruction_flag") is not False
        or evidence.get("exact_account_replay_required") is not True
        or evidence.get("sentinel_economic_records_allowed") is not False
        or evidence.get("paired_evidence_reconciliation_required") is not True
        or evidence.get("normal_stressed_episode_pairing_required") is not True
        or set(_tuple(evidence.get("required_datasets"))) != set(REQUIRED_DATASETS)
        or destination != allowed
    ):
        raise SelectiveVetoManifestError("0034 EvidenceBundle contract drift")
    forbidden = (
        "live_trading_allowed",
        "broker_connection_allowed",
        "orders_allowed",
        "q4_access_allowed",
        "new_mission_allowed",
        "new_service_allowed",
        "new_database_allowed",
        "new_registry_writer_allowed",
        "controller_version_change_required",
        "status_inheritance_allowed",
        "threshold_lowering_after_results_allowed",
        "broad_research_framework_allowed",
        "xfa_work_allowed",
    )
    if any(governance.get(key) is not False for key in forbidden):
        raise SelectiveVetoManifestError("0034 governance fail-closed contract drift")
    if (
        governance.get("new_data_purchase_allowed") is not True
        or governance.get("purchase_only_after_seed_gate") is not True
        or not _close(governance.get("maximum_incremental_spend_usd"), 8.0)
        or not _close(governance.get("minimum_budget_reserve_usd"), 20.0)
    ):
        raise SelectiveVetoManifestError("0034 conditional purchase governance drift")


def _hashed_sources(
    source: Mapping[str, Any], *, root: Path, required: frozenset[str], label: str
) -> None:
    values = _mapping(source, "source_hashes")
    if not required <= {str(key) for key in values}:
        raise SelectiveVetoManifestError(f"0034 {label} checksum closure is incomplete")
    for name, descriptor in values.items():
        if not isinstance(descriptor, Mapping):
            raise SelectiveVetoManifestError(f"0034 {label} descriptor absent: {name}")
        target = _project_file(root, descriptor.get("path"), f"{label} {name}")
        claimed = str(descriptor.get("sha256") or "")
        if not _SHA256.fullmatch(claimed) or _sha256(target) != claimed:
            raise SelectiveVetoManifestError(f"0034 {label} checksum drift: {name}")


def _project_root(path: Path) -> Path:
    if len(path.parents) < 3:
        raise SelectiveVetoManifestError("0034 manifest path is outside config/v7")
    root = path.parents[2]
    try:
        path.relative_to(root / "config/v7")
    except ValueError as exc:
        raise SelectiveVetoManifestError(
            "0034 manifest path must be under config/v7"
        ) from exc
    return root


def _project_file(root: Path, value: Any, label: str) -> Path:
    raw = str(value or "")
    if not raw:
        raise SelectiveVetoManifestError(f"0034 {label} path is invalid")
    target = (root / raw).resolve()
    try:
        target.relative_to(root)
    except ValueError as exc:
        raise SelectiveVetoManifestError(
            f"0034 {label} path escapes project root"
        ) from exc
    if not target.is_file():
        raise SelectiveVetoManifestError(f"0034 {label} file is missing")
    return target


def _mapping(value: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    result = value.get(key)
    if not isinstance(result, Mapping):
        raise SelectiveVetoManifestError(f"0034 mapping absent: {key}")
    return result


def _tuple(value: Any) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return ()
    return tuple(str(item) for item in value)


def _integer_tuple(value: Any) -> tuple[int, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return ()
    try:
        return tuple(_integer(item) for item in value)
    except SelectiveVetoManifestError:
        return ()


def _float_tuple(value: Any) -> tuple[float, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return ()
    try:
        return tuple(_finite(item) for item in value)
    except SelectiveVetoManifestError:
        return ()


def _integer(value: Any) -> int:
    if isinstance(value, bool):
        raise SelectiveVetoManifestError("0034 integer declaration is invalid")
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise SelectiveVetoManifestError("0034 integer declaration is invalid") from exc
    if isinstance(value, float) and not value.is_integer():
        raise SelectiveVetoManifestError("0034 integer declaration is invalid")
    return result


def _finite(value: Any) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise SelectiveVetoManifestError("0034 finite numeric declaration is invalid") from exc
    if not math.isfinite(result):
        raise SelectiveVetoManifestError("0034 finite numeric declaration is invalid")
    return result


def _close(value: Any, expected: float, *, tolerance: float = 1e-9) -> bool:
    try:
        return math.isclose(_finite(value), expected, rel_tol=0.0, abs_tol=tolerance)
    except SelectiveVetoManifestError:
        return False


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


__all__ = [
    "ACCOUNT_RULE_SNAPSHOTS",
    "ACCOUNT_SIZES_USD",
    "CAMPAIGN_ID",
    "CAMPAIGN_MODE",
    "CLASS_ID",
    "LONG_SAMPLE_DECISIONS",
    "MATERIAL_STRESSED_TARGET_PROGRESS_UPLIFT_MINIMUM",
    "OFFICIAL_RULE_SOURCE_URLS",
    "PRIMARY_ACTIONS",
    "PRIMARY_SEED_ID",
    "RUNTIME_VERSION",
    "SECONDARY_SEED_ID",
    "SEED_DECISIONS",
    "SEED_IDS",
    "SEED_STATUS",
    "STRUCTURAL_FAMILIES",
    "SelectiveVetoManifestError",
    "validate_selective_veto_manifest",
]
