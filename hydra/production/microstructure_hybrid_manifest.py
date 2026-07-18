"""Fail-closed manifest contract for HYDRA campaign 0033.

Campaign 0033 is a bounded hybrid experiment.  It may pair clean structural
opportunities from campaign 0028 with the immutable event-level information
sealed by campaigns 0031 and 0032.  It may not reinterpret a predecessor as
successful, expand the action/risk frontier after outcomes, access Q4, or buy
additional data before the frozen pilot gate authorises a costed extension.

This module validates a manifest only.  It does not reserve multiplicity,
write mission state, or execute economic work; those responsibilities remain
with the existing persistent controller and its single authoritative writer.
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
CAMPAIGN_MODE = "HYBRID_STRUCTURAL_ALPHA_ORDER_FLOW"
CAMPAIGN_ID = "hydra_hybrid_structural_alpha_order_flow_0033"
CLASS_ID = "HYBRID_STRUCTURAL_ALPHA_ORDER_FLOW_V1"
RUNTIME_VERSION = "hydra_hybrid_structural_alpha_order_flow_runtime_v1"

ACTION_IDS = ("A0", "A1", "A2", "A3", "A4", "A5")
ACTION_TYPES = (
    "BASELINE_IMMEDIATE",
    "ABSTAIN",
    "WAIT_FOR_FLOW_CONFIRMATION",
    "PULLBACK_OR_MARKETABLE_LIMIT",
    "PASSIVE_JOIN",
    "EARLY_INVALIDATION",
)
RISK_LEVELS = (0.5, 1.0, 1.5)
CHRONOLOGICAL_ROLE_COUNTS = (3, 1, 1)
MAXIMUM_STRUCTURAL_ANCHORS = 24
FROZEN_STRUCTURAL_ANCHOR_COUNT = 22
MAXIMUM_HYBRID_POLICIES = 20
ANCHOR_MARKETS = ("NQ", "YM")
ANCHOR_SESSION_DATES = (
    "2024-07-08",
    "2024-07-09",
    "2024-07-10",
    "2024-07-11",
    "2024-07-12",
)
PILOT_DECISIONS = (
    "HYBRID_OVERLAY_GREEN",
    "HYBRID_OVERLAY_WEAK",
    "HYBRID_OVERLAY_FALSIFIED",
)

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_GIT_SHA = re.compile(r"^[0-9a-f]{40}$")
_REQUIRED_IMPLEMENTATION_FILES = frozenset(
    {
        "hydra/production/microstructure_hybrid_manifest.py",
        "hydra/production/microstructure_hybrid_pilot.py",
        "hydra/production/microstructure_hybrid_runtime.py",
        "hydra/production/manifest.py",
        "hydra/production/runtime.py",
        "scripts/run_economic_production_manifest.py",
    }
)
_REQUIRED_0032_SOURCES = frozenset(
    {
        "authoritative_result",
        "evidence_bundle_receipt",
        "decision_report",
        "opportunity_episodes",
        "opportunity_outcomes",
    }
)
_REQUIRED_0031_STORE_SOURCES = frozenset(
    {
        "event_store_receipt",
        "raw_dbn",
        "book_snapshots",
        "derived_events",
        "feature_matrices",
        "outcome_labels",
    }
)
_REQUIRED_0028_ANCHOR_SOURCES = frozenset(
    {
        "source_result",
        "candidate_population",
    }
)


class HybridManifestError(RuntimeError):
    """The campaign-0033 preregistration is incomplete, unsafe, or drifted."""


def validate_microstructure_hybrid_manifest(
    manifest: Mapping[str, Any], *, manifest_path: str | Path
) -> None:
    """Validate the immutable scientific and operational contract for 0033."""

    path = Path(manifest_path).resolve()
    root = _project_root(path)
    _identity(manifest)
    _implementation(manifest, root)
    _source_0032(manifest, root)
    _source_store_0031(manifest, root)
    _structural_anchors_0028(manifest, root)
    _runtime_and_compute(manifest, root)
    _causal_episode_contract(manifest)
    _paired_action_frontier(manifest)
    _chronological_roles(manifest)
    _pilot_gate(manifest)
    _conditional_extension(manifest)
    _multiplicity(manifest)
    _evidence_and_governance(manifest, root)


def _identity(manifest: Mapping[str, Any]) -> None:
    try:
        created = datetime.fromisoformat(
            str(manifest.get("created_at_utc") or "").replace("Z", "+00:00")
        )
    except ValueError as exc:
        raise HybridManifestError("0033 freeze timestamp is invalid") from exc
    claimed_hash = str(manifest.get("manifest_hash") or "")
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
        or not _SHA256.fullmatch(claimed_hash)
        or stable_hash(payload) != claimed_hash
    ):
        raise HybridManifestError("0033 identity or semantic hash drift")


def _implementation(manifest: Mapping[str, Any], root: Path) -> None:
    files = _mapping(manifest, "implementation_files")
    declared = {str(key) for key in files}
    if not _REQUIRED_IMPLEMENTATION_FILES <= declared:
        raise HybridManifestError("0033 implementation closure is incomplete")
    for relative, claimed_raw in files.items():
        target = _project_file(root, relative, "implementation")
        claimed = str(claimed_raw or "")
        if not _SHA256.fullmatch(claimed) or _sha256(target) != claimed:
            raise HybridManifestError(
                f"0033 implementation checksum drift: {relative}"
            )


def _source_0032(manifest: Mapping[str, Any], root: Path) -> None:
    source = _mapping(manifest, "terminal_source_0032")
    if (
        source.get("campaign_id")
        != "hydra_microstructure_sparse_alpha_distillation_0032"
        or source.get("terminal_status") != "SPARSE_PILOT_WEAK"
        or source.get("reuse_mode") != "IMMUTABLE_READ_ONLY"
        or source.get("retry_or_retune_allowed") is not False
        or source.get("status_inheritance_allowed") is not False
        or source.get("outcomes_are_development_only") is not True
    ):
        raise HybridManifestError("0033 terminal 0032 source drift")
    _hashed_sources(
        source,
        root=root,
        required=_REQUIRED_0032_SOURCES,
        label="0032 source",
    )


def _source_store_0031(manifest: Mapping[str, Any], root: Path) -> None:
    source = _mapping(manifest, "immutable_source_store_0031")
    if (
        source.get("campaign_id")
        != "hydra_microstructure_order_flow_foundry_0031"
        or source.get("terminal_status") != "MICROSTRUCTURE_PILOT_FALSIFIED"
        or source.get("store_status") != "BOOK_STATE_RECONSTRUCTION_GREEN"
        or source.get("reuse_mode") != "IMMUTABLE_READ_ONLY"
        or source.get("raw_rewrite_allowed") is not False
        or source.get("source_feature_recomputation_allowed") is not False
        or source.get("outcome_labels_physically_separate") is not True
        or source.get("status_inheritance_allowed") is not False
    ):
        raise HybridManifestError("0033 immutable 0031 store drift")
    _hashed_sources(
        source,
        root=root,
        required=_REQUIRED_0031_STORE_SOURCES,
        label="0031 store source",
    )


def _structural_anchors_0028(manifest: Mapping[str, Any], root: Path) -> None:
    anchors = _mapping(manifest, "clean_structural_anchors_0028")
    anchor_ids = _tuple(anchors.get("anchor_ids"))
    event_ledgers = _mapping(anchors, "event_ledgers")
    if (
        anchors.get("campaign_id") != "hydra_causal_target_velocity_0028"
        or anchors.get("terminal_status")
        != "CAUSAL_TARGET_VELOCITY_INCONCLUSIVE_COVERAGE_LIMITED"
        or anchors.get("component_status")
        != "LOW_VELOCITY_CAUSAL_REFERENCE_BANK"
        or anchors.get("reuse_mode") != "IMMUTABLE_READ_ONLY"
        or _integer(anchors.get("maximum_anchor_count"))
        != MAXIMUM_STRUCTURAL_ANCHORS
        or _integer(anchors.get("frozen_anchor_count"))
        != FROZEN_STRUCTURAL_ANCHOR_COUNT
        or len(anchor_ids) != FROZEN_STRUCTURAL_ANCHOR_COUNT
        or len(set(str(value) for value in anchor_ids)) != len(anchor_ids)
        or any(not str(value).startswith("hazard_") for value in anchor_ids)
        or _tuple(anchors.get("markets")) != ANCHOR_MARKETS
        or _tuple(anchors.get("coverage_session_dates")) != ANCHOR_SESSION_DATES
        or set(str(value) for value in anchor_ids)
        != {str(value) for value in event_ledgers}
        or anchors.get("anchor_selection_frozen_before_hybrid_outcomes") is not True
        or anchors.get("promotion_status_inheritance_allowed") is not False
    ):
        raise HybridManifestError("0033 clean 0028 anchor contract drift")
    _hashed_sources(
        anchors,
        root=root,
        required=_REQUIRED_0028_ANCHOR_SOURCES,
        label="0028 anchor source",
    )
    for candidate_id, raw in event_ledgers.items():
        if not isinstance(raw, Mapping):
            raise HybridManifestError(
                f"0033 0028 event-ledger descriptor absent: {candidate_id}"
            )
        active_dates = _tuple(raw.get("active_session_dates"))
        if (
            str(raw.get("candidate_id") or "") != str(candidate_id)
            or str(raw.get("market") or "") not in ANCHOR_MARKETS
            or not active_dates
            or not set(str(value) for value in active_dates).issubset(
                ANCHOR_SESSION_DATES
            )
            or _integer(raw.get("event_count_in_window")) < 1
        ):
            raise HybridManifestError(
                f"0033 0028 event-ledger identity drift: {candidate_id}"
            )
        target = _project_file(
            root,
            raw.get("path"),
            f"0028 stage2 event ledger {candidate_id}",
        )
        claimed = str(raw.get("sha256") or "")
        if not _SHA256.fullmatch(claimed) or _sha256(target) != claimed:
            raise HybridManifestError(
                f"0033 0028 event-ledger checksum drift: {candidate_id}"
            )


def _runtime_and_compute(manifest: Mapping[str, Any], root: Path) -> None:
    runtime = _mapping(manifest, "runtime")
    compute = _mapping(manifest, "compute_contract")
    output = (root / str(runtime.get("output_dir") or "")).resolve()
    allowed = (root / "reports/economic_evolution").resolve()
    if (
        runtime.get("engine") != "production_kernel_v1"
        or runtime.get("runner") != "scripts/run_economic_production_manifest.py"
        or runtime.get("hybrid_runtime_version") != RUNTIME_VERSION
        or runtime.get("result_schema") != "hydra_economic_production_result_v1"
        or runtime.get("result_name") != "economic_production_result.json"
        or runtime.get("controller_source_change_required") is not False
        or runtime.get("resume_from_checkpoint") is not True
        or _integer(runtime.get("orchestrator_count")) != 1
        or _integer(runtime.get("worker_count")) != 2
        or _integer(runtime.get("asynchronous_evidence_writer_count")) != 1
        or output == allowed
        or allowed not in output.parents
        or _integer(compute.get("orchestrator_count")) != 1
        or _integer(compute.get("cpu_worker_count")) != 2
        or _integer(compute.get("authoritative_writer_count")) != 1
        or compute.get("cpu_workers_read_only") is not True
        or compute.get("single_writer_atomic_commits") is not True
        or compute.get("oversubscription_allowed") is not False
    ):
        raise HybridManifestError("0033 runtime/compute topology drift")


def _causal_episode_contract(manifest: Mapping[str, Any]) -> None:
    contract = _mapping(manifest, "structural_opportunity_episode_contract")
    required_timestamps = (
        "event_time",
        "available_at",
        "decision_time",
        "order_submit_time",
        "earliest_executable_time",
        "fill_time",
    )
    if (
        contract.get("schema") != "hydra_structural_opportunity_episode_v1"
        or contract.get("availability_rule") != "available_at<=decision_time"
        or _tuple(contract.get("persist_timestamps")) != required_timestamps
        or contract.get("future_outcomes_are_labels_only") is not True
        or contract.get("outcome_labels_physically_separate") is not True
        or contract.get("post_confirmation_episode_fields_in_decision_allowed")
        is not False
        or contract.get("future_label_availability_in_eligibility") is not False
        or contract.get("one_decision_per_episode") is not True
        or contract.get("duplicate_episode_ids_allowed") is not False
        or contract.get("batch_streaming_decision_equality_required") is not True
        or contract.get("missing_future_coverage_status")
        != "CENSORED_FUTURE_COVERAGE"
        or contract.get("censored_suppresses_decision") is not False
    ):
        raise HybridManifestError("0033 causal structural-episode contract drift")


def _paired_action_frontier(manifest: Mapping[str, Any]) -> None:
    frontier = _mapping(manifest, "paired_action_frontier")
    action_ids = _tuple(frontier.get("action_ids"))
    action_specs = _tuple(frontier.get("actions"))
    spec_ids = tuple(
        str(value.get("action_id")) if isinstance(value, Mapping) else ""
        for value in action_specs
    )
    spec_types = tuple(
        str(value.get("action_type")) if isinstance(value, Mapping) else ""
        for value in action_specs
    )
    side_lane_flags = tuple(
        value.get("side_lane_only") if isinstance(value, Mapping) else None
        for value in action_specs
    )
    if (
        action_ids != ACTION_IDS
        or spec_ids != ACTION_IDS
        or spec_types != ACTION_TYPES
        or side_lane_flags != (False, False, False, False, True, False)
        or len({str(value) for value in spec_ids}) != len(ACTION_IDS)
        or any(
            not isinstance(value, Mapping)
            or not str(value.get("description") or "").strip()
            for value in action_specs
        )
        or _float_tuple(frontier.get("risk_levels")) != RISK_LEVELS
        or _integer(frontier.get("maximum_policy_count"))
        != MAXIMUM_HYBRID_POLICIES
        or frontier.get("paired_on_identical_episode") is not True
        or frontier.get("paired_on_identical_start_and_costs") is not True
        or frontier.get("action_selected_from_decision_time_features_only") is not True
        or frontier.get("abstention_has_zero_exposure_and_cost") is not True
        or frontier.get("continuous_risk_optimization_allowed") is not False
        or frontier.get("neighbor_action_generation_allowed") is not False
        or frontier.get("frontier_frozen_before_outcomes") is not True
    ):
        raise HybridManifestError("0033 paired action/risk frontier drift")


def _chronological_roles(manifest: Mapping[str, Any]) -> None:
    roles = _mapping(manifest, "chronological_roles")
    counts = (
        _integer(roles.get("discovery_sessions")),
        _integer(roles.get("validation_sessions")),
        _integer(roles.get("final_development_sessions")),
    )
    if (
        counts != CHRONOLOGICAL_ROLE_COUNTS
        or roles.get("random_temporal_mixing_allowed") is not False
        or roles.get("roles_frozen_before_outcomes") is not True
        or roles.get("validation_or_final_used_for_thresholds") is not False
        or roles.get("final_development_is_independent_confirmation") is not False
    ):
        raise HybridManifestError("0033 chronological role contract drift")


def _pilot_gate(manifest: Mapping[str, Any]) -> None:
    gate = _mapping(manifest, "development_gate")
    if (
        _tuple(gate.get("allowed_decisions")) != PILOT_DECISIONS
        or gate.get("thresholds_may_change_after_results") is not False
        or gate.get("development_only") is not True
        or gate.get("independent_confirmation_claim_allowed") is not False
        or gate.get("positive_stressed_economics_required") is not True
        or gate.get("material_uplift_over_structural_anchor_required") is not True
        or gate.get("acceptable_mll_and_consistency_required") is not True
        or gate.get("final_development_evidence_required") is not True
    ):
        raise HybridManifestError("0033 development decision gate drift")


def _conditional_extension(manifest: Mapping[str, Any]) -> None:
    extension = _mapping(manifest, "conditional_extension")
    weak = _mapping(extension, "weak_qualification")
    maximum = _finite(extension.get("maximum_incremental_spend_usd"))
    reserve = _finite(extension.get("minimum_budget_reserve_usd"))
    available = _finite(extension.get("current_remaining_budget_usd"))
    if (
        extension.get("enabled_before_qualified_gate") is not False
        or _tuple(extension.get("trigger_decisions"))
        != ("HYBRID_OVERLAY_GREEN", "HYBRID_OVERLAY_WEAK")
        or extension.get("weak_qualification_required") is not True
        or weak.get("positive_paired_uplift_validation") is not True
        or weak.get("positive_paired_uplift_final_development") is not True
        or _integer(
            weak.get("minimum_near_break_even_stressed_strategy_count")
        )
        != 1
        or maximum < 0.0
        or maximum > 3.25
        or reserve < 25.0
        or available - maximum < reserve
        or _integer(extension.get("maximum_extension_count")) != 1
        or extension.get("official_cost_estimate_required_before_purchase") is not True
        or extension.get("automatic_purchase_allowed") is not False
        or extension.get("broad_historical_purchase_allowed") is not False
        or extension.get("q4_access_allowed") is not False
    ):
        raise HybridManifestError("0033 conditional data extension drift")


def _multiplicity(manifest: Mapping[str, Any]) -> None:
    multiplicity = _mapping(manifest, "multiplicity")
    prior = _integer(multiplicity.get("prior_global_N_trials"))
    prospective = _integer(multiplicity.get("prospective_comparisons"))
    delta = _integer(multiplicity.get("reserved_delta_trials"))
    expected = _integer(
        multiplicity.get("expected_global_N_trials_after_reservation")
    )
    inflation = _finite(multiplicity.get("campaign_specific_inflation"))
    if (
        min(prior, prospective, delta, expected) < 0
        or prospective != MAXIMUM_HYBRID_POLICIES
        or not math.isclose(inflation, 1.5, rel_tol=0.0, abs_tol=1e-12)
        or delta != math.ceil(prospective * inflation)
        or expected != prior + delta
        or multiplicity.get("reservation_required_before_outcome_access") is not True
        or multiplicity.get("proof_window_consumed") is not False
    ):
        raise HybridManifestError("0033 multiplicity reservation drift")


def _evidence_and_governance(manifest: Mapping[str, Any], root: Path) -> None:
    evidence = _mapping(manifest, "evidence_bundle")
    governance = _mapping(manifest, "governance")
    destination = (root / str(evidence.get("destination") or "")).resolve()
    if (
        evidence.get("required") is not True
        or evidence.get("atomic_single_writer_finalization") is not True
        or evidence.get("summary_only_complete_allowed") is not False
        or evidence.get("evidence_status") != "FRESH_DEVELOPMENT_EVIDENCE"
        or evidence.get("reconstruction_flag") is not False
        or _tuple(evidence.get("required_datasets")) != tuple(REQUIRED_DATASETS)
        or root not in destination.parents
        or governance.get("live_trading_allowed") is not False
        or governance.get("broker_connection_allowed") is not False
        or governance.get("orders_allowed") is not False
        or governance.get("q4_access_allowed") is not False
        or governance.get("new_mission_allowed") is not False
        or governance.get("new_service_allowed") is not False
        or governance.get("new_database_allowed") is not False
        or governance.get("new_registry_writer_allowed") is not False
        or governance.get("controller_version_change_required") is not False
        or governance.get("status_inheritance_allowed") is not False
        or governance.get("threshold_lowering_after_results_allowed") is not False
    ):
        raise HybridManifestError("0033 evidence or governance drift")


def _hashed_sources(
    source: Mapping[str, Any],
    *,
    root: Path,
    required: frozenset[str],
    label: str,
) -> None:
    sources = _mapping(source, "source_hashes")
    if not required <= {str(key) for key in sources}:
        raise HybridManifestError(f"0033 {label} checksum closure is incomplete")
    for name, raw in sources.items():
        if not isinstance(raw, Mapping):
            raise HybridManifestError(f"0033 {label} descriptor absent: {name}")
        target = _project_file(root, raw.get("path"), f"{label} {name}")
        claimed = str(raw.get("sha256") or "")
        if not _SHA256.fullmatch(claimed) or _sha256(target) != claimed:
            raise HybridManifestError(f"0033 {label} checksum drift: {name}")


def _project_root(path: Path) -> Path:
    try:
        root = path.parents[2]
    except IndexError as exc:
        raise HybridManifestError("0033 manifest path is outside config/v7") from exc
    if path.parent.name != "v7" or path.parent.parent.name != "config":
        raise HybridManifestError("0033 manifest path must be under config/v7")
    return root


def _mapping(parent: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    value = parent.get(key)
    if not isinstance(value, Mapping):
        raise HybridManifestError(f"0033 mapping absent: {key}")
    return value


def _tuple(value: Any) -> tuple[Any, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return ()
    return tuple(value)


def _float_tuple(value: Any) -> tuple[float, ...]:
    try:
        return tuple(_finite(item) for item in _tuple(value))
    except HybridManifestError:
        return ()


def _integer(value: Any) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        return -1
    return value


def _finite(value: Any) -> float:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise HybridManifestError("0033 finite numeric declaration is invalid")
    result = float(value)
    if not math.isfinite(result):
        raise HybridManifestError("0033 finite numeric declaration is invalid")
    return result


def _project_file(root: Path, value: Any, label: str) -> Path:
    if not isinstance(value, str) or not value or Path(value).is_absolute():
        raise HybridManifestError(f"0033 {label} path is invalid")
    target = (root / value).resolve()
    try:
        target.relative_to(root)
    except ValueError as exc:
        raise HybridManifestError(f"0033 {label} path escapes project root") from exc
    if not target.is_file():
        raise HybridManifestError(f"0033 {label} file is missing")
    return target


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = [
    "ACTION_IDS",
    "ACTION_TYPES",
    "ANCHOR_MARKETS",
    "ANCHOR_SESSION_DATES",
    "CAMPAIGN_ID",
    "CAMPAIGN_MODE",
    "CHRONOLOGICAL_ROLE_COUNTS",
    "CLASS_ID",
    "FROZEN_STRUCTURAL_ANCHOR_COUNT",
    "HybridManifestError",
    "MANIFEST_SCHEMA",
    "MAXIMUM_HYBRID_POLICIES",
    "MAXIMUM_STRUCTURAL_ANCHORS",
    "PILOT_DECISIONS",
    "RISK_LEVELS",
    "RUNTIME_VERSION",
    "validate_microstructure_hybrid_manifest",
]
