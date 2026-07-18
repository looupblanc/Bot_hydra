"""Fail-closed manifest contract for HYDRA campaign 0032.

Campaign 0032 is a bounded reuse of the immutable campaign-0031 event store.
It may distil sparse, causal opportunities from that store, but it may not
reinterpret 0031 as successful, mutate its source evidence, access Q4, or
silently expand the data budget.  The concrete manifest is intentionally kept
separate from this validator so that it can be frozen only after every source
and implementation hash is known.
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
CAMPAIGN_MODE = "MICROSTRUCTURE_SPARSE_ALPHA_DISTILLATION"
CAMPAIGN_ID = "hydra_microstructure_sparse_alpha_distillation_0032"
CLASS_ID = "MICROSTRUCTURE_SPARSE_ALPHA_DISTILLATION_V1"
RUNTIME_VERSION = "hydra_microstructure_sparse_alpha_runtime_v1"

EDGE_TO_COST_RATIOS = (1.25, 1.5, 2.0, 3.0)
TRADE_BUDGETS_PER_SESSION = (2, 4, 8, 12)
HOLDING_HORIZONS_SECONDS = (30, 120, 300, 900)
ACCOUNT_HORIZONS_DAYS = (5, 10, 20)
EXIT_TYPES = (
    "FIXED_TARGET_STOP",
    "ORDER_FLOW_DECAY",
    "OPPOSITE_STATE_TRANSITION",
    "TIME_STOP",
    "VWAP_LIQUIDITY_LEVEL",
    "EVENT_STATE_RESET",
)
GATE_DECISIONS = (
    "SPARSE_PILOT_GREEN",
    "SPARSE_PILOT_WEAK",
    "SPARSE_PILOT_FALSIFIED",
)
MODEL_CLASSES = (
    "REGULARIZED_LOGISTIC_REGRESSION",
    "SHALLOW_DECISION_TREE",
    "MONOTONIC_GRADIENT_BOOSTING",
)
ACCOUNT_SIZES_USD = (50_000, 100_000, 150_000)

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_GIT_SHA = re.compile(r"^[0-9a-f]{40}$")
_REQUIRED_IMPLEMENTATION_FILES = frozenset(
    {
        "hydra/production/microstructure_sparse_manifest.py",
        "scripts/run_economic_production_manifest.py",
    }
)
_REQUIRED_SOURCE_OBJECTS = frozenset(
    {
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
    }
)


class SparseManifestError(RuntimeError):
    """The campaign-0032 preregistration is incomplete, unsafe, or drifted."""


def validate_microstructure_sparse_manifest(
    manifest: Mapping[str, Any], *, manifest_path: str | Path
) -> None:
    """Validate the complete immutable scientific and operational contract.

    The function is deliberately non-mutating.  Multiplicity reservation and
    evidence finalization remain the responsibility of the single persistent
    mission writer.
    """

    path = Path(manifest_path).resolve()
    if len(path.parents) < 3:
        raise SparseManifestError("0032 manifest path is outside config/v7")
    root = path.parents[2]
    _identity(manifest)
    _implementation(manifest, root)
    _terminal_baseline(manifest)
    _source_store(manifest, root)
    _runtime(manifest, root)
    _forensic_bridge(manifest)
    _opportunity_episode_contract(manifest)
    _finite_state_engine(manifest)
    _meta_labeling(manifest)
    _execution_model(manifest)
    _frontiers(manifest)
    _development_gate(manifest)
    _conditional_extension(manifest)
    _multiplicity(manifest)
    _evidence_and_governance(manifest, root)


def _identity(manifest: Mapping[str, Any]) -> None:
    try:
        created = datetime.fromisoformat(
            str(manifest.get("created_at_utc") or "").replace("Z", "+00:00")
        )
    except ValueError as exc:
        raise SparseManifestError("0032 freeze timestamp is invalid") from exc
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
        raise SparseManifestError("0032 identity or semantic hash drift")


def _implementation(manifest: Mapping[str, Any], root: Path) -> None:
    files = _mapping(manifest, "implementation_files")
    if not _REQUIRED_IMPLEMENTATION_FILES <= set(str(key) for key in files):
        raise SparseManifestError("0032 implementation closure is incomplete")
    for relative, claimed in files.items():
        target = _project_file(root, relative, "implementation")
        if not _SHA256.fullmatch(str(claimed or "")) or _sha256(target) != claimed:
            raise SparseManifestError(
                f"0032 implementation checksum drift: {relative}"
            )


def _terminal_baseline(manifest: Mapping[str, Any]) -> None:
    baseline = _mapping(manifest, "terminal_baseline_0031")
    expected: Mapping[str, Any] = {
        "campaign_id": "hydra_microstructure_order_flow_foundry_0031",
        "terminal_status": "MICROSTRUCTURE_PILOT_FALSIFIED",
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
        "mass_scale_started": False,
        "xfa_paths": 0,
        "retry_or_retune_allowed": False,
        "status_inheritance_allowed": False,
    }
    if any(baseline.get(key) != value for key, value in expected.items()):
        raise SparseManifestError("0032 terminal 0031 baseline drift")
    if (
        not _close(baseline.get("actual_spend_usd"), 8.648637887836)
        or not _close(
            baseline.get("remaining_budget_usd"), 28.498462508622012
        )
        or not _SHA256.fullmatch(str(baseline.get("manifest_hash") or ""))
        or not _SHA256.fullmatch(
            str(baseline.get("authoritative_bundle_content_sha256") or "")
        )
        or not _SHA256.fullmatch(
            str(baseline.get("account_terminal_recovery_receipt_hash") or "")
        )
    ):
        raise SparseManifestError("0032 terminal 0031 provenance drift")


def _source_store(manifest: Mapping[str, Any], root: Path) -> None:
    store = _mapping(manifest, "source_store")
    sources = _mapping(store, "source_hashes")
    if (
        store.get("source_campaign_id")
        != "hydra_microstructure_order_flow_foundry_0031"
        or store.get("reuse_mode") != "IMMUTABLE_READ_ONLY"
        or store.get("raw_rewrite_allowed") is not False
        or store.get("source_feature_recomputation_allowed") is not False
        or store.get("outcome_labels_physically_separate") is not True
        or store.get("source_status_inheritance_allowed") is not False
        or not _REQUIRED_SOURCE_OBJECTS <= set(str(key) for key in sources)
    ):
        raise SparseManifestError("0032 immutable source-store contract drift")
    for label, value in sources.items():
        if not isinstance(value, Mapping):
            raise SparseManifestError(f"0032 source descriptor absent: {label}")
        target = _project_file(root, value.get("path"), f"source {label}")
        claimed = str(value.get("sha256") or "")
        if not _SHA256.fullmatch(claimed) or _sha256(target) != claimed:
            raise SparseManifestError(f"0032 source checksum drift: {label}")


def _runtime(manifest: Mapping[str, Any], root: Path) -> None:
    runtime = _mapping(manifest, "runtime")
    compute = _mapping(manifest, "compute_contract")
    output = (root / str(runtime.get("output_dir") or "")).resolve()
    allowed = (root / "reports/economic_evolution").resolve()
    if (
        runtime.get("engine") != "production_kernel_v1"
        or runtime.get("runner") != "scripts/run_economic_production_manifest.py"
        or runtime.get("sparse_runtime_version") != RUNTIME_VERSION
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
        raise SparseManifestError("0032 runtime/compute topology drift")


def _forensic_bridge(manifest: Mapping[str, Any]) -> None:
    bridge = _mapping(manifest, "forensic_bridge")
    if (
        bridge.get("required_before_sparse_outcomes") is not True
        or bridge.get("source_signal_trade_episode_reconciliation") is not True
        or bridge.get("gross_cost_net_arithmetic_reconciliation") is not True
        or bridge.get("account_terminal_precedence_preserved") is not True
        or bridge.get("post_mll_path_truncation_preserved") is not True
        or bridge.get("source_features_signals_trades_mutable") is not False
        or bridge.get("source_result_reinterpretation_allowed") is not False
        or bridge.get("bridge_is_new_economic_evidence") is not False
    ):
        raise SparseManifestError("0032 forensic bridge drift")


def _opportunity_episode_contract(manifest: Mapping[str, Any]) -> None:
    contract = _mapping(manifest, "opportunity_episode_contract")
    if (
        contract.get("availability_rule") != "available_at<=decision_time"
        or contract.get("future_outcomes_are_labels_only") is not True
        or contract.get("future_label_availability_in_eligibility") is not False
        or contract.get("negative_shift_in_decision_code") is not False
        or contract.get("one_decision_per_opportunity") is not True
        or contract.get("duplicate_opportunities_allowed") is not False
        or contract.get("missing_future_coverage_status")
        != "CENSORED_FUTURE_COVERAGE"
        or contract.get("censored_in_headline_denominator") is not False
        or contract.get("chronological_roles_frozen_before_outcomes") is not True
    ):
        raise SparseManifestError("0032 causal opportunity/episode contract drift")


def _finite_state_engine(manifest: Mapping[str, Any]) -> None:
    engine = _mapping(manifest, "finite_state_engine")
    expected_states = (
        "FLAT",
        "ARMED",
        "ENTRY_PENDING",
        "OPEN",
        "EXIT_PENDING",
        "COOLDOWN",
    )
    if (
        _tuple(engine.get("states")) != expected_states
        or engine.get("single_authoritative_step") is not True
        or engine.get("batch_streaming_decision_equality") is not True
        or engine.get("event_time_sequence_required") is not True
        or engine.get("restart_resume_idempotent") is not True
        or engine.get("duplicate_event_idempotent") is not True
        or engine.get("future_state_access_allowed") is not False
    ):
        raise SparseManifestError("0032 finite-state engine drift")


def _meta_labeling(manifest: Mapping[str, Any]) -> None:
    labeling = _mapping(manifest, "meta_labeling")
    if (
        labeling.get("teacher_source") != "CAMPAIGN_0031_MBO_OUTCOME_LABELS"
        or labeling.get("student_source") != "CAMPAIGN_0031_CAUSAL_FEATURE_MATRICES"
        or _tuple(labeling.get("model_classes")) != MODEL_CLASSES
        or labeling.get("chronological_cross_fit") is not True
        or labeling.get("random_temporal_mixing_allowed") is not False
        or labeling.get("teacher_fields_at_inference_allowed") is not False
        or labeling.get("classification_accuracy_alone_promotable") is not False
        or labeling.get("economic_utility_required") is not True
    ):
        raise SparseManifestError("0032 causal meta-labeling contract drift")


def _execution_model(manifest: Mapping[str, Any]) -> None:
    execution = _mapping(manifest, "execution_model")
    if (
        execution.get("decision_after_completed_event") is not True
        or execution.get("earliest_fill_after_decision") is not True
        or execution.get("touch_implies_fill") is not False
        or execution.get("partial_fills_modeled") is not True
        or execution.get("available_depth_enforced") is not True
        or execution.get("fees_and_slippage_frozen") is not True
        or not _close(execution.get("normal_cost_multiplier"), 1.0)
        or not _close(execution.get("stressed_cost_multiplier"), 1.5)
        or execution.get("sub_millisecond_latency_arbitrage_allowed") is not False
    ):
        raise SparseManifestError("0032 execution model drift")


def _frontiers(manifest: Mapping[str, Any]) -> None:
    sparse = _mapping(manifest, "sparse_policy_frontier")
    exits = _mapping(manifest, "holding_exit_frontier")
    account = _mapping(manifest, "account_evaluation")
    sizes = _mapping(manifest, "account_size_frontier")
    if (
        _float_tuple(sparse.get("edge_to_cost_ratios")) != EDGE_TO_COST_RATIOS
        or _int_tuple(sparse.get("trade_budgets_per_session"))
        != TRADE_BUDGETS_PER_SESSION
        or _integer(sparse.get("max_strategies")) != 30
        or sparse.get("continuous_threshold_optimization_allowed") is not False
        or sparse.get("parameter_clone_admission_allowed") is not False
        or sparse.get("frontier_frozen_before_outcomes") is not True
        or _int_tuple(exits.get("horizons_seconds")) != HOLDING_HORIZONS_SECONDS
        or _tuple(exits.get("exit_types")) != EXIT_TYPES
        or exits.get("event_state_reset_is_causal") is not True
        or exits.get("unrestricted_exit_search_allowed") is not False
        or _int_tuple(account.get("horizons_days")) != ACCOUNT_HORIZONS_DAYS
        or _tuple(account.get("cost_scenarios"))
        != ("NORMAL", "STRESSED_1_5X")
        or account.get("full_coverage_only_headline") is not True
        or account.get("overlapping_windows_independent") is not False
        or account.get("xfa_enabled") is not False
        or _int_tuple(sizes.get("account_sizes_usd")) != ACCOUNT_SIZES_USD
        or sizes.get("official_rule_snapshot_per_size_required") is not True
        or sizes.get("legal_contract_limits_required") is not True
        or sizes.get("selected_after_final_outcomes") is not False
    ):
        raise SparseManifestError("0032 sparse/exit/account frontier drift")


def _development_gate(manifest: Mapping[str, Any]) -> None:
    gate = _mapping(manifest, "development_gate")
    green = _mapping(gate, "green_requirements")
    if (
        _tuple(gate.get("allowed_decisions")) != GATE_DECISIONS
        or gate.get("thresholds_may_change_after_results") is not False
        or gate.get("development_only") is not True
        or gate.get("independent_confirmation_claim_allowed") is not False
        or green.get("material_target_velocity_uplift") is not True
        or green.get("positive_stressed_economics") is not True
        or green.get("acceptable_mll_and_consistency") is not True
        or green.get("final_development_evidence") is not True
        or green.get("deployable_causal_strategy") is not True
        or gate.get("weak_requires_information_uplift") is not True
        or gate.get("falsified_when_no_material_uplift") is not True
    ):
        raise SparseManifestError("0032 development decision gate drift")


def _conditional_extension(manifest: Mapping[str, Any]) -> None:
    extension = _mapping(manifest, "conditional_extension")
    maximum = _finite(extension.get("maximum_incremental_spend_usd"))
    reserve = _finite(extension.get("minimum_budget_reserve_usd"))
    available = _finite(extension.get("current_remaining_budget_usd"))
    if (
        extension.get("enabled") is not True
        or _tuple(extension.get("trigger_decisions"))
        != ("SPARSE_PILOT_GREEN", "SPARSE_PILOT_WEAK")
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
        raise SparseManifestError("0032 conditional data extension drift")


def _multiplicity(manifest: Mapping[str, Any]) -> None:
    multiplicity = _mapping(manifest, "multiplicity")
    prior = _integer(multiplicity.get("prior_global_N_trials"))
    prospective = _integer(multiplicity.get("prospective_comparisons"))
    delta = _integer(multiplicity.get("reserved_delta_trials"))
    expected = _integer(multiplicity.get("expected_global_N_trials_after_reservation"))
    inflation = _finite(multiplicity.get("campaign_specific_inflation"))
    if (
        min(prior, prospective, delta, expected) < 0
        or prospective != 30
        or not math.isclose(inflation, 1.5, rel_tol=0.0, abs_tol=1e-12)
        or delta != math.ceil(prospective * inflation)
        or prior + delta != expected
        or multiplicity.get("reservation_required_before_outcome_access") is not True
        or multiplicity.get("proof_window_consumed") is not False
    ):
        raise SparseManifestError("0032 multiplicity reservation drift")


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
        or governance.get("xfa_before_clean_combine_survivors_allowed") is not False
    ):
        raise SparseManifestError("0032 evidence or governance drift")


def _mapping(parent: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    value = parent.get(key)
    if not isinstance(value, Mapping):
        raise SparseManifestError(f"0032 mapping absent: {key}")
    return value


def _tuple(value: Any) -> tuple[Any, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return ()
    return tuple(value)


def _float_tuple(value: Any) -> tuple[float, ...]:
    try:
        return tuple(_finite(item) for item in _tuple(value))
    except SparseManifestError:
        return ()


def _int_tuple(value: Any) -> tuple[int, ...]:
    return tuple(_integer(item) for item in _tuple(value))


def _integer(value: Any) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        return -1
    return value


def _finite(value: Any) -> float:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise SparseManifestError("0032 finite numeric declaration is invalid")
    result = float(value)
    if not math.isfinite(result):
        raise SparseManifestError("0032 finite numeric declaration is invalid")
    return result


def _close(value: Any, expected: float) -> bool:
    try:
        return math.isclose(
            _finite(value), expected, rel_tol=0.0, abs_tol=1e-12
        )
    except SparseManifestError:
        return False


def _project_file(root: Path, value: Any, label: str) -> Path:
    if not isinstance(value, str) or not value or Path(value).is_absolute():
        raise SparseManifestError(f"0032 {label} path is invalid")
    target = (root / value).resolve()
    try:
        target.relative_to(root)
    except ValueError as exc:
        raise SparseManifestError(f"0032 {label} path escapes project root") from exc
    if not target.is_file():
        raise SparseManifestError(f"0032 {label} file is missing")
    return target


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = [
    "ACCOUNT_HORIZONS_DAYS",
    "CAMPAIGN_ID",
    "CAMPAIGN_MODE",
    "CLASS_ID",
    "EDGE_TO_COST_RATIOS",
    "EXIT_TYPES",
    "GATE_DECISIONS",
    "HOLDING_HORIZONS_SECONDS",
    "RUNTIME_VERSION",
    "SparseManifestError",
    "TRADE_BUDGETS_PER_SESSION",
    "validate_microstructure_sparse_manifest",
]
