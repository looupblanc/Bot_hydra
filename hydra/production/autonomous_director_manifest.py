"""Fail-closed manifest contract for the autonomous economic director.

The director is a campaign mode of the existing production kernel.  It owns no
service, controller, database, or writer; it merely preregisters the two-lane
economic branch allocator that the persistent V17 controller launches through
the normal manifest runner.

This module is intentionally side-effect free.  In particular, validating a
manifest never reserves multiplicity, buys data, or mutates mission state.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping

from hydra.economic_evolution.schema import stable_hash


MANIFEST_SCHEMA = "hydra_economic_production_manifest_v1"
CAMPAIGN_MODE = "AUTONOMOUS_ECONOMIC_DISCOVERY_DIRECTOR"
CAMPAIGN_ID = "hydra_autonomous_economic_discovery_director_0035"
CLASS_ID = "AUTONOMOUS_ECONOMIC_DISCOVERY_DIRECTOR_V1"
RUNTIME_VERSION = "hydra_autonomous_economic_discovery_director_runtime_v1"

LANE_IDS = ("EXPLOITATION", "EXPLORATION")
EXPLOITATION_BRANCH = "0034_NQ_SELECTIVE_EXECUTION_CONFIRMATION"
EXPLORATION_BRANCH = "DIRECT_LEGAL_ACCOUNT_FEASIBILITY"
ACCOUNT_SIZES_USD = (50_000, 100_000, 150_000)
EVIDENCE_TIERS = ("H", "E", "Q", "G", "C", "F")
PARSED_RULE_FIELDS = ("combine", "combine_common", "xfa", "product_restrictions")
THREAD_ENVIRONMENT = (
    "OMP_NUM_THREADS",
    "MKL_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "NUMEXPR_NUM_THREADS",
)

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_GIT_SHA = re.compile(r"^[0-9a-f]{40}$")
_REQUIRED_IMPLEMENTATION_FILES = frozenset(
    {
        "hydra/production/autonomous_director_manifest.py",
        "hydra/production/autonomous_director_runtime.py",
        "hydra/production/autonomous_exact_replay.py",
        "hydra/production/manifest.py",
        "hydra/production/runtime.py",
        "scripts/run_economic_production_manifest.py",
    }
)
_MISSION_PATHS = {
    "objective": "MISSION_OBJECTIVE.md",
    "current_state": "mission/state/CURRENT_STATE.json",
    "decision_ledger": "mission/state/decision_ledger.jsonl",
    "economic_scorecard": "mission/state/ECONOMIC_SCORECARD.json",
    "branch_state": "mission/state/AUTONOMOUS_BRANCH_STATE.json",
}
_FORBIDDEN_GOVERNANCE_TRUE = (
    "live_trading_allowed",
    "broker_connection_allowed",
    "orders_allowed",
    "q4_access_allowed",
    "protected_holdout_access_allowed",
    "new_mission_allowed",
    "new_service_allowed",
    "new_database_allowed",
    "new_registry_writer_allowed",
    "controller_version_change_required",
    "status_inheritance_allowed",
    "falsified_result_resurrection_allowed",
    "causality_weakening_allowed",
    "accounting_weakening_allowed",
    "silent_purchase_above_authority_allowed",
)
_DECISION_CARD_FIELDS = frozenset(
    {
        "hypothesis",
        "strongest_argument_against",
        "smallest_decisive_falsification_experiment",
        "expected_runtime_minutes",
        "expected_data_cost_usd",
        "expected_information_gain",
        "expected_economic_upside",
        "next_materially_distinct_alternative",
    }
)
_PRIMARY_METRICS = frozenset(
    {
        "P_PASS_5D_NORMAL",
        "P_PASS_5D_STRESSED",
        "P_PASS_10D_NORMAL",
        "P_PASS_10D_STRESSED",
        "P_PASS_20D_NORMAL",
        "P_PASS_20D_STRESSED",
        "P_PASS_BEFORE_BREACH",
        "EXPECTED_TRADING_DAYS_TO_PASS",
        "EXPECTED_COMBINE_COST_TO_XFA",
        "MLL_BREACH_RATE",
        "MINIMUM_MLL_BUFFER",
        "CONSISTENCY_COMPLIANCE",
        "STRESSED_NET",
        "LOWER_QUARTILE_TARGET_PROGRESS",
        "MEDIAN_TARGET_PROGRESS",
        "OPPORTUNITY_DENSITY",
        "DEPLOYABILITY",
    }
)


class AutonomousDirectorManifestError(RuntimeError):
    """The autonomous director manifest is incomplete, unsafe, or mutable."""


def validate_autonomous_director_manifest(
    manifest: Mapping[str, Any], *, manifest_path: str | Path
) -> None:
    """Validate the frozen master campaign contract without side effects."""

    path = Path(manifest_path).resolve()
    root = _project_root(path)
    _validate_identity(manifest)
    _validate_implementation(manifest, root)
    _validate_runtime(manifest)
    _validate_evidence_bundle(manifest)
    _validate_artifact_compatibility(manifest)
    _validate_compute(manifest)
    _validate_governance(manifest)
    _validate_rule_snapshot(manifest, root)
    _validate_mission_state_contract(manifest, root)
    _validate_branch_portfolio(manifest)
    _validate_epoch_policy(manifest)
    _validate_evidence_tiers(manifest)
    _validate_objective(manifest)
    _validate_data_and_multiplicity(manifest, root)


def _validate_identity(manifest: Mapping[str, Any]) -> None:
    try:
        created = datetime.fromisoformat(
            str(manifest.get("created_at_utc") or "").replace("Z", "+00:00")
        )
    except ValueError as exc:
        raise AutonomousDirectorManifestError(
            "autonomous-director freeze timestamp is invalid"
        ) from exc
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
        raise AutonomousDirectorManifestError(
            "autonomous-director identity or semantic hash drift"
        )


def _validate_implementation(manifest: Mapping[str, Any], root: Path) -> None:
    files = _mapping(manifest, "implementation_files")
    if not _REQUIRED_IMPLEMENTATION_FILES <= {str(key) for key in files}:
        raise AutonomousDirectorManifestError(
            "autonomous-director implementation checksum closure is incomplete"
        )
    for raw_relative, raw_claimed in files.items():
        relative = str(raw_relative)
        target = _project_file(root, relative, "implementation")
        claimed = str(raw_claimed or "")
        if not _SHA256.fullmatch(claimed) or _sha256(target) != claimed:
            raise AutonomousDirectorManifestError(
                f"autonomous-director implementation checksum drift: {relative}"
            )


def _validate_runtime(manifest: Mapping[str, Any]) -> None:
    runtime = _mapping(manifest, "runtime")
    if (
        runtime.get("engine") != "production_kernel_v1"
        or runtime.get("runner") != "scripts/run_economic_production_manifest.py"
        or runtime.get("result_schema") != "hydra_economic_production_result_v1"
        or runtime.get("autonomous_director_runtime_version") != RUNTIME_VERSION
        or runtime.get("controller_source_change_required") is not False
        or runtime.get("resume_from_checkpoint") is not True
        or int(runtime.get("orchestrator_count", 0)) != 1
        or int(runtime.get("worker_count", 0)) != 2
        or int(runtime.get("asynchronous_evidence_writer_count", 0)) != 1
    ):
        raise AutonomousDirectorManifestError(
            "invalid stable autonomous-director runtime declaration"
        )


def _validate_evidence_bundle(manifest: Mapping[str, Any]) -> None:
    evidence = _mapping(manifest, "evidence_bundle")
    required = {
        "component_signals",
        "component_entries",
        "component_exits",
        "component_trades",
        "account_policy_membership",
        "account_daily_paths",
        "episodes",
        "provenance",
    }
    datasets = set(_tuple(evidence.get("required_datasets")))
    if (
        evidence.get("required") is not True
        or evidence.get("atomic_single_writer_finalization") is not True
        or evidence.get("exact_account_replay_required") is not True
        or evidence.get("sentinel_economic_records_allowed") is not False
        or evidence.get("destination") != "data/cache/evidence_bundles"
        or not required <= datasets
    ):
        raise AutonomousDirectorManifestError(
            "autonomous-director EvidenceBundle contract drift"
        )


def _validate_artifact_compatibility(manifest: Mapping[str, Any]) -> None:
    raw = manifest.get("compatible_artifact_manifest_hashes") or []
    if not isinstance(raw, (list, tuple)):
        raise AutonomousDirectorManifestError("artifact compatibility drift")
    values = tuple(str(item) for item in raw)
    if (
        len(values) > 8
        or len(values) != len(set(values))
        or str(manifest.get("manifest_hash") or "") in values
        or any(not _SHA256.fullmatch(value) for value in values)
    ):
        raise AutonomousDirectorManifestError("artifact compatibility drift")
    if values:
        repair = _mapping(manifest, "post_launch_pre_economic_counter_repair")
        if (
            repair.get("economic_outcomes_changed") is not False
            or repair.get("scientific_policy_changed") is not False
        ):
            raise AutonomousDirectorManifestError("artifact compatibility drift")


def _validate_compute(manifest: Mapping[str, Any]) -> None:
    compute = _mapping(manifest, "compute_contract")
    if (
        int(compute.get("host_logical_cpu_count", 0)) < 3
        or int(compute.get("economic_process_slot_count", 0)) != 3
        or int(compute.get("reserved_logical_cpu_count", -1))
        != int(compute.get("host_logical_cpu_count", 0)) - 3
        or int(compute.get("orchestrator_count", 0)) != 1
        or int(compute.get("cpu_worker_count", 0)) != 2
        or int(compute.get("cpu_worker_maximum", 0)) != 2
        or int(compute.get("authoritative_writer_count", 0)) != 1
        or compute.get("cpu_workers_read_only") is not True
        or compute.get("single_writer_atomic_commits") is not True
        or compute.get("oversubscription_allowed") is not False
        or float(compute.get("economic_wall_clock_minimum", 0.0)) < 0.85
        or float(compute.get("target_cpu_utilization_min", 0.0)) < 0.80
        or float(compute.get("target_cpu_utilization_max", 2.0)) > 0.95
    ):
        raise AutonomousDirectorManifestError(
            "three-process-slot autonomous compute contract drift"
        )
    limits = _mapping(compute, "thread_limits")
    if any(str(limits.get(name)) != "1" for name in THREAD_ENVIRONMENT):
        raise AutonomousDirectorManifestError("numeric thread limit drift")
    worker_roles = _mapping(compute, "worker_roles")
    if (
        worker_roles.get("worker_a") != "EXPLOITATION"
        or worker_roles.get("worker_b") != "EXPLORATION"
    ):
        raise AutonomousDirectorManifestError("economic worker-role drift")


def _validate_governance(manifest: Mapping[str, Any]) -> None:
    governance = _mapping(manifest, "governance")
    if any(governance.get(key) is not False for key in _FORBIDDEN_GOVERNANCE_TRUE):
        raise AutonomousDirectorManifestError("unsafe autonomous governance declaration")
    if governance.get("data_purchase_policy") != "AUTHORITATIVE_LEDGER_BOUNDED_ONLY":
        raise AutonomousDirectorManifestError("data-purchase authority drift")
    if (
        int(governance.get("q4_access_count_delta", -1)) != 0
        or int(governance.get("broker_connection_count", -1)) != 0
        or int(governance.get("order_count", -1)) != 0
    ):
        raise AutonomousDirectorManifestError("unsafe initial governance counters")


def _validate_rule_snapshot(manifest: Mapping[str, Any], root: Path) -> None:
    declared = _mapping(manifest, "official_rule_snapshot")
    snapshot_path = _project_file(root, declared.get("path"), "rule snapshot")
    claimed_file_hash = str(declared.get("file_sha256") or "")
    parsed_hash = str(declared.get("parsed_rule_hash") or "")
    if (
        not _SHA256.fullmatch(claimed_file_hash)
        or _sha256(snapshot_path) != claimed_file_hash
        or not _SHA256.fullmatch(parsed_hash)
        or declared.get("provenance") != "OFFICIAL_TOPSTEP_SOURCES"
        or declared.get("stale") is not False
        or _int_tuple(declared.get("account_sizes_usd")) != ACCOUNT_SIZES_USD
    ):
        raise AutonomousDirectorManifestError("official rule-snapshot binding drift")
    try:
        snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise AutonomousDirectorManifestError("official rule snapshot is unreadable") from exc
    if str(snapshot.get("parsed_rule_hash") or "") != parsed_hash:
        raise AutonomousDirectorManifestError("parsed rule hash does not match snapshot")
    if any(snapshot.get(field) is None for field in PARSED_RULE_FIELDS):
        raise AutonomousDirectorManifestError(
            "official rule snapshot lacks canonical parsed sections"
        )
    recomputed_parsed_hash = stable_hash(
        {field: snapshot.get(field) for field in PARSED_RULE_FIELDS}
    )
    if recomputed_parsed_hash != parsed_hash:
        raise AutonomousDirectorManifestError(
            "parsed rule hash does not match canonical rule fields"
        )
    if _int_tuple(snapshot.get("account_sizes_usd")) != ACCOUNT_SIZES_USD:
        raise AutonomousDirectorManifestError("official account-size coverage drift")
    sources = tuple(str(value) for value in declared.get("official_source_urls") or ())
    if not sources or any(
        not value.startswith("https://help.topstep.com/") for value in sources
    ):
        raise AutonomousDirectorManifestError("official rule sources are incomplete")
    try:
        retrieved = datetime.fromisoformat(
            str(declared.get("retrieved_at_utc") or "").replace("Z", "+00:00")
        )
    except ValueError as exc:
        raise AutonomousDirectorManifestError("rule retrieval timestamp is invalid") from exc
    if retrieved.tzinfo is None:
        raise AutonomousDirectorManifestError("rule retrieval timestamp is not aware")


def _validate_mission_state_contract(manifest: Mapping[str, Any], root: Path) -> None:
    contract = _mapping(manifest, "mission_state_contract")
    files = _mapping(contract, "files")
    if set(files) != set(_MISSION_PATHS):
        raise AutonomousDirectorManifestError("compact source-of-truth file set drift")
    for label, expected in _MISSION_PATHS.items():
        if str(files.get(label)) != expected:
            raise AutonomousDirectorManifestError(
                f"compact source-of-truth path drift: {label}"
            )
        target = _project_file(root, expected, f"mission state {label}")
        if not target.is_file():
            raise AutonomousDirectorManifestError(
                f"compact source-of-truth file missing: {label}"
            )
    objective_hash = str(contract.get("objective_sha256") or "")
    if (
        not _SHA256.fullmatch(objective_hash)
        or _sha256(root / _MISSION_PATHS["objective"]) != objective_hash
        or contract.get("decision_ledger_append_only") is not True
        or contract.get("create_new_reporting_framework") is not False
    ):
        raise AutonomousDirectorManifestError("mission-state contract drift")
    for label in ("current_state", "economic_scorecard", "branch_state"):
        target = root / _MISSION_PATHS[label]
        try:
            value = json.loads(target.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise AutonomousDirectorManifestError(
                f"mission state JSON unreadable: {label}"
            ) from exc
        if not isinstance(value, dict):
            raise AutonomousDirectorManifestError(
                f"mission state JSON must be an object: {label}"
            )


def _validate_branch_portfolio(manifest: Mapping[str, Any]) -> None:
    portfolio = _mapping(manifest, "branch_portfolio")
    lanes = list(portfolio.get("lanes") or ())
    if (
        len(lanes) != 2
        or tuple(str(row.get("lane_id")) for row in lanes) != LANE_IDS
        or portfolio.get("immutable_initial_allocation") is not True
        or portfolio.get("one_lane_block_must_not_idle_other") is not True
    ):
        raise AutonomousDirectorManifestError("two-lane branch portfolio drift")

    exploitation = lanes[0]
    if (
        exploitation.get("initial_branch_id") != EXPLOITATION_BRANCH
        or exploitation.get("source_campaign") != "0034"
        or int(exploitation.get("internal_robustness_decision_maximum", 0)) != 1
        or int(exploitation.get("fresh_confirmation_attempt_maximum", 0)) != 1
        or exploitation.get("threshold_refinement_allowed") is not False
        or exploitation.get("permanent_loyalty") is not False
    ):
        raise AutonomousDirectorManifestError("bounded 0034 exploitation contract drift")

    exploration = lanes[1]
    diagnostics = _tuple(exploration.get("diagnostics"))
    if (
        exploration.get("initial_branch_id") != EXPLORATION_BRANCH
        or exploration.get("source_population") != "CLEAN_CAUSAL_0029_LEDGER_BANK"
        or exploration.get("materially_distinct_from_exploitation") is not True
        or _int_tuple(exploration.get("account_sizes_usd")) != ACCOUNT_SIZES_USD
        or diagnostics
        != (
            "UNIFORM_LEGAL_SCALE_FRONTIER",
            "CAUSAL_QUALITY_TIER_FRONTIER",
            "NON_DEPLOYABLE_LEGAL_UPPER_BOUND",
        )
        or exploration.get("non_deployable_upper_bound_promotable") is not False
    ):
        raise AutonomousDirectorManifestError(
            "direct legal-feasibility exploration contract drift"
        )


def _validate_epoch_policy(manifest: Mapping[str, Any]) -> None:
    epoch = _mapping(manifest, "economic_epoch_policy")
    if (
        int(epoch.get("minimum_minutes", 0)) != 45
        or int(epoch.get("maximum_minutes", 0)) != 120
        or int(epoch.get("branch_materially_distinct_attempt_maximum", 0)) != 2
        or int(epoch.get("economic_worker_idle_timeout_minutes", 0)) != 10
        or epoch.get("unchanged_epoch_repeat_allowed") is not False
        or epoch.get("gate_is_user_handoff") is not False
        or epoch.get("continue_after_gate") is not True
    ):
        raise AutonomousDirectorManifestError("economic epoch policy drift")
    required = _tuple(epoch.get("required_frozen_fields"))
    if required != (
        "hypothesis",
        "compute_budget",
        "data_budget",
        "promotion_gate",
        "falsification_gate",
        "next_branch_rule",
    ):
        raise AutonomousDirectorManifestError("economic epoch preregistration drift")
    card = _mapping(manifest, "research_board")
    if (
        set(card.get("decision_card_fields") or ()) != _DECISION_CARD_FIELDS
        or card.get("persist_private_reasoning") is not False
        or card.get("materially_distinct_alternative_required") is not True
    ):
        raise AutonomousDirectorManifestError("research-board decision contract drift")


def _validate_evidence_tiers(manifest: Mapping[str, Any]) -> None:
    tiers = _mapping(manifest, "evidence_tiers")
    if (
        _tuple(tiers.get("ordered_tiers")) != EVIDENCE_TIERS
        or tiers.get("status_inheritance_allowed") is not False
        or tiers.get("collapse_to_validated_allowed") is not False
        or tiers.get("independent_confirmation_required_for_tier_c") is not True
        or tiers.get("f0_required_for_tier_f") is not True
    ):
        raise AutonomousDirectorManifestError("evidence-tier contract drift")


def _validate_objective(manifest: Mapping[str, Any]) -> None:
    objective = _mapping(manifest, "economic_objective")
    if (
        _int_tuple(objective.get("headline_horizons_trading_days")) != (5, 10, 20)
        or _int_tuple(objective.get("account_sizes_usd")) != ACCOUNT_SIZES_USD
        or not _PRIMARY_METRICS <= set(objective.get("required_metrics") or ())
        or objective.get("pareto_frontier") is not True
        or objective.get("largest_account_default") is not False
        or objective.get("exact_mll_required") is not True
        or objective.get("exact_consistency_required") is not True
        or objective.get("causal_executable_fills_required") is not True
        or objective.get("combine_and_funded_products_separate") is not True
        or objective.get("xfa_before_credible_combine_allowed") is not False
    ):
        raise AutonomousDirectorManifestError("direct economic objective drift")


def _validate_data_and_multiplicity(
    manifest: Mapping[str, Any], root: Path
) -> None:
    data = _mapping(manifest, "data_policy")
    ledger = _project_file(root, data.get("budget_ledger_path"), "budget ledger")
    if (
        data.get("existing_data_first") is not True
        or data.get("q4_access_allowed") is not False
        or data.get("protected_holdout_access_allowed") is not False
        or data.get("official_cost_estimate_before_purchase") is not True
        or data.get("freeze_roles_before_purchase") is not True
        or data.get("silent_purchase_allowed") is not False
        or not ledger.is_file()
    ):
        raise AutonomousDirectorManifestError("autonomous data policy drift")
    multiplicity = _mapping(manifest, "multiplicity")
    if (
        int(multiplicity.get("campaign_run_limit", 0)) != 1
        or multiplicity.get("controller_reservation_required") is not True
        or multiplicity.get("single_existing_controller") is not True
        or multiplicity.get("single_authoritative_writer") is not True
    ):
        raise AutonomousDirectorManifestError("autonomous multiplicity drift")


def _project_root(path: Path) -> Path:
    for candidate in (path.parent, *path.parents):
        if (candidate / "hydra").is_dir() and (candidate / "scripts").is_dir():
            return candidate
    # Unit fixtures deliberately reproduce the repository shape under tmp_path.
    if len(path.parents) >= 3:
        return path.parents[2]
    raise AutonomousDirectorManifestError("cannot resolve project root")


def _project_file(root: Path, value: Any, label: str) -> Path:
    relative = Path(str(value or ""))
    if not str(value or "") or relative.is_absolute():
        raise AutonomousDirectorManifestError(f"unsafe {label} path")
    target = (root / relative).resolve()
    if target == root or root not in target.parents:
        raise AutonomousDirectorManifestError(f"{label} path escapes project root")
    return target


def _mapping(value: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    result = value.get(key)
    if not isinstance(result, Mapping):
        raise AutonomousDirectorManifestError(f"missing manifest mapping: {key}")
    return result


def _tuple(value: Any) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)):
        return ()
    return tuple(str(item) for item in value)


def _int_tuple(value: Any) -> tuple[int, ...]:
    if not isinstance(value, (list, tuple)):
        return ()
    try:
        return tuple(int(item) for item in value)
    except (TypeError, ValueError):
        return ()


def _sha256(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError as exc:
        raise AutonomousDirectorManifestError(f"required file unreadable: {path}") from exc
