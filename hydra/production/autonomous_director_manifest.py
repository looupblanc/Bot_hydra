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
ARTIFACT_COMPATIBILITY_LIMIT = 13

_POST_MACRO_WORKER_IMPLEMENTATIONS = {
    "TIER_Q_2026_FINAL_DEVELOPMENT_READ_ONLY": (
        "hydra/production/tier_q_2026_two_stage_runner.py"
    ),
    "CL_FRONT_SECOND_TERM_STRUCTURE_READ_ONLY": (
        "hydra/research/cl_front_second_term_structure_economic_runner.py"
    ),
    "TREASURY_THREE_TENOR_CURVATURE_READ_ONLY": (
        "hydra/research/treasury_three_tenor_curvature_tripwire.py"
    ),
}
_POST_MACRO_WORKER_CONTRACTS = {
    "TIER_Q_2026_FINAL_DEVELOPMENT_READ_ONLY": {
        "relay_key": "POST_MACRO_TIER_Q_2026_FINAL_DEVELOPMENT",
        "schema": "hydra_tier_q_2026_two_stage_economic_result_v2",
        "statuses": ("FINAL_DEVELOPMENT_CONSUMED",),
        "evidence_role_field": "role",
        "evidence_role": "FINAL_DEVELOPMENT",
        "source_modes": (
            "PREEXISTING_HASH_BOUND",
            "GENERATE_READ_ONLY_ONCE",
        ),
    },
    "CL_FRONT_SECOND_TERM_STRUCTURE_READ_ONLY": {
        "relay_key": "POST_MACRO_CL_FRONT_SECOND_TERM_STRUCTURE",
        "schema": "hydra_cl_front_second_term_structure_economic_tripwire_v1",
        "statuses": (
            "TERM_STRUCTURE_TRIPWIRE_GREEN_TIER_E",
            "TERM_STRUCTURE_TRIPWIRE_WEAK",
            "TERM_STRUCTURE_TRIPWIRE_FALSIFIED",
            "TERM_STRUCTURE_TRIPWIRE_UNDERPOWERED_NO_THRESHOLD_RELAXATION",
        ),
        "evidence_role_field": "evidence_role",
        "evidence_role": "VIEWED_PRE_Q4_DEVELOPMENT_TRIPWIRE_ONLY",
        "source_modes": (
            "PREEXISTING_HASH_BOUND",
            "GENERATE_READ_ONLY_ONCE",
        ),
    },
    "TREASURY_THREE_TENOR_CURVATURE_READ_ONLY": {
        "relay_key": "POST_MACRO_TREASURY_THREE_TENOR_CURVATURE",
        "schema": "hydra_treasury_three_tenor_curvature_tripwire_v1",
        "statuses": (
            "TREASURY_CURVATURE_TO_BELLY_GREEN_TIER_E",
            "TREASURY_CURVATURE_TO_BELLY_WEAK",
            "TREASURY_CURVATURE_TO_BELLY_FALSIFIED",
            "TREASURY_CURVATURE_UNDERPOWERED_NO_THRESHOLD_RELAXATION",
            "TREASURY_CURVATURE_RISK_GRANULARITY_BLOCKED_AND_COVERAGE_UNDERPOWERED",
        ),
        "evidence_role_field": "evidence_role",
        "evidence_role": "VIEWED_PRE_Q4_DEVELOPMENT_TRIPWIRE_ONLY",
        "source_modes": (
            "PREEXISTING_HASH_BOUND",
            "GENERATE_READ_ONLY_ONCE",
        ),
    },
}
_POST_MACRO_REQUIRED_SAFETY_FIELDS = {
    "TIER_Q_2026_FINAL_DEVELOPMENT_READ_ONLY": {
        "q4_access_count_delta": 0,
        "broker_connections": 0,
        "orders": 0,
    },
    "CL_FRONT_SECOND_TERM_STRUCTURE_READ_ONLY": {
        "governance.q4_rows": 0,
        "governance.protected_data_access_count_delta": 0,
        "governance.broker_connections": 0,
        "governance.orders": 0,
    },
    "TREASURY_THREE_TENOR_CURVATURE_READ_ONLY": {
        "governance.q4_access_count_delta": 0,
        "governance.broker_connections": 0,
        "governance.orders": 0,
    },
}

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
    if manifest.get("post_macro_branch_portfolio") is not None:
        _validate_post_macro_branch_portfolio(manifest, root)
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
        len(values) > ARTIFACT_COMPATIBILITY_LIMIT
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


def _validate_post_macro_branch_portfolio(
    manifest: Mapping[str, Any], root: Path
) -> None:
    """Validate the bounded, parent-published multi-card continuation queue."""

    section = _mapping(manifest, "post_macro_branch_portfolio")
    cards = list(section.get("cards") or ())
    if (
        section.get("schema") != "hydra_post_macro_branch_portfolio_v1"
        or len(cards) != 3
        or int(section.get("card_capacity", 0)) != len(cards)
        or int(section.get("worker_maximum", 0)) != 2
        or int(section.get("launch_count_maximum_per_card", 0)) != 1
        or int(section.get("lease_resume_maximum", -1)) != 1
        or int(section.get("lease_seconds", 0)) < 60
        or section.get("expired_lease_resume_same_attempt") is not True
        or section.get("read_only_workers") is not True
        or section.get("parent_only_authoritative_writer") is not True
        or section.get("empty_inventory_launch_allowed") is not False
        or section.get("missing_input_spin_allowed") is not False
        or section.get("result_recompute_when_valid_exists") is not False
        or section.get("q4_access_allowed") is not False
        or section.get("broker_connection_allowed") is not False
        or section.get("orders_allowed") is not False
        or section.get("new_service_allowed") is not False
        or section.get("new_controller_version_allowed") is not False
        or section.get("new_database_allowed") is not False
        or section.get("new_registry_writer_allowed") is not False
    ):
        raise AutonomousDirectorManifestError(
            "post-macro bounded continuation contract drift"
        )
    next_branch = section.get("next_branch")
    if not isinstance(next_branch, Mapping):
        raise AutonomousDirectorManifestError(
            "post-macro next-branch waiting card is absent"
        )
    row = dict(next_branch)
    if (
        row.get("action")
        != "ADVANCE_TO_PREAPPENDED_RESEARCH_BOARD_SUCCESSOR"
        or not str(row.get("lane_id") or "")
        or not str(row.get("branch_id") or "")
        or row.get("append_required") is not True
        or row.get("automatic_parameter_neighbor") is not False
    ):
        raise AutonomousDirectorManifestError(
            "post-macro next-branch waiting card drift"
        )
    evidence_contract = _mapping(manifest, "evidence_bundle")
    if (
        evidence_contract.get("evidence_status")
        != "AUTHORITATIVE_DEVELOPMENT_RECONSTRUCTION"
        or evidence_contract.get("reconstruction_flag") is not True
    ):
        raise AutonomousDirectorManifestError(
            "post-macro deterministic materialization must be declared as "
            "authoritative development reconstruction"
        )

    output = _project_file(
        root, _mapping(manifest, "runtime").get("output_dir"), "runtime output"
    )
    branch_root = (output / "branch_results").resolve()
    implementations = _mapping(manifest, "implementation_files")
    terminal_adapter = "hydra/evidence/causal_target_velocity_adapter.py"
    terminal_adapter_sha = str(implementations.get(terminal_adapter) or "")
    if (
        not _SHA256.fullmatch(terminal_adapter_sha)
        or terminal_adapter_sha
        != _sha256(
            _project_file(
                root, terminal_adapter, "post-macro terminal evidence adapter"
            )
        )
    ):
        raise AutonomousDirectorManifestError(
            "post-macro terminal EvidenceBundle adapter is outside checksum closure"
        )
    seen_keys: set[str] = set()
    seen_branches: set[str] = set()
    seen_paths: set[Path] = set()
    allowed_workers = set(_POST_MACRO_WORKER_IMPLEMENTATIONS)
    for raw in cards:
        if not isinstance(raw, Mapping):
            raise AutonomousDirectorManifestError(
                "post-macro card must be a mapping"
            )
        card = dict(raw)
        relay_key = str(card.get("relay_key") or "")
        branch_id = str(card.get("branch_id") or "")
        allowed_statuses = _tuple(card.get("allowed_statuses"))
        expected_fields = card.get("expected_fields")
        worker_kind = str(card.get("worker_kind") or "")
        worker_contract = _POST_MACRO_WORKER_CONTRACTS.get(worker_kind, {})
        evidence_role_field = str(
            worker_contract.get("evidence_role_field") or ""
        )
        source_mode = str(card.get("source_mode") or "")
        preexisting_result_hash = card.get("preexisting_result_hash")
        if (
            str(card.get("lane_id") or "") not in (*LANE_IDS, "DIRECTOR")
            or not relay_key
            or relay_key in seen_keys
            or not branch_id
            or branch_id in seen_branches
            or worker_kind not in allowed_workers
            or relay_key != worker_contract.get("relay_key")
            or card.get("expected_schema") != worker_contract.get("schema")
            or allowed_statuses != tuple(worker_contract.get("statuses") or ())
            or card.get("hash_field") != "result_hash"
            or not isinstance(expected_fields, Mapping)
            or dict(expected_fields).get(evidence_role_field)
            != worker_contract.get("evidence_role")
            or source_mode not in tuple(worker_contract.get("source_modes") or ())
            or (
                source_mode == "PREEXISTING_HASH_BOUND"
                and not _SHA256.fullmatch(str(preexisting_result_hash or ""))
            )
            or (
                source_mode == "GENERATE_READ_ONLY_ONCE"
                and preexisting_result_hash is not None
            )
            or card.get("source_result_absent_launch_once") is not True
            or card.get("valid_existing_result_relay_only") is not True
            or card.get("worker_receives_output_path") is not False
            or card.get("parent_publishes_source_and_relay") is not True
            or not str(card.get("relay_evidence_tier") or "")
            or not _POST_MACRO_REQUIRED_SAFETY_FIELDS[worker_kind].items()
            <= dict(expected_fields).items()
        ):
            raise AutonomousDirectorManifestError(
                "post-macro frozen card identity or execution drift"
            )
        seen_keys.add(relay_key)
        seen_branches.add(branch_id)
        source_target: Path | None = None
        for key in (
            "source_result_path",
            "relay_result_path",
            "launch_receipt_path",
            "resume_receipt_path",
        ):
            target = _project_file(root, card.get(key), f"post-macro {key}")
            try:
                target.relative_to(branch_root)
            except ValueError as exc:
                raise AutonomousDirectorManifestError(
                    f"post-macro {key} leaves authoritative branch root"
                ) from exc
            if target in seen_paths:
                raise AutonomousDirectorManifestError(
                    "post-macro source/relay/receipt path collision"
                )
            seen_paths.add(target)
            if key == "source_result_path":
                source_target = target
        if source_mode == "PREEXISTING_HASH_BOUND":
            if source_target is None or not source_target.is_file():
                raise AutonomousDirectorManifestError(
                    "post-macro preexisting source result is absent"
                )
            try:
                source_payload = json.loads(source_target.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise AutonomousDirectorManifestError(
                    "post-macro preexisting source result is invalid"
                ) from exc
            if (
                not isinstance(source_payload, Mapping)
                or source_payload.get("result_hash") != preexisting_result_hash
            ):
                raise AutonomousDirectorManifestError(
                    "post-macro preexisting source result hash drift"
                )
        worker_inputs = card.get("worker_inputs")
        if not isinstance(worker_inputs, Mapping) or not worker_inputs:
            raise AutonomousDirectorManifestError(
                "post-macro worker input inventory is empty"
            )
        for name, raw_binding in worker_inputs.items():
            if not str(name) or not isinstance(raw_binding, Mapping):
                raise AutonomousDirectorManifestError(
                    "post-macro worker input binding drift"
                )
            binding = dict(raw_binding)
            target = _project_file(
                root, binding.get("path"), f"post-macro worker input {name}"
            )
            claimed = str(binding.get("sha256") or "")
            if (
                not target.is_file()
                or not _SHA256.fullmatch(claimed)
                or _sha256(target) != claimed
            ):
                raise AutonomousDirectorManifestError(
                    f"post-macro frozen worker input drift: {name}"
                )
        worker_file = str(card.get("worker_implementation_file") or "")
        implementation_sha256 = str(
            card.get("worker_implementation_sha256") or ""
        )
        if (
            worker_file != _POST_MACRO_WORKER_IMPLEMENTATIONS[worker_kind]
            or worker_file not in implementations
            or not _SHA256.fullmatch(implementation_sha256)
            or str(implementations.get(worker_file) or "")
            != implementation_sha256
            or implementation_sha256
            != _sha256(
                _project_file(
                    root, worker_file, "post-macro worker implementation"
                )
            )
        ):
            raise AutonomousDirectorManifestError(
                "post-macro worker implementation is outside checksum closure"
            )

    if seen_keys != {
        str(value["relay_key"])
        for value in _POST_MACRO_WORKER_CONTRACTS.values()
    }:
        raise AutonomousDirectorManifestError(
            "post-macro frozen worker portfolio is incomplete"
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
