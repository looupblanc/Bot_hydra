"""Fail-closed production manifest for the bounded HYDRA 0036 router.

The scientific implementation lives in
``hydra.research.cross_ecology_session_path_analog_router``.  This module only
binds that read-only economic result to the existing production kernel.  It
does not create a controller, service, database, writer, or research grammar.
"""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping

from hydra.economic_evolution.schema import stable_hash
from hydra.evidence import REQUIRED_DATASETS


MANIFEST_SCHEMA = "hydra_economic_production_manifest_v1"
CAMPAIGN_MODE = "CROSS_ECOLOGY_SESSION_PATH_ANALOG_ROUTER"
CAMPAIGN_ID = "hydra_cross_ecology_session_path_analog_router_0036"
CAMPAIGN_ORDINAL = 36
CLASS_ID = "CAUSAL_CROSS_ECOLOGY_SESSION_PATH_ANALOG_ROUTER_V1"
RUNTIME_VERSION = "hydra_cross_ecology_session_path_analog_runtime_v1"
COMPLETED_RESULT_ADOPTION_SCHEMA = (
    "hydra_cross_ecology_0036_completed_scientific_result_adoption_v1"
)
COMPLETED_RESULT_ADOPTION_CLASSIFICATION = (
    "COMPLETED_SCIENTIFIC_RESULT_ADOPTION_WITHOUT_ECONOMIC_REPLAY"
)
NON_ECONOMIC_TERMINAL_MODE = "NON_ECONOMIC_AUDIT_ONLY"
NON_ECONOMIC_CANONICAL_STATUS = "NO_GENUINELY_MATERIALIZED_POLICY_FRAGMENT"
NON_ECONOMIC_AUDIT_RECEIPT_SCHEMA = "hydra_non_economic_audit_receipt_v1"
SCIENTIFIC_RESULT_SCHEMA = "hydra_cross_ecology_session_path_analog_router_v1"
DECISION_CARD_SCHEMA = "hydra_autonomous_branch_decision_card_v1"
ROOT_AUTHORIZATION = "ROOT_AUTHORIZED_CROSS_ECOLOGY_SESSION_ANALOG_REPLAY_V1"
DEFAULT_MANIFEST_PATH = "config/v7/cross_ecology_session_path_analog_router_0036.json"
SOURCE_MODES = ("PREEXISTING_HASH_BOUND", "GENERATE_READ_ONLY_ONCE")
SCIENTIFIC_STATUSES = (
    "SESSION_PATH_ANALOG_TIER_E_DIAGNOSTIC_GREEN",
    "SESSION_PATH_ANALOG_FALSIFIED",
    "SESSION_PATH_ANALOG_UNDERPOWERED_NO_THRESHOLD_RELAXATION",
)
EVIDENCE_ROLE = "VIEWED_PRE_Q4_DEVELOPMENT_TRIPWIRE_ONLY"
TIER_CEILING = "E"

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_GIT_SHA = re.compile(r"^[0-9a-f]{40}$")
_REQUIRED_IMPLEMENTATION_FILES = frozenset(
    {
        "config/research/cross_ecology_session_path_analog_router_v1.json",
        "hydra/research/cross_ecology_session_path_analog_router.py",
        "scripts/run_cross_ecology_session_path_analog_router.py",
        "tests/test_cross_ecology_session_path_analog_router.py",
        "hydra/production/cross_ecology_analog_manifest.py",
        "hydra/production/cross_ecology_analog_runtime.py",
        "hydra/production/autonomous_director_manifest.py",
        "hydra/production/manifest.py",
        "hydra/production/runtime.py",
        "hydra/production/__init__.py",
        "hydra/mission/economic_evolution_manifest_runtime.py",
        "scripts/run_economic_production_manifest.py",
    }
)
_FORBIDDEN_GOVERNANCE_TRUE = (
    "q4_access_allowed",
    "protected_holdout_access_allowed",
    "new_data_purchase_allowed",
    "network_access_allowed",
    "broker_connection_allowed",
    "orders_allowed",
    "mission_database_write_allowed",
    "registry_write_allowed",
    "cemetery_write_allowed",
    "controller_version_change_required",
    "status_inheritance_allowed",
    "tier_q_allowed",
    "promotion_allowed",
)
_GOVERNANCE_ZERO_COUNTER_FIELDS = (
    "network_requests",
    "data_purchase_count",
    "q4_access_count_delta",
    "broker_connections",
    "orders",
    "mission_database_writes",
    "registry_writes",
    "cemetery_writes",
)


class CrossEcologyAnalogManifestError(RuntimeError):
    """The immutable 0036 production contract is incomplete or has drifted."""


def validate_cross_ecology_analog_manifest(
    manifest: Mapping[str, Any], *, manifest_path: str | Path
) -> None:
    """Validate the frozen 0036 structure without opening economic outcomes.

    In particular, a ``PREEXISTING_HASH_BOUND`` scientific result is *not*
    opened, hashed, or parsed here.  The production runtime may validate that
    source only after the authoritative controller multiplicity reservation
    has been proven.  This separation keeps generic manifest discovery from
    becoming an unregistered economic-outcome access path.
    """

    path = Path(manifest_path).resolve()
    root = _project_root(path)
    _identity(manifest)
    _implementation(manifest, root)
    _committed_implementation(manifest, root)
    _research_source(manifest, root)
    _runtime(manifest)
    _multiplicity(manifest)
    _evidence(manifest)
    _governance(manifest)
    _completed_result_adoption(manifest, root)


def _identity(manifest: Mapping[str, Any]) -> None:
    claimed = str(manifest.get("manifest_hash") or "")
    payload = dict(manifest)
    payload.pop("manifest_hash", None)
    try:
        created = datetime.fromisoformat(
            str(manifest.get("created_at_utc") or "").replace("Z", "+00:00")
        )
    except ValueError as exc:
        raise CrossEcologyAnalogManifestError("0036 freeze timestamp is invalid") from exc
    if (
        manifest.get("schema") != MANIFEST_SCHEMA
        or manifest.get("campaign_mode") != CAMPAIGN_MODE
        or manifest.get("campaign_id") != CAMPAIGN_ID
        or int(manifest.get("campaign_ordinal", -1)) != CAMPAIGN_ORDINAL
        or manifest.get("class_id") != CLASS_ID
        or tuple(manifest.get("policy_classes") or ()) != (CLASS_ID,)
        or manifest.get("development_only") is not True
        or created.tzinfo is None
        or not _GIT_SHA.fullmatch(str(manifest.get("source_commit") or ""))
        or not str(manifest.get("economic_hypothesis") or "").strip()
        or not _SHA256.fullmatch(claimed)
        or stable_hash(payload) != claimed
    ):
        raise CrossEcologyAnalogManifestError("0036 identity or semantic hash drift")


def _implementation(manifest: Mapping[str, Any], root: Path) -> None:
    files = _mapping(manifest, "implementation_files")
    if not _REQUIRED_IMPLEMENTATION_FILES <= {str(value) for value in files}:
        raise CrossEcologyAnalogManifestError("0036 implementation closure is incomplete")
    for relative, claimed_raw in files.items():
        target = _project_file(root, relative, "implementation")
        claimed = str(claimed_raw or "")
        if not _SHA256.fullmatch(claimed) or _sha256(target) != claimed:
            raise CrossEcologyAnalogManifestError(
                f"0036 implementation checksum drift: {relative}"
            )


def _committed_implementation(manifest: Mapping[str, Any], root: Path) -> None:
    """Bind every live implementation artifact to the frozen source commit."""

    source_commit = str(manifest["source_commit"])
    exists = subprocess.run(
        ["git", "cat-file", "-e", f"{source_commit}^{{commit}}"],
        cwd=root,
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    ancestor = subprocess.run(
        ["git", "merge-base", "--is-ancestor", source_commit, "HEAD"],
        cwd=root,
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    if exists.returncode != 0 or ancestor.returncode != 0:
        raise CrossEcologyAnalogManifestError(
            "0036 source commit is not a committed live-HEAD ancestor"
        )
    for relative, expected_raw in sorted(
        _mapping(manifest, "implementation_files").items()
    ):
        expected = str(expected_raw)
        blob = subprocess.run(
            ["git", "show", f"{source_commit}:{relative}"],
            cwd=root,
            check=False,
            capture_output=True,
        )
        if (
            blob.returncode != 0
            or hashlib.sha256(blob.stdout).hexdigest() != expected
        ):
            raise CrossEcologyAnalogManifestError(
                f"0036 implementation is not frozen in source commit: {relative}"
            )


def _research_source(manifest: Mapping[str, Any], root: Path) -> None:
    source = _mapping(manifest, "research_source")
    card_path = _project_file(root, source.get("decision_card_path"), "decision card")
    module_path = _project_file(root, source.get("module_path"), "research module")
    runner_path = _project_file(root, source.get("runner_path"), "research runner")
    if (
        card_path.relative_to(root).as_posix()
        != "config/research/cross_ecology_session_path_analog_router_v1.json"
        or module_path.relative_to(root).as_posix()
        != "hydra/research/cross_ecology_session_path_analog_router.py"
        or runner_path.relative_to(root).as_posix()
        != "scripts/run_cross_ecology_session_path_analog_router.py"
    ):
        raise CrossEcologyAnalogManifestError("0036 scientific source path drift")
    for path, field in (
        (card_path, "decision_card_file_sha256"),
        (module_path, "module_file_sha256"),
        (runner_path, "runner_file_sha256"),
    ):
        claimed = str(source.get(field) or "")
        if not _SHA256.fullmatch(claimed) or _sha256(path) != claimed:
            raise CrossEcologyAnalogManifestError(f"0036 source checksum drift: {field}")

    card = _load_json(card_path)
    card_payload = dict(card)
    card_claimed = str(card_payload.pop("card_hash", ""))
    card_governance = card.get("governance")
    if (
        card.get("schema") != DECISION_CARD_SCHEMA
        or card.get("campaign_id") != CAMPAIGN_ID
        or card.get("selected_branch") != CLASS_ID
        or card_claimed != stable_hash(card_payload)
        or source.get("decision_card_hash") != card_claimed
        or source.get("frozen_input_contract_hash")
        != card.get("frozen_input_contract_hash")
        or source.get("root_authorization") != ROOT_AUTHORIZATION
        or not isinstance(card_governance, Mapping)
    ):
        raise CrossEcologyAnalogManifestError("0036 decision-card binding drift")
    if any(
        type(card_governance.get(field)) is not int
        or card_governance.get(field) != 0
        for field in _GOVERNANCE_ZERO_COUNTER_FIELDS
    ):
        raise CrossEcologyAnalogManifestError(
            "0036 decision-card governance counters drift"
        )

    mode = str(source.get("source_mode") or "")
    if mode not in SOURCE_MODES:
        raise CrossEcologyAnalogManifestError("0036 scientific source mode is invalid")
    result_path = _project_path(root, source.get("result_path"), "scientific result")
    allowed = (root / "reports/economic_evolution").resolve()
    if result_path == allowed or allowed not in result_path.parents:
        raise CrossEcologyAnalogManifestError("0036 scientific result escapes reports")
    if mode == "PREEXISTING_HASH_BOUND":
        # Validate only the frozen declarations here.  File existence and
        # content checks are outcome access and therefore belong exclusively
        # to the post-reservation runtime path.
        expected_file = str(source.get("result_file_sha256") or "")
        expected_result = str(source.get("result_hash") or "")
        if not _SHA256.fullmatch(expected_file) or not _SHA256.fullmatch(
            expected_result
        ):
            raise CrossEcologyAnalogManifestError(
                "0036 preexisting scientific result binding declaration drift"
            )
    elif any(source.get(field) not in (None, "") for field in (
        "result_file_sha256",
        "result_hash",
    )):
        raise CrossEcologyAnalogManifestError(
            "0036 generated mode may not predeclare an unseen result hash"
        )
    if int(source.get("maximum_economic_replays", -1)) != 1:
        raise CrossEcologyAnalogManifestError("0036 must permit exactly one economic replay")


def _runtime(manifest: Mapping[str, Any]) -> None:
    runtime = _mapping(manifest, "runtime")
    if (
        runtime.get("engine") != "production_kernel_v1"
        or runtime.get("runner") != "scripts/run_economic_production_manifest.py"
        or runtime.get("result_schema") != "hydra_economic_production_result_v1"
        or runtime.get("result_name") != "economic_production_result.json"
        or runtime.get("controller_source_change_required") is not False
        or runtime.get("resume_from_checkpoint") is not True
        or int(runtime.get("worker_count", -1)) != 1
        or int(runtime.get("asynchronous_evidence_writer_count", -1)) != 1
        or runtime.get("runtime_version") != RUNTIME_VERSION
    ):
        raise CrossEcologyAnalogManifestError("0036 stable runtime declaration drift")
    output = str(runtime.get("output_dir") or "")
    if output != "reports/economic_evolution/cross_ecology_session_path_analog_router_0036":
        raise CrossEcologyAnalogManifestError("0036 output directory drift")


def _multiplicity(manifest: Mapping[str, Any]) -> None:
    value = _mapping(manifest, "multiplicity")
    prior = _integer(value, "prior_global_N_trials")
    delta = _integer(value, "reserved_delta_trials")
    expected = _integer(value, "expected_global_N_trials_after_reservation")
    comparisons = _integer(value, "prospective_comparisons")
    inflation = value.get("campaign_specific_inflation")
    receipt_path = str(value.get("reservation_receipt_path") or "")
    receipt_sha = str(value.get("reservation_receipt_sha256") or "")
    if (
        prior < 0
        or delta != 6
        or comparisons != 6
        or prior + delta != expected
        or not isinstance(inflation, (int, float))
        or isinstance(inflation, bool)
        or float(inflation) < 1.0
        or receipt_path
        != (
            "reports/economic_evolution/"
            "hydra_cross_ecology_session_path_analog_router_0036_"
            "multiplicity_reservation.json"
        )
        or not _SHA256.fullmatch(receipt_sha)
    ):
        raise CrossEcologyAnalogManifestError("0036 multiplicity reservation drift")


def _evidence(manifest: Mapping[str, Any]) -> None:
    evidence = _mapping(manifest, "evidence_bundle")
    if (
        evidence.get("contract") != "HYDRA_EVIDENCE_BUNDLE_V1"
        or tuple(evidence.get("required_datasets") or ()) != tuple(REQUIRED_DATASETS)
        or evidence.get("destination") != "data/cache/evidence_bundles"
        or evidence.get("evidence_status") != "FRESH_DEVELOPMENT_EVIDENCE"
        or evidence.get("reconstruction_flag") is not False
        or evidence.get("embedded_material_requires_replay") is not False
        or evidence.get("summary_only_completion_allowed") is not False
    ):
        raise CrossEcologyAnalogManifestError("0036 EvidenceBundle contract drift")


def _governance(manifest: Mapping[str, Any]) -> None:
    governance = _mapping(manifest, "governance")
    if any(governance.get(field) is not False for field in _FORBIDDEN_GOVERNANCE_TRUE):
        raise CrossEcologyAnalogManifestError("0036 unsafe governance declaration")
    if governance.get("tier_ceiling") != TIER_CEILING or governance.get(
        "independent_confirmation_claimed"
    ) is not False:
        raise CrossEcologyAnalogManifestError("0036 governance counters drift")
    if any(
        type(governance.get(field)) is not int or governance.get(field) != 0
        for field in _GOVERNANCE_ZERO_COUNTER_FIELDS
    ):
        raise CrossEcologyAnalogManifestError("0036 governance counters drift")


def _completed_result_adoption(manifest: Mapping[str, Any], root: Path) -> None:
    """Validate an exact technical adoption without authorising another replay.

    This optional contract exists only for a scientific result that was already
    completed under the original preregistration but contains no materialized
    economic policy fragment.  It binds every old mutable/immutable artifact
    independently; a broad compatibility hash can never authorize adoption.
    """

    raw = manifest.get("completed_scientific_result_adoption")
    if raw is None:
        if manifest.get("compatible_artifact_manifest_hashes") not in (None, [], ()):
            raise CrossEcologyAnalogManifestError("0036 adoption compatibility drift")
        if manifest.get("resumable_snapshot_compatibility") is not None:
            raise CrossEcologyAnalogManifestError("0036 snapshot compatibility drift")
        return
    if not isinstance(raw, Mapping):
        raise CrossEcologyAnalogManifestError("0036 completed-result adoption drift")
    adoption = dict(raw)
    required = {
        "schema",
        "classification",
        "terminal_mode",
        "economic_outcomes_changed",
        "scientific_policy_changed",
        "economic_replay_allowed",
        "source_manifest_hash",
        "source_manifest_file_sha256",
        "source_manifest_path",
        "source_manifest_worm_commit",
        "source_manifest_worm_tag",
        "source_commit",
        "scientific_status",
        "canonical_evidence_status",
        "scientific_result_path",
        "scientific_result_hash",
        "scientific_result_file_sha256",
        "replay_lease_path",
        "replay_lease_attempt_hash",
        "replay_lease_file_sha256",
        "multiplicity_preregistration_hash",
        "audit_receipt_path",
    }
    source = _mapping(manifest, "research_source")
    runtime = _mapping(manifest, "runtime")
    old_manifest_hash = str(adoption.get("source_manifest_hash") or "")
    old_source_commit = str(adoption.get("source_commit") or "")
    result_path = str(source.get("result_path") or "")
    output = Path(str(runtime.get("output_dir") or ""))
    if (
        set(adoption) != required
        or adoption.get("schema") != COMPLETED_RESULT_ADOPTION_SCHEMA
        or adoption.get("classification")
        != COMPLETED_RESULT_ADOPTION_CLASSIFICATION
        or adoption.get("terminal_mode") != NON_ECONOMIC_TERMINAL_MODE
        or adoption.get("economic_outcomes_changed") is not False
        or adoption.get("scientific_policy_changed") is not False
        or adoption.get("economic_replay_allowed") is not False
        or adoption.get("scientific_status")
        != "SESSION_PATH_ANALOG_UNDERPOWERED_NO_THRESHOLD_RELAXATION"
        or adoption.get("canonical_evidence_status")
        != NON_ECONOMIC_CANONICAL_STATUS
        or not _SHA256.fullmatch(old_manifest_hash)
        or old_manifest_hash == str(manifest.get("manifest_hash") or "")
        or not _SHA256.fullmatch(
            str(adoption.get("source_manifest_file_sha256") or "")
        )
        or adoption.get("source_manifest_path") != DEFAULT_MANIFEST_PATH
        or not _GIT_SHA.fullmatch(
            str(adoption.get("source_manifest_worm_commit") or "")
        )
        or not re.fullmatch(
            r"worm/[A-Za-z0-9][A-Za-z0-9._/-]+",
            str(adoption.get("source_manifest_worm_tag") or ""),
        )
        or not _GIT_SHA.fullmatch(old_source_commit)
        or old_source_commit == str(manifest.get("source_commit") or "")
        or adoption.get("scientific_result_path") != result_path
        or source.get("source_mode") != "PREEXISTING_HASH_BOUND"
        or source.get("result_hash") != adoption.get("scientific_result_hash")
        or source.get("result_file_sha256")
        != adoption.get("scientific_result_file_sha256")
        or not _SHA256.fullmatch(str(adoption.get("scientific_result_hash") or ""))
        or not _SHA256.fullmatch(
            str(adoption.get("scientific_result_file_sha256") or "")
        )
        or adoption.get("replay_lease_path")
        != (output / "scientific_replay_attempt.json").as_posix()
        or not _SHA256.fullmatch(
            str(adoption.get("replay_lease_attempt_hash") or "")
        )
        or not _SHA256.fullmatch(
            str(adoption.get("replay_lease_file_sha256") or "")
        )
        or adoption.get("multiplicity_preregistration_hash") != old_manifest_hash
        or adoption.get("audit_receipt_path")
        != (output / "non_economic_audit_receipt.json").as_posix()
    ):
        raise CrossEcologyAnalogManifestError("0036 completed-result adoption drift")

    compatible = manifest.get("compatible_artifact_manifest_hashes")
    if not isinstance(compatible, (list, tuple)) or tuple(compatible) != (
        old_manifest_hash,
    ):
        raise CrossEcologyAnalogManifestError("0036 adoption compatibility drift")
    _resumable_snapshot_adoption(
        manifest,
        old_manifest_hash=old_manifest_hash,
        old_source_commit=old_source_commit,
    )
    _verify_adopted_source_manifest_blob(root, adoption)


def _verify_adopted_source_manifest_blob(
    root: Path, adoption: Mapping[str, Any]
) -> None:
    commit = str(adoption["source_manifest_worm_commit"])
    tag = str(adoption["source_manifest_worm_tag"])
    tagged = subprocess.run(
        ["git", "rev-parse", f"{tag}^{{commit}}"],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )
    blob = subprocess.run(
        ["git", "show", f"{commit}:{adoption['source_manifest_path']}"],
        cwd=root,
        check=False,
        capture_output=True,
    )
    if (
        tagged.returncode != 0
        or tagged.stdout.strip() != commit
        or blob.returncode != 0
        or hashlib.sha256(blob.stdout).hexdigest()
        != adoption.get("source_manifest_file_sha256")
    ):
        raise CrossEcologyAnalogManifestError("0036 adopted source-manifest WORM drift")
    try:
        frozen = json.loads(blob.stdout)
    except json.JSONDecodeError as exc:
        raise CrossEcologyAnalogManifestError(
            "0036 adopted source-manifest WORM drift"
        ) from exc
    if (
        not isinstance(frozen, Mapping)
        or frozen.get("manifest_hash") != adoption.get("source_manifest_hash")
        or frozen.get("source_commit") != adoption.get("source_commit")
        or frozen.get("campaign_id") != CAMPAIGN_ID
    ):
        raise CrossEcologyAnalogManifestError("0036 adopted source-manifest WORM drift")


def _resumable_snapshot_adoption(
    manifest: Mapping[str, Any], *, old_manifest_hash: str, old_source_commit: str
) -> None:
    section = manifest.get("resumable_snapshot_compatibility")
    if not isinstance(section, Mapping):
        raise CrossEcologyAnalogManifestError("0036 snapshot compatibility drift")
    expected_top = {
        "schema",
        "classification",
        "economic_outcomes_changed",
        "scientific_policy_changed",
        "bindings",
    }
    if (
        set(section) != expected_top
        or section.get("schema") != "hydra_resumable_snapshot_compatibility_v1"
        or section.get("classification")
        != "HASH_BOUND_TECHNICAL_REVISION_RESUME_ONLY"
        or section.get("economic_outcomes_changed") is not False
        or section.get("scientific_policy_changed") is not False
    ):
        raise CrossEcologyAnalogManifestError("0036 snapshot compatibility drift")
    output = Path(str(_mapping(manifest, "runtime").get("output_dir") or ""))
    specs = {
        (output / "production_state.json").as_posix(): (
            "hydra_economic_production_state_v1",
            "state_hash",
        ),
        (output / "production_kpis.json").as_posix(): (
            "hydra_economic_production_kpis_v1",
            "kpi_hash",
        ),
    }
    bindings = section.get("bindings")
    if not isinstance(bindings, (list, tuple)) or len(bindings) != len(specs):
        raise CrossEcologyAnalogManifestError("0036 snapshot compatibility drift")
    observed: set[str] = set()
    required_fields = {
        "path",
        "schema",
        "hash_field",
        "manifest_hash",
        "source_commit",
        "snapshot_hash",
    }
    for raw_binding in bindings:
        if not isinstance(raw_binding, Mapping) or set(raw_binding) != required_fields:
            raise CrossEcologyAnalogManifestError("0036 snapshot compatibility drift")
        path = str(raw_binding.get("path") or "")
        expected = specs.get(path)
        if (
            expected is None
            or path in observed
            or raw_binding.get("schema") != expected[0]
            or raw_binding.get("hash_field") != expected[1]
            or raw_binding.get("manifest_hash") != old_manifest_hash
            or raw_binding.get("source_commit") != old_source_commit
            or not _SHA256.fullmatch(str(raw_binding.get("snapshot_hash") or ""))
        ):
            raise CrossEcologyAnalogManifestError("0036 snapshot compatibility drift")
        observed.add(path)
    if observed != set(specs):
        raise CrossEcologyAnalogManifestError("0036 snapshot compatibility drift")


def _project_root(path: Path) -> Path:
    try:
        root = path.parents[2]
    except IndexError as exc:
        raise CrossEcologyAnalogManifestError("0036 manifest path is too shallow") from exc
    if path.parent.name != "v7" or path.parent.parent.name != "config":
        raise CrossEcologyAnalogManifestError("0036 manifest must live under config/v7")
    return root.resolve()


def _project_path(root: Path, value: Any, label: str) -> Path:
    if not isinstance(value, str) or not value or Path(value).is_absolute():
        raise CrossEcologyAnalogManifestError(f"0036 {label} path is invalid")
    resolved = (root / value).resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise CrossEcologyAnalogManifestError(f"0036 {label} path escapes root") from exc
    return resolved


def _project_file(root: Path, value: Any, label: str) -> Path:
    resolved = _project_path(root, value, label)
    if not resolved.is_file():
        raise CrossEcologyAnalogManifestError(f"0036 {label} file is missing")
    return resolved


def _mapping(value: Mapping[str, Any], field: str) -> Mapping[str, Any]:
    candidate = value.get(field)
    if not isinstance(candidate, Mapping):
        raise CrossEcologyAnalogManifestError(f"0036 {field} must be an object")
    return candidate


def _integer(value: Mapping[str, Any], field: str) -> int:
    candidate = value.get(field)
    if not isinstance(candidate, int) or isinstance(candidate, bool):
        raise CrossEcologyAnalogManifestError(f"0036 {field} must be an integer")
    return candidate


def _load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CrossEcologyAnalogManifestError(f"invalid JSON: {path}") from exc
    if not isinstance(value, dict):
        raise CrossEcologyAnalogManifestError(f"JSON object required: {path}")
    return value


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = [
    "CAMPAIGN_ID",
    "CAMPAIGN_MODE",
    "CAMPAIGN_ORDINAL",
    "CLASS_ID",
    "CrossEcologyAnalogManifestError",
    "DEFAULT_MANIFEST_PATH",
    "EVIDENCE_ROLE",
    "ROOT_AUTHORIZATION",
    "RUNTIME_VERSION",
    "SCIENTIFIC_RESULT_SCHEMA",
    "SCIENTIFIC_STATUSES",
    "SOURCE_MODES",
    "TIER_CEILING",
    "validate_cross_ecology_analog_manifest",
]
