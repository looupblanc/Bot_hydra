"""Production-kernel adapter for the bounded HYDRA 0036 economic router.

The adapter either adopts one explicitly hash-bound scientific result or runs
the exact root-authorized read-only router once.  It then materializes the
canonical ledgers embedded by that same economic run into EvidenceBundle v1.
No market, signal, trade, or account replay occurs during evidence sealing.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np

from hydra.economic_evolution.schema import stable_hash
from hydra.evidence import (
    EvidenceBundleWriter,
    RECORD_SPECS,
    REQUIRED_DATASETS,
    iter_evidence_records,
    recover_finalized_evidence_bundle,
    verify_evidence_bundle,
)
from hydra.evidence.schema import validate_identity
from hydra.production.cross_ecology_analog_manifest import (
    CAMPAIGN_ID,
    CAMPAIGN_MODE,
    CAMPAIGN_ORDINAL,
    CLASS_ID,
    COMPLETED_RESULT_ADOPTION_CLASSIFICATION,
    COMPLETED_RESULT_ADOPTION_SCHEMA,
    DEFAULT_MANIFEST_PATH,
    EVIDENCE_ROLE,
    NON_ECONOMIC_AUDIT_RECEIPT_SCHEMA,
    NON_ECONOMIC_CANONICAL_STATUS,
    NON_ECONOMIC_TERMINAL_MODE,
    ROOT_AUTHORIZATION,
    RUNTIME_VERSION,
    SCIENTIFIC_RESULT_SCHEMA,
    SCIENTIFIC_STATUSES,
    validate_cross_ecology_analog_manifest,
)
from hydra.production.halving import build_final_result_payload


STATE_SCHEMA = "hydra_economic_production_state_v1"
KPI_SCHEMA = "hydra_economic_production_kpis_v1"
RESULT_SCHEMA = "hydra_economic_production_result_v1"
REPLAY_LEASE_SCHEMA = "hydra_cross_ecology_0036_single_run_lease_v1"
REPLAY_LEASE_STATUSES = frozenset({"RUNNING", "COMPLETE"})
SAFETY_COUNTER_FIELDS = (
    "network_requests",
    "data_purchase_count",
    "q4_access_count_delta",
    "broker_connections",
    "orders",
    "mission_database_writes",
    "registry_writes",
    "cemetery_writes",
)


class CrossEcologyAnalogRuntimeError(RuntimeError):
    """0036 cannot proceed without violating its immutable production contract."""


def read_cross_ecology_analog_status(manifest_path: str | Path) -> dict[str, Any]:
    path = Path(manifest_path).resolve()
    from hydra.production.manifest import load_and_validate_production_manifest

    manifest = load_and_validate_production_manifest(path)
    validate_cross_ecology_analog_manifest(manifest, manifest_path=path)
    root = path.parents[2]
    _assert_closed_governance_environment()
    _verify_multiplicity_reservation(root, manifest)
    output = root / str(manifest["runtime"]["output_dir"])
    result_path = output / "economic_production_result.json"
    if result_path.is_file():
        return _load_terminal_result(result_path, manifest, output)
    checkpoint = _read_checkpoint_pair(output, manifest)
    if checkpoint is not None:
        return checkpoint[0]
    adoption = _completed_adoption(manifest)
    return {
        "campaign_id": CAMPAIGN_ID,
        "state": "NOT_STARTED",
        "next_action": (
            "ADOPT_COMPLETED_HASH_BOUND_RESULT_WITHOUT_REPLAY"
            if adoption is not None
            else "RUN_OR_ADOPT_EXACT_ROOT_AUTHORIZED_0036_REPLAY_ONCE"
        ),
    }


def run_cross_ecology_analog_manifest(
    manifest_path: str | Path,
    *,
    contract_map_path: str | Path | None = None,
    cache_root: str | Path | None = None,
    stop_after: str | None = None,
) -> dict[str, Any]:
    """Run/adopt 0036 once and publish the terminal result as the last write."""

    del contract_map_path, cache_root
    if stop_after is not None and os.environ.get("HYDRA_PRODUCTION_TEST_MODE") != "1":
        raise CrossEcologyAnalogRuntimeError(
            "0036 stop_after is restricted to explicit test mode"
        )
    path = Path(manifest_path).resolve()
    root = path.parents[2]
    from hydra.production.manifest import load_and_validate_production_manifest

    manifest = load_and_validate_production_manifest(path)
    validate_cross_ecology_analog_manifest(manifest, manifest_path=path)
    _assert_closed_governance_environment()
    _verify_multiplicity_reservation(root, manifest)
    output = root / str(manifest["runtime"]["output_dir"])
    result_path = output / "economic_production_result.json"

    # A complete result is a read-only terminal state.  This branch deliberately
    # precedes mkdir, snapshot refreshes, receipts, and every other durable write.
    if result_path.is_file():
        return _load_terminal_result(result_path, manifest, output)

    _set_single_thread_libraries()
    output.mkdir(parents=True, exist_ok=True)
    started_wall = time.perf_counter()
    started_cpu = time.process_time()
    sequence = _next_sequence(output, manifest)
    _publish(
        output,
        manifest,
        state="STARTING",
        stage="SCIENTIFIC_SOURCE_BINDING",
        next_action=(
            "ADOPT_COMPLETED_HASH_BOUND_RESULT_WITHOUT_REPLAY"
            if _completed_adoption(manifest) is not None
            else "ADOPT_OR_RUN_EXACT_ROOT_AUTHORIZED_READ_ONLY_RESULT_ONCE"
        ),
        sequence=sequence,
        metrics=None,
        elapsed=0.0,
        cpu_seconds=0.0,
    )
    if stop_after and stop_after.upper() in {"START", "STARTING"}:
        return _read_snapshot(output / "production_state.json", "state_hash", manifest)

    scientific_path, replay_executed = _obtain_scientific_result(
        root,
        output,
        manifest,
        production_manifest_path=path,
    )
    scientific = _load_scientific_result(scientific_path, manifest)
    canonical = _canonical_material(scientific, manifest)
    metrics = _economic_metrics(scientific, canonical)
    elapsed = max(time.perf_counter() - started_wall, 1e-9)
    cpu_seconds = max(time.process_time() - started_cpu, 0.0)
    sequence += 1
    _publish(
        output,
        manifest,
        state=(
            "EXACT_REPLAY_ACTIVE"
            if replay_executed
            else (
                "NON_ECONOMIC_AUDIT_ACTIVE"
                if canonical is None
                else "CANONICAL_ADOPTION_ACTIVE"
            )
        ),
        stage="CANONICAL_EVIDENCE_ADOPTION",
        next_action=(
            "PUBLISH_NON_ECONOMIC_AUDIT_WITHOUT_REPLAY"
            if canonical is None
            else "SEAL_EMBEDDED_CANONICAL_EVIDENCE_WITHOUT_REPLAY"
        ),
        sequence=sequence,
        metrics=metrics,
        elapsed=elapsed,
        cpu_seconds=cpu_seconds,
        replay_executed=replay_executed,
    )

    receipt = (
        _publish_non_economic_audit(
            root,
            output,
            manifest,
            scientific,
            scientific_path,
            metrics,
        )
        if canonical is None
        else _seal_evidence(root, output, manifest, scientific, canonical, metrics)
    )
    decision_report = _decision_report(manifest, scientific, metrics, scientific_path)
    _atomic_json(output / "decision_report.json", decision_report)

    elapsed = max(time.perf_counter() - started_wall, 1e-9)
    cpu_seconds = max(time.process_time() - started_cpu, 0.0)
    sequence += 1
    final_kpis = _kpis(
        manifest,
        state="COMPLETE",
        sequence=sequence,
        metrics=metrics,
        elapsed=elapsed,
        cpu_seconds=cpu_seconds,
        replay_executed=replay_executed,
    )
    terminal = _terminal_result(
        manifest=manifest,
        scientific=scientific,
        scientific_path=scientific_path,
        receipt=receipt,
        metrics=metrics,
        kpis=final_kpis,
        decision_report=decision_report,
        replay_executed=replay_executed,
        non_economic_audit=receipt if canonical is None else None,
    )

    # COMPLETE views are published first.  The atomic terminal result is the
    # literal final durable write, so a restart can return without touching disk.
    _write_state(
        output,
        manifest,
        state="COMPLETE",
        stage=(
            "NON_ECONOMIC_BRANCH_DECISION_SEALED"
            if canonical is None
            else "TIER_E_BRANCH_DECISION_SEALED"
        ),
        next_action=str(terminal["autonomous_next_action"]["action"]),
        sequence=sequence,
        metrics=metrics,
    )
    _atomic_json(output / "production_kpis.json", final_kpis)
    _atomic_json(result_path, terminal)
    return terminal


def _obtain_scientific_result(
    root: Path,
    output: Path,
    manifest: Mapping[str, Any],
    *,
    production_manifest_path: str | Path | None = None,
) -> tuple[Path, bool]:
    # This helper can be called independently of the outer runtime in tests or
    # future adapters.  Re-prove the controller reservation here before even
    # testing whether a generated/preexisting outcome file exists.  Economic
    # source access must never be reachable through generic manifest loading.
    _verify_multiplicity_reservation(root, manifest)
    source = manifest["research_source"]
    source_path = _inside(root, source["result_path"])
    if source["source_mode"] == "PREEXISTING_HASH_BOUND":
        _load_scientific_result(source_path, manifest, require_hash_binding=True)
        if _completed_adoption(manifest) is not None:
            _validate_adopted_replay_lease(output, manifest, source_path)
        return source_path, False

    lease_path = output / "scientific_replay_attempt.json"
    if source_path.is_file():
        if not lease_path.is_file():
            raise CrossEcologyAnalogRuntimeError(
                "unleased generated scientific result; bind it PREEXISTING_HASH_BOUND"
            )
        lease = _read_hashed(lease_path, "attempt_hash")
        _validate_replay_lease(lease, manifest)
        _load_scientific_result(source_path, manifest)
        if lease.get("status") == "COMPLETE":
            if (
                lease.get("result_hash") != _read_json(source_path).get("result_hash")
                or lease.get("result_file_sha256") != _sha256(source_path)
            ):
                raise CrossEcologyAnalogRuntimeError(
                    "0036 completed scientific replay lease/result drift"
                )
        else:
            complete = dict(lease)
            complete.pop("attempt_hash", None)
            complete.update(
                status="COMPLETE",
                result_hash=_read_json(source_path)["result_hash"],
                result_file_sha256=_sha256(source_path),
            )
            complete["attempt_hash"] = stable_hash(complete)
            _atomic_json(lease_path, complete)
        # The lease proves that the sole replay happened previously.  Merely
        # adopting its immutable output in this invocation is not a replay.
        return source_path, False

    if lease_path.is_file():
        lease = _read_hashed(lease_path, "attempt_hash")
        _validate_replay_lease(lease, manifest)
        if lease.get("status") == "RUNNING":
            raise CrossEcologyAnalogRuntimeError(
                "0036 root-authorized replay already started without a durable result; relaunch forbidden"
            )
        raise CrossEcologyAnalogRuntimeError("0036 scientific replay lease/result mismatch")

    lease = {
        "schema": REPLAY_LEASE_SCHEMA,
        "campaign_id": CAMPAIGN_ID,
        "manifest_hash": manifest["manifest_hash"],
        "source_commit": manifest["source_commit"],
        "generation": 0,
        "maximum_generations": 1,
        "status": "RUNNING",
        "authorization": ROOT_AUTHORIZATION,
        "runner_pid": os.getpid(),
        "q4_access_count_delta": 0,
        "data_purchase_count": 0,
        "network_requests": 0,
        "broker_connections": 0,
        "orders": 0,
        "mission_database_writes": 0,
        "registry_writes": 0,
        "cemetery_writes": 0,
    }
    lease["attempt_hash"] = stable_hash(lease)
    _atomic_json(lease_path, lease)

    from hydra.research.cross_ecology_session_path_analog_router import (
        run_economic_tripwire,
    )

    result = run_economic_tripwire(
        root,
        authorization=ROOT_AUTHORIZATION,
        card_path=source["decision_card_path"],
        production_manifest_path=(
            production_manifest_path or DEFAULT_MANIFEST_PATH
        ),
    )
    _validate_scientific_payload(result, manifest)
    _atomic_json(source_path, result)
    lease.pop("attempt_hash", None)
    lease.update(
        status="COMPLETE",
        result_hash=result["result_hash"],
        result_file_sha256=_sha256(source_path),
    )
    lease["attempt_hash"] = stable_hash(lease)
    _atomic_json(lease_path, lease)
    return source_path, True


def _load_scientific_result(
    path: Path,
    manifest: Mapping[str, Any],
    *,
    require_hash_binding: bool = False,
) -> dict[str, Any]:
    result = _read_json(path)
    _validate_scientific_payload(result, manifest)
    adoption = _completed_adoption(manifest)
    if adoption is not None and (
        _sha256(path) != adoption.get("scientific_result_file_sha256")
        or result.get("result_hash") != adoption.get("scientific_result_hash")
    ):
        raise CrossEcologyAnalogRuntimeError(
            "0036 adopted scientific result hash binding drift"
        )
    if require_hash_binding:
        source = manifest["research_source"]
        if (
            _sha256(path) != source.get("result_file_sha256")
            or result.get("result_hash") != source.get("result_hash")
        ):
            raise CrossEcologyAnalogRuntimeError("0036 scientific result hash binding drift")
    return result


def _validate_scientific_payload(
    result: Mapping[str, Any], manifest: Mapping[str, Any]
) -> None:
    claimed = str(result.get("result_hash") or "")
    core = dict(result)
    core.pop("result_hash", None)
    source = manifest["research_source"]
    adoption = _completed_adoption(manifest)
    expected_source_commit = (
        adoption["source_commit"] if adoption is not None else manifest.get("source_commit")
    )
    expected_manifest_hash = (
        adoption["source_manifest_hash"]
        if adoption is not None
        else manifest.get("manifest_hash")
    )
    expected_implementation_files = (
        None if adoption is not None else dict(sorted(manifest["implementation_files"].items()))
    )
    audit = result.get("source_audit")
    governance = result.get("governance")
    production = result.get("production_manifest")
    multiplicity = manifest["multiplicity"]
    production_reservation = (
        production.get("multiplicity_reservation")
        if isinstance(production, Mapping)
        else None
    )
    if (
        result.get("schema") != SCIENTIFIC_RESULT_SCHEMA
        or result.get("campaign_id") != CAMPAIGN_ID
        or result.get("branch_id") != CLASS_ID
        or result.get("source_commit") != expected_source_commit
        or result.get("status") not in SCIENTIFIC_STATUSES
        or result.get("evidence_role") != EVIDENCE_ROLE
        or result.get("evidence_tier_ceiling") != "TIER_E_EXECUTABLE_DIAGNOSTIC"
        or not claimed
        or stable_hash(core) != claimed
        or not isinstance(audit, Mapping)
        or audit.get("decision_card_hash") != source.get("decision_card_hash")
        or audit.get("decision_card_file_sha256")
        != source.get("decision_card_file_sha256")
        or not isinstance(production, Mapping)
        or production.get("schema") != manifest.get("schema")
        or production.get("campaign_id") != CAMPAIGN_ID
        or int(production.get("campaign_ordinal", -1)) != CAMPAIGN_ORDINAL
        or production.get("path") != DEFAULT_MANIFEST_PATH
        or production.get("production_manifest_hash")
        != expected_manifest_hash
        or production.get("source_commit") != expected_source_commit
        or production.get("decision_card_hash")
        != source.get("decision_card_hash")
        or (
            expected_implementation_files is not None
            and production.get("implementation_files") != expected_implementation_files
        )
        or production.get("verified_against_committed_blobs") is not True
        or production.get("source_commit_is_live_head_ancestor") is not True
        or not isinstance(production_reservation, Mapping)
        or production_reservation.get("path")
        != multiplicity.get("reservation_receipt_path")
        or production_reservation.get("sha256")
        != multiplicity.get("reservation_receipt_sha256")
        or production_reservation.get("reserved_delta_trials")
        != multiplicity.get("reserved_delta_trials")
        or not isinstance(governance, Mapping)
        or governance.get("tier_q_allowed") is not False
        or governance.get("promotion_allowed") is not False
    ):
        raise CrossEcologyAnalogRuntimeError("0036 scientific result identity drift")
    _require_exact_zero_counters(
        audit, SAFETY_COUNTER_FIELDS, "0036 scientific source_audit"
    )
    manifest_file_sha = str(production.get("manifest_file_sha256") or "")
    if len(manifest_file_sha) != 64 or any(
        value not in "0123456789abcdef" for value in manifest_file_sha
    ):
        raise CrossEcologyAnalogRuntimeError(
            "0036 scientific production-manifest file hash is invalid"
        )
    if adoption is not None and (
        result.get("result_hash") != adoption.get("scientific_result_hash")
        or manifest_file_sha != adoption.get("source_manifest_file_sha256")
        or result.get("status") != adoption.get("scientific_status")
        or result.get("canonical_evidence_status")
        != adoption.get("canonical_evidence_status")
    ):
        raise CrossEcologyAnalogRuntimeError(
            "0036 adopted scientific result identity drift"
        )
    _require_exact_zero_counters(
        governance, SAFETY_COUNTER_FIELDS, "0036 scientific governance"
    )
    spend = governance.get("incremental_data_spend_usd")
    if (
        not isinstance(spend, (int, float))
        or isinstance(spend, bool)
        or not math.isfinite(float(spend))
        or float(spend) != 0.0
    ):
        raise CrossEcologyAnalogRuntimeError(
            "0036 scientific governance invariant violated: incremental_data_spend_usd"
        )


def _completed_adoption(manifest: Mapping[str, Any]) -> dict[str, Any] | None:
    raw = manifest.get("completed_scientific_result_adoption")
    if raw is None:
        return None
    if (
        not isinstance(raw, Mapping)
        or raw.get("schema") != COMPLETED_RESULT_ADOPTION_SCHEMA
        or raw.get("classification") != COMPLETED_RESULT_ADOPTION_CLASSIFICATION
        or raw.get("terminal_mode") != NON_ECONOMIC_TERMINAL_MODE
        or raw.get("economic_outcomes_changed") is not False
        or raw.get("scientific_policy_changed") is not False
        or raw.get("economic_replay_allowed") is not False
    ):
        raise CrossEcologyAnalogRuntimeError("0036 completed-result adoption drift")
    return dict(raw)


def _canonical_material(
    scientific: Mapping[str, Any], manifest: Mapping[str, Any]
) -> dict[str, Any] | None:
    material = scientific.get("canonical_evidence_material")
    if not isinstance(material, Mapping):
        _validate_nonmaterialized_scientific_result(scientific, manifest)
        return None
    checked = dict(material)
    claimed = str(checked.pop("canonical_material_hash", ""))
    identity = material.get("identity")
    datasets = material.get("datasets")
    hashes = material.get("dataset_hashes")
    source_audit = material.get("source_audit")
    governance = material.get("governance")
    if (
        material.get("contract") != "HYDRA_EVIDENCE_BUNDLE_V1"
        or int(material.get("schema_version", -1)) != 1
        or material.get("adapter_requires_economic_replay") is not False
        or not claimed
        or stable_hash(checked) != claimed
        or not isinstance(identity, Mapping)
        or not isinstance(datasets, Mapping)
        or set(datasets) != set(REQUIRED_DATASETS)
        or not isinstance(hashes, Mapping)
        or set(hashes) != set(REQUIRED_DATASETS)
        or not isinstance(source_audit, Mapping)
        or not isinstance(governance, Mapping)
    ):
        raise CrossEcologyAnalogRuntimeError("0036 canonical material contract drift")
    _require_exact_zero_counters(
        source_audit, SAFETY_COUNTER_FIELDS, "0036 canonical source_audit"
    )
    _require_exact_zero_counters(
        governance, SAFETY_COUNTER_FIELDS, "0036 canonical governance"
    )
    identity = validate_identity(identity)
    if (
        identity["campaign_id"] != CAMPAIGN_ID
        or identity["source_commit"] != manifest.get("source_commit")
        or identity["grammar_id"] != CLASS_ID
        or identity["configuration_sha256"]
        != manifest["research_source"]["decision_card_hash"]
    ):
        raise CrossEcologyAnalogRuntimeError("0036 canonical identity drift")
    for dataset in REQUIRED_DATASETS:
        rows = datasets[dataset]
        if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes)) or not rows:
            raise CrossEcologyAnalogRuntimeError(
                f"0036 canonical dataset is empty: {dataset}"
            )
        if stable_hash(rows) != hashes[dataset]:
            raise CrossEcologyAnalogRuntimeError(
                f"0036 canonical dataset hash drift: {dataset}"
            )
    if scientific.get("canonical_evidence_material_hash") not in (None, claimed):
        raise CrossEcologyAnalogRuntimeError("0036 top-level canonical hash drift")
    return dict(material)


def _validate_nonmaterialized_scientific_result(
    scientific: Mapping[str, Any], manifest: Mapping[str, Any]
) -> None:
    """Accept only the exact zero-material underpowered scientific outcome."""

    decisions = scientific.get("candidate_decisions")
    bundles = scientific.get("evidence_bundles")
    bundle_hashes = scientific.get("evidence_bundle_hashes")
    gate = scientific.get("branch_gate")
    if (
        scientific.get("status")
        != "SESSION_PATH_ANALOG_UNDERPOWERED_NO_THRESHOLD_RELAXATION"
        or scientific.get("canonical_evidence_status") != NON_ECONOMIC_CANONICAL_STATUS
        or scientific.get("canonical_evidence_material") is not None
        or scientific.get("canonical_evidence_material_hash") not in (None, "")
        or not isinstance(decisions, Sequence)
        or isinstance(decisions, (str, bytes))
        or len(decisions) != int(manifest["multiplicity"]["prospective_comparisons"])
        or not isinstance(bundles, Mapping)
        or not isinstance(bundle_hashes, Mapping)
        or not isinstance(gate, Mapping)
        or gate.get("status") != scientific.get("status")
        or list(gate.get("passed_candidate_ids") or ())
        or gate.get("threshold_relaxation_allowed") is not False
    ):
        raise CrossEcologyAnalogRuntimeError(
            "0036 non-economic scientific result contract drift"
        )
    candidate_ids = [str(row.get("candidate_id") or "") for row in decisions]
    if (
        not all(candidate_ids)
        or len(candidate_ids) != len(set(candidate_ids))
        or set(bundles) != set(candidate_ids)
        or set(bundle_hashes) != set(candidate_ids)
    ):
        raise CrossEcologyAnalogRuntimeError(
            "0036 non-economic candidate inventory drift"
        )
    empty_ledger_hash = stable_hash([])
    for decision in decisions:
        if not isinstance(decision, Mapping):
            raise CrossEcologyAnalogRuntimeError(
                "0036 non-economic decision is malformed"
            )
        scalar_zero_fields = (
            "materialized_economic_event_count",
            "routed_event_count",
            "future_outcome_censored_signal_count",
        )
        if any(
            type(decision.get(field)) is not int or decision.get(field) != 0
            for field in scalar_zero_fields
        ):
            raise CrossEcologyAnalogRuntimeError(
                "0036 non-economic decision unexpectedly materialized an event"
            )
        for field in (
            "routed_signals_by_role",
            "routed_events_by_role",
            "control_event_counts",
        ):
            counts = decision.get(field)
            if not isinstance(counts, Mapping) or any(
                type(value) is not int or value != 0 for value in counts.values()
            ):
                raise CrossEcologyAnalogRuntimeError(
                    "0036 non-economic decision count drift"
                )
        if (
            decision.get("routed_events_by_market") != {}
            or decision.get("control_opportunity_identity_equal") is not True
            or decision.get("event_ledger_hash") != empty_ledger_hash
            or not _is_exact_zero_number(
                decision.get("discovery_stressed_net_per_one_micro_usd")
            )
            or not _is_exact_zero_number(decision.get("discovery_target_first_rate"))
        ):
            raise CrossEcologyAnalogRuntimeError(
                "0036 non-economic decision ledger identity drift"
            )
        _validate_zero_decision_account_material(decision)
        candidate_id = str(decision["candidate_id"])
        bundle = bundles[candidate_id]
        if not isinstance(bundle, Mapping):
            raise CrossEcologyAnalogRuntimeError(
                "0036 non-economic candidate fragment is malformed"
            )
        bundle_core = dict(bundle)
        claimed = str(bundle_core.pop("evidence_bundle_hash", ""))
        row_counts = bundle.get("row_counts")
        if (
            not claimed
            or stable_hash(bundle_core) != claimed
            or bundle_hashes[candidate_id] != claimed
            or decision.get("evidence_bundle_hash") != claimed
            or bundle.get("schema") != "hydra_compact_complete_evidence_bundle_v1"
            or bundle.get("campaign_id") != CAMPAIGN_ID
            or bundle.get("candidate_id") != candidate_id
            or bundle.get("source_commit") != scientific.get("source_commit")
            or bundle.get("tier_ceiling") != "E"
            or bundle.get("tier_q_allowed") is not False
            or bundle.get("promotion_allowed") is not False
            or bundle.get("complete") is not False
            or bundle.get("materialization_status")
            != "NON_MATERIALIZED_FRAGMENT_EXCLUDED"
            or bundle.get("canonical_evidence_material") is not None
            or not isinstance(row_counts, Mapping)
            or any(
                type(row_counts.get(field)) is not int
                or row_counts.get(field) != expected
                for field, expected in (
                    ("canonical_rows", 0),
                    ("controls", 5),
                    ("routed_event_rows", 0),
                    ("routed_trade_rows", 0),
                    ("scenarios", 2),
                )
            )
        ):
            raise CrossEcologyAnalogRuntimeError(
                "0036 non-economic candidate fragment drift"
            )
        _validate_empty_routed_ledgers(bundle, empty_ledger_hash)
        _validate_zero_account_episode_material(bundle.get("account_episode_material"))
        ledger_hashes = bundle.get("ledger_hashes")
        if (
            not isinstance(ledger_hashes, Mapping)
            or ledger_hashes.get("account_material")
            != stable_hash(bundle.get("account_episode_material"))
        ):
            raise CrossEcologyAnalogRuntimeError(
                "0036 non-economic account material hash drift"
            )

    power = scientific.get("power_preflight")
    if (
        not isinstance(power, Mapping)
        or power.get("passed") is not False
        or power.get("underpowered_status") != scientific.get("status")
    ):
        raise CrossEcologyAnalogRuntimeError(
            "0036 non-economic power preflight drift"
        )


def _validate_empty_routed_ledgers(
    bundle: Mapping[str, Any], empty_ledger_hash: str
) -> None:
    expected_controls = {
        "PRIMARY",
        "OWN_PATH_ONLY",
        "SESSION_MARKET_EXPOSURE_MATCHED_RANDOM",
        "ANALOG_LABEL_PERMUTATION",
        "DIRECTION_FLIP",
    }
    expected_scenarios = {"NORMAL", "STRESSED_1_5X"}
    events = bundle.get("routed_event_ledgers")
    trades = bundle.get("routed_trade_ledgers")
    hashes = bundle.get("ledger_hashes")
    event_hashes = hashes.get("events") if isinstance(hashes, Mapping) else None
    trade_hashes = hashes.get("trades") if isinstance(hashes, Mapping) else None
    if (
        not isinstance(events, Mapping)
        or not isinstance(trades, Mapping)
        or not isinstance(event_hashes, Mapping)
        or not isinstance(trade_hashes, Mapping)
        or set(events) != expected_controls
        or set(event_hashes) != expected_controls
        or set(trades) != expected_controls
        or set(trade_hashes) != expected_controls
    ):
        raise CrossEcologyAnalogRuntimeError(
            "0036 non-economic routed ledger contract drift"
        )
    for control, rows in events.items():
        if rows != [] or event_hashes.get(control) != empty_ledger_hash:
            raise CrossEcologyAnalogRuntimeError(
                "0036 non-economic fragment contains a routed event"
            )
    for control, scenarios in trades.items():
        expected = trade_hashes.get(control)
        if (
            not isinstance(scenarios, Mapping)
            or not isinstance(expected, Mapping)
            or set(scenarios) != expected_scenarios
            or set(expected) != expected_scenarios
        ):
            raise CrossEcologyAnalogRuntimeError(
                "0036 non-economic routed trade ledger contract drift"
            )
        for scenario, rows in scenarios.items():
            if rows != [] or expected.get(scenario) != empty_ledger_hash:
                raise CrossEcologyAnalogRuntimeError(
                    "0036 non-economic fragment contains a routed trade"
                )


def _validate_zero_account_episode_material(value: Any) -> None:
    if not isinstance(value, Mapping):
        raise CrossEcologyAnalogRuntimeError(
            "0036 non-economic account material is malformed"
        )
    evaluations = value.get("evaluations")
    if not isinstance(evaluations, Mapping):
        raise CrossEcologyAnalogRuntimeError(
            "0036 non-economic account evaluations are absent"
        )
    _validate_zero_evaluation_tree(evaluations, require_episode_ledger=True)


def _validate_zero_decision_account_material(decision: Mapping[str, Any]) -> None:
    frontier = decision.get("account_frontier")
    selected = decision.get("discovery_selected_account_cell")
    if (
        not isinstance(frontier, Sequence)
        or isinstance(frontier, (str, bytes))
        or not frontier
        or not isinstance(selected, Mapping)
    ):
        raise CrossEcologyAnalogRuntimeError(
            "0036 non-economic decision account frontier drift"
        )
    for cell in [*frontier, selected]:
        if not isinstance(cell, Mapping) or not isinstance(cell.get("evaluations"), Mapping):
            raise CrossEcologyAnalogRuntimeError(
                "0036 non-economic decision account cell drift"
            )
        _validate_zero_evaluation_tree(
            cell["evaluations"], require_episode_ledger=False
        )
    context = decision.get("stressed_context_economics_per_one_micro")
    temporal = context.get("net_by_temporal_subblock_usd") if isinstance(context, Mapping) else None
    if (
        not isinstance(context, Mapping)
        or set(context)
        != {
            "future_outcome_censored_signal_count",
            "net_by_market_usd",
            "net_by_temporal_subblock_usd",
            "positive_market_or_temporal_context_count",
            "positive_markets",
            "positive_temporal_subblocks",
        }
        or type(context.get("future_outcome_censored_signal_count")) is not int
        or context.get("future_outcome_censored_signal_count") != 0
        or context.get("net_by_market_usd") != {}
        or not isinstance(temporal, Mapping)
        or any(
            not isinstance(value, (int, float))
            or isinstance(value, bool)
            or not math.isfinite(float(value))
            or float(value) != 0.0
            for value in temporal.values()
        )
        or type(context.get("positive_market_or_temporal_context_count")) is not int
        or context.get("positive_market_or_temporal_context_count") != 0
        or context.get("positive_markets") != []
        or context.get("positive_temporal_subblocks") != []
    ):
        raise CrossEcologyAnalogRuntimeError(
            "0036 non-economic decision context contains economic evidence"
        )


def _validate_zero_evaluation_tree(
    evaluations: Mapping[str, Any], *, require_episode_ledger: bool
) -> None:
    found = 0

    def visit(node: Any) -> None:
        nonlocal found
        if isinstance(node, Mapping):
            if "episode_ledger" in node or "episodes" in node:
                found += 1
                exact_zero_ints = ("episodes", "passes", "mll_breaches")
                ledger = node.get("episode_ledger", [])
                if (
                    (require_episode_ledger and "episode_ledger" not in node)
                    or ledger != []
                    or node.get("episode_ledger_hash") != stable_hash([])
                    or any(
                        type(node.get(field)) is not int or node.get(field) != 0
                        for field in exact_zero_ints
                    )
                    or not _is_exact_zero_number(node.get("net_total_usd"))
                    or not _is_exact_zero_number(node.get("pass_rate"))
                    or not _is_exact_zero_number(node.get("mll_breach_rate"))
                    or not _is_exact_zero_number(node.get("target_progress_median"))
                    or not _is_exact_zero_number(node.get("target_progress_p25"))
                ):
                    raise CrossEcologyAnalogRuntimeError(
                        "0036 non-economic fragment contains account evidence"
                    )
            if "full_coverage_start_count" in node and (
                type(node.get("full_coverage_start_count")) is not int
                or node.get("full_coverage_start_count") != 0
            ):
                raise CrossEcologyAnalogRuntimeError(
                    "0036 non-economic fragment contains a full-coverage start"
                )
            for child in node.values():
                visit(child)
        elif isinstance(node, Sequence) and not isinstance(node, (str, bytes)):
            for child in node:
                visit(child)

    visit(evaluations)
    if found == 0:
        raise CrossEcologyAnalogRuntimeError(
            "0036 non-economic account evaluation contract drift"
        )


def _is_exact_zero_number(value: Any) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(float(value))
        and float(value) == 0.0
    )


def _validate_adopted_replay_lease(
    output: Path, manifest: Mapping[str, Any], source_path: Path
) -> dict[str, Any]:
    adoption = _completed_adoption(manifest)
    if adoption is None:
        raise CrossEcologyAnalogRuntimeError("0036 completed-result adoption is absent")
    root = source_path.parents[3]
    lease_path = _inside(root, adoption["replay_lease_path"])
    expected_path = (output / "scientific_replay_attempt.json").resolve()
    if (
        lease_path != expected_path
        or not lease_path.is_file()
        or _sha256(lease_path) != adoption.get("replay_lease_file_sha256")
    ):
        raise CrossEcologyAnalogRuntimeError("0036 adopted replay lease binding drift")
    lease = _read_hashed(lease_path, "attempt_hash")
    _validate_replay_lease(lease, manifest)
    if (
        lease.get("status") != "COMPLETE"
        or lease.get("attempt_hash") != adoption.get("replay_lease_attempt_hash")
        or lease.get("result_hash") != adoption.get("scientific_result_hash")
        or lease.get("result_file_sha256")
        != adoption.get("scientific_result_file_sha256")
    ):
        raise CrossEcologyAnalogRuntimeError("0036 adopted replay lease/result drift")
    return lease


def _seal_evidence(
    root: Path,
    output: Path,
    manifest: Mapping[str, Any],
    scientific: Mapping[str, Any],
    canonical: Mapping[str, Any],
    metrics: Mapping[str, Any],
) -> dict[str, Any]:
    base = root / str(manifest["evidence_bundle"]["destination"])
    final = base / f"{CAMPAIGN_ID}.evidence-v1"
    lightweight = output / "evidence_bundle_receipt.json"
    identity = canonical["identity"]
    if final.is_dir():
        receipt = recover_finalized_evidence_bundle(
            base,
            CAMPAIGN_ID,
            lightweight_manifest_path=lightweight,
            expected_identity=identity,
        )
        _verify_sealed_matches_canonical(receipt.bundle_path, canonical)
        return receipt.to_dict()
    staging = base / f".{CAMPAIGN_ID}.evidence-v1.staging"
    writer = (
        EvidenceBundleWriter.resume(base, CAMPAIGN_ID, expected_identity=identity)
        if staging.is_dir()
        else EvidenceBundleWriter.create(base, identity, writer_id=CAMPAIGN_ID)
    )
    compact = _compact_outputs(scientific, metrics)
    try:
        for dataset in REQUIRED_DATASETS:
            expected_rows = canonical["datasets"][dataset]
            # Always replay the deterministic batch identifier into the writer.
            # An exact committed batch is idempotently returned; a same-ID
            # payload drift fails before finalization.  Extra/differently named
            # staging parts are then rejected by the exact row-count check.
            writer.append_records(
                dataset,
                expected_rows,
                batch_id=f"0036-{dataset}-embedded-0000",
            )
            if int(writer.dataset_row_counts.get(dataset, -1)) != len(expected_rows):
                raise CrossEcologyAnalogRuntimeError(
                    f"0036 staging dataset count drift: {dataset}"
                )
        for name, value in compact.items():
            writer.write_compact_output(name, value)
        receipt = writer.finalize(
            evidence_status="FRESH_DEVELOPMENT_EVIDENCE",
            lightweight_manifest_path=lightweight,
        )
    finally:
        writer.close()
    verify_evidence_bundle(receipt.bundle_path, deep=True)
    _verify_sealed_matches_canonical(receipt.bundle_path, canonical)
    return receipt.to_dict()


def _publish_non_economic_audit(
    root: Path,
    output: Path,
    manifest: Mapping[str, Any],
    scientific: Mapping[str, Any],
    scientific_path: Path,
    metrics: Mapping[str, Any],
) -> dict[str, Any]:
    """Publish an audit receipt without manufacturing economic ledger rows."""

    adoption = _completed_adoption(manifest)
    if adoption is None:
        raise CrossEcologyAnalogRuntimeError(
            "0036 non-economic terminal requires explicit completed-result adoption"
        )
    lease = _validate_adopted_replay_lease(output, manifest, scientific_path)
    receipt_path = _inside(root, adoption["audit_receipt_path"])
    if receipt_path != (output / "non_economic_audit_receipt.json").resolve():
        raise CrossEcologyAnalogRuntimeError("0036 non-economic audit path drift")
    bundle_hashes = dict(sorted(scientific["evidence_bundle_hashes"].items()))
    core = {
        "schema": NON_ECONOMIC_AUDIT_RECEIPT_SCHEMA,
        "campaign_id": CAMPAIGN_ID,
        "manifest_hash": manifest["manifest_hash"],
        "source_commit": manifest["source_commit"],
        "terminal_mode": NON_ECONOMIC_TERMINAL_MODE,
        "classification": NON_ECONOMIC_CANONICAL_STATUS,
        "scientific_status": scientific["status"],
        "scientific_result_path": str(scientific_path),
        "scientific_result_hash": scientific["result_hash"],
        "scientific_result_file_sha256": _sha256(scientific_path),
        "source_manifest_hash": adoption["source_manifest_hash"],
        "source_manifest_file_sha256": adoption["source_manifest_file_sha256"],
        "source_manifest_worm_commit": adoption["source_manifest_worm_commit"],
        "source_manifest_worm_tag": adoption["source_manifest_worm_tag"],
        "source_commit_adopted": adoption["source_commit"],
        "replay_lease_path": str((output / "scientific_replay_attempt.json").resolve()),
        "replay_lease_attempt_hash": lease["attempt_hash"],
        "replay_lease_file_sha256": _sha256(
            output / "scientific_replay_attempt.json"
        ),
        "scientific_replay_previously_completed": True,
        "economic_replay_executed_by_adapter": False,
        "economic_outcomes_changed": False,
        "scientific_policy_changed": False,
        "economic_evidence_bundle_created": False,
        "evidence_tier_awarded": None,
        "diagnostic_candidate_count": int(metrics["diagnostic_candidate_count"]),
        "materialized_candidate_count": 0,
        "materialized_event_count": 0,
        "materialized_trade_count": 0,
        "materialized_episode_count": 0,
        "candidate_fragment_hashes": bundle_hashes,
        "candidate_fragment_hashes_hash": stable_hash(bundle_hashes),
        "promotion_allowed": False,
        "tier_q_allowed": False,
        "q4_access_count_delta": 0,
        "data_purchase_count": 0,
        "network_requests": 0,
        "broker_connections": 0,
        "orders": 0,
        "mission_database_writes": 0,
        "registry_writes": 0,
        "cemetery_writes": 0,
    }
    receipt = {**core, "receipt_hash": stable_hash(core)}
    _atomic_json(receipt_path, receipt)
    return {
        "schema": NON_ECONOMIC_AUDIT_RECEIPT_SCHEMA,
        "terminal_mode": NON_ECONOMIC_TERMINAL_MODE,
        "classification": NON_ECONOMIC_CANONICAL_STATUS,
        "path": str(receipt_path),
        "file_sha256": _sha256(receipt_path),
        "receipt_hash": receipt["receipt_hash"],
    }


def _verify_sealed_matches_canonical(
    bundle_path: str | Path, canonical: Mapping[str, Any]
) -> None:
    """Prove that recovery/adoption sealed the exact embedded row material."""

    for dataset in REQUIRED_DATASETS:
        sort_fields = RECORD_SPECS[dataset].sort_fields
        expected = sorted(
            [dict(row) for row in canonical["datasets"][dataset]],
            key=lambda row: tuple(str(row[field]) for field in sort_fields),
        )
        observed = sorted(
            list(iter_evidence_records(bundle_path, dataset)),
            key=lambda row: tuple(str(row[field]) for field in sort_fields),
        )
        if stable_hash(observed) != stable_hash(expected):
            raise CrossEcologyAnalogRuntimeError(
                f"0036 sealed evidence differs from embedded material: {dataset}"
            )


def _economic_metrics(
    scientific: Mapping[str, Any], canonical: Mapping[str, Any] | None
) -> dict[str, Any]:
    decisions = list(scientific.get("candidate_decisions") or [])
    if not decisions:
        raise CrossEcologyAnalogRuntimeError("0036 has no evaluated candidate decisions")
    if canonical is None:
        return {
            "proposal_count": len(decisions),
            "diagnostic_candidate_count": len(decisions),
            "candidate_count": 0,
            "canonical_policy_count": 0,
            "control_policy_count": 0,
            "normal_episode_count": 0,
            "stressed_episode_count": 0,
            "combine_episode_count": 0,
            "normal_pass_candidate_count": 0,
            "stressed_pass_candidate_count": 0,
            "positive_stressed_count": 0,
            "best_normal_pass_rate": 0.0,
            "median_normal_pass_rate": 0.0,
            "best_stressed_pass_rate": 0.0,
            "median_stressed_pass_rate": 0.0,
            "best_stressed_target_progress": 0.0,
            "median_stressed_target_progress": 0.0,
            "minimum_stressed_mll_breach_rate": 0.0,
            "maximum_stressed_mll_breach_rate": 0.0,
            "near_pass_count": 0,
            "tier_e_passed_candidate_ids": [],
            "headline_by_candidate": [],
            "economic_evidence_materialized": False,
            "canonical_evidence_status": NON_ECONOMIC_CANONICAL_STATUS,
        }
    bundles = scientific.get("evidence_bundles")
    if not isinstance(bundles, Mapping):
        raise CrossEcologyAnalogRuntimeError("0036 candidate EvidenceBundle fragments are absent")
    materialized_ids = {
        str(candidate_id)
        for candidate_id, bundle in bundles.items()
        if isinstance(bundle, Mapping)
        and isinstance(bundle.get("canonical_evidence_material"), Mapping)
    }
    primary_rows: list[dict[str, Any]] = []
    for decision in decisions:
        if str(decision.get("candidate_id") or "") not in materialized_ids:
            continue
        cell = decision.get("discovery_selected_account_cell")
        if not isinstance(cell, Mapping):
            raise CrossEcologyAnalogRuntimeError("0036 candidate lacks a discovery-selected cell")
        try:
            headline = cell["evaluations"]["PRIMARY"]["FINAL_DEVELOPMENT"]["20"]
            normal = headline["NORMAL"]
            stressed = headline["STRESSED_1_5X"]
        except (KeyError, TypeError) as exc:
            raise CrossEcologyAnalogRuntimeError("0036 headline frontier is incomplete") from exc
        primary_rows.append(
            {
                "candidate_id": str(decision["candidate_id"]),
                "normal": dict(normal),
                "stressed": dict(stressed),
            }
        )
    normal_rates = [float(row["normal"]["pass_rate"]) for row in primary_rows]
    stressed_rates = [float(row["stressed"]["pass_rate"]) for row in primary_rows]
    stressed_progress = [
        float(row["stressed"]["target_progress_median"]) for row in primary_rows
    ]
    mll_rates = [float(row["stressed"]["mll_breach_rate"]) for row in primary_rows]
    episodes = list(canonical["datasets"]["episodes"])
    normal_episodes = sum(row["cost_scenario"] == "NORMAL" for row in episodes)
    stressed_episodes = sum(row["cost_scenario"] == "STRESSED_1_5X" for row in episodes)
    if normal_episodes != stressed_episodes:
        raise CrossEcologyAnalogRuntimeError("0036 scenario episode counts do not reconcile")
    canonical_policy_count = len(canonical["identity"]["policy_fingerprints"])
    candidate_count = len(primary_rows)
    if candidate_count <= 0:
        raise CrossEcologyAnalogRuntimeError("0036 has no materialized executable candidate")
    if canonical_policy_count < candidate_count:
        raise CrossEcologyAnalogRuntimeError("0036 canonical policy inventory is incomplete")
    passed_ids = list(scientific.get("branch_gate", {}).get("passed_candidate_ids") or [])
    return {
        "proposal_count": len(decisions),
        "candidate_count": candidate_count,
        "canonical_policy_count": canonical_policy_count,
        "control_policy_count": canonical_policy_count - candidate_count,
        "normal_episode_count": normal_episodes,
        "stressed_episode_count": stressed_episodes,
        "combine_episode_count": normal_episodes + stressed_episodes,
        "normal_pass_candidate_count": sum(row["normal"]["passes"] > 0 for row in primary_rows),
        "stressed_pass_candidate_count": sum(row["stressed"]["passes"] > 0 for row in primary_rows),
        "positive_stressed_count": sum(row["stressed"]["net_total_usd"] > 0 for row in primary_rows),
        "best_normal_pass_rate": max(normal_rates),
        "median_normal_pass_rate": float(np.median(normal_rates)),
        "best_stressed_pass_rate": max(stressed_rates),
        "median_stressed_pass_rate": float(np.median(stressed_rates)),
        "best_stressed_target_progress": max(stressed_progress),
        "median_stressed_target_progress": float(np.median(stressed_progress)),
        "minimum_stressed_mll_breach_rate": min(mll_rates),
        "maximum_stressed_mll_breach_rate": max(mll_rates),
        "near_pass_count": sum(value >= 0.60 for value in stressed_progress),
        "tier_e_passed_candidate_ids": passed_ids,
        "headline_by_candidate": primary_rows,
        "economic_evidence_materialized": True,
        "canonical_evidence_status": "AGGREGATE_CANONICAL_EVIDENCE_MATERIALIZED",
    }


def _compact_outputs(
    scientific: Mapping[str, Any], metrics: Mapping[str, Any]
) -> dict[str, Any]:
    action = _next_action(str(scientific["status"]), metrics)
    failure = _failure_vectors(scientific)
    return {
        "campaign_summary": {
            "schema": "hydra_cross_ecology_0036_campaign_summary_v1",
            "campaign_id": CAMPAIGN_ID,
            "scientific_status": scientific["status"],
            "evidence_role": EVIDENCE_ROLE,
            "tier_ceiling": "E",
            "metrics": dict(metrics),
        },
        "failure_vectors": failure,
        "pareto_archive": {
            "schema": "hydra_cross_ecology_0036_pareto_archive_v1",
            "campaign_id": CAMPAIGN_ID,
            "candidate_ids": [
                str(row["candidate_id"])
                for row in scientific.get("candidate_decisions") or []
            ],
            "tier_e_passed_candidate_ids": list(
                metrics["tier_e_passed_candidate_ids"]
            ),
            "selection_role": "DISCOVERY_ONLY",
            "tier_q_allowed": False,
        },
        "next_campaign_recommendations": {
            "schema": "hydra_production_next_campaign_recommendations_v1",
            "campaign_id": CAMPAIGN_ID,
            "recommendation": action,
        },
    }


def _failure_vectors(scientific: Mapping[str, Any]) -> dict[str, Any]:
    rows: dict[str, Any] = {}
    for gate in scientific.get("branch_gate", {}).get("candidate_gates") or []:
        checks = dict(gate.get("checks") or {})
        rows[str(gate["candidate_id"])] = {
            "failed_checks": sorted(key for key, value in checks.items() if value is not True),
            "checks": checks,
            "matched_control_checks": dict(gate.get("matched_control_checks") or {}),
        }
    return {
        "schema": "hydra_cross_ecology_0036_failure_vectors_v1",
        "campaign_id": CAMPAIGN_ID,
        "by_candidate": rows,
        "threshold_relaxation_allowed": False,
    }


def _next_action(status: str, metrics: Mapping[str, Any]) -> dict[str, Any]:
    if status == "SESSION_PATH_ANALOG_TIER_E_DIAGNOSTIC_GREEN":
        action = "FREEZE_TIER_E_DIAGNOSTIC_AND_REQUIRE_SEPARATELY_FROZEN_UNSEEN_CONFIRMATION"
    elif status == "SESSION_PATH_ANALOG_UNDERPOWERED_NO_THRESHOLD_RELAXATION":
        action = "PRESERVE_NON_MATERIALIZED_DIAGNOSTIC_AND_PREAPPEND_DISTINCT_SUCCESSOR"
    else:
        action = "QUEUE_MATERIALLY_DISTINCT_MECHANISM_MANIFEST"
    return {
        "action": action,
        "manifest_required": True,
        "candidate_ids": list(metrics["tier_e_passed_candidate_ids"]),
        "tier_ceiling": "E",
        "tier_q_allowed": False,
        "q4_access_authorized": False,
        "new_data_purchase_authorized": False,
        "network_access_authorized": False,
        "broker_or_orders_authorized": False,
        "mission_database_write_authorized": False,
        "registry_write_authorized": False,
        "cemetery_write_authorized": False,
    }


def _terminal_result(
    *,
    manifest: Mapping[str, Any],
    scientific: Mapping[str, Any],
    scientific_path: Path,
    receipt: Mapping[str, Any],
    metrics: Mapping[str, Any],
    kpis: Mapping[str, Any],
    decision_report: Mapping[str, Any],
    replay_executed: bool,
    non_economic_audit: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if non_economic_audit is not None:
        return _non_economic_terminal_result(
            manifest=manifest,
            scientific=scientific,
            scientific_path=scientific_path,
            audit=non_economic_audit,
            metrics=metrics,
            kpis=kpis,
            decision_report=decision_report,
        )
    economic_results = {
        "schema": "hydra_cross_ecology_0036_economics_v1",
        "production_counters": {
            "serious_exact_account_replays": int(metrics["candidate_count"]),
            "predeclared_control_policy_replays": int(metrics["control_policy_count"]),
            "combine_episodes_completed": int(metrics["combine_episode_count"]),
            "normal_episodes_completed": int(metrics["normal_episode_count"]),
            "stressed_episodes_completed": int(metrics["stressed_episode_count"]),
        },
        "production_kpis": {
            field: kpis[field]
            for field in (
                "rates_per_hour",
                "economic_research_wall_clock_fraction",
                "cpu_utilization_fraction",
                "workers",
                "duplicate_rejection_rate",
                "cache_hit_rate",
            )
        },
        "economic_frontier": {
            "candidate_count": int(metrics["candidate_count"]),
            "positive_stressed_net_count": int(metrics["positive_stressed_count"]),
            "normal_pass_fraction_best": float(metrics["best_normal_pass_rate"]),
            "normal_pass_fraction_median": float(metrics["median_normal_pass_rate"]),
            "stressed_pass_fraction_best": float(metrics["best_stressed_pass_rate"]),
            "stressed_pass_fraction_median": float(metrics["median_stressed_pass_rate"]),
            "stressed_target_progress_median_best": float(
                metrics["best_stressed_target_progress"]
            ),
            "stressed_target_progress_median_population": float(
                metrics["median_stressed_target_progress"]
            ),
            "stressed_mll_breach_rate_minimum": float(
                metrics["minimum_stressed_mll_breach_rate"]
            ),
            "stressed_mll_breach_rate_maximum": float(
                metrics["maximum_stressed_mll_breach_rate"]
            ),
        },
        "normal_pass_candidate_count": int(metrics["normal_pass_candidate_count"]),
        "stressed_pass_candidate_count": int(metrics["stressed_pass_candidate_count"]),
        "positive_stressed_net_count": int(metrics["positive_stressed_count"]),
        "confirmation_ready_candidate_ids": [],
        "stage5_96_start_candidate_ids": [],
        "development_finalist_ids": [],
        "matched_controls_status": "COMPLETE_EXPOSURE_MATCHED_FOUR_CONTROL_FAMILIES",
        "null_status": "COMPLETE_RANDOM_PERMUTATION_DIRECTION_AND_OWN_PATH_CONTROLS",
        "development_only": True,
        "independently_confirmed": False,
    }
    economic_results["summary_hash"] = stable_hash(economic_results)
    next_action = _next_action(str(scientific["status"]), metrics)
    result = build_final_result_payload(
        manifest=manifest,
        kpis=kpis,
        economic_results=economic_results,
        successive_halving={
            "schema": "hydra_cross_ecology_0036_bounded_tripwire_v1",
            "stage_decisions": [
                {
                    "stage": "SIX_RULE_CROSS_ECOLOGY_TRIPWIRE",
                    "input_count": int(metrics["candidate_count"]),
                    "output_count": len(metrics["tier_e_passed_candidate_ids"]),
                    "selected_policy_ids": list(metrics["tier_e_passed_candidate_ids"]),
                }
            ],
            "thresholds_changed_after_results": False,
        },
        matched_controls={
            "schema": "hydra_cross_ecology_0036_matched_controls_v1",
            "control_ids": [
                "OWN_PATH_ONLY",
                "SESSION_MARKET_EXPOSURE_MATCHED_RANDOM",
                "ANALOG_LABEL_PERMUTATION",
                "DIRECTION_FLIP",
            ],
            "control_policy_count": int(metrics["control_policy_count"]),
            "same_opportunity_and_exposure_required": True,
            "controls_selected_after_outcomes": False,
        },
        failure_vectors=_failure_vectors(scientific),
        evidence_receipt=receipt,
        autonomous_next_action=next_action,
        scientific_status=str(scientific["status"]),
    )
    result.pop("result_hash", None)
    result.update(
        {
            "campaign_mode": CAMPAIGN_MODE,
            "campaign_ordinal": CAMPAIGN_ORDINAL,
            "runtime_version": RUNTIME_VERSION,
            "scientific_result": {
                "path": str(scientific_path),
                "file_sha256": _sha256(scientific_path),
                "result_hash": scientific["result_hash"],
                "source_mode": manifest["research_source"]["source_mode"],
                "economic_replay_executed_by_adapter": bool(replay_executed),
                "scientific_replay_previously_completed": bool(
                    replay_executed
                    or manifest["research_source"]["source_mode"]
                    == "GENERATE_READ_ONLY_ONCE"
                ),
            },
            "canonical_evidence_material_hash": scientific[
                "canonical_evidence_material"
            ]["canonical_material_hash"],
            "decision_report_hash": decision_report["decision_report_hash"],
            "evidence_tier_ceiling": "E",
            "tier_q_allowed": False,
            "promotion_allowed": False,
            "network_requests": 0,
            "mission_database_writes": 0,
            "registry_writes": 0,
            "cemetery_writes": 0,
        }
    )
    result["result_hash"] = stable_hash(result)
    return result


def _non_economic_terminal_result(
    *,
    manifest: Mapping[str, Any],
    scientific: Mapping[str, Any],
    scientific_path: Path,
    audit: Mapping[str, Any],
    metrics: Mapping[str, Any],
    kpis: Mapping[str, Any],
    decision_report: Mapping[str, Any],
) -> dict[str, Any]:
    economic_results = {
        "schema": "hydra_cross_ecology_0036_non_economic_terminal_v1",
        "terminal_mode": NON_ECONOMIC_TERMINAL_MODE,
        "canonical_evidence_status": NON_ECONOMIC_CANONICAL_STATUS,
        "production_counters": {
            "diagnostic_candidates_evaluated": int(metrics["diagnostic_candidate_count"]),
            "serious_exact_account_replays": 0,
            "predeclared_control_policy_replays": 0,
            "combine_episodes_completed": 0,
            "normal_episodes_completed": 0,
            "stressed_episodes_completed": 0,
        },
        "economic_frontier": {
            "candidate_count": 0,
            "positive_stressed_net_count": 0,
            "normal_pass_fraction_best": None,
            "stressed_pass_fraction_best": None,
            "stressed_target_progress_median_best": None,
            "stressed_mll_breach_rate_minimum": None,
        },
        "economic_evidence_materialized": False,
        "matched_controls_status": "NOT_MATERIALIZED_NO_ROUTED_EVENTS",
        "null_status": "NOT_MATERIALIZED_NO_ROUTED_EVENTS",
        "confirmation_ready_candidate_ids": [],
        "development_finalist_ids": [],
        "development_only": True,
        "independently_confirmed": False,
    }
    economic_results["summary_hash"] = stable_hash(economic_results)
    result = {
        "schema": RESULT_SCHEMA,
        "campaign_id": CAMPAIGN_ID,
        "manifest_hash": manifest["manifest_hash"],
        "source_commit": manifest["source_commit"],
        "status": "COMPLETE",
        "scientific_status": scientific["status"],
        "kpis": dict(kpis),
        "economic_results": economic_results,
        "successive_halving": {
            "schema": "hydra_cross_ecology_0036_non_economic_tripwire_v1",
            "stage_decisions": [
                {
                    "stage": "SIX_RULE_CROSS_ECOLOGY_TRIPWIRE",
                    "input_count": int(metrics["diagnostic_candidate_count"]),
                    "output_count": 0,
                    "selected_policy_ids": [],
                    "economic_materialization_required": True,
                }
            ],
            "thresholds_changed_after_results": False,
        },
        "matched_controls": {
            "schema": "hydra_cross_ecology_0036_non_economic_controls_v1",
            "status": "NOT_MATERIALIZED_NO_ROUTED_EVENTS",
            "control_policy_count": 0,
        },
        "failure_vectors": _failure_vectors(scientific),
        "evidence_bundle": None,
        "evidence_verification_manifest_sha256": None,
        "non_economic_audit": dict(audit),
        "autonomous_next_action": _next_action(str(scientific["status"]), metrics),
        "development_only": True,
        "independently_confirmed": False,
        "status_inheritance": False,
        "q4_access_delta": 0,
        "new_data_purchase_count": 0,
        "broker_connections": 0,
        "orders": 0,
        "campaign_mode": CAMPAIGN_MODE,
        "campaign_ordinal": CAMPAIGN_ORDINAL,
        "runtime_version": RUNTIME_VERSION,
        "scientific_result": {
            "path": str(scientific_path),
            "file_sha256": _sha256(scientific_path),
            "result_hash": scientific["result_hash"],
            "source_mode": manifest["research_source"]["source_mode"],
            "economic_replay_executed_by_adapter": False,
            "scientific_replay_previously_completed": True,
        },
        "canonical_evidence_material_hash": None,
        "decision_report_hash": decision_report["decision_report_hash"],
        "evidence_tier_ceiling": "E",
        "evidence_tier_awarded": None,
        "tier_q_allowed": False,
        "promotion_allowed": False,
        "network_requests": 0,
        "mission_database_writes": 0,
        "registry_writes": 0,
        "cemetery_writes": 0,
    }
    result["result_hash"] = stable_hash(result)
    return result


def _decision_report(
    manifest: Mapping[str, Any],
    scientific: Mapping[str, Any],
    metrics: Mapping[str, Any],
    scientific_path: Path,
) -> dict[str, Any]:
    core = {
        "schema": "hydra_cross_ecology_0036_decision_report_v1",
        "campaign_id": CAMPAIGN_ID,
        "campaign_ordinal": CAMPAIGN_ORDINAL,
        "manifest_hash": manifest["manifest_hash"],
        "source_commit": manifest["source_commit"],
        "scientific_status": scientific["status"],
        "scientific_result_path": str(scientific_path),
        "scientific_result_hash": scientific["result_hash"],
        "tier_ceiling": "E",
        "tier_q_allowed": False,
        "promotion_allowed": False,
        "metrics": dict(metrics),
        "autonomous_next_action": _next_action(str(scientific["status"]), metrics),
        "q4_access_count_delta": 0,
        "data_purchase_count": 0,
        "network_requests": 0,
        "broker_connections": 0,
        "orders": 0,
        "mission_database_writes": 0,
        "registry_writes": 0,
        "cemetery_writes": 0,
    }
    return {**core, "decision_report_hash": stable_hash(core)}


def _publish(
    output: Path,
    manifest: Mapping[str, Any],
    *,
    state: str,
    stage: str,
    next_action: str,
    sequence: int,
    metrics: Mapping[str, Any] | None,
    elapsed: float,
    cpu_seconds: float,
    replay_executed: bool = False,
) -> None:
    _write_state(
        output,
        manifest,
        state=state,
        stage=stage,
        next_action=next_action,
        sequence=sequence,
        metrics=metrics,
    )
    _atomic_json(
        output / "production_kpis.json",
        _kpis(
            manifest,
            state=state,
            sequence=sequence,
            metrics=metrics,
            elapsed=elapsed,
            cpu_seconds=cpu_seconds,
            replay_executed=replay_executed,
        ),
    )


def _write_state(
    output: Path,
    manifest: Mapping[str, Any],
    *,
    state: str,
    stage: str,
    next_action: str,
    sequence: int,
    metrics: Mapping[str, Any] | None,
) -> None:
    metrics = metrics or {}
    base = Path(str(manifest["evidence_bundle"]["destination"]))
    economic_evidence_materialized = bool(
        metrics.get("economic_evidence_materialized", True)
    )
    core = {
        "schema": STATE_SCHEMA,
        "campaign_id": CAMPAIGN_ID,
        "campaign_ordinal": CAMPAIGN_ORDINAL,
        "manifest_hash": manifest["manifest_hash"],
        "source_commit": manifest["source_commit"],
        "state": state,
        "stage": stage,
        "next_action": next_action,
        "checkpoint_sequence": int(sequence),
        "runner_pid": os.getpid(),
        "worker_count": 1,
        "evidence_writer_count": 1 if economic_evidence_materialized else 0,
        "policies_proposed": int(metrics.get("proposal_count", 0)),
        "unique_policies_screened": int(metrics.get("proposal_count", 0)),
        "exact_account_replays": int(metrics.get("candidate_count", 0)),
        "combine_episodes_completed": int(metrics.get("combine_episode_count", 0)),
        "last_completed_policy_id": (
            str(metrics.get("headline_by_candidate", [{}])[-1].get("candidate_id") or "")
            if metrics.get("headline_by_candidate")
            else ""
        ),
        "evidence_staging_path": (
            str(base / f".{CAMPAIGN_ID}.evidence-v1.staging")
            if economic_evidence_materialized
            else ""
        ),
        "evidence_final_path": (
            str(base / f"{CAMPAIGN_ID}.evidence-v1")
            if economic_evidence_materialized
            else ""
        ),
        "non_economic_audit_path": (
            str(
                _completed_adoption(manifest).get("audit_receipt_path")
                if _completed_adoption(manifest) is not None
                else ""
            )
            if not economic_evidence_materialized
            else ""
        ),
        "broker_connections": 0,
        "orders": 0,
        "q4_access_count_delta": 0,
        "data_purchase_count": 0,
        "network_requests": 0,
        "mission_database_writes": 0,
        "registry_writes": 0,
        "cemetery_writes": 0,
    }
    core["state_hash"] = stable_hash(core)
    _atomic_json(output / "production_state.json", core)


def _kpis(
    manifest: Mapping[str, Any],
    *,
    state: str,
    sequence: int,
    metrics: Mapping[str, Any] | None,
    elapsed: float,
    cpu_seconds: float,
    replay_executed: bool,
) -> dict[str, Any]:
    metrics = metrics or {}
    proposals = int(metrics.get("proposal_count", 0))
    candidates = int(metrics.get("candidate_count", 0))
    episodes = int(metrics.get("combine_episode_count", 0))
    non_economic = metrics.get("economic_evidence_materialized") is False
    hours = max(float(elapsed) / 3600.0, 1e-12)
    economic_fraction = 1.0 if replay_executed and candidates else 0.0
    cpu_fraction = min(max(float(cpu_seconds) / max(float(elapsed), 1e-9), 0.0), 1.0)
    core = {
        "schema": KPI_SCHEMA,
        "campaign_id": CAMPAIGN_ID,
        "campaign_ordinal": CAMPAIGN_ORDINAL,
        "manifest_hash": manifest["manifest_hash"],
        "source_commit": manifest["source_commit"],
        "checkpoint_sequence": int(sequence),
        "updated_at_utc": _utc_now(),
        "state": state,
        "rates_per_hour": {
            "policies_proposed": float(proposals / hours),
            "unique_policies_screened": float(proposals / hours),
            "exact_account_replays": float(candidates / hours),
            "combine_episodes": float(episodes / hours),
        },
        "workers": {"compute": 1, "evidence_writer": 0 if non_economic else 1},
        "policies_proposed": proposals,
        "unique_policies_screened": proposals,
        "exact_account_replays": candidates,
        "combine_episodes_completed": episodes,
        "normal_episodes_completed": int(metrics.get("normal_episode_count", 0)),
        "stressed_episodes_completed": int(metrics.get("stressed_episode_count", 0)),
        "positive_stressed_net_candidates": int(metrics.get("positive_stressed_count", 0)),
        "candidates_with_normal_pass": int(metrics.get("normal_pass_candidate_count", 0)),
        "candidates_with_stressed_pass": int(metrics.get("stressed_pass_candidate_count", 0)),
        "best_normal_pass_rate": _unit(metrics.get("best_normal_pass_rate", 0.0)),
        "best_stressed_pass_rate": _unit(metrics.get("best_stressed_pass_rate", 0.0)),
        "median_normal_pass_rate": _unit(metrics.get("median_normal_pass_rate", 0.0)),
        "median_stressed_pass_rate": _unit(metrics.get("median_stressed_pass_rate", 0.0)),
        "near_pass_count": int(metrics.get("near_pass_count", 0)),
        "candidates_promoted_96": 0,
        "candidates_surviving_96": 0,
        "confirmation_ready_candidates": 0,
        "duplicate_rejection_rate": 0.0,
        "cache_hit_rate": 1.0 if candidates else 0.0,
        "economic_research_wall_clock_fraction": economic_fraction,
        "cpu_utilization_fraction": cpu_fraction,
        "admin_overhead_alert": False,
        "matched_controls_status": (
            "NOT_MATERIALIZED_NO_ROUTED_EVENTS"
            if non_economic
            else
            "COMPLETE_EXPOSURE_MATCHED_FOUR_CONTROL_FAMILIES"
            if candidates
            else "PENDING_BOUNDED_TRIPWIRE_CONTROLS"
        ),
        "null_status": (
            "NOT_MATERIALIZED_NO_ROUTED_EVENTS"
            if non_economic
            else
            "COMPLETE_RANDOM_PERMUTATION_DIRECTION_AND_OWN_PATH_CONTROLS"
            if candidates
            else "PENDING_BOUNDED_TRIPWIRE_NULLS"
        ),
        "broker_connections": 0,
        "orders": 0,
        "q4_access_count_delta": 0,
        "data_purchase_count": 0,
        "network_requests": 0,
        "mission_database_writes": 0,
        "registry_writes": 0,
        "cemetery_writes": 0,
    }
    core["kpi_hash"] = stable_hash(core)
    return core


def _load_terminal_result(
    result_path: Path, manifest: Mapping[str, Any], output: Path
) -> dict[str, Any]:
    root = result_path.parents[3]
    _verify_multiplicity_reservation(root, manifest)
    from hydra.production.runtime import load_and_verify_production_result

    result = load_and_verify_production_result(result_path, manifest, deep_evidence=True)
    if (
        result.get("campaign_mode") != CAMPAIGN_MODE
        or int(result.get("campaign_ordinal", -1)) != CAMPAIGN_ORDINAL
        or result.get("runtime_version") != RUNTIME_VERSION
        or result.get("scientific_status") not in SCIENTIFIC_STATUSES
        or result.get("evidence_tier_ceiling") != "E"
        or result.get("tier_q_allowed") is not False
        or result.get("promotion_allowed") is not False
    ):
        raise CrossEcologyAnalogRuntimeError("0036 terminal production result drift")
    state = _read_snapshot(output / "production_state.json", "state_hash", manifest)
    kpis = _read_snapshot(output / "production_kpis.json", "kpi_hash", manifest)
    if state.get("state") != "COMPLETE" or kpis.get("state") != "COMPLETE":
        raise CrossEcologyAnalogRuntimeError("0036 terminal views are not COMPLETE")
    _validate_terminal_safety(result, state, kpis)
    scientific = result.get("scientific_result")
    if not isinstance(scientific, Mapping):
        raise CrossEcologyAnalogRuntimeError("0036 terminal result omits scientific source")
    source_path = Path(str(scientific.get("path") or ""))
    if not source_path.is_absolute():
        source_path = (root / source_path).resolve()
    allowed = (root / "reports/economic_evolution").resolve()
    if (
        not source_path.is_file()
        or source_path == allowed
        or allowed not in source_path.parents
        or source_path
        != _inside(root, manifest["research_source"]["result_path"])
        or _sha256(source_path) != scientific.get("file_sha256")
        or _read_json(source_path).get("result_hash") != scientific.get("result_hash")
    ):
        raise CrossEcologyAnalogRuntimeError("0036 terminal scientific source drift")
    _validate_terminal_scientific_replay(
        output,
        manifest,
        scientific,
        source_path,
    )
    source_result = _load_scientific_result(
        source_path,
        manifest,
        require_hash_binding=(
            manifest["research_source"]["source_mode"]
            == "PREEXISTING_HASH_BOUND"
        ),
    )
    canonical = _canonical_material(source_result, manifest)
    if canonical is None:
        if (
            result.get("canonical_evidence_material_hash") is not None
            or result.get("evidence_bundle") is not None
            or not isinstance(result.get("non_economic_audit"), Mapping)
        ):
            raise CrossEcologyAnalogRuntimeError(
                "0036 non-economic terminal publication drift"
            )
    elif (
        canonical.get("canonical_material_hash")
        != result.get("canonical_evidence_material_hash")
    ):
        raise CrossEcologyAnalogRuntimeError("0036 terminal canonical source drift")
    report = _read_hashed(output / "decision_report.json", "decision_report_hash")
    if report.get("decision_report_hash") != result.get("decision_report_hash"):
        raise CrossEcologyAnalogRuntimeError("0036 terminal decision report drift")
    _require_zero_fields(
        report,
        (
            "q4_access_count_delta",
            "data_purchase_count",
            "network_requests",
            "broker_connections",
            "orders",
            "mission_database_writes",
            "registry_writes",
            "cemetery_writes",
        ),
        "0036 terminal decision report",
    )
    if report.get("autonomous_next_action") != result.get("autonomous_next_action"):
        raise CrossEcologyAnalogRuntimeError("0036 terminal decision next-action drift")
    _validate_terminal_semantics(result, state, kpis, report, source_result)
    return result


def _read_snapshot(
    path: Path, hash_field: str, manifest: Mapping[str, Any]
) -> dict[str, Any]:
    value = _read_hashed(path, hash_field)
    expected_schema = STATE_SCHEMA if hash_field == "state_hash" else KPI_SCHEMA
    from hydra.production.autonomous_director_manifest import (
        resumable_snapshot_identity_matches,
    )

    root = path.resolve().parents[3]
    try:
        relative = path.resolve().relative_to(root).as_posix()
    except ValueError:
        relative = ""
    if (
        not resumable_snapshot_identity_matches(
            value,
            manifest,
            path=relative,
            schema=expected_schema,
            hash_field=hash_field,
        )
    ):
        raise CrossEcologyAnalogRuntimeError(f"0036 {path.name} identity drift")
    _require_zero_fields(
        value,
        (
            "q4_access_count_delta",
            "data_purchase_count",
            "network_requests",
            "broker_connections",
            "orders",
            "mission_database_writes",
            "registry_writes",
            "cemetery_writes",
        ),
        f"0036 {path.name}",
    )
    return value


def _validate_terminal_safety(
    result: Mapping[str, Any],
    state: Mapping[str, Any],
    kpis: Mapping[str, Any],
) -> None:
    """Re-prove every closed safety surface on terminal resume."""

    _require_zero_fields(
        result,
        (
            "q4_access_delta",
            "new_data_purchase_count",
            "network_requests",
            "broker_connections",
            "orders",
            "mission_database_writes",
            "registry_writes",
            "cemetery_writes",
        ),
        "0036 terminal result",
    )
    embedded_kpis = result.get("kpis")
    if not isinstance(embedded_kpis, Mapping):
        raise CrossEcologyAnalogRuntimeError("0036 terminal embedded KPIs are absent")
    for label, value in (
        ("terminal embedded KPIs", embedded_kpis),
        ("terminal KPI sidecar", kpis),
        ("terminal state sidecar", state),
    ):
        _require_zero_fields(
            value,
            (
                "q4_access_count_delta",
                "data_purchase_count",
                "network_requests",
                "broker_connections",
                "orders",
                "mission_database_writes",
                "registry_writes",
                "cemetery_writes",
            ),
            f"0036 {label}",
        )
    if embedded_kpis.get("kpi_hash") != kpis.get("kpi_hash"):
        raise CrossEcologyAnalogRuntimeError("0036 terminal KPI sidecar drift")
    next_action = result.get("autonomous_next_action")
    if not isinstance(next_action, Mapping):
        raise CrossEcologyAnalogRuntimeError("0036 terminal next action is absent")
    if (
        next_action.get("tier_q_allowed") is not False
        or next_action.get("q4_access_authorized") is not False
        or next_action.get("new_data_purchase_authorized") is not False
        or next_action.get("network_access_authorized") is not False
        or next_action.get("broker_or_orders_authorized") is not False
        or next_action.get("mission_database_write_authorized") is not False
        or next_action.get("registry_write_authorized") is not False
        or next_action.get("cemetery_write_authorized") is not False
        or state.get("next_action") != next_action.get("action")
        or result.get("development_only") is not True
        or result.get("independently_confirmed") is not False
        or result.get("status_inheritance") is not False
    ):
        raise CrossEcologyAnalogRuntimeError("0036 terminal next-action safety drift")


def _validate_terminal_semantics(
    result: Mapping[str, Any],
    state: Mapping[str, Any],
    kpis: Mapping[str, Any],
    report: Mapping[str, Any],
    scientific: Mapping[str, Any],
) -> None:
    """Recompute the frozen branch decision instead of trusting rehashed views."""

    metrics = report.get("metrics")
    branch_gate = scientific.get("branch_gate")
    if not isinstance(metrics, Mapping) or not isinstance(branch_gate, Mapping):
        raise CrossEcologyAnalogRuntimeError("0036 terminal decision metrics are absent")
    scientific_status = str(scientific.get("status") or "")
    passed_ids = branch_gate.get("passed_candidate_ids")
    if not isinstance(passed_ids, Sequence) or isinstance(passed_ids, (str, bytes)):
        raise CrossEcologyAnalogRuntimeError(
            "0036 terminal scientific branch gate is invalid"
        )
    frozen_passed_ids = [str(value) for value in passed_ids]
    try:
        expected_action = _next_action(
            scientific_status,
            {"tier_e_passed_candidate_ids": frozen_passed_ids},
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise CrossEcologyAnalogRuntimeError(
            "0036 terminal decision metrics are invalid"
        ) from exc
    state_sequence = state.get("checkpoint_sequence")
    kpi_sequence = kpis.get("checkpoint_sequence")
    embedded_kpis = result.get("kpis")
    embedded_sequence = (
        embedded_kpis.get("checkpoint_sequence")
        if isinstance(embedded_kpis, Mapping)
        else None
    )
    if (
        result.get("scientific_status") != scientific_status
        or report.get("scientific_status") != scientific_status
        or metrics.get("tier_e_passed_candidate_ids") != frozen_passed_ids
        or result.get("autonomous_next_action") != expected_action
        or report.get("autonomous_next_action") != expected_action
        or state.get("next_action") != expected_action["action"]
        or state.get("stage")
        != (
            "NON_ECONOMIC_BRANCH_DECISION_SEALED"
            if scientific.get("canonical_evidence_status")
            == NON_ECONOMIC_CANONICAL_STATUS
            else "TIER_E_BRANCH_DECISION_SEALED"
        )
        or type(state_sequence) is not int
        or type(kpi_sequence) is not int
        or type(embedded_sequence) is not int
        or state_sequence != kpi_sequence
        or state_sequence != embedded_sequence
    ):
        raise CrossEcologyAnalogRuntimeError("0036 terminal semantic reconciliation drift")


def _validate_terminal_scientific_replay(
    output: Path,
    manifest: Mapping[str, Any],
    scientific: Mapping[str, Any],
    source_path: Path,
) -> None:
    """Bind terminal source metadata to the frozen mode and one-run lease."""

    source_mode = manifest["research_source"]["source_mode"]
    generated = source_mode == "GENERATE_READ_ONLY_ONCE"
    adoption = _completed_adoption(manifest)
    executed_now = scientific.get("economic_replay_executed_by_adapter")
    previously_completed = scientific.get("scientific_replay_previously_completed")
    if (
        scientific.get("source_mode") != source_mode
        or not isinstance(executed_now, bool)
        or not isinstance(previously_completed, bool)
        or (adoption is not None and executed_now is not False)
        or (adoption is not None and previously_completed is not True)
        or (adoption is None and generated and previously_completed is not True)
        or (adoption is None and not generated and previously_completed is not False)
        or (not generated and executed_now is not False)
    ):
        raise CrossEcologyAnalogRuntimeError("0036 terminal scientific source-mode drift")
    lease_path = output / "scientific_replay_attempt.json"
    if not generated:
        if adoption is not None:
            _validate_adopted_replay_lease(output, manifest, source_path)
            return
        if lease_path.is_file():
            raise CrossEcologyAnalogRuntimeError(
                "0036 preexisting terminal source unexpectedly has a replay lease"
            )
        return
    if not lease_path.is_file():
        raise CrossEcologyAnalogRuntimeError(
            "0036 generated terminal source lacks its single-run lease"
        )
    lease = _read_hashed(lease_path, "attempt_hash")
    _validate_replay_lease(lease, manifest)
    if lease.get("status") != "COMPLETE":
        raise CrossEcologyAnalogRuntimeError(
            "0036 generated terminal source lease is not COMPLETE"
        )
    source = _read_json(source_path)
    actual_file_hash = _sha256(source_path)
    actual_result_hash = source.get("result_hash")
    if (
        lease.get("result_file_sha256") != actual_file_hash
        or lease.get("result_hash") != actual_result_hash
        or scientific.get("file_sha256") != actual_file_hash
        or scientific.get("result_hash") != actual_result_hash
    ):
        raise CrossEcologyAnalogRuntimeError(
            "0036 terminal scientific replay lease/source hash drift"
        )


def _require_zero_fields(
    value: Mapping[str, Any], fields: Sequence[str], label: str
) -> None:
    for field in fields:
        observed = value.get(field)
        if (
            not isinstance(observed, (int, float))
            or isinstance(observed, bool)
            or not math.isfinite(float(observed))
            or float(observed) != 0.0
        ):
            raise CrossEcologyAnalogRuntimeError(
                f"{label} safety invariant violated: {field}"
            )


def _require_exact_zero_counters(
    value: Mapping[str, Any], fields: Sequence[str], label: str
) -> None:
    for field in fields:
        observed = value.get(field)
        if type(observed) is not int or observed != 0:
            raise CrossEcologyAnalogRuntimeError(
                f"{label} must declare exact integer zero for {field}"
            )


def _assert_closed_governance_environment() -> None:
    for field in (
        "HYDRA_Q4_ACCESS_ALLOWED",
        "HYDRA_NEW_DATA_PURCHASE_ALLOWED",
        "HYDRA_NETWORK_ACCESS_ALLOWED",
        "HYDRA_BROKER_CONNECTION_ALLOWED",
        "HYDRA_ORDERS_ALLOWED",
    ):
        if os.environ.get(field, "0") != "0":
            raise CrossEcologyAnalogRuntimeError(f"unsafe 0036 environment: {field}")


def _validate_replay_lease(
    lease: Mapping[str, Any], manifest: Mapping[str, Any]
) -> None:
    """Validate the complete single-run lease before any resumed source use."""

    generation = lease.get("generation")
    maximum = lease.get("maximum_generations")
    runner_pid = lease.get("runner_pid")
    status = lease.get("status")
    adoption = _completed_adoption(manifest)
    expected_manifest_hash = (
        adoption["source_manifest_hash"]
        if adoption is not None
        else manifest.get("manifest_hash")
    )
    expected_source_commit = (
        adoption["source_commit"]
        if adoption is not None
        else manifest.get("source_commit")
    )
    if (
        lease.get("schema") != REPLAY_LEASE_SCHEMA
        or lease.get("campaign_id") != CAMPAIGN_ID
        or lease.get("manifest_hash") != expected_manifest_hash
        or lease.get("source_commit") != expected_source_commit
        or not isinstance(generation, int)
        or isinstance(generation, bool)
        or generation != 0
        or not isinstance(maximum, int)
        or isinstance(maximum, bool)
        or maximum != 1
        or status not in REPLAY_LEASE_STATUSES
        or lease.get("authorization") != ROOT_AUTHORIZATION
        or not isinstance(runner_pid, int)
        or isinstance(runner_pid, bool)
        or runner_pid <= 0
    ):
        raise CrossEcologyAnalogRuntimeError("0036 scientific replay lease drift")
    _require_zero_fields(
        lease,
        (
            "q4_access_count_delta",
            "data_purchase_count",
            "network_requests",
            "broker_connections",
            "orders",
            "mission_database_writes",
            "registry_writes",
            "cemetery_writes",
        ),
        "0036 scientific replay lease",
    )
    if status == "RUNNING":
        if lease.get("result_hash") not in (None, "") or lease.get(
            "result_file_sha256"
        ) not in (None, ""):
            raise CrossEcologyAnalogRuntimeError(
                "0036 running scientific replay lease declares a result"
            )
        return
    for field in ("result_hash", "result_file_sha256"):
        observed = str(lease.get(field) or "")
        if len(observed) != 64 or any(
            character not in "0123456789abcdef" for character in observed
        ):
            raise CrossEcologyAnalogRuntimeError(
                f"0036 completed scientific replay lease invalid: {field}"
            )


def _verify_multiplicity_reservation(
    root: Path, manifest: Mapping[str, Any]
) -> None:
    """Require the controller's prospective reservation before outcome access."""

    from hydra.governance.proof_registry import (
        MULTIPLICITY_EVENT,
        load_and_verify,
        multiplicity_trial_count,
    )

    proof = load_and_verify(root / "mission/state/proof_registry.json")
    event_id = f"{CAMPAIGN_ID}_multiplicity_reservation"
    matches = [
        row
        for row in proof.get("entries", [])
        if row.get("event_type") == MULTIPLICITY_EVENT
        and row.get("event_id") == event_id
    ]
    multiplicity = manifest["multiplicity"]
    if len(matches) != 1:
        raise CrossEcologyAnalogRuntimeError(
            "0036 economic outcome access requires one prior multiplicity reservation"
        )
    entry = matches[0]
    evidence = entry.get("evidence")
    reservation = entry.get("multiplicity")
    adoption = _completed_adoption(manifest)
    expected_preregistration_hash = (
        adoption["multiplicity_preregistration_hash"]
        if adoption is not None
        else manifest.get("manifest_hash")
    )
    if (
        not isinstance(evidence, Mapping)
        or not isinstance(reservation, Mapping)
        or evidence.get("campaign_id") != CAMPAIGN_ID
        or evidence.get("class_id") != CLASS_ID
        or evidence.get("preregistration_hash") != expected_preregistration_hash
        or reservation.get("previous_N_trials")
        != multiplicity.get("prior_global_N_trials")
        or reservation.get("delta_trials") != multiplicity.get("reserved_delta_trials")
        or reservation.get("cumulative_N_trials")
        != multiplicity.get("expected_global_N_trials_after_reservation")
        or multiplicity_trial_count(proof)
        < int(multiplicity["expected_global_N_trials_after_reservation"])
    ):
        raise CrossEcologyAnalogRuntimeError("0036 multiplicity reservation drift")
    receipt_path = _inside(root, multiplicity["reservation_receipt_path"])
    receipt = _read_json(receipt_path)
    if (
        _sha256(receipt_path) != multiplicity["reservation_receipt_sha256"]
        or receipt.get("schema") != "hydra_manifest_campaign_multiplicity_v1"
        or receipt.get("campaign_id") != CAMPAIGN_ID
        or receipt.get("event_id") != event_id
        or receipt.get("previous_N_trials")
        != multiplicity["prior_global_N_trials"]
        or receipt.get("reserved_delta_trials")
        != multiplicity["reserved_delta_trials"]
        or receipt.get("cumulative_N_trials")
        != multiplicity["expected_global_N_trials_after_reservation"]
        or receipt.get("q4_access_delta") != 0
        or receipt.get("new_data_purchase_count") != 0
        or receipt.get("orders") != 0
    ):
        raise CrossEcologyAnalogRuntimeError("0036 multiplicity receipt drift")


def _set_single_thread_libraries() -> None:
    for field in (
        "OMP_NUM_THREADS",
        "MKL_NUM_THREADS",
        "OPENBLAS_NUM_THREADS",
        "NUMEXPR_NUM_THREADS",
    ):
        os.environ[field] = "1"


def _next_sequence(output: Path, manifest: Mapping[str, Any]) -> int:
    checkpoint = _read_checkpoint_pair(output, manifest)
    if checkpoint is None:
        return 1
    state, _kpis = checkpoint
    return max(int(state["checkpoint_sequence"]) + 1, 1)


def _read_checkpoint_pair(
    output: Path, manifest: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]] | None:
    state_path = output / "production_state.json"
    kpi_path = output / "production_kpis.json"
    if not state_path.is_file() and not kpi_path.is_file():
        return None
    if state_path.is_file() != kpi_path.is_file():
        raise CrossEcologyAnalogRuntimeError(
            "0036 prior checkpoint pair is incomplete"
        )
    try:
        state = _read_snapshot(state_path, "state_hash", manifest)
        kpis = _read_snapshot(kpi_path, "kpi_hash", manifest)
        state_sequence = state.get("checkpoint_sequence")
        kpi_sequence = kpis.get("checkpoint_sequence")
        if (
            type(state_sequence) is not int
            or type(kpi_sequence) is not int
            or state_sequence != kpi_sequence
        ):
            raise CrossEcologyAnalogRuntimeError(
                "0036 prior checkpoint pair sequence drift"
            )
        return state, kpis
    except Exception as exc:
        raise CrossEcologyAnalogRuntimeError("0036 prior checkpoint is corrupt") from exc


def _inside(root: Path, value: Any) -> Path:
    if not isinstance(value, str) or not value or Path(value).is_absolute():
        raise CrossEcologyAnalogRuntimeError("0036 path is unsafe")
    target = (root / value).resolve()
    try:
        target.relative_to(root)
    except ValueError as exc:
        raise CrossEcologyAnalogRuntimeError("0036 path escapes root") from exc
    return target


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CrossEcologyAnalogRuntimeError(f"invalid JSON: {path}") from exc
    if not isinstance(value, dict):
        raise CrossEcologyAnalogRuntimeError(f"JSON object required: {path}")
    return value


def _read_hashed(path: Path, hash_field: str) -> dict[str, Any]:
    value = _read_json(path)
    claimed = str(value.get(hash_field) or "")
    core = dict(value)
    core.pop(hash_field, None)
    if not claimed or stable_hash(core) != claimed:
        raise CrossEcologyAnalogRuntimeError(f"0036 hash drift: {path.name}")
    return value


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    raw = (
        json.dumps(value, indent=2, sort_keys=True, allow_nan=False, default=str)
        + "\n"
    ).encode("utf-8")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with temporary.open("wb") as handle:
        handle.write(raw)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
    descriptor = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _unit(value: Any) -> float:
    number = float(value)
    if not math.isfinite(number) or not 0.0 <= number <= 1.0:
        raise CrossEcologyAnalogRuntimeError("0036 KPI fraction is invalid")
    return number


__all__ = [
    "CrossEcologyAnalogRuntimeError",
    "read_cross_ecology_analog_status",
    "run_cross_ecology_analog_manifest",
]
