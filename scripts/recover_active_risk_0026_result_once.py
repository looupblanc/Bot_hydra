#!/usr/bin/env python3
"""One-time sealed-result recovery for active-risk campaign 0026.

The economic run can seal a valid multi-horizon EvidenceBundle and then fail
while comparing its canonical episode counter with the larger number of
persisted horizon rows.  This campaign-specific utility repairs only that
seal-to-result transaction.  It never replays a policy, reads episode outcomes,
changes the frozen manifest, or writes into the EvidenceBundle.

Execution is deliberately fail-closed.  It is allowed only after the original
runner has exited with the exact known error and the bundle is sealed.  The
initial invocation consumes one attempt before publishing the immutable result;
later invocations may only resume that exact proof-bound terminalization.
"""

from __future__ import annotations

import argparse
import gc
import hashlib
import json
import os
import re
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from hydra.economic_evolution.schema import stable_hash


CAMPAIGN_ID = "hydra_active_risk_pool_target_velocity_0026"
MANIFEST_HASH = "db52ec45979beed05e3fa4128c4c54fc252d87c899dfd0f434647eab351b2005"
SOURCE_COMMIT = "988999af3d31017e70c6b487a4cef18849bdbd11"
EXPECTED_OUTPUT_DIR = (
    "reports/economic_evolution/active_risk_pool_target_velocity_0026_revision_02"
)
EXPECTED_RESULT_NAME = "economic_production_result.json"
PREFLIGHT_RESULT_NAME = ".sealed_result_recovery_preflight.json"
PREFLIGHT_PROOF_NAME = ".sealed_result_recovery_preflight_proof.json"
ATTEMPT_NAME = ".sealed_result_recovery_attempt.json"
RECOVERY_RECEIPT_NAME = "sealed_result_recovery_receipt.json"
RECOVERY_VERSION = "hydra_active_risk_0026_sealed_result_recovery_v1"
RECOVERY_SCHEMA = "hydra_active_risk_0026_sealed_result_recovery_receipt_v1"
EXPECTED_FAILED_STATE = "FAILED_CLOSED"
EXPECTED_FAILED_STAGE = "EVIDENCE_BUNDLE_ATOMIC_FINALIZE"
EXPECTED_ERROR_TYPE = "ActiveRiskRuntimeError"
EXPECTED_ERROR = "active-risk counters diverge from persisted multi-horizon episodes"
EXPECTED_NEXT_ACTION = "REQUIRE_SPECIFIC_ACTIVE_RISK_RUNTIME_REPAIR"
EXPECTED_HORIZONS: tuple[int | str, ...] = (20, 40, 60, 90, "FULL")
EXPECTED_CANONICAL_EPISODES = 35_328
EXPECTED_NORMAL_EPISODES = 17_664
EXPECTED_STRESSED_EPISODES = 17_664
EXPECTED_PERSISTED_EPISODE_ROWS = 152_064
DEFAULT_CONTROLLER_POLL_TIMEOUT_SECONDS = 900.0
CONTROLLER_POLL_INTERVAL_SECONDS = 2.0
EXPECTED_GROUPS: Mapping[str, Mapping[str, int | str]] = {
    "active:stage2-eliminated": {
        "partition_count": 256,
        "persisted_rows": 6_144,
        "horizon_multiplicity": 1,
        "policy_count": 768,
        "incremental_start_count": 4,
        "scenario_count": 2,
        "canonical_role": "ELIMINATED_EXACT_POLICIES_FINAL_BOUNDARY",
    },
    "active:stage3": {
        "partition_count": 256,
        "persisted_rows": 122_880,
        "horizon_multiplicity": 5,
        "policy_count": 256,
        "incremental_start_count": 48,
        "scenario_count": 2,
        "canonical_role": "BASE_48_STARTS",
    },
    "active:stage4": {
        "partition_count": 32,
        "persisted_rows": 15_360,
        "horizon_multiplicity": 5,
        "policy_count": 32,
        "incremental_start_count": 48,
        "scenario_count": 2,
        "canonical_role": "INCREMENT_FROM_48_TO_96_STARTS",
    },
    "active:stage5": {
        "partition_count": 8,
        "persisted_rows": 7_680,
        "horizon_multiplicity": 5,
        "policy_count": 8,
        "incremental_start_count": 96,
        "scenario_count": 2,
        "canonical_role": "INCREMENT_FROM_96_TO_192_STARTS",
    },
}
_BATCH_PATTERN = re.compile(
    r"^(active:(?:stage2-eliminated|stage3|stage4|stage5)):(\d{6}):episodes$"
)


class RecoveryError(RuntimeError):
    """The sealed result cannot be recovered without weakening a guard."""


class ControllerHandoffRequired(RecoveryError):
    """The result is published, but snapshot reconciliation is not coherent."""

    def __init__(self, payload: Mapping[str, Any]) -> None:
        self.payload = dict(payload)
        super().__init__(
            "immutable result published; bounded snapshot reconciliation pending"
        )


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RecoveryError(f"invalid JSON prerequisite: {path}") from exc
    if not isinstance(value, dict):
        raise RecoveryError(f"JSON prerequisite is not an object: {path}")
    return value


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as exc:
        raise RecoveryError(f"cannot hash prerequisite: {path}") from exc
    return digest.hexdigest()


def _recovery_implementation_sha256() -> str:
    """Fingerprint the exact one-time recovery implementation in use."""

    return _sha256(Path(__file__).resolve())


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise RecoveryError(message)


def _verify_snapshot_hash(value: Mapping[str, Any], field: str) -> None:
    claimed = str(value.get(field) or "")
    payload = dict(value)
    payload.pop(field, None)
    _require(bool(claimed) and stable_hash(payload) == claimed, f"{field} drift")


def _pid_is_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def validate_episode_partition_accounting(
    *,
    frozen_horizons: Sequence[int | str],
    evidence_manifest: Mapping[str, Any],
    evidence_receipt: Mapping[str, Any],
    campaign_summary: Mapping[str, Any],
) -> dict[str, Any]:
    """Reconcile canonical work from sealed partition metadata only.

    Stage 2 eliminated rows were materialized once.  Stage 3--5 rows were
    materialized once for every frozen reporting horizon.  No episode payload or
    economic outcome is opened by this function.
    """

    _require(
        tuple(frozen_horizons) == EXPECTED_HORIZONS,
        "frozen horizon policy differs from the exact recovery contract",
    )
    files = evidence_manifest.get("files")
    _require(isinstance(files, Mapping), "sealed file manifest is malformed")

    grouped: dict[str, dict[str, Any]] = {
        prefix: {"partition_count": 0, "persisted_rows": 0, "batch_ids": set()}
        for prefix in EXPECTED_GROUPS
    }
    for relative_path, raw in files.items():
        if not isinstance(raw, Mapping):
            raise RecoveryError("sealed file entry is malformed")
        if raw.get("kind") != "dataset_partition" or raw.get("dataset") != "episodes":
            continue
        _require(
            isinstance(relative_path, str)
            and relative_path.startswith("datasets/episodes/part-")
            and relative_path.endswith(".jsonl.gz"),
            "episode partition path is outside the frozen layout",
        )
        batch_id = str(raw.get("batch_id") or "")
        match = _BATCH_PATTERN.fullmatch(batch_id)
        _require(match is not None, f"unknown episode batch: {batch_id}")
        assert match is not None
        prefix = match.group(1)
        row_count = int(raw.get("row_count", -1))
        _require(row_count > 0, f"non-positive episode rows in {batch_id}")
        _require(
            batch_id not in grouped[prefix]["batch_ids"],
            f"duplicate episode batch: {batch_id}",
        )
        grouped[prefix]["batch_ids"].add(batch_id)
        grouped[prefix]["partition_count"] += 1
        grouped[prefix]["persisted_rows"] += row_count

    canonical_total = 0
    persisted_total = 0
    public_groups: dict[str, dict[str, int | str]] = {}
    for prefix, expected in EXPECTED_GROUPS.items():
        observed = grouped[prefix]
        partitions = int(observed["partition_count"])
        rows = int(observed["persisted_rows"])
        multiplicity = int(expected["horizon_multiplicity"])
        policy_count = int(expected["policy_count"])
        incremental_starts = int(expected["incremental_start_count"])
        scenario_count = int(expected["scenario_count"])
        expected_canonical_rows = policy_count * incremental_starts * scenario_count
        expected_persisted_rows = expected_canonical_rows * multiplicity
        _require(
            partitions == int(expected["partition_count"]),
            f"{prefix} partition count drift",
        )
        _require(rows == int(expected["persisted_rows"]), f"{prefix} row count drift")
        _require(
            rows == expected_persisted_rows,
            f"{prefix} policy/start/scenario/horizon product drift",
        )
        _require(rows % multiplicity == 0, f"{prefix} horizon divisibility drift")
        canonical_rows = rows // multiplicity
        _require(
            canonical_rows == expected_canonical_rows,
            f"{prefix} canonical incremental work drift",
        )
        persisted_total += rows
        canonical_total += canonical_rows
        public_groups[prefix] = {
            "partition_count": partitions,
            "persisted_rows": rows,
            "horizon_multiplicity": multiplicity,
            "policy_count": policy_count,
            "incremental_start_count": incremental_starts,
            "scenario_count": scenario_count,
            "canonical_role": str(expected["canonical_role"]),
            "canonical_episode_computations": canonical_rows,
        }

    manifest_counts = evidence_manifest.get("dataset_row_counts")
    receipt_counts = evidence_receipt.get("dataset_row_counts")
    _require(
        isinstance(manifest_counts, Mapping) and isinstance(receipt_counts, Mapping),
        "episode dataset counters are absent",
    )
    manifest_persisted = int(manifest_counts.get("episodes", -1))
    receipt_persisted = int(receipt_counts.get("episodes", -1))
    _require(
        persisted_total
        == manifest_persisted
        == receipt_persisted
        == EXPECTED_PERSISTED_EPISODE_ROWS,
        "persisted episode rows disagree with sealed partition metadata",
    )

    counters = campaign_summary.get("production_counters")
    _require(isinstance(counters, Mapping), "campaign summary lacks production counters")
    canonical_claimed = int(counters.get("combine_episodes_completed", -1))
    normal = int(counters.get("normal_episodes_completed", -1))
    stressed = int(counters.get("stressed_episodes_completed", -1))
    _require(
        canonical_total
        == canonical_claimed
        == normal + stressed
        == EXPECTED_CANONICAL_EPISODES,
        "canonical episode computation count drift",
    )
    _require(normal == EXPECTED_NORMAL_EPISODES, "normal episode counter drift")
    _require(stressed == EXPECTED_STRESSED_EPISODES, "stressed episode counter drift")

    return {
        "accounting_semantics": (
            "STAGE2_ROWS_ONCE_PLUS_STAGE3_TO_STAGE5_ROWS_DIVIDED_BY_"
            "FROZEN_HORIZON_MULTIPLICITY"
        ),
        "outcome_rows_read": 0,
        "alternative_lifecycle_paths_added_to_combine_count": False,
        "stage4_and_stage5_are_disjoint_incremental_start_sets": True,
        "groups": public_groups,
        "persisted_multi_horizon_episode_rows": persisted_total,
        "canonical_episode_computations": canonical_total,
        "normal_episode_computations": normal,
        "stressed_episode_computations": stressed,
    }


def normalized_terminal_kpis(
    prior_kpis: Mapping[str, Any],
    *,
    state: Mapping[str, Any],
    campaign_summary: Mapping[str, Any],
    finalized_at_utc: str,
) -> dict[str, Any]:
    """Build deterministic terminal KPIs from pre-seal compact metrics."""

    _verify_snapshot_hash(prior_kpis, "kpi_hash")
    counters = campaign_summary.get("production_counters")
    summary_kpis = campaign_summary.get("production_kpis")
    _require(isinstance(counters, Mapping), "sealed summary counters are malformed")
    _require(isinstance(summary_kpis, Mapping), "sealed summary KPIs are malformed")
    assert isinstance(counters, Mapping) and isinstance(summary_kpis, Mapping)

    value = dict(prior_kpis)
    value.update(
        {
            "campaign_id": CAMPAIGN_ID,
            "manifest_hash": MANIFEST_HASH,
            "source_commit": SOURCE_COMMIT,
            "state": "COMPLETE",
            "checkpoint_sequence": int(state.get("checkpoint_sequence", 0)) + 1,
            "updated_at_utc": finalized_at_utc,
            "policies_proposed": int(campaign_summary["governor_proposals_generated"]),
            "unique_policies_screened": int(campaign_summary["unique_policies_screened"]),
            "exact_account_replays": int(campaign_summary["exact_account_replays"]),
            "combine_episodes_completed": int(counters["combine_episodes_completed"]),
            "normal_episodes_completed": int(counters["normal_episodes_completed"]),
            "stressed_episodes_completed": int(counters["stressed_episodes_completed"]),
            "candidates_promoted_96": int(campaign_summary["policies_promoted_to_96"]),
            "candidates_surviving_96": int(campaign_summary["policies_surviving_96"]),
            "candidates_promoted_192": int(campaign_summary["policies_surviving_96"]),
            "confirmation_ready_candidates": len(
                campaign_summary.get("confirmation_ready_candidate_ids") or ()
            ),
            "positive_stressed_net_candidates": int(
                campaign_summary["positive_stressed_net_count"]
            ),
            "candidates_with_normal_pass": int(
                campaign_summary["normal_pass_candidate_count"]
            ),
            "candidates_with_stressed_pass": int(
                campaign_summary["stressed_pass_candidate_count"]
            ),
            "best_normal_pass_rate": float(campaign_summary["best_normal_pass_rate"]),
            "best_stressed_pass_rate": float(campaign_summary["best_stressed_pass_rate"]),
            "median_normal_pass_rate": float(
                campaign_summary["median_normal_pass_rate"]
            ),
            "median_stressed_pass_rate": float(
                campaign_summary["median_stressed_pass_rate"]
            ),
            "matched_controls_status": str(campaign_summary["matched_controls_status"]),
            "rates_per_hour": dict(summary_kpis["rates_per_hour"]),
            "economic_research_wall_clock_fraction": float(
                summary_kpis["economic_research_wall_clock_fraction"]
            ),
            "cpu_utilization_fraction": float(
                summary_kpis["cpu_utilization_fraction"]
            ),
            "workers": dict(summary_kpis["workers"]),
            "duplicate_rejection_rate": float(
                summary_kpis["duplicate_rejection_rate"]
            ),
            "cache_hit_rate": float(summary_kpis["cache_hit_rate"]),
        }
    )
    value.pop("kpi_hash", None)
    value["kpi_hash"] = stable_hash(value)
    return value


def _verify_frozen_implementation(root: Path, manifest: Mapping[str, Any]) -> None:
    files = manifest.get("implementation_files")
    _require(isinstance(files, Mapping) and bool(files), "implementation freeze is absent")
    assert isinstance(files, Mapping)
    for relative, expected in files.items():
        path = root / str(relative)
        _require(path.is_file() and not path.is_symlink(), f"missing implementation: {relative}")
        _require(_sha256(path) == str(expected), f"implementation checksum drift: {relative}")


def _validate_manifest(root: Path, manifest_path: Path) -> dict[str, Any]:
    from hydra.production.manifest import load_and_validate_production_manifest

    manifest = load_and_validate_production_manifest(manifest_path)
    _require(manifest.get("campaign_id") == CAMPAIGN_ID, "campaign identity drift")
    _require(manifest.get("campaign_mode") == "ACTIVE_RISK_POOL", "campaign mode drift")
    _require(manifest.get("manifest_hash") == MANIFEST_HASH, "manifest hash drift")
    _require(manifest.get("source_commit") == SOURCE_COMMIT, "source commit drift")
    runtime = manifest.get("runtime")
    evidence = manifest.get("evidence_bundle")
    halving = manifest.get("successive_halving")
    _require(isinstance(runtime, Mapping), "runtime manifest section is malformed")
    _require(isinstance(evidence, Mapping), "evidence manifest section is malformed")
    _require(isinstance(halving, Mapping), "halving manifest section is malformed")
    assert isinstance(runtime, Mapping)
    assert isinstance(evidence, Mapping)
    assert isinstance(halving, Mapping)
    _require(runtime.get("output_dir") == EXPECTED_OUTPUT_DIR, "output directory drift")
    _require(runtime.get("result_name") == EXPECTED_RESULT_NAME, "result name drift")
    _require(tuple(halving.get("frozen_horizons") or ()) == EXPECTED_HORIZONS, "horizon drift")
    _require(evidence.get("atomic_finalize") is True, "atomic evidence finalize disabled")
    _require(
        evidence.get("reconstruction_flag") is False,
        "campaign evidence unexpectedly marked as reconstruction",
    )
    _verify_frozen_implementation(root, manifest)
    return manifest


def _validate_failed_state(state: Mapping[str, Any], prior_kpis: Mapping[str, Any]) -> None:
    _verify_snapshot_hash(state, "state_hash")
    _verify_snapshot_hash(prior_kpis, "kpi_hash")
    _require(state.get("campaign_id") == CAMPAIGN_ID, "failed state campaign drift")
    _require(state.get("manifest_hash") == MANIFEST_HASH, "failed state manifest drift")
    _require(state.get("source_commit") == SOURCE_COMMIT, "failed state source drift")
    _require(state.get("state") == EXPECTED_FAILED_STATE, "state is not exact FAILED_CLOSED")
    _require(state.get("stage") == EXPECTED_FAILED_STAGE, "failure stage drift")
    _require(state.get("error_type") == EXPECTED_ERROR_TYPE, "failure type drift")
    _require(state.get("error") == EXPECTED_ERROR, "failure message drift")
    _require(state.get("next_action") == EXPECTED_NEXT_ACTION, "failure next action drift")
    _require(prior_kpis.get("campaign_id") == CAMPAIGN_ID, "KPI campaign drift")
    _require(prior_kpis.get("manifest_hash") == MANIFEST_HASH, "KPI manifest drift")
    _require(prior_kpis.get("source_commit") == SOURCE_COMMIT, "KPI source drift")
    _require(prior_kpis.get("state") == EXPECTED_FAILED_STATE, "KPI state is not FAILED_CLOSED")
    _require(
        int(state.get("combine_episodes_completed", -1)) == EXPECTED_CANONICAL_EPISODES,
        "failed-state episode count drift",
    )
    _require(
        int(prior_kpis.get("combine_episodes_completed", -1))
        == EXPECTED_CANONICAL_EPISODES,
        "failed KPI episode count drift",
    )
    for field in (
        "broker_connections",
        "orders",
        "q4_access_count_delta",
        "data_purchase_count",
    ):
        _require(int(state.get(field, -1)) == 0, f"unsafe failed-state {field}")
        _require(int(prior_kpis.get(field, -1)) == 0, f"unsafe failed-KPI {field}")
    runner_pid = int(state.get("runner_pid", -1))
    _require(not _pid_is_alive(runner_pid), "original production runner is still alive")


def _validate_receipt(
    *,
    final_bundle: Path,
    evidence_manifest: Mapping[str, Any],
    receipt: Mapping[str, Any],
) -> None:
    manifest_path = final_bundle / "evidence_bundle_manifest.json"
    _require(
        evidence_manifest.get("campaign_id") == CAMPAIGN_ID,
        "sealed EvidenceBundle campaign drift",
    )
    _require(evidence_manifest.get("status") == "COMPLETE", "sealed bundle is not COMPLETE")
    _require(
        evidence_manifest.get("evidence_status") == "FRESH_DEVELOPMENT_EVIDENCE",
        "sealed bundle evidence status drift",
    )
    _require(
        evidence_manifest.get("reconstruction_flag") is False,
        "sealed bundle reconstruction flag drift",
    )
    _require(receipt.get("campaign_id") == CAMPAIGN_ID, "receipt campaign drift")
    _require(
        Path(str(receipt.get("bundle_path") or "")).resolve() == final_bundle,
        "receipt bundle path drift",
    )
    _require(
        Path(str(receipt.get("manifest_path") or "")).resolve() == manifest_path,
        "receipt manifest path drift",
    )
    _require(
        receipt.get("manifest_sha256") == _sha256(manifest_path),
        "receipt manifest hash drift",
    )
    _require(
        receipt.get("bundle_content_sha256")
        == evidence_manifest.get("bundle_content_sha256"),
        "receipt bundle content hash drift",
    )
    _require(
        receipt.get("evidence_status") == "FRESH_DEVELOPMENT_EVIDENCE",
        "evidence status drift",
    )
    _require(receipt.get("reconstruction_flag") is False, "reconstruction flag drift")
    _require(
        receipt.get("dataset_row_counts") == evidence_manifest.get("dataset_row_counts"),
        "receipt dataset counters drift",
    )


def _load_result_after_two_preregistered_guards(
    path: Path,
    manifest: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate the recovery result without running a third deep bundle guard.

    The exact FAILED_CLOSED marker admitted by :func:`recover_once` is emitted
    only after both preregistered deep guards complete and the subsequent
    canonical-versus-multi-horizon counter assertion fails.  Repeating the
    generic production-result loader here would run two additional deep scans.
    Bind the result to that completed-guard evidence using the sealed manifest,
    receipt and immutable hashes instead.
    """

    result = _read_json(path)
    claimed = str(result.get("result_hash") or "")
    payload = dict(result)
    payload.pop("result_hash", None)
    _require(bool(claimed) and stable_hash(payload) == claimed, "result hash drift")

    from hydra.production.manifest import PRODUCTION_RESULT_SCHEMA

    _require(
        result.get("schema") == PRODUCTION_RESULT_SCHEMA
        and result.get("campaign_id") == manifest.get("campaign_id")
        and result.get("manifest_hash") == manifest.get("manifest_hash")
        and result.get("source_commit") == manifest.get("source_commit")
        and result.get("status") == "COMPLETE",
        "production result identity/status drift",
    )
    receipt = result.get("evidence_bundle")
    _require(
        isinstance(receipt, Mapping) and bool(receipt.get("bundle_path")),
        "production result lacks sealed EvidenceBundle receipt",
    )
    assert isinstance(receipt, Mapping)
    final_bundle = Path(str(receipt["bundle_path"])).resolve()

    from hydra.evidence import verify_evidence_bundle

    evidence_manifest = verify_evidence_bundle(final_bundle, deep=False)
    _validate_receipt(
        final_bundle=final_bundle,
        evidence_manifest=evidence_manifest,
        receipt=receipt,
    )
    _require(
        result.get("evidence_verification_manifest_sha256")
        == receipt.get("manifest_sha256"),
        "result EvidenceBundle manifest linkage drift",
    )
    return result


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _preflight_validate_and_publish_result(
    *,
    output_dir: Path,
    result_path: Path,
    result: Mapping[str, Any],
    manifest: Mapping[str, Any],
    writer: Any,
    result_loader: Callable[[Path, Mapping[str, Any]], Mapping[str, Any]],
    proof_payload: Mapping[str, Any],
    revalidate_guards: Callable[[], None],
    consume_attempt: Callable[[Mapping[str, Any]], None],
    reconcile_snapshots: Callable[[Mapping[str, Any]], None],
) -> tuple[dict[str, Any], str, dict[str, Any]]:
    """Check against completed guards, publish once, then reconcile snapshots."""

    preflight_path = output_dir / PREFLIGHT_RESULT_NAME
    proof_path = output_dir / PREFLIGHT_PROOF_NAME
    _require(not preflight_path.exists(), "stale recovery preflight result exists")
    _require(not proof_path.exists(), "stale recovery preflight proof exists")
    _require(not result_path.exists(), "production result already exists")
    preflight_receipt = writer.write_json(preflight_path.name, dict(result))
    checked_raw = result_loader(preflight_path, manifest)
    _require(isinstance(checked_raw, Mapping), "result preflight returned no object")
    checked = dict(checked_raw)
    _require(checked == dict(result), "result preflight changed the payload")

    proof = {
        **dict(proof_payload),
        "schema": "hydra_active_risk_0026_result_preflight_proof_v1",
        "campaign_id": CAMPAIGN_ID,
        "manifest_hash": MANIFEST_HASH,
        "source_commit": SOURCE_COMMIT,
        "recovery_version": RECOVERY_VERSION,
        "result_hash": str(checked["result_hash"]),
        "result_sha256": preflight_receipt.sha256,
        "recovery_implementation_sha256": _recovery_implementation_sha256(),
        "validator": "load_result_after_two_preregistered_guards",
        "preregistered_deep_guard_count": 2,
        "additional_deep_guard_performed": False,
        "validated_at_utc": _utc_now(),
    }
    proof["proof_hash"] = stable_hash(proof)
    writer.write_json(proof_path.name, proof)

    # These callbacks close the sealed-proof race window and consume the
    # one-time attempt only after the recovery validator has succeeded.
    revalidate_guards()
    persisted_proof = _read_json(proof_path)
    _require(persisted_proof == proof, "preflight proof changed before publish")
    _verify_preflight_proof(persisted_proof)
    consume_attempt(proof)
    _require(not result_path.exists(), "production result appeared before publication")
    os.replace(preflight_path, result_path)
    _fsync_directory(output_dir)
    published_sha256 = _sha256(result_path)
    _require(
        published_sha256 == preflight_receipt.sha256,
        "official result bytes differ from the guard-bound preflight",
    )
    _require(_read_json(result_path) == checked, "published result payload drift")
    reconcile_snapshots(checked)
    return checked, published_sha256, proof


def _verified_controller_handoffs(
    runtime_state: Mapping[str, Any],
) -> list[dict[str, Any]]:
    _require(
        runtime_state.get("schema") == "hydra_manifest_campaign_runtime_v1",
        "persistent controller runtime schema drift",
    )
    raw = runtime_state.get("production_successor_handoffs") or []
    _require(isinstance(raw, list), "persistent controller handoff chain is malformed")
    verified: list[dict[str, Any]] = []
    previous_hash: str | None = None
    for sequence, item in enumerate(raw, start=1):
        _require(isinstance(item, Mapping), "persistent controller handoff is malformed")
        assert isinstance(item, Mapping)
        entry = dict(item)
        claimed = str(entry.pop("handoff_hash", ""))
        _require(
            entry.get("schema") == "hydra_production_successor_handoff_v1"
            and int(entry.get("sequence", -1)) == sequence
            and entry.get("previous_handoff_hash") == previous_hash
            and bool(claimed)
            and stable_hash(entry) == claimed
            and isinstance(entry.get("recommendation"), Mapping),
            "persistent controller handoff hash-chain drift",
        )
        entry["handoff_hash"] = claimed
        verified.append(entry)
        previous_hash = claimed
    return verified


def _observe_controller_completion(
    *,
    output_dir: Path,
    controller_runtime_state_path: Path,
    result: Mapping[str, Any],
    original_failed_state_hash: str,
    original_failed_kpi_hash: str,
) -> dict[str, Any] | None:
    """Read only the two authoritative terminalization sources."""

    state = _read_json(output_dir / "production_state.json")
    kpis = _read_json(output_dir / "production_kpis.json")
    _verify_snapshot_hash(state, "state_hash")
    _verify_snapshot_hash(kpis, "kpi_hash")
    state_status = state.get("state")
    kpi_status = kpis.get("state")
    _require(
        state_status in {EXPECTED_FAILED_STATE, "COMPLETE"},
        "unsupported production state during snapshot reconciliation",
    )
    _require(
        kpi_status in {EXPECTED_FAILED_STATE, "COMPLETE"},
        "unsupported KPI state during snapshot reconciliation",
    )

    if state_status == EXPECTED_FAILED_STATE:
        _require(
            state.get("state_hash") == original_failed_state_hash,
            "FAILED_CLOSED production state changed after result publication",
        )
    else:
        _require(state.get("campaign_id") == CAMPAIGN_ID, "COMPLETE state campaign drift")
        _require(state.get("manifest_hash") == MANIFEST_HASH, "COMPLETE state manifest drift")
        _require(state.get("source_commit") == SOURCE_COMMIT, "COMPLETE state source drift")
        _require(
            int(state.get("combine_episodes_completed", -1))
            == EXPECTED_CANONICAL_EPISODES,
            "COMPLETE state episode counter drift",
        )

    if kpi_status == EXPECTED_FAILED_STATE:
        _require(
            kpis.get("kpi_hash") == original_failed_kpi_hash,
            "FAILED_CLOSED KPI snapshot changed after result publication",
        )
    else:
        _require(kpis.get("campaign_id") == CAMPAIGN_ID, "COMPLETE KPI campaign drift")
        _require(kpis.get("manifest_hash") == MANIFEST_HASH, "COMPLETE KPI manifest drift")
        _require(kpis.get("source_commit") == SOURCE_COMMIT, "COMPLETE KPI source drift")
        _require(
            int(kpis.get("combine_episodes_completed", -1))
            == EXPECTED_CANONICAL_EPISODES,
            "COMPLETE KPI episode counter drift",
        )

    # The bounded reconciler publishes the snapshots with separate atomic
    # renames.  Either COMPLETE/FAILED_CLOSED ordering is a transient window;
    # accept only the same COMPLETE checkpoint generation on both sides.
    if state_status == "COMPLETE" and kpi_status == "COMPLETE":
        state_sequence = int(state.get("checkpoint_sequence", -1))
        kpi_sequence = int(kpis.get("checkpoint_sequence", -2))
        _require(
            state_sequence >= 0 and state_sequence == kpi_sequence,
            "COMPLETE snapshot checkpoint generation drift",
        )
        return {
            "terminal_source": "PRODUCTION_SNAPSHOTS_COMPLETE",
            "source_truth_complete": True,
            "checkpoint_sequence": state_sequence,
            "state_hash": state["state_hash"],
            "kpi_hash": kpis["kpi_hash"],
        }

    if not controller_runtime_state_path.is_file():
        return None
    runtime_state = _read_json(controller_runtime_state_path)
    matching: list[dict[str, Any]] = []
    for handoff in _verified_controller_handoffs(runtime_state):
        if handoff.get("campaign_id") != CAMPAIGN_ID:
            continue
        _require(
            handoff.get("manifest_hash") == MANIFEST_HASH,
            "controller handoff manifest drift for campaign 0026",
        )
        _require(
            handoff.get("result_hash") == result.get("result_hash"),
            "controller handoff result drift for campaign 0026",
        )
        _require(
            handoff.get("recommendation") == result.get("autonomous_next_action"),
            "controller handoff recommendation drift for campaign 0026",
        )
        handoff_identity = {
            "campaign_id": CAMPAIGN_ID,
            "manifest_hash": MANIFEST_HASH,
            "result_hash": result.get("result_hash"),
            "recommendation": result.get("autonomous_next_action"),
        }
        _require(
            handoff.get("handoff_id") == stable_hash(handoff_identity),
            "controller handoff identity drift for campaign 0026",
        )
        recommendation = result.get("autonomous_next_action")
        assert isinstance(recommendation, Mapping)
        expected_handoff_state = (
            "WORM_MANIFEST_REQUIRED"
            if recommendation.get("manifest_required") is True
            else "NO_SUCCESSOR_MANIFEST_REQUIRED"
        )
        _require(
            handoff.get("handoff_state") == expected_handoff_state,
            "controller handoff terminal state drift for campaign 0026",
        )
        matching.append(handoff)
    _require(len(matching) <= 1, "multiple controller handoffs for campaign 0026")
    if not matching:
        return None
    handoff = matching[0]
    return {
        "terminal_source": "PERSISTENT_CONTROLLER_SUCCESSOR_HANDOFF",
        "source_truth_complete": False,
        "controller_runtime_state": str(runtime_state.get("state") or "UNKNOWN"),
        "handoff_id": handoff["handoff_id"],
        "handoff_hash": handoff["handoff_hash"],
        "handoff_state": handoff["handoff_state"],
        "state_hash": state["state_hash"],
        "kpi_hash": kpis["kpi_hash"],
    }


def _poll_for_controller_completion(
    *,
    output_dir: Path,
    controller_runtime_state_path: Path,
    result: Mapping[str, Any],
    original_failed_state_hash: str,
    original_failed_kpi_hash: str,
    timeout_seconds: float,
    snapshot_reconciliation_attempted: bool = False,
    interval_seconds: float = CONTROLLER_POLL_INTERVAL_SECONDS,
    monotonic: Callable[[], float] = time.monotonic,
    sleeper: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    """Wait boundedly for a coherent result-to-snapshot terminal generation."""

    _require(timeout_seconds >= 0.0, "negative controller poll timeout")
    _require(interval_seconds > 0.0, "non-positive controller poll interval")
    deadline = monotonic() + timeout_seconds
    terminal_handoff: dict[str, Any] | None = None
    while True:
        observed = _observe_controller_completion(
            output_dir=output_dir,
            controller_runtime_state_path=controller_runtime_state_path,
            result=result,
            original_failed_state_hash=original_failed_state_hash,
            original_failed_kpi_hash=original_failed_kpi_hash,
        )
        if observed is not None and observed.get("source_truth_complete") is True:
            return observed
        if observed is not None:
            terminal_handoff = observed
        remaining = deadline - monotonic()
        if remaining <= 0.0:
            payload: dict[str, Any] = {
                "schema": "hydra_active_risk_0026_controller_handoff_required_v1",
                "campaign_id": CAMPAIGN_ID,
                "result_path": str(output_dir / EXPECTED_RESULT_NAME),
                "result_hash": str(result.get("result_hash") or ""),
                "snapshot_reconciliation_attempted": (
                    snapshot_reconciliation_attempted
                ),
                "local_state_or_kpi_write_performed": (
                    snapshot_reconciliation_attempted
                ),
            }
            if terminal_handoff is None:
                payload.update(
                    state="RESULT_PUBLISHED_SNAPSHOTS_NOT_COMPLETE",
                    required_action=(
                        "RELAUNCH_BOUNDED_RECOVERY_TO_RESUME_SNAPSHOT_RECONCILIATION"
                    ),
                )
            else:
                payload.update(
                    state="CONTROLLER_ACCEPTED_RESULT_SNAPSHOTS_NOT_COMPLETE",
                    required_action=(
                        "RELAUNCH_BOUNDED_RECOVERY_TO_RESUME_SNAPSHOT_RECONCILIATION"
                    ),
                    controller_terminal_observation=terminal_handoff,
                )
            raise ControllerHandoffRequired(payload)
        sleeper(min(interval_seconds, remaining))


def _write_attempt_marker(
    *,
    attempt_path: Path,
    output_dir: Path,
    proof: Mapping[str, Any],
    original_state_hash: str,
    original_kpi_hash: str,
    sealed_bundle_content_sha256: str,
) -> dict[str, Any]:
    _require(
        original_state_hash == proof.get("original_failed_state_hash")
        and original_kpi_hash == proof.get("original_failed_kpi_hash"),
        "attempt snapshots differ from the sealed preflight proof",
    )
    attempt = {
        "schema": "hydra_active_risk_0026_sealed_result_recovery_attempt_v1",
        "campaign_id": CAMPAIGN_ID,
        "recovery_version": RECOVERY_VERSION,
        "started_at_utc": _utc_now(),
        "original_state_hash": original_state_hash,
        "original_kpi_hash": original_kpi_hash,
        "sealed_bundle_content_sha256": sealed_bundle_content_sha256,
        "preflight_proof_hash": proof["proof_hash"],
        "result_hash": proof["result_hash"],
        "result_sha256": proof["result_sha256"],
        "recovery_implementation_sha256": proof[
            "recovery_implementation_sha256"
        ],
    }
    attempt["attempt_hash"] = stable_hash(attempt)
    descriptor = os.open(
        attempt_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600
    )
    try:
        payload = (json.dumps(attempt, indent=2, sort_keys=True) + "\n").encode(
            "utf-8"
        )
        offset = 0
        while offset < len(payload):
            written = os.write(descriptor, payload[offset:])
            if written <= 0:
                raise RecoveryError("could not persist the one-time recovery attempt")
            offset += written
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    _fsync_directory(output_dir)
    return attempt


def _verify_preflight_proof(proof: Mapping[str, Any]) -> None:
    claimed = str(proof.get("proof_hash") or "")
    payload = dict(proof)
    payload.pop("proof_hash", None)
    _require(bool(claimed) and stable_hash(payload) == claimed, "preflight proof drift")
    _require(
        proof.get("schema") == "hydra_active_risk_0026_result_preflight_proof_v1"
        and proof.get("campaign_id") == CAMPAIGN_ID
        and proof.get("manifest_hash") == MANIFEST_HASH
        and proof.get("source_commit") == SOURCE_COMMIT
        and proof.get("recovery_version") == RECOVERY_VERSION
        and proof.get("validator")
        == "load_result_after_two_preregistered_guards"
        and proof.get("preregistered_deep_guard_count") == 2
        and proof.get("additional_deep_guard_performed") is False,
        "preflight proof identity drift",
    )
    for field in (
        "result_hash",
        "result_sha256",
        "sealed_bundle_content_sha256",
        "sealed_bundle_manifest_sha256",
        "recovery_implementation_sha256",
        "original_failed_state_hash",
        "original_failed_kpi_hash",
    ):
        _require(
            bool(re.fullmatch(r"[0-9a-f]{64}", str(proof.get(field) or ""))),
            f"preflight proof {field} is malformed",
        )
    _require(
        proof.get("recovery_implementation_sha256")
        == _recovery_implementation_sha256(),
        "recovery implementation differs from preflight proof",
    )


def _verify_attempt_marker(
    attempt: Mapping[str, Any], proof: Mapping[str, Any]
) -> None:
    claimed = str(attempt.get("attempt_hash") or "")
    payload = dict(attempt)
    payload.pop("attempt_hash", None)
    _require(bool(claimed) and stable_hash(payload) == claimed, "attempt marker drift")
    _require(
        attempt.get("schema")
        == "hydra_active_risk_0026_sealed_result_recovery_attempt_v1"
        and attempt.get("campaign_id") == CAMPAIGN_ID
        and attempt.get("recovery_version") == RECOVERY_VERSION
        and attempt.get("preflight_proof_hash") == proof.get("proof_hash")
        and attempt.get("result_hash") == proof.get("result_hash")
        and attempt.get("result_sha256") == proof.get("result_sha256")
        and attempt.get("recovery_implementation_sha256")
        == proof.get("recovery_implementation_sha256")
        and attempt.get("original_state_hash")
        == proof.get("original_failed_state_hash")
        and attempt.get("original_kpi_hash")
        == proof.get("original_failed_kpi_hash")
        and attempt.get("sealed_bundle_content_sha256")
        == proof.get("sealed_bundle_content_sha256"),
        "attempt marker disagrees with preflight proof",
    )
    _require(
        bool(re.fullmatch(r"[0-9a-f]{64}", str(attempt.get("original_state_hash") or "")))
        and bool(re.fullmatch(r"[0-9a-f]{64}", str(attempt.get("original_kpi_hash") or ""))),
        "attempt marker snapshot hashes are malformed",
    )


def _verify_result_from_preflight_proof(
    path: Path,
    result: Mapping[str, Any],
    proof: Mapping[str, Any],
) -> None:
    claimed = str(result.get("result_hash") or "")
    payload = dict(result)
    payload.pop("result_hash", None)
    _require(bool(claimed) and stable_hash(payload) == claimed, "result hash drift")
    _require(_sha256(path) == proof.get("result_sha256"), "result byte hash drift")
    _require(
        result.get("campaign_id") == CAMPAIGN_ID
        and result.get("manifest_hash") == MANIFEST_HASH
        and result.get("source_commit") == SOURCE_COMMIT
        and result.get("status") == "COMPLETE"
        and claimed == proof.get("result_hash"),
        "result identity differs from preflight proof",
    )
    evidence = result.get("evidence_bundle")
    _require(isinstance(evidence, Mapping), "result evidence receipt is malformed")
    assert isinstance(evidence, Mapping)
    _require(
        evidence.get("bundle_content_sha256")
        == proof.get("sealed_bundle_content_sha256")
        and evidence.get("manifest_sha256")
        == proof.get("sealed_bundle_manifest_sha256"),
        "result evidence differs from preflight proof",
    )


def _verify_sealed_evidence_against_proof(
    *,
    root: Path,
    manifest: Mapping[str, Any],
    proof: Mapping[str, Any],
) -> None:
    """Cheaply rebind a resumed publication to the still-sealed bundle."""

    evidence = manifest.get("evidence_bundle")
    _require(isinstance(evidence, Mapping), "resume manifest lacks evidence contract")
    assert isinstance(evidence, Mapping)
    evidence_base = (root / str(evidence.get("destination") or "")).resolve()
    final_bundle = evidence_base / f"{CAMPAIGN_ID}.evidence-v1"
    staging_bundle = evidence_base / f".{CAMPAIGN_ID}.evidence-v1.staging"
    receipt_path = (root / str(evidence.get("lightweight_manifest_path") or "")).resolve()
    _require(
        final_bundle.is_dir() and not final_bundle.is_symlink(),
        "resume sealed EvidenceBundle is absent",
    )
    _require(not staging_bundle.exists(), "resume evidence staging reappeared")
    _require(receipt_path.is_file(), "resume lightweight evidence receipt is absent")

    from hydra.evidence import verify_evidence_bundle

    evidence_manifest = verify_evidence_bundle(final_bundle, deep=False)
    receipt = _read_json(receipt_path)
    _validate_receipt(
        final_bundle=final_bundle,
        evidence_manifest=evidence_manifest,
        receipt=receipt,
    )
    _require(
        evidence_manifest.get("bundle_content_sha256")
        == proof.get("sealed_bundle_content_sha256"),
        "resume sealed bundle content differs from preflight proof",
    )
    _require(
        receipt.get("manifest_sha256")
        == proof.get("sealed_bundle_manifest_sha256"),
        "resume sealed bundle manifest differs from preflight proof",
    )


def _resume_terminalization_only(
    *,
    manifest_path: Path,
    manifest: Mapping[str, Any],
    root: Path,
    output_dir: Path,
    timeout_seconds: float,
) -> dict[str, Any]:
    """Resume only publication/reconciliation from a prior deep proof."""

    result_path = output_dir / EXPECTED_RESULT_NAME
    preflight_path = output_dir / PREFLIGHT_RESULT_NAME
    proof_path = output_dir / PREFLIGHT_PROOF_NAME
    attempt_path = output_dir / ATTEMPT_NAME
    receipt_path = output_dir / RECOVERY_RECEIPT_NAME
    controller_path = root / "mission/state/economic_evolution_manifest_runtime.json"

    _require(proof_path.is_file(), "post-publication resume lacks preflight proof")
    proof = _read_json(proof_path)
    _verify_preflight_proof(proof)
    _verify_sealed_evidence_against_proof(root=root, manifest=manifest, proof=proof)

    if receipt_path.is_file():
        receipt = _read_json(receipt_path)
        claimed = str(receipt.get("recovery_receipt_hash") or "")
        payload = dict(receipt)
        payload.pop("recovery_receipt_hash", None)
        _require(
            bool(claimed)
            and stable_hash(payload) == claimed
            and receipt.get("campaign_id") == CAMPAIGN_ID
            and result_path.is_file()
            and not preflight_path.exists()
            and attempt_path.is_file()
            and _sha256(result_path) == receipt.get("result_sha256")
            and receipt.get("preflight_proof_hash") == proof.get("proof_hash")
            and receipt.get("recovery_implementation_sha256")
            == proof.get("recovery_implementation_sha256")
            and receipt.get("original_failed_state_hash")
            == proof.get("original_failed_state_hash")
            and receipt.get("original_failed_kpi_hash")
            == proof.get("original_failed_kpi_hash"),
            "existing recovery receipt drift",
        )
        result = _read_json(result_path)
        _verify_result_from_preflight_proof(result_path, result, proof)
        attempt = _read_json(attempt_path)
        _verify_attempt_marker(attempt, proof)
        completion = receipt.get("controller_completion")
        _require(
            isinstance(completion, Mapping)
            and completion.get("source_truth_complete") is True,
            "existing recovery receipt lacks coherent COMPLETE snapshots",
        )
        _require(
            receipt.get("authoritative_snapshot_writer")
            == "BOUNDED_ONE_TIME_RECOVERY_RECONCILER"
            and receipt.get("local_state_or_kpi_write_performed") is True,
            "existing recovery receipt snapshot provenance drift",
        )
        observed = _observe_controller_completion(
            output_dir=output_dir,
            controller_runtime_state_path=controller_path,
            result={
                "result_hash": result["result_hash"],
                "autonomous_next_action": result["autonomous_next_action"],
            },
            original_failed_state_hash=str(attempt["original_state_hash"]),
            original_failed_kpi_hash=str(attempt["original_kpi_hash"]),
        )
        _require(
            isinstance(observed, Mapping)
            and observed.get("source_truth_complete") is True,
            "existing receipt no longer has coherent COMPLETE snapshots",
        )
        assert isinstance(observed, Mapping)
        _require(
            observed.get("state_hash") == receipt.get("state_hash_after")
            == receipt.get("state_hash")
            and observed.get("kpi_hash") == receipt.get("kpi_hash_after")
            == receipt.get("kpi_hash"),
            "live COMPLETE snapshots differ from the recovery receipt",
        )
        return receipt

    _require(
        result_path.is_file() != preflight_path.is_file(),
        "resume requires exactly one preflight or official result",
    )
    candidate_path = result_path if result_path.is_file() else preflight_path
    result = _read_json(candidate_path)
    _verify_result_from_preflight_proof(candidate_path, result, proof)

    if attempt_path.is_file():
        attempt = _read_json(attempt_path)
        _verify_attempt_marker(attempt, proof)
    else:
        _require(
            preflight_path.is_file() and not result_path.exists(),
            "official result without one-time attempt marker",
        )
        state = _read_json(output_dir / "production_state.json")
        kpis = _read_json(output_dir / "production_kpis.json")
        _validate_failed_state(state, kpis)
        attempt = _write_attempt_marker(
            attempt_path=attempt_path,
            output_dir=output_dir,
            proof=proof,
            original_state_hash=state["state_hash"],
            original_kpi_hash=kpis["kpi_hash"],
            sealed_bundle_content_sha256=str(
                proof["sealed_bundle_content_sha256"]
            ),
        )

    if preflight_path.is_file():
        os.replace(preflight_path, result_path)
        _fsync_directory(output_dir)
    result = _read_json(result_path)
    _verify_result_from_preflight_proof(result_path, result, proof)

    before_state = _read_json(output_dir / "production_state.json")
    before_kpis = _read_json(output_dir / "production_kpis.json")
    _verify_snapshot_hash(before_state, "state_hash")
    _verify_snapshot_hash(before_kpis, "kpi_hash")
    _require(
        before_state.get("state") in {EXPECTED_FAILED_STATE, "COMPLETE"}
        and before_kpis.get("state") in {EXPECTED_FAILED_STATE, "COMPLETE"},
        "resume encountered an unsupported snapshot state",
    )
    if before_state.get("state") == EXPECTED_FAILED_STATE:
        _require(
            before_state.get("state_hash") == attempt.get("original_state_hash"),
            "FAILED_CLOSED state changed since the recovery attempt",
        )
    if before_kpis.get("state") == EXPECTED_FAILED_STATE:
        _require(
            before_kpis.get("kpi_hash") == attempt.get("original_kpi_hash"),
            "FAILED_CLOSED KPI changed since the recovery attempt",
        )

    from hydra.production.active_risk_runtime import ActiveRiskPoolRun

    run = ActiveRiskPoolRun(
        manifest_path=manifest_path,
        contract_map_path=root,
        cache_root=root,
        stop_after=None,
    )
    run._reconcile_completed_result_snapshots(result)
    completion = _poll_for_controller_completion(
        output_dir=output_dir,
        controller_runtime_state_path=controller_path,
        result={
            "result_hash": result["result_hash"],
            "autonomous_next_action": result["autonomous_next_action"],
        },
        original_failed_state_hash=str(attempt["original_state_hash"]),
        original_failed_kpi_hash=str(attempt["original_kpi_hash"]),
        timeout_seconds=timeout_seconds,
        snapshot_reconciliation_attempted=True,
    )
    _require(
        completion.get("source_truth_complete") is True,
        "snapshot reconciliation did not reach coherent COMPLETE",
    )
    after_state = _read_json(output_dir / "production_state.json")
    after_kpis = _read_json(output_dir / "production_kpis.json")
    _verify_snapshot_hash(after_state, "state_hash")
    _verify_snapshot_hash(after_kpis, "kpi_hash")
    embedded_recovery = result.get("sealed_result_recovery")
    _require(isinstance(embedded_recovery, Mapping), "result recovery audit is absent")
    assert isinstance(embedded_recovery, Mapping)
    recovery_receipt = {
        "schema": RECOVERY_SCHEMA,
        "campaign_id": CAMPAIGN_ID,
        "recovery_version": RECOVERY_VERSION,
        "completed_at_utc": _utc_now(),
        "result_hash": result["result_hash"],
        "result_sha256": proof["result_sha256"],
        "production_state_observed": after_state["state"],
        "state_hash": after_state["state_hash"],
        "kpi_hash": after_kpis["kpi_hash"],
        "controller_completion": completion,
        "authoritative_snapshot_writer": "BOUNDED_ONE_TIME_RECOVERY_RECONCILER",
        "local_state_or_kpi_write_performed": True,
        "local_write_claim_scope": "BOUNDED_RECOVERY_LIFECYCLE",
        "snapshot_reconciliation_method_invoked_this_invocation": True,
        "snapshot_hash_changed_this_invocation": (
            before_state["state_hash"] != after_state["state_hash"]
            or before_kpis["kpi_hash"] != after_kpis["kpi_hash"]
        ),
        "snapshot_reconciliation_call_count_this_invocation": 1,
        "resume_without_deep_validation": True,
        "deep_validation_call_count_this_invocation": 0,
        "preflight_deep_validation_count": 1,
        "state_hash_before": before_state["state_hash"],
        "kpi_hash_before": before_kpis["kpi_hash"],
        "state_hash_after": after_state["state_hash"],
        "kpi_hash_after": after_kpis["kpi_hash"],
        "snapshot_files": {
            "production_state": str(output_dir / "production_state.json"),
            "production_kpis": str(output_dir / "production_kpis.json"),
        },
        "preflight_proof_hash": proof["proof_hash"],
        "recovery_implementation_sha256": proof[
            "recovery_implementation_sha256"
        ],
        "original_failed_state_hash": proof["original_failed_state_hash"],
        "original_failed_kpi_hash": proof["original_failed_kpi_hash"],
        "sealed_bundle_content_sha256": proof["sealed_bundle_content_sha256"],
        "sealed_bundle_manifest_sha256": proof["sealed_bundle_manifest_sha256"],
        "economic_outcomes_recomputed": False,
        "manifest_mutated": False,
        "evidence_bundle_mutated": False,
        "episode_accounting": embedded_recovery["episode_accounting"],
    }
    recovery_receipt["recovery_receipt_hash"] = stable_hash(recovery_receipt)
    from hydra.compute.result_writer import AtomicResultWriter

    AtomicResultWriter(output_dir).write_json(RECOVERY_RECEIPT_NAME, recovery_receipt)
    return recovery_receipt


def recover_once(
    manifest_path: Path,
    *,
    controller_poll_timeout_seconds: float = DEFAULT_CONTROLLER_POLL_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """Execute the single bounded seal-to-result recovery."""

    manifest_path = manifest_path.resolve()
    root = manifest_path.parents[2]
    manifest = _validate_manifest(root, manifest_path)
    output_dir = (root / EXPECTED_OUTPUT_DIR).resolve()
    evidence_base = (root / str(manifest["evidence_bundle"]["destination"])).resolve()
    final_bundle = evidence_base / f"{CAMPAIGN_ID}.evidence-v1"
    staging_bundle = evidence_base / f".{CAMPAIGN_ID}.evidence-v1.staging"
    result_path = output_dir / EXPECTED_RESULT_NAME
    recovery_receipt_path = output_dir / RECOVERY_RECEIPT_NAME
    preflight_path = output_dir / PREFLIGHT_RESULT_NAME
    proof_path = output_dir / PREFLIGHT_PROOF_NAME
    attempt_path = output_dir / ATTEMPT_NAME
    controller_runtime_state_path = (
        root / "mission/state/economic_evolution_manifest_runtime.json"
    )
    evidence_receipt_path = root / str(
        manifest["evidence_bundle"]["lightweight_manifest_path"]
    )

    recovery_artifacts = (
        result_path,
        recovery_receipt_path,
        preflight_path,
        proof_path,
        attempt_path,
    )
    if any(path.exists() for path in recovery_artifacts):
        return _resume_terminalization_only(
            manifest_path=manifest_path,
            manifest=manifest,
            root=root,
            output_dir=output_dir,
            timeout_seconds=controller_poll_timeout_seconds,
        )

    _require(output_dir.is_dir(), "campaign output directory is absent")
    _require(not result_path.exists(), "production result already exists")
    _require(not recovery_receipt_path.exists(), "recovery receipt already exists")
    _require(not preflight_path.exists(), "recovery preflight already exists")
    _require(not proof_path.exists(), "recovery preflight proof already exists")
    _require(not attempt_path.exists(), "bounded recovery attempt was already consumed")
    _require(not staging_bundle.exists(), "evidence staging still exists")
    _require(final_bundle.is_dir() and not final_bundle.is_symlink(), "sealed bundle is absent")
    _require(evidence_receipt_path.is_file(), "lightweight evidence receipt is absent")

    state = _read_json(output_dir / "production_state.json")
    prior_kpis = _read_json(output_dir / "production_kpis.json")
    _validate_failed_state(state, prior_kpis)

    identity = _read_json(final_bundle / "identity.json")
    _require(identity.get("campaign_id") == CAMPAIGN_ID, "bundle identity campaign drift")
    _require(identity.get("source_commit") == SOURCE_COMMIT, "bundle identity source drift")
    _require(
        identity.get("configuration_sha256") == _sha256(manifest_path),
        "bundle identity configuration drift",
    )

    # Structural metadata is checked now.  The original runner has already
    # completed the two preregistered deep guards; recovery must not run a
    # third full guard.
    from hydra.evidence import verify_evidence_bundle

    evidence_manifest = verify_evidence_bundle(final_bundle, deep=False)
    receipt = _read_json(evidence_receipt_path)
    _validate_receipt(
        final_bundle=final_bundle,
        evidence_manifest=evidence_manifest,
        receipt=receipt,
    )
    outputs = {
        name: _read_json(final_bundle / "outputs" / f"{name}.json")
        for name in (
            "campaign_summary",
            "failure_vectors",
            "pareto_archive",
            "next_campaign_recommendations",
        )
    }
    summary = outputs["campaign_summary"]
    _require(
        summary.get("schema") == "hydra_active_risk_campaign_summary_v1",
        "summary schema drift",
    )
    _require(summary.get("campaign_id") == CAMPAIGN_ID, "summary campaign drift")
    _require(summary.get("governor_proposals_generated") == 20_000, "proposal count drift")
    _require(summary.get("unique_policies_screened") == 4_096, "screen count drift")
    _require(summary.get("exact_account_replays") == 1_024, "exact replay count drift")
    _require(summary.get("stage3_policy_count") == 256, "Stage-3 count drift")
    _require(summary.get("policies_promoted_to_96") == 32, "96-start promotion drift")
    _require(summary.get("policies_surviving_96") == 8, "192-start survivor drift")
    accounting = validate_episode_partition_accounting(
        frozen_horizons=manifest["successive_halving"]["frozen_horizons"],
        evidence_manifest=evidence_manifest,
        evidence_receipt=receipt,
        campaign_summary=summary,
    )

    terminal_kpis = normalized_terminal_kpis(
        prior_kpis,
        state=state,
        campaign_summary=summary,
        finalized_at_utc=str(receipt["finalized_at_utc"]),
    )
    pareto = outputs["pareto_archive"]
    stage_decisions = pareto.get("stage_decisions")
    controls = summary.get("matched_controls")
    recommendation = outputs["next_campaign_recommendations"].get("recommendation")
    _require(isinstance(stage_decisions, list), "sealed halving decisions are malformed")
    _require(isinstance(controls, Mapping), "sealed matched controls are malformed")
    _require(isinstance(recommendation, Mapping), "sealed next action is malformed")

    from hydra.production.active_risk_runtime import _sealed_active_scientific_status
    from hydra.production.halving import build_final_result_payload

    result = build_final_result_payload(
        manifest=manifest,
        kpis=terminal_kpis,
        economic_results=summary,
        successive_halving={"stage_decisions": list(stage_decisions)},
        matched_controls=dict(controls),
        failure_vectors=outputs["failure_vectors"],
        evidence_receipt=receipt,
        autonomous_next_action=dict(recommendation),
        scientific_status=_sealed_active_scientific_status(summary),
    )
    result.pop("result_hash", None)
    result["sealed_result_recovery"] = {
        "schema": RECOVERY_SCHEMA,
        "recovery_version": RECOVERY_VERSION,
        "failure_stage": EXPECTED_FAILED_STAGE,
        "failure_type": EXPECTED_ERROR_TYPE,
        "failure_message": EXPECTED_ERROR,
        "economic_outcomes_recomputed": False,
        "episode_outcomes_read_for_accounting": False,
        "sealed_economic_outcomes_reused_verbatim": True,
        "manifest_mutated": False,
        "evidence_bundle_mutated": False,
        "thresholds_mutated": False,
        "episode_accounting": accounting,
        "preregistered_deep_guard_count": 2,
        "additional_deep_guard_performed": False,
        "deep_guard_completion_proof": (
            "EXACT_POST_GUARD_FAILED_CLOSED_COUNTER_ASSERTION"
        ),
    }
    result["result_hash"] = stable_hash(result)

    def revalidate_guards() -> None:
        """Close the sealed-preflight race window without another deep scan."""

        latest_state = _read_json(output_dir / "production_state.json")
        latest_kpis = _read_json(output_dir / "production_kpis.json")
        _validate_failed_state(latest_state, latest_kpis)
        _require(
            latest_state["state_hash"] == state["state_hash"],
            "failed state changed during verification",
        )
        _require(
            latest_kpis["kpi_hash"] == prior_kpis["kpi_hash"],
            "KPI snapshot changed during verification",
        )
        _verify_frozen_implementation(root, manifest)
        _require(
            not recovery_receipt_path.exists(),
            "recovery receipt appeared during verification",
        )
        _require(not attempt_path.exists(), "recovery attempt appeared during verification")
        _require(not staging_bundle.exists(), "evidence staging reappeared during verification")
        _require(
            final_bundle.is_dir() and not final_bundle.is_symlink(),
            "sealed bundle changed during verification",
        )
        latest_manifest = verify_evidence_bundle(final_bundle, deep=False)
        _require(
            latest_manifest.get("bundle_content_sha256")
            == evidence_manifest.get("bundle_content_sha256"),
            "sealed bundle manifest changed during recovery preflight",
        )
        latest_receipt = _read_json(evidence_receipt_path)
        _require(
            latest_receipt == receipt,
            "evidence receipt changed during recovery preflight",
        )
        _validate_receipt(
            final_bundle=final_bundle,
            evidence_manifest=latest_manifest,
            receipt=latest_receipt,
        )

    def consume_attempt(proof: Mapping[str, Any]) -> None:
        """Consume exactly one attempt after the sealed preflight succeeds."""

        _write_attempt_marker(
            attempt_path=attempt_path,
            output_dir=output_dir,
            proof=proof,
            original_state_hash=state["state_hash"],
            original_kpi_hash=prior_kpis["kpi_hash"],
            sealed_bundle_content_sha256=str(receipt["bundle_content_sha256"]),
        )

    from hydra.compute.result_writer import AtomicResultWriter
    from hydra.production.active_risk_runtime import ActiveRiskPoolRun

    reconciliation_call_count = 0

    def reconcile_snapshots(checked_result: Mapping[str, Any]) -> None:
        """Run the existing light result-to-snapshot transaction exactly once."""

        nonlocal reconciliation_call_count
        reconciliation_call_count += 1
        _require(
            reconciliation_call_count == 1,
            "active-risk snapshot reconciliation invoked more than once",
        )
        run = ActiveRiskPoolRun(
            manifest_path=manifest_path,
            contract_map_path=root,
            cache_root=root,
            stop_after=None,
        )
        run._reconcile_completed_result_snapshots(checked_result)

    writer = AtomicResultWriter(output_dir)
    checked, published_result_sha256, preflight_proof = (
        _preflight_validate_and_publish_result(
            output_dir=output_dir,
            result_path=result_path,
            result=result,
            manifest=manifest,
            writer=writer,
            result_loader=_load_result_after_two_preregistered_guards,
            proof_payload={
                "sealed_bundle_content_sha256": receipt["bundle_content_sha256"],
                "sealed_bundle_manifest_sha256": receipt["manifest_sha256"],
                "original_failed_state_hash": state["state_hash"],
                "original_failed_kpi_hash": prior_kpis["kpi_hash"],
            },
            revalidate_guards=revalidate_guards,
            consume_attempt=consume_attempt,
            reconcile_snapshots=reconcile_snapshots,
        )
    )
    _require(reconciliation_call_count == 1, "snapshot reconciliation did not run once")
    terminal_result_identity = {
        "result_hash": checked["result_hash"],
        "autonomous_next_action": dict(checked["autonomous_next_action"]),
    }
    terminal_result_hash = str(checked["result_hash"])
    del checked, result, outputs, summary, terminal_kpis, pareto, stage_decisions
    gc.collect()

    # Reconciliation publishes state and KPI with separate atomic renames.
    # Observe read-only until both sides expose the same terminal generation.
    controller_completion = _poll_for_controller_completion(
        output_dir=output_dir,
        controller_runtime_state_path=controller_runtime_state_path,
        result=terminal_result_identity,
        original_failed_state_hash=state["state_hash"],
        original_failed_kpi_hash=prior_kpis["kpi_hash"],
        timeout_seconds=controller_poll_timeout_seconds,
        snapshot_reconciliation_attempted=True,
    )
    _require(
        controller_completion.get("source_truth_complete") is True,
        "fresh reconciliation did not reach coherent COMPLETE snapshots",
    )
    observed_state = _read_json(output_dir / "production_state.json")
    observed_kpis = _read_json(output_dir / "production_kpis.json")
    _verify_snapshot_hash(observed_state, "state_hash")
    _verify_snapshot_hash(observed_kpis, "kpi_hash")

    recovery_receipt = {
        "schema": RECOVERY_SCHEMA,
        "campaign_id": CAMPAIGN_ID,
        "recovery_version": RECOVERY_VERSION,
        "completed_at_utc": _utc_now(),
        "result_hash": terminal_result_hash,
        "result_sha256": published_result_sha256,
        "production_state_observed": str(observed_state.get("state") or "UNKNOWN"),
        "state_hash": observed_state["state_hash"],
        "kpi_hash": observed_kpis["kpi_hash"],
        "controller_completion": controller_completion,
        "authoritative_snapshot_writer": "BOUNDED_ONE_TIME_RECOVERY_RECONCILER",
        "local_state_or_kpi_write_performed": True,
        "local_write_claim_scope": "BOUNDED_RECOVERY_LIFECYCLE",
        "snapshot_reconciliation_method_invoked_this_invocation": True,
        "snapshot_hash_changed_this_invocation": (
            state["state_hash"] != observed_state["state_hash"]
            or prior_kpis["kpi_hash"] != observed_kpis["kpi_hash"]
        ),
        "snapshot_reconciliation_call_count": reconciliation_call_count,
        "deep_validation_call_count": 1,
        "resume_without_deep_validation": False,
        "state_hash_before": state["state_hash"],
        "kpi_hash_before": prior_kpis["kpi_hash"],
        "state_hash_after": observed_state["state_hash"],
        "kpi_hash_after": observed_kpis["kpi_hash"],
        "snapshot_files": {
            "production_state": str(output_dir / "production_state.json"),
            "production_kpis": str(output_dir / "production_kpis.json"),
        },
        "preflight_proof_hash": preflight_proof["proof_hash"],
        "recovery_implementation_sha256": preflight_proof[
            "recovery_implementation_sha256"
        ],
        "original_failed_state_hash": preflight_proof[
            "original_failed_state_hash"
        ],
        "original_failed_kpi_hash": preflight_proof[
            "original_failed_kpi_hash"
        ],
        "sealed_bundle_content_sha256": receipt["bundle_content_sha256"],
        "sealed_bundle_manifest_sha256": receipt["manifest_sha256"],
        "economic_outcomes_recomputed": False,
        "manifest_mutated": False,
        "evidence_bundle_mutated": False,
        "episode_accounting": accounting,
    }
    recovery_receipt["recovery_receipt_hash"] = stable_hash(recovery_receipt)
    writer.write_json(recovery_receipt_path.name, recovery_receipt)
    return recovery_receipt


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path("config/v7/active_risk_pool_target_velocity_0026_revision_02.json"),
    )
    parser.add_argument(
        "--execute-exact-recovery",
        action="store_true",
        help="required acknowledgement; there is no dry-run mutation path",
    )
    parser.add_argument(
        "--controller-poll-timeout-seconds",
        type=float,
        default=DEFAULT_CONTROLLER_POLL_TIMEOUT_SECONDS,
        help="bounded read-only wait for persistent-controller terminalization",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if not args.execute_exact_recovery:
        raise RecoveryError("refusing recovery without --execute-exact-recovery")
    try:
        receipt = recover_once(
            args.manifest,
            controller_poll_timeout_seconds=args.controller_poll_timeout_seconds,
        )
    except ControllerHandoffRequired as exc:
        print(json.dumps(exc.payload, indent=2, sort_keys=True))
        return 3
    print(json.dumps(receipt, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
