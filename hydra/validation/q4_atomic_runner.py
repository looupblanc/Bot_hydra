from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import subprocess
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from hydra.governance.q4_one_shot import (
    AuthorizedQ4Capability,
    append_q4_access_once,
    close_q4_transaction,
    consume_authorization_once,
    mark_q4_data_opened,
)
from hydra.promotion.final_cohort import validate_final_cohort_manifest
from hydra.utils.time import utc_now_iso


RESULT_SCHEMA = "hydra_q4_atomic_result_v1"
Q4_CLASSIFICATIONS = {
    "Q4_LOCKBOX_PASS",
    "Q4_LOCKBOX_FAIL",
    "Q4_LOCKBOX_INSUFFICIENT",
}


class Q4AtomicRunnerError(RuntimeError):
    pass


def validate_q4_atomic_result(
    manifest: Mapping[str, Any], result: Mapping[str, Any]
) -> None:
    _validate_result_bundle(manifest, result)
    if int(result.get("q4_access_count_delta") or 0) != 1:
        raise Q4AtomicRunnerError("Q4 atomic result did not consume exactly one access.")
    if int(result.get("outbound_orders") or 0) != 0 or int(
        result.get("broker_connections") or 0
    ) != 0:
        raise Q4AtomicRunnerError("Q4 atomic result contains broker/order activity.")


Q4Evaluator = Callable[
    [Mapping[str, Any], AuthorizedQ4Capability],
    Sequence[Mapping[str, Any]] | Mapping[str, Any],
]


def classify_role_specific_q4_result(
    candidate: Mapping[str, Any],
    metrics: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    role = str(candidate.get("role") or "")
    events = int(metrics.get("events") or 0)
    hard_failures = [str(value) for value in metrics.get("hard_failures") or []]
    reasons: list[str] = []
    if hard_failures:
        return {
            "classification": "Q4_LOCKBOX_FAIL",
            "reasons": sorted(set(hard_failures)),
        }
    minimum_events = int(policy.get("minimum_executable_events") or 5)
    if events < minimum_events:
        return {
            "classification": "Q4_LOCKBOX_INSUFFICIENT",
            "reasons": [f"executable_events_below_frozen_floor:{events}<{minimum_events}"],
        }
    net = float(metrics.get("net_pnl") or 0.0)
    mll_breached = bool(metrics.get("mll_breached"))
    concentration = float(metrics.get("best_day_positive_pnl_fraction") or 0.0)
    maximum_concentration = float(
        policy.get("maximum_best_day_positive_pnl_fraction") or 0.50
    )
    if role == "COMBINE_PASSER":
        if net <= 0.0:
            reasons.append("nonpositive_q4_net_after_frozen_costs")
        if mll_breached:
            reasons.append("q4_mll_breach")
        if float(metrics.get("target_progress") or 0.0) <= 0.0:
            reasons.append("nonpositive_q4_target_progress")
        if concentration > maximum_concentration:
            reasons.append("best_day_concentration_exceeds_frozen_limit")
    elif role == "XFA_PAYOUT":
        if net <= 0.0:
            reasons.append("nonpositive_q4_net_after_frozen_costs")
        if mll_breached:
            reasons.append("q4_mll_breach")
        qualifying = int(metrics.get("qualifying_days") or 0)
        if qualifying < int(policy.get("minimum_xfa_qualifying_days") or 2):
            reasons.append("insufficient_q4_xfa_qualifying_days")
        if concentration > maximum_concentration:
            reasons.append("best_day_concentration_exceeds_frozen_limit")
        if bool(metrics.get("catastrophic_xfa_contradiction")):
            reasons.append("catastrophic_xfa_path_contradiction")
    elif role in {"DEFENSIVE", "DEFENSIVE_ACCOUNT", "PORTFOLIO_ONLY"}:
        account = dict(metrics.get("account_utility") or {})
        control_count = int(account.get("control_count") or 0)
        minimum_controls = int(policy.get("minimum_defensive_control_count") or 32)
        if control_count < minimum_controls:
            return {
                "classification": "Q4_LOCKBOX_INSUFFICIENT",
                "reasons": [
                    f"matched_account_controls_below_frozen_floor:{control_count}<{minimum_controls}"
                ],
            }
        material = bool(
            float(account.get("maximum_drawdown_reduction") or 0.0) > 0.0
            or float(account.get("min_mll_buffer_delta") or 0.0) > 0.0
            or int(account.get("shared_loss_days_reduction") or 0) > 0
        )
        if not material:
            reasons.append("no_material_q4_account_protection")
        if bool(account.get("hard_risk_violation")):
            reasons.append("defensive_candidate_created_hard_account_risk")
        if float(account.get("matched_control_probability") or 1.0) > float(
            policy.get("maximum_defensive_matched_control_probability") or 0.10
        ):
            reasons.append("defensive_matched_control_not_supported")
        if float(account.get("target_velocity_loss_fraction") or 0.0) > float(
            policy.get("maximum_defensive_target_velocity_loss_fraction") or 0.25
        ):
            reasons.append("destructive_target_velocity_loss")
    else:
        reasons.append("unsupported_frozen_candidate_role")
    return {
        "classification": (
            "Q4_LOCKBOX_PASS" if not reasons else "Q4_LOCKBOX_FAIL"
        ),
        "reasons": reasons,
    }


def run_q4_atomic_one_shot(
    output_dir: str | Path,
    *,
    cohort_manifest_path: str | Path,
    cohort_manifest_sha256: str,
    cohort_manifest_hash: str,
    authorization_path: str | Path,
    authorization_hash: str,
    authorization_token: str,
    code_commit: str,
    mission_db_path: str | Path,
    registry_db_path: str | Path,
    access_ledger_path: str | Path,
    evaluator: Q4Evaluator,
) -> dict[str, Any]:
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    manifest_path = Path(cohort_manifest_path)
    if not manifest_path.is_file() or _sha256(manifest_path) != cohort_manifest_sha256:
        raise Q4AtomicRunnerError("Frozen cohort file hash mismatch before Q4.")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    validate_final_cohort_manifest(manifest)
    if str(manifest.get("manifest_hash")) != cohort_manifest_hash:
        raise Q4AtomicRunnerError("Frozen cohort semantic hash mismatch before Q4.")
    if str(manifest.get("source_commit")) != code_commit:
        raise Q4AtomicRunnerError("Frozen cohort source commit mismatch.")
    if len(code_commit) == 40:
        actual = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
        if actual != code_commit:
            raise Q4AtomicRunnerError("Q4 runner commit differs from frozen cohort.")
    for database, label in (
        (mission_db_path, "mission DB"),
        (registry_db_path, "registry DB"),
    ):
        if _sqlite_integrity(database) != "ok":
            raise Q4AtomicRunnerError(f"{label} integrity failed before Q4.")
    capability = consume_authorization_once(
        token=authorization_token,
        authorization_path=authorization_path,
        expected_authorization_hash=authorization_hash,
        expected_manifest_hash=cohort_manifest_hash,
        expected_source_commit=code_commit,
        access_ledger_path=access_ledger_path,
    )
    staging = root / f".q4_staging_{capability.token_id}"
    quarantine = root / f"q4_quarantine_{capability.token_id}"
    result_path = root / "q4_atomic_result.json"
    opened = False
    try:
        if result_path.exists() or staging.exists():
            raise Q4AtomicRunnerError("Q4 output path already exists; automatic retry refused.")
        staging.mkdir(parents=True, exist_ok=False)
        mark_q4_data_opened(capability)
        opened = True
        evaluated = evaluator(manifest, capability)
        if isinstance(evaluated, Mapping):
            candidate_results = [
                dict(row) for row in evaluated.get("candidate_results") or []
            ]
            run_metadata = dict(evaluated.get("run_metadata") or {})
        else:
            candidate_results = [dict(row) for row in evaluated]
            run_metadata = {}
        _validate_candidate_results(manifest, candidate_results)
        status_counts = {
            status: sum(
                str(row["classification"]) == status for row in candidate_results
            )
            for status in sorted(Q4_CLASSIFICATIONS)
        }
        payload: dict[str, Any] = {
            "schema": RESULT_SCHEMA,
            "cohort_id": manifest["cohort_id"],
            "cohort_manifest_hash": cohort_manifest_hash,
            "source_commit": code_commit,
            "q4_period": list(manifest["q4_period"]),
            "candidate_count": len(candidate_results),
            "candidate_results": candidate_results,
            "status_counts": status_counts,
            "q4_pass_candidate_ids": sorted(
                row["candidate_id"]
                for row in candidate_results
                if row["classification"] == "Q4_LOCKBOX_PASS"
            ),
            "paper_shadow_ready_candidate_ids": sorted(
                row["candidate_id"]
                for row in candidate_results
                if row["classification"] == "Q4_LOCKBOX_PASS"
            ),
            "q4_access_count_delta": 1,
            "parameters_mutated": False,
            "automatic_retry_allowed": False,
            "outbound_orders": 0,
            "broker_connections": 0,
            "run_metadata": run_metadata,
            "completed_at_utc": utc_now_iso(),
        }
        payload["result_hash"] = _stable_hash(payload)
        staged_result = staging / "q4_atomic_result.json"
        _write_new(staged_result, payload)
        verified = json.loads(staged_result.read_text(encoding="utf-8"))
        _validate_result_bundle(manifest, verified)
        os.replace(staged_result, result_path)
        staging.rmdir()
        result_sha = _sha256(result_path)
        access_hash = append_q4_access_once(
            capability,
            ledger_path=access_ledger_path,
            candidate_ids=list(manifest["candidate_ids"]),
            result_bundle_sha256=result_sha,
        )
        closure = close_q4_transaction(
            capability,
            status="COMMITTED",
            result_bundle_path=str(result_path.resolve()),
            result_bundle_sha256=result_sha,
            access_record_hash=access_hash,
        )
        return {
            **verified,
            "result_path": str(result_path.resolve()),
            "result_sha256": result_sha,
            "closure_path": str(closure.resolve()),
            "access_record_hash": access_hash,
        }
    except BaseException as exc:
        if staging.exists():
            if quarantine.exists():
                raise Q4AtomicRunnerError("Q4 quarantine collision.") from exc
            os.replace(staging, quarantine)
        if opened:
            failure = root / f"q4_failure_receipt_{capability.token_id}.json"
            _write_new(
                failure,
                {
                    "schema": "hydra_q4_failure_receipt_v1",
                    "token_id": capability.token_id,
                    "cohort_manifest_hash": cohort_manifest_hash,
                    "status": "Q4_REVIEW_REQUIRED",
                    "error_type": type(exc).__name__,
                    "error": str(exc)[:2000],
                    "automatic_retry_allowed": False,
                    "created_at_utc": utc_now_iso(),
                },
            )
            failure_sha = _sha256(failure)
            access_hash = append_q4_access_once(
                capability,
                ledger_path=access_ledger_path,
                candidate_ids=list(manifest["candidate_ids"]),
                result_bundle_sha256=failure_sha,
            )
            close_q4_transaction(
                capability,
                status="Q4_REVIEW_REQUIRED",
                result_bundle_path=str(failure.resolve()),
                result_bundle_sha256=failure_sha,
                access_record_hash=access_hash,
                error=f"{type(exc).__name__}:{exc}",
            )
        else:
            close_q4_transaction(
                capability,
                status="Q4_REVIEW_REQUIRED",
                result_bundle_path=None,
                result_bundle_sha256=None,
                access_record_hash=None,
                error=f"{type(exc).__name__}:{exc}",
            )
        raise


def _validate_candidate_results(
    manifest: Mapping[str, Any], candidate_results: Sequence[Mapping[str, Any]]
) -> None:
    expected = sorted(str(value) for value in manifest.get("candidate_ids") or [])
    observed = sorted(str(row.get("candidate_id") or "") for row in candidate_results)
    if observed != expected or len(observed) != len(set(observed)):
        raise Q4AtomicRunnerError("Atomic Q4 candidate result set is incomplete or duplicated.")
    for row in candidate_results:
        if str(row.get("classification")) not in Q4_CLASSIFICATIONS:
            raise Q4AtomicRunnerError("Unsupported Q4 classification.")
        if not isinstance(row.get("metrics"), dict) or not isinstance(
            row.get("reasons"), list
        ):
            raise Q4AtomicRunnerError("Q4 candidate evidence is incomplete.")


def _validate_result_bundle(
    manifest: Mapping[str, Any], result: Mapping[str, Any]
) -> None:
    semantic = {
        key: value
        for key, value in result.items()
        if key
        not in {
            "result_path",
            "result_sha256",
            "closure_path",
            "access_record_hash",
        }
    }
    expected_hash = str(semantic.pop("result_hash", ""))
    if not expected_hash or _stable_hash(semantic) != expected_hash:
        raise Q4AtomicRunnerError("Q4 result semantic hash is invalid.")
    _validate_candidate_results(manifest, list(result.get("candidate_results") or []))
    if int(result.get("q4_access_count_delta") or 0) != 1:
        raise Q4AtomicRunnerError("Q4 result must consume exactly one cohort access.")
    if bool(result.get("parameters_mutated")):
        raise Q4AtomicRunnerError("Q4 result reports parameter mutation.")


def _sqlite_integrity(path: str | Path) -> str:
    connection = sqlite3.connect(f"file:{Path(path).resolve()}?mode=ro", uri=True)
    try:
        return str(connection.execute("PRAGMA integrity_check").fetchone()[0])
    finally:
        connection.close()


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_new(path: Path, payload: Mapping[str, Any]) -> None:
    encoded = json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n"
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:
        path.unlink(missing_ok=True)
        raise
