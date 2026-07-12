from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from hydra.governance.cohort_authorization import (
    CohortAuthorizationError,
    validate_authorization,
)
from hydra.utils.time import utc_now_iso


CONSUMPTION_SCHEMA = "hydra_q4_one_shot_consumption_v1"
CLOSURE_SCHEMA = "hydra_q4_one_shot_closure_v1"


class Q4OneShotError(RuntimeError):
    pass


@dataclass(frozen=True)
class AuthorizedQ4Capability:
    token_id: str
    cohort_id: str
    cohort_manifest_hash: str
    source_commit: str
    consumption_path: str
    _scope_marker: str

    def validate_scope(self) -> None:
        expected = hashlib.sha256(
            f"{self.token_id}:{self.cohort_manifest_hash}:{self.source_commit}".encode()
        ).hexdigest()
        if self._scope_marker != expected:
            raise Q4OneShotError("Q4 capability scope marker is invalid.")


def consume_authorization_once(
    *,
    token: str,
    authorization_path: str | Path,
    expected_authorization_hash: str,
    expected_manifest_hash: str,
    expected_source_commit: str,
    access_ledger_path: str | Path,
) -> AuthorizedQ4Capability:
    try:
        authorization = validate_authorization(
            token=token,
            authorization_path=authorization_path,
            expected_authorization_hash=expected_authorization_hash,
            expected_manifest_hash=expected_manifest_hash,
            expected_source_commit=expected_source_commit,
            access_ledger_path=access_ledger_path,
        )
    except CohortAuthorizationError as exc:
        raise Q4OneShotError(str(exc)) from exc
    token_id = str(authorization["token_id"])
    consumption_path = Path(authorization_path).parent / "consumption.json"
    payload = {
        "schema": CONSUMPTION_SCHEMA,
        "token_id": token_id,
        "cohort_id": authorization["cohort_id"],
        "cohort_manifest_hash": expected_manifest_hash,
        "source_commit": expected_source_commit,
        "consumed_at_utc": utc_now_iso(),
        "status": "CONSUMED_SINGLE_ATTEMPT_Q4_CLOSED_PENDING_OPEN",
    }
    payload["consumption_hash"] = _stable_hash(payload)
    _write_exclusive(consumption_path, payload)
    marker = hashlib.sha256(
        f"{token_id}:{expected_manifest_hash}:{expected_source_commit}".encode()
    ).hexdigest()
    return AuthorizedQ4Capability(
        token_id=token_id,
        cohort_id=str(authorization["cohort_id"]),
        cohort_manifest_hash=expected_manifest_hash,
        source_commit=expected_source_commit,
        consumption_path=str(consumption_path.resolve()),
        _scope_marker=marker,
    )


def mark_q4_data_opened(capability: AuthorizedQ4Capability) -> Path:
    capability.validate_scope()
    root = Path(capability.consumption_path).parent
    marker = root / "data_opened.json"
    payload = {
        "schema": "hydra_q4_data_opened_v1",
        "token_id": capability.token_id,
        "cohort_id": capability.cohort_id,
        "cohort_manifest_hash": capability.cohort_manifest_hash,
        "opened_at_utc": utc_now_iso(),
        "status": "Q4_OPENED_SINGLE_ATTEMPT",
    }
    payload["opened_hash"] = _stable_hash(payload)
    _write_exclusive(marker, payload)
    return marker


def close_q4_transaction(
    capability: AuthorizedQ4Capability,
    *,
    status: str,
    result_bundle_path: str | None,
    result_bundle_sha256: str | None,
    access_record_hash: str | None,
    error: str | None = None,
) -> Path:
    capability.validate_scope()
    if status not in {"COMMITTED", "Q4_REVIEW_REQUIRED"}:
        raise Q4OneShotError("Unsupported Q4 closure status.")
    root = Path(capability.consumption_path).parent
    closure = root / "closure.json"
    payload = {
        "schema": CLOSURE_SCHEMA,
        "token_id": capability.token_id,
        "cohort_id": capability.cohort_id,
        "cohort_manifest_hash": capability.cohort_manifest_hash,
        "source_commit": capability.source_commit,
        "closed_at_utc": utc_now_iso(),
        "status": status,
        "result_bundle_path": result_bundle_path,
        "result_bundle_sha256": result_bundle_sha256,
        "access_record_hash": access_record_hash,
        "automatic_retry_allowed": False,
        "error": str(error)[:2000] if error else None,
    }
    payload["closure_hash"] = _stable_hash(payload)
    _write_exclusive(closure, payload)
    return closure


def append_q4_access_once(
    capability: AuthorizedQ4Capability,
    *,
    ledger_path: str | Path,
    candidate_ids: list[str],
    result_bundle_sha256: str,
) -> str:
    capability.validate_scope()
    path = Path(ledger_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = []
    if path.exists():
        existing = [
            json.loads(line)
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
    matches = [row for row in existing if row.get("q4_transaction_id") == capability.token_id]
    if matches:
        if len(matches) != 1:
            raise Q4OneShotError("Q4 transaction appears multiple times in access ledger.")
        return str(matches[0].get("record_hash") or "")
    record: dict[str, Any] = {
        "code_commit": capability.source_commit,
        "process_id": os.getpid(),
        "timestamp_utc": utc_now_iso(),
        "period_accessed": "2024-10-01:2025-01-01_EXCLUSIVE",
        "data_role": "FINAL_LOCKBOX",
        "requesting_module": "hydra.validation.q4_atomic_runner",
        "candidate_ids": sorted(candidate_ids),
        "reason_for_access": "manifest_bound_atomic_q4_one_shot",
        "freeze_manifest_hash": capability.cohort_manifest_hash,
        "parameters_mutable": False,
        "q4_transaction_id": capability.token_id,
        "result_bundle_sha256": result_bundle_sha256,
    }
    record["record_hash"] = _stable_hash(record)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, sort_keys=True, default=str) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    return str(record["record_hash"])


def audit_q4_one_shot_state(
    *, authorization_root: str | Path, ledger_path: str | Path
) -> dict[str, Any]:
    root = Path(authorization_root)
    ledger_rows = []
    path = Path(ledger_path)
    if path.exists():
        ledger_rows = [
            json.loads(line)
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip() and json.loads(line).get("q4_transaction_id")
        ]
    closures = list(root.glob("*/closure.json")) if root.exists() else []
    consumptions = list(root.glob("*/consumption.json")) if root.exists() else []
    opened = list(root.glob("*/data_opened.json")) if root.exists() else []
    if not ledger_rows and not closures and not consumptions and not opened:
        return {"valid": True, "status": "Q4_UNOPENED", "transaction_count": 0}
    if not closures:
        return {
            "valid": False,
            "status": "Q4_INCOMPLETE_TRANSACTION_REVIEW_REQUIRED",
            "transaction_count": max(len(consumptions), len(opened)),
        }
    if len(ledger_rows) != 1 or len(closures) != 1:
        return {"valid": False, "status": "Q4_AUDIT_CARDINALITY_ERROR"}
    closure = json.loads(closures[0].read_text(encoding="utf-8"))
    row = ledger_rows[0]
    valid = bool(
        closure.get("token_id") == row.get("q4_transaction_id")
        and closure.get("cohort_manifest_hash") == row.get("freeze_manifest_hash")
        and closure.get("status") in {"COMMITTED", "Q4_REVIEW_REQUIRED"}
        and closure.get("automatic_retry_allowed") is False
    )
    if closure.get("status") in {"COMMITTED", "Q4_REVIEW_REQUIRED"}:
        result_path = Path(str(closure.get("result_bundle_path") or ""))
        valid = bool(
            valid
            and result_path.is_file()
            and _sha256(result_path) == str(closure.get("result_bundle_sha256") or "")
            and str(closure.get("access_record_hash") or "")
            == str(row.get("record_hash") or "")
        )
    return {
        "valid": valid,
        "status": str(closure.get("status") or "UNKNOWN"),
        "transaction_count": 1,
        "token_id": closure.get("token_id"),
        "cohort_manifest_hash": closure.get("cohort_manifest_hash"),
    }


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_exclusive(path: Path, payload: dict[str, Any]) -> None:
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
