from __future__ import annotations

import hashlib
import json
import os
import secrets
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from hydra.governance.invariants import q4_access_count
from hydra.promotion.final_cohort import validate_final_cohort_manifest
from hydra.utils.time import utc_now_iso


AUTHORIZATION_SCHEMA = "hydra_q4_cohort_authorization_v1"


class CohortAuthorizationError(RuntimeError):
    pass


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


@dataclass(frozen=True)
class IssuedCohortAuthorization:
    token: str
    token_id: str
    token_sha256: str
    authorization_path: str
    authorization_hash: str


def issue_cohort_authorization(
    *,
    cohort_manifest_path: str | Path,
    cohort_manifest_sha256: str,
    cohort_manifest_hash: str,
    source_commit: str,
    governance_semantic_hash: str,
    governance_yaml_sha256: str,
    authorization_root: str | Path,
    access_ledger_path: str | Path,
) -> IssuedCohortAuthorization:
    manifest_path = Path(cohort_manifest_path)
    if not manifest_path.is_file() or _sha256(manifest_path) != cohort_manifest_sha256:
        raise CohortAuthorizationError("Frozen cohort file hash mismatch.")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    validate_final_cohort_manifest(manifest)
    if str(manifest.get("manifest_hash")) != cohort_manifest_hash:
        raise CohortAuthorizationError("Frozen cohort semantic hash mismatch.")
    if str(manifest.get("source_commit")) != source_commit:
        raise CohortAuthorizationError("Authorization commit differs from frozen cohort.")
    if q4_access_count(str(access_ledger_path)) != 0:
        raise CohortAuthorizationError("Q4 authoritative access count must be zero.")
    root = Path(authorization_root) / str(manifest["cohort_id"])
    root.mkdir(parents=True, exist_ok=True)
    authorization_path = root / "authorization.json"
    consumption_path = root / "consumption.json"
    closure_path = root / "closure.json"
    if authorization_path.exists() or consumption_path.exists() or closure_path.exists():
        raise CohortAuthorizationError("A Q4 capability already exists for this cohort.")
    token = secrets.token_urlsafe(48)
    token_sha256 = hashlib.sha256(token.encode()).hexdigest()
    token_id = hashlib.sha256(
        f"{cohort_manifest_hash}:{token_sha256}".encode()
    ).hexdigest()[:24]
    payload: dict[str, Any] = {
        "schema": AUTHORIZATION_SCHEMA,
        "token_id": token_id,
        "token_sha256": token_sha256,
        "cohort_id": manifest["cohort_id"],
        "cohort_manifest_path": str(manifest_path.resolve()),
        "cohort_manifest_sha256": cohort_manifest_sha256,
        "cohort_manifest_hash": cohort_manifest_hash,
        "source_commit": source_commit,
        "governance_semantic_hash": governance_semantic_hash,
        "governance_yaml_sha256": governance_yaml_sha256,
        "q4_access_count_before": 0,
        "candidate_ids": list(manifest["candidate_ids"]),
        "issued_at_utc": utc_now_iso(),
        "status": "AUTHORIZED_SINGLE_USE_UNCONSUMED",
    }
    payload["authorization_hash"] = _stable_hash(payload)
    _write_exclusive(authorization_path, payload)
    return IssuedCohortAuthorization(
        token=token,
        token_id=token_id,
        token_sha256=token_sha256,
        authorization_path=str(authorization_path.resolve()),
        authorization_hash=str(payload["authorization_hash"]),
    )


def validate_authorization(
    *,
    token: str,
    authorization_path: str | Path,
    expected_authorization_hash: str,
    expected_manifest_hash: str,
    expected_source_commit: str,
    access_ledger_path: str | Path,
) -> dict[str, Any]:
    path = Path(authorization_path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    semantic = dict(payload)
    actual_hash = str(semantic.pop("authorization_hash", ""))
    if actual_hash != expected_authorization_hash or _stable_hash(semantic) != actual_hash:
        raise CohortAuthorizationError("Authorization record hash is invalid.")
    if hashlib.sha256(token.encode()).hexdigest() != str(payload["token_sha256"]):
        raise CohortAuthorizationError("Q4 capability token is invalid.")
    if str(payload["cohort_manifest_hash"]) != expected_manifest_hash:
        raise CohortAuthorizationError("Q4 capability is bound to another manifest.")
    if str(payload["source_commit"]) != expected_source_commit:
        raise CohortAuthorizationError("Q4 capability is bound to another commit.")
    if str(payload.get("status")) != "AUTHORIZED_SINGLE_USE_UNCONSUMED":
        raise CohortAuthorizationError("Q4 capability is not unconsumed.")
    root = path.parent
    if (root / "consumption.json").exists() or (root / "closure.json").exists():
        raise CohortAuthorizationError("Q4 capability was already consumed.")
    if q4_access_count(str(access_ledger_path)) != 0:
        raise CohortAuthorizationError("Q4 access count changed after authorization.")
    return payload


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
