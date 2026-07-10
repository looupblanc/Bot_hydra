from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any

from hydra.validation.evidence_scope import ComputationMode, EvidenceScope


VALIDATION_VERSION = "validation_provenance_v2"
FULL = "FULL"
PROXY = "PROXY"
INHERITED_INVALID = "INHERITED_INVALID"
UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class StatusProvenance:
    status: str
    scope: EvidenceScope
    input_hash: str
    code_commit: str
    data_fingerprint: str
    validation_version: str
    policy_version: str
    computation_mode: ComputationMode
    computed_at_utc: str
    evidence_strength: float
    passed: bool

    def to_dict(self) -> dict[str, Any]:
        out = asdict(self)
        out["scope"] = self.scope.value
        out["computation_mode"] = self.computation_mode.value
        return out


def provenance_hash(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def make_status_provenance(
    *,
    status: str,
    scope: EvidenceScope,
    payload: dict[str, Any],
    code_commit: str,
    data_fingerprint: str,
    validation_version: str,
    policy_version: str,
    computation_mode: ComputationMode,
    evidence_strength: float,
    passed: bool,
) -> StatusProvenance:
    if computation_mode != ComputationMode.FULL:
        passed = False
    return StatusProvenance(
        status=status,
        scope=scope,
        input_hash=provenance_hash(payload),
        code_commit=code_commit,
        data_fingerprint=data_fingerprint,
        validation_version=validation_version,
        policy_version=policy_version,
        computation_mode=computation_mode,
        computed_at_utc=datetime.now(timezone.utc).isoformat(),
        evidence_strength=float(evidence_strength),
        passed=bool(passed),
    )


def assert_not_stale(record: StatusProvenance, *, current_input_hash: str, current_validation_version: str) -> None:
    if record.input_hash != current_input_hash or record.validation_version != current_validation_version:
        raise ValueError("Stale validation evidence cannot be counted as passed.")


@dataclass(frozen=True)
class ValidationProvenance:
    input_fingerprint: str
    validation_version: str
    computed_at: str
    computation_mode: str
    gate_modes: dict[str, str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_validation_provenance(*, input_fingerprint: str, gate_modes: dict[str, str]) -> ValidationProvenance:
    modes = set(gate_modes.values())
    if modes and modes <= {FULL}:
        mode = FULL
    elif PROXY in modes:
        mode = PROXY
    elif INHERITED_INVALID in modes:
        mode = INHERITED_INVALID
    else:
        mode = UNKNOWN
    return ValidationProvenance(
        input_fingerprint=input_fingerprint,
        validation_version=VALIDATION_VERSION,
        computed_at=datetime.now(timezone.utc).isoformat(),
        computation_mode=mode,
        gate_modes=dict(gate_modes),
    )


def is_full_status_usable(record: dict[str, Any], current_input_fingerprint: str) -> bool:
    if not record:
        return False
    if record.get("input_fingerprint") != current_input_fingerprint:
        return False
    if record.get("validation_version") != VALIDATION_VERSION:
        return False
    if record.get("computation_mode") != FULL:
        return False
    return all(mode == FULL for mode in dict(record.get("gate_modes") or {}).values())


def reject_stale_status(record: dict[str, Any], current_input_fingerprint: str) -> bool:
    return not is_full_status_usable(record, current_input_fingerprint)
