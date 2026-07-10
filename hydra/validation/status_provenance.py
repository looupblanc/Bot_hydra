from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Any

from hydra.utils.time import utc_now_iso


VALIDATION_VERSION = "promotion_status_v2"

FULL = "FULL"
PROXY = "PROXY"
INHERITED_INVALID = "INHERITED_INVALID"
UNKNOWN = "UNKNOWN"

STRONG = "STRONG"
MODERATE = "MODERATE"
WEAK = "WEAK"
LEGACY_UNVERSIONED = "LEGACY_UNVERSIONED"


@dataclass(frozen=True)
class ValidationProvenance:
    validation_version: str
    input_fingerprint: str
    computed_at: str
    computation_mode: str
    evidence_strength: str
    gate_modes: dict[str, str]
    notes: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def stable_fingerprint(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def build_promotion_input_fingerprint(
    *,
    candidate: Any,
    metrics: dict[str, Any],
    topstep_record: dict[str, Any],
    split_scores: dict[str, Any],
    leak_ok: bool,
    leak_reason: str,
    data_validation: dict[str, Any],
    max_correlation: float,
    seed: int,
) -> str:
    return stable_fingerprint(
        {
            "candidate": {
                "candidate_id": getattr(candidate, "candidate_id", ""),
                "family": getattr(candidate, "family", ""),
                "symbol": getattr(candidate, "symbol", ""),
                "timeframe": getattr(candidate, "timeframe", ""),
                "parameters": getattr(candidate, "parameters", {}),
                "risk_parameters": getattr(candidate, "risk_parameters", {}),
                "parent_candidate_id": getattr(candidate, "parent_candidate_id", None),
                "mutation_type": getattr(candidate, "mutation_type", None),
            },
            "metrics": metrics,
            "topstep_record": _compact_topstep_record(topstep_record),
            "split_scores": split_scores,
            "leak_ok": bool(leak_ok),
            "leak_reason": leak_reason,
            "data_validation": data_validation,
            "max_correlation": float(max_correlation),
            "seed": int(seed),
            "validation_version": VALIDATION_VERSION,
        }
    )


def build_validation_provenance(
    *,
    input_fingerprint: str,
    gate_modes: dict[str, str],
    notes: list[str] | None = None,
) -> ValidationProvenance:
    modes = set(gate_modes.values())
    if INHERITED_INVALID in modes or UNKNOWN in modes:
        mode = INHERITED_INVALID if INHERITED_INVALID in modes else UNKNOWN
        strength = WEAK
    elif PROXY in modes:
        mode = PROXY
        strength = MODERATE
    else:
        mode = FULL
        strength = STRONG
    return ValidationProvenance(
        validation_version=VALIDATION_VERSION,
        input_fingerprint=input_fingerprint,
        computed_at=utc_now_iso(),
        computation_mode=mode,
        evidence_strength=strength,
        gate_modes=dict(sorted(gate_modes.items())),
        notes=list(notes or []),
    )


def gate_computation_modes() -> dict[str, str]:
    return {
        "DATA_INTEGRITY": FULL,
        "DUPLICATE_FINGERPRINT": FULL,
        "NO_LOOKAHEAD": FULL,
        "ECONOMIC_PROFILE": FULL,
        "WALK_FORWARD": FULL,
        "OOS": FULL,
        "MONTE_CARLO": FULL,
        "PARAMETER_SENSITIVITY": PROXY,
        "TOPSTEP_COMBINE": FULL,
        "FUNDED_XFA": FULL,
        "PAYOUT_SURVIVAL": FULL,
        "CORRELATION": PROXY,
        "PORTFOLIO_INTERACTION": PROXY,
        "EXECUTION_READINESS": FULL,
    }


def legacy_provenance_status(row: dict[str, Any]) -> dict[str, Any]:
    if row.get("validation_version") and row.get("input_fingerprint"):
        return {
            "mode": row.get("computation_mode") or UNKNOWN,
            "evidence_strength": row.get("evidence_strength") or UNKNOWN,
            "validation_version": row.get("validation_version"),
            "input_fingerprint": row.get("input_fingerprint"),
            "usable_for_final_promotion": is_full_status_usable(row, row.get("input_fingerprint")),
        }
    return {
        "mode": UNKNOWN,
        "evidence_strength": LEGACY_UNVERSIONED,
        "validation_version": "",
        "input_fingerprint": "",
        "usable_for_final_promotion": False,
    }


def is_full_status_usable(row_or_provenance: dict[str, Any], expected_input_fingerprint: str | None) -> bool:
    if not row_or_provenance:
        return False
    if row_or_provenance.get("validation_version") != VALIDATION_VERSION:
        return False
    if not row_or_provenance.get("input_fingerprint"):
        return False
    if expected_input_fingerprint and row_or_provenance.get("input_fingerprint") != expected_input_fingerprint:
        return False
    return row_or_provenance.get("computation_mode") == FULL and row_or_provenance.get("evidence_strength") == STRONG


def reject_stale_status(row_or_provenance: dict[str, Any], expected_input_fingerprint: str) -> bool:
    return not is_full_status_usable(row_or_provenance, expected_input_fingerprint)


def _compact_topstep_record(record: dict[str, Any]) -> dict[str, Any]:
    excluded = {"standard", "consistency"}
    return {key: value for key, value in record.items() if key not in excluded}
