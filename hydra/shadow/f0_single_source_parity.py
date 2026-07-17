"""Seal a fail-closed F0 receipt when frozen development evidence is causal-contaminated.

This module is intentionally narrow.  It reads only the already sealed Operating
Package V1 manifests, the bounded online/offline audit, and the source files
which define the legacy forward-outcome dependency.  It never reads forward
bars, never changes a frozen package, and can never write the F0 authorization
receipt.

The receipt separates an exact *technical* replay of the legacy development
components from scientific compatibility.  A replay can reproduce a legacy
artifact while that artifact remains unsuitable for causal online activation.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence


F0_RECEIPT_SCHEMA = "hydra_f0_single_source_parity_receipt_v1"
DEVELOPMENT_EVIDENCE_CONTAMINATED = "DEVELOPMENT_EVIDENCE_CONTAMINATED"
LEGACY_FAIL_CLOSED_STATUS = "ONLINE_OFFLINE_EQUIVALENCE_NOT_PROVEN_FAIL_CLOSED"

DEVELOPMENT_FILL_MODEL_ID = "DEVELOPMENT_FILL_MODEL"
FORWARD_CONSERVATIVE_FILL_MODEL_ID = "FORWARD_CONSERVATIVE_FILL_MODEL"

DEFAULT_AUDIT_PATH = Path(
    "reports/operating/hydra_operating_package_v1/"
    "online_offline_equivalence_audit.json"
)
DEFAULT_OPERATING_MANIFEST_PATH = Path(
    "reports/operating/hydra_operating_package_v1/OPERATING_PACKAGE_V1.json"
)
DEFAULT_FORENSICS_PATH = Path(
    "reports/operating/hydra_operating_package_v1/F0_ROOT_CAUSE_FORENSICS.json"
)
DEFAULT_OUTPUT_PATH = Path(
    "reports/operating/hydra_operating_package_v1/"
    "F0_SINGLE_SOURCE_ENGINE_PARITY_CONTAMINATION_RECEIPT.json"
)
AUTHORIZATION_RECEIPT_PATH = Path(
    "mission/state/operating_package_v1_parity/"
    "online_offline_equivalence_receipt.json"
)

EXPECTED_BOOK_COUNT = 6
EXPECTED_SLEEVE_COUNT = 18
EXPECTED_AFFECTED_SLEEVE_COUNT = 5
EXPECTED_SIGNAL_DIVERGENCE_COUNT = 21
EXPECTED_UNPREDICTABLE_GAP_COUNT = 6
EXPECTED_LEGACY_EVENT_COUNT = 2_052

_SOURCE_PATHS = (
    Path("hydra/research/rolling_combine_replay.py"),
    Path("hydra/research/turbo_feature_builder.py"),
    Path("hydra/shadow/active_risk_online_equivalence.py"),
    Path("hydra/shadow/f0_single_source_parity.py"),
    Path("scripts/finalize_f0_single_source_parity.py"),
)

DEVELOPMENT_FILL_MODEL = {
    "fill_policy_id": DEVELOPMENT_FILL_MODEL_ID,
    "version": "hydra_development_fill_model_legacy_v1",
    "scope": "DEVELOPMENT_COMPATIBILITY_ONLY",
    "entry": "FROZEN_FEATURE_MATRIX_NEXT_ROW_CLOSE",
    "entry_price_availability": "ONE_MINUTE_AFTER_LABELED_DECISION_TIME",
    "legacy_entry_time_label": "DECISION_TIME",
    "exit": "FROZEN_HORIZON_CLOSE",
    "costs": "FROZEN_DEVELOPMENT_COMPONENT_COSTS",
    "causal_forward_authorization": False,
}

FORWARD_CONSERVATIVE_FILL_MODEL = {
    "fill_policy_id": FORWARD_CONSERVATIVE_FILL_MODEL_ID,
    "version": "hydra_forward_conservative_fill_model_v1",
    "scope": "ZERO_ORDER_APPEND_ONLY_FORWARD_OBSERVATION",
    "entry": "NEXT_BAR_OPEN_PLUS_EXACTLY_ONE_ADVERSE_TICK",
    "entry_slippage_ticks": 1,
    "exit": "FROZEN_SLEEVE_EXIT_MINUS_EXACTLY_ONE_ADVERSE_TICK",
    "exit_slippage_ticks": 1,
    "same_bar_ambiguity": "ADVERSE_PATH",
    "deterministic": True,
    "availability_safe": True,
    "real_order_submission": False,
}


class F0SingleSourceParityError(RuntimeError):
    """An immutable input, receipt invariant, or output boundary failed."""


def stable_hash(value: Any) -> str:
    """Return a deterministic SHA-256 over strict canonical JSON."""

    try:
        encoded = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise F0SingleSourceParityError("payload is not strict canonical JSON") from exc
    return hashlib.sha256(encoded).hexdigest()


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_f0_contamination_receipt(
    *,
    repository_root: str | Path,
    audit_path: str | Path = DEFAULT_AUDIT_PATH,
    operating_manifest_path: str | Path = DEFAULT_OPERATING_MANIFEST_PATH,
    created_at: datetime | None = None,
) -> dict[str, Any]:
    """Build, but do not write, the terminal contamination receipt.

    The function fails unless the existing audit proves both exact legacy
    component replay and a material future-outcome dependency.  Consequently a
    caller cannot manufacture ``DEVELOPMENT_EVIDENCE_CONTAMINATED`` from a
    clean or merely incomplete audit.
    """

    root = Path(repository_root).resolve()
    frozen = _load_and_validate_frozen_inputs(
        root=root,
        audit_path=audit_path,
        operating_manifest_path=operating_manifest_path,
    )
    audit = frozen["audit"]
    causal_rows = audit["causal_horizon_audit"]["sleeves"]
    affected_rows = [row for row in causal_rows if int(row["mismatch_count"]) > 0]

    sleeve_mismatches = [
        {
            "sleeve_id": str(row["sleeve_id"]),
            "legacy_signal_count": int(row["offline_signal_count"]),
            "causal_signal_mismatch_count": int(row["mismatch_count"]),
            "additional_signal_count_without_future_outcome_mask": int(
                row["additional_signal_count_without_forward_outcome_mask"]
            ),
            "scheduled_boundary_count": int(
                row["known_session_roll_holiday_boundary_count"]
            ),
            "unpredictable_gap_count": int(row["unpredictable_gap_count"]),
        }
        for row in sorted(causal_rows, key=lambda item: str(item["sleeve_id"]))
    ]
    affected_sleeves = [str(row["sleeve_id"]) for row in affected_rows]

    root_causes = []
    for row in sorted(affected_rows, key=lambda item: str(item["sleeve_id"])):
        unpredictable = int(row["unpredictable_gap_count"])
        scheduled = int(row["known_session_roll_holiday_boundary_count"])
        classifications: list[str] = []
        if unpredictable:
            classifications.append("DEVELOPMENT_LOOKAHEAD_DEFECT")
        if scheduled:
            classifications.append("SESSION_BOUNDARY_MISMATCH")
        root_causes.append(
            {
                "sleeve_id": str(row["sleeve_id"]),
                "mismatch_count": int(row["mismatch_count"]),
                "primary_classification": classifications[0],
                "classifications": classifications,
                "first_causal_field": "horizon_available",
                "legacy_value_source": (
                    "np.isfinite(forward_move__holding_events)"
                ),
                "causal_reason": (
                    "legacy signal eligibility depends on the future exit row "
                    "remaining in the same contiguous segment"
                ),
                "scheduled_boundary_count": scheduled,
                "unpredictable_gap_count": unpredictable,
            }
        )

    book_mismatches = [
        {
            "candidate_id": str(row["candidate_id"]),
            "causal_signal_mismatch_count": int(
                row["causal_signal_divergence_count"]
            ),
            "timeline_affected": bool(row["timeline_affected"]),
            "account_compatibility": str(row["causal_account_status"]),
        }
        for row in sorted(
            audit["account_evidence"]["per_book"],
            key=lambda item: str(item["candidate_id"]),
        )
    ]
    package_rows = frozen["packages"]
    source_files = frozen["source_files"]
    commit = _git_head(root)
    timestamp = (created_at or datetime.now(UTC)).astimezone(UTC)

    development_fill = dict(DEVELOPMENT_FILL_MODEL)
    development_fill["fill_policy_hash"] = stable_hash(DEVELOPMENT_FILL_MODEL)
    forward_fill = dict(FORWARD_CONSERVATIVE_FILL_MODEL)
    forward_fill["fill_policy_hash"] = stable_hash(FORWARD_CONSERVATIVE_FILL_MODEL)

    receipt: dict[str, Any] = {
        "schema": F0_RECEIPT_SCHEMA,
        "status": DEVELOPMENT_EVIDENCE_CONTAMINATED,
        "operating_package_manifest_hash": frozen["operating_package_hash"],
        "created_at_utc": timestamp.isoformat().replace("+00:00", "Z"),
        "receipt_scope": (
            "FAIL_CLOSED_F0_CONTAMINATION_CLASSIFICATION;NOT_FORWARD_AUTHORIZATION"
        ),
        "authorization": {
            "online_offline_equivalence_proven": False,
            "authorization_receipt_written": False,
            "authorization_receipt_path": AUTHORIZATION_RECEIPT_PATH.as_posix(),
            "post_freeze_backlog_activation_authorized": False,
            "f1_authorized": False,
        },
        "f0_subcontracts": {
            "F0-A": {
                "name": "DEVELOPMENT_DECISION_COMPATIBILITY",
                "status": (
                    "LEGACY_COMPONENT_COMPATIBILITY_EXACT_"
                    "BUT_SCIENTIFICALLY_CONTAMINATED"
                ),
                "legacy_component_mismatch_count": 0,
                "raw_to_feature_status": "BYTE_EXACT",
                "signals": EXPECTED_LEGACY_EVENT_COUNT,
                "entries": EXPECTED_LEGACY_EVENT_COUNT,
                "exits": EXPECTED_LEGACY_EVENT_COUNT,
                "trades": EXPECTED_LEGACY_EVENT_COUNT,
                "scientific_compatibility": "FAILED_CAUSAL_AVAILABILITY",
                "authorizable": False,
            },
            "F0-B": {
                "name": "FORWARD_STREAMING_EQUIVALENCE",
                "status": "NOT_RUN_CONTAMINATED_PACKAGE",
                "authorizable": False,
            },
            "F0-C": {
                "name": "ACCOUNT_STATE_EQUIVALENCE",
                "status": "NOT_RUN_CONTAMINATED_PACKAGE",
                "authorizable": False,
            },
        },
        "fill_models": {
            DEVELOPMENT_FILL_MODEL_ID: development_fill,
            FORWARD_CONSERVATIVE_FILL_MODEL_ID: forward_fill,
            "models_required_to_match_each_other": False,
        },
        "development_contamination": {
            "found": True,
            "material": True,
            "overall_classification": "DEVELOPMENT_LOOKAHEAD_DEFECT",
            "future_outcome_field": "forward_move__holding_events",
            "future_outcome_use": (
                "signal eligibility mask requires np.isfinite(forward)"
            ),
            "unpredictable_gap_exclusion_count": EXPECTED_UNPREDICTABLE_GAP_COUNT,
            "unpredictable_gap_events": audit["causal_horizon_audit"][
                "unpredictable_gap_events"
            ],
            "root_causes": root_causes,
            "forensic_event_count": len(
                frozen["forensics"]["all_divergent_events"]
            ),
            "first_divergence_trace_count": len(
                frozen["forensics"]["first_divergence_traces"]
            ),
            "forensics_path": frozen["forensics_path"],
            "forensics_file_sha256": frozen["forensics_file_sha256"],
            "entry_availability_defect": {
                "classification": "AVAILABILITY_SEMANTICS_MISMATCH",
                "affected_legacy_entry_count": EXPECTED_LEGACY_EVENT_COUNT,
                "labeled_time": "decision_ns",
                "price_source": "groups[close].shift(-1)",
                "price_available_at": "decision_ns_plus_one_minute",
                "finding": (
                    "the legacy entry price is the next-row close while the event is "
                    "labeled at decision_ns; exact technical reproduction is not an "
                    "availability-safe forward fill"
                ),
            },
            "economic_impact": (
                "NOT_IDENTIFIABLE_WITHOUT_FABRICATING_OUTCOMES_FOR_CAUSAL_ONLY_SIGNALS"
            ),
        },
        "mismatch_counts": {
            "legacy_component_replay": 0,
            "causal_signal_total": EXPECTED_SIGNAL_DIVERGENCE_COUNT,
            "sleeves": sleeve_mismatches,
            "affected_sleeve_count": len(affected_sleeves),
            "books": book_mismatches,
            "affected_book_count": len(book_mismatches),
            "replicated_book_comparison_total": sum(
                row["causal_signal_mismatch_count"] for row in book_mismatches
            ),
            "legacy_lower_bound_total": int(audit["mismatch_count"]),
        },
        "quarantine": {
            "action": "FAIL_CLOSED_NO_FORWARD_ECONOMIC_ACTIVATION",
            "affected_sleeve_ids": sorted(affected_sleeves),
            "affected_book_ids": [row["candidate_id"] for row in book_mismatches],
            "frozen_manifests_modified": False,
            "development_evidence_deleted": False,
            "status_inheritance_allowed": False,
        },
        "technical_oracle": {
            "status": "NOT_GENERATED_CONTAMINATED_PACKAGE",
            "fabricated": False,
            "reason": (
                "a causal per-bar account oracle cannot be derived for the frozen "
                "books without choosing outcomes for the 21 causal-only signals"
            ),
        },
        "prior_restart_and_order_results": audit["guards"],
        "immutable_packages": package_rows,
        "packages": package_rows,
        "provenance": {
            "git_commit": commit,
            "audit_path": frozen["audit_path"],
            "audit_file_sha256": frozen["audit_file_sha256"],
            "audit_proof_hash": str(audit["proof_hash"]),
            "operating_manifest_path": frozen["operating_manifest_path"],
            "operating_manifest_file_sha256": frozen[
                "operating_manifest_file_sha256"
            ],
            "operating_package_hash": frozen["operating_package_hash"],
            "source_files": source_files,
            "forensics_path": frozen["forensics_path"],
            "forensics_file_sha256": frozen["forensics_file_sha256"],
        },
        "safety": {
            "post_freeze_price_records_read": 0,
            "post_freeze_backlog_records_processed": 0,
            "receipt_finalizer_market_data_purchase_usd": 0.0,
            "q4_access_count": 0,
            "broker_connections": 0,
            "orders": 0,
            "frozen_book_mutations": 0,
        },
    }
    receipt["receipt_hash"] = stable_hash(receipt)
    verify_f0_contamination_receipt(receipt, repository_root=root)
    return receipt


def write_f0_contamination_receipt(
    *,
    repository_root: str | Path,
    output_path: str | Path = DEFAULT_OUTPUT_PATH,
    audit_path: str | Path = DEFAULT_AUDIT_PATH,
    operating_manifest_path: str | Path = DEFAULT_OPERATING_MANIFEST_PATH,
    created_at: datetime | None = None,
) -> dict[str, Any]:
    """Build, verify, and atomically persist a non-authorization receipt."""

    root = Path(repository_root).resolve()
    output = _validate_output_path(root, output_path)
    receipt = build_f0_contamination_receipt(
        repository_root=root,
        audit_path=audit_path,
        operating_manifest_path=operating_manifest_path,
        created_at=created_at,
    )
    _atomic_json(output, receipt)
    verify_f0_contamination_receipt(output, repository_root=root)
    return receipt


def verify_f0_contamination_receipt(
    receipt_or_path: Mapping[str, Any] | str | Path,
    repository_root: str | Path | None = None,
    expected_package_manifest_hash: str | None = None,
    expected_package_ids: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Strictly verify receipt structure, hash, frozen inputs, and fail-closedness."""

    root = Path(repository_root).resolve() if repository_root is not None else None
    if isinstance(receipt_or_path, Mapping):
        receipt = dict(receipt_or_path)
    else:
        if root is None:
            raise F0SingleSourceParityError(
                "repository_root is required when verifying a receipt path"
            )
        path = _validate_output_path(root, receipt_or_path)
        receipt = _read_json(path)

    expected_hash = str(receipt.get("receipt_hash") or "")
    unhashed = dict(receipt)
    unhashed.pop("receipt_hash", None)
    if not expected_hash or expected_hash != stable_hash(unhashed):
        raise F0SingleSourceParityError("F0 receipt hash drift")
    if receipt.get("schema") != F0_RECEIPT_SCHEMA:
        raise F0SingleSourceParityError("F0 receipt schema drift")
    if receipt.get("status") != DEVELOPMENT_EVIDENCE_CONTAMINATED:
        raise F0SingleSourceParityError(
            "this bounded receipt may only classify development contamination"
        )
    top_level_package_hash = str(
        receipt.get("operating_package_manifest_hash") or ""
    )
    if not top_level_package_hash:
        raise F0SingleSourceParityError("operating package manifest hash is absent")
    if (
        expected_package_manifest_hash is not None
        and top_level_package_hash != expected_package_manifest_hash
    ):
        raise F0SingleSourceParityError("expected operating package hash drift")

    authorization = receipt.get("authorization") or {}
    if any(
        bool(authorization.get(field))
        for field in (
            "online_offline_equivalence_proven",
            "authorization_receipt_written",
            "post_freeze_backlog_activation_authorized",
            "f1_authorized",
        )
    ):
        raise F0SingleSourceParityError("contaminated receipt cannot authorize F0")
    if authorization.get("authorization_receipt_path") != (
        AUTHORIZATION_RECEIPT_PATH.as_posix()
    ):
        raise F0SingleSourceParityError("authorization receipt path drift")

    subcontracts = receipt.get("f0_subcontracts") or {}
    f0_a = subcontracts.get("F0-A") or {}
    if f0_a.get("status") != (
        "LEGACY_COMPONENT_COMPATIBILITY_EXACT_BUT_SCIENTIFICALLY_CONTAMINATED"
    ):
        raise F0SingleSourceParityError("F0-A contamination semantics drift")
    if int(f0_a.get("legacy_component_mismatch_count", -1)) != 0:
        raise F0SingleSourceParityError("legacy component compatibility is not exact")
    if f0_a.get("scientific_compatibility") != "FAILED_CAUSAL_AVAILABILITY":
        raise F0SingleSourceParityError("F0-A scientific failure missing")
    for name in ("F0-B", "F0-C"):
        if (subcontracts.get(name) or {}).get("status") != (
            "NOT_RUN_CONTAMINATED_PACKAGE"
        ):
            raise F0SingleSourceParityError(f"{name} must remain unrun")

    contamination = receipt.get("development_contamination") or {}
    if not contamination.get("found") or not contamination.get("material"):
        raise F0SingleSourceParityError(
            "non-contamination cannot claim DEVELOPMENT_EVIDENCE_CONTAMINATED"
        )
    if contamination.get("future_outcome_field") != "forward_move__holding_events":
        raise F0SingleSourceParityError("future-outcome dependency field drift")
    if contamination.get("overall_classification") != "DEVELOPMENT_LOOKAHEAD_DEFECT":
        raise F0SingleSourceParityError("overall contamination classification drift")
    if int(contamination.get("unpredictable_gap_exclusion_count", 0)) != (
        EXPECTED_UNPREDICTABLE_GAP_COUNT
    ):
        raise F0SingleSourceParityError("unpredictable-gap evidence drift")

    fill_models = receipt.get("fill_models") or {}
    _verify_fill_model(fill_models, DEVELOPMENT_FILL_MODEL_ID, DEVELOPMENT_FILL_MODEL)
    _verify_fill_model(
        fill_models,
        FORWARD_CONSERVATIVE_FILL_MODEL_ID,
        FORWARD_CONSERVATIVE_FILL_MODEL,
    )
    if fill_models.get("models_required_to_match_each_other") is not False:
        raise F0SingleSourceParityError("fill models were incorrectly conflated")

    mismatches = receipt.get("mismatch_counts") or {}
    sleeves = mismatches.get("sleeves") or []
    books = mismatches.get("books") or []
    if len(sleeves) != EXPECTED_SLEEVE_COUNT:
        raise F0SingleSourceParityError("receipt does not contain all 18 sleeves")
    if len({row["sleeve_id"] for row in sleeves}) != EXPECTED_SLEEVE_COUNT:
        raise F0SingleSourceParityError("duplicate sleeve mismatch row")
    if sum(int(row["causal_signal_mismatch_count"]) for row in sleeves) != (
        EXPECTED_SIGNAL_DIVERGENCE_COUNT
    ):
        raise F0SingleSourceParityError("causal sleeve mismatch total drift")
    affected_sleeves = sorted(
        row["sleeve_id"]
        for row in sleeves
        if int(row["causal_signal_mismatch_count"]) > 0
    )
    if len(affected_sleeves) != EXPECTED_AFFECTED_SLEEVE_COUNT:
        raise F0SingleSourceParityError("affected sleeve count drift")
    if len(books) != EXPECTED_BOOK_COUNT:
        raise F0SingleSourceParityError("receipt does not contain all six books")
    if any(int(row["causal_signal_mismatch_count"]) != 21 for row in books):
        raise F0SingleSourceParityError("book causal mismatch replication drift")
    if int(mismatches.get("replicated_book_comparison_total", -1)) != 126:
        raise F0SingleSourceParityError("replicated book mismatch total drift")

    quarantine = receipt.get("quarantine") or {}
    if sorted(quarantine.get("affected_sleeve_ids") or []) != affected_sleeves:
        raise F0SingleSourceParityError("affected sleeves are not fully quarantined")
    book_ids = sorted(row["candidate_id"] for row in books)
    if sorted(quarantine.get("affected_book_ids") or []) != book_ids:
        raise F0SingleSourceParityError("affected books are not fully quarantined")
    if quarantine.get("frozen_manifests_modified") is not False:
        raise F0SingleSourceParityError("frozen manifests must remain immutable")

    oracle = receipt.get("technical_oracle") or {}
    if oracle.get("status") != "NOT_GENERATED_CONTAMINATED_PACKAGE":
        raise F0SingleSourceParityError("a technical oracle was improperly claimed")
    if oracle.get("fabricated") is not False:
        raise F0SingleSourceParityError("fabricated technical oracle")
    safety = receipt.get("safety") or {}
    if any(
        int(safety.get(field, -1)) != 0
        for field in (
            "post_freeze_price_records_read",
            "post_freeze_backlog_records_processed",
            "q4_access_count",
            "broker_connections",
            "orders",
            "frozen_book_mutations",
        )
    ):
        raise F0SingleSourceParityError("safety boundary drift")
    if float(safety.get("receipt_finalizer_market_data_purchase_usd", -1.0)) != 0.0:
        raise F0SingleSourceParityError("receipt finalizer purchased market data")

    packages = receipt.get("packages") or []
    if len(packages) != EXPECTED_BOOK_COUNT:
        raise F0SingleSourceParityError("immutable package inventory drift")
    if sorted(row["candidate_id"] for row in packages) != book_ids:
        raise F0SingleSourceParityError("package/book identity drift")
    if receipt.get("immutable_packages") != packages:
        raise F0SingleSourceParityError("immutable package aliases drift")
    if expected_package_ids is not None and sorted(expected_package_ids) != book_ids:
        raise F0SingleSourceParityError("expected package identity drift")

    if root is not None:
        provenance = receipt.get("provenance") or {}
        frozen = _load_and_validate_frozen_inputs(
            root=root,
            audit_path=provenance.get("audit_path", DEFAULT_AUDIT_PATH),
            operating_manifest_path=provenance.get(
                "operating_manifest_path", DEFAULT_OPERATING_MANIFEST_PATH
            ),
        )
        _require_equal(
            provenance.get("audit_file_sha256"),
            frozen["audit_file_sha256"],
            "audit file hash",
        )
        _require_equal(
            provenance.get("audit_proof_hash"),
            frozen["audit"]["proof_hash"],
            "audit proof hash",
        )
        _require_equal(
            provenance.get("operating_manifest_file_sha256"),
            frozen["operating_manifest_file_sha256"],
            "operating manifest file hash",
        )
        _require_equal(
            provenance.get("operating_package_hash"),
            frozen["operating_package_hash"],
            "operating package hash",
        )
        _require_equal(
            top_level_package_hash,
            frozen["operating_package_hash"],
            "top-level operating package hash",
        )
        if packages != frozen["packages"]:
            raise F0SingleSourceParityError("frozen package integrity drift")
        if provenance.get("source_files") != frozen["source_files"]:
            raise F0SingleSourceParityError("source code hash drift")
        _require_equal(
            provenance.get("forensics_file_sha256"),
            frozen["forensics_file_sha256"],
            "root-cause forensics hash",
        )
        audit_sleeves = {
            str(row["sleeve_id"]): int(row["mismatch_count"])
            for row in frozen["audit"]["causal_horizon_audit"]["sleeves"]
        }
        receipt_sleeves = {
            str(row["sleeve_id"]): int(row["causal_signal_mismatch_count"])
            for row in sleeves
        }
        if receipt_sleeves != audit_sleeves:
            raise F0SingleSourceParityError("receipt/audit sleeve mismatch drift")

    return receipt


def _load_and_validate_frozen_inputs(
    *,
    root: Path,
    audit_path: str | Path,
    operating_manifest_path: str | Path,
) -> dict[str, Any]:
    audit_file = _resolve_input(root, audit_path)
    operating_file = _resolve_input(root, operating_manifest_path)
    forensics_file = _resolve_input(root, DEFAULT_FORENSICS_PATH)
    audit = _read_json(audit_file)
    operating = _read_json(operating_file)
    forensics = _read_json(forensics_file)

    if forensics.get("schema") != "hydra_f0_root_cause_forensics_v1":
        raise F0SingleSourceParityError("root-cause forensics schema drift")
    if forensics.get("global_classification") != "DEVELOPMENT_LOOKAHEAD_DEFECT":
        raise F0SingleSourceParityError("root-cause forensics classification drift")
    if len(forensics.get("all_divergent_events") or ()) != (
        EXPECTED_SIGNAL_DIVERGENCE_COUNT
    ):
        raise F0SingleSourceParityError("root-cause forensic event count drift")
    if len(forensics.get("first_divergence_traces") or ()) != (
        EXPECTED_AFFECTED_SLEEVE_COUNT
    ):
        raise F0SingleSourceParityError("first-divergence trace count drift")

    if audit.get("status") != LEGACY_FAIL_CLOSED_STATUS:
        raise F0SingleSourceParityError("source audit is not the preserved F0 failure")
    _verify_embedded_hash(audit, "proof_hash", "audit")
    if operating.get("schema") != "hydra_operating_package_v1":
        raise F0SingleSourceParityError("Operating Package V1 schema drift")
    _verify_embedded_hash(operating, "manifest_hash", "operating package")

    component = audit.get("component_evidence") or {}
    if component.get("status") != "EXACT_DETERMINISTIC_RECONCILIATION_COMPLETED":
        raise F0SingleSourceParityError("legacy component replay is not exact")
    if int(component.get("mismatch_count", -1)) != 0:
        raise F0SingleSourceParityError("legacy component mismatch is nonzero")
    for field in ("signals", "entries", "exits", "trades"):
        if int(component.get(field, -1)) != EXPECTED_LEGACY_EVENT_COUNT:
            raise F0SingleSourceParityError(f"legacy {field} count drift")

    causal = audit.get("causal_horizon_audit") or {}
    if causal.get("status") != "CAUSAL_EQUIVALENCE_FAILED":
        raise F0SingleSourceParityError("causal failure evidence is absent")
    if int(causal.get("additional_signals", -1)) != EXPECTED_SIGNAL_DIVERGENCE_COUNT:
        raise F0SingleSourceParityError("causal signal total drift")
    if int(causal.get("sleeves_affected", -1)) != EXPECTED_AFFECTED_SLEEVE_COUNT:
        raise F0SingleSourceParityError("causal affected-sleeve total drift")
    if int(causal.get("unpredictable_gap_exclusions", -1)) != (
        EXPECTED_UNPREDICTABLE_GAP_COUNT
    ):
        raise F0SingleSourceParityError("material unpredictable-gap evidence absent")
    causal_rows = causal.get("sleeves") or []
    if len(causal_rows) != EXPECTED_SLEEVE_COUNT:
        raise F0SingleSourceParityError("audit does not cover all 18 sleeves")
    if sum(int(row["mismatch_count"]) for row in causal_rows) != (
        EXPECTED_SIGNAL_DIVERGENCE_COUNT
    ):
        raise F0SingleSourceParityError("audit causal row total drift")
    if sum(int(row["unpredictable_gap_count"]) for row in causal_rows) != (
        EXPECTED_UNPREDICTABLE_GAP_COUNT
    ):
        raise F0SingleSourceParityError("audit unpredictable-gap row total drift")

    source_files = []
    for relative in _SOURCE_PATHS:
        source = _resolve_input(root, relative)
        source_files.append(
            {"path": relative.as_posix(), "sha256": sha256_file(source)}
        )
    replay_source = (root / _SOURCE_PATHS[0]).read_text(encoding="utf-8")
    feature_source = (root / _SOURCE_PATHS[1]).read_text(encoding="utf-8")
    if "np.isfinite(forward)" not in replay_source:
        raise F0SingleSourceParityError("legacy future-outcome eligibility mask changed")
    if 'groups["timestamp"].shift(-(horizon + 1))' not in feature_source:
        raise F0SingleSourceParityError("future exit timestamp derivation changed")
    if 'groups["close"].shift(-(horizon + 1))' not in feature_source:
        raise F0SingleSourceParityError("future exit price derivation changed")
    if 'entry_price = groups["close"].shift(-1)' not in feature_source:
        raise F0SingleSourceParityError("legacy entry availability derivation changed")

    audit_packages = audit.get("packages") or []
    account_rows = (audit.get("account_evidence") or {}).get("per_book") or []
    if len(audit_packages) != EXPECTED_BOOK_COUNT or len(account_rows) != (
        EXPECTED_BOOK_COUNT
    ):
        raise F0SingleSourceParityError("six-book audit inventory drift")
    packages = []
    common_sleeves: set[str] | None = None
    for audit_row in sorted(audit_packages, key=lambda item: item["candidate_id"]):
        package_file = _resolve_input(root, str(audit_row["path"]))
        package = _read_json(package_file)
        _verify_embedded_hash(package, "package_hash", "shadow package")
        candidate_id = str(package.get("candidate_id") or "")
        if candidate_id != str(audit_row["candidate_id"]):
            raise F0SingleSourceParityError("shadow package candidate identity drift")
        file_sha = sha256_file(package_file)
        _require_equal(file_sha, audit_row["file_sha256"], "shadow package file hash")
        _require_equal(
            package["package_hash"], audit_row["package_hash"], "shadow package hash"
        )
        sleeve_ids = set((package.get("signal_policy") or {}).get("signal_ledger_sha256") or {})
        if len(sleeve_ids) != EXPECTED_SLEEVE_COUNT:
            raise F0SingleSourceParityError("shadow package sleeve inventory drift")
        if common_sleeves is None:
            common_sleeves = sleeve_ids
        elif common_sleeves != sleeve_ids:
            raise F0SingleSourceParityError("six books do not share the frozen sleeves")
        packages.append(
            {
                "candidate_id": candidate_id,
                "path": _relative(root, package_file),
                "file_sha256": file_sha,
                "package_hash": str(package["package_hash"]),
                "freeze_timestamp_utc": str(package["freeze_timestamp_utc"]),
                "source_commit": str(package["source_commit"]),
                "sleeve_count": len(sleeve_ids),
            }
        )

    audit_book_ids = sorted(str(row["candidate_id"]) for row in account_rows)
    package_book_ids = sorted(row["candidate_id"] for row in packages)
    if audit_book_ids != package_book_ids:
        raise F0SingleSourceParityError("audit/package book identity drift")
    if any(
        int(row["causal_signal_divergence_count"])
        != EXPECTED_SIGNAL_DIVERGENCE_COUNT
        for row in account_rows
    ):
        raise F0SingleSourceParityError("book signal mismatch replication drift")
    integration = audit.get("integration") or {}
    _require_equal(
        integration.get("operating_package_manifest_hash"),
        operating["manifest_hash"],
        "audit/operating package hash",
    )
    _require_equal(
        integration.get("operating_package_manifest_file_sha256"),
        sha256_file(operating_file),
        "audit/operating package file hash",
    )
    if audit.get("authorization_receipt_written") is not False:
        raise F0SingleSourceParityError("legacy audit claims an authorization receipt")

    return {
        "audit": audit,
        "audit_path": _relative(root, audit_file),
        "audit_file_sha256": sha256_file(audit_file),
        "operating_manifest_path": _relative(root, operating_file),
        "operating_manifest_file_sha256": sha256_file(operating_file),
        "operating_package_hash": str(operating["manifest_hash"]),
        "forensics": forensics,
        "forensics_path": _relative(root, forensics_file),
        "forensics_file_sha256": sha256_file(forensics_file),
        "packages": packages,
        "source_files": source_files,
    }


def _verify_fill_model(
    fill_models: Mapping[str, Any], policy_id: str, expected: Mapping[str, Any]
) -> None:
    actual = dict(fill_models.get(policy_id) or {})
    policy_hash = actual.pop("fill_policy_hash", None)
    if actual != dict(expected) or policy_hash != stable_hash(expected):
        raise F0SingleSourceParityError(f"{policy_id} contract/hash drift")


def _verify_embedded_hash(
    payload: Mapping[str, Any], hash_field: str, label: str
) -> None:
    expected = str(payload.get(hash_field) or "")
    unhashed = dict(payload)
    unhashed.pop(hash_field, None)
    if not expected or expected != stable_hash(unhashed):
        raise F0SingleSourceParityError(f"{label} embedded hash drift")


def _require_equal(actual: Any, expected: Any, label: str) -> None:
    if actual != expected:
        raise F0SingleSourceParityError(f"{label} drift")


def _resolve_input(root: Path, path: str | Path) -> Path:
    candidate = Path(path)
    resolved = (candidate if candidate.is_absolute() else root / candidate).resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise F0SingleSourceParityError("input path escapes repository root") from exc
    if not resolved.is_file():
        raise F0SingleSourceParityError(f"required input is absent: {resolved}")
    return resolved


def _validate_output_path(root: Path, path: str | Path) -> Path:
    candidate = Path(path)
    resolved = (candidate if candidate.is_absolute() else root / candidate).resolve()
    allowed = (
        (root / "reports/operating").resolve(),
        (root / "mission/state").resolve(),
    )
    if not any(_is_relative_to(resolved, parent) for parent in allowed):
        raise F0SingleSourceParityError(
            "F0 receipt output must be under reports/operating or mission/state"
        )
    authorization = (root / AUTHORIZATION_RECEIPT_PATH).resolve()
    if resolved == authorization:
        raise F0SingleSourceParityError(
            "contamination finalizer may never write the authorization receipt"
        )
    if resolved == (root / DEFAULT_AUDIT_PATH).resolve():
        raise F0SingleSourceParityError("contamination receipt cannot overwrite audit")
    return resolved


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return True


def _relative(root: Path, path: Path) -> str:
    return path.resolve().relative_to(root).as_posix()


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise F0SingleSourceParityError(f"cannot read strict JSON: {path}") from exc
    if not isinstance(value, dict):
        raise F0SingleSourceParityError(f"JSON object required: {path}")
    return value


def _git_head(root: Path) -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        raise F0SingleSourceParityError("cannot resolve exact Git commit") from exc
    commit = result.stdout.strip()
    if len(commit) != 40 or any(character not in "0123456789abcdef" for character in commit):
        raise F0SingleSourceParityError("Git HEAD is not a full lowercase SHA-1")
    return commit


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(
                payload,
                handle,
                indent=2,
                sort_keys=True,
                ensure_ascii=True,
                allow_nan=False,
            )
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


__all__ = [
    "AUTHORIZATION_RECEIPT_PATH",
    "DEFAULT_AUDIT_PATH",
    "DEFAULT_OPERATING_MANIFEST_PATH",
    "DEFAULT_FORENSICS_PATH",
    "DEFAULT_OUTPUT_PATH",
    "DEVELOPMENT_EVIDENCE_CONTAMINATED",
    "DEVELOPMENT_FILL_MODEL",
    "DEVELOPMENT_FILL_MODEL_ID",
    "F0SingleSourceParityError",
    "FORWARD_CONSERVATIVE_FILL_MODEL",
    "FORWARD_CONSERVATIVE_FILL_MODEL_ID",
    "build_f0_contamination_receipt",
    "stable_hash",
    "verify_f0_contamination_receipt",
    "write_f0_contamination_receipt",
]
