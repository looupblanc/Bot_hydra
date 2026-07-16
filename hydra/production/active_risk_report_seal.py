"""Atomic commit marker for the campaign-0026 decision report.

The JSON and Markdown artifacts are individually atomic.  A hash-bound receipt
is published last and is the only commit marker for consumers.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from hydra.compute.result_writer import AtomicResultWriter


REPORT_JSON_NAME = "decision_report_revision_02.json"
REPORT_MARKDOWN_NAME = "decision_report_revision_02.md"
REPORT_RECEIPT_NAME = "decision_report_revision_02_seal_receipt.json"
SEAL_SCHEMA = "hydra_active_risk_decision_report_seal_v1"


class ActiveRiskReportSealError(RuntimeError):
    """Raised when an atomic report seal is incomplete or divergent."""


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _validate_utc_timestamp(value: Any, *, label: str) -> str:
    text = str(value or "")
    if not text.endswith("Z"):
        raise ActiveRiskReportSealError(f"{label} must be an ISO-8601 UTC timestamp")
    try:
        parsed = datetime.fromisoformat(text[:-1] + "+00:00")
    except ValueError as exc:
        raise ActiveRiskReportSealError(
            f"{label} must be an ISO-8601 UTC timestamp"
        ) from exc
    if parsed.utcoffset() is None or parsed.utcoffset().total_seconds() != 0:
        raise ActiveRiskReportSealError(f"{label} is not UTC")
    return text


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _canonical_hash(value: Any) -> str:
    return _sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("utf-8")
    )


def _validate_report_hash(report: Mapping[str, Any]) -> str:
    claimed = str(report.get("report_hash") or "")
    if not claimed:
        raise ActiveRiskReportSealError("report_hash is required")
    unhashed = dict(report)
    unhashed.pop("report_hash", None)
    if _canonical_hash(unhashed) != claimed:
        raise ActiveRiskReportSealError("report_hash does not bind report content")
    return claimed


def seal_active_risk_decision_report(
    report: Mapping[str, Any],
    *,
    markdown_text: str,
    output_dir: str | Path,
) -> Mapping[str, Any]:
    """Publish report artifacts and their final receipt without partial success."""

    claimed_report_hash = _validate_report_hash(report)
    root = Path(output_dir).resolve()
    json_payload = (
        json.dumps(
            report,
            sort_keys=True,
            indent=2,
            ensure_ascii=True,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")
    markdown_payload = markdown_text.encode("utf-8")
    if claimed_report_hash not in markdown_text:
        raise ActiveRiskReportSealError("Markdown does not identify the sealed report hash")

    artifacts = {
        REPORT_JSON_NAME: json_payload,
        REPORT_MARKDOWN_NAME: markdown_payload,
    }
    receipt_path = root / REPORT_RECEIPT_NAME
    if receipt_path.is_file():
        receipt = verify_active_risk_decision_report_seal(root)
        if any((root / name).read_bytes() != payload for name, payload in artifacts.items()):
            raise ActiveRiskReportSealError(
                "existing sealed report differs from requested immutable artifacts"
            )
        return receipt
    artifact_manifest = {
        relative_path: {
            "relative_path": relative_path,
            "sha256": _sha256(payload),
            "size_bytes": len(payload),
        }
        for relative_path, payload in artifacts.items()
    }
    receipt_body: dict[str, Any] = {
        "schema_version": SEAL_SCHEMA,
        "campaign_id": str(report.get("campaign_id") or ""),
        "report_schema": str(
            report.get("schema_version") or report.get("schema") or ""
        ),
        "report_revision": str(
            report.get("report_revision") or report.get("revision") or ""
        ),
        "report_hash": claimed_report_hash,
        "sealed_at_utc": _utc_now(),
        "artifacts": artifact_manifest,
        "publication_contract": {
            "artifacts_written_atomically_before_receipt": True,
            "receipt_is_commit_marker": True,
            "immutable": True,
        },
    }
    receipt = dict(receipt_body)
    receipt["receipt_hash"] = _canonical_hash(receipt_body)

    writer = AtomicResultWriter(root, immutable=True)
    writer.write_batch(artifacts)
    writer.write_json(REPORT_RECEIPT_NAME, receipt)
    return verify_active_risk_decision_report_seal(root)


def verify_active_risk_decision_report_seal(
    output_dir: str | Path,
) -> Mapping[str, Any]:
    """Verify the final receipt, both artifacts, and the embedded report hash."""

    root = Path(output_dir).resolve()
    receipt_path = root / REPORT_RECEIPT_NAME
    if not receipt_path.is_file():
        raise ActiveRiskReportSealError("decision-report seal receipt is absent")
    try:
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ActiveRiskReportSealError("decision-report seal receipt is unreadable") from exc
    if not isinstance(receipt, dict):
        raise ActiveRiskReportSealError("decision-report seal receipt is malformed")
    claimed_receipt_hash = str(receipt.get("receipt_hash") or "")
    receipt_body = dict(receipt)
    receipt_body.pop("receipt_hash", None)
    if not claimed_receipt_hash or _canonical_hash(receipt_body) != claimed_receipt_hash:
        raise ActiveRiskReportSealError("decision-report receipt hash mismatch")
    if receipt.get("schema_version") != SEAL_SCHEMA:
        raise ActiveRiskReportSealError("decision-report seal schema mismatch")
    _validate_utc_timestamp(
        receipt.get("sealed_at_utc"), label="decision-report sealed_at_utc"
    )

    artifacts = receipt.get("artifacts")
    if not isinstance(artifacts, Mapping) or set(artifacts) != {
        REPORT_JSON_NAME,
        REPORT_MARKDOWN_NAME,
    }:
        raise ActiveRiskReportSealError("decision-report artifact manifest is incomplete")
    for relative_path, raw_metadata in artifacts.items():
        if not isinstance(raw_metadata, Mapping):
            raise ActiveRiskReportSealError("decision-report artifact metadata is malformed")
        if raw_metadata.get("relative_path") != relative_path:
            raise ActiveRiskReportSealError("decision-report artifact path binding mismatch")
        artifact_path = (root / relative_path).resolve()
        try:
            artifact_path.relative_to(root)
        except ValueError as exc:
            raise ActiveRiskReportSealError("decision-report artifact escapes report root") from exc
        if not artifact_path.is_file():
            raise ActiveRiskReportSealError(f"missing decision-report artifact: {relative_path}")
        payload = artifact_path.read_bytes()
        if len(payload) != int(raw_metadata.get("size_bytes", -1)):
            raise ActiveRiskReportSealError(f"size mismatch for {relative_path}")
        if _sha256(payload) != str(raw_metadata.get("sha256") or ""):
            raise ActiveRiskReportSealError(f"hash mismatch for {relative_path}")

    try:
        report = json.loads((root / REPORT_JSON_NAME).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ActiveRiskReportSealError("sealed report JSON is unreadable") from exc
    if not isinstance(report, dict):
        raise ActiveRiskReportSealError("sealed report JSON is malformed")
    if _validate_report_hash(report) != receipt.get("report_hash"):
        raise ActiveRiskReportSealError("receipt and report hash bindings diverge")
    markdown = (root / REPORT_MARKDOWN_NAME).read_text(encoding="utf-8")
    if str(receipt["report_hash"]) not in markdown:
        raise ActiveRiskReportSealError("sealed Markdown omits the report hash")
    return receipt


__all__ = [
    "ActiveRiskReportSealError",
    "REPORT_JSON_NAME",
    "REPORT_MARKDOWN_NAME",
    "REPORT_RECEIPT_NAME",
    "SEAL_SCHEMA",
    "seal_active_risk_decision_report",
    "verify_active_risk_decision_report_seal",
]
