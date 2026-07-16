from __future__ import annotations

import json
from pathlib import Path

import pytest

from hydra.production.active_risk_report_seal import (
    ActiveRiskReportSealError,
    REPORT_JSON_NAME,
    REPORT_MARKDOWN_NAME,
    REPORT_RECEIPT_NAME,
    seal_active_risk_decision_report,
    verify_active_risk_decision_report_seal,
)
from hydra.production.active_risk_decision_report import canonical_hash


def _report() -> dict[str, object]:
    value: dict[str, object] = {
        "schema_version": "hydra_active_risk_decision_report_v1",
        "report_revision": "revision_02",
        "campaign_id": "hydra_active_risk_pool_target_velocity_0026",
        "payload": {"finalists": 8},
    }
    value["report_hash"] = canonical_hash(value)
    return value


def test_seal_publishes_receipt_last_and_verifies(tmp_path: Path) -> None:
    report = _report()
    markdown = f"# Decision report\n\nReport hash: `{report['report_hash']}`\n"
    receipt = seal_active_risk_decision_report(
        report,
        markdown_text=markdown,
        output_dir=tmp_path,
    )

    assert receipt["publication_contract"]["receipt_is_commit_marker"] is True
    assert receipt["sealed_at_utc"].endswith("Z")
    assert set(receipt["artifacts"]) == {REPORT_JSON_NAME, REPORT_MARKDOWN_NAME}
    assert (tmp_path / REPORT_RECEIPT_NAME).is_file()
    assert verify_active_risk_decision_report_seal(tmp_path) == receipt

    # Identical reruns are idempotent, while the receipt remains immutable.
    assert (
        seal_active_risk_decision_report(
            report,
            markdown_text=markdown,
            output_dir=tmp_path,
        )["receipt_hash"]
        == receipt["receipt_hash"]
    )


@pytest.mark.parametrize("artifact", [REPORT_JSON_NAME, REPORT_MARKDOWN_NAME])
def test_verify_rejects_artifact_tampering(tmp_path: Path, artifact: str) -> None:
    report = _report()
    seal_active_risk_decision_report(
        report,
        markdown_text=f"Report hash: {report['report_hash']}\n",
        output_dir=tmp_path,
    )
    (tmp_path / artifact).write_bytes(b"tampered\n")

    with pytest.raises(ActiveRiskReportSealError, match="mismatch"):
        verify_active_risk_decision_report_seal(tmp_path)


def test_verify_rejects_receipt_tampering(tmp_path: Path) -> None:
    report = _report()
    seal_active_risk_decision_report(
        report,
        markdown_text=f"Report hash: {report['report_hash']}\n",
        output_dir=tmp_path,
    )
    receipt_path = tmp_path / REPORT_RECEIPT_NAME
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt["campaign_id"] = "tampered"
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")

    with pytest.raises(ActiveRiskReportSealError, match="receipt hash mismatch"):
        verify_active_risk_decision_report_seal(tmp_path)


def test_seal_rejects_unbound_report_or_markdown(tmp_path: Path) -> None:
    report = _report()
    report["payload"] = {"finalists": 7}
    with pytest.raises(ActiveRiskReportSealError, match="report_hash"):
        seal_active_risk_decision_report(
            report,
            markdown_text="missing hash",
            output_dir=tmp_path,
        )

    valid = _report()
    with pytest.raises(ActiveRiskReportSealError, match="Markdown"):
        seal_active_risk_decision_report(
            valid,
            markdown_text="missing hash",
            output_dir=tmp_path,
        )
