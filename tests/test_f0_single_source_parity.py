from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from hydra.shadow.f0_single_source_parity import (
    AUTHORIZATION_RECEIPT_PATH,
    DEVELOPMENT_EVIDENCE_CONTAMINATED,
    F0SingleSourceParityError,
    build_f0_contamination_receipt,
    stable_hash,
    verify_f0_contamination_receipt,
    write_f0_contamination_receipt,
)


ROOT = Path(__file__).resolve().parents[1]
CREATED_AT = datetime(2026, 7, 17, 0, 0, tzinfo=UTC)


@pytest.fixture(scope="module")
def receipt() -> dict:
    return build_f0_contamination_receipt(
        repository_root=ROOT,
        created_at=CREATED_AT,
    )


def _rehash(receipt: dict) -> dict:
    changed = dict(receipt)
    changed.pop("receipt_hash", None)
    changed["receipt_hash"] = stable_hash(changed)
    return changed


def test_real_frozen_audit_and_six_packages_reconcile(receipt: dict) -> None:
    expected_ids = [row["candidate_id"] for row in receipt["packages"]]
    verified = verify_f0_contamination_receipt(
        receipt,
        ROOT,
        receipt["operating_package_manifest_hash"],
        expected_ids,
    )
    assert verified["status"] == DEVELOPMENT_EVIDENCE_CONTAMINATED
    assert len(verified["immutable_packages"]) == 6
    assert len(verified["mismatch_counts"]["sleeves"]) == 18
    assert verified["mismatch_counts"]["causal_signal_total"] == 21
    assert verified["mismatch_counts"]["replicated_book_comparison_total"] == 126
    assert verified["authorization"]["authorization_receipt_written"] is False
    assert (
        verified["development_contamination"]["entry_availability_defect"]
        ["affected_legacy_entry_count"]
        == 2_052
    )


def test_receipt_hash_is_strict(receipt: dict) -> None:
    tampered = dict(receipt)
    tampered["created_at_utc"] = "2026-07-17T00:00:01Z"
    with pytest.raises(F0SingleSourceParityError, match="receipt hash drift"):
        verify_f0_contamination_receipt(tampered)


def test_authorization_path_is_rejected_before_any_write() -> None:
    authorization = ROOT / AUTHORIZATION_RECEIPT_PATH
    existed_before = authorization.exists()
    with pytest.raises(F0SingleSourceParityError, match="never write"):
        write_f0_contamination_receipt(
            repository_root=ROOT,
            output_path=AUTHORIZATION_RECEIPT_PATH,
            created_at=CREATED_AT,
        )
    assert authorization.exists() is existed_before


def test_non_contamination_cannot_claim_terminal_status(receipt: dict) -> None:
    changed = dict(receipt)
    changed["development_contamination"] = dict(
        changed["development_contamination"], found=False
    )
    changed = _rehash(changed)
    with pytest.raises(F0SingleSourceParityError, match="non-contamination"):
        verify_f0_contamination_receipt(changed)


def test_package_and_audit_provenance_are_revalidated(receipt: dict) -> None:
    changed = dict(receipt)
    changed["provenance"] = dict(
        changed["provenance"], audit_proof_hash="0" * 64
    )
    changed = _rehash(changed)
    with pytest.raises(F0SingleSourceParityError, match="audit proof hash drift"):
        verify_f0_contamination_receipt(changed, repository_root=ROOT)

    changed = dict(receipt)
    packages = [dict(row) for row in changed["packages"]]
    packages[0]["package_hash"] = "0" * 64
    changed["packages"] = packages
    changed["immutable_packages"] = packages
    changed = _rehash(changed)
    with pytest.raises(F0SingleSourceParityError, match="package integrity drift"):
        verify_f0_contamination_receipt(changed, repository_root=ROOT)
