from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from hydra.governance.cohort_authorization import (
    CohortAuthorizationError,
    issue_cohort_authorization,
    revoke_unconsumed_authorization,
)
from hydra.governance.q4_one_shot import (
    Q4OneShotError,
    append_q4_access_once,
    audit_q4_one_shot_state,
    close_q4_transaction,
    consume_authorization_once,
    mark_q4_data_opened,
)
from hydra.promotion.final_cohort import stable_hash


def _manifest(tmp_path: Path) -> tuple[Path, dict[str, object]]:
    payload: dict[str, object] = {
        "schema": "hydra_final_q4_cohort_v4",
        "cohort_id": "cohort_test",
        "candidate_ids": ["a", "b", "c"],
        "candidate_count": 3,
        "candidates": [{"candidate_id": value} for value in ("a", "b", "c")],
        "source_commit": "a" * 40,
        "q4_access_count_before": 0,
        "q4_access_authorized": False,
    }
    payload["manifest_hash"] = stable_hash(payload)
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
    return path, payload


def test_manifest_bound_token_is_single_use_and_audited_once(tmp_path: Path) -> None:
    manifest_path, manifest = _manifest(tmp_path)
    ledger = tmp_path / "access.jsonl"
    auth_root = tmp_path / "q4"
    issued = issue_cohort_authorization(
        cohort_manifest_path=manifest_path,
        cohort_manifest_sha256=hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
        cohort_manifest_hash=str(manifest["manifest_hash"]),
        source_commit="a" * 40,
        governance_semantic_hash="b" * 64,
        governance_yaml_sha256="c" * 64,
        authorization_root=auth_root,
        access_ledger_path=ledger,
    )
    capability = consume_authorization_once(
        token=issued.token,
        authorization_path=issued.authorization_path,
        expected_authorization_hash=issued.authorization_hash,
        expected_manifest_hash=str(manifest["manifest_hash"]),
        expected_source_commit="a" * 40,
        access_ledger_path=ledger,
    )
    with pytest.raises(Q4OneShotError, match="already consumed"):
        consume_authorization_once(
            token=issued.token,
            authorization_path=issued.authorization_path,
            expected_authorization_hash=issued.authorization_hash,
            expected_manifest_hash=str(manifest["manifest_hash"]),
            expected_source_commit="a" * 40,
            access_ledger_path=ledger,
        )
    mark_q4_data_opened(capability)
    result = tmp_path / "result.json"
    result.write_text('{"complete":true}\n', encoding="utf-8")
    result_sha = hashlib.sha256(result.read_bytes()).hexdigest()
    record_hash = append_q4_access_once(
        capability,
        ledger_path=ledger,
        candidate_ids=["a", "b", "c"],
        result_bundle_sha256=result_sha,
    )
    assert append_q4_access_once(
        capability,
        ledger_path=ledger,
        candidate_ids=["a", "b", "c"],
        result_bundle_sha256=result_sha,
    ) == record_hash
    close_q4_transaction(
        capability,
        status="COMMITTED",
        result_bundle_path=str(result),
        result_bundle_sha256=result_sha,
        access_record_hash=record_hash,
    )
    audit = audit_q4_one_shot_state(
        authorization_root=auth_root, ledger_path=ledger
    )
    assert audit == {
        "valid": True,
        "status": "COMMITTED",
        "transaction_count": 1,
        "token_id": capability.token_id,
        "cohort_manifest_hash": manifest["manifest_hash"],
    }
    assert len(ledger.read_text().splitlines()) == 1


def test_wrong_manifest_commit_or_existing_access_is_rejected(tmp_path: Path) -> None:
    manifest_path, manifest = _manifest(tmp_path)
    ledger = tmp_path / "access.jsonl"
    with pytest.raises(CohortAuthorizationError, match="commit"):
        issue_cohort_authorization(
            cohort_manifest_path=manifest_path,
            cohort_manifest_sha256=hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
            cohort_manifest_hash=str(manifest["manifest_hash"]),
            source_commit="d" * 40,
            governance_semantic_hash="b" * 64,
            governance_yaml_sha256="c" * 64,
            authorization_root=tmp_path / "q4",
            access_ledger_path=ledger,
        )


def test_unconsumed_orphan_can_be_revoked_without_q4_access(tmp_path: Path) -> None:
    manifest_path, manifest = _manifest(tmp_path)
    ledger = tmp_path / "access.jsonl"
    issued = issue_cohort_authorization(
        cohort_manifest_path=manifest_path,
        cohort_manifest_sha256=hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
        cohort_manifest_hash=str(manifest["manifest_hash"]),
        source_commit="a" * 40,
        governance_semantic_hash="b" * 64,
        governance_yaml_sha256="c" * 64,
        authorization_root=tmp_path / "q4",
        access_ledger_path=ledger,
    )
    revocation = revoke_unconsumed_authorization(
        issued.authorization_path,
        reason="preflight failed before Q4 enqueue",
        access_ledger_path=ledger,
    )
    payload = json.loads(revocation.read_text())
    assert payload["q4_access_count"] == 0
    assert payload["authorization_was_consumed"] is False
    assert not ledger.exists()
    ledger.write_text(
        json.dumps(
            {
                "period_accessed": "2024-10-01:2025-01-01_EXCLUSIVE",
                "data_role": "FINAL_LOCKBOX",
            }
        )
        + "\n"
    )
    with pytest.raises(CohortAuthorizationError, match="count must be zero"):
        issue_cohort_authorization(
            cohort_manifest_path=manifest_path,
            cohort_manifest_sha256=hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
            cohort_manifest_hash=str(manifest["manifest_hash"]),
            source_commit="a" * 40,
            governance_semantic_hash="b" * 64,
            governance_yaml_sha256="c" * 64,
            authorization_root=tmp_path / "q4",
            access_ledger_path=ledger,
        )
