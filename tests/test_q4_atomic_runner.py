from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path

import pytest

from hydra.governance.cohort_authorization import issue_cohort_authorization
from hydra.governance.invariants import q4_access_count
from hydra.governance.q4_one_shot import audit_q4_one_shot_state
from hydra.promotion.final_cohort import stable_hash
from hydra.validation.promotion_contract import paper_shadow_ready_after_q4
from hydra.validation.q4_atomic_runner import (
    Q4AtomicRunnerError,
    classify_role_specific_q4_result,
    run_q4_atomic_one_shot,
)


POLICY = {
    "minimum_executable_events": 5,
    "maximum_best_day_positive_pnl_fraction": 0.50,
    "minimum_xfa_qualifying_days": 2,
    "maximum_defensive_target_velocity_loss_fraction": 0.25,
    "maximum_defensive_matched_control_probability": 0.10,
    "minimum_defensive_control_count": 32,
}


def _manifest(tmp_path: Path) -> tuple[Path, dict[str, object]]:
    candidates = [
        {"candidate_id": "alpha", "role": "COMBINE_PASSER"},
        {"candidate_id": "payout", "role": "XFA_PAYOUT"},
        {"candidate_id": "defensive", "role": "DEFENSIVE"},
    ]
    payload: dict[str, object] = {
        "schema": "hydra_final_q4_cohort_v4",
        "cohort_id": "cohort_atomic_test",
        "candidate_ids": [row["candidate_id"] for row in candidates],
        "candidate_count": 3,
        "candidates": candidates,
        "source_commit": "a" * 40,
        "q4_period": ["2024-10-01", "2025-01-01"],
        "q4_decision_policy": POLICY,
        "q4_access_count_before": 0,
        "q4_access_authorized": False,
    }
    payload["manifest_hash"] = stable_hash(payload)
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
    return path, payload


def _database(path: Path) -> None:
    connection = sqlite3.connect(path)
    connection.execute("create table sentinel(value integer)")
    connection.commit()
    connection.close()


def _issued(tmp_path: Path) -> tuple[Path, dict[str, object], object, Path, Path]:
    manifest_path, manifest = _manifest(tmp_path)
    ledger = tmp_path / "access.jsonl"
    auth_root = tmp_path / "authorization"
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
    return manifest_path, manifest, issued, ledger, auth_root


def test_role_specific_decisions_are_frozen_and_not_universal() -> None:
    alpha = classify_role_specific_q4_result(
        {"role": "COMBINE_PASSER"},
        {
            "events": 8,
            "net_pnl": 500.0,
            "target_progress": 0.05,
            "mll_breached": False,
            "best_day_positive_pnl_fraction": 0.30,
        },
        POLICY,
    )
    assert alpha["classification"] == "Q4_LOCKBOX_PASS"
    payout = classify_role_specific_q4_result(
        {"role": "XFA_PAYOUT"},
        {
            "events": 8,
            "net_pnl": 300.0,
            "mll_breached": False,
            "qualifying_days": 1,
            "best_day_positive_pnl_fraction": 0.30,
        },
        POLICY,
    )
    assert payout["classification"] == "Q4_LOCKBOX_FAIL"
    defensive = classify_role_specific_q4_result(
        {"role": "DEFENSIVE"},
        {
            "events": 5,
            "account_utility": {
                "control_count": 63,
                "maximum_drawdown_reduction": 100.0,
                "matched_control_probability": 0.08,
                "target_velocity_loss_fraction": 0.10,
                "hard_risk_violation": False,
            },
        },
        POLICY,
    )
    assert defensive["classification"] == "Q4_LOCKBOX_PASS"
    insufficient = classify_role_specific_q4_result(
        {"role": "COMBINE_PASSER"}, {"events": 4}, POLICY
    )
    assert insufficient["classification"] == "Q4_LOCKBOX_INSUFFICIENT"
    assert paper_shadow_ready_after_q4(
        q4_classification="Q4_LOCKBOX_PASS",
        shadow_package_complete=True,
        hard_integrity_issue=False,
        deterministic_forward_features=True,
        fail_closed_virtual_execution=True,
        broker_or_order_capability=False,
    )
    assert not paper_shadow_ready_after_q4(
        q4_classification="Q4_LOCKBOX_PASS",
        shadow_package_complete=True,
        hard_integrity_issue=False,
        deterministic_forward_features=True,
        fail_closed_virtual_execution=True,
        broker_or_order_capability=True,
    )


def test_atomic_runner_commits_all_candidates_and_access_exactly_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest_path, manifest, issued, ledger, auth_root = _issued(tmp_path)
    mission_db = tmp_path / "mission.db"
    registry_db = tmp_path / "registry.db"
    _database(mission_db)
    _database(registry_db)
    monkeypatch.setattr(
        "subprocess.check_output", lambda *args, **kwargs: "a" * 40 + "\n"
    )

    def evaluator(manifest_value: dict[str, object], capability: object):
        capability.validate_scope()
        return [
            {
                "candidate_id": candidate_id,
                "classification": "Q4_LOCKBOX_PASS",
                "reasons": [],
                "metrics": {"events": 10, "net_pnl": 100.0},
            }
            for candidate_id in manifest_value["candidate_ids"]
        ]

    result = run_q4_atomic_one_shot(
        tmp_path / "output",
        cohort_manifest_path=manifest_path,
        cohort_manifest_sha256=hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
        cohort_manifest_hash=str(manifest["manifest_hash"]),
        authorization_path=issued.authorization_path,
        authorization_hash=issued.authorization_hash,
        authorization_token=issued.token,
        code_commit="a" * 40,
        mission_db_path=mission_db,
        registry_db_path=registry_db,
        access_ledger_path=ledger,
        evaluator=evaluator,
    )
    assert result["status_counts"]["Q4_LOCKBOX_PASS"] == 3
    assert result["paper_shadow_ready_candidate_ids"] == ["alpha", "defensive", "payout"]
    assert q4_access_count(str(ledger)) == 1
    assert len(ledger.read_text().splitlines()) == 1
    assert audit_q4_one_shot_state(
        authorization_root=auth_root, ledger_path=ledger
    )["valid"]


def test_partial_scientific_result_is_quarantined_and_never_retried(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest_path, manifest, issued, ledger, auth_root = _issued(tmp_path)
    mission_db = tmp_path / "mission.db"
    registry_db = tmp_path / "registry.db"
    _database(mission_db)
    _database(registry_db)
    monkeypatch.setattr(
        "subprocess.check_output", lambda *args, **kwargs: "a" * 40 + "\n"
    )

    def incomplete(_manifest: object, _capability: object):
        return [
            {
                "candidate_id": "alpha",
                "classification": "Q4_LOCKBOX_PASS",
                "reasons": [],
                "metrics": {"events": 10},
            }
        ]

    with pytest.raises(Q4AtomicRunnerError, match="incomplete"):
        run_q4_atomic_one_shot(
            tmp_path / "output",
            cohort_manifest_path=manifest_path,
            cohort_manifest_sha256=hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
            cohort_manifest_hash=str(manifest["manifest_hash"]),
            authorization_path=issued.authorization_path,
            authorization_hash=issued.authorization_hash,
            authorization_token=issued.token,
            code_commit="a" * 40,
            mission_db_path=mission_db,
            registry_db_path=registry_db,
            access_ledger_path=ledger,
            evaluator=incomplete,
        )
    assert q4_access_count(str(ledger)) == 1
    audit = audit_q4_one_shot_state(
        authorization_root=auth_root, ledger_path=ledger
    )
    assert audit["valid"] and audit["status"] == "Q4_REVIEW_REQUIRED"
    assert list((tmp_path / "output").glob("q4_quarantine_*"))
