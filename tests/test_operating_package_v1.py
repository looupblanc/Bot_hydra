from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from hydra.operating.package_v1 import (
    ALL_BOOK_IDS,
    ROLE_IDS,
    ROLE_RULE_VERSION,
    OperatingPackageError,
    _post_payout_per_attempt,
    stable_hash,
    validate_operating_package_v1,
    verify_operating_package_seal,
)


def _minimal_package() -> dict:
    profile_summary = {
        scenario: {
            "expected_trader_net_payout_per_transition": 100.0,
            "first_payout_rate": 0.5,
            "probability_at_least_two_payouts": 0.25,
            "payout_cycles_per_transition": 2.0,
        }
        for scenario in ("normal", "stressed_1_5x")
    }
    books = [
        {
            "policy_id": policy_id,
            "selected_xfa_path": "CONSISTENCY",
            "outbound_order_capability": False,
            "broker_connectivity": False,
            "selected_post_payout_profile": {
                "book_id": policy_id,
                "path": "XFA_CONSISTENCY",
                "summary": profile_summary,
                "derived_per_new_combine_attempt": {
                    scenario: _post_payout_per_attempt(
                        summary,
                        combine_pass_rate=0.25,
                        combine_denominator=192,
                    )
                    for scenario, summary in profile_summary.items()
                },
            },
            "combine_evidence": {
                "normal": {"starts": 192, "pass_rate": 0.25},
                "stressed_1_5x": {"starts": 192, "pass_rate": 0.25},
            },
            "xfa_path_comparison_stressed": {
                "paths_are_alternative_not_additive": True,
                "standard": {
                    "expected_trader_payout_per_new_combine_attempt_usd": 1.0,
                },
                "consistency": {
                    "expected_trader_payout_per_new_combine_attempt_usd": 2.0,
                },
            },
        }
        for policy_id in ALL_BOOK_IDS
    ]
    payload = {
        "schema": "hydra_operating_package_v1",
        "package_version": 1,
        "research_only": True,
        "development_selected": True,
        "independently_confirmed": False,
        "paper_shadow_ready": False,
        "broker_connections": 0,
        "outbound_orders": 0,
        "outbound_order_capability": False,
        "q4_access_authorized": False,
        "book_semantics": {
            "complete_books_are_alternatives": True,
            "complete_books_may_be_stacked_on_one_account": False,
            "eventual_account_uses_exactly_one_frozen_book": True,
            "new_simulation_required_before_any_book_merge": True,
        },
        "books": books,
        "role_rule": {
            "version": ROLE_RULE_VERSION,
            "assignments": [
                {"role": key, "policy_id": value}
                for key, value in ROLE_IDS.items()
            ]
        },
        "canonical_sleeve_inventory": {
            "sleeve_count": 18,
            "shared_by_all_six_books": True,
        },
        "redundancy_audit": _redundancy_audit(),
        "data_acquisition_authorization": {
            "authorization_basis": (
                "DIRECT_USER_DIRECTIVE_HYDRA_OPERATING_PACKAGE_V1_2026_07_16"
            ),
            "maximum_total_incremental_spend_usd": 10.0,
            "minimum_remaining_budget_reserve_usd": 25.0,
            "broad_historical_purchase_authorized": False,
            "q4_access_authorized": False,
            "broker_or_order_authorized": False,
        },
    }
    payload["manifest_hash"] = stable_hash(payload)
    return payload


def _redundancy_audit() -> dict:
    derived = {
        ROLE_IDS["CORE_BOOK"]: "CORE",
        ROLE_IDS["SAFETY_BOOK"]: "SAFETY_BUFFER",
        ROLE_IDS["DIVERSIFIER_BOOK"]: "DIVERSIFIER_GOVERNOR_RELATIVE",
        ROLE_IDS["BACKUP_BOOK"]: "BACKUP_DIVERSE",
    }
    audit = {
        "schema": "hydra_operating_redundancy_audit_v1",
        "pairwise_redundancy": [{"pair": index} for index in range(15)],
        "inventory": {
            "sleeve_count": 18,
            "shared_by_all_six_books": True,
        },
        "roles": [
            {
                "policy_id": policy_id,
                "derived_operating_role": derived.get(policy_id, "RESERVE"),
            }
            for policy_id in ALL_BOOK_IDS
        ],
    }
    audit["audit_hash"] = stable_hash(audit)
    return audit


def test_operating_package_keeps_books_alternative_and_xfa_nonadditive() -> None:
    validate_operating_package_v1(_minimal_package())


def test_operating_package_rejects_book_stacking() -> None:
    payload = _minimal_package()
    payload["book_semantics"]["complete_books_may_be_stacked_on_one_account"] = True
    payload["manifest_hash"] = stable_hash(
        {key: value for key, value in payload.items() if key != "manifest_hash"}
    )
    with pytest.raises(OperatingPackageError, match="stacking"):
        validate_operating_package_v1(payload)


def test_operating_package_rejects_additive_xfa_paths() -> None:
    payload = copy.deepcopy(_minimal_package())
    payload["books"][0]["xfa_path_comparison_stressed"][
        "paths_are_alternative_not_additive"
    ] = False
    payload["manifest_hash"] = stable_hash(
        {key: value for key, value in payload.items() if key != "manifest_hash"}
    )
    with pytest.raises(OperatingPackageError, match="additive"):
        validate_operating_package_v1(payload)


def test_operating_package_rejects_forward_validation_inheritance() -> None:
    payload = _minimal_package()
    payload["paper_shadow_ready"] = True
    payload["manifest_hash"] = stable_hash(
        {key: value for key, value in payload.items() if key != "manifest_hash"}
    )
    with pytest.raises(OperatingPackageError, match="validation"):
        validate_operating_package_v1(payload)


def test_operating_package_receipt_is_required_commit_marker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "reports/operating/hydra_operating_package_v1"
    output.mkdir(parents=True)
    manifest = _minimal_package()
    manifest["source_commit"] = "a" * 40
    manifest["manifest_hash"] = stable_hash(
        {key: value for key, value in manifest.items() if key != "manifest_hash"}
    )
    manifest_path = output / "OPERATING_PACKAGE_V1.json"
    report_path = output / "OPERATING_PACKAGE_V1.md"
    manifest_path.write_text(
        json.dumps(manifest, sort_keys=True, indent=2) + "\n", encoding="utf-8"
    )
    report_path.write_text("# sealed\n", encoding="utf-8")
    with pytest.raises(OperatingPackageError):
        verify_operating_package_seal(output, project_root=tmp_path)

    import hashlib

    def digest(path: Path) -> str:
        return hashlib.sha256(path.read_bytes()).hexdigest()

    receipt = {
        "schema": "hydra_operating_package_v1_seal_receipt",
        "manifest_sha256": digest(manifest_path),
        "manifest_hash": manifest["manifest_hash"],
        "report_sha256": digest(report_path),
        "source_commit": manifest["source_commit"],
        "artifacts": {
            "OPERATING_PACKAGE_V1.json": {
                "relative_path": "OPERATING_PACKAGE_V1.json",
                "sha256": digest(manifest_path),
                "size_bytes": manifest_path.stat().st_size,
            },
            "OPERATING_PACKAGE_V1.md": {
                "relative_path": "OPERATING_PACKAGE_V1.md",
                "sha256": digest(report_path),
                "size_bytes": report_path.stat().st_size,
            },
        },
        "publication_contract": {
            "manifest_and_report_written_before_receipt": True,
            "receipt_is_commit_marker": True,
            "immutable": True,
        },
    }
    receipt["receipt_hash"] = stable_hash(receipt)
    (output / "OPERATING_PACKAGE_V1_seal_receipt.json").write_text(
        json.dumps(receipt, sort_keys=True, indent=2) + "\n", encoding="utf-8"
    )
    monkeypatch.setattr(
        "hydra.operating.package_v1._validate_bound_sources",
        lambda *_args, **_kwargs: None,
    )

    assert verify_operating_package_seal(
        output, project_root=tmp_path
    )["receipt_hash"] == receipt["receipt_hash"]
