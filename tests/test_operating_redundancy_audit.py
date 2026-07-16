from __future__ import annotations

import copy

import pytest

from hydra.economic_evolution.schema import stable_hash
from scripts.build_operating_redundancy_audit import (
    FROZEN_BOOK_IDS,
    ROLE_BY_BOOK,
    SCHEMA,
    RedundancyAuditError,
    validate_redundancy_audit,
)


def _minimal_audit() -> dict:
    pairs = [
        {
            "left_policy_id": left,
            "right_policy_id": right,
            "membership_jaccard": 1.0,
        }
        for left_index, left in enumerate(FROZEN_BOOK_IDS)
        for right in FROZEN_BOOK_IDS[left_index + 1 :]
    ]
    payload = {
        "schema": SCHEMA,
        "pairwise_redundancy": pairs,
        "roles": [
            {"policy_id": policy_id, "derived_operating_role": role}
            for policy_id, role in ROLE_BY_BOOK.items()
        ],
    }
    payload["audit_hash"] = stable_hash(payload)
    return payload


def test_redundancy_audit_freezes_15_pairs_membership_roles_and_hash() -> None:
    payload = _minimal_audit()
    validate_redundancy_audit(payload)
    assert len(payload["pairwise_redundancy"]) == 15
    assert all(row["membership_jaccard"] == 1.0 for row in payload["pairwise_redundancy"])
    assert {row["policy_id"]: row["derived_operating_role"] for row in payload["roles"]} == ROLE_BY_BOOK
    unhashed = dict(payload)
    assert unhashed.pop("audit_hash") == stable_hash(unhashed)


def test_redundancy_audit_rejects_content_drift() -> None:
    payload = copy.deepcopy(_minimal_audit())
    payload["pairwise_redundancy"][0]["membership_jaccard"] = 0.99
    payload["audit_hash"] = stable_hash(
        {key: value for key, value in payload.items() if key != "audit_hash"}
    )
    with pytest.raises(RedundancyAuditError, match="membership"):
        validate_redundancy_audit(payload)
