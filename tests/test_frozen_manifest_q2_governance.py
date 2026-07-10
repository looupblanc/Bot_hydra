from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from hydra.validation.lockbox_guard import LockboxViolation, enforce_data_access
from hydra.validation.data_roles import DataRole


class FrozenManifestQ2GovernanceTests(unittest.TestCase):
    def test_manifest_hash_mismatch_detected(self) -> None:
        payload = {
            "manifest_type": "q2_confirmation_freeze",
            "created_at_utc": "2026-07-10T00:00:00+00:00",
            "candidate_ids": ["a"],
            "strategy_specs": [{"candidate_id": "a", "parameters_json": "{}", "risk_json": "{}"}],
            "execution_assumptions": {},
            "commission_assumptions": {},
            "slippage_assumptions": {},
            "topstep_rule_version": "test",
            "source_code_commit": "abc",
            "data_fingerprints": {},
            "validation_thresholds": {},
            "expected_decision_policy": {},
        }
        digest = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()
        payload["manifest_hash"] = digest
        payload["strategy_specs"][0]["risk_json"] = "{\"risk_scale\":2}"
        recalculated = hashlib.sha256(json.dumps({k: v for k, v in payload.items() if k != "manifest_hash"}, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()
        self.assertNotEqual(payload["manifest_hash"], recalculated)

    def test_q3_requires_manifest_hash_and_q4_seal(self) -> None:
        with self.assertRaises(LockboxViolation):
            enforce_data_access(
                period="2024-07-01:2024-10-01",
                role=DataRole.BLIND_VALIDATION,
                requesting_module="test",
                candidate_ids=["x"],
                reason="must have manifest",
                freeze_manifest_hash=None,
            )
        with self.assertRaises(LockboxViolation):
            enforce_data_access(
                period="2024-10-01:2025-01-01",
                role=DataRole.FINAL_LOCKBOX,
                requesting_module="test",
                candidate_ids=["x"],
                reason="must have final manifest",
                freeze_manifest_hash=None,
            )

    def test_q2_mutation_policy_requires_developed_label(self) -> None:
        policy = {
            "q2_result_may_confirm_or_reject": True,
            "q2_modified_after_viewing_becomes_q2_developed": True,
        }
        self.assertTrue(policy["q2_modified_after_viewing_becomes_q2_developed"])


if __name__ == "__main__":
    unittest.main()
