from __future__ import annotations

import unittest

from hydra.factory.reward_model import policy_allocation_caps, promotion_aligned_reward
from hydra.validation.status_provenance import (
    FULL,
    INHERITED_INVALID,
    PROXY,
    VALIDATION_VERSION,
    build_validation_provenance,
    is_full_status_usable,
    reject_stale_status,
)


class StatusProvenanceAndRewardTests(unittest.TestCase):
    def test_none_or_unknown_status_never_becomes_full_pass(self) -> None:
        self.assertFalse(is_full_status_usable({}, "abc"))
        provenance = build_validation_provenance(input_fingerprint="abc", gate_modes={"OOS": INHERITED_INVALID})
        self.assertFalse(is_full_status_usable(provenance.to_dict(), "abc"))

    def test_stale_cached_validation_is_rejected_after_input_change(self) -> None:
        provenance = build_validation_provenance(input_fingerprint="old", gate_modes={"OOS": FULL, "MONTE_CARLO": FULL})
        self.assertTrue(is_full_status_usable(provenance.to_dict(), "old"))
        self.assertTrue(reject_stale_status(provenance.to_dict(), "new"))

    def test_proxy_evidence_cannot_support_final_promotion(self) -> None:
        provenance = build_validation_provenance(input_fingerprint="abc", gate_modes={"OOS": FULL, "CORRELATION": PROXY})
        self.assertEqual(provenance.computation_mode, PROXY)
        self.assertFalse(is_full_status_usable(provenance.to_dict(), "abc"))

    def test_expensive_status_records_version_and_input_hash(self) -> None:
        provenance = build_validation_provenance(input_fingerprint="abc", gate_modes={"OOS": FULL, "MONTE_CARLO": FULL})
        self.assertEqual(provenance.validation_version, VALIDATION_VERSION)
        self.assertEqual(provenance.input_fingerprint, "abc")
        self.assertTrue(provenance.computed_at)

    def test_promotion_stage_advances_dominate_cheap_local_score_changes(self) -> None:
        parent = {"promotion_score": 0.50, "promotion_stage": "WALK_FORWARD_PASSED", "gate_history": [{"name": "OOS", "passed": False}]}
        cheap = {"promotion_score": 0.58, "promotion_stage": "WALK_FORWARD_PASSED", "gate_history": [{"name": "OOS", "passed": False}]}
        advanced = {"promotion_score": 0.51, "promotion_stage": "OOS_PASSED", "gate_history": [{"name": "OOS", "passed": True}]}
        cheap_reward = promotion_aligned_reward(parent, cheap).total
        advanced_reward = promotion_aligned_reward(parent, advanced).total
        self.assertGreater(advanced_reward, cheap_reward)

    def test_reward_sign_cannot_be_inverted_for_degraded_child(self) -> None:
        parent = {"promotion_score": 0.70, "gate_history": []}
        child = {"promotion_score": 0.50, "gate_history": []}
        reward = promotion_aligned_reward(parent, child)
        self.assertEqual(reward.local_improvement, 0.0)

    def test_zero_oos_passes_cannot_be_rewarded_as_oos_success(self) -> None:
        parent = {"promotion_score": 0.50, "gate_history": [{"name": "OOS", "passed": False}]}
        child = {"promotion_score": 0.60, "gate_history": [{"name": "OOS", "passed": False}]}
        reward = promotion_aligned_reward(parent, child)
        self.assertEqual(reward.oos_pass, 0.0)

    def test_allocation_caps_are_conservative_before_oos_or_q2_progress(self) -> None:
        caps = policy_allocation_caps()
        self.assertLessEqual(caps["max_policy_share"], 0.35)
        self.assertLessEqual(caps["max_family_share"], 0.30)
        self.assertLessEqual(caps["max_lineage_share"], 0.02)
        self.assertGreaterEqual(caps["minimum_controlled_exploration_share"], 0.15)


if __name__ == "__main__":
    unittest.main()
