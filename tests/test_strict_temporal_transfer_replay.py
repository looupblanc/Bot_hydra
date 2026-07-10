from __future__ import annotations

import unittest

from hydra.validation.replay_manifest import FrozenReplayCandidate, assert_same_candidate_spec, build_replay_manifest
from hydra.validation.status_policy import MANDATORY_NULLS, candidate_level_null_decision, previous_status_semantics
from hydra.validation.temporal_transfer import classify_temporal_transfer


class TestStrictTemporalTransferReplay(unittest.TestCase):
    def test_previous_matched_null_semantics_are_not_candidate_level(self) -> None:
        semantics = previous_status_semantics()
        self.assertIn("component-level", semantics["MATCHED_NULL_BEATEN"])
        self.assertIn("cannot support Q4", semantics["defect"])

    def test_candidate_level_null_requires_all_mandatory_nulls(self) -> None:
        result = {
            "event_count": 100,
            "real_effect": 1.0,
            "random_matched_effect": 0.1,
            "block_shuffled_effect": 0.1,
            "delayed_effect": 0.1,
            "sign_flipped_effect": 0.1,
            "momentum_baseline_effect": 1.2,
            "mean_reversion_baseline_effect": 0.1,
        }
        decision = candidate_level_null_decision(result, candidate_id="c1", effective_trial_count=1)
        self.assertFalse(decision.passed)
        self.assertIn("momentum_baseline_effect", decision.decision_reason)

    def test_candidate_level_null_pass_records_policy_version(self) -> None:
        result = {"event_count": 100, "real_effect": 1.0} | {key: 0.1 for key in MANDATORY_NULLS}
        decision = candidate_level_null_decision(result, candidate_id="c1", effective_trial_count=1)
        self.assertTrue(decision.passed)
        self.assertEqual(decision.null_tests_passed, len(MANDATORY_NULLS))
        self.assertTrue(decision.policy_version)

    def test_manifest_hash_changes_when_candidate_parameter_changes(self) -> None:
        base = FrozenReplayCandidate(
            candidate_id="c1",
            lane="overnight_inventory_rth_resolution",
            structural_id="s1",
            variant_id="v1",
            symbol="MES",
            components=("opening_response",),
            horizon=20,
            threshold_rank=1,
            previous_statuses=("REPRESENTATION_EVIDENCE_PASS",),
            replay_role="REPRESENTATION_EVIDENCE_CANDIDATE",
        )
        changed = FrozenReplayCandidate(
            candidate_id="c1",
            lane="overnight_inventory_rth_resolution",
            structural_id="s1",
            variant_id="v1",
            symbol="MES",
            components=("opening_response",),
            horizon=30,
            threshold_rank=1,
            previous_statuses=("REPRESENTATION_EVIDENCE_PASS",),
            replay_role="REPRESENTATION_EVIDENCE_CANDIDATE",
        )
        first = build_replay_manifest(baseline_commit="abc", candidates=[base], data_fingerprints={}, source_report="r")
        second = build_replay_manifest(baseline_commit="abc", candidates=[changed], data_fingerprints={}, source_report="r")
        self.assertNotEqual(first["manifest_hash"], second["manifest_hash"])

    def test_manifest_mismatch_blocks_period_specific_parameter_change(self) -> None:
        candidate = FrozenReplayCandidate(
            candidate_id="c1",
            lane="intraday_range_migration_path_asymmetry",
            structural_id="s1",
            variant_id="v1",
            symbol="MNQ",
            components=("time_at_extremes",),
            horizon=20,
            threshold_rank=1,
            previous_statuses=("REPRESENTATION_EVIDENCE_PASS",),
            replay_role="REPRESENTATION_EVIDENCE_CANDIDATE",
        )
        with self.assertRaises(ValueError):
            assert_same_candidate_spec(candidate, candidate.to_dict() | {"horizon": 40})

    def test_temporal_transfer_failed_when_pooled_negative(self) -> None:
        periods = {
            "q1": {"net_pnl": 100.0, "trade_count": 20, "trades": [{"net_pnl": 10.0}] * 20},
            "q2": {"net_pnl": 100.0, "trade_count": 20, "trades": [{"net_pnl": 10.0}] * 20},
            "q3": {"net_pnl": -1000.0, "trade_count": 20, "trades": [{"net_pnl": -50.0}] * 20},
        }
        nulls = {key: {"passed": True} for key in periods}
        decision = classify_temporal_transfer("c1", periods, nulls, {"passed": True})
        self.assertEqual(decision.status, "TEMPORAL_TRANSFER_FAILED")

    def test_temporal_transfer_strong_requires_pooled_candidate_null(self) -> None:
        periods = {
            "q1": {"net_pnl": 500.0, "trade_count": 20, "trades": [{"net_pnl": 25.0}] * 20},
            "q2": {"net_pnl": 500.0, "trade_count": 20, "trades": [{"net_pnl": 25.0}] * 20},
            "q3": {"net_pnl": -50.0, "trade_count": 20, "trades": [{"net_pnl": -2.5}] * 20},
        }
        nulls = {key: {"passed": True} for key in periods}
        decision = classify_temporal_transfer("c1", periods, nulls, {"passed": False})
        self.assertNotEqual(decision.status, "TEMPORAL_TRANSFER_STRONG")


if __name__ == "__main__":
    unittest.main()
