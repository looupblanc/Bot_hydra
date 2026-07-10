from __future__ import annotations

import json
import unittest

from hydra.validation.oos_forensics import (
    INSUFFICIENT_OOS_TRADES,
    METRIC_DIRECTION_BUG,
    MISSING_DATA,
    STATUS_INHERITANCE_BUG,
    THRESHOLD_TOO_STRICT,
    TRUE_EDGE_DECAY,
    classify_oos_failure,
)


class OOSForensicsTests(unittest.TestCase):
    def test_missing_split_score_is_missing_data(self) -> None:
        row = {"candidate_id": "x", "family": "f", "symbol": "NQ", "topstep_split_scores_json": "{}", "gate_history_json": "[]"}
        self.assertEqual(classify_oos_failure(row).classification, MISSING_DATA)

    def test_metric_direction_bug_when_score_passes_but_gate_failed(self) -> None:
        row = {
            "candidate_id": "x",
            "family": "f",
            "symbol": "NQ",
            "topstep_split_scores_json": json.dumps({"mar": 0.50}),
            "gate_history_json": json.dumps([{"name": "OOS", "passed": False}]),
        }
        self.assertEqual(classify_oos_failure(row).classification, METRIC_DIRECTION_BUG)

    def test_status_inheritance_bug_when_stage_passed_but_score_failed(self) -> None:
        row = {
            "candidate_id": "x",
            "family": "f",
            "symbol": "NQ",
            "promotion_stage": "OOS_PASSED",
            "topstep_split_scores_json": json.dumps({"mar": 0.20}),
            "gate_history_json": json.dumps([{"name": "OOS", "passed": True}]),
        }
        self.assertEqual(classify_oos_failure(row).classification, STATUS_INHERITANCE_BUG)

    def test_insufficient_oos_trades_has_specific_class(self) -> None:
        row = {"candidate_id": "x", "family": "f", "symbol": "NQ", "topstep_split_scores_json": json.dumps({"mar": 0.10}), "gate_history_json": "[]"}
        recomputed = {"split_trade_counts": {"mar": 3}}
        self.assertEqual(classify_oos_failure(row, recomputed).classification, INSUFFICIENT_OOS_TRADES)

    def test_near_threshold_is_not_automatically_edge_decay(self) -> None:
        row = {"candidate_id": "x", "family": "f", "symbol": "NQ", "topstep_split_scores_json": json.dumps({"jan": 0.4, "feb": 0.4, "mar": 0.33}), "gate_history_json": "[]"}
        self.assertEqual(classify_oos_failure(row).classification, THRESHOLD_TOO_STRICT)

    def test_positive_train_negative_oos_is_edge_decay(self) -> None:
        row = {"candidate_id": "x", "family": "f", "symbol": "NQ", "topstep_split_scores_json": json.dumps({"jan": 0.6, "feb": 0.6, "mar": 0.1}), "gate_history_json": "[]"}
        recomputed = {"split_net_pnl": {"jan": 1000, "feb": 500, "mar": -250}}
        self.assertEqual(classify_oos_failure(row, recomputed).classification, TRUE_EDGE_DECAY)


if __name__ == "__main__":
    unittest.main()
