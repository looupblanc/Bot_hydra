from __future__ import annotations

import unittest

import pandas as pd

from hydra.data.contract_mapping import active_contract, build_rule_based_roll_map, is_unsafe_roll_window, synchronized_pair_contracts
from hydra.data.roll_audit import audit_trade_roll_exposure
from hydra.promotion.cluster_calibration import calibrate_clustering_controls, cluster_sketches


class RollMappingAndClusteringTests(unittest.TestCase):
    def test_contract_expiry_mapping_and_roll_selection(self) -> None:
        roll_map = build_rule_based_roll_map(["ES", "NQ"], start="2024-01-01", end="2024-07-01")
        self.assertEqual(active_contract(roll_map, "ES", "2024-03-14T12:00:00Z").contract, "ESH4")
        self.assertEqual(active_contract(roll_map, "ES", "2024-03-15T12:00:00Z").contract, "ESM4")
        self.assertEqual(active_contract(roll_map, "NQ", "2024-06-21T12:00:00Z").contract, "NQU4")

    def test_synchronized_pair_mapping(self) -> None:
        roll_map = build_rule_based_roll_map(["ES", "NQ"], start="2024-01-01", end="2024-07-01")
        pair = synchronized_pair_contracts(roll_map, ("NQ", "ES"), "2024-04-15T14:00:00Z")
        self.assertEqual(pair["NQ"][-2], pair["ES"][-2])

    def test_roll_discontinuity_trade_handling(self) -> None:
        roll_map = build_rule_based_roll_map(["NQ"], start="2024-01-01", end="2024-07-01")
        self.assertTrue(is_unsafe_roll_window(roll_map, "NQ", "2024-03-15T12:00:00Z"))
        trades = [
            {
                "symbol": "NQ",
                "entry_timestamp": "2024-03-14T20:00:00Z",
                "exit_timestamp": "2024-03-15T13:00:00Z",
                "pnl": 100.0,
            }
        ]
        audit = audit_trade_roll_exposure(trades, roll_map)
        self.assertTrue(audit["roll_sensitive"])
        self.assertEqual(audit["cross_roll_trade_count"], 1)

    def test_clustering_positive_controls_and_negative_controls(self) -> None:
        sketches = [
            {
                "candidate_id": "a",
                "daily_pnl_hash": "p1",
                "trade_timestamp_signature": "t1",
                "direction_signature": "d1",
                "tail_event_signature": "x1",
                "holding_time_histogram": {"0-4": 10},
                "session_histogram": {"rth": 10},
                "symbol_exposure": {"NQ": 10},
            },
            {
                "candidate_id": "b",
                "daily_pnl_hash": "p2",
                "trade_timestamp_signature": "t2",
                "direction_signature": "d2",
                "tail_event_signature": "x2",
                "holding_time_histogram": {"20-24": 10},
                "session_histogram": {"overnight": 10},
                "symbol_exposure": {"ES": 10},
            },
        ]
        report = calibrate_clustering_controls(sketches)
        self.assertGreaterEqual(report["recall_known_clones"], 1.0)
        self.assertEqual(report["false_merge_rate"], 0.0)
        clusters = cluster_sketches(sketches)
        self.assertEqual(len(clusters), 2)


if __name__ == "__main__":
    unittest.main()
