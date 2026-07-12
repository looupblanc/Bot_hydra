from __future__ import annotations

import unittest

import pandas as pd

from hydra.data.contract_mapping import (
    active_contract,
    build_explicit_roll_map,
    build_rule_based_roll_map,
    is_unsafe_roll_window,
    synchronized_pair_contracts,
)
from hydra.data.pair_contract_synchronization import pair_validity_at
from hydra.data.roll_audit import audit_trade_roll_exposure
from hydra.promotion.cluster_calibration import calibrate_clustering_controls, cluster_sketches
from scripts.build_databento_contract_map import parse_definition_frame


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

    def test_explicit_roll_transition_and_definition_parsing(self) -> None:
        frame = pd.DataFrame(
            [
                {"instrument_id": "5602", "ts_event": "2024-01-01T00:00:00Z", "expiration": "2024-06-21T13:30:00Z", "activation": "2022-03-18T13:30:00Z", "min_price_increment": 0.25},
                {"instrument_id": "13743", "ts_event": "2024-01-01T00:00:00Z", "expiration": "2024-06-21T13:30:00Z", "activation": "2023-03-17T13:30:00Z", "min_price_increment": 0.25},
            ]
        )
        definitions = parse_definition_frame(frame, ["5602", "13743"])
        roll_map = build_explicit_roll_map(
            ["ES", "NQ"],
            start="2024-01-01",
            end="2024-07-01",
            continuous_mapping={
                "ES.c.0": [{"d0": "2024-03-17", "d1": "2024-06-23", "s": "5602"}],
                "NQ.c.0": [{"d0": "2024-03-17", "d1": "2024-06-23", "s": "13743"}],
            },
            raw_symbol_mapping={"5602": "ESM4", "13743": "NQM4"},
            definition_records=definitions,
        )
        self.assertEqual(active_contract(roll_map, "ES", "2024-04-01T12:00:00Z").contract, "ESM4")
        self.assertEqual(active_contract(roll_map, "ES", "2024-04-01T12:00:00Z").tick_value, 12.5)

    def test_explicit_roll_map_accepts_timezone_aware_request_bounds(self) -> None:
        roll_map = build_explicit_roll_map(
            ["ES"],
            start="2026-07-10T00:00:00Z",
            end="2026-07-11T23:30:00Z",
            continuous_mapping={
                "ES.c.0": [
                    {"d0": "2026-07-10", "d1": "2026-07-12", "s": "101"}
                ]
            },
            raw_symbol_mapping={"101": "ESU6"},
            definition_records={
                "101": {"expiration": "2026-09-18T00:00:00+00:00"}
            },
        )

        self.assertEqual(roll_map.contracts[0].contract, "ESU6")
        self.assertEqual(roll_map.contracts[0].roll_date, "2026-07-12")

    def test_mismatched_maturity_rejection(self) -> None:
        roll_map = build_explicit_roll_map(
            ["ES", "NQ"],
            start="2024-01-01",
            end="2024-07-01",
            continuous_mapping={
                "ES.c.0": [{"d0": "2024-03-17", "d1": "2024-06-23", "s": "5602"}],
                "NQ.c.0": [{"d0": "2024-03-17", "d1": "2024-06-23", "s": "4358"}],
            },
            raw_symbol_mapping={"5602": "ESM4", "4358": "NQU4"},
            definition_records={},
        )
        validity = pair_validity_at(roll_map, "2024-04-15T14:00:00Z", left_symbol="NQ", right_symbol="ES")
        self.assertFalse(validity.pair_valid)
        self.assertEqual(validity.reason, "mismatched_quarterly_maturity")

    def test_explicit_builder_uses_date_aware_definition_for_reused_id(self) -> None:
        history = pd.DataFrame(
            [
                {
                    "ts_event": "2023-01-01T17:00:00Z",
                    "instrument_id": 5602,
                    "raw_symbol": "ZNH3",
                    "instrument_class": "F",
                    "security_type": "FUT",
                    "asset": "ZN",
                    "min_price_increment": 0.015625,
                    "expiration": "2023-03-17T13:30:00Z",
                    "activation": "2022-03-18T13:30:00Z",
                },
                {
                    "ts_event": "2024-03-17T11:04:10Z",
                    "instrument_id": 5602,
                    "raw_symbol": "ESM4",
                    "instrument_class": "F",
                    "security_type": "FUT",
                    "asset": "ES",
                    "min_price_increment": 0.25,
                    "expiration": "2024-06-21T13:30:00Z",
                    "activation": "2023-06-16T13:30:00Z",
                },
            ]
        )
        roll_map = build_explicit_roll_map(
            ["ES"],
            start="2024-01-01",
            end="2024-07-01",
            continuous_mapping={
                "ES.c.0": [{"d0": "2024-03-17", "d1": "2024-06-23", "s": "5602"}]
            },
            raw_symbol_mapping={"5602": "ZNH3"},
            definition_records={},
            definition_history=history,
        )
        self.assertEqual(roll_map.contracts[0].contract, "ESM4")
        self.assertEqual(roll_map.contracts[0].tick_size, 0.25)
        self.assertEqual(
            roll_map.map_type,
            "EXPLICIT_DATABENTO_CONTINUOUS_SYMBOLOGY_DATE_AWARE_DEFINITIONS_V2",
        )

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
        self.assertGreaterEqual(report["precision_known_clones"], 0.90)
        self.assertGreaterEqual(report["recall_known_clones"], 0.90)
        self.assertEqual(report["false_merge_rate"], 0.0)
        clusters = cluster_sketches(sketches)
        self.assertEqual(len(clusters), 2)


if __name__ == "__main__":
    unittest.main()
