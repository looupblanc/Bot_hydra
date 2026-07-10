from __future__ import annotations

import unittest

import pandas as pd

from hydra.promotion.pre_backtest_dedup import reject_logical_duplicates
from hydra.research.cluster_aware_generator import generate_cluster_aware_prototypes
from hydra.research.component_attribution import ComponentDefinition
from hydra.research.matched_nulls import evaluate_matched_nulls, multiple_testing_adjusted_alpha
from hydra.research.opportunity_matched_nulls import opportunity_count_matched_signal
from hydra.research.representation_ablation import ablation_id, evaluate_component_ablation, remove_component_signal
from hydra.research.representation_lab import q4_access_guard


class TargetedRepresentationRepairTests(unittest.TestCase):
    def test_deterministic_ablation_ids_and_component_removal(self) -> None:
        left = ablation_id("lane", ["a", "b"])
        right = ablation_id("lane", ["b", "a"])
        self.assertEqual(left, right)
        frame = pd.DataFrame({"a": [1, -1, 1], "b": [1, 1, -1]})
        components = [
            ComponentDefinition("a", "lane", "a", 1, "a"),
            ComponentDefinition("b", "lane", "b", 1, "b"),
        ]
        signal = remove_component_signal(frame, components, "b")
        self.assertEqual(signal.tolist(), [1, -1, 1])

    def test_matched_null_generation_and_baselines(self) -> None:
        frame = pd.DataFrame(
            {
                "timestamp": pd.date_range("2024-01-02T14:00:00Z", periods=200, freq="min"),
                "symbol": ["ES"] * 200,
                "close": [5000 + i * 0.1 for i in range(200)],
                "volatility_decile": [i % 10 for i in range(200)],
            }
        )
        signal = pd.Series([1 if i % 25 == 0 else 0 for i in range(200)])
        forward = pd.Series([0.001 if i % 25 == 0 else 0.0 for i in range(200)])
        matched = opportunity_count_matched_signal(frame, signal, seed=1)
        self.assertEqual(int(matched.ne(0).sum()), int(signal.ne(0).sum()))
        result = evaluate_matched_nulls(frame, signal, forward, signal_id="s", seed=1)
        self.assertEqual(result.event_count, int(signal.ne(0).sum()))
        self.assertIn(result.status, {"MATCHED_NULL_BEATEN", "FALSIFIED"})

    def test_component_ablation_evaluates_minimal_combinations(self) -> None:
        frame = pd.DataFrame(
            {
                "timestamp": pd.date_range("2024-01-02T14:00:00Z", periods=300, freq="min"),
                "symbol": ["ES"] * 300,
                "close": [5000 + i * 0.1 for i in range(300)],
                "a": [1 if i % 2 == 0 else -1 for i in range(300)],
                "b": [1 if i % 3 == 0 else -1 for i in range(300)],
                "forward_return": [0.001 if i % 2 == 0 else -0.001 for i in range(300)],
            }
        )
        components = [
            ComponentDefinition("a", "lane", "a", 1, "a"),
            ComponentDefinition("b", "lane", "b", 1, "b"),
        ]
        rows = evaluate_component_ablation(frame, components, lane="lane", seed=2)
        self.assertGreaterEqual(len(rows), 4)
        self.assertTrue(all("matched_null" in row for row in rows))

    def test_multiple_testing_adjustment(self) -> None:
        self.assertEqual(multiple_testing_adjusted_alpha(0.05, 10), 0.005)

    def test_pre_backtest_duplicate_rejection(self) -> None:
        sketches = [
            {"prototype_id": "a", "logical_fingerprint": "x", "event_signature": "e", "direction_signature": "d", "event_count": 5},
            {"prototype_id": "b", "logical_fingerprint": "x", "event_signature": "e", "direction_signature": "d", "event_count": 5},
            {"prototype_id": "c", "logical_fingerprint": "z", "event_signature": "e", "direction_signature": "d", "event_count": 5},
        ]
        accepted, rejected = reject_logical_duplicates(sketches)
        self.assertEqual(len(accepted), 1)
        self.assertEqual(len(rejected), 2)

    def test_prototype_caps(self) -> None:
        prototypes = generate_cluster_aware_prototypes(
            lanes=["a", "b"],
            lane_components={"a": ["c1", "c2", "c3"], "b": ["d1", "d2", "d3"]},
            symbols=["ES", "MES"],
            max_total=80,
            max_structures=20,
            max_variants_per_structure=4,
        )
        self.assertLessEqual(len(prototypes), 80)
        self.assertLessEqual(len({item.structural_id for item in prototypes}), 20)
        per_struct = {}
        for item in prototypes:
            per_struct[item.structural_id] = per_struct.get(item.structural_id, 0) + 1
        self.assertLessEqual(max(per_struct.values()), 4)

    def test_q4_access_prohibited_for_ablation_context(self) -> None:
        with self.assertRaises(RuntimeError):
            q4_access_guard("2024-10-01", "2025-01-01")


if __name__ == "__main__":
    unittest.main()
