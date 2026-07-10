from __future__ import annotations

import unittest

import pandas as pd

from hydra.factory.lineage_tombstone import LineageTombstoneViolation, assert_not_tombstoned
from hydra.research.null_models import block_shuffle
from hydra.research.representation_lab import RepresentationSpec, generate_bounded_prototypes, q4_access_guard


class MarketRepresentationLabGovernanceTests(unittest.TestCase):
    def test_killed_lineage_cannot_be_reactivated(self) -> None:
        with self.assertRaises(LineageTombstoneViolation):
            assert_not_tombstoned(
                {
                    "candidate_id": "old_proxy",
                    "family": "topstep_nq_es_divergence_controlled",
                    "parameters": {"divergence_min": 0.5, "max_range_expansion": 2.0},
                }
            )

    def test_equivalent_renamed_fingerprint_is_blocked(self) -> None:
        with self.assertRaises(LineageTombstoneViolation):
            assert_not_tombstoned(
                {
                    "candidate_id": "renamed_proxy",
                    "family": "directional_nq_es_divergence",
                    "parameters": {"divergence_min": 0.7, "max_range_expansion": 1.8},
                }
            )

    def test_true_paired_reformulation_is_allowed(self) -> None:
        assert_not_tombstoned(
            {
                "candidate_id": "new_pair",
                "family": "roll_aware_beta_neutral_nq_es_residual_divergence",
                "parameters": {
                    "left_symbol": "NQ",
                    "right_symbol": "ES",
                    "hedge_ratio_method": "rolling_ols",
                    "two_leg_execution": "ATOMIC_CONSERVATIVE",
                    "pair_validity_required": True,
                    "beta_neutral": True,
                },
            }
        )

    def test_q4_access_is_prohibited(self) -> None:
        with self.assertRaises(RuntimeError):
            q4_access_guard("2024-10-01", "2025-01-01")
        q4_access_guard("2024-07-01", "2024-10-01")

    def test_block_shuffle_preserves_length_and_values(self) -> None:
        series = pd.Series(range(100))
        shuffled = block_shuffle(series, block_size=10, seed=7)
        self.assertEqual(len(shuffled), len(series))
        self.assertEqual(sorted(shuffled.tolist()), sorted(series.tolist()))

    def test_prototype_family_cap_and_no_duplicate_ids(self) -> None:
        specs = [
            RepresentationSpec(
                name=f"family_{idx}",
                rank=idx,
                selected=True,
                economic_hypothesis="h",
                mechanism="m",
                expected_regime="r",
                expected_failure_regime="f",
                topstep_role="t",
                roll_sensitivity="low",
                reason="test",
            )
            for idx in range(1, 6)
        ]
        prototypes = generate_bounded_prototypes(specs, total_cap=100, max_family_share=0.30, seed=1)
        ids = [prototype.prototype_id for prototype in prototypes]
        self.assertEqual(len(ids), len(set(ids)))
        counts = {}
        for prototype in prototypes:
            counts[prototype.family] = counts.get(prototype.family, 0) + 1
        self.assertLessEqual(max(counts.values()), 30)


if __name__ == "__main__":
    unittest.main()
