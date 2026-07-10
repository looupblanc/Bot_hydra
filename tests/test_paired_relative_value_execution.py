from __future__ import annotations

import unittest

import numpy as np
import pandas as pd

from hydra.backtest.two_leg_execution import ExecutionMode, build_two_leg_trade
from hydra.data.contract_mapping import build_rule_based_roll_map
from hydra.data.pair_contract_synchronization import pair_validity_at
from hydra.representations.dynamic_hedge_ratio import rolling_ols_beta
from hydra.representations.paired_relative_value import PairedRelativeValueConfig, build_paired_residual_frame
from hydra.risk.pair_risk import directional_beta_audit, integer_hedge_ratio


class PairedRelativeValueExecutionTests(unittest.TestCase):
    def test_past_only_hedge_ratio_does_not_change_after_future_perturbation(self) -> None:
        right = pd.Series(np.linspace(0.001, 0.002, 100))
        left = right * 1.5
        beta = rolling_ols_beta(left, right, window=20)
        perturbed = left.copy()
        perturbed.iloc[80:] = perturbed.iloc[80:] * 100
        beta_perturbed = rolling_ols_beta(perturbed, right, window=20)
        pd.testing.assert_series_equal(beta.iloc[:60], beta_perturbed.iloc[:60])

    def test_synchronized_pair_and_mismatched_expiry_rejection(self) -> None:
        roll_map = build_rule_based_roll_map(["ES", "NQ"], start="2024-01-01", end="2024-07-01")
        valid = pair_validity_at(roll_map, "2024-04-15T14:00:00Z", left_symbol="NQ", right_symbol="ES")
        self.assertTrue(valid.pair_valid)
        invalid = pair_validity_at(roll_map, "2024-03-15T14:00:00Z", left_symbol="NQ", right_symbol="ES")
        self.assertTrue(invalid.roll_transition_exclusion)

    def test_paired_residual_frame_uses_shifted_signal(self) -> None:
        timestamps = pd.date_range("2024-04-15T14:00:00Z", periods=200, freq="min")
        rows = []
        for symbol, base in [("NQ", 18000.0), ("ES", 5200.0)]:
            for i, ts in enumerate(timestamps):
                price = base + i * (2.0 if symbol == "NQ" else 0.5)
                rows.append({"timestamp": ts, "symbol": symbol, "open": price, "high": price + 1, "low": price - 1, "close": price, "volume": 10, "timeframe": "1m", "session_id": "2024-04-15"})
        frame = pd.DataFrame(rows)
        roll_map = build_rule_based_roll_map(["ES", "NQ"], start="2024-01-01", end="2024-07-01")
        residual = build_paired_residual_frame(frame, roll_map, PairedRelativeValueConfig(hedge_window=20, z_window=20))
        self.assertIn("feature", residual.columns)
        self.assertIn("forward_return", residual.columns)
        self.assertEqual(int(residual["signal"].iloc[0]), 0)

    def test_integer_hedge_ratio_respects_topstep_mini_equivalent_limit(self) -> None:
        hedge = integer_hedge_ratio(
            left_symbol="MNQ",
            right_symbol="MES",
            theoretical_ratio=1.2,
            left_price=18000,
            right_price=5200,
            max_mini_equivalents=15,
            prefer_micro=True,
        )
        self.assertLessEqual(hedge.mini_equivalent_contracts, 15)
        self.assertGreaterEqual(hedge.left_quantity, 1)
        self.assertGreaterEqual(hedge.right_quantity, 1)

    def test_two_leg_commissions_slippage_and_legging_stress(self) -> None:
        atomic = build_two_leg_trade(
            entry_timestamp="2024-04-15T14:00:00Z",
            exit_timestamp="2024-04-15T15:00:00Z",
            left_symbol="MNQ",
            right_symbol="MES",
            left_quantity=2,
            right_quantity=3,
            direction=1,
            left_entry=18000,
            right_entry=5200,
            left_exit=18010,
            right_exit=5199,
            mode=ExecutionMode.ATOMIC_CONSERVATIVE,
            slippage_bps=1.0,
        )
        stress = build_two_leg_trade(
            entry_timestamp="2024-04-15T14:00:00Z",
            exit_timestamp="2024-04-15T15:00:00Z",
            left_symbol="MNQ",
            right_symbol="MES",
            left_quantity=2,
            right_quantity=3,
            direction=1,
            left_entry=18000,
            right_entry=5200,
            left_exit=18010,
            right_exit=5199,
            mode=ExecutionMode.LEG_SEQUENTIAL_STRESS,
            slippage_bps=1.0,
        )
        self.assertLess(atomic.net_pnl, atomic.gross_pnl)
        self.assertLess(stress.net_pnl, atomic.net_pnl)

    def test_directional_beta_detection(self) -> None:
        returns = pd.Series(np.linspace(-0.01, 0.01, 100))
        pnl = returns * 10000
        audit = directional_beta_audit(pnl, returns, pd.Series(np.random.default_rng(1).normal(0, 0.001, 100)))
        self.assertTrue(audit["directional_dominance"])


if __name__ == "__main__":
    unittest.main()
