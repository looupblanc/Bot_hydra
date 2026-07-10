from __future__ import annotations

import unittest

from hydra.backtest.two_leg_execution import ExecutionMode, build_two_leg_trade
from hydra.execution.cost_units import leg_cost_breakdown, points_to_dollars, ticks_to_dollars
from hydra.execution.two_leg_cost_audit import audit_two_leg_trade


class ExecutionCostForensicsTests(unittest.TestCase):
    def test_tick_to_dollar_and_point_to_dollar_conversion(self) -> None:
        self.assertEqual(ticks_to_dollars("ES", 1, 1), 12.5)
        self.assertEqual(ticks_to_dollars("MES", 1, 1), 1.25)
        self.assertEqual(ticks_to_dollars("NQ", 1, 1), 5.0)
        self.assertEqual(ticks_to_dollars("MNQ", 1, 1), 0.5)
        self.assertEqual(points_to_dollars("ES", 1, 2), 100.0)
        self.assertEqual(points_to_dollars("MNQ", 1, 2), 4.0)

    def test_one_es_one_nq_exact_round_trip_cost(self) -> None:
        es = leg_cost_breakdown(symbol="ES", quantity=1, reference_price=5000, entry_slippage_ticks=1, exit_slippage_ticks=1)
        nq = leg_cost_breakdown(symbol="NQ", quantity=1, reference_price=18000, entry_slippage_ticks=1, exit_slippage_ticks=1)
        self.assertEqual(es.round_turn_commission_usd, 9.0)
        self.assertEqual(es.slippage_cost_usd, 25.0)
        self.assertEqual(nq.round_turn_commission_usd, 9.0)
        self.assertEqual(nq.slippage_cost_usd, 10.0)
        self.assertEqual(es.execution_cost_usd + nq.execution_cost_usd, 53.0)

    def test_one_mes_one_mnq_exact_round_trip_cost(self) -> None:
        mes = leg_cost_breakdown(symbol="MES", quantity=1, reference_price=5000, entry_slippage_ticks=1, exit_slippage_ticks=1)
        mnq = leg_cost_breakdown(symbol="MNQ", quantity=1, reference_price=18000, entry_slippage_ticks=1, exit_slippage_ticks=1)
        self.assertEqual(mes.execution_cost_usd, 7.0)
        self.assertEqual(mnq.execution_cost_usd, 5.5)
        self.assertEqual(mes.execution_cost_usd + mnq.execution_cost_usd, 12.5)

    def test_mixed_micro_ratio_and_quantity_applied_once(self) -> None:
        mes = leg_cost_breakdown(symbol="MES", quantity=2, reference_price=5000, entry_slippage_ticks=1, exit_slippage_ticks=1)
        mnq = leg_cost_breakdown(symbol="MNQ", quantity=1, reference_price=18000, entry_slippage_ticks=1, exit_slippage_ticks=1)
        self.assertEqual(mes.execution_cost_usd + mnq.execution_cost_usd, 19.5)

    def test_zero_slippage_is_commission_only(self) -> None:
        cost = leg_cost_breakdown(symbol="ES", quantity=3, reference_price=5000, entry_slippage_ticks=0, exit_slippage_ticks=0)
        self.assertEqual(cost.execution_cost_usd, 27.0)

    def test_spread_and_forced_liquidation_are_explicit_components(self) -> None:
        cost = leg_cost_breakdown(
            symbol="NQ",
            quantity=2,
            reference_price=18000,
            entry_slippage_ticks=1,
            exit_slippage_ticks=1,
            spread_ticks=1,
            forced_liquidation_ticks=2,
        )
        self.assertEqual(cost.slippage_cost_usd, 20.0)
        self.assertEqual(cost.spread_cost_usd, 10.0)
        self.assertEqual(cost.forced_liquidation_cost_usd, 20.0)
        self.assertEqual(cost.execution_cost_usd, 68.0)

    def test_notional_is_not_execution_cost(self) -> None:
        cost = leg_cost_breakdown(symbol="ES", quantity=1, reference_price=5000, entry_slippage_ticks=1, exit_slippage_ticks=1)
        self.assertGreater(cost.notional_exposure_usd, 200000)
        self.assertLess(cost.execution_cost_usd, 100)

    def test_two_leg_trade_separates_legging_stress(self) -> None:
        atomic = build_two_leg_trade(
            entry_timestamp="2024-04-15T14:00:00Z",
            exit_timestamp="2024-04-15T15:00:00Z",
            left_symbol="MNQ",
            right_symbol="MES",
            left_quantity=1,
            right_quantity=1,
            direction=1,
            left_entry=18000,
            right_entry=5000,
            left_exit=18001,
            right_exit=4999,
            slippage_ticks=1,
        )
        stress = build_two_leg_trade(
            entry_timestamp="2024-04-15T14:00:00Z",
            exit_timestamp="2024-04-15T15:00:00Z",
            left_symbol="MNQ",
            right_symbol="MES",
            left_quantity=1,
            right_quantity=1,
            direction=1,
            left_entry=18000,
            right_entry=5000,
            left_exit=18001,
            right_exit=4999,
            mode=ExecutionMode.LEG_SEQUENTIAL_STRESS,
            slippage_ticks=1,
            legging_delay_bars=2,
        )
        self.assertEqual(atomic.execution_cost_usd, 12.5)
        self.assertEqual(stress.execution_cost_usd, 12.5)
        self.assertLess(stress.legging_risk_pnl, 0)
        self.assertLess(stress.net_pnl, atomic.net_pnl)

    def test_cost_audit_distinguishes_legacy_mislabeled_cost(self) -> None:
        row = audit_two_leg_trade(
            prototype_id="x",
            left_symbol="MNQ",
            right_symbol="MES",
            left_quantity=1,
            right_quantity=1,
            theoretical_hedge_ratio=1.0,
            executable_hedge_ratio=1.0,
            left_entry=18000,
            right_entry=5000,
            left_exit=18001,
            right_exit=4999,
            direction=1,
        )
        self.assertEqual(row.execution_cost_usd, 12.5)
        self.assertGreater(row.legacy_mislabeled_cost_usd, row.execution_cost_usd)
        self.assertGreater(row.notional_exposure_usd, row.execution_cost_usd)


if __name__ == "__main__":
    unittest.main()
