from __future__ import annotations

import unittest

from hydra.factory.gate_aware_remediation import child_from_registry_row
from hydra.factory.run_control import RunControlConfig, RunControlState, evaluate_stop, validated_quality_target_reached
from hydra.promotion.gates import strategy_fingerprint


class RunControlTests(unittest.TestCase):
    def test_runtime_cap_is_not_minimum_runtime(self) -> None:
        cfg = RunControlConfig(
            min_runtime_hours=5.5,
            max_runtime_hours=6.0,
            continue_until_deadline=True,
            minimum_cycles=20,
            minimum_remediation_children=5000,
            allow_early_stop_on_exhaustion=True,
        )
        state = RunControlState(
            elapsed_seconds=120.0,
            cycles_completed=20,
            remediation_children_completed=5000,
            queue_size=0,
            eligible_parents=0,
            proven_work_exhaustion=True,
        )
        decision = evaluate_stop(cfg, state)
        self.assertFalse(decision.should_stop)
        self.assertEqual(decision.reason, "continue")

    def test_max_runtime_is_a_hard_stop(self) -> None:
        cfg = RunControlConfig(min_runtime_hours=5.5, max_runtime_hours=6.0, continue_until_deadline=True)
        state = RunControlState(
            elapsed_seconds=6.01 * 3600.0,
            cycles_completed=1,
            remediation_children_completed=1,
            queue_size=10,
            eligible_parents=10,
        )
        decision = evaluate_stop(cfg, state)
        self.assertTrue(decision.should_stop)
        self.assertEqual(decision.reason, "max_runtime_reached")

    def test_provisional_quality_target_never_stops_strict_run(self) -> None:
        cfg = RunControlConfig(
            min_runtime_hours=0.0,
            max_runtime_hours=6.0,
            stop_only_on_valid_quality_target=True,
        )
        state = RunControlState(
            elapsed_seconds=3600.0,
            cycles_completed=50,
            remediation_children_completed=10000,
            queue_size=0,
            eligible_parents=100,
            provisional_quality_target_reached=True,
            valid_quality_target_reached=False,
        )
        decision = evaluate_stop(cfg, state)
        self.assertFalse(decision.should_stop)
        self.assertTrue(decision.diagnostics["provisional_target_ignored"])

    def test_continue_until_deadline_ignores_valid_quality_before_max(self) -> None:
        cfg = RunControlConfig(
            min_runtime_hours=0.0,
            max_runtime_hours=6.0,
            continue_until_deadline=True,
            minimum_cycles=1,
            minimum_remediation_children=1,
        )
        state = RunControlState(
            elapsed_seconds=3600.0,
            cycles_completed=5,
            remediation_children_completed=500,
            queue_size=100,
            eligible_parents=100,
            valid_quality_target_reached=True,
        )
        decision = evaluate_stop(cfg, state)
        self.assertFalse(decision.should_stop)

    def test_validated_quality_requires_trading_ready_q4_and_execution(self) -> None:
        self.assertFalse(validated_quality_target_reached(trading_ready=50, q4_passes=0, execution_passes=50, target_units=50))
        self.assertFalse(validated_quality_target_reached(trading_ready=50, q4_passes=50, execution_passes=0, target_units=50))
        self.assertTrue(validated_quality_target_reached(trading_ready=50, q4_passes=50, execution_passes=50, target_units=50))

    def test_child_variants_create_distinct_strategy_fingerprints(self) -> None:
        row = {
            "candidate_id": "parent_1",
            "family": "topstep_nq_es_divergence_controlled",
            "symbol": "NQ",
            "timeframe": "1m",
            "parameters_json": '{"threshold": 1.25}',
            "risk_json": '{"risk_scale": 1.0, "holding_period": 8, "daily_profit_lock": 1500}',
            "rejection_reason": "combine_profit_target_not_reached",
        }
        first = child_from_registry_row(row, variant=1)
        second = child_from_registry_row(row, variant=2)
        self.assertNotEqual(strategy_fingerprint(first.child), strategy_fingerprint(second.child))


if __name__ == "__main__":
    unittest.main()
