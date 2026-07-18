from __future__ import annotations

from dataclasses import asdict
from types import SimpleNamespace

import numpy as np
import pytest

from hydra.evidence.schema import RECORD_SPECS
from hydra.production.microstructure_sparse_pilot import (
    SparsePilotConfig,
    SparseStrategySpec,
    SparseTrade,
    _account_paths,
    _depth_fill,
    _evidence_censored_state,
    _gate_decision,
    _no_trade_abstention_signal,
)


def _spec(identifier: str = "sparse-test") -> SparseStrategySpec:
    return SparseStrategySpec(
        strategy_id=identifier,
        mechanism="INITIATIVE",
        tier="L1",
        deployability_tier="L1_DEPLOYABLE",
        edge_to_cost_ratio=1.25,
        trade_budget_per_session=2,
        holding_horizon_seconds=30,
        exit_policy="FIXED_TARGET_STOP",
        target_ticks=16.0,
        stop_ticks=6.0,
        quantity=1,
        model_hash="a" * 64,
        specification_hash="b" * 64,
    )


def _trade(session: str, pnl: float = 100.0) -> SparseTrade:
    return SparseTrade(
        strategy_id="sparse-test",
        opportunity_id=f"opportunity-{session}",
        trade_id=f"trade-{session}",
        market="NQ",
        session_id=session,
        role="DISCOVERY",
        direction=1,
        entry_index=0,
        exit_index=1,
        entry_time_ns=1,
        exit_time_ns=2,
        entry_price=20_000.0,
        exit_price=20_005.0,
        stop_price=19_998.5,
        target_price=20_004.0,
        exit_reason="PROFIT_TARGET",
        quantity=1,
        gross_reference_pnl_usd=pnl + 23.8,
        spread_cost_usd=10.0,
        marketable_slippage_usd=10.0,
        depth_slippage_usd=0.0,
        commission_usd=3.8,
        adverse_selection_usd=0.0,
        normal_net_pnl_usd=pnl,
        stressed_net_pnl_usd=pnl - 5.0,
        minimum_unrealized_pnl_usd=-20.0,
        prediction=0.9,
        expected_edge_to_cost=2.0,
    )


def _green_row(mechanism: str) -> dict:
    spec = asdict(_spec(f"sparse-{mechanism}"))
    spec["mechanism"] = mechanism
    metrics = {
        "NORMAL": {"mll_breach_count": 0},
        "STRESSED_1_5X": {"mll_breach_count": 0},
    }
    return {
        "strategy": spec,
        "validation_normal_net_usd": 10.0,
        "validation_stressed_net_usd": 5.0,
        "final_development_normal_net_usd": 12.0,
        "final_development_stressed_net_usd": 6.0,
        "trades_per_session": 2.0,
        "single_event_concentration": 0.3,
        "trade_count": 10,
        "gross_reference_pnl_usd": 100.0,
        "account_frontier": {"50K": metrics},
    }


def test_frozen_three_core_and_sparse_frontiers() -> None:
    config = SparsePilotConfig()
    config.validate()
    assert config.cpu_worker_count == 2
    assert config.edge_to_cost_ratios == (1.25, 1.5, 2.0, 3.0)
    assert config.trade_budgets == (2, 4, 8, 12)
    assert config.target_ticks == 16.0
    assert config.stop_ticks == 6.0


def test_displayed_depth_fill_is_partial_and_deterministic() -> None:
    assert _depth_fill("[[100.0,2],[100.25,3]]", 4) == (4, 100.125, 0.125)
    assert _depth_fill("[[100.0,1]]", 4) == (1, 100.0, 0.0)


def test_account_windows_separate_full_coverage_and_censoring() -> None:
    sessions = tuple(f"2024-07-{value:02d}" for value in range(8, 13))
    paths = _account_paths(
        "sparse-test",
        [_trade(session) for session in sessions],
        sessions,
        {"account_size": 50_000.0, "target": 3_000.0, "mll": 2_000.0, "consistency": 0.5},
    )
    full = [row for row in paths if row["full_coverage"]]
    assert len(full) == 2
    assert {row["scenario"] for row in full} == {"NORMAL", "STRESSED_1_5X"}
    assert all(row["horizon_days"] == 5 for row in full)
    assert all(row["terminal_state"] == "OPERATIONAL_HORIZON_NOT_REACHED" for row in full)
    assert all(row["terminal_state"] == "DATA_CENSORED" for row in paths if not row["full_coverage"])
    assert _evidence_censored_state("OPERATIONAL_HORIZON_NOT_REACHED") is True
    assert _evidence_censored_state("DATA_CENSORED") is True
    assert _evidence_censored_state("TARGET_REACHED") is False
    assert _evidence_censored_state("MLL_BREACHED") is False


def test_sparse_gate_requires_two_distinct_mechanisms() -> None:
    one = _green_row("INITIATIVE")
    status, families, checks = _gate_decision([one], forensic_report=None)
    assert status == "SPARSE_PILOT_WEAK"
    assert families == ("INITIATIVE",)
    assert checks["two_behaviorally_distinct_families"] is False

    two = _green_row("ABSORPTION")
    status, families, checks = _gate_decision([one, two], forensic_report=None)
    assert status == "SPARSE_PILOT_GREEN"
    assert families == ("ABSORPTION", "INITIATIVE")
    assert all(checks.values())


def test_sparse_gate_falsifies_without_positive_gross_markout() -> None:
    row = _green_row("INITIATIVE")
    row["validation_stressed_net_usd"] = -1.0
    row["gross_reference_pnl_usd"] = -10.0
    status, _, _ = _gate_decision([row], forensic_report=None)
    assert status == "SPARSE_PILOT_FALSIFIED"


def test_config_rejects_worker_oversubscription() -> None:
    with pytest.raises(Exception, match="exactly two"):
        SparsePilotConfig(cpu_worker_count=3).validate()


def test_zero_trade_component_persists_honest_abstention_signal() -> None:
    config = SparsePilotConfig()
    store = SimpleNamespace(
        decision_ns=np.asarray([1_720_425_600_000_000_000], dtype=np.int64),
        market=np.asarray(["NQ"], dtype=object),
        contract=np.asarray(["NQU4"], dtype=object),
    )
    signal = _no_trade_abstention_signal(config, store, _spec())

    RECORD_SPECS["component_signals"].validate(
        signal,
        campaign_id=config.campaign_id,
    )
    assert signal["signal"] == 0
    assert signal["sizing"] == 0.0
    assert signal["veto"] is True
    assert signal["evidence_only_reason"] == (
        "NO_EXECUTABLE_SIGNAL_OBSERVED_AFTER_FROZEN_ABSTENTION"
    )
