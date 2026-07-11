from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime, timedelta, timezone

import pytest

from hydra.shadow.prior_trade_guard import (
    PriorTradeGuardError,
    PriorTradeGuardSpecification,
)
from hydra.shadow.runner import ShadowRunner
from hydra.shadow.signal_bus import ShadowSignal
from hydra.shadow.specification import ShadowSpecification


def _specification() -> ShadowSpecification:
    guard = PriorTradeGuardSpecification(
        trailing_window=1,
        minimum_prior_observations=1,
        warmup_completed_trades=1,
        frozen_threshold=0.0,
    )
    return ShadowSpecification(
        strategy_id="guarded-shadow-v1",
        strategy_version="v1",
        feature_versions=("f1", guard.version),
        markets=("MYM",),
        timeframes=("1m",),
        session_rules={"flatten": "15:10"},
        entry_rules={"event": "test", "prior_trade_guard": guard.to_dict()},
        exit_rules={"bars": 10},
        sizing={"contracts": 1},
        costs={"round_turn": 1.5},
        stale_data_seconds=90,
        expected_update_seconds=60,
        duplicate_signal_window_seconds=60,
        maximum_exposure=1.0,
        simulated_mll_floor=-2_500.0,
        internal_daily_risk_limit=800.0,
        kill_conditions=("stale_data",),
        logging={"prior_trade_guard_decisions": True},
        reconciliation={
            "prior_trade_guard_restart": "verify_state_hash_or_fail_closed"
        },
        source_manifest_hash="guard-test-manifest",
        outbound_orders_enabled=False,
    )


def _signal(specification: ShadowSpecification, decision_at: datetime) -> ShadowSignal:
    return ShadowSignal(
        specification.strategy_id,
        "MYM",
        1,
        1,
        decision_at,
        decision_at,
        40_000.0,
    )


def _process(runner: ShadowRunner, decision_at: datetime) -> dict[str, object]:
    return runner.process(
        _signal(runner.specification, decision_at),
        now=decision_at,
        latest_data_at=decision_at,
        market_price=40_000.0,
        proposed_exposure=1.0,
        session_open=True,
        simulated_mll=-100.0,
        daily_pnl=0.0,
        slippage_per_unit=1.0,
        round_turn_cost=1.5,
    )


def test_runner_guard_allows_blocks_restarts_and_rejects_equal_timestamp(
    tmp_path,
) -> None:
    specification = _specification()
    state_path = tmp_path / "prior_trade_guard_state.json"
    runner = ShadowRunner(
        specification,
        prior_trade_guard_state_path=state_path,
        initialize_prior_trade_guard_genesis=True,
    )
    assert runner.prior_trade_guard_reconciliation == "EXPLICIT_HASHED_GENESIS"
    first_at = datetime(2026, 7, 11, 10, 0, tzinfo=timezone.utc)
    first = _process(runner, first_at)
    assert first["status"] == "VIRTUAL_FILLED"
    first_id = str(first["fill"]["signal_id"])
    completed_at = first_at + timedelta(minutes=1)
    runner.record_completed_virtual_trade(
        signal_id=first_id,
        completed_at=completed_at,
        net_pnl=5.0,
    )

    restarted = ShadowRunner(
        specification,
        prior_trade_guard_state_path=state_path,
    )
    assert restarted.prior_trade_guard_reconciliation == "HASHED_STATE_RESTORED"
    equal = _process(restarted, completed_at)
    assert equal["status"] == "REJECTED"
    assert equal["reason"] == "NON_PRIOR_COMPLETED_TRADE_FAIL_CLOSED"

    second_at = completed_at + timedelta(minutes=1)
    second = _process(restarted, second_at)
    assert second["status"] == "VIRTUAL_FILLED"
    second_id = str(second["fill"]["signal_id"])
    second_completed_at = second_at + timedelta(minutes=1)
    restarted.record_completed_virtual_trade(
        signal_id=second_id,
        completed_at=second_completed_at,
        net_pnl=-10.0,
    )
    blocked = _process(restarted, second_completed_at + timedelta(minutes=1))
    assert blocked["status"] == "REJECTED"
    assert blocked["reason"] == "PRIOR_WINDOW_BELOW_FROZEN_THRESHOLD"

    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert len(state["completed_trades"]) == 2
    assert len(state["state_hash"]) == 64
    audit_rows = [
        json.loads(row)
        for row in (tmp_path / "prior_trade_guard_state.json.audit.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
    ]
    assert any(row["event"] == "PRIOR_TRADE_GUARD_EXPLICIT_HASHED_GENESIS" for row in audit_rows)
    assert any(row["event"] == "PRIOR_TRADE_GUARD_HASHED_STATE_RESTORED" for row in audit_rows)
    assert sum(
        row["event"] == "VIRTUAL_TRADE_COMPLETED_AND_GUARD_PERSISTED"
        for row in audit_rows
    ) == 2
    assert all(
        row["previous_event_hash"] == "GENESIS"
        if index == 0
        else row["previous_event_hash"] == audit_rows[index - 1]["event_hash"]
        for index, row in enumerate(audit_rows)
    )
    assert not hasattr(restarted.execution, "submit_order")
    assert not hasattr(restarted, "broker")


def test_guard_state_updates_only_for_known_completed_virtual_fills(tmp_path) -> None:
    runner = ShadowRunner(
        _specification(),
        prior_trade_guard_state_path=tmp_path / "guard.json",
        initialize_prior_trade_guard_genesis=True,
    )
    with pytest.raises(PriorTradeGuardError, match="known open virtual fill"):
        runner.record_completed_virtual_trade(
            signal_id="never-filled",
            completed_at=datetime(2026, 7, 11, tzinfo=timezone.utc),
            net_pnl=1.0,
        )
    decision_at = datetime(2026, 7, 11, 10, 0, tzinfo=timezone.utc)
    fill = _process(runner, decision_at)
    with pytest.raises(PriorTradeGuardError, match="strictly after"):
        runner.record_completed_virtual_trade(
            signal_id=str(fill["fill"]["signal_id"]),
            completed_at=decision_at,
            net_pnl=1.0,
        )


def test_guarded_runner_without_state_path_fails_closed() -> None:
    runner = ShadowRunner(_specification())
    decision_at = datetime(2026, 7, 11, 10, 0, tzinfo=timezone.utc)
    result = _process(runner, decision_at)
    assert result["status"] == "REJECTED"
    assert result["reason"] == "MISSING_RECONCILED_GUARD_STATE_FAIL_CLOSED"


def test_shadow_command_initializes_once_then_missing_state_fails_closed(
    tmp_path,
) -> None:
    configuration_path = tmp_path / "guarded_shadow.json"
    specification = _specification()
    specification.write_immutable(configuration_path)
    state_dir = tmp_path / "state"
    command = [
        sys.executable,
        "scripts/run_shadow_portfolio.py",
        "--configuration",
        str(configuration_path),
        "--state-dir",
        str(state_dir),
    ]
    first = subprocess.run(command, check=True, capture_output=True, text=True)
    first_status = json.loads(first.stdout)
    assert first_status["prior_trade_guard_reconciliation"] == "EXPLICIT_HASHED_GENESIS"
    state_path = state_dir / f"{specification.strategy_id}.prior_trade_guard.json"
    marker_path = state_dir / (
        f"{specification.strategy_id}.prior_trade_guard_initialized.json"
    )
    assert state_path.is_file() and marker_path.is_file()

    state_path.unlink()
    second = subprocess.run(command, check=True, capture_output=True, text=True)
    second_status = json.loads(second.stdout)
    assert second_status["prior_trade_guard_reconciliation"] == "MISSING_STATE_FAIL_CLOSED"
    assert second_status["prior_trade_guard_fail_closed"] is True
    assert not state_path.exists()
