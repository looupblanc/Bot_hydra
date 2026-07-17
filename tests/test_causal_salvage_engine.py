from __future__ import annotations

from dataclasses import replace
import json

import numpy as np

from hydra.account_policy.active_risk_pool import (
    ActiveRiskPoolPolicy,
    ConcurrencyScaling,
    SameInstrumentConflictRule,
    TargetProtectionMode,
)
from hydra.account_policy.causal_active_pool_replay import (
    run_causal_shared_account_episode,
)
from hydra.economic_evolution.schema import EconomicRole, SleeveSpec
from hydra.features.feature_matrix import FeatureMatrix
from hydra.propfirm.combine_episode import TradePathEvent
from hydra.research.causal_sleeve_replay import (
    CENSORED_FUTURE_COVERAGE,
    CausalFillPolicy,
    CausalInputOrderGuard,
    CausalSleeveCheckpoint,
    CausalSleeveStreamingKernel,
    CausalTradeMark,
    CausalTradeTrajectory,
    iter_causal_bar_records,
    replay_causal_sleeve_batch,
    replay_causal_sleeve_streaming,
)
from hydra.shadow.active_risk_package import FrozenSignalBinding


MINUTE_NS = 60_000_000_000
DAY = int(np.datetime64("2023-01-03", "D").astype(np.int64))
SHA = "a" * 64


def _binding() -> FrozenSignalBinding:
    return FrozenSignalBinding(
        sleeve_id="sleeve-causal",
        trigger_feature="trigger",
        trigger_operator="GT",
        trigger_threshold=0.5,
        context_feature=None,
        context_operator=None,
        context_threshold=None,
        calibration_start="2023-01-01",
        calibration_end_exclusive="2024-10-01",
        trigger_finite_observation_count=100,
        context_finite_observation_count=None,
        source_execution_fingerprint=SHA,
        source_cheap_screen_path="reports/source.jsonl",
        source_cheap_screen_sha256=SHA,
        source_cheap_screen_row_sha256=SHA,
        feature_matrix_manifest_path="data/cache/features/manifest.json",
        feature_matrix_manifest_sha256=SHA,
        feature_matrix_schema="test",
        feature_matrix_bundle_hash=SHA,
        feature_matrix_source_data_sha256=SHA,
        feature_matrix_roll_map_sha256=SHA,
        feature_matrix_market="ES",
        feature_matrix_execution_market="MES",
        feature_bundle_version="test",
        feature_dag_hash=SHA,
        trigger_array_sha256=SHA,
        context_array_sha256=None,
        session_day_array_sha256=SHA,
        session_code_array_sha256=SHA,
    )


def _spec() -> SleeveSpec:
    return SleeveSpec(
        sleeve_id="sleeve-causal",
        component_ids=("component-causal",),
        market="ES",
        execution_market="MES",
        timeframe="5m",
        session_code=0,
        trigger_feature="trigger",
        trigger_operator="GT",
        trigger_quantile=0.5,
        context_feature=None,
        context_operator=None,
        context_quantile=None,
        side=1,
        holding_bars=5,
        exit_style="TIME_ONLY",
        role=EconomicRole.PRIMARY_ALPHA,
        source_campaign="test",
        lineage_id="lineage-causal",
    )


def _matrix(*, gap_after: int | None = None, forward_finite: bool = True) -> FeatureMatrix:
    rows = 14
    timestamp = np.arange(rows, dtype=np.int64) * MINUTE_NS
    segment = np.zeros(rows, dtype=np.int64)
    if gap_after is not None:
        timestamp[gap_after + 1 :] += MINUTE_NS
        segment[gap_after + 1 :] = 1
    trigger = np.zeros(rows, dtype=float)
    trigger[0] = 1.0
    trigger[7] = 1.0
    forward = np.ones(rows, dtype=float)
    if not forward_finite:
        forward[:] = np.nan
    arrays = {
        "timestamp_ns": timestamp,
        "decision_ns": timestamp + MINUTE_NS,
        "availability_ns": timestamp + MINUTE_NS,
        "segment_code": segment,
        "contract_code": np.zeros(rows, dtype=np.int16),
        "session_day": np.full(rows, DAY, dtype=np.int32),
        "session_code": np.zeros(rows, dtype=np.int16),
        "bar_open": np.arange(rows, dtype=float) + 100.0,
        "bar_high": np.arange(rows, dtype=float) + 101.0,
        "bar_low": np.arange(rows, dtype=float) + 99.0,
        "bar_close": np.arange(rows, dtype=float) + 100.5,
        "feature__trigger": trigger,
        "feature__ctx_60m_volatility_expansion": np.ones(rows, dtype=float),
        "forward_move__5": forward,
    }
    for value in arrays.values():
        value.flags.writeable = False
    return FeatureMatrix(
        root=None,  # type: ignore[arg-type]
        manifest={"row_count": rows, "bundle_hash": "synthetic"},
        arrays=arrays,
    )


def test_future_label_availability_cannot_change_signal_decisions() -> None:
    finite = replay_causal_sleeve_batch(_spec(), _binding(), _matrix())
    missing = replay_causal_sleeve_batch(
        _spec(), _binding(), _matrix(forward_finite=False)
    )
    assert finite.decision_hash == missing.decision_hash
    assert finite.signal_count == missing.signal_count == 2
    assert finite.completed_trade_count == missing.completed_trade_count == 2


def test_missing_exact_fill_is_censored_after_signal_not_suppressed() -> None:
    replay = replay_causal_sleeve_batch(
        _spec(), _binding(), _matrix(gap_after=0)
    )
    assert replay.signal_count == 2
    assert replay.censored_signal_count >= 1
    assert replay.signals[0].outcome_status == CENSORED_FUTURE_COVERAGE
    assert replay.signals[0].fill_time_ns is None


def test_contract_roll_between_decision_and_fill_is_censored() -> None:
    matrix = _matrix()
    arrays = dict(matrix.arrays)
    contracts = arrays["contract_code"].copy()
    segments = arrays["segment_code"].copy()
    contracts[1:] = 1
    segments[1:] = 1
    arrays["contract_code"] = contracts
    arrays["segment_code"] = segments
    rolled = FeatureMatrix(root=None, manifest=matrix.manifest, arrays=arrays)  # type: ignore[arg-type]
    replay = replay_causal_sleeve_batch(_spec(), _binding(), rolled)
    assert replay.signals[0].outcome_status == CENSORED_FUTURE_COVERAGE
    assert "ROLL" in str(replay.signals[0].censor_reason)


def test_session_transition_between_decision_and_fill_is_censored() -> None:
    matrix = _matrix()
    arrays = dict(matrix.arrays)
    days = arrays["session_day"].copy()
    segments = arrays["segment_code"].copy()
    days[1:] = DAY + 1
    segments[1:] = 1
    arrays["session_day"] = days
    arrays["segment_code"] = segments
    transitioned = FeatureMatrix(root=None, manifest=matrix.manifest, arrays=arrays)  # type: ignore[arg-type]
    replay = replay_causal_sleeve_batch(_spec(), _binding(), transitioned)
    assert replay.signals[0].outcome_status == CENSORED_FUTURE_COVERAGE
    assert "SESSION" in str(replay.signals[0].censor_reason)


def test_batch_and_streaming_use_the_same_decision_and_fill_path() -> None:
    batch = replay_causal_sleeve_batch(_spec(), _binding(), _matrix())
    stream = replay_causal_sleeve_streaming(_spec(), _binding(), _matrix())
    assert batch.decision_hash == stream.decision_hash
    assert batch.normal_event_hash == stream.normal_event_hash
    assert batch.stressed_event_hash == stream.stressed_event_hash
    first = batch.signals[0]
    assert first.signal_time_ns == MINUTE_NS
    assert first.decision_time_ns == MINUTE_NS
    assert first.fill_time_ns == MINUTE_NS
    assert first.exit_fill_time_ns == 6 * MINUTE_NS
    assert first.normal_entry_fill_price == 101.25
    assert first.stressed_entry_fill_price == 101.375
    assert first.normal_exit_fill_price == 105.75
    assert first.stressed_exit_fill_price == 105.625
    trajectory = batch.normal_trajectories[0]
    assert trajectory.initial_unrealized_pnl == -2.49
    assert all(mark.current_unrealized_pnl is not None for mark in trajectory.marks)
    # Gross is raw-open to raw-open. Net alone carries entry/exit slippage and fee.
    event = trajectory.event
    assert event.gross_pnl == 25.0
    assert event.net_pnl == 21.26
    assert np.isclose(event.gross_pnl - event.net_pnl, 3.74)


def test_duplicate_restart_and_repeated_resume_are_idempotent() -> None:
    guard = CausalInputOrderGuard()
    assert guard.accept(timestamp_ns=MINUTE_NS, segment_code=1, contract_code=2)
    checkpoint = guard.checkpoint()
    resumed = CausalInputOrderGuard(checkpoint)
    assert not resumed.accept(
        timestamp_ns=MINUTE_NS, segment_code=1, contract_code=2
    )
    second_checkpoint = resumed.checkpoint()
    repeated = CausalInputOrderGuard(second_checkpoint)
    assert not repeated.accept(
        timestamp_ns=MINUTE_NS, segment_code=1, contract_code=2
    )
    assert repeated.accept(
        timestamp_ns=2 * MINUTE_NS, segment_code=1, contract_code=2
    )
    with np.testing.assert_raises_regex(ValueError, "OUT_OF_ORDER_CAUSAL_BAR"):
        repeated.accept(timestamp_ns=MINUTE_NS, segment_code=1, contract_code=2)


def test_true_step_checkpoint_resume_and_duplicate_are_exact() -> None:
    records = list(iter_causal_bar_records(_binding(), _matrix()))
    uninterrupted = CausalSleeveStreamingKernel(_spec(), _binding())
    for record in records:
        assert uninterrupted.step(record)
    expected = uninterrupted.finalize()

    partial = CausalSleeveStreamingKernel(_spec(), _binding())
    for record in records[:4]:
        assert partial.step(record)
    assert partial.pending is None
    assert partial.open_trade is not None
    encoded = json.loads(json.dumps(partial.checkpoint().to_dict()))
    checkpoint = CausalSleeveCheckpoint.from_mapping(encoded)
    resumed = CausalSleeveStreamingKernel.from_checkpoint(
        _spec(), _binding(), checkpoint
    )
    assert not resumed.step(records[3])
    for record in records[4:]:
        assert resumed.step(record)
    actual = resumed.finalize()
    assert actual.decision_hash == expected.decision_hash
    assert actual.normal_event_hash == expected.normal_event_hash
    assert actual.stressed_event_hash == expected.stressed_event_hash
    assert (
        actual.normal_censored_trajectory_hash
        == expected.normal_censored_trajectory_hash
    )

    altered = replace(records[-1], bar_close=records[-1].bar_close + 1.0)
    with np.testing.assert_raises_regex(
        ValueError, "ALTERED_DUPLICATE_OR_TIMESTAMP_COLLISION"
    ):
        resumed.step(altered)


def test_filled_then_missing_exit_is_retained_as_censored_trajectory() -> None:
    replay = replay_causal_sleeve_batch(
        _spec(), _binding(), _matrix(gap_after=3)
    )
    signal = replay.signals[0]
    assert signal.fill_time_ns == MINUTE_NS
    assert signal.outcome_status == CENSORED_FUTURE_COVERAGE
    assert signal.censor_time_ns == 4 * MINUTE_NS
    assert len(replay.normal_censored_trajectories) >= 1
    normal = replay.normal_censored_trajectories[0]
    stressed = replay.stressed_censored_trajectories[0]
    assert not normal.completed
    assert normal.terminal_status == CENSORED_FUTURE_COVERAGE
    assert normal.event.decision_ns == MINUTE_NS
    assert normal.event.quantity == 1
    assert normal.event.mini_equivalent == 0.1
    assert normal.initial_unrealized_pnl == -2.49
    assert stressed.initial_unrealized_pnl == -3.115
    assert normal.marks[-1].current_unrealized_pnl is not None
    assert np.isclose(normal.event.net_pnl, 10.01)
    assert np.isclose(normal.event.gross_pnl, 12.5)
    assert np.isclose(normal.event.gross_pnl - normal.event.net_pnl, 2.49)


def test_signal_time_uses_availability_and_never_fills_before_it() -> None:
    matrix = _matrix()
    arrays = dict(matrix.arrays)
    availability = arrays["availability_ns"].copy()
    availability[0] += MINUTE_NS // 2
    arrays["availability_ns"] = availability
    delayed = FeatureMatrix(root=None, manifest=matrix.manifest, arrays=arrays)  # type: ignore[arg-type]
    replay = replay_causal_sleeve_batch(_spec(), _binding(), delayed)
    first = replay.signals[0]
    assert first.signal_time_ns == MINUTE_NS + MINUTE_NS // 2
    assert first.decision_time_ns == first.signal_time_ns
    assert first.order_submit_time_ns == first.signal_time_ns
    assert first.earliest_executable_time_ns == 2 * MINUTE_NS
    assert first.fill_time_ns == 2 * MINUTE_NS


def test_resolved_fill_hash_binds_frozen_costs_and_ticks() -> None:
    policy = CausalFillPolicy()
    mes = policy.resolved_payload("MES", 5)
    mcl = policy.resolved_payload("MCL", 5)
    mes_60m = policy.resolved_payload("MES", 60)
    assert mes["normal_slippage_ticks_per_side"] == 1.0
    assert mes["stressed_slippage_ticks_per_side"] == 1.5
    assert mes_60m["normal_slippage_ticks_per_side"] == 0.75
    assert mes_60m["stressed_slippage_ticks_per_side"] == 1.125
    assert mes_60m["holding_horizon"] == "60m"
    assert mes["commission_round_turn_usd"] == 1.24
    assert mcl["commission_round_turn_usd"] == 1.54
    assert policy.resolved_fingerprint("MES", 5) != policy.resolved_fingerprint(
        "MCL", 5
    )
    assert policy.resolved_fingerprint("MES", 5) != policy.resolved_fingerprint(
        "MES", 60
    )


def _policy() -> ActiveRiskPoolPolicy:
    ids = ("sleeve-a", "sleeve-b")
    return ActiveRiskPoolPolicy(
        policy_id="causal-policy",
        component_priority=ids,
        nominal_risk_charge_per_mini=((ids[0], 100.0), (ids[1], 100.0)),
        maximum_concurrent_sleeves=2,
        aggregate_open_risk_ceiling=1000.0,
        maximum_mll_buffer_fraction=1.0,
        protected_mll_buffer=0.0,
        maximum_mini_equivalent=15.0,
        concurrency_scaling=ConcurrencyScaling.PROPORTIONAL,
        same_instrument_conflict_rule=SameInstrumentConflictRule.PRIORITY,
        daily_loss_guard=4500.0,
        daily_consistency_profit_guard=4500.0,
        target_protection_distance=0.0,
        target_protection_mode=TargetProtectionMode.NONE,
        static_risk_tier=1.0,
    )


def _trajectory(
    component: str,
    market: str,
    entry: int,
    exit_: int,
    marks: tuple[tuple[int, float, float], ...],
) -> CausalTradeTrajectory:
    event = TradePathEvent(
        event_id=f"{component}-event",
        decision_ns=entry,
        exit_ns=exit_,
        session_day=DAY,
        net_pnl=100.0,
        gross_pnl=101.24,
        worst_unrealized_pnl=min(row[1] for row in marks),
        best_unrealized_pnl=max(row[2] for row in marks),
        quantity=1,
        mini_equivalent=0.1,
    )
    return CausalTradeTrajectory(
        component_id=component,
        market=market,
        side=1,
        event=event,
        marks=tuple(
            CausalTradeMark(
                availability_time_ns=row[0],
                worst_unrealized_pnl=row[1],
                best_unrealized_pnl=row[2],
                current_unrealized_pnl=(row[1] + row[2]) / 2.0,
            )
            for row in marks
        ),
    )


def test_future_mark_cannot_change_an_earlier_governor_decision() -> None:
    first = _trajectory(
        "sleeve-a",
        "MES",
        MINUTE_NS,
        4 * MINUTE_NS,
        (
            (2 * MINUTE_NS, -10.0, 10.0),
            (3 * MINUTE_NS, -20.0, 20.0),
            (4 * MINUTE_NS, -30.0, 30.0),
        ),
    )
    changed = replace(
        first,
        marks=(
            first.marks[0],
            replace(first.marks[1], best_unrealized_pnl=4000.0),
            first.marks[2],
        ),
    )
    second = _trajectory(
        "sleeve-b",
        "MNQ",
        2 * MINUTE_NS,
        4 * MINUTE_NS,
        (
            (3 * MINUTE_NS, -10.0, 10.0),
            (4 * MINUTE_NS, -10.0, 10.0),
        ),
    )
    original = run_causal_shared_account_episode(
        {"sleeve-a": (first,), "sleeve-b": (second,)},
        (DAY,),
        policy=_policy(),
        start_day=DAY,
        maximum_duration_days=1,
    )
    mutated = run_causal_shared_account_episode(
        {"sleeve-a": (changed,), "sleeve-b": (second,)},
        (DAY,),
        policy=_policy(),
        start_day=DAY,
        maximum_duration_days=1,
    )
    before = [
        row
        for row in original.risk_allocation_path
        if row["component_id"] == "sleeve-b"
    ][0]
    after = [
        row
        for row in mutated.risk_allocation_path
        if row["component_id"] == "sleeve-b"
    ][0]
    assert before["allow"] == after["allow"]
    assert before["quantity"] == after["quantity"]
