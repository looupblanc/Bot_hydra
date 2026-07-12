from __future__ import annotations

import numpy as np

from hydra.propfirm.combine_episode import TradePathEvent
from hydra.research.v7_hypothesis_grammar import V7MarketBars, V7Signal
from hydra.validation.v7_grammar_0002_validation import (
    _invert_event,
    _is_sim_exploit,
    _shift_signals_by_minutes,
    _shift_signals_by_sessions,
)


def test_sim_exploit_requires_a_measured_positive_base_edge() -> None:
    assert not _is_sim_exploit(0.0, 0.0)
    assert not _is_sim_exploit(-1.0, -2.0)
    assert not _is_sim_exploit(1.0, 0.1)
    assert _is_sim_exploit(1.0, 0.0)
    assert _is_sim_exploit(1.0, -0.1)


def test_sign_inversion_preserves_cost_and_reverses_path() -> None:
    event = TradePathEvent(
        event_id="base",
        decision_ns=1,
        exit_ns=2,
        session_day=10,
        gross_pnl=100.0,
        net_pnl=90.0,
        worst_unrealized_pnl=-60.0,
        best_unrealized_pnl=140.0,
        quantity=1,
        mini_equivalent=1.0,
    )

    inverted = _invert_event(event)

    assert inverted.gross_pnl == -100.0
    assert inverted.net_pnl == -110.0
    assert inverted.worst_unrealized_pnl == -160.0
    assert inverted.best_unrealized_pnl == 40.0


def test_shifted_nulls_preserve_side_and_use_closed_decision_bar() -> None:
    bars = _bars()
    signal = _signal(bars, day=100, entry_minute=9 * 60, exit_minute=10 * 60)

    delayed = _shift_signals_by_sessions([signal], bars, sessions=5)
    clock = _shift_signals_by_minutes([signal], bars, minutes=30)

    assert len(delayed) == 1
    assert delayed[0].session_day == 105
    assert delayed[0].side == signal.side
    assert delayed[0].availability_ns <= delayed[0].decision_ns
    assert delayed[0].decision_ns <= delayed[0].entry_ns
    assert len(clock) == 1
    assert bars.local_minute[clock[0].entry_index] == 9 * 60 + 30
    assert bars.local_minute[clock[0].exit_index] == 10 * 60 + 30


def test_null_shift_fails_closed_on_roll_crossing() -> None:
    bars = _bars()
    signal = _signal(bars, day=100, entry_minute=9 * 60, exit_minute=10 * 60)
    target = np.flatnonzero(bars.session_day == 105)
    bars.segment_code[target[bars.local_minute[target] >= 9 * 60 + 30]] = 2

    delayed = _shift_signals_by_sessions([signal], bars, sessions=5)

    assert delayed == ()


def _signal(
    bars: V7MarketBars, *, day: int, entry_minute: int, exit_minute: int
) -> V7Signal:
    positions = np.flatnonzero(bars.session_day == day)
    decision = int(positions[bars.local_minute[positions] == entry_minute - 1][0])
    entry = int(positions[bars.local_minute[positions] == entry_minute][0])
    exit_index = int(positions[bars.local_minute[positions] == exit_minute][0])
    return V7Signal(
        candidate_id="candidate",
        hypothesis_id="hypothesis",
        market="ES",
        source_market=None,
        session_day=day,
        side=1,
        decision_ns=int(bars.decision_ns[decision]),
        availability_ns=int(bars.availability_ns[decision]),
        entry_index=entry,
        exit_index=exit_index,
        entry_ns=int(bars.timestamp_ns[entry]),
        exit_ns=int(bars.timestamp_ns[exit_index] + 60_000_000_000),
        contract_code=0,
        segment_code=0,
        feature_snapshot_hash="a" * 64,
    )


def _bars() -> V7MarketBars:
    minutes = list(range(17 * 60, 24 * 60)) + list(range(0, 15 * 60 + 10))
    timestamp: list[int] = []
    session_day: list[int] = []
    local_minute: list[int] = []
    for offset in range(10):
        for position, minute in enumerate(minutes):
            timestamp.append(
                (offset * len(minutes) + position) * 60_000_000_000
            )
            session_day.append(100 + offset)
            local_minute.append(minute)
    ts = np.asarray(timestamp, dtype=np.int64)
    count = len(ts)
    price = 1000.0 + np.arange(count) * 0.001
    return V7MarketBars(
        market="ES",
        timestamp_ns=ts,
        decision_ns=ts + 60_000_000_000,
        availability_ns=ts + 60_000_000_000,
        session_day=np.asarray(session_day, dtype=np.int32),
        contract_code=np.zeros(count, dtype=np.int16),
        segment_code=np.zeros(count, dtype=np.int64),
        open=price,
        high=price + 0.25,
        low=price - 0.25,
        close=price,
        local_minute=np.asarray(local_minute, dtype=np.int16),
        local_weekday=np.zeros(count, dtype=np.int8),
        bundle_hash="b" * 64,
    )
