from __future__ import annotations

import numpy as np

from hydra.research.v7_hypothesis_grammar import V7MarketBars, V7Signal
from hydra.validation.v7_grammar_0003_validation import (
    _matched_non_signal_day_signals,
)


def test_matched_non_signal_days_are_later_unique_and_not_signal_days() -> None:
    bars = _bars()
    signals = (
        _signal(bars, day=100, side=1),
        _signal(bars, day=102, side=-1),
    )

    matched = _matched_non_signal_day_signals(signals, bars)

    assert [row.session_day for row in matched] == [101, 103]
    assert [row.side for row in matched] == [1, -1]
    assert len({row.session_day for row in matched}) == len(matched)
    assert not ({row.session_day for row in matched} & {100, 102})
    assert all(row.availability_ns <= row.decision_ns for row in matched)
    assert all(row.decision_ns <= row.entry_ns < row.exit_ns for row in matched)


def test_matched_non_signal_day_fails_closed_on_segment_break() -> None:
    bars = _bars()
    signal = _signal(bars, day=100, side=1)
    target = np.flatnonzero(bars.session_day == 101)
    bars.segment_code[target[bars.local_minute[target] >= 9 * 60 + 30]] = 2

    matched = _matched_non_signal_day_signals((signal,), bars)

    assert [row.session_day for row in matched] == [102]


def test_no_later_non_signal_day_returns_no_control() -> None:
    bars = _bars(day_count=3)
    signals = (
        _signal(bars, day=100, side=1),
        _signal(bars, day=101, side=1),
        _signal(bars, day=102, side=1),
    )

    assert _matched_non_signal_day_signals(signals, bars) == ()


def _signal(bars: V7MarketBars, *, day: int, side: int) -> V7Signal:
    positions = np.flatnonzero(bars.session_day == day)
    decision = int(positions[bars.local_minute[positions] == 8 * 60 + 29][0])
    entry = int(positions[bars.local_minute[positions] == 8 * 60 + 30][0])
    exit_index = int(positions[bars.local_minute[positions] == 15 * 60 + 8][0])
    return V7Signal(
        candidate_id="candidate",
        hypothesis_id="hypothesis",
        market="ES",
        source_market=None,
        session_day=day,
        side=side,
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


def _bars(*, day_count: int = 6) -> V7MarketBars:
    minutes = list(range(8 * 60, 15 * 60 + 10))
    timestamp: list[int] = []
    session_day: list[int] = []
    local_minute: list[int] = []
    for offset in range(day_count):
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
