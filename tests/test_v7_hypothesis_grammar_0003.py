from __future__ import annotations

import numpy as np

from hydra.research.v7_hypothesis_grammar import MARKETS, V7MarketBars
from hydra.research.v7_hypothesis_grammar_0003 import (
    candidate_specs,
    generate_signal_population,
)


def test_grammar_0003_has_fixed_distinct_structures() -> None:
    specs = candidate_specs()

    assert len(specs) == 19
    assert len({row.candidate_id for row in specs}) == 19
    assert len({row.specification_hash for row in specs}) == 19
    assert {row.market for row in specs} == set(MARKETS)


def test_grammar_0003_is_deterministic_past_only_and_outcome_free() -> None:
    bars = {market: _synthetic_bars(market) for market in MARKETS}

    first = generate_signal_population(bars, graveyard_path=None)
    second = generate_signal_population(bars, graveyard_path=None)

    assert first == second
    assert sum(len(rows) for rows in first.values()) > 0
    for signals in first.values():
        assert all(signal.availability_ns <= signal.decision_ns for signal in signals)
        assert all(signal.decision_ns <= signal.entry_ns < signal.exit_ns for signal in signals)


def test_grammar_0003_rejects_current_execution_roll_crossing() -> None:
    bars = {market: _synthetic_bars(market) for market in MARKETS}
    es = bars["ES"]
    es.segment_code[es.local_minute >= 12 * 60] = 2

    population = generate_signal_population(bars, graveyard_path=None)

    assert population["v7g3_turn_month_ES"] == ()
    assert population["v7g3_underreaction_ES"] == ()


def _synthetic_bars(market: str) -> V7MarketBars:
    minutes = list(range(17 * 60, 24 * 60)) + list(range(0, 15 * 60 + 10))
    timestamps: list[int] = []
    session_days: list[int] = []
    local_minutes: list[int] = []
    for offset in range(140):
        for position, minute in enumerate(minutes):
            timestamps.append(
                (offset * len(minutes) + position) * 60_000_000_000
            )
            session_days.append(19_360 + offset)
            local_minutes.append(minute)
    timestamp = np.asarray(timestamps, dtype=np.int64)
    count = len(timestamp)
    day_index = np.repeat(np.arange(140), len(minutes))
    phase = np.asarray(local_minutes, dtype=float)
    close = 1000.0 + 0.4 * day_index + np.sin(np.arange(count) / 31.0) + 0.001 * phase
    open_ = np.concatenate(([close[0]], close[:-1]))
    high = np.maximum(open_, close) + 0.25
    low = np.minimum(open_, close) - 0.25
    return V7MarketBars(
        market=market,
        timestamp_ns=timestamp,
        decision_ns=timestamp + 60_000_000_000,
        availability_ns=timestamp + 60_000_000_000,
        session_day=np.asarray(session_days, dtype=np.int32),
        contract_code=np.zeros(count, dtype=np.int16),
        segment_code=np.zeros(count, dtype=np.int64),
        open=open_,
        high=high,
        low=low,
        close=close,
        local_minute=np.asarray(local_minutes, dtype=np.int16),
        local_weekday=np.zeros(count, dtype=np.int8),
        bundle_hash="c" * 64,
    )
