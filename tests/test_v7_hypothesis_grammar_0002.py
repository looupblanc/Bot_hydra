from __future__ import annotations

import numpy as np

from hydra.research.v7_hypothesis_grammar import MARKETS, V7MarketBars
from hydra.research.v7_hypothesis_grammar_0002 import (
    candidate_specs,
    generate_signal_population,
)


def test_grammar_0002_has_only_fixed_structural_cells() -> None:
    specs = candidate_specs()

    assert len(specs) == 23
    assert len({row.candidate_id for row in specs}) == 23
    assert len({row.specification_hash for row in specs}) == 23
    assert {row.market for row in specs} == set(MARKETS)


def test_grammar_0002_signal_schema_has_no_outcomes() -> None:
    from hydra.research.v7_hypothesis_grammar import V7Signal

    fields = set(V7Signal.__dataclass_fields__)
    assert not fields & {
        "pnl",
        "net_pnl",
        "gross_pnl",
        "future_return",
        "target",
        "label",
        "mfe",
        "mae",
    }


def test_grammar_0002_is_deterministic_and_past_only() -> None:
    bars = {market: _synthetic_bars(market) for market in MARKETS}

    first = generate_signal_population(bars, graveyard_path=None)
    second = generate_signal_population(bars, graveyard_path=None)

    assert first == second
    assert set(first) == {row.candidate_id for row in candidate_specs()}
    assert sum(len(rows) for rows in first.values()) > 0
    for signals in first.values():
        assert all(signal.availability_ns <= signal.decision_ns for signal in signals)
        assert all(signal.decision_ns <= signal.entry_ns < signal.exit_ns for signal in signals)


def test_grammar_0002_never_executes_across_roll_segment() -> None:
    bars = {market: _synthetic_bars(market) for market in MARKETS}
    es = bars["ES"]
    es.segment_code[es.local_minute >= 12 * 60] = 2

    population = generate_signal_population(bars, graveyard_path=None)

    assert population["v7g2_afternoon_reacceleration_ES"] == ()


def _synthetic_bars(market: str) -> V7MarketBars:
    timestamps: list[int] = []
    session_days: list[int] = []
    local_minutes: list[int] = []
    weekdays: list[int] = []
    start_day = 19_360
    minutes = list(range(17 * 60, 24 * 60)) + list(range(0, 15 * 60 + 10))
    for offset in range(90):
        day = start_day + offset
        weekday = (offset + 1) % 7
        base_ns = offset * len(minutes) * 60_000_000_000
        for position, minute in enumerate(minutes):
            timestamps.append(base_ns + position * 60_000_000_000)
            session_days.append(day)
            local_minutes.append(minute)
            weekdays.append(weekday)
    timestamp = np.asarray(timestamps, dtype=np.int64)
    count = len(timestamp)
    phase = np.asarray(local_minutes, dtype=float)
    day_index = np.repeat(np.arange(90), len(minutes))
    close = (
        1000.0
        + day_index * 0.2
        + np.sin(np.arange(count) / 17.0)
        + 0.002 * phase
    )
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
        local_weekday=np.asarray(weekdays, dtype=np.int8),
        bundle_hash="b" * 64,
    )
