from __future__ import annotations

import numpy as np

from hydra.research.v7_hypothesis_grammar import (
    MARKETS,
    V7MarketBars,
    candidate_specs,
    generate_signal_population,
)


def test_grammar_has_exactly_one_structure_per_preregistered_cell() -> None:
    specs = candidate_specs()

    assert len(specs) == 24
    assert len({row.candidate_id for row in specs}) == 24
    assert {row.market for row in specs} == set(MARKETS)
    assert all(len(row.specification_hash) == 64 for row in specs)


def test_signal_type_contains_no_future_outcome_or_pnl_field() -> None:
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


def test_population_generation_is_deterministic_and_past_only() -> None:
    bars = {market: _synthetic_bars(market) for market in MARKETS}

    first = generate_signal_population(bars, graveyard_path=None)
    second = generate_signal_population(bars, graveyard_path=None)

    assert first == second
    assert set(first) == {row.candidate_id for row in candidate_specs()}
    for signals in first.values():
        assert all(signal.availability_ns <= signal.decision_ns for signal in signals)
        assert all(signal.decision_ns <= signal.entry_ns < signal.exit_ns for signal in signals)


def test_cross_market_signal_cannot_cross_target_segment() -> None:
    bars = {market: _synthetic_bars(market) for market in MARKETS}
    target = bars["CL"]
    # Force every possible 09:01-to-10:00 target interval to cross a segment.
    target.segment_code[target.local_minute >= 9 * 60 + 30] = 2

    population = generate_signal_population(bars, graveyard_path=None)

    assert population["v7g1_risk_transfer_ES_CL"] == ()


def _synthetic_bars(market: str) -> V7MarketBars:
    # Seventy complete Chicago trading sessions provide the fixed rolling
    # histories without embedding any candidate outcome in the generator.
    timestamps: list[int] = []
    session_days: list[int] = []
    local_minutes: list[int] = []
    weekdays: list[int] = []
    start_day = 19_360
    for offset in range(75):
        day = start_day + offset
        weekday = (offset + 1) % 7
        # Full 17:00-to-15:09 session represented in chronological order.
        minutes = list(range(17 * 60, 24 * 60)) + list(range(0, 15 * 60 + 10))
        base_ns = offset * len(minutes) * 60_000_000_000
        for position, minute in enumerate(minutes):
            timestamps.append(base_ns + position * 60_000_000_000)
            session_days.append(day)
            local_minutes.append(minute)
            weekdays.append(weekday)
    timestamp = np.asarray(timestamps, dtype=np.int64)
    count = len(timestamp)
    drift = np.linspace(0.0, 100.0, count)
    wave = np.sin(np.arange(count) / 13.0)
    close = 1000.0 + drift + wave
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
        bundle_hash="a" * 64,
    )
