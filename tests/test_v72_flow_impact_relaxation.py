from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from hydra.research.v72_flow_impact_relaxation import (
    build_flow_impact_states,
    candidate_specs,
    generate_signal_population,
    signal_path_hash,
)


ROOT = Path(__file__).resolve().parents[1]
MINUTE_NS = 60_000_000_000


def test_frozen_candidate_population_is_complete_and_deterministic() -> None:
    left = candidate_specs(ROOT)
    right = candidate_specs(ROOT)
    assert len(left) == 36
    assert len({row.candidate_id for row in left}) == 36
    assert [row.specification_hash for row in left] == [
        row.specification_hash for row in right
    ]
    assert {row.response_window_minutes for row in left} == {2, 4}
    assert {row.holding_minutes for row in left} == {30, 60}


def test_response_state_uses_only_completed_minutes_and_delays_entry() -> None:
    minute = _minute_frame()
    states, audit = build_flow_impact_states(minute)
    assert audit["minute_count"] == len(minute)
    assert audit["impulse_count"] >= 1
    row = states[2].iloc[0]
    assert row["decision_ns"] == row["entry_minute_start_ns"]
    impulse_start = row["impulse_minute_start_ns"]
    assert row["decision_ns"] == impulse_start + 3 * MINUTE_NS
    assert row["state_QUIET_PASSIVE_EXTENSION"]


def test_signal_population_is_nonoverlapping_and_hash_stable() -> None:
    minute = _minute_frame(minutes=150)
    states, _ = build_flow_impact_states(minute)
    left = generate_signal_population(states, project_root=ROOT, graveyard_path=None)
    right = generate_signal_population(states, project_root=ROOT, graveyard_path=None)
    assert set(left) == set(right)
    for candidate_id, signals in left.items():
        assert signal_path_hash(signals) == signal_path_hash(right[candidate_id])
        for previous, current in zip(signals, signals[1:]):
            if previous.session_day == current.session_day:
                assert previous.exit_minute_start_ns <= current.decision_ns
        for signal in signals:
            assert signal.availability_ns <= signal.decision_ns
            assert signal.decision_ns <= signal.entry_minute_start_ns


def _minute_frame(minutes: int = 100) -> pd.DataFrame:
    start = pd.Timestamp("2024-08-05 08:30:00", tz="America/Chicago").tz_convert("UTC").value
    starts = start + np.arange(minutes, dtype=np.int64) * MINUTE_NS
    flow = np.full(minutes, 0.02)
    volume = np.full(minutes, 1000)
    change = np.zeros(minutes)
    open_ = np.full(minutes, 5000.0)
    close = open_.copy()
    impulse = 40
    flow[impulse] = 0.80
    volume[impulse] = 5000
    change[impulse] = 1.0
    close[impulse:] += 1.0
    flow[impulse + 1 : impulse + 3] = 0.05
    close[impulse + 1] = 5001.2
    close[impulse + 2 :] = 5001.5
    return pd.DataFrame(
        {
            "product": "ES",
            "contract": "ESU4",
            "instrument_id": 118,
            "calendar_year": 2024,
            "minute_start_ns": starts,
            "source_close_ns": starts + MINUTE_NS,
            "availability_ns": starts + MINUTE_NS,
            "open": open_,
            "high": np.maximum(open_, close),
            "low": np.minimum(open_, close),
            "close": close,
            "vwap": (open_ + close) / 2.0,
            "trade_count": 100,
            "total_volume": volume,
            "buy_aggressor_volume": 600,
            "sell_aggressor_volume": 400,
            "unknown_side_volume": 0,
            "signed_aggressor_volume": (flow * volume).astype(int),
            "signed_aggressor_fraction": flow,
            "price_change_points": change,
            "path_length_points": np.maximum(np.abs(change), 0.25),
            "signed_path_efficiency": change,
            "transformation_version": "test",
        }
    )
