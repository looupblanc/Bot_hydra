from __future__ import annotations

import numpy as np
import pandas as pd

from hydra.execution.v7_cost_model import CostStress, load_cost_model
from hydra.research.v7_d1_microstructure_grammar import (
    candidate_specs,
    generate_signal_population,
    load_feature_store,
)
from hydra.validation.v7_d1_new_dataset_tripwire import (
    D1NullControl,
    build_candidate_events,
    build_null_feature_world,
)


def test_real_event_replay_uses_conservative_next_bar_fills() -> None:
    minute, event = load_feature_store(".")
    signals = generate_signal_population(minute, event)
    specs = {row.candidate_id: row for row in candidate_specs()}

    events = build_candidate_events(
        minute,
        event,
        signals,
        specs,
        load_cost_model(),
        stress=CostStress.BASE,
    )

    assert sum(len(rows) for rows in events.values()) > 0
    assert all(row.exit_ns > row.decision_ns for rows in events.values() for row in rows)
    assert all(not row.same_bar_ambiguous for rows in events.values() for row in rows)


def test_null_worlds_are_deterministic_and_preserve_flow() -> None:
    minute, event = load_feature_store(".")

    first_minute, first_event = build_null_feature_world(
        minute, event, control=D1NullControl.DAILY_BLOCK_SHUFFLE
    )
    second_minute, second_event = build_null_feature_world(
        minute, event, control=D1NullControl.DAILY_BLOCK_SHUFFLE
    )

    pd.testing.assert_frame_equal(first_minute, second_minute)
    pd.testing.assert_frame_equal(first_event, second_event)
    for column in (
        "total_volume",
        "buy_aggressor_volume",
        "sell_aggressor_volume",
        "signed_aggressor_volume",
    ):
        assert np.array_equal(first_minute[column], minute[column])
        assert np.array_equal(first_event[column], event[column])
    assert not np.array_equal(first_minute["close"], minute["close"])


def test_year_permutation_keeps_timestamps_and_changes_prices() -> None:
    minute, event = load_feature_store(".")

    null_minute, null_event = build_null_feature_world(
        minute, event, control=D1NullControl.YEAR_BLOCK_PERMUTATION
    )

    assert np.array_equal(null_minute["minute_start_ns"], minute["minute_start_ns"])
    assert np.array_equal(null_event["start_event_ns"], event["start_event_ns"])
    assert not np.array_equal(null_event["close"], event["close"])


def test_random_walk_reconstructs_valid_ohlc() -> None:
    minute, event = load_feature_store(".")

    null_minute, null_event = build_null_feature_world(
        minute, event, control=D1NullControl.VOLATILITY_MATCHED_RANDOM_WALK
    )

    for frame in (null_minute, null_event):
        assert np.all(frame["high"] >= frame[["open", "close"]].max(axis=1))
        assert np.all(frame["low"] <= frame[["open", "close"]].min(axis=1))
        assert np.all(frame["low"] > 0.0)
