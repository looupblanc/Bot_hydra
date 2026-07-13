from __future__ import annotations

import numpy as np
import pandas as pd

from hydra.research.v72_executed_price_occupancy import (
    build_executed_price_occupancy_states,
    candidate_specs,
    generate_signal_population,
    load_executed_price_occupancy_sources,
)


def _synthetic_sources() -> tuple[pd.DataFrame, pd.DataFrame]:
    count = 150
    starts = np.arange(count, dtype=np.int64) * 60_000_000_000
    feature = pd.DataFrame(
        {
            "calendar_year": [2024] * count,
            "session_date": ["2024-08-01"] * count,
            "contract": ["ESU4"] * count,
            "minute_start_ns": starts,
            "availability_ns": starts + 60_000_000_000,
            "occupancy_entropy": [0.5] * count,
            "mode_volume_share": [0.5] * count,
            "top_two_volume_share": [0.7] * count,
            "second_to_first_mode_ratio": [0.5] * count,
            "revisit_ratio": [0.4] * count,
            "signed_flow_fraction": [0.1] * count,
            "mode_signed_flow_fraction": [0.1] * count,
            "mode_tick": [400] * count,
            "second_mode_tick": [402.0] * count,
            "last_tick": [401] * count,
            "last_minus_mode_ticks": [1] * count,
            "maximum_excursion_from_mode_ticks": [2] * count,
            "maximum_excursion_direction": [1] * count,
            "mode_migration_ticks": [0.0] * count,
        }
    )
    feature.loc[70, [
        "occupancy_entropy", "mode_volume_share", "last_tick",
        "last_minus_mode_ticks", "maximum_excursion_from_mode_ticks",
    ]] = [0.1, 0.9, 405, 5, 5]
    feature.loc[72, [
        "occupancy_entropy", "mode_volume_share", "last_tick",
        "last_minus_mode_ticks", "maximum_excursion_from_mode_ticks",
        "maximum_excursion_direction",
    ]] = [0.1, 0.9, 400, 0, 10, 1]
    feature.loc[74, [
        "revisit_ratio", "signed_flow_fraction", "last_tick",
        "last_minus_mode_ticks",
    ]] = [0.9, 0.9, 404, 4]
    feature.loc[76, [
        "revisit_ratio", "signed_flow_fraction", "last_tick",
        "last_minus_mode_ticks",
    ]] = [0.9, 0.9, 398, -2]
    feature.loc[78, [
        "mode_migration_ticks", "signed_flow_fraction", "mode_volume_share",
    ]] = [5.0, 0.9, 0.7]
    feature.loc[80, [
        "top_two_volume_share", "second_to_first_mode_ratio", "mode_tick",
        "second_mode_tick", "last_tick", "last_minus_mode_ticks",
    ]] = [0.95, 0.9, 400, 408.0, 409, 9]
    minute = feature[
        ["calendar_year", "contract", "minute_start_ns", "availability_ns"]
    ].copy()
    return feature, minute


def test_occupancy_motifs_use_only_past_completed_minutes() -> None:
    feature, minute = _synthetic_sources()
    states, _ = build_executed_price_occupancy_states(feature, minute)
    state = states[60]
    assert bool(state.loc[70, "state_CONCENTRATED_MODE_ESCAPE"])
    assert bool(state.loc[72, "state_CONCENTRATED_MODE_RECAPTURE"])
    assert bool(state.loc[74, "state_REVISIT_PRESSURE_BREAK"])
    assert bool(state.loc[76, "state_REVISIT_PRESSURE_FAILURE"])
    assert bool(state.loc[78, "state_MODE_MIGRATION_PERSISTENCE"])
    assert bool(state.loc[80, "state_BIMODAL_AUCTION_RESOLUTION"])
    state_columns = [column for column in state if column.startswith("state_")]
    assert state.loc[:59, state_columns].sum().sum() == 0


def test_occupancy_population_is_frozen_deterministic_and_executable() -> None:
    specs = candidate_specs(".")
    assert len(specs) == 24
    assert len({row.specification_hash for row in specs}) == 24
    _, states, audit = load_executed_price_occupancy_sources(".")
    assert audit["minute_count"] == 17_200
    first = generate_signal_population(states, project_root=".", graveyard_path=None)
    second = generate_signal_population(states, project_root=".", graveyard_path=None)
    assert first == second
    assert sum(len(rows) for rows in first.values()) > 0
    for rows in first.values():
        assert all(
            row.availability_ns <= row.decision_ns <= row.entry_minute_start_ns
            for row in rows
        )
        assert all(
            row.entry_minute_start_ns < row.exit_minute_start_ns for row in rows
        )


def test_occupancy_signals_do_not_cross_contract_or_session() -> None:
    _, states, _ = load_executed_price_occupancy_sources(".")
    signals = generate_signal_population(states, project_root=".", graveyard_path=None)
    for spec in candidate_specs("."):
        state = states[spec.history_window].set_index("minute_start_ns", drop=False)
        for signal in signals[spec.candidate_id]:
            entry = state.loc[signal.entry_minute_start_ns]
            exit_ = state.loc[signal.exit_minute_start_ns]
            assert str(entry["contract"]) == signal.contract
            assert str(exit_["contract"]) == signal.contract
            assert str(entry["session_day"]) == signal.session_day
            assert str(exit_["session_day"]) == signal.session_day
