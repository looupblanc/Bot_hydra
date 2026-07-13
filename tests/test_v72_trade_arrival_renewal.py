from __future__ import annotations

import numpy as np
import pandas as pd

from hydra.research.v72_trade_arrival_renewal import (
    build_trade_arrival_renewal_states,
    candidate_specs,
    generate_signal_population,
    load_trade_arrival_renewal_sources,
)


def _synthetic_sources() -> tuple[pd.DataFrame, pd.DataFrame]:
    count = 100
    starts = np.arange(count, dtype=np.int64) * 60_000_000_000
    feature = pd.DataFrame(
        {
            "calendar_year": [2024] * count,
            "contract": ["ESU4"] * count,
            "minute_start_ns": starts,
            "availability_ns": starts + 60_000_000_000,
            "trade_count": [100] * count,
            "positive_gap_median_ns": [10.0] * count,
            "arrival_entropy": [0.5] * count,
            "maximum_five_second_share": [0.2] * count,
            "signed_flow_fraction": [0.1] * count,
            "price_progress_points": [0.25] * count,
        }
    )
    # Six structurally distinct states after enough past-only history.
    feature.loc[61, ["positive_gap_median_ns", "arrival_entropy", "maximum_five_second_share", "trade_count", "signed_flow_fraction", "price_progress_points"]] = [1.0, 0.1, 0.8, 200, 0.9, 1.0]
    feature.loc[63, ["positive_gap_median_ns", "arrival_entropy", "maximum_five_second_share", "trade_count", "signed_flow_fraction", "price_progress_points"]] = [0.8, 0.08, 0.85, 210, 0.8, -0.25]
    feature.loc[65, ["positive_gap_median_ns", "arrival_entropy", "maximum_five_second_share", "trade_count", "signed_flow_fraction", "price_progress_points"]] = [12.0, 0.9, 0.05, 220, -0.9, -0.5]
    feature.loc[69, ["positive_gap_median_ns", "trade_count"]] = [100.0, 5]
    feature.loc[70, ["positive_gap_median_ns", "arrival_entropy", "maximum_five_second_share", "trade_count", "signed_flow_fraction", "price_progress_points"]] = [0.5, 0.05, 0.9, 230, 0.95, 0.5]
    feature.loc[72, ["positive_gap_median_ns", "arrival_entropy", "maximum_five_second_share", "trade_count", "signed_flow_fraction", "price_progress_points"]] = [0.4, 0.04, 0.9, 230, -0.95, -0.5]
    feature.loc[73, ["positive_gap_median_ns", "trade_count", "signed_flow_fraction", "price_progress_points"]] = [120.0, 4, 0.01, 0.0]
    feature.loc[75, ["positive_gap_median_ns", "arrival_entropy", "maximum_five_second_share", "trade_count", "signed_flow_fraction", "price_progress_points"]] = [0.3, 0.03, 0.95, 240, 0.001, 0.5]
    minute = feature[["calendar_year", "contract", "minute_start_ns", "availability_ns"]].copy()
    return feature, minute


def test_trade_arrival_motifs_use_only_past_completed_minutes() -> None:
    feature, minute = _synthetic_sources()
    states, _ = build_trade_arrival_renewal_states(feature, minute)
    state = states[60]
    assert bool(state.loc[61, "state_CLUSTERED_DIRECTIONAL_ARRIVAL"])
    assert bool(state.loc[63, "state_CLUSTERED_ABSORBED_ARRIVAL"])
    assert bool(state.loc[65, "state_DISTRIBUTED_DIRECTIONAL_SLICING"])
    assert bool(state.loc[70, "state_SILENCE_TO_BURST_RELEASE"])
    assert bool(state.loc[73, "state_BURST_TO_SILENCE_EXHAUSTION"])
    assert bool(state.loc[75, "state_TWO_SIDED_CLUSTERED_INVENTORY"])
    assert state.loc[:59, [column for column in state if column.startswith("state_")]].sum().sum() == 0


def test_trade_arrival_population_is_frozen_deterministic_and_executable() -> None:
    specs = candidate_specs(".")
    assert len(specs) == 24
    assert len({row.specification_hash for row in specs}) == 24
    _, states, audit = load_trade_arrival_renewal_sources(".")
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


def test_trade_arrival_signals_do_not_cross_contract_or_session() -> None:
    _, states, _ = load_trade_arrival_renewal_sources(".")
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
