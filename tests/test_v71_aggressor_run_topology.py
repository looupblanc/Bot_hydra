from __future__ import annotations

import pandas as pd

from hydra.research.v71_aggressor_run_topology import (
    build_aggressor_run_topology_states,
    candidate_specs,
    generate_signal_population,
    load_aggressor_run_topology_sources,
)


def test_aggressor_run_topology_motifs_are_exact_and_outcome_free() -> None:
    starts = [1_700_000_040_000_000_000 + i * 60_000_000_000 for i in range(4)]
    feature = pd.DataFrame(
        {
            "calendar_year": [2024] * 4,
            "contract": ["ESU4"] * 4,
            "minute_start_ns": starts,
            "availability_ns": [x + 60_000_000_000 for x in starts],
            "longest_buy_run": [5, 5, 3, 4],
            "longest_sell_run": [2, 2, 6, 4],
            "first_price": [100, 100, 100, 100],
            "last_price": [101, 99, 99, 101],
        }
    )
    minute = feature[["calendar_year", "contract", "minute_start_ns", "availability_ns"]].copy()
    states, audit = build_aggressor_run_topology_states(feature, minute)
    assert bool(states.loc[0, "state_DOMINANT_RUN_WITH_PROGRESS"])
    assert bool(states.loc[1, "state_DOMINANT_RUN_WITHOUT_PROGRESS"])
    assert bool(states.loc[2, "state_DOMINANT_RUN_WITH_PROGRESS"])
    assert not states.loc[3, [column for column in states if column.startswith("state_")]].any()
    assert audit.unique_dominant_run_count == 3
    assert audit.tied_run_count == 1


def test_aggressor_run_topology_is_bounded_deterministic_and_executable() -> None:
    specs = candidate_specs(".")
    assert len(specs) == 4
    assert len({row.specification_hash for row in specs}) == 4
    _, states, audit = load_aggressor_run_topology_sources(".")
    assert audit.minute_count == 17_200
    first = generate_signal_population(states, project_root=".", graveyard_path=None)
    second = generate_signal_population(states, project_root=".", graveyard_path=None)
    assert first == second
    assert sum(len(rows) for rows in first.values()) > 0
    for rows in first.values():
        assert all(
            row.availability_ns <= row.decision_ns <= row.entry_minute_start_ns
            for row in rows
        )
        assert all(row.entry_minute_start_ns < row.exit_minute_start_ns for row in rows)


def test_aggressor_run_topology_does_not_cross_contract_or_session() -> None:
    _, states, _ = load_aggressor_run_topology_sources(".")
    signals = generate_signal_population(states, project_root=".", graveyard_path=None)
    indexed = states.set_index("minute_start_ns", drop=False)
    for rows in signals.values():
        for signal in rows:
            entry = indexed.loc[signal.entry_minute_start_ns]
            exit_ = indexed.loc[signal.exit_minute_start_ns]
            assert str(entry["contract"]) == signal.contract
            assert str(exit_["contract"]) == signal.contract
            assert str(entry["session_day"]) == signal.session_day
            assert str(exit_["session_day"]) == signal.session_day
