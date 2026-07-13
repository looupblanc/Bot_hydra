from __future__ import annotations

import numpy as np
import pandas as pd

from hydra.research.v71_flow_sign_sequence import (
    build_flow_sign_sequence_states,
    candidate_specs,
    generate_signal_population,
    load_flow_sign_sequence_sources,
)


def test_flow_sign_sequence_definitions_are_exact_and_past_only() -> None:
    signs = [1, 1, 1, -1, 1, -1, 1, -1, -1]
    starts = np.arange(len(signs), dtype=np.int64) * 60_000_000_000
    frame = pd.DataFrame(
        {
            "calendar_year": 2024,
            "contract": "ESH4",
            "minute_start_ns": starts,
            "availability_ns": starts + 60_000_000_000,
            "signed_aggressor_volume": signs,
        }
    )
    states, audit = build_flow_sign_sequence_states(frame)
    assert bool(states.loc[3, "state_RUN_TERMINATION_HANDOFF"])
    assert bool(states.loc[4, "state_RUN_RESTART_AFTER_ONE_COUNTER"])
    assert bool(states.loc[8, "state_ALTERNATION_BREAK_TO_PERSISTENCE"])
    assert audit.run_termination_handoff_count >= 1
    assert audit.run_restart_after_one_counter_count >= 1
    assert audit.alternation_break_to_persistence_count >= 1


def test_flow_sign_sequence_is_bounded_deterministic_and_executable() -> None:
    specs = candidate_specs(".")
    assert len(specs) == 6
    assert len({row.specification_hash for row in specs}) == 6
    _, states, audit = load_flow_sign_sequence_sources(".")
    assert audit.nonzero_flow_minute_count > 0
    first = generate_signal_population(states, project_root=".", graveyard_path=None)
    second = generate_signal_population(states, project_root=".", graveyard_path=None)
    assert first == second
    assert sum(len(rows) for rows in first.values()) > 0
    for rows in first.values():
        assert all(row.availability_ns <= row.decision_ns <= row.entry_minute_start_ns for row in rows)
        assert all(row.entry_minute_start_ns < row.exit_minute_start_ns for row in rows)


def test_flow_sign_sequence_does_not_cross_contract_or_session() -> None:
    _, states, _ = load_flow_sign_sequence_sources(".")
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
