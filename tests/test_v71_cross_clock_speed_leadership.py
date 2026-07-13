from __future__ import annotations

from hydra.research.v71_cross_clock_speed_leadership import (
    candidate_specs,
    generate_signal_population,
    load_speed_leadership_sources,
)


def test_speed_leadership_grammar_is_bounded_past_only_and_deterministic() -> None:
    specs = candidate_specs(".")
    assert len(specs) == 12
    assert len({row.specification_hash for row in specs}) == 12
    minute, transitions, audit = load_speed_leadership_sources(".")
    assert audit.speed_leadership_transition_count == len(transitions)
    assert audit.speed_leadership_transition_count > 1000
    assert (
        transitions[["volume_availability_ns", "dollar_availability_ns"]]
        .max(axis=1)
        .le(transitions["decision_ns"])
        .all()
    )
    first = generate_signal_population(
        minute, transitions, project_root=".", graveyard_path=None
    )
    second = generate_signal_population(
        minute, transitions, project_root=".", graveyard_path=None
    )
    assert first == second
    assert sum(len(rows) for rows in first.values()) > 1000
    for rows in first.values():
        assert all(
            row.availability_ns <= row.decision_ns <= row.entry_minute_start_ns
            for row in rows
        )
        assert all(
            row.entry_minute_start_ns < row.exit_minute_start_ns
            for row in rows
        )
