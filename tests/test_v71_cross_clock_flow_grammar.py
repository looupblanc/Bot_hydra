from __future__ import annotations

from hydra.research.v71_cross_clock_flow_grammar import (
    candidate_specs,
    generate_signal_population,
    load_cross_clock_sources,
)


def test_cross_clock_grammar_is_bounded_and_deterministic() -> None:
    specs = candidate_specs(".")
    assert len(specs) == 12
    assert len({row.specification_hash for row in specs}) == 12
    minute, pairs, audit = load_cross_clock_sources(".")
    first = generate_signal_population(minute, pairs, project_root=".", graveyard_path=None)
    second = generate_signal_population(minute, pairs, project_root=".", graveyard_path=None)
    assert first == second
    assert audit.aligned_availability_minute_count > 1000
    for rows in first.values():
        assert all(row.availability_ns <= row.decision_ns <= row.entry_minute_start_ns for row in rows)
        assert all(row.entry_minute_start_ns < row.exit_minute_start_ns for row in rows)
