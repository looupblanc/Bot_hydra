from __future__ import annotations

from collections import Counter

from hydra.research.v71_event_time_grammar import (
    candidate_specs,
    generate_signal_population,
    load_event_time_sources,
)


def test_event_time_grammar_is_bounded() -> None:
    specs = candidate_specs(".")

    assert len(specs) == 128
    assert len({row.candidate_id for row in specs}) == 128
    assert set(Counter(row.family_id for row in specs).values()) == {32}
    assert {row.holding_minutes for row in specs} == {5, 15, 30, 60}


def test_event_time_source_excludes_cross_session_and_invalid_duration() -> None:
    _minute, event, audit = load_event_time_sources(".")

    assert audit.cross_chicago_date_count == 122
    assert audit.nonpositive_duration_count > 0
    assert audit.availability_before_end_count == 0
    assert (event["duration_seconds"] > 0.0).all()
    assert (event["availability_ns"] >= event["end_event_ns"]).all()
    assert len(event) == audit.retained_event_count


def test_event_time_signals_are_available_and_nonoverlapping() -> None:
    minute, event, _audit = load_event_time_sources(".")
    signals = generate_signal_population(minute, event, project_root=".")

    assert len(signals) == 128
    assert sum(len(rows) for rows in signals.values()) > 0
    for rows in signals.values():
        for signal in rows:
            assert signal.availability_ns <= signal.decision_ns
            assert signal.decision_ns <= signal.entry_minute_start_ns
            assert signal.entry_minute_start_ns < signal.exit_minute_start_ns
        for left, right in zip(rows, rows[1:], strict=False):
            assert right.decision_ns >= left.exit_minute_start_ns
