from __future__ import annotations

from collections import Counter

from hydra.research.v71_opportunity_density_grammar import (
    candidate_specs,
    generate_signal_population,
    load_v71_minute_features,
    signal_path_hash,
)


def test_opportunity_density_grammar_is_bounded_and_structural() -> None:
    specs = candidate_specs(".")

    assert len(specs) == 128
    assert len({row.candidate_id for row in specs}) == 128
    assert set(Counter(row.family_id for row in specs).values()) == {32}
    assert {row.holding_minutes for row in specs} == {5, 15, 30, 60}
    assert {row.response_policy for row in specs} == {
        "CONTINUATION",
        "REVERSAL",
    }


def test_opportunity_density_signals_are_deterministic_and_past_only() -> None:
    minute = load_v71_minute_features(".")
    first = generate_signal_population(minute, project_root=".")
    second = generate_signal_population(minute, project_root=".")

    assert set(first) == set(second)
    assert sum(len(rows) for rows in first.values()) > 0
    for candidate_id, rows in first.items():
        assert signal_path_hash(rows) == signal_path_hash(second[candidate_id])
        for signal in rows:
            assert signal.availability_ns <= signal.decision_ns
            assert signal.decision_ns <= signal.entry_minute_start_ns
            assert signal.entry_minute_start_ns < signal.exit_minute_start_ns
            assert signal.contract
