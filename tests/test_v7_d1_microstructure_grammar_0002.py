from __future__ import annotations

from hydra.research.v7_d1_microstructure_grammar import load_feature_store
from hydra.research.v7_d1_microstructure_grammar_0002 import (
    build_five_minute_features,
    candidate_specs,
    generate_signal_population,
)


def test_grammar0002_has_eight_preregistered_structures() -> None:
    specs = candidate_specs(".")

    assert len(specs) == 8
    assert len({row.candidate_id for row in specs}) == 8
    assert all(row.product in {"ES", "MES"} for row in specs)


def test_five_minute_features_are_closed_and_explicit_contract() -> None:
    minute, _event = load_feature_store(".")
    blocks = build_five_minute_features(minute)

    assert not blocks.empty
    assert (blocks["availability_ns"] >= blocks["source_close_ns"]).all()
    assert blocks["contract"].str.startswith(("ES", "MES")).all()


def test_signal_population_is_deterministic_past_only_and_nonoverlapping() -> None:
    minute, _event = load_feature_store(".")

    first = generate_signal_population(minute, project_root=".")
    second = generate_signal_population(minute, project_root=".")

    assert first == second
    assert set(first) == {row.candidate_id for row in candidate_specs(".")}
    assert sum(len(rows) for rows in first.values()) > 0
    for rows in first.values():
        assert all(row.availability_ns <= row.entry_minute_start_ns for row in rows)
        assert all(row.entry_minute_start_ns < row.exit_minute_start_ns for row in rows)
        assert all(left.exit_minute_start_ns <= right.entry_minute_start_ns for left, right in zip(rows, rows[1:]))
        assert all(not hasattr(row, "pnl") for row in rows)
