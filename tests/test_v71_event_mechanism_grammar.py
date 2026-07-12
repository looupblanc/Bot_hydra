from __future__ import annotations

from collections import Counter

from hydra.research.v71_event_mechanism_grammar import (
    V71Signal,
    candidate_specs,
    generate_signal_population,
    load_v71_minute_features,
    signal_path_hash,
)


def test_v71_grammar_has_256_structures_across_eight_families() -> None:
    specs = candidate_specs(".")
    counts = Counter(row.family_id for row in specs)

    assert len(specs) == 256
    assert len(counts) == 8
    assert set(counts.values()) == {32}
    assert len({row.specification_hash for row in specs}) == 256
    assert all(row.mechanism_class != "ARB_INTRA_PRODUIT" for row in specs)


def test_v71_signal_schema_has_no_outcome_field() -> None:
    fields = set(V71Signal.__dataclass_fields__)
    assert not fields & {
        "pnl",
        "net_pnl",
        "gross_pnl",
        "future_return",
        "target",
        "label",
        "mfe",
        "mae",
    }


def test_v71_generation_is_deterministic_past_only_and_nonoverlapping() -> None:
    minute = load_v71_minute_features(".")
    first = generate_signal_population(minute, project_root=".")
    second = generate_signal_population(minute, project_root=".")

    assert first == second
    assert len(first) == 256
    assert sum(len(rows) for rows in first.values()) > 0
    for rows in first.values():
        assert all(row.availability_ns <= row.decision_ns for row in rows)
        assert all(row.decision_ns <= row.entry_minute_start_ns < row.exit_minute_start_ns for row in rows)
        assert all(left.exit_minute_start_ns <= right.decision_ns for left, right in zip(rows, rows[1:]))
        assert len(signal_path_hash(rows)) == 64
