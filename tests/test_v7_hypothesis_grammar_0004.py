from __future__ import annotations

import json
from dataclasses import replace

import numpy as np
import pytest

from hydra.research.v7_hypothesis_grammar import V7GrammarError
from hydra.research.v7_hypothesis_grammar_0004 import (
    candidate_specs,
    generate_signal_population,
    load_v7_market_bars,
)


def test_grammar_0004_has_fixed_distinct_structures() -> None:
    specs = candidate_specs()

    assert len(specs) == 11
    assert len({row.candidate_id for row in specs}) == 11
    assert len({row.specification_hash for row in specs}) == 11
    assert len({row.mechanism_class for row in specs}) == 5


def test_grammar_0004_is_deterministic_past_only_and_outcome_free() -> None:
    bars = load_v7_market_bars(".")

    first = generate_signal_population(bars, graveyard_path=None)
    second = generate_signal_population(bars, graveyard_path=None)

    assert first == second
    assert sum(len(rows) for rows in first.values()) > 0
    for candidate_id, rows in first.items():
        assert candidate_id
        for row in rows:
            assert row.availability_ns <= row.decision_ns <= row.entry_ns
            assert row.entry_ns < row.exit_ns
            assert row.contract_code == bars[row.market].contract_code[row.entry_index]
            assert "pnl" not in json.dumps(row.to_dict()).lower()


def test_grammar_0004_rejects_cross_market_future_availability() -> None:
    bars = load_v7_market_bars(".")
    copied = dict(bars)
    es = copied["ES"]
    availability = np.array(es.availability_ns, copy=True)
    mask = es.local_minute == 9 * 60 + 30
    availability[mask] += 2 * 60_000_000_000
    copied["ES"] = replace(es, availability_ns=availability)

    with pytest.raises(V7GrammarError, match="unavailable"):
        generate_signal_population(copied, graveyard_path=None)


def test_grammar_0004_rejects_target_roll_crossing() -> None:
    bars = load_v7_market_bars(".")
    baseline = generate_signal_population(bars, graveyard_path=None)
    first = baseline["v7g4_friday_gamma_ES"][0]
    es = bars["ES"]
    segment_code = np.array(es.segment_code, copy=True)
    segment_code[first.exit_index] += 1
    bars["ES"] = replace(es, segment_code=segment_code)

    changed = generate_signal_population(bars, graveyard_path=None)

    assert len(changed["v7g4_friday_gamma_ES"]) == len(
        baseline["v7g4_friday_gamma_ES"]
    ) - 1
