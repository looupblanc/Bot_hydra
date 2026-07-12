from __future__ import annotations

import json

import numpy as np

from hydra.research.v7_d1_microstructure_grammar import (
    _past_rolling_quantile,
    candidate_specs,
    generate_signal_population,
    load_feature_store,
)


def test_d1_grammar_has_eight_fixed_distinct_structures() -> None:
    specs = candidate_specs()

    assert len(specs) == 8
    assert len({row.candidate_id for row in specs}) == 8
    assert len({row.specification_hash for row in specs}) == 8
    assert len({row.mechanism_class for row in specs}) == 4


def test_past_rolling_quantile_excludes_current_value() -> None:
    values = np.asarray([1.0, 2.0, 3.0, 1000.0, 5.0])

    result = _past_rolling_quantile(values, 3, 0.5)

    assert np.isnan(result[:3]).all()
    assert result[3] == 2.0
    assert result[4] == 3.0


def test_d1_signals_are_deterministic_available_and_outcome_free() -> None:
    minute, event = load_feature_store(".")

    first = generate_signal_population(minute, event)
    second = generate_signal_population(minute, event)

    assert first == second
    assert sum(len(rows) for rows in first.values()) > 0
    for rows in first.values():
        for row in rows:
            assert row.availability_ns <= row.decision_ns
            payload = json.dumps(row.to_dict()).lower()
            assert "pnl" not in payload
            assert "exit_price" not in payload


def test_signal_years_and_contracts_remain_explicit() -> None:
    minute, event = load_feature_store(".")
    signals = generate_signal_population(minute, event)

    years = {row.calendar_year for rows in signals.values() for row in rows}
    contracts = {row.contract for rows in signals.values() for row in rows}

    assert years == {2023, 2024}
    assert contracts <= {"ESU3", "MESU3", "ESU4", "MESU4"}
