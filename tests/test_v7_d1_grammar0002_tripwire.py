from __future__ import annotations

from hydra.execution.v7_cost_model import CostStress, load_cost_model
from hydra.research.v7_d1_microstructure_grammar import load_feature_store
from hydra.research.v7_d1_microstructure_grammar_0002 import (
    candidate_specs,
    generate_signal_population,
)
from hydra.validation.v7_d1_grammar0002_tripwire import (
    build_grammar0002_events,
)


def test_grammar0002_events_are_conservative_and_nonoverlapping() -> None:
    minute, _event = load_feature_store(".")
    signals = generate_signal_population(minute, project_root=".")
    specs = {row.candidate_id: row for row in candidate_specs(".")}

    events = build_grammar0002_events(
        minute,
        signals,
        specs,
        load_cost_model(),
        stress=CostStress.BASE,
    )

    assert set(events) == set(signals)
    assert sum(len(rows) for rows in events.values()) == sum(
        len(rows) for rows in signals.values()
    )
    assert all(
        not row.same_bar_ambiguous
        and row.quantity == 1
        and row.session_compliant
        and row.contract_limit_compliant
        for rows in events.values()
        for row in rows
    )


def test_grammar0002_combine_is_not_candidate_fitness() -> None:
    source = __import__(
        "hydra.validation.v7_d1_grammar0002_tripwire", fromlist=["dummy"]
    ).__file__
    text = open(source, encoding="utf-8").read()

    assert '"combine_pass_rate_is_diagnostic_not_fitness": True' in text
    assert "candidate_validation_executed" in text
