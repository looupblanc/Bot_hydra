from __future__ import annotations

from hydra.execution.v7_cost_model import load_cost_model
from hydra.research.v71_event_mechanism_grammar import (
    candidate_specs,
    generate_signal_population,
    load_v71_minute_features,
)
from hydra.validation.v71_event_funnel import (
    _minute_replay_cache,
    _replay_candidate,
)


def test_v71_replay_uses_fixed_signals_exact_costs_and_explicit_contracts() -> None:
    minute = load_v71_minute_features(".")
    specs = {row.candidate_id: row for row in candidate_specs(".")}
    signals = generate_signal_population(minute, project_root=".")
    candidate_id = next(key for key, rows in signals.items() if rows)

    first = _replay_candidate(
        specs[candidate_id],
        signals[candidate_id],
        _minute_replay_cache(minute),
        cost_model=load_cost_model(),
    )
    second = _replay_candidate(
        specs[candidate_id],
        signals[candidate_id],
        _minute_replay_cache(minute),
        cost_model=load_cost_model(),
    )

    assert first == second
    assert len(first) == len(signals[candidate_id])
    assert all(row["cost_usd"] > 0.0 for row in first)
    assert all(row["decision_ns"] < row["exit_ns"] for row in first)
