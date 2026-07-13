from __future__ import annotations

from hydra.research.v71_trade_size_composition import (
    candidate_specs,
    generate_signal_population,
    load_trade_size_composition_sources,
)


def test_trade_size_composition_is_bounded_past_only_and_deterministic() -> None:
    specs = candidate_specs(".")
    assert len(specs) == 6
    assert len({row.specification_hash for row in specs}) == 6
    _, states, audit = load_trade_size_composition_sources(".")
    assert audit.same_contract_prior_session_baseline_count > 0
    assert audit.session_count > 0
    baseline_columns = [
        "prior_baseline_average_trade_size",
        "prior_baseline_trade_count",
        "prior_baseline_absolute_signed_fraction",
    ]
    signaled = states[
        states[
            [
                "state_LARGE_CLIP_FLOW_ONSET",
                "state_LARGE_CLIP_ABSORPTION",
                "state_SMALL_CLIP_PARTICIPATION_BURST",
            ]
        ].any(axis=1)
    ]
    assert signaled[baseline_columns].notna().all(axis=None)
    first = generate_signal_population(
        states, project_root=".", graveyard_path=None
    )
    second = generate_signal_population(
        states, project_root=".", graveyard_path=None
    )
    assert first == second
    assert sum(len(rows) for rows in first.values()) > 0
    for rows in first.values():
        assert all(
            row.availability_ns <= row.decision_ns <= row.entry_minute_start_ns
            for row in rows
        )
        assert all(
            row.entry_minute_start_ns < row.exit_minute_start_ns
            for row in rows
        )


def test_trade_size_composition_does_not_cross_contract_or_session() -> None:
    _, states, _ = load_trade_size_composition_sources(".")
    signals = generate_signal_population(
        states, project_root=".", graveyard_path=None
    )
    indexed = states.set_index("minute_start_ns", drop=False)
    for rows in signals.values():
        for signal in rows:
            entry = indexed.loc[signal.entry_minute_start_ns]
            exit_ = indexed.loc[signal.exit_minute_start_ns]
            assert str(entry["contract"]) == signal.contract
            assert str(exit_["contract"]) == signal.contract
            assert str(entry["session_day"]) == signal.session_day
            assert str(exit_["session_day"]) == signal.session_day
