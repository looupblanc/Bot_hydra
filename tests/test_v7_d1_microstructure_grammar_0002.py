from __future__ import annotations

import pandas as pd

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


def test_zero_volume_null_session_is_skipped_without_nan_signal() -> None:
    minute, _event = load_feature_store(".")
    null_minute = minute.copy()
    timestamps = pd.to_datetime(
        null_minute["minute_start_ns"], unit="ns", utc=True
    ).dt.tz_convert("America/Chicago")
    local_dates = timestamps.dt.date
    for (_product, _year), positions in null_minute.groupby(
        ["product", "calendar_year"], sort=True
    ).groups.items():
        group_dates = local_dates.loc[list(positions)]
        first_date = min(group_dates)
        selected = group_dates.index[group_dates == first_date]
        null_minute.loc[
            selected,
            [
                "total_volume",
                "signed_aggressor_volume",
                "signed_aggressor_fraction",
            ],
        ] = 0.0

    signals = generate_signal_population(null_minute, project_root=".")

    assert set(signals) == {row.candidate_id for row in candidate_specs(".")}
