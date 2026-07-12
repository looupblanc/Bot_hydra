from __future__ import annotations

from pathlib import Path

import numpy as np

from hydra.features.feature_matrix import FeatureMatrix
from hydra.research.mechanism_grammar_v6 import (
    fast_screen_mechanism,
    generate_mechanism_population,
    signal_positions,
)


def _matrix(rows: int = 1200) -> FeatureMatrix:
    days = np.repeat(np.arange(19358, 19358 + rows // 4 + 1), 4)[:rows].astype(np.int32)
    segment = np.repeat(np.arange(rows // 100 + 1), 100)[:rows].astype(np.int64)
    base = np.sin(np.arange(rows) / 17.0)
    arrays: dict[str, np.ndarray] = {
        "session_day": days,
        "session_code": (np.arange(rows) % 3).astype(np.int16),
        "segment_code": segment,
        "decision_ns": np.arange(rows, dtype=np.int64) * 60_000_000_000,
        "timestamp_ns": np.arange(rows, dtype=np.int64) * 60_000_000_000,
        "entry_price": np.full(rows, 100.0),
        "bar_high": np.full(rows, 101.0),
        "bar_low": np.full(rows, 99.0),
    }
    features = (
        "past_return_60",
        "ctx_15m_return",
        "ctx_60m_volatility_expansion",
        "extreme_dwell",
        "past_participation",
        "directional_pressure_without_progress",
        "failed_expansion",
        "past_volatility",
        "rv_short_long_ratio",
        "ctx_5m_return",
        "ctx_60m_return",
        "ctx_30m_volatility_expansion",
        "old_region_reentry",
        "shared_loss_risk_state",
    )
    for index, feature in enumerate(features):
        arrays[f"feature__{feature}"] = base + index * 0.01
    for horizon in (5, 15, 30, 60):
        move = np.cos(np.arange(rows) / (11.0 + horizon)) * 0.5
        move[-(horizon + 2) :] = np.nan
        arrays[f"forward_move__{horizon}"] = move
    manifest = {"row_count": rows, "bundle_hash": "synthetic"}
    return FeatureMatrix(Path("/tmp/synthetic-v6"), manifest, arrays)


def test_new_grammar_generates_distinct_multi_condition_graphs() -> None:
    matrices = {market: _matrix() for market in ("ES", "NQ", "RTY", "YM", "GC", "CL")}
    population = generate_mechanism_population(
        matrices, count=240, generation_index=0
    )

    assert len(population) == 240
    assert len({row.structural_fingerprint for row in population}) == 240
    assert all(len(row.conditions) >= 2 for row in population)
    assert len({row.mechanism_kind for row in population}) >= 5
    assert {row.market for row in population} == set(matrices)


def test_signal_positions_do_not_depend_on_future_returns() -> None:
    matrix = _matrix()
    spec = generate_mechanism_population({"ES": matrix}, count=1, generation_index=0)[0]
    before = signal_positions(spec, matrix)
    changed_arrays = dict(matrix.arrays)
    changed_arrays[f"forward_move__{spec.holding_events}"] = np.where(
        np.isfinite(changed_arrays[f"forward_move__{spec.holding_events}"]),
        999999.0,
        np.nan,
    )
    changed = FeatureMatrix(matrix.root, matrix.manifest, changed_arrays)
    after = signal_positions(spec, changed)

    assert np.array_equal(before, after)


def test_fast_screen_is_deterministic_and_reports_costed_economics() -> None:
    matrix = _matrix()
    spec = generate_mechanism_population({"ES": matrix}, count=1, generation_index=1)[0]
    first = fast_screen_mechanism(spec, matrix)
    second = fast_screen_mechanism(spec, matrix)

    assert first == second
    assert first["candidate_id"] == spec.candidate_id
    assert first["event_count"] >= 0
    assert "cost_stress_1_5x_net" in first
    assert len(first["event_signature"]) == 64
