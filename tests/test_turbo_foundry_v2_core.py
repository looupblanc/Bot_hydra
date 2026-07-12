from __future__ import annotations

from pathlib import Path

import numpy as np

from hydra.features.feature_matrix import FeatureMatrix
from hydra.research.qd_economic_tournament import FEATURES, MARKET_PAIRS
from hydra.research.turbo_feature_builder import CONTEXT_MINUTES, HORIZONS
from hydra.research.turbo_foundry_v2 import (
    _population_coverage,
    _stage1_event_matrix,
    generate_turbo_population,
)


def _matrix(market: str, rows: int = 2_000) -> FeatureMatrix:
    rng = np.random.default_rng(sum(map(ord, market)))
    days = np.datetime64("2023-01-02", "D").astype(np.int64) + np.arange(rows) // 20
    decisions = (
        np.datetime64("2023-01-02T14:30", "ns").astype(np.int64)
        + np.arange(rows, dtype=np.int64) * 60_000_000_000
    )
    arrays: dict[str, np.ndarray] = {
        "timestamp_ns": decisions - 60_000_000_000,
        "decision_ns": decisions,
        "availability_ns": decisions,
        "segment_code": np.arange(rows) // 20,
        "session_day": days.astype(np.int32),
        "session_code": (np.arange(rows) % 3).astype(np.int16),
        "contract_code": np.zeros(rows, dtype=np.int16),
        "entry_price": np.full(rows, 100.0),
    }
    for feature in FEATURES:
        arrays[f"feature__{feature}"] = rng.normal(size=rows)
    for minutes in CONTEXT_MINUTES:
        arrays[f"feature__ctx_{minutes}m_return"] = rng.normal(size=rows)
        arrays[f"feature__ctx_{minutes}m_volatility_expansion"] = rng.integers(
            0, 2, size=rows
        ).astype(float)
    for horizon in HORIZONS:
        arrays[f"forward_move__{horizon}"] = rng.normal(size=rows)
    return FeatureMatrix(
        root=Path("."),
        manifest={"row_count": rows, "bundle_hash": market},
        arrays=arrays,
    )


def test_turbo_population_is_deterministic_unique_and_respects_caps():
    matrices = {market: _matrix(market) for market in MARKET_PAIRS}
    left = generate_turbo_population(
        matrices, count=500, batch_index=0, random_seed=77
    )
    right = generate_turbo_population(
        matrices, count=500, batch_index=0, random_seed=77
    )
    assert [row.candidate_id for row in left] == [row.candidate_id for row in right]
    assert len({row.candidate_id for row in left}) == 500
    coverage = _population_coverage(left)
    assert coverage["maximum_family_share"] <= 0.25
    assert coverage["maximum_ecology_share"] <= 0.40
    assert coverage["maximum_lineage_share"] <= 0.02
    assert set(coverage["market_ecologies"]) == {"equity_indices", "metals", "energy"}


def test_stage1_matrix_contains_only_closed_development_rows():
    matrix = _matrix("ES", rows=8_000)
    stage1 = _stage1_event_matrix(matrix)
    assert stage1.event_count > 0
    assert (stage1.availability_ns <= stage1.decision_ns).all()
    assert set(np.unique(stage1.session_codes)) <= {0, 1, 2}


def test_missing_metal_ecology_redistributes_quota_before_stage1():
    matrices = {
        market: _matrix(market)
        for market in MARKET_PAIRS
        if market != "GC"
    }
    population = generate_turbo_population(
        matrices, count=400, batch_index=0, random_seed=78
    )
    coverage = _population_coverage(population)
    assert len(population) == 400
    assert coverage["ecology_cap_relaxed_for_feasibility"]
    assert coverage["missing_ecologies"] == ["metals"]
    assert coverage["market_ecologies"] == {
        "energy": 200,
        "equity_indices": 200,
    }
