from __future__ import annotations

import numpy as np

from hydra.features.canonical_store import CanonicalFeatureKey, CanonicalFeatureStore
from hydra.features.feature_matrix import FeatureMatrix
from hydra.research.turbo_exact_replay import (
    benchmark_exact_replay,
    exact_replay,
    exact_replay_reference,
)
from hydra.strategies.turbo_dsl import ComparisonOperator, StrategyRole, StrategySpec


def _matrix(tmp_path, rows: int = 5_000) -> FeatureMatrix:
    rng = np.random.default_rng(19)
    start = np.datetime64("2024-01-02T14:30", "ns").astype(np.int64)
    decision = start + np.arange(rows, dtype=np.int64) * 60_000_000_000
    day = np.datetime64("2024-01-02", "D").astype(np.int64) + np.arange(rows) // 400
    arrays = {
        "timestamp_ns": decision - 60_000_000_000,
        "decision_ns": decision,
        "availability_ns": decision,
        "segment_code": np.arange(rows) // 400,
        "session_day": day.astype(np.int32),
        "session_code": (np.arange(rows) % 3).astype(np.int16),
        "contract_code": np.zeros(rows, dtype=np.int16),
        "entry_price": np.full(rows, 5_000.0),
        "feature__signal": rng.normal(size=rows),
        "feature__context": rng.normal(size=rows),
        "forward_move__5": rng.normal(0.05, 1.0, size=rows),
    }
    key = CanonicalFeatureKey(
        market="ES", explicit_contract_scope="ESH4", start_inclusive="2024-01-01",
        end_exclusive="2024-10-01", source_data_sha256="a" * 64,
        roll_map_hash="b" * 64, transformation_version="test",
        feature_dag_hash="c" * 64, timeframes=("1m",),
    )
    result = CanonicalFeatureStore(tmp_path).put(key, arrays, provenance={})
    return FeatureMatrix.open(result.path)


def _spec(index: int) -> StrategySpec:
    return StrategySpec(
        candidate_id=f"exact_{index}", lineage_id=f"lineage_{index}",
        family="test", market="ES", timeframe="1m|5m", feature="signal",
        operator=ComparisonOperator.GREATER_EQUAL, threshold=-0.5 + index * 0.02,
        side=1 if index % 2 == 0 else -1, holding_events=5,
        point_value=50.0, round_turn_cost=4.8, role=StrategyRole.COMBINE_PASSER,
        context_feature="context", context_operator=ComparisonOperator.GREATER_THAN,
        context_threshold=0.0, session_code=index % 3,
    )


def test_exact_replay_vector_mask_matches_scalar_oracle(tmp_path):
    matrix = _matrix(tmp_path)
    for index in range(12):
        assert exact_replay(_spec(index), matrix) == exact_replay_reference(_spec(index), matrix)


def test_exact_replay_benchmark_is_identical_and_faster(tmp_path):
    matrix = _matrix(tmp_path, 10_000)
    benchmark = benchmark_exact_replay([_spec(index) for index in range(30)], matrix, repeats=1)
    assert benchmark.outputs_identical
    assert benchmark.speedup >= 3.0
