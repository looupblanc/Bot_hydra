from __future__ import annotations

from dataclasses import FrozenInstanceError, replace

import numpy as np
import pytest

from hydra.strategies.turbo_batch_fingerprint import (
    batch_fingerprints,
    deduplicate_specs,
    structural_fingerprint,
)
from hydra.strategies.turbo_compiler import compile_strategy_batch
from hydra.strategies.turbo_dsl import ComparisonOperator, StrategyRole, StrategySpec
from hydra.strategies.turbo_vectorized_executor import (
    EventMatrix,
    benchmark_stage1,
    execute_stage1_reference,
    execute_stage1_vectorized,
)


def _spec(index: int, **updates: object) -> StrategySpec:
    values: dict[str, object] = {
        "candidate_id": f"turbo_{index}",
        "lineage_id": f"lineage_{index}",
        "family": "state_transition",
        "market": "ES",
        "timeframe": "5m",
        "feature": "path_efficiency",
        "operator": ComparisonOperator.GREATER_THAN,
        "threshold": -0.2 + (index % 40) * 0.01,
        "side": 1 if index % 2 == 0 else -1,
        "holding_events": (1, 3, 6)[index % 3],
        "point_value": 50.0,
        "round_turn_cost": 4.8,
        "role": StrategyRole.ALPHA,
        "context_feature": "volatility_state" if index % 3 == 0 else None,
        "context_operator": (
            ComparisonOperator.LESS_EQUAL if index % 3 == 0 else None
        ),
        "context_threshold": 1.1 if index % 3 == 0 else None,
        "session_code": index % 2,
        "quantity": 1,
    }
    values.update(updates)
    return StrategySpec(**values)  # type: ignore[arg-type]


def _matrix(events: int = 401) -> EventMatrix:
    rng = np.random.default_rng(20260712)
    decisions = np.arange(events, dtype=np.int64) * 60_000_000_000
    features = rng.normal(size=(events, 3))
    forward = rng.normal(0.015, 0.8, size=(3, events))
    forward[:, -6:] = np.nan
    return EventMatrix.from_arrays(
        feature_names=("path_efficiency", "volatility_state", "participation"),
        holding_horizons=(1, 3, 6),
        features=features,
        forward_moves=forward,
        decision_ns=decisions,
        availability_ns=decisions - 1,
        session_codes=np.arange(events, dtype=np.int16) % 2,
    )


def test_spec_is_immutable_and_rejects_partial_context() -> None:
    spec = _spec(0)
    with pytest.raises(FrozenInstanceError):
        spec.threshold = 2.0  # type: ignore[misc]
    with pytest.raises(ValueError, match="supplied together"):
        _spec(1, context_feature="participation", context_operator=None, context_threshold=None)


def test_fingerprints_are_deterministic_and_names_cannot_evade_deduplication() -> None:
    original = _spec(1)
    renamed = replace(
        original,
        candidate_id="renamed",
        lineage_id="new_lineage",
        family="cosmetic_new_family",
        role=StrategyRole.XFA_PAYOUT,
        point_value=5.0,
        round_turn_cost=1.2,
        quantity=3,
        version=2,
    )
    distinct = replace(original, candidate_id="distinct", threshold=0.777)

    assert structural_fingerprint(original) == structural_fingerprint(renamed)
    assert structural_fingerprint(original) != structural_fingerprint(distinct)
    assert np.array_equal(batch_fingerprints([original]), batch_fingerprints([original]))
    result = deduplicate_specs([original, renamed, distinct])
    assert [item.candidate_id for item in result.specs] == ["turbo_1", "distinct"]
    assert result.duplicate_indices == (1,)


def test_event_matrix_fails_closed_on_future_feature_availability() -> None:
    matrix = _matrix(20)
    with pytest.raises(ValueError, match="availability"):
        EventMatrix.from_arrays(
            feature_names=matrix.feature_names,
            holding_horizons=matrix.holding_horizons,
            features=matrix.features,
            forward_moves=matrix.forward_moves,
            decision_ns=matrix.decision_ns,
            availability_ns=matrix.decision_ns + 1,
            session_codes=matrix.session_codes,
        )


def test_compiled_arrays_are_compact_readonly_and_validate_feature_names() -> None:
    specs = [_spec(index) for index in range(12)]
    matrix = _matrix()
    compiled = compile_strategy_batch(specs, matrix.feature_names, matrix.holding_horizons)

    assert compiled.feature_indices.dtype == np.int16
    assert compiled.operator_codes.dtype == np.int8
    assert compiled.thresholds.dtype == np.float64
    assert not compiled.thresholds.flags.writeable
    with pytest.raises(ValueError, match="unknown features"):
        compile_strategy_batch(
            [replace(specs[0], feature="future_unknown")],
            matrix.feature_names,
            matrix.holding_horizons,
        )


def test_vectorized_stage1_is_identical_to_scalar_reference_across_micro_batches() -> None:
    specs = [_spec(index) for index in range(257)]
    matrix = _matrix()
    compiled = compile_strategy_batch(specs, matrix.feature_names, matrix.holding_horizons)
    reference = execute_stage1_reference(compiled, matrix)

    for micro_batch_size in (1, 17, 128, 1024):
        optimized = execute_stage1_vectorized(
            compiled, matrix, micro_batch_size=micro_batch_size
        )
        reference.assert_equivalent(optimized)
    assert (reference.opportunity_count > 0).all()
    assert (reference.approximate_max_drawdown >= 0.0).all()
    assert np.isfinite(reference.net_pnl).all()


def test_benchmark_uses_identical_inputs_and_reports_measured_speedup() -> None:
    specs = [_spec(index) for index in range(600)]
    matrix = _matrix(1_000)
    compiled = compile_strategy_batch(specs, matrix.feature_names, matrix.holding_horizons)
    benchmark = benchmark_stage1(compiled, matrix, repeats=2, micro_batch_size=300)

    assert benchmark.outputs_identical
    assert benchmark.strategies == 600
    assert benchmark.events == 1_000
    assert benchmark.reference_seconds > 0.0
    assert benchmark.vectorized_seconds > 0.0
    assert benchmark.speedup > 1.0
    assert benchmark.vectorized_candidates_per_second > benchmark.reference_candidates_per_second
