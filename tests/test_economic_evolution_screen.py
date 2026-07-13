from __future__ import annotations

from dataclasses import replace

import numpy as np

from hydra.economic_evolution.generator import generate_structural_population
from hydra.economic_evolution.screen import CheapScreenPolicy, run_ultra_cheap_screen
from hydra.features.feature_matrix import FeatureMatrix
from hydra.research.turbo_feature_builder import HORIZONS, feature_names_for_bundle


def _matrix(rows: int = 600) -> FeatureMatrix:
    names = feature_names_for_bundle()
    base = np.linspace(-1.0, 1.0, rows, dtype=np.float64)
    start = int(np.datetime64("2023-01-01", "D").astype(np.int64))
    days = start + np.arange(rows, dtype=np.int64)
    arrays: dict[str, np.ndarray] = {
        "session_day": days,
        "session_code": np.arange(rows, dtype=np.int16) % 3,
        "decision_ns": np.arange(rows, dtype=np.int64) * 60_000_000_000,
        "availability_ns": np.arange(rows, dtype=np.int64) * 60_000_000_000,
        "segment_code": np.zeros(rows, dtype=np.int64),
    }
    for index, name in enumerate(names):
        arrays[f"feature__{name}"] = base + index * 0.001
    for horizon in HORIZONS:
        # A deterministic development-only target used solely to verify screen
        # arithmetic; the production code never places it in feature inputs.
        arrays[f"forward_move__{horizon}"] = np.where(base >= 0.0, 1.0, -0.5)
    for value in arrays.values():
        value.setflags(write=False)
    return FeatureMatrix(
        root=None,  # type: ignore[arg-type]
        manifest={
            "row_count": rows,
            "bundle_hash": "synthetic",
            "provenance": {"point_value": 5.0, "round_turn_cost": 1.0},
        },
        arrays=arrays,
    )


def test_ultra_cheap_screen_is_deterministic_cached_and_scope_bounded() -> None:
    population = generate_structural_population(
        campaign_id="screen_population",
        raw_proposal_count=2_000,
        market_pairs={"ES": "MES"},
    )
    policy = CheapScreenPolicy(
        calibration_start="2023-01-01",
        calibration_end_exclusive="2023-05-01",
        screen_start="2023-05-01",
        screen_end_exclusive="2024-01-01",
        minimum_opportunities=5,
        stress_cost_multiplier=1.5,
        maximum_best_positive_event_share=1.0,
        maximum_approximate_drawdown=100_000.0,
        require_nonnegative_half=False,
        micro_batch_size=32,
    )
    first = run_ultra_cheap_screen(
        population.sleeves, {"ES": _matrix()}, policy=policy
    )
    second = run_ultra_cheap_screen(
        population.sleeves, {"ES": _matrix()}, policy=policy
    )

    assert first.rows == second.rows
    assert first.bound_count == len(population.sleeves)
    assert first.unique_execution_path_count < first.bound_count
    assert first.execution_cache_hit_count > 0
    assert first.screens_per_second > 0.0
    assert all(row["walk_forward_executed"] is False for row in first.rows)
    assert all(row["tripwire_executed"] is False for row in first.rows)
    assert all(row["DSR_BH_executed"] is False for row in first.rows)
    assert all(row["rolling_combine_executed"] is False for row in first.rows)
    assert all(row["validation_scope"] == "ULTRA_CHEAP_DEVELOPMENT_SCREEN_ONLY" for row in first.rows)


def test_screen_rejects_future_feature_availability() -> None:
    matrix = _matrix()
    availability = np.array(matrix.array("availability_ns"), copy=True)
    availability[10] += 1
    availability.setflags(write=False)
    arrays = dict(matrix.arrays)
    arrays["availability_ns"] = availability
    future_matrix = replace(matrix, arrays=arrays)
    population = generate_structural_population(
        campaign_id="availability_test",
        raw_proposal_count=30,
        market_pairs={"ES": "MES"},
    )
    policy = CheapScreenPolicy(
        calibration_start="2023-01-01",
        calibration_end_exclusive="2023-05-01",
        screen_start="2023-05-01",
        screen_end_exclusive="2024-01-01",
        minimum_opportunities=5,
        stress_cost_multiplier=1.5,
        maximum_best_positive_event_share=1.0,
        maximum_approximate_drawdown=100_000.0,
        require_nonnegative_half=False,
    )
    # The modified row is outside the selected screen interval; make one inside
    # it invalid to prove EventMatrix's availability guard remains authoritative.
    arrays = dict(future_matrix.arrays)
    availability = np.array(arrays["availability_ns"], copy=True)
    availability[150] = arrays["decision_ns"][150] + 1
    availability.setflags(write=False)
    arrays["availability_ns"] = availability
    future_matrix = replace(future_matrix, arrays=arrays)
    import pytest

    with pytest.raises(ValueError, match="availability cannot follow"):
        run_ultra_cheap_screen(
            population.sleeves, {"ES": future_matrix}, policy=policy
        )


def test_insufficient_feature_calibration_rejects_sleeves_not_campaign() -> None:
    matrix = _matrix()
    arrays = dict(matrix.arrays)
    extreme = np.array(arrays["feature__extreme_dwell"], copy=True)
    extreme[:181] = np.nan
    extreme.setflags(write=False)
    arrays["feature__extreme_dwell"] = extreme
    sparse = replace(matrix, arrays=arrays)
    population = generate_structural_population(
        campaign_id="sparse_calibration_test",
        raw_proposal_count=500,
        market_pairs={"ES": "MES"},
    )
    policy = CheapScreenPolicy(
        calibration_start="2023-01-01",
        calibration_end_exclusive="2023-05-01",
        screen_start="2023-05-01",
        screen_end_exclusive="2024-01-01",
        minimum_opportunities=5,
        stress_cost_multiplier=1.5,
        maximum_best_positive_event_share=1.0,
        maximum_approximate_drawdown=100_000.0,
        require_nonnegative_half=False,
    )

    result = run_ultra_cheap_screen(
        population.sleeves, {"ES": sparse}, policy=policy
    )
    rejected = [
        row
        for row in result.rows
        if row["disposition"] == "HARD_INSUFFICIENT_CALIBRATION_AVAILABILITY"
    ]

    assert len(result.rows) == len(population.sleeves)
    assert rejected
    assert all(row["calibration_unavailable_feature"] == "extreme_dwell" for row in rejected)
    assert result.summary()["structural_rejection_count"] == len(rejected)
