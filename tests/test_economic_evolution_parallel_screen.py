from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np

from hydra.economic_evolution.generator import generate_structural_population
from hydra.economic_evolution.parallel_screen import (
    run_ultra_cheap_screen_parallel,
    run_ultra_cheap_screen_processes,
)
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
        arrays[f"forward_move__{horizon}"] = np.where(base >= 0.0, 1.0, -0.5)
    for value in arrays.values():
        value.setflags(write=False)
    return FeatureMatrix(
        root=Path("."),
        manifest={
            "row_count": rows,
            "bundle_hash": "synthetic",
            "provenance": {"point_value": 5.0, "round_turn_cost": 1.0},
        },
        arrays=arrays,
    )


def test_parallel_screen_is_bit_identical_to_serial_market_order() -> None:
    population = generate_structural_population(
        campaign_id="parallel_screen_population",
        raw_proposal_count=4_000,
        market_pairs={"ES": "MES", "NQ": "MNQ", "YM": "MYM"},
    )
    matrices = {"ES": _matrix(), "NQ": _matrix(), "YM": _matrix()}
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

    serial = run_ultra_cheap_screen(population.sleeves, matrices, policy=policy)
    parallel = run_ultra_cheap_screen_parallel(
        population.sleeves, matrices, policy=policy, worker_count=3
    )

    assert parallel.rows == serial.rows
    assert parallel.proposal_count == serial.proposal_count
    assert parallel.bound_count == serial.bound_count
    assert parallel.unique_execution_path_count == serial.unique_execution_path_count
    assert parallel.execution_cache_hit_count == serial.execution_cache_hit_count


def test_parallel_screen_rejects_invalid_worker_count() -> None:
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
    import pytest

    with pytest.raises(ValueError, match="worker_count"):
        run_ultra_cheap_screen_parallel((), {}, policy=policy, worker_count=0)


def test_process_screen_is_bit_identical_to_serial(
    tmp_path: Path,
) -> None:
    population = generate_structural_population(
        campaign_id="process_screen_population",
        raw_proposal_count=1_000,
        market_pairs={"ES": "MES", "NQ": "MNQ"},
    )
    matrices = {"ES": _matrix(), "NQ": _matrix()}
    roots = {
        market: _write_matrix(tmp_path / market, matrix)
        for market, matrix in matrices.items()
    }
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

    serial = run_ultra_cheap_screen(population.sleeves, matrices, policy=policy)
    processes = run_ultra_cheap_screen_processes(
        population.sleeves, roots, policy=policy, worker_count=2
    )

    assert processes.rows == serial.rows
    assert processes.unique_execution_path_count == serial.unique_execution_path_count


def _write_matrix(root: Path, matrix: FeatureMatrix) -> Path:
    root.mkdir(parents=True)
    arrays: dict[str, dict[str, object]] = {}
    for name, value in matrix.arrays.items():
        path = root / f"{name}.npy"
        np.save(path, value, allow_pickle=False)
        arrays[name] = {
            "path": path.name,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "shape": list(value.shape),
            "dtype": str(value.dtype),
        }
    manifest: dict[str, object] = {
        "row_count": matrix.row_count,
        "provenance": dict(matrix.manifest["provenance"]),
        "arrays": arrays,
    }
    manifest["bundle_hash"] = hashlib.sha256(
        json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    (root / "manifest.json").write_text(
        json.dumps(manifest, sort_keys=True), encoding="utf-8"
    )
    return root
