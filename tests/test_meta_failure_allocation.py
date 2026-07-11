from __future__ import annotations

from pathlib import Path

import pytest

from hydra.mission.calibration_retest_execution import _stable_hash
from hydra.research.meta_failure_allocation import (
    MetaFailureAllocationError,
    recommend_allocation,
    run_meta_failure_allocation,
)


def _snapshot() -> dict[str, object]:
    return {
        "strategy_prototypes_generated": 7998,
        "shadow_active_candidates": 4,
        "executable_baskets": 3,
        "experiments": [
            {
                "experiment_id": "cross-daily",
                "experiment_type": "cross_asset_daily_horizon_primary",
                "engine": "cross_asset_daily",
                "structural_prototypes": 720,
                "promising_candidates": 2,
                "shadow_candidates": 1,
                "topstep_path_candidates": 1,
                "scientific_conclusion": "CROSS_ASSET_DAILY_SHADOW_CANDIDATES_FOUND",
                "performance": {"total_seconds": 238.0},
            },
            {
                "experiment_id": "gc",
                "experiment_type": "gc_session_geometry_fresh_primary",
                "engine": "session_geometry",
                "structural_prototypes": 1,
                "promising_candidates": 1,
                "shadow_candidates": 0,
                "scientific_conclusion": (
                    "GC_SESSION_GEOMETRY_FRESH_PRIMARY_FALSIFIED_OR_INSUFFICIENT"
                ),
            },
        ],
    }


def test_allocation_is_shrunk_constrained_and_preserves_exploration() -> None:
    allocation = recommend_allocation(_snapshot(), [], __import__("collections").Counter())

    assert sum(allocation.values()) == 100
    assert max(allocation.values()) <= 25
    assert allocation["novel_methods"] > 0
    assert allocation["structural_discovery"] + allocation["novel_methods"] >= 15
    assert all(value > 0 for value in allocation.values())


def test_run_is_deterministic_in_science_fields_and_reads_no_market_data(
    tmp_path: Path,
) -> None:
    snapshot = _snapshot()
    task = Path(
        "reports/engineering/hydra_meta_failure_allocation_20260711.md"
    )
    result = run_meta_failure_allocation(
        tmp_path / "one",
        engineering_task_path=task,
        engineering_task_sha256=(
            "e637f4f50d01326a10f3a5a00e4bbdb9c5229abaa7d488831a38067c74ec0129"
        ),
        snapshot=snapshot,
        snapshot_hash=_stable_hash(snapshot),
        code_commit="test",
    )

    assert result["recommended_compute_allocation_pct"]
    assert result["constraints"]["allocation_total_pct"] == 100
    assert result["governance"]["market_data_rows_read"] == 0
    assert result["governance"]["shared_ledger_writes"] == 0
    assert result["governance"]["q4_access_count_delta"] == 0
    assert result["candidate_count"] == 0


def test_snapshot_hash_mismatch_fails_closed(tmp_path: Path) -> None:
    with pytest.raises(MetaFailureAllocationError, match="snapshot hash"):
        run_meta_failure_allocation(
            tmp_path,
            engineering_task_path=Path(
                "reports/engineering/hydra_meta_failure_allocation_20260711.md"
            ),
            engineering_task_sha256=(
                "e637f4f50d01326a10f3a5a00e4bbdb9c5229abaa7d488831a38067c74ec0129"
            ),
            snapshot=_snapshot(),
            snapshot_hash="wrong",
            code_commit="test",
        )
