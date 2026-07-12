from __future__ import annotations

import math
import os
import sqlite3
import time
from pathlib import Path

import pytest

from hydra.compute.backpressure import (
    BackpressureLimits,
    BoundedAdmissionQueue,
    assess_backpressure,
)
from hydra.compute.resource_monitor import summarize_task_resources
from hydra.compute.result_writer import AtomicResultWriter, ResultWriterError
from hydra.compute.worker_pool import LongLivedWorkerPool, PureTask
from hydra.research.adaptive_batch_size import (
    BatchCapacity,
    choose_adaptive_batch_size,
)
from hydra.research.power_planner import (
    INSUFFICIENT_BATCH_POWER,
    POWER_SUFFICIENT,
    PowerPlanningRequest,
    plan_experiment_power,
)
from hydra.research.turbo_meta_screen import (
    fit_temporal_meta_screen,
    prioritize_with_exploration,
)


def _cpu_work(value: int) -> tuple[int, int]:
    total = 0
    for index in range(120_000):
        total += (index * (value + 1)) % 97
    return os.getpid(), total


def _attempt_writable_sqlite(path: str) -> str:
    sqlite3.connect(path)
    return "unexpected"


def _meta_rows(count: int = 150) -> list[dict[str, float | int | str]]:
    rows: list[dict[str, float | int | str]] = []
    for index in range(count):
        signal = ((index * 37) % 101) / 100.0
        cost = ((index * 19) % 43) / 43.0
        # Deterministic relationship with both classes in every temporal block.
        target = int(signal - 0.35 * cost + (index % 7) * 0.015 > 0.48)
        rows.append(
            {
                "candidate_id": f"candidate-{index:04d}",
                "signal": signal,
                "cost": cost,
                "stage1_success": target,
            }
        )
    return rows


def test_power_plan_reports_required_events_structures_and_insufficiency() -> None:
    sufficient = plan_experiment_power(
        PowerPlanningRequest(
            minimum_useful_effect=0.25,
            outcome_variance=1.0,
            expected_opportunity_frequency=0.10,
            observations_per_structure=2_000,
            available_events=10_000,
            maximum_structures=1_000,
            effect_prevalence=0.05,
        )
    )
    assert sufficient.status == POWER_SUFFICIENT
    assert sufficient.required_events > 100
    assert sufficient.structures_required >= sufficient.search_coverage_structures_required
    assert sufficient.assumptions["strategy_evidence"] is False

    insufficient = plan_experiment_power(
        PowerPlanningRequest(
            minimum_useful_effect=0.10,
            outcome_variance=1.0,
            expected_opportunity_frequency=0.002,
            observations_per_structure=100,
            available_events=50,
            maximum_structures=32,
            effect_prevalence=0.01,
        )
    )
    assert insufficient.status == INSUFFICIENT_BATCH_POWER
    assert "AVAILABLE_EVENTS" in insufficient.limiting_factors
    assert insufficient.achieved_power_at_available_events < insufficient.target_power


def test_adaptive_batch_never_claims_power_when_capacity_is_too_small() -> None:
    power = plan_experiment_power(
        PowerPlanningRequest(
            minimum_useful_effect=0.3,
            outcome_variance=1.0,
            expected_opportunity_frequency=0.2,
            observations_per_structure=1_000,
            available_events=50_000,
            maximum_structures=10_000,
        )
    )
    limited = choose_adaptive_batch_size(
        power,
        BatchCapacity(
            maximum_proposals=10_000,
            available_proposals=10_000,
            candidates_per_second=1.0,
            wall_time_budget_seconds=2.0,
            micro_batch_size=256,
            worker_count=2,
        ),
    )
    assert limited.status == INSUFFICIENT_BATCH_POWER
    assert limited.capacity_limited
    assert limited.scheduled_structures == 2

    runnable = choose_adaptive_batch_size(
        power,
        BatchCapacity(
            maximum_proposals=20_000,
            available_proposals=20_000,
            candidates_per_second=2_000.0,
            wall_time_budget_seconds=60.0,
            micro_batch_size=32,
            worker_count=2,
        ),
    )
    assert runnable.status == POWER_SUFFICIENT
    assert runnable.scheduled_structures == power.structures_required
    assert runnable.micro_batch_count == math.ceil(
        power.structures_required / runnable.micro_batch_size
    )


def test_meta_screen_is_temporal_calibrated_allocation_only_and_explores() -> None:
    history = _meta_rows()
    fitted = fit_temporal_meta_screen(
        history,
        feature_names=("signal", "cost"),
        target_name="stage1_success",
    )
    assert fitted.train_count == 90
    assert fitted.calibration_count == 30
    assert fitted.oos_count == 30
    assert fitted.oos_start_index == 120
    assert fitted.strategy_evidence is False
    assert fitted.may_validate_or_promote is False
    assert 0.0 <= fitted.oos_brier_score <= 1.0
    assert fitted.calibration_bins

    candidates = _meta_rows(50)
    allocation = prioritize_with_exploration(
        candidates,
        fitted=fitted,
        capacity=20,
        exploration_share=0.20,
        seed=73,
    )
    assert allocation.exploration_count == 4
    assert allocation.exploration_share >= 0.20
    assert allocation.strategy_evidence is False
    assert allocation.may_validate_or_promote is False
    assert len({row.candidate_id for row in allocation.selected}) == 20
    assert sum(row.lane == "PURE_EXPLORATION" for row in allocation.selected) == 4
    repeated = prioritize_with_exploration(
        list(reversed(candidates)),
        fitted=fitted,
        capacity=20,
        exploration_share=0.20,
        seed=73,
    )
    assert [row.candidate_id for row in allocation.selected] == [
        row.candidate_id for row in repeated.selected
    ]


def test_long_lived_pool_reuses_workers_and_rejects_writable_sqlite(tmp_path: Path) -> None:
    started = time.perf_counter()
    with LongLivedWorkerPool(max_workers=2) as pool:
        first = pool.run_batch(
            _cpu_work,
            [PureTask(task_id=f"a-{index}", payload=index) for index in range(4)],
        )
        second = pool.run_batch(
            _cpu_work,
            [PureTask(task_id=f"b-{index}", payload=index) for index in range(4)],
        )
        prohibited = pool.run_batch(
            _attempt_writable_sqlite,
            [PureTask(task_id="db-write", payload=str(tmp_path / "forbidden.db"))],
        )
    elapsed = time.perf_counter() - started
    assert all(row.succeeded for row in (*first, *second))
    first_pids = {row.worker_pid for row in first}
    second_pids = {row.worker_pid for row in second}
    assert first_pids == second_pids
    assert len(first_pids) == 2
    assert prohibited[0].status == "FAILED"
    assert prohibited[0].error_type == "WorkerWriteProhibited"
    assert not (tmp_path / "forbidden.db").exists()

    metrics = summarize_task_resources(
        (*first, *second),
        batch_wall_seconds=elapsed,
        worker_count_budget=2,
        scheduler_idle_seconds=0.01,
    )
    assert metrics.completed_count == 8
    assert metrics.unique_worker_pids == 2
    assert 0.0 < metrics.aggregate_worker_utilization_pct <= 100.0
    assert metrics.maximum_worker_rss_mb > 0.0
    assert metrics.scheduler_idle_rate_pct >= 0.0


def test_backpressure_is_bounded_and_throttles_upstream() -> None:
    limits = BackpressureLimits(
        discovery_queue=10,
        exact_replay_queue=10,
        promotion_queue=10,
        writer_queue=10,
    )
    decision = assess_backpressure(
        {
            "discovery_queue": 2,
            "exact_replay_queue": 8,
            "promotion_queue": 9,
            "writer_queue": 1,
        },
        limits=limits,
    )
    assert decision.status == "THROTTLE"
    assert decision.discovery_admission_fraction == 0.25
    assert decision.exact_replay_admission_fraction == 0.25

    queue_ = BoundedAdmissionQueue[int](maxsize=2)
    assert queue_.admit(1)
    assert queue_.admit(2)
    assert not queue_.admit(3)
    assert queue_.depth == 2
    assert queue_.take() == 1


def test_atomic_result_writer_is_file_only_immutable_and_idempotent(
    tmp_path: Path,
) -> None:
    writer = AtomicResultWriter(tmp_path)
    first = writer.write_json("batch/results.json", {"count": 2, "valid": True})
    second = writer.write_json("batch/results.json", {"count": 2, "valid": True})
    assert first.idempotent_existing is False
    assert second.idempotent_existing is True
    assert first.sha256 == second.sha256
    assert not list(tmp_path.rglob("*.tmp-*"))
    with pytest.raises(ResultWriterError, match="divergent immutable"):
        writer.write_json("batch/results.json", {"count": 3})
    with pytest.raises(ResultWriterError, match="escapes"):
        writer.write_text("../outside.txt", "bad")
    with pytest.raises(ResultWriterError, match="database"):
        writer.write_bytes("mission.db", b"not-a-database")
