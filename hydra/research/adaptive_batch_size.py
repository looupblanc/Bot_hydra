"""Capacity-aware translation of a scientific power plan into micro-batches."""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from typing import Literal

from hydra.research.power_planner import (
    INSUFFICIENT_BATCH_POWER,
    POWER_SUFFICIENT,
    PowerPlan,
)


ADAPTIVE_BATCH_VERSION = "hydra_adaptive_batch_v1"


class AdaptiveBatchError(ValueError):
    pass


@dataclass(frozen=True)
class BatchCapacity:
    maximum_proposals: int
    available_proposals: int
    candidates_per_second: float
    wall_time_budget_seconds: float
    micro_batch_size: int = 256
    worker_count: int = 2


@dataclass(frozen=True)
class AdaptiveBatchPlan:
    schema: str
    status: Literal["POWER_SUFFICIENT", "INSUFFICIENT_BATCH_POWER"]
    requested_structures: int
    scheduled_structures: int
    micro_batch_size: int
    micro_batch_count: int
    final_micro_batch_size: int
    expected_wall_seconds: float
    worker_count: int
    capacity_limited: bool
    recommended_action: str
    power_plan_status: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def choose_adaptive_batch_size(
    power_plan: PowerPlan,
    capacity: BatchCapacity,
) -> AdaptiveBatchPlan:
    """Schedule no more work than can finish inside a bounded research cycle."""

    _validate_capacity(capacity)
    time_capacity = math.floor(
        capacity.candidates_per_second * capacity.wall_time_budget_seconds
    )
    hard_capacity = min(
        capacity.maximum_proposals,
        capacity.available_proposals,
        time_capacity,
    )
    requested = power_plan.structures_required
    scheduled = min(requested, hard_capacity)
    if scheduled <= 0:
        scheduled = 0
        batch_count = 0
        final_size = 0
    else:
        batch_count = math.ceil(scheduled / capacity.micro_batch_size)
        final_size = scheduled - capacity.micro_batch_size * (batch_count - 1)
    capacity_limited = scheduled < requested
    sufficient = power_plan.sufficient and not capacity_limited
    if sufficient:
        status: Literal["POWER_SUFFICIENT", "INSUFFICIENT_BATCH_POWER"] = (
            POWER_SUFFICIENT
        )
        action = "RUN_MICRO_BATCHES"
    else:
        status = INSUFFICIENT_BATCH_POWER
        if power_plan.status == INSUFFICIENT_BATCH_POWER:
            action = power_plan.recommended_action
        elif time_capacity < requested:
            action = "INCREASE_TIME_BUDGET_OR_THROUGHPUT"
        elif capacity.available_proposals < requested:
            action = "GENERATE_MORE_STRUCTURAL_PROPOSALS"
        else:
            action = "INCREASE_MAXIMUM_BATCH_SIZE"
    expected_wall = (
        scheduled / capacity.candidates_per_second if scheduled else 0.0
    )
    return AdaptiveBatchPlan(
        schema=ADAPTIVE_BATCH_VERSION,
        status=status,
        requested_structures=requested,
        scheduled_structures=scheduled,
        micro_batch_size=capacity.micro_batch_size,
        micro_batch_count=batch_count,
        final_micro_batch_size=final_size,
        expected_wall_seconds=float(expected_wall),
        worker_count=capacity.worker_count,
        capacity_limited=capacity_limited,
        recommended_action=action,
        power_plan_status=power_plan.status,
    )


def _validate_capacity(capacity: BatchCapacity) -> None:
    for name, value in {
        "micro_batch_size": capacity.micro_batch_size,
        "worker_count": capacity.worker_count,
    }.items():
        if int(value) != value or value <= 0:
            raise AdaptiveBatchError(f"{name} must be a positive integer")
    for name, value in {
        "maximum_proposals": capacity.maximum_proposals,
        "available_proposals": capacity.available_proposals,
    }.items():
        if int(value) != value or value < 0:
            raise AdaptiveBatchError(f"{name} must be a non-negative integer")
    for name, value in {
        "candidates_per_second": capacity.candidates_per_second,
        "wall_time_budget_seconds": capacity.wall_time_budget_seconds,
    }.items():
        if not math.isfinite(value) or value <= 0.0:
            raise AdaptiveBatchError(f"{name} must be finite and positive")
