"""Low-overhead process and host resource accounting for Turbo batches."""

from __future__ import annotations

import os
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Protocol


@dataclass(frozen=True)
class ResourceSnapshot:
    captured_monotonic: float
    cpu_count: int
    load_1m: float
    load_5m: float
    load_15m: float
    memory_total_mb: float
    memory_available_mb: float
    process_rss_mb: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class BatchResourceMetrics:
    task_count: int
    completed_count: int
    failed_count: int
    unique_worker_pids: int
    worker_count_budget: int
    batch_wall_seconds: float
    aggregate_worker_cpu_seconds: float
    aggregate_worker_busy_wall_seconds: float
    aggregate_worker_utilization_pct: float
    maximum_worker_rss_mb: float
    estimated_peak_worker_rss_mb: float
    scheduler_idle_seconds: float
    scheduler_idle_rate_pct: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class TaskResourceLike(Protocol):
    status: str
    worker_pid: int
    wall_seconds: float
    cpu_seconds: float
    max_rss_mb: float


def capture_resource_snapshot() -> ResourceSnapshot:
    load = os.getloadavg()
    memory = _memory_info()
    return ResourceSnapshot(
        captured_monotonic=time.monotonic(),
        cpu_count=os.cpu_count() or 1,
        load_1m=float(load[0]),
        load_5m=float(load[1]),
        load_15m=float(load[2]),
        memory_total_mb=memory["MemTotal"] / 1024.0,
        memory_available_mb=memory.get("MemAvailable", memory.get("MemFree", 0.0))
        / 1024.0,
        process_rss_mb=_process_rss_mb(),
    )


def summarize_task_resources(
    results: Iterable[TaskResourceLike],
    *,
    batch_wall_seconds: float,
    worker_count_budget: int,
    scheduler_idle_seconds: float = 0.0,
) -> BatchResourceMetrics:
    rows = list(results)
    if batch_wall_seconds <= 0.0:
        raise ValueError("batch_wall_seconds must be positive")
    if worker_count_budget <= 0:
        raise ValueError("worker_count_budget must be positive")
    if scheduler_idle_seconds < 0.0:
        raise ValueError("scheduler_idle_seconds cannot be negative")
    cpu_seconds = sum(max(float(row.cpu_seconds), 0.0) for row in rows)
    busy_wall = sum(max(float(row.wall_seconds), 0.0) for row in rows)
    utilization = 100.0 * cpu_seconds / (
        batch_wall_seconds * worker_count_budget
    )
    utilization = min(max(utilization, 0.0), 100.0)
    rss_by_pid: dict[int, float] = {}
    for row in rows:
        rss_by_pid[row.worker_pid] = max(
            rss_by_pid.get(row.worker_pid, 0.0), float(row.max_rss_mb)
        )
    idle_rate = min(
        max(100.0 * scheduler_idle_seconds / batch_wall_seconds, 0.0), 100.0
    )
    return BatchResourceMetrics(
        task_count=len(rows),
        completed_count=sum(row.status == "COMPLETED" for row in rows),
        failed_count=sum(row.status != "COMPLETED" for row in rows),
        unique_worker_pids=len(rss_by_pid),
        worker_count_budget=worker_count_budget,
        batch_wall_seconds=float(batch_wall_seconds),
        aggregate_worker_cpu_seconds=float(cpu_seconds),
        aggregate_worker_busy_wall_seconds=float(busy_wall),
        aggregate_worker_utilization_pct=float(utilization),
        maximum_worker_rss_mb=max(rss_by_pid.values(), default=0.0),
        estimated_peak_worker_rss_mb=float(sum(rss_by_pid.values())),
        scheduler_idle_seconds=float(scheduler_idle_seconds),
        scheduler_idle_rate_pct=float(idle_rate),
    )


def _memory_info() -> dict[str, float]:
    values: dict[str, float] = {}
    for line in Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
        name, value = line.split(":", maxsplit=1)
        values[name] = float(value.strip().split()[0])
    return values


def _process_rss_mb() -> float:
    for line in Path("/proc/self/status").read_text(encoding="utf-8").splitlines():
        if line.startswith("VmRSS:"):
            return float(line.split()[1]) / 1024.0
    return 0.0
