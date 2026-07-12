"""Bounded admission and deterministic backpressure for asynchronous lanes."""

from __future__ import annotations

import queue
from dataclasses import dataclass
from typing import Generic, Mapping, TypeVar


ItemT = TypeVar("ItemT")


@dataclass(frozen=True)
class BackpressureLimits:
    discovery_queue: int = 10_000
    exact_replay_queue: int = 500
    promotion_queue: int = 100
    writer_queue: int = 1_000
    high_watermark: float = 0.80
    critical_watermark: float = 0.95

    def capacities(self) -> dict[str, int]:
        return {
            "discovery_queue": self.discovery_queue,
            "exact_replay_queue": self.exact_replay_queue,
            "promotion_queue": self.promotion_queue,
            "writer_queue": self.writer_queue,
        }


@dataclass(frozen=True)
class BackpressureDecision:
    status: str
    discovery_admission_fraction: float
    exact_replay_admission_fraction: float
    promotion_admission_fraction: float
    saturated_queues: tuple[str, ...]
    critical_queues: tuple[str, ...]
    reason: str


def assess_backpressure(
    depths: Mapping[str, int],
    *,
    limits: BackpressureLimits = BackpressureLimits(),
) -> BackpressureDecision:
    capacities = limits.capacities()
    if not 0.0 < limits.high_watermark < limits.critical_watermark <= 1.0:
        raise ValueError("watermarks must satisfy 0 < high < critical <= 1")
    utilization: dict[str, float] = {}
    for lane, capacity in capacities.items():
        if capacity <= 0:
            raise ValueError(f"{lane} capacity must be positive")
        depth = int(depths.get(lane, 0))
        if depth < 0:
            raise ValueError(f"{lane} depth cannot be negative")
        utilization[lane] = depth / capacity
    saturated = tuple(
        sorted(lane for lane, value in utilization.items() if value >= limits.high_watermark)
    )
    critical = tuple(
        sorted(lane for lane, value in utilization.items() if value >= limits.critical_watermark)
    )

    if "writer_queue" in critical:
        return BackpressureDecision(
            status="PAUSE_UPSTREAM",
            discovery_admission_fraction=0.0,
            exact_replay_admission_fraction=0.0,
            promotion_admission_fraction=0.0,
            saturated_queues=saturated,
            critical_queues=critical,
            reason="SINGLE_WRITER_CRITICAL",
        )
    discovery_fraction = 1.0
    exact_fraction = 1.0
    promotion_fraction = 1.0
    if "promotion_queue" in saturated:
        exact_fraction = 0.25
        promotion_fraction = 0.25
    if "exact_replay_queue" in saturated:
        discovery_fraction = 0.25
    if "discovery_queue" in critical:
        discovery_fraction = 0.0
    if "writer_queue" in saturated:
        discovery_fraction = min(discovery_fraction, 0.25)
        exact_fraction = min(exact_fraction, 0.25)
        promotion_fraction = min(promotion_fraction, 0.25)
    status = "THROTTLE" if saturated else "ACCEPT"
    return BackpressureDecision(
        status=status,
        discovery_admission_fraction=discovery_fraction,
        exact_replay_admission_fraction=exact_fraction,
        promotion_admission_fraction=promotion_fraction,
        saturated_queues=saturated,
        critical_queues=critical,
        reason="DOWNSTREAM_QUEUE_PRESSURE" if saturated else "CAPACITY_AVAILABLE",
    )


class BoundedAdmissionQueue(Generic[ItemT]):
    """Small queue façade which never grows beyond its declared capacity."""

    def __init__(self, maxsize: int) -> None:
        if maxsize <= 0:
            raise ValueError("maxsize must be positive")
        self.maxsize = int(maxsize)
        self._queue: queue.Queue[ItemT] = queue.Queue(maxsize=self.maxsize)

    def admit(self, item: ItemT) -> bool:
        try:
            self._queue.put_nowait(item)
        except queue.Full:
            return False
        return True

    def take(self) -> ItemT:
        return self._queue.get_nowait()

    @property
    def depth(self) -> int:
        return self._queue.qsize()

    @property
    def remaining_capacity(self) -> int:
        return self.maxsize - self.depth
