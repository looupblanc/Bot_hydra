"""Pure compute-plane primitives for HYDRA Turbo Foundry."""

from hydra.compute.backpressure import (
    BackpressureDecision,
    BackpressureLimits,
    BoundedAdmissionQueue,
    assess_backpressure,
)
from hydra.compute.resource_monitor import (
    BatchResourceMetrics,
    ResourceSnapshot,
    capture_resource_snapshot,
    summarize_task_resources,
)
from hydra.compute.result_writer import AtomicResultWriter, AtomicWriteReceipt
from hydra.compute.worker_pool import (
    LongLivedWorkerPool,
    PureTask,
    TaskResult,
    WorkerWriteProhibited,
)

__all__ = [
    "AtomicResultWriter",
    "AtomicWriteReceipt",
    "BackpressureDecision",
    "BackpressureLimits",
    "BatchResourceMetrics",
    "BoundedAdmissionQueue",
    "LongLivedWorkerPool",
    "PureTask",
    "ResourceSnapshot",
    "TaskResult",
    "WorkerWriteProhibited",
    "assess_backpressure",
    "capture_resource_snapshot",
    "summarize_task_resources",
]
