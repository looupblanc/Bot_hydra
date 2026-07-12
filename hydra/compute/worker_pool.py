"""Long-lived process workers which return values to the sole coordinator."""

from __future__ import annotations

import multiprocessing
import os
import resource
import sqlite3
import time
import traceback
from concurrent.futures import Future, ProcessPoolExecutor
from dataclasses import dataclass
from typing import Any, Callable, Generic, Iterable, Iterator, TypeVar


PayloadT = TypeVar("PayloadT")
ValueT = TypeVar("ValueT")


class WorkerWriteProhibited(RuntimeError):
    """A compute worker attempted to open a writable shared-state database."""


@dataclass(frozen=True)
class PureTask(Generic[PayloadT]):
    task_id: str
    payload: PayloadT


@dataclass(frozen=True)
class TaskResult(Generic[ValueT]):
    task_id: str
    status: str
    value: ValueT | None
    error_type: str | None
    error_message: str | None
    traceback_text: str | None
    worker_pid: int
    wall_seconds: float
    cpu_seconds: float
    max_rss_mb: float
    shared_state_writes: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status == "COMPLETED"


def _install_worker_safety() -> None:
    os.environ["HYDRA_COMPUTE_WORKER"] = "1"
    os.environ["HYDRA_SHARED_STATE_WRITES_ALLOWED"] = "0"
    original_connect = sqlite3.connect

    def guarded_connect(database: Any, *args: Any, **kwargs: Any) -> sqlite3.Connection:
        target = str(database)
        uri = bool(kwargs.get("uri", False))
        read_only = uri and ("mode=ro" in target or "immutable=1" in target)
        if not read_only:
            raise WorkerWriteProhibited(
                "compute workers may open SQLite only with uri=True and mode=ro"
            )
        return original_connect(database, *args, **kwargs)

    sqlite3.connect = guarded_connect  # type: ignore[assignment]


def _run_pure_task(
    worker: Callable[[PayloadT], ValueT], task: PureTask[PayloadT]
) -> TaskResult[ValueT]:
    wall_start = time.perf_counter()
    cpu_start = time.process_time()
    try:
        value = worker(task.payload)
    except BaseException as exc:  # worker failures are data, not pool corruption
        return TaskResult(
            task_id=task.task_id,
            status="FAILED",
            value=None,
            error_type=type(exc).__name__,
            error_message=str(exc),
            traceback_text=traceback.format_exc(),
            worker_pid=os.getpid(),
            wall_seconds=time.perf_counter() - wall_start,
            cpu_seconds=time.process_time() - cpu_start,
            max_rss_mb=_max_rss_mb(),
        )
    return TaskResult(
        task_id=task.task_id,
        status="COMPLETED",
        value=value,
        error_type=None,
        error_message=None,
        traceback_text=None,
        worker_pid=os.getpid(),
        wall_seconds=time.perf_counter() - wall_start,
        cpu_seconds=time.process_time() - cpu_start,
        max_rss_mb=_max_rss_mb(),
    )


def _max_rss_mb() -> float:
    # Linux reports ru_maxrss in KiB; macOS reports bytes. HYDRA deploys on Linux.
    value = float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    return value / 1024.0


class LongLivedWorkerPool:
    """A reusable pool scoped to a rolling batch or controller compute context.

    Constructing the class starts no work.  The same executor and PIDs are
    reused across every ``run_batch``/``submit_batch`` call until ``close``.
    Workers receive payloads and return immutable results; they never receive a
    mission/registry connection and writable SQLite opens are rejected.
    """

    def __init__(
        self,
        *,
        max_workers: int,
        start_method: str = "spawn",
    ) -> None:
        if max_workers <= 0:
            raise ValueError("max_workers must be positive")
        if start_method not in multiprocessing.get_all_start_methods():
            raise ValueError(f"unsupported multiprocessing start method: {start_method}")
        self.max_workers = int(max_workers)
        self.start_method = start_method
        self._owner_pid = os.getpid()
        self._closed = False
        self._executor = ProcessPoolExecutor(
            max_workers=self.max_workers,
            mp_context=multiprocessing.get_context(start_method),
            initializer=_install_worker_safety,
        )

    def submit_batch(
        self,
        worker: Callable[[PayloadT], ValueT],
        tasks: Iterable[PureTask[PayloadT]],
    ) -> tuple[Future[TaskResult[ValueT]], ...]:
        self._check_owner()
        return tuple(
            self._executor.submit(_run_pure_task, worker, task) for task in tasks
        )

    def run_batch(
        self,
        worker: Callable[[PayloadT], ValueT],
        tasks: Iterable[PureTask[PayloadT]],
    ) -> tuple[TaskResult[ValueT], ...]:
        futures = self.submit_batch(worker, tasks)
        return tuple(future.result() for future in futures)

    def iter_completed(
        self,
        worker: Callable[[PayloadT], ValueT],
        tasks: Iterable[PureTask[PayloadT]],
    ) -> Iterator[TaskResult[ValueT]]:
        # Preserve deterministic input/result ordering.  Backpressure is applied
        # before this layer, so ordering is preferable to unbounded completion
        # buffering in the sole writer.
        yield from self.run_batch(worker, tasks)

    def close(self, *, cancel_futures: bool = False) -> None:
        self._check_owner(allow_closed=True)
        if self._closed:
            return
        self._executor.shutdown(wait=True, cancel_futures=cancel_futures)
        self._closed = True

    def _check_owner(self, *, allow_closed: bool = False) -> None:
        if os.getpid() != self._owner_pid:
            raise WorkerWriteProhibited("worker pool may only be controlled by owner PID")
        if self._closed and not allow_closed:
            raise RuntimeError("worker pool is closed")

    def __enter__(self) -> LongLivedWorkerPool:
        self._check_owner()
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback_value: Any) -> None:
        self.close(cancel_futures=exc_type is not None)
