"""Shared worker scheduler for CQ runtime operations."""

from __future__ import annotations

import atexit
import multiprocessing
import threading
from collections.abc import Callable, Iterable
from concurrent.futures import Future, ProcessPoolExecutor, ThreadPoolExecutor, wait
from dataclasses import dataclass
from typing import ParamSpec

from tools.cq.core.runtime.execution_policy import (
    ParallelismPolicy,
    default_runtime_execution_policy,
)

P = ParamSpec("P")


@dataclass(slots=True)
class WorkerBatchResult[T]:
    """Result bundle for bounded worker collection."""

    done: list[T]
    timed_out: int


class WorkerScheduler:
    """Lazily-initialized shared worker pools with bounded collection helpers."""

    def __init__(self, policy: ParallelismPolicy) -> None:
        """Initialize scheduler pools for the provided policy.

        Args:
            policy: Parallel execution settings.
        """
        self._policy = policy
        self._lock = threading.Lock()
        self._cpu_pool: ProcessPoolExecutor | None = None
        self._io_pool: ThreadPoolExecutor | None = None

    @property
    def policy(self) -> ParallelismPolicy:
        """Return active scheduler policy."""
        return self._policy

    def io_pool(self) -> ThreadPoolExecutor:
        """Return shared IO pool."""
        with self._lock:
            if self._io_pool is None:
                self._io_pool = ThreadPoolExecutor(max_workers=self._policy.io_workers)
            return self._io_pool

    def cpu_pool(self) -> ProcessPoolExecutor:
        """Return shared CPU pool configured with spawn context."""
        with self._lock:
            if self._cpu_pool is None:
                ctx = multiprocessing.get_context("spawn")
                self._cpu_pool = ProcessPoolExecutor(
                    max_workers=self._policy.cpu_workers,
                    mp_context=ctx,
                )
            return self._cpu_pool

    def submit_io[T](self, fn: Callable[P, T], /, *args: P.args, **kwargs: P.kwargs) -> Future[T]:
        """Submit an I/O task.

        Returns:
            Future representing the scheduled task.
        """
        return self.io_pool().submit(fn, *args, **kwargs)

    def submit_cpu[T](self, fn: Callable[P, T], /, *args: P.args, **kwargs: P.kwargs) -> Future[T]:
        """Submit a CPU task.

        Returns:
            Future representing the scheduled task.
        """
        if self._policy.enable_process_pool:
            return self.cpu_pool().submit(fn, *args, **kwargs)
        return self.io_pool().submit(fn, *args, **kwargs)

    @staticmethod
    def collect_bounded[T](
        futures: Iterable[Future[T]],
        *,
        timeout_seconds: float,
    ) -> WorkerBatchResult[T]:
        """Collect futures up to timeout, preserving deterministic completion order.

        Returns:
            Completed values and timeout count.
        """
        future_list = list(futures)
        if not future_list:
            return WorkerBatchResult(done=[], timed_out=0)
        done, pending = wait(future_list, timeout=timeout_seconds)
        values: list[T] = [future.result() for future in future_list if future in done]
        for future in pending:
            future.cancel()
        return WorkerBatchResult(done=values, timed_out=len(pending))

    def close(self) -> None:
        """Shutdown shared pools."""
        with self._lock:
            cpu_pool = self._cpu_pool
            io_pool = self._io_pool
            self._cpu_pool = None
            self._io_pool = None
        if cpu_pool is not None:
            cpu_pool.shutdown(wait=False, cancel_futures=True)
        if io_pool is not None:
            io_pool.shutdown(wait=False, cancel_futures=True)


_SCHEDULER_LOCK = threading.Lock()


class _SchedulerState:
    """Mutable holder for process-global scheduler singleton."""

    def __init__(self) -> None:
        """Initialize empty scheduler state."""
        self.scheduler: WorkerScheduler | None = None


_SCHEDULER_STATE = _SchedulerState()


def get_worker_scheduler() -> WorkerScheduler:
    """Return process-global CQ worker scheduler."""
    with _SCHEDULER_LOCK:
        if _SCHEDULER_STATE.scheduler is None:
            policy = default_runtime_execution_policy().parallelism
            _SCHEDULER_STATE.scheduler = WorkerScheduler(policy)
        return _SCHEDULER_STATE.scheduler


def close_worker_scheduler() -> None:
    """Close and clear process-global worker scheduler."""
    with _SCHEDULER_LOCK:
        scheduler = _SCHEDULER_STATE.scheduler
        _SCHEDULER_STATE.scheduler = None
    if scheduler is not None:
        scheduler.close()


atexit.register(close_worker_scheduler)


__all__ = [
    "WorkerBatchResult",
    "WorkerScheduler",
    "close_worker_scheduler",
    "get_worker_scheduler",
]
