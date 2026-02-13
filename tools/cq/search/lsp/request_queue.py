"""Bounded LSP request queue built on shared worker scheduler."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from concurrent.futures import Future

from tools.cq.core.runtime.worker_scheduler import get_worker_scheduler


def run_lsp_requests(
    callables: Iterable[Callable[[], object]],
    *,
    timeout_seconds: float,
) -> tuple[list[object], int]:
    """Execute request callables in shared IO pool with bounded timeout.

    Returns:
        Completed results and number of timed-out requests.
    """
    scheduler = get_worker_scheduler()
    futures: list[Future[object]] = [
        scheduler.submit_io(callable_item) for callable_item in callables
    ]
    batch = scheduler.collect_bounded(futures, timeout_seconds=timeout_seconds)
    return batch.done, batch.timed_out


__all__ = ["run_lsp_requests"]
