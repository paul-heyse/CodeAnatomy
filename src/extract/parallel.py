"""Parallel execution helpers for extractors."""

from __future__ import annotations

import multiprocessing
import os
import sys
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from typing import TypeVar

from arrowdsl.core.execution_context import ExecutionContext

T = TypeVar("T")
U = TypeVar("U")


def _gil_disabled() -> bool:
    checker = getattr(sys, "_is_gil_enabled", None)
    if not callable(checker):
        return False
    try:
        return not bool(checker())
    except (RuntimeError, TypeError, ValueError):
        return False


def gil_disabled() -> bool:
    """Return True when running without the GIL.

    Returns
    -------
    bool
        ``True`` when Python is running without the GIL.
    """
    return _gil_disabled()


def supports_fork() -> bool:
    """Return True when the multiprocessing runtime supports fork.

    Returns
    -------
    bool
        ``True`` when the fork start method is available.
    """
    return "fork" in multiprocessing.get_all_start_methods()


def _process_executor(max_workers: int | None) -> ProcessPoolExecutor:
    if supports_fork():
        ctx = multiprocessing.get_context("fork")
        return ProcessPoolExecutor(max_workers=max_workers, mp_context=ctx)
    return ProcessPoolExecutor(max_workers=max_workers)


def parallel_map(
    items: Iterable[T],
    fn: Callable[[T], U],
    *,
    max_workers: int | None = None,
) -> Iterator[U]:
    """Map items in parallel, preferring processes unless free-threaded.

    Yields
    ------
    U
        Results produced by applying the function to each item.
    """
    if _gil_disabled():
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for item in executor.map(fn, items):
                yield item
        return
    with _process_executor(max_workers) as executor:
        for item in executor.map(fn, items):
            yield item


def resolve_max_workers(
    max_workers: int | None,
    *,
    ctx: ExecutionContext | None,
    kind: str = "cpu",
) -> int:
    """Resolve max_workers using runtime defaults when unset.

    Returns
    -------
    int
        Effective worker count.
    """
    if max_workers is not None:
        return max(1, max_workers)
    if ctx is not None:
        runtime = ctx.runtime
        if kind == "io" and runtime.io_threads is not None:
            return max(1, runtime.io_threads)
        if kind != "io" and runtime.cpu_threads is not None:
            return max(1, runtime.cpu_threads)
    cpu_count = os.cpu_count() or 1
    return max(1, cpu_count)


__all__ = ["gil_disabled", "parallel_map", "resolve_max_workers", "supports_fork"]
