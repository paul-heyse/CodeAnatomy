"""Parallel execution helpers for extractors."""

from __future__ import annotations

import multiprocessing
import os
import sys
import threading
from collections.abc import Callable, Iterable, Iterator, Mapping
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from typing import TypeVar

from opentelemetry import context as otel_context
from opentelemetry import propagate

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
    """Return True when the multiprocessing runtime can safely use fork.

    Returns
    -------
    bool
        ``True`` when the fork start method is available and the process
        is not multi-threaded.
    """
    if "fork" not in multiprocessing.get_all_start_methods():
        return False
    return threading.active_count() <= 1


def _process_executor(
    max_workers: int | None,
    carrier: Mapping[str, str] | None,
) -> ProcessPoolExecutor:
    def _worker_init(carrier_payload: Mapping[str, str] | None) -> None:
        from obs.otel import configure_otel

        configure_otel(service_name="codeanatomy")
        if carrier_payload:
            context = propagate.extract(carrier_payload)
            otel_context.attach(context)

    ctx = (
        multiprocessing.get_context("fork")
        if supports_fork()
        else multiprocessing.get_context("spawn")
    )
    return ProcessPoolExecutor(
        max_workers=max_workers,
        mp_context=ctx,
        initializer=_worker_init,
        initargs=(carrier,),
    )


def _build_propagation_carrier() -> dict[str, str]:
    carrier: dict[str, str] = {}
    propagate.inject(carrier)
    return carrier


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
        current = otel_context.get_current()

        def _wrapped(item: T) -> U:
            token = otel_context.attach(current)
            try:
                return fn(item)
            finally:
                otel_context.detach(token)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for item in executor.map(_wrapped, items):
                yield item
        return
    carrier = _build_propagation_carrier()
    with _process_executor(max_workers, carrier) as executor:
        for item in executor.map(fn, items):
            yield item


def resolve_max_workers(
    max_workers: int | None,
    *,
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
    _ = kind
    cpu_count = os.cpu_count() or 1
    return max(1, cpu_count)


__all__ = ["gil_disabled", "parallel_map", "resolve_max_workers", "supports_fork"]
