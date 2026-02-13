"""Bounded keyed LSP request queue helpers."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from concurrent.futures import wait
from dataclasses import dataclass
from time import monotonic

from tools.cq.core.runtime.worker_scheduler import get_worker_scheduler

_FAIL_OPEN_EXCEPTIONS = (OSError, RuntimeError, ValueError, TypeError)


@dataclass(frozen=True, slots=True)
class LspRequestBatchResult:
    """Deterministic keyed LSP request batch result."""

    results: dict[str, object]
    timed_out: tuple[str, ...]


def run_lsp_requests(
    callables: Mapping[str, Callable[[], object]],
    *,
    timeout_seconds: float,
    use_workers: bool = False,
) -> LspRequestBatchResult:
    """Execute keyed requests with bounded timeout and deterministic mapping.

    For session-bound LSP clients, default sequential execution is safer than
    thread-level concurrent calls over one stdio stream. Optional worker mode
    remains available for callables that are known to be thread-safe.

    Returns:
        Keyed result mapping and timed-out key tuple.
    """
    if not callables:
        return LspRequestBatchResult(results={}, timed_out=())
    if use_workers:
        return _run_lsp_requests_parallel(callables, timeout_seconds=timeout_seconds)
    return _run_lsp_requests_sequential(callables, timeout_seconds=timeout_seconds)


def _run_lsp_requests_sequential(
    callables: Mapping[str, Callable[[], object]],
    *,
    timeout_seconds: float,
) -> LspRequestBatchResult:
    deadline = monotonic() + max(0.05, float(timeout_seconds))
    results: dict[str, object] = {}
    timed_out: list[str] = []
    for key, callable_item in callables.items():
        if monotonic() >= deadline:
            timed_out.append(key)
            continue
        try:
            results[key] = callable_item()
        except TimeoutError:
            timed_out.append(key)
        except _FAIL_OPEN_EXCEPTIONS:
            continue
    return LspRequestBatchResult(results=results, timed_out=tuple(timed_out))


def _run_lsp_requests_parallel(
    callables: Mapping[str, Callable[[], object]],
    *,
    timeout_seconds: float,
) -> LspRequestBatchResult:
    scheduler = get_worker_scheduler()
    keyed_futures = {
        key: scheduler.submit_lsp(callable_item) for key, callable_item in callables.items()
    }
    done_set, pending = wait(
        keyed_futures.values(),
        timeout=max(0.05, float(timeout_seconds)),
    )
    for future in pending:
        future.cancel()
    results: dict[str, object] = {}
    timed_out: list[str] = []
    for key, future in keyed_futures.items():
        if future not in done_set:
            timed_out.append(key)
            continue
        try:
            results[key] = future.result()
        except TimeoutError:
            timed_out.append(key)
        except _FAIL_OPEN_EXCEPTIONS:
            continue
    return LspRequestBatchResult(results=results, timed_out=tuple(timed_out))


__all__ = ["LspRequestBatchResult", "run_lsp_requests"]
