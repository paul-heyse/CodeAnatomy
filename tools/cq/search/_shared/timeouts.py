"""Shared timeout wrappers for search work."""

from __future__ import annotations

import asyncio
from concurrent.futures import TimeoutError as FuturesTimeoutError
from typing import TYPE_CHECKING

from tools.cq.core.runtime.worker_scheduler import get_worker_scheduler

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

__all__ = ["search_async_with_timeout", "search_sync_with_timeout"]


def search_sync_with_timeout[T](
    fn: Callable[..., T],
    timeout: float,
    *,
    args: tuple[object, ...] | None = None,
    kwargs: dict[str, object] | None = None,
) -> T:
    """Execute a synchronous function with a timeout.

    Returns:
        T: Function result.

    Raises:
        ValueError: If timeout is negative.
        TimeoutError: If execution exceeds timeout.
    """
    if timeout < 0:
        msg = "Timeout must be positive"
        raise ValueError(msg)

    args = args or ()
    kwargs = kwargs or {}

    scheduler = get_worker_scheduler()
    future = scheduler.submit_io(fn, *args, **kwargs)
    try:
        return future.result(timeout=timeout if timeout > 0 else None)
    except FuturesTimeoutError as e:
        msg = f"Search operation timed out after {timeout:.1f} seconds"
        raise TimeoutError(msg) from e


async def search_async_with_timeout[T](coro: Awaitable[T], timeout_seconds: float) -> T:
    """Execute an awaitable with a timeout.

    Returns:
        T: Awaitable result.

    Raises:
        ValueError: If timeout is negative.
        TimeoutError: If execution exceeds timeout.
    """
    if timeout_seconds < 0:
        msg = "Timeout must be positive"
        raise ValueError(msg)

    try:
        return await asyncio.wait_for(
            coro,
            timeout=timeout_seconds if timeout_seconds > 0 else None,
        )
    except TimeoutError as e:
        msg = f"Async search operation timed out after {timeout_seconds:.1f} seconds"
        raise TimeoutError(msg) from e
