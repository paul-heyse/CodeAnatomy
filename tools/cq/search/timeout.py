"""Timeout utilities for search operations.

Provides synchronous and asynchronous timeout wrappers for ripgrep operations.
"""

from __future__ import annotations

import asyncio
from concurrent.futures import TimeoutError as FuturesTimeoutError
from typing import TYPE_CHECKING

from tools.cq.core.runtime.worker_scheduler import get_worker_scheduler

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable


def search_sync_with_timeout[T](
    fn: Callable[..., T],
    timeout: float,
    *,
    args: tuple[object, ...] | None = None,
    kwargs: dict[str, object] | None = None,
) -> T:
    """Execute a synchronous function with a timeout.

    Args:
        fn: Callable to run.
        timeout: Timeout in seconds; `0` means no timeout.
        args: Positional arguments for `fn`.
        kwargs: Keyword arguments for `fn`.

    Returns:
        T: Return value from `fn`.

    Raises:
        TimeoutError: If execution exceeds the timeout.
        ValueError: If `timeout` is negative.
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


async def search_async_with_timeout[T](
    coro: Awaitable[T],
    timeout_seconds: float,
) -> T:
    """Execute an awaitable with a timeout.

    Args:
        coro: Awaitable to run.
        timeout_seconds: Timeout in seconds; `0` means no timeout.

    Returns:
        T: Awaited result.

    Raises:
        TimeoutError: If execution exceeds the timeout.
        ValueError: If `timeout_seconds` is negative.
    """
    if timeout_seconds < 0:
        msg = "Timeout must be positive"
        raise ValueError(msg)

    try:
        return await asyncio.wait_for(
            coro, timeout=timeout_seconds if timeout_seconds > 0 else None
        )
    except TimeoutError as e:
        msg = f"Async search operation timed out after {timeout_seconds:.1f} seconds"
        raise TimeoutError(msg) from e
