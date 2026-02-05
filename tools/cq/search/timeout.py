"""Timeout utilities for search operations.

Provides synchronous and asynchronous timeout wrappers for rpygrep operations.
"""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeoutError
from typing import TYPE_CHECKING

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

    Parameters
    ----------
    fn : Callable[..., T]
        Function to execute
    timeout : float
        Timeout in seconds
    args : tuple[object, ...] | None, optional
        Positional arguments to pass to fn
    kwargs : dict[str, object] | None, optional
        Keyword arguments to pass to fn

    Returns
    -------
    T
        Result from fn

    Raises
    ------
    ValueError
        If timeout is negative
    TimeoutError
        If the function execution exceeds the timeout

    """
    if timeout < 0:
        msg = "Timeout must be positive"
        raise ValueError(msg)

    args = args or ()
    kwargs = kwargs or {}

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(fn, *args, **kwargs)
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

    Parameters
    ----------
    coro : Awaitable[T]
        Coroutine or awaitable to execute
    timeout_seconds : float
        Timeout in seconds

    Returns
    -------
    T
        Result from the awaitable

    Raises
    ------
    ValueError
        If timeout is negative
    TimeoutError
        If the execution exceeds the timeout

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
