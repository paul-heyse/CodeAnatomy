"""Public dispatch helpers for parsed Cyclopts commands."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable
from inspect import BoundArguments
from typing import Any


def dispatch_bound_command(command: Callable[..., Any], bound: BoundArguments) -> Any:
    """Execute a parsed command using only public Python/Cyclopts primitives.

    Returns:
        Any: Command return value.

    Raises:
        RuntimeError: If an awaitable is dispatched from a sync path with a running loop.
    """
    result = command(*bound.args, **bound.kwargs)
    if not inspect.isawaitable(result):
        return result

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        awaitable: Awaitable[Any] = result
        return asyncio.run(_await_result(awaitable))

    # Prevent un-awaited coroutine warnings when sync dispatch is used in a running loop.
    if inspect.iscoroutine(result):
        result.close()
    msg = "Awaitable command dispatched from sync path while an event loop is running."
    raise RuntimeError(msg)


async def _await_result(awaitable: Awaitable[Any]) -> Any:
    return await awaitable


__all__ = ["dispatch_bound_command"]
