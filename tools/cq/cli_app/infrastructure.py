"""Consolidated CLI infrastructure: config, groups, decorators, dispatch."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable
from inspect import BoundArguments
from pathlib import Path
from typing import Any

from cyclopts import Group

from tools.cq.cli_app.context import CliContext

# ── Configuration Providers ──


def build_config_chain(
    config_file: str | None = None,
    *,
    use_config: bool = True,
) -> list[Any]:
    """Build Cyclopts config providers with `CLI > env > config > defaults` precedence.

    Returns:
        list[Any]: Ordered config providers for Cyclopts.
    """
    from cyclopts.config import Env, Toml

    providers: list[Any] = [Env(prefix="CQ_", command=False)]
    if not use_config:
        return providers

    if config_file:
        providers.append(Toml(Path(config_file), must_exist=True))
    else:
        providers.append(Toml("pyproject.toml", root_keys=("tool", "cq"), must_exist=False))

    return providers


# ── Command Groups ──

global_group = Group(
    "Global Options",
    help="Options applied to every CQ command.",
    sort_key=0,
    help_formatter="default",
)
analysis_group = Group(
    "Analysis",
    help="",
    sort_key=1,
    help_formatter="default",
)
admin_group = Group(
    "Administration",
    help="",
    sort_key=2,
    help_formatter="default",
)
protocol_group = Group(
    "Protocols",
    help="",
    sort_key=3,
    help_formatter="default",
)
setup_group = Group(
    "Setup",
    help="Shell and developer setup commands.",
    sort_key=4,
    help_formatter="plain",
)


# ── Context Decorators ──


def require_context(ctx: CliContext | None) -> CliContext:
    """Return a non-optional context or raise when injection is missing.

    Raises:
        RuntimeError: If context injection is missing.
    """
    if ctx is None:
        msg = "Context not injected"
        raise RuntimeError(msg)
    return ctx


# ── Command Dispatch ──


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


__all__ = [
    "admin_group",
    "analysis_group",
    "build_config_chain",
    "dispatch_bound_command",
    "global_group",
    "protocol_group",
    "require_context",
    "setup_group",
]
