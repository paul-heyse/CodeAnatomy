"""Shared decorators/utilities for CQ CLI commands."""

from __future__ import annotations

from collections.abc import Callable
from functools import wraps
from typing import Any

from tools.cq.cli_app.context import CliContext


def require_ctx(func: Callable[..., Any]) -> Callable[..., Any]:
    """Ensure the command is invoked with an injected ``ctx`` keyword argument.

    Returns:
        Callable[..., Any]: Wrapped callable.
    """

    @wraps(func)
    def _wrapped(*args: object, **kwargs: object) -> Any:
        if kwargs.get("ctx") is None:
            msg = "Context not injected"
            raise RuntimeError(msg)
        return func(*args, **kwargs)

    return _wrapped


def require_context(ctx: CliContext | None) -> CliContext:
    """Return a non-optional context or raise when injection is missing.

    Raises:
        RuntimeError: If context injection is missing.
    """
    if ctx is None:
        msg = "Context not injected"
        raise RuntimeError(msg)
    return ctx


__all__ = ["require_context", "require_ctx"]
