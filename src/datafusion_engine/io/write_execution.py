"""Execution helpers for DataFusion write operations."""

from __future__ import annotations

from collections.abc import Callable


def execute_write(callable_write: Callable[[object], object], request: object) -> object:
    """Execute a prepared write callable with its request payload.

    Returns:
    -------
    object
        Result produced by the write callable.
    """
    return callable_write(request)


__all__ = ["execute_write"]
