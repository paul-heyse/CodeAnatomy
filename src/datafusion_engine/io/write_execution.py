"""Execution helpers for DataFusion write operations."""

from __future__ import annotations

from collections.abc import Callable

_RETRYABLE_DELTA_STREAM_ERROR_MARKERS: tuple[str, ...] = (
    "c data interface error",
    "expected 3 buffers for imported type string",
)


def execute_write(callable_write: Callable[[object], object], request: object) -> object:
    """Execute a prepared write callable with its request payload.

    Returns:
        object: Write result returned by ``callable_write``.
    """
    return callable_write(request)


def is_retryable_delta_stream_error(exc: Exception) -> bool:
    """Return whether the streaming Delta error is safe to retry with fallback paths."""
    message = str(exc).lower()
    return any(marker in message for marker in _RETRYABLE_DELTA_STREAM_ERROR_MARKERS)


def is_delta_observability_operation(operation: str | None) -> bool:
    """Return whether a write operation targets internal Delta observability tables."""
    if operation is None:
        return False
    return operation.startswith(
        (
            "delta_mutation_",
            "delta_snapshot_",
            "delta_scan_plan",
            "delta_maintenance_",
            "delta_observability_",
        )
    )


__all__ = [
    "execute_write",
    "is_delta_observability_operation",
    "is_retryable_delta_stream_error",
]
