"""PyArrow function catalog helpers for diagnostics."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import cast

from arrowdsl.core.interop import pc


def pyarrow_compute_functions() -> tuple[str, ...]:
    """Return sorted PyArrow compute function names.

    Returns
    -------
    tuple[str, ...]
        Sorted function names from ``pyarrow.compute``.

    Raises
    ------
    TypeError
        Raised when ``pyarrow.compute.list_functions`` is unavailable.
    """
    list_functions = getattr(pc, "list_functions", None)
    if not callable(list_functions):
        msg = "pyarrow.compute.list_functions is unavailable."
        raise TypeError(msg)
    callable_list = cast("Callable[[], Sequence[str]]", list_functions)
    return tuple(sorted(callable_list()))


def pyarrow_registry_snapshot() -> dict[str, object]:
    """Return a snapshot of available PyArrow compute kernels.

    Returns
    -------
    dict[str, object]
        Snapshot payload for kernel availability.
    """
    functions = list(pyarrow_compute_functions())
    return {
        "version": 1,
        "available_kernels": functions,
        "registered_udfs": [],
        "registered_kernels": [],
        "pyarrow_compute": functions,
    }


__all__ = [
    "pyarrow_compute_functions",
    "pyarrow_registry_snapshot",
]
