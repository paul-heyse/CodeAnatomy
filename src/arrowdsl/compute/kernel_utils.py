"""Kernel lookup helpers for Arrow compute functions."""

from __future__ import annotations

from collections.abc import Sequence

import pyarrow.compute as pc


def list_kernels() -> tuple[str, ...]:
    """Return available Arrow compute kernel names.

    Returns
    -------
    tuple[str, ...]
        Sorted kernel names available in the runtime.
    """
    return tuple(sorted(pc.list_functions()))


def resolve_kernel(
    name: str,
    *,
    fallbacks: Sequence[str] = (),
    required: bool = False,
) -> str | None:
    """Resolve a compute kernel name from candidates.

    Returns
    -------
    str | None
        Resolved kernel name or ``None`` when unavailable and not required.

    Raises
    ------
    KeyError
        Raised when no candidate kernels exist and ``required=True``.
    """
    for candidate in (name, *fallbacks):
        try:
            pc.get_function(candidate)
        except KeyError:
            continue
        return candidate
    if required:
        msg = f"Missing compute kernel: {name!r}."
        raise KeyError(msg)
    return None


__all__ = ["list_kernels", "resolve_kernel"]
