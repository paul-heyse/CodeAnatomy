"""Intentional static-analysis diagnostics fixture.

This file is not imported at runtime by tests; it exists so semantic/diagnostic
planes have realistic material when enabled.
"""

from __future__ import annotations


def expects_int(value: int) -> int:
    """Return ``value + 1`` for integer input.

    Returns:
        int: Incremented integer value.
    """
    return value + 1


def diagnostics_sample() -> int:
    """Return a deliberately invalid call result for diagnostics fixtures.

    Returns:
        int: Function result used to surface static-analysis diagnostics.
    """
    maybe_number: str = "not-an-int"
    return expects_int(maybe_number)
