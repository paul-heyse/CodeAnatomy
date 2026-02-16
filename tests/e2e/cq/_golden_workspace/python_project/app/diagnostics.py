"""Intentional static-analysis diagnostics fixture.

This file is not imported at runtime by tests; it exists so semantic/diagnostic
planes have realistic material when enabled.
"""

from __future__ import annotations


def expects_int(value: int) -> int:
    """Expects int."""
    return value + 1


def diagnostics_sample() -> int:
    """Diagnostics sample."""
    maybe_number: str = "not-an-int"
    return expects_int(maybe_number)
