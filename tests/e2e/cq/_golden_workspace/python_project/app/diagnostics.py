"""Intentional static-analysis diagnostics fixture.

This file is not imported at runtime by tests; it exists so LSP/diagnostic
planes have realistic material when enabled.
"""

from __future__ import annotations


def expects_int(value: int) -> int:
    return value + 1


def diagnostics_sample() -> int:
    maybe_number: str = "not-an-int"
    return expects_int(maybe_number)
