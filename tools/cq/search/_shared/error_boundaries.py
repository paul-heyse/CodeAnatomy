"""Canonical fail-open exception boundaries for enrichment paths."""

from __future__ import annotations

ENRICHMENT_ERRORS: tuple[type[Exception], ...] = (
    RuntimeError,
    TypeError,
    ValueError,
    AttributeError,
    UnicodeError,
    IndexError,
    KeyError,
    OSError,
)


__all__ = ["ENRICHMENT_ERRORS"]
