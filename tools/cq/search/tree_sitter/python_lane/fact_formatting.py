"""Formatting helpers for Python lane facts."""

from __future__ import annotations

from collections.abc import Mapping

__all__ = ["format_python_facts"]


def format_python_facts(facts: Mapping[str, object]) -> dict[str, object]:
    """Normalize fact payload into render-friendly mapping.

    Returns:
        dict[str, object]: Normalized fact mapping.
    """
    formatted: dict[str, object] = {}
    for key, value in facts.items():
        if isinstance(value, tuple):
            formatted[key] = list(value)
        else:
            formatted[key] = value
    return formatted
