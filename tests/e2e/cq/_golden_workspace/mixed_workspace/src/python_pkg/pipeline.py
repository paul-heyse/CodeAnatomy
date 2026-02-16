"""Python side of mixed-language fixture."""

from __future__ import annotations


def mixed_symbol() -> str:
    """Return the Python fixture symbol identifier.

    Returns:
        str: Fixed symbol value used by mixed-language fixture tests.
    """
    return "python-mixed"


def resolve_name(name: str) -> str:
    """Return a Python-prefixed fixture name.

    Returns:
        str: Name prefixed with ``py:`` for fixture resolution checks.
    """
    return f"py:{name}"
