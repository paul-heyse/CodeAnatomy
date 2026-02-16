"""Python side of mixed-language fixture."""

from __future__ import annotations


def mixed_symbol() -> str:
    """Mixed symbol."""
    return "python-mixed"


def resolve_name(name: str) -> str:
    """Resolve name."""
    return f"py:{name}"
