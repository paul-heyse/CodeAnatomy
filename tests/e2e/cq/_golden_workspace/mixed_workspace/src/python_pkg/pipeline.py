"""Python side of mixed-language fixture."""

from __future__ import annotations


def mixed_symbol() -> str:
    return "python-mixed"


def resolve_name(name: str) -> str:
    return f"py:{name}"
