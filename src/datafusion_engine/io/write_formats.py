"""Canonical write-format registry helpers."""

from __future__ import annotations

KNOWN_WRITE_FORMATS: frozenset[str] = frozenset({"parquet", "csv", "json", "delta", "arrow"})


def is_known_write_format(name: str) -> bool:
    """Return True when a write format is supported."""
    return name.lower() in KNOWN_WRITE_FORMATS


__all__ = ["KNOWN_WRITE_FORMATS", "is_known_write_format"]
