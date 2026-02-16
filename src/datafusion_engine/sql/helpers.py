"""Shared SQL text helpers."""

from __future__ import annotations


def sql_identifier(name: str) -> str:
    """Quote a SQL identifier when needed.

    Identifiers consisting of dot-separated Python identifiers are returned
    unchanged. Everything else is double-quoted with embedded quote escaping.

    Returns:
    -------
    str
        SQL-safe identifier token.
    """
    parts = [part for part in name.split(".") if part]
    if parts and all(part.isidentifier() for part in parts):
        return name
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


__all__ = ["sql_identifier"]
