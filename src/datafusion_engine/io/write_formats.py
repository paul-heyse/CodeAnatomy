"""Canonical write-format registry helpers."""

from __future__ import annotations

from collections.abc import Mapping

KNOWN_WRITE_FORMATS: frozenset[str] = frozenset({"parquet", "csv", "json", "delta", "arrow"})


def is_known_write_format(name: str) -> bool:
    """Return True when a write format is supported."""
    return name.lower() in KNOWN_WRITE_FORMATS


def sql_string_literal(value: str) -> str:
    """Return SQL-safe string literal."""
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def copy_options_clause(options: Mapping[str, str]) -> str | None:
    """Render COPY OPTIONS(...) clause from key/value options.

    Returns:
        str | None: SQL options clause or ``None`` when no options are provided.
    """
    if not options:
        return None
    items = ", ".join(
        f"{sql_string_literal(key)} {sql_string_literal(value)}"
        for key, value in sorted(options.items(), key=lambda item: item[0])
    )
    return f"OPTIONS ({items})"


__all__ = [
    "KNOWN_WRITE_FORMATS",
    "copy_options_clause",
    "is_known_write_format",
    "sql_string_literal",
]
