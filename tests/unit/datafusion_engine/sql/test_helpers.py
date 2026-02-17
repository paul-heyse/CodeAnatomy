"""Tests for SQL identifier helper behavior."""

from __future__ import annotations

from datafusion_engine.sql.helpers import sql_identifier


def test_sql_identifier_plain_identifier_is_unchanged() -> None:
    """Plain identifiers pass through unchanged."""
    assert sql_identifier("events") == "events"


def test_sql_identifier_dotted_identifiers_are_unchanged() -> None:
    """Dotted identifiers pass through unchanged."""
    assert sql_identifier("public.events") == "public.events"


def test_sql_identifier_quotes_non_identifier_names() -> None:
    """Invalid SQL identifiers are quoted."""
    assert sql_identifier("events-table") == '"events-table"'


def test_sql_identifier_escapes_embedded_quotes() -> None:
    """Embedded quotes are escaped in SQL identifiers."""
    assert sql_identifier('name"with"quote') == '"name""with""quote"'
