"""Unit tests for schema introspection helpers."""

from __future__ import annotations

from datafusion import SQLOptions

from datafusion_engine.schema.introspection_common import read_only_sql_options


def test_read_only_sql_options_returns_sql_options() -> None:
    """The read-only helper returns a ``datafusion.SQLOptions`` instance."""
    options = read_only_sql_options()
    assert isinstance(options, SQLOptions)
