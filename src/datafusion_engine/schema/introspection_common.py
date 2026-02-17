"""Shared helpers for DataFusion schema/session introspection."""

from __future__ import annotations

from datafusion import SQLOptions

from datafusion_engine.sql.options import sql_options_for_profile


def read_only_sql_options() -> SQLOptions:
    """Return read-only SQL options used by introspection call paths."""
    return sql_options_for_profile(None)


__all__ = ["read_only_sql_options"]
