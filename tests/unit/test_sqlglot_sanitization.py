"""Tests for SQLGlot strict parsing helpers."""

from __future__ import annotations

import pytest
from sqlglot.errors import ParseError

from sqlglot_tools.optimizer import (
    parse_sql_strict,
    register_datafusion_dialect,
    sanitize_templated_sql,
)


def test_sanitize_templated_sql_replaces_placeholders() -> None:
    """Replace templated SQL segments with safe identifiers."""
    sql = "SELECT {{col}} FROM {{table}} WHERE id = :id"
    sanitized = sanitize_templated_sql(sql)
    assert "{{" not in sanitized
    assert "}}" not in sanitized
    assert ":id" not in sanitized


def test_parse_sql_strict_handles_templated_sql() -> None:
    """Parse templated SQL after sanitization."""
    register_datafusion_dialect("datafusion_ext")
    expr = parse_sql_strict("SELECT {{col}} FROM {{table}}", dialect="datafusion_ext")
    assert expr is not None


def test_parse_sql_strict_raises_on_invalid_sql() -> None:
    """Raise immediately on invalid SQL."""
    register_datafusion_dialect("datafusion_ext")
    with pytest.raises(ParseError):
        parse_sql_strict("SELECT FROM", dialect="datafusion_ext")
