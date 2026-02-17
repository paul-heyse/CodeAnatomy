"""Unit tests for dataset DDL type aliases."""

from __future__ import annotations

from datafusion_engine.dataset.ddl_types import (
    DDL_COMPLEX_TYPE_TOKENS,
    ddl_type_alias,
)


def test_ddl_type_alias_handles_known_and_case_insensitive_values() -> None:
    """Alias resolution handles known values and case normalization."""
    assert ddl_type_alias("int64") == "BIGINT"
    assert ddl_type_alias("UTF8") == "VARCHAR"
    assert ddl_type_alias("  bool  ") == "BOOLEAN"


def test_ddl_type_alias_returns_none_for_unknown_or_empty() -> None:
    """Unknown or empty aliases resolve to ``None``."""
    assert ddl_type_alias("") is None
    assert ddl_type_alias("unknown_type") is None


def test_ddl_complex_type_tokens_include_structural_markers() -> None:
    """Complex-type tokens include expected structural markers."""
    assert "struct<" in DDL_COMPLEX_TYPE_TOKENS
    assert "map<" in DDL_COMPLEX_TYPE_TOKENS
