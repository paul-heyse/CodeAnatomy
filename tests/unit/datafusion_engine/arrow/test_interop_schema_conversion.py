"""Tests for Arrow/DataFusion schema conversion helpers."""

from __future__ import annotations

import pyarrow as pa
import pytest

from datafusion_engine.arrow.interop import arrow_schema_from_df, arrow_schema_from_dfschema


class _SchemaWrapper:
    def __init__(self, schema: pa.Schema) -> None:
        self._schema = schema

    def to_arrow(self) -> pa.Schema:
        return self._schema


class _DataFrameWithSchemaMethod:
    def __init__(self, schema_value: object) -> None:
        self._schema_value = schema_value

    def schema(self) -> object:
        return self._schema_value


def test_arrow_schema_from_df_supports_arrow_schema() -> None:
    """DataFrame schema() returning Arrow schema is accepted directly."""
    schema = pa.schema([pa.field("id", pa.int64())])
    df = _DataFrameWithSchemaMethod(schema)
    assert arrow_schema_from_df(df) == schema


def test_arrow_schema_from_df_supports_schema_with_to_arrow() -> None:
    """DataFrame schema() returning wrapper with to_arrow is supported."""
    schema = pa.schema([pa.field("name", pa.string())])
    df = _DataFrameWithSchemaMethod(_SchemaWrapper(schema))
    assert arrow_schema_from_df(df) == schema


def test_arrow_schema_from_dfschema_returns_none_for_uncoercible_values() -> None:
    """Uncoercible schema values return None from low-level converter."""
    assert arrow_schema_from_dfschema(object()) is None


def test_arrow_schema_from_df_raises_on_uncoercible_schema() -> None:
    """High-level conversion raises on uncoercible DataFrame schema values."""
    df = _DataFrameWithSchemaMethod(object())
    with pytest.raises(TypeError, match="Failed to resolve DataFusion schema"):
        _ = arrow_schema_from_df(df)
