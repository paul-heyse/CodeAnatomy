"""Tests for DataFusion schema registry helpers."""

from __future__ import annotations

import pyarrow as pa
from datafusion import SessionContext

from datafusion_engine.schema_registry import register_all_schemas, schema_for, schema_names


def _to_arrow_schema(value: object) -> pa.Schema:
    if isinstance(value, pa.Schema):
        return value
    to_pyarrow = getattr(value, "to_pyarrow", None)
    if callable(to_pyarrow):
        return to_pyarrow()
    msg = f"Unsupported schema type: {type(value)}"
    raise TypeError(msg)


def test_register_all_schemas_roundtrip() -> None:
    """Ensure schemas are registered and round-trip from the DataFusion context."""
    ctx = SessionContext()
    register_all_schemas(ctx)
    for name in schema_names():
        actual = _to_arrow_schema(ctx.table(name).schema())
        expected = schema_for(name)
        assert actual == expected
