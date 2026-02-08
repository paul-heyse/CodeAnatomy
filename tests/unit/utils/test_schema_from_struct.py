"""Tests for schema_from_struct utility."""

from __future__ import annotations

import pyarrow as pa
import pytest

from utils.schema_from_struct import schema_from_struct


def test_schema_from_struct_preserves_field_shape() -> None:
    """Struct fields should be copied into top-level schema fields."""
    original = pa.struct(
        [
            pa.field("name", pa.string(), nullable=False),
            pa.field("count", pa.int64(), metadata={b"unit": b"rows"}),
        ]
    )
    schema = schema_from_struct(original)
    assert schema.names == ["name", "count"]
    assert schema.field("name").nullable is False
    assert schema.field("count").metadata == {b"unit": b"rows"}


def test_schema_from_struct_accepts_struct_list_field() -> None:
    """List<struct<...>> fields should be unwrapped before conversion."""
    field = pa.field(
        "nodes",
        pa.list_(
            pa.struct(
                [
                    pa.field("node_id", pa.string()),
                    pa.field("kind", pa.string()),
                ]
            )
        ),
    )
    schema = schema_from_struct(field)
    assert schema.names == ["node_id", "kind"]


def test_schema_from_struct_rejects_non_struct_values() -> None:
    """Non-struct types should fail with a TypeError."""
    with pytest.raises(TypeError, match="Expected struct-compatible value"):
        _ = schema_from_struct(pa.int64())
