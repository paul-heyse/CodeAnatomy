"""Tests for Arrow interop protocol surface."""

from __future__ import annotations

import pyarrow as pa

from datafusion_engine.arrow import interop


def test_interop_protocol_surface_exports_expected_protocols() -> None:
    """Interop module exports required protocol aliases."""
    assert hasattr(interop, "DataTypeLike")
    assert hasattr(interop, "FieldLike")
    assert hasattr(interop, "SchemaLike")


def test_coerce_arrow_schema_round_trip_for_arrow_schema() -> None:
    """Schema coercion is a no-op for concrete Arrow schemas."""
    schema = pa.schema([pa.field("id", pa.int64())])
    assert interop.coerce_arrow_schema(schema) == schema
