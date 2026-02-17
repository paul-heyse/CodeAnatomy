"""Tests for schema metadata identity extraction."""

from __future__ import annotations

import pyarrow as pa

from datafusion_engine.arrow.metadata_schema import schema_identity


def test_schema_identity_reads_schema_metadata() -> None:
    """Schema identity helper decodes expected metadata values."""
    schema = pa.schema([], metadata={b"schema.identity_hash": b"abc"})
    assert schema_identity(schema)["schema.identity_hash"] == "abc"
