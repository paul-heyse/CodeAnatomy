"""Tests for schema metadata merge helpers."""

from __future__ import annotations

from datafusion_engine.arrow.metadata import SchemaMetadataSpec
from datafusion_engine.arrow.metadata_write import merge_schema_metadata


def test_merge_schema_metadata_returns_spec() -> None:
    """Metadata merge returns canonical schema metadata spec."""
    merged = merge_schema_metadata(None)
    assert isinstance(merged, SchemaMetadataSpec)
