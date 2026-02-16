# ruff: noqa: D100, D103, INP001
from __future__ import annotations

from datafusion_engine.arrow.metadata import SchemaMetadataSpec
from datafusion_engine.arrow.metadata_write import merge_schema_metadata


def test_merge_schema_metadata_returns_spec() -> None:
    merged = merge_schema_metadata(None)
    assert isinstance(merged, SchemaMetadataSpec)
