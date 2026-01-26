"""Tests for DataFusion-backed schema authority."""

from __future__ import annotations

import pyarrow as pa

from datafusion_engine.runtime import DataFusionRuntimeProfile, dataset_schema_from_context
from datafusion_engine.schema_registry import nested_view_spec


def _to_arrow_schema(value: object) -> pa.Schema:
    if isinstance(value, pa.Schema):
        return value
    to_arrow = getattr(value, "to_arrow", None)
    if callable(to_arrow):
        resolved = to_arrow()
        if isinstance(resolved, pa.Schema):
            return resolved
    msg = f"Unsupported schema type: {type(value)}"
    raise TypeError(msg)


def test_catalog_schema_resolves_nested_dataset() -> None:
    """Ensure nested dataset schemas resolve through DataFusion."""
    name = "cst_nodes"
    ctx = DataFusionRuntimeProfile().session_context()
    expected = nested_view_spec(ctx, name).schema
    resolved = dataset_schema_from_context(name)
    assert expected.equals(_to_arrow_schema(resolved))
