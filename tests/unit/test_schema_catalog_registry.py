"""Tests for DataFusion-backed schema authority."""

from __future__ import annotations

import pyarrow as pa

from datafusion_engine.session.runtime_dataset_io import dataset_schema_from_context
from tests.test_helpers.datafusion_runtime import df_ctx


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
    ctx = df_ctx()
    expected = _to_arrow_schema(ctx.table(name).schema())
    resolved = _to_arrow_schema(dataset_schema_from_context(name))
    assert expected.equals(resolved)
