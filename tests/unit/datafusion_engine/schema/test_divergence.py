"""Tests for schema divergence helpers."""

from __future__ import annotations

import pyarrow as pa

from datafusion_engine.schema.divergence import compute_schema_divergence


def test_compute_schema_divergence_detects_added_removed_and_mismatched_types() -> None:
    """Divergence helper should report structural schema differences."""
    spec_schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("removed_col", pa.int32()),
        ]
    )
    plan_schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("name", pa.large_string()),
            pa.field("added_col", pa.bool_()),
        ]
    )

    divergence = compute_schema_divergence(spec_schema, plan_schema)

    assert divergence.has_divergence is True
    assert divergence.added_columns == ("added_col",)
    assert divergence.removed_columns == ("removed_col",)
    assert divergence.type_mismatches == (("name", "string", "large_string"),)
