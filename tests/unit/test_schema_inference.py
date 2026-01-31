"""Tests for schema inference from DataFusion plans."""

from __future__ import annotations

import pyarrow as pa
import pytest
from datafusion import SessionContext

from datafusion_engine.schema.inference import (
    SchemaInferenceResult,
    infer_column_lineage,
    infer_schema_from_dataframe,
    infer_schema_from_logical_plan,
    infer_schema_with_lineage,
    schema_fingerprint_from_inference,
    validate_inferred_schema,
)
from tests.test_helpers.optional_deps import require_datafusion

require_datafusion()


def _simple_ctx() -> SessionContext:
    """Return a simple DataFusion SessionContext for tests.

    Returns
    -------
    SessionContext
        A minimal SessionContext for testing.
    """
    return SessionContext()


def _register_base_tables(ctx: SessionContext) -> None:
    events = pa.table({"id": [1, 2], "label": ["a", "b"], "value": [10.0, 20.0]})
    users = pa.table({"id": [1, 3], "name": ["alpha", "beta"]})
    ctx.register_record_batches("events", [events.to_batches()])
    ctx.register_record_batches("users", [users.to_batches()])


def test_infers_simple_select_schema() -> None:
    """Infer schema from a simple SELECT statement."""
    ctx = _simple_ctx()
    _register_base_tables(ctx)
    df = ctx.sql("SELECT id, label FROM events")
    schema = infer_schema_from_dataframe(df)
    assert schema.names == ["id", "label"]
    assert pa.types.is_int64(schema.field("id").type)
    assert pa.types.is_string(schema.field("label").type) or pa.types.is_large_string(
        schema.field("label").type
    )


def test_infers_aggregation_schema() -> None:
    """Infer schema with aggregation expressions."""
    ctx = _simple_ctx()
    _register_base_tables(ctx)
    df = ctx.sql("SELECT COUNT(*) as cnt, SUM(value) as total FROM events")
    schema = infer_schema_from_dataframe(df)
    assert "cnt" in schema.names
    assert "total" in schema.names


def test_infers_join_schema() -> None:
    """Infer schema from a join query."""
    ctx = _simple_ctx()
    _register_base_tables(ctx)
    df = ctx.sql("SELECT e.id, e.label, u.name FROM events e JOIN users u ON e.id = u.id")
    schema = infer_schema_from_dataframe(df)
    assert set(schema.names) == {"id", "label", "name"}


def test_infer_schema_from_logical_plan_raises_without_schema_method() -> None:
    """Test that infer_schema_from_logical_plan raises for plans without schema."""
    ctx = _simple_ctx()
    _register_base_tables(ctx)
    df = ctx.sql("SELECT id, label FROM events WHERE id > 0")
    plan = df.optimized_logical_plan()
    # DataFusion Python's LogicalPlan doesn't expose schema() directly
    # This test documents that behavior
    with pytest.raises(TypeError, match="LogicalPlan does not expose a schema"):
        infer_schema_from_logical_plan(plan)


def test_traces_direct_column_references() -> None:
    """Trace columns that directly reference source columns."""
    ctx = _simple_ctx()
    _register_base_tables(ctx)
    df = ctx.sql("SELECT id, label FROM events")
    lineage = infer_column_lineage(df)
    assert "id" in lineage
    assert "label" in lineage
    assert "events" in lineage["id"].source_tables


def test_traces_aliased_columns() -> None:
    """Trace columns with aliases."""
    ctx = _simple_ctx()
    _register_base_tables(ctx)
    df = ctx.sql("SELECT id AS event_id FROM events")
    lineage = infer_column_lineage(df)
    assert "event_id" in lineage


def test_traces_join_columns() -> None:
    """Trace columns from join queries."""
    ctx = _simple_ctx()
    _register_base_tables(ctx)
    df = ctx.sql("SELECT e.id, u.name FROM events e JOIN users u ON e.id = u.id")
    lineage = infer_column_lineage(df)
    assert "id" in lineage
    assert "name" in lineage


def test_returns_complete_result() -> None:
    """Return complete SchemaInferenceResult."""
    ctx = _simple_ctx()
    _register_base_tables(ctx)
    df = ctx.sql("SELECT id, label FROM events")
    result = infer_schema_with_lineage(df)
    assert isinstance(result, SchemaInferenceResult)
    assert result.output_schema.names == ["id", "label"]
    assert "events" in result.source_tables
    assert "id" in result.column_lineage
    assert result.lineage_report is not None


def test_captures_multiple_source_tables() -> None:
    """Capture all source tables in joins."""
    ctx = _simple_ctx()
    _register_base_tables(ctx)
    df = ctx.sql("SELECT e.id, u.name FROM events e JOIN users u ON e.id = u.id")
    result = infer_schema_with_lineage(df)
    assert "events" in result.source_tables
    assert "users" in result.source_tables


def test_generates_stable_fingerprint() -> None:
    """Generate consistent fingerprints for same schema."""
    ctx = _simple_ctx()
    _register_base_tables(ctx)
    df = ctx.sql("SELECT id FROM events")
    result1 = infer_schema_with_lineage(df)
    result2 = infer_schema_with_lineage(df)
    fp1 = schema_fingerprint_from_inference(result1)
    fp2 = schema_fingerprint_from_inference(result2)
    assert fp1 == fp2
    assert ":" in fp1  # Contains schema hash and source tables


def test_passes_matching_schema() -> None:
    """Pass validation when schemas match."""
    ctx = _simple_ctx()
    _register_base_tables(ctx)
    df = ctx.sql("SELECT id, label FROM events")
    result = infer_schema_with_lineage(df)
    violations = validate_inferred_schema(result, result.output_schema)
    assert violations == []


def test_detects_missing_column() -> None:
    """Detect missing columns in inferred schema."""
    ctx = _simple_ctx()
    _register_base_tables(ctx)
    df = ctx.sql("SELECT id FROM events")
    result = infer_schema_with_lineage(df)
    expected = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("missing", pa.string()),
        ]
    )
    violations = validate_inferred_schema(result, expected)
    assert any("Missing column: missing" in v for v in violations)


def test_detects_extra_column_in_strict_mode() -> None:
    """Detect extra columns when strict=True."""
    ctx = _simple_ctx()
    _register_base_tables(ctx)
    df = ctx.sql("SELECT id, label FROM events")
    result = infer_schema_with_lineage(df)
    expected = pa.schema([pa.field("id", pa.int64())])
    violations = validate_inferred_schema(result, expected, strict=True)
    assert any("Extra column: label" in v for v in violations)


def test_allows_extra_column_in_non_strict_mode() -> None:
    """Allow extra columns when strict=False."""
    ctx = _simple_ctx()
    _register_base_tables(ctx)
    df = ctx.sql("SELECT id, label FROM events")
    result = infer_schema_with_lineage(df)
    expected = pa.schema([pa.field("id", pa.int64())])
    violations = validate_inferred_schema(result, expected, strict=False)
    assert not any("Extra column" in v for v in violations)
