"""Tests for DataFusion schema registry helpers."""

from __future__ import annotations

import pyarrow as pa

from arrow_utils.core.schema_constants import KEY_FIELDS_META, REQUIRED_NON_NULL_META
from datafusion_engine.arrow_schema.build import empty_table
from datafusion_engine.arrow_schema.metadata import metadata_list_bytes
from datafusion_engine.io_adapter import DataFusionIOAdapter
from datafusion_engine.runtime import DataFusionRuntimeProfile
from datafusion_engine.schema_registry import (
    AST_VIEW_NAMES,
    DATAFUSION_HAMILTON_EVENTS_SCHEMA,
    HAMILTON_PLAN_DRIFT_SCHEMA,
    HAMILTON_TASK_EXPANSION_SCHEMA,
    HAMILTON_TASK_GROUPING_SCHEMA,
    HAMILTON_TASK_SUBMISSION_SCHEMA,
    LIBCST_FILES_SCHEMA,
    SYMTABLE_FILES_SCHEMA,
    nested_view_spec,
    validate_ast_views,
    validate_required_bytecode_functions,
    validate_required_cst_functions,
    validate_required_engine_functions,
    validate_required_symtable_functions,
)
from datafusion_engine.udf_runtime import register_rust_udfs


def _to_arrow_schema(value: object) -> pa.Schema:
    if isinstance(value, pa.Schema):
        return value
    to_pyarrow = getattr(value, "to_pyarrow", None)
    if callable(to_pyarrow):
        return to_pyarrow()
    msg = f"Unsupported schema type: {type(value)}"
    raise TypeError(msg)


def test_nested_view_spec_roundtrip() -> None:
    """Ensure nested view specs round-trip from the DataFusion context."""
    profile = DataFusionRuntimeProfile()
    ctx = profile.session_context()
    session_runtime = profile.session_runtime()
    adapter = DataFusionIOAdapter(ctx=ctx, profile=profile)
    adapter.register_arrow_table(
        "libcst_files_v1",
        empty_table(LIBCST_FILES_SCHEMA),
        overwrite=True,
    )
    view_spec = nested_view_spec(ctx, "cst_parse_manifest")
    view_spec.register(session_runtime, validate=False)
    actual = _to_arrow_schema(ctx.table(view_spec.name).schema())
    assert "file_id" in actual.names
    assert "path" in actual.names


def test_symtable_schema_metadata() -> None:
    """Expose symtable schema metadata and span ABI tags."""
    schema = SYMTABLE_FILES_SCHEMA
    meta = schema.metadata or {}
    expected = metadata_list_bytes(("file_id", "path"))
    assert meta.get(KEY_FIELDS_META) == expected
    assert meta.get(REQUIRED_NON_NULL_META) == expected
    blocks_field = schema.field("blocks")
    assert pa.types.is_list(blocks_field.type)
    block_struct = blocks_field.type.value_type
    span_field = block_struct.field("span_hint")
    span_meta = span_field.metadata or {}
    assert span_meta.get(b"line_base") == b"0"
    assert span_meta.get(b"col_unit") == b"utf32"
    assert span_meta.get(b"end_exclusive") == b"true"


def test_required_functions_present() -> None:
    """Validate required CST function inventory and signatures."""
    profile = DataFusionRuntimeProfile()
    ctx = profile.session_context()
    register_rust_udfs(ctx)
    validate_required_cst_functions(ctx)


def test_required_symtable_functions_present() -> None:
    """Validate required symtable function inventory and signatures."""
    profile = DataFusionRuntimeProfile()
    ctx = profile.session_context()
    register_rust_udfs(ctx)
    validate_required_symtable_functions(ctx)


def test_required_bytecode_functions_present() -> None:
    """Validate required bytecode function inventory and signatures."""
    profile = DataFusionRuntimeProfile()
    ctx = profile.session_context()
    register_rust_udfs(ctx)
    validate_required_bytecode_functions(ctx)


def test_required_engine_functions_present() -> None:
    """Validate required engine function inventory."""
    profile = DataFusionRuntimeProfile()
    ctx = profile.session_context()
    register_rust_udfs(ctx)
    validate_required_engine_functions(ctx)


def test_validate_ast_views_smoke() -> None:
    """Ensure AST view validation runs against registered views."""
    profile = DataFusionRuntimeProfile()
    ctx = profile.session_context()
    register_rust_udfs(ctx)
    validate_ast_views(ctx, view_names=AST_VIEW_NAMES)


def test_hamilton_diagnostics_schemas_cover_plan_events() -> None:
    """Hamilton plan diagnostics schemas expose the expected contract fields."""
    submission_fields = set(HAMILTON_TASK_SUBMISSION_SCHEMA.names)
    assert {
        "run_id",
        "task_id",
        "plan_signature",
        "reduced_plan_signature",
        "task_facts",
    }.issubset(submission_fields)
    grouping_fields = set(HAMILTON_TASK_GROUPING_SCHEMA.names)
    assert {"run_id", "task_ids", "task_count"}.issubset(grouping_fields)
    expansion_fields = set(HAMILTON_TASK_EXPANSION_SCHEMA.names)
    assert {"run_id", "task_id", "parameter_keys"}.issubset(expansion_fields)
    drift_fields = set(HAMILTON_PLAN_DRIFT_SCHEMA.names)
    assert {
        "plan_task_count",
        "admitted_task_count",
        "missing_generations",
        "submission_event_count",
    }.issubset(drift_fields)
    events_fields = set(DATAFUSION_HAMILTON_EVENTS_SCHEMA.names)
    assert {
        "event_time_unix_ms",
        "run_id",
        "event_name",
        "plan_signature",
        "reduced_plan_signature",
        "event_payload_json",
        "event_payload_hash",
    }.issubset(events_fields)
