"""Tests for DataFusion schema registry helpers."""

from __future__ import annotations

import pyarrow as pa
import pytest

from arrow_utils.core.schema_constants import KEY_FIELDS_META, REQUIRED_NON_NULL_META
from datafusion_engine.arrow.build import empty_table
from datafusion_engine.arrow.metadata_codec import encode_metadata_list
from datafusion_engine.arrow.semantic import SEMANTIC_TYPE_META
from datafusion_engine.io.adapter import DataFusionIOAdapter
from datafusion_engine.schema import (
    AST_VIEW_NAMES,
    DATAFUSION_PIPELINE_EVENTS_V2_SCHEMA,
    LIBCST_FILES_SCHEMA,
    PIPELINE_PLAN_DRIFT_SCHEMA,
    PIPELINE_TASK_EXPANSION_SCHEMA,
    PIPELINE_TASK_GROUPING_SCHEMA,
    PIPELINE_TASK_SUBMISSION_SCHEMA,
    SYMTABLE_FILES_SCHEMA,
    _semantic_validation_tables,
    nested_view_spec,
    validate_ast_views,
    validate_required_bytecode_functions,
    validate_required_cst_functions,
    validate_required_engine_functions,
    validate_required_symtable_functions,
    validate_semantic_types,
)
from datafusion_engine.udf.platform import ensure_rust_udfs
from tests.test_helpers.datafusion_runtime import df_profile
from tests.test_helpers.optional_deps import require_datafusion_udfs

require_datafusion_udfs()


def _to_arrow_schema(value: object) -> pa.Schema:
    if isinstance(value, pa.Schema):
        return value
    to_pyarrow = getattr(value, "to_pyarrow", None)
    if callable(to_pyarrow):
        return to_pyarrow()
    msg = f"Unsupported schema type: {type(value)}"
    raise TypeError(msg)


def test_nested_view_spec_roundtrip() -> None:
    """Ensure nested view specs round-trip from the DataFusion context.

    Raises:
        AssertionError: If the operation cannot be completed.
    """
    profile = df_profile()
    ctx = profile.session_context()
    adapter = DataFusionIOAdapter(ctx=ctx, profile=profile)
    adapter.register_arrow_table(
        "libcst_files_v1",
        empty_table(LIBCST_FILES_SCHEMA),
        overwrite=True,
    )
    view_spec = nested_view_spec(ctx, "cst_parse_manifest")
    if view_spec.builder is None:
        msg = "Nested view spec missing builder."
        raise AssertionError(msg)
    actual = _to_arrow_schema(view_spec.builder(ctx).schema())
    assert "file_id" in actual.names
    assert "path" in actual.names


def test_symtable_schema_metadata() -> None:
    """Expose symtable schema metadata and span ABI tags."""
    schema = SYMTABLE_FILES_SCHEMA
    meta = schema.metadata or {}
    expected = encode_metadata_list(("file_id", "path"))
    assert meta.get(KEY_FIELDS_META) == expected
    assert meta.get(REQUIRED_NON_NULL_META) == expected
    blocks_field = schema.field("blocks")
    assert pa.types.is_list(blocks_field.type)
    block_struct = blocks_field.type.value_type
    span_field = block_struct.field("span_hint")
    span_meta = span_field.metadata or {}
    assert span_meta.get(b"line_base") == b"0"
    assert span_meta.get(b"col_unit") == b"byte"
    assert span_meta.get(b"end_exclusive") == b"true"


def test_required_functions_present(require_native_runtime: None) -> None:
    """Validate required CST function inventory and signatures."""
    _ = require_native_runtime
    profile = df_profile()
    ctx = profile.session_context()
    ensure_rust_udfs(ctx)
    validate_required_cst_functions(ctx)


def test_required_symtable_functions_present(require_native_runtime: None) -> None:
    """Validate required symtable function inventory and signatures."""
    _ = require_native_runtime
    profile = df_profile()
    ctx = profile.session_context()
    ensure_rust_udfs(ctx)
    validate_required_symtable_functions(ctx)


def test_required_bytecode_functions_present(require_native_runtime: None) -> None:
    """Validate required bytecode function inventory and signatures."""
    _ = require_native_runtime
    profile = df_profile()
    ctx = profile.session_context()
    ensure_rust_udfs(ctx)
    validate_required_bytecode_functions(ctx)


def test_required_engine_functions_present(require_native_runtime: None) -> None:
    """Validate required engine function inventory."""
    _ = require_native_runtime
    profile = df_profile()
    ctx = profile.session_context()
    ensure_rust_udfs(ctx)
    validate_required_engine_functions(ctx)


def test_validate_ast_views_smoke(require_native_runtime: None) -> None:
    """Ensure AST view validation runs against registered views."""
    _ = require_native_runtime
    profile = df_profile()
    ctx = profile.session_context()
    ensure_rust_udfs(ctx)
    validate_ast_views(ctx, view_names=AST_VIEW_NAMES)


def test_hamilton_diagnostics_schemas_cover_plan_events() -> None:
    """Hamilton plan diagnostics schemas expose the expected contract fields."""
    submission_fields = set(PIPELINE_TASK_SUBMISSION_SCHEMA.names)
    assert {
        "run_id",
        "task_id",
        "plan_signature",
        "reduced_plan_signature",
        "task_facts",
    }.issubset(submission_fields)
    grouping_fields = set(PIPELINE_TASK_GROUPING_SCHEMA.names)
    assert {"run_id", "task_ids", "task_count"}.issubset(grouping_fields)
    expansion_fields = set(PIPELINE_TASK_EXPANSION_SCHEMA.names)
    assert {"run_id", "task_id", "parameter_keys"}.issubset(expansion_fields)
    drift_fields = set(PIPELINE_PLAN_DRIFT_SCHEMA.names)
    assert {
        "plan_task_count",
        "admitted_task_count",
        "missing_generations",
        "submission_event_count",
    }.issubset(drift_fields)
    events_fields = set(DATAFUSION_PIPELINE_EVENTS_V2_SCHEMA.names)
    assert {
        "event_time_unix_ms",
        "run_id",
        "event_name",
        "plan_signature",
        "reduced_plan_signature",
        "event_payload_msgpack",
        "event_payload_hash",
    }.issubset(events_fields)


def test_semantic_type_validation_uses_schema_metadata_on_zero_rows() -> None:
    """Semantic type checks should pass on empty tables with metadata."""
    profile = df_profile()
    ctx = profile.session_context()
    adapter = DataFusionIOAdapter(ctx=ctx, profile=profile)
    schema = pa.schema(
        [
            pa.field(
                "node_id",
                pa.string(),
                metadata={SEMANTIC_TYPE_META: b"NodeId"},
            )
        ]
    )
    adapter.register_arrow_table("cpg_nodes", empty_table(schema), overwrite=True)
    validate_semantic_types(
        ctx,
        table_names=("cpg_nodes",),
        allow_row_probe_fallback=False,
    )


def test_semantic_type_validation_raises_when_metadata_missing_and_no_row_fallback() -> None:
    """Semantic type checks should fail without schema metadata in zero-row mode."""
    profile = df_profile()
    ctx = profile.session_context()
    adapter = DataFusionIOAdapter(ctx=ctx, profile=profile)
    schema = pa.schema([pa.field("node_id", pa.string())])
    adapter.register_arrow_table("cpg_nodes", empty_table(schema), overwrite=True)
    with pytest.raises(ValueError, match="Missing semantic type metadata"):
        validate_semantic_types(
            ctx,
            table_names=("cpg_nodes",),
            allow_row_probe_fallback=False,
        )


def test_semantic_validation_table_names_use_canonical_cpg_outputs() -> None:
    """Semantic validation tables should only include canonical CPG outputs."""
    tables = _semantic_validation_tables()
    assert "cpg_nodes" in tables
    assert "cpg_edges" in tables
    assert "cpg_nodes_v1" not in tables
    assert "cpg_edges_v1" not in tables
