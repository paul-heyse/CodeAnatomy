"""Tests for DataFusion schema registry helpers."""

from __future__ import annotations

import pyarrow as pa
from datafusion import SessionContext

from arrowdsl.core.schema_constants import KEY_FIELDS_META, REQUIRED_NON_NULL_META
from arrowdsl.schema.metadata import metadata_list_bytes
from datafusion_engine.schema_registry import (
    AST_VIEW_NAMES,
    nested_view_specs,
    register_all_schemas,
    register_schema,
    schema_for,
    schema_names,
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


def test_register_all_schemas_roundtrip() -> None:
    """Ensure schemas are registered and round-trip from the DataFusion context."""
    ctx = SessionContext()
    register_all_schemas(ctx)
    for name in schema_names():
        actual = _to_arrow_schema(ctx.table(name).schema())
        expected = schema_for(name)
        assert actual == expected


def test_symtable_schema_metadata() -> None:
    """Expose symtable schema metadata and span ABI tags."""
    schema = schema_for("symtable_files_v1")
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
    ctx = SessionContext()
    register_rust_udfs(ctx)
    validate_required_cst_functions(ctx)


def test_required_symtable_functions_present() -> None:
    """Validate required symtable function inventory and signatures."""
    ctx = SessionContext()
    register_rust_udfs(ctx)
    validate_required_symtable_functions(ctx)


def test_required_bytecode_functions_present() -> None:
    """Validate required bytecode function inventory and signatures."""
    ctx = SessionContext()
    register_rust_udfs(ctx)
    validate_required_bytecode_functions(ctx)


def test_required_engine_functions_present() -> None:
    """Validate required engine function inventory."""
    ctx = SessionContext()
    register_rust_udfs(ctx)
    validate_required_engine_functions(ctx)


def test_validate_ast_views_smoke() -> None:
    """Ensure AST view validation runs against registered views."""
    ctx = SessionContext()
    register_rust_udfs(ctx)
    register_schema(ctx, "ast_files_v1", schema_for("ast_files_v1"))
    views = [view for view in nested_view_specs() if view.name in AST_VIEW_NAMES]
    for view in views:
        view.register(ctx, validate=False)
    validate_ast_views(ctx)
