"""Tests for DataFusion nested registry helpers."""

from __future__ import annotations

from datafusion import SessionContext

from arrowdsl.schema.build import empty_table
from datafusion_engine.io_adapter import DataFusionIOAdapter
from datafusion_engine.schema_registry import (
    LIBCST_FILES_SCHEMA,
    SCIP_DOCUMENT_SYMBOLS_SCHEMA,
    SCIP_METADATA_SCHEMA,
    SCIP_OCCURRENCES_SCHEMA,
    nested_view_spec,
)


def test_nested_schema_for_cst_parse_manifest() -> None:
    """Ensure nested schema derivation for LibCST parse manifest."""
    ctx = SessionContext()
    adapter = DataFusionIOAdapter(ctx=ctx, profile=None)
    adapter.register_arrow_table(
        "libcst_files_v1",
        empty_table(LIBCST_FILES_SCHEMA),
        overwrite=True,
    )
    schema = nested_view_spec(ctx, "cst_parse_manifest").schema
    assert schema.names[:2] == ("file_id", "path")
    assert "module_name" in schema.names
    assert "schema_fingerprint" in schema.names


def test_nested_schema_for_cst_refs() -> None:
    """Ensure nested schema derivation for LibCST references."""
    ctx = SessionContext()
    adapter = DataFusionIOAdapter(ctx=ctx, profile=None)
    adapter.register_arrow_table(
        "libcst_files_v1",
        empty_table(LIBCST_FILES_SCHEMA),
        overwrite=True,
    )
    schema = nested_view_spec(ctx, "cst_refs").schema
    assert "ref_id" in schema.names
    assert "ref_kind" in schema.names
    assert "ref_text" in schema.names


def test_nested_schema_for_cst_call_args() -> None:
    """Ensure nested schema derivation for LibCST call arguments."""
    ctx = SessionContext()
    adapter = DataFusionIOAdapter(ctx=ctx, profile=None)
    adapter.register_arrow_table(
        "libcst_files_v1",
        empty_table(LIBCST_FILES_SCHEMA),
        overwrite=True,
    )
    schema = nested_view_spec(ctx, "cst_call_args").schema
    assert "call_id" in schema.names
    assert "arg_index" in schema.names
    assert "arg_text" in schema.names


def test_scip_metadata_schema_includes_identity_fields() -> None:
    """Ensure SCIP metadata schemas include identity fields."""
    schema = SCIP_METADATA_SCHEMA
    assert "tool_arguments" in schema.names
    assert "project_name" in schema.names
    assert "project_version" in schema.names
    assert "project_namespace" in schema.names


def test_scip_occurrences_schema_includes_role_flags() -> None:
    """Ensure SCIP occurrences include decoded role flags."""
    schema = SCIP_OCCURRENCES_SCHEMA
    for name in ("is_definition", "is_import", "is_write", "is_read", "syntax_kind_name"):
        assert name in schema.names


def test_scip_document_symbols_schema_contains_document_fields() -> None:
    """Ensure document symbols schemas include document linkage."""
    schema = SCIP_DOCUMENT_SYMBOLS_SCHEMA
    assert "document_id" in schema.names
    assert "symbol" in schema.names
