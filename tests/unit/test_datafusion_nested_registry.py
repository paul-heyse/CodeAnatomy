"""Tests for DataFusion nested registry helpers."""

from __future__ import annotations

from datafusion_engine.schema_registry import nested_schema_for, schema_for


def test_nested_schema_for_cst_parse_manifest() -> None:
    """Ensure nested schema derivation for LibCST parse manifest."""
    schema = nested_schema_for("cst_parse_manifest")
    assert schema.names[:2] == ("file_id", "path")
    assert "module_name" in schema.names
    assert "schema_fingerprint" in schema.names


def test_nested_schema_for_cst_refs() -> None:
    """Ensure nested schema derivation for LibCST references."""
    schema = nested_schema_for("cst_refs")
    assert "ref_id" in schema.names
    assert "ref_kind" in schema.names
    assert "ref_text" in schema.names


def test_nested_schema_for_cst_call_args() -> None:
    """Ensure nested schema derivation for LibCST call arguments."""
    schema = nested_schema_for("cst_call_args")
    assert "call_id" in schema.names
    assert "arg_index" in schema.names
    assert "arg_text" in schema.names


def test_scip_metadata_schema_includes_identity_fields() -> None:
    """Ensure SCIP metadata schemas include identity fields."""
    schema = schema_for("scip_metadata_v1")
    assert "tool_arguments" in schema.names
    assert "project_name" in schema.names
    assert "project_version" in schema.names
    assert "project_namespace" in schema.names


def test_scip_occurrences_schema_includes_role_flags() -> None:
    """Ensure SCIP occurrences include decoded role flags."""
    schema = schema_for("scip_occurrences_v1")
    for name in ("is_definition", "is_import", "is_write", "is_read", "syntax_kind_name"):
        assert name in schema.names


def test_scip_document_symbols_schema_contains_document_fields() -> None:
    """Ensure document symbols schemas include document linkage."""
    schema = schema_for("scip_document_symbols_v1")
    assert "document_id" in schema.names
    assert "symbol" in schema.names
