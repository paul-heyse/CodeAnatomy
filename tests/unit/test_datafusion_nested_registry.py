"""Tests for DataFusion nested registry helpers."""

from __future__ import annotations

from datafusion_engine.schema_registry import nested_base_sql, nested_schema_for


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


def test_nested_base_sql_for_scip_documents() -> None:
    """Ensure nested base SQL is emitted for SCIP documents."""
    sql = nested_base_sql("scip_documents")
    assert "CROSS JOIN unnest" in sql
    assert "documents" in sql


def test_nested_base_sql_for_scip_occurrences() -> None:
    """Ensure nested base SQL handles multi-level paths."""
    expected_joins = 2
    sql = nested_base_sql("scip_occurrences")
    assert sql.count("CROSS JOIN unnest") == expected_joins
    assert "occurrences" in sql


def test_nested_base_sql_for_scip_metadata() -> None:
    """Ensure nested base SQL handles struct-only paths."""
    sql = nested_base_sql("scip_metadata")
    assert "metadata" in sql
    assert "CROSS JOIN unnest" not in sql
