"""Tests for diagnostics table builders."""

from __future__ import annotations

from obs.diagnostics_tables import datafusion_schema_registry_validation_table


def test_schema_registry_validation_includes_view_and_parse_errors() -> None:
    """Ensure view and SQL parse errors are represented in diagnostics tables."""
    records = [
        {
            "event_time_unix_ms": 123,
            "missing": ["missing_schema"],
            "type_errors": {"tree_sitter_files_v1": "type mismatch"},
            "view_errors": {"ts_views": "view validation failed"},
            "sql_parse_errors": {
                "ts_nodes": [
                    {
                        "line": 1,
                        "col": 5,
                        "description": "unexpected token",
                    }
                ]
            },
        }
    ]
    table = datafusion_schema_registry_validation_table(records)
    rows = table.to_pylist()
    issue_types = {row.get("issue_type") for row in rows}
    assert "view_error" in issue_types
    assert "sql_parse_error" in issue_types
