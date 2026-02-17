"""Extraction contract tests for semantic-input mapping."""

from __future__ import annotations

from extraction.contracts import resolve_semantic_input_locations


def test_resolve_semantic_input_locations_prefers_canonical_candidates() -> None:
    """Test resolve semantic input locations prefers canonical candidates."""
    locations = {
        "repo_files_v1": "/tmp/repo_files_v1",
        "file_line_index_v1": "/tmp/file_line_index_v1",
        "libcst_files": "/tmp/libcst_files",
        "symtable_files_v1": "/tmp/symtable_files_v1",
        "scip_index": "/tmp/scip_index",
    }

    resolved = resolve_semantic_input_locations(locations)

    assert resolved["repo_files_v1"] == "/tmp/repo_files_v1"
    assert resolved["file_line_index_v1"] == "/tmp/file_line_index_v1"
    assert resolved["cst_imports"] == "/tmp/libcst_files"
    assert resolved["cst_defs"] == "/tmp/libcst_files"
    assert resolved["symtable_scopes"] == "/tmp/symtable_files_v1"
    assert resolved["symtable_symbols"] == "/tmp/symtable_files_v1"
    assert resolved["scip_occurrences"] == "/tmp/scip_index"
    assert resolved["scip_diagnostics"] == "/tmp/scip_index"
