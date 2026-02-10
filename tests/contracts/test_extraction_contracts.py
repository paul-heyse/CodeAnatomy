"""Extraction contract tests for semantic-input compatibility mapping."""

from __future__ import annotations

from extraction.contracts import resolve_semantic_input_locations, with_compat_aliases


def test_with_compat_aliases_includes_legacy_keys() -> None:
    locations = {
        "repo_files_v1": "/tmp/repo_files_v1",
        "libcst_files": "/tmp/libcst_files",
        "tree_sitter_files": "/tmp/tree_sitter_files",
        "symtable_files_v1": "/tmp/symtable_files_v1",
        "bytecode_files_v1": "/tmp/bytecode_files_v1",
        "python_external_interfaces": "/tmp/python_external_interfaces",
        "scip_index": "/tmp/scip_index",
    }

    resolved = with_compat_aliases(locations)

    assert resolved["repo_files"] == "/tmp/repo_files_v1"
    assert resolved["cst_imports"] == "/tmp/libcst_files"
    assert resolved["ts_imports"] == "/tmp/tree_sitter_files"
    assert resolved["symtable_files"] == "/tmp/symtable_files_v1"
    assert resolved["bytecode_files"] == "/tmp/bytecode_files_v1"
    assert resolved["python_external"] == "/tmp/python_external_interfaces"
    assert resolved["scip_symbols"] == "/tmp/scip_index"


def test_resolve_semantic_input_locations_prefers_canonical_candidates() -> None:
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
