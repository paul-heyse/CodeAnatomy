"""Extraction layer.

These modules are "bytes-first" and output PyArrow Tables ("fact tables") that downstream
normalization + relationship rules can join without requiring full intermediate schema
specification.

Exports:
- repo scanning -> repo_files table
- AST extraction -> py_ast_nodes, py_ast_edges, py_ast_errors
- CST extraction (LibCST) -> parse manifest/errors + name refs/imports/calls/defs
- SCIP extraction -> documents/occurrences/symbol_information tables (protobuf)
- symtable extraction -> scopes/symbols/scope edges/namespace edges/function partitions
- bytecode extraction -> code units/instructions/exception table (+ optional blocks/CFG)
"""

from __future__ import annotations

from extract.ast_extract import ASTExtractOptions, ASTExtractResult, extract_ast, extract_ast_tables
from extract.bytecode_extract import (
    BytecodeExtractOptions,
    BytecodeExtractResult,
    extract_bytecode,
    extract_bytecode_table,
)
from extract.cst_extract import CSTExtractOptions, CSTExtractResult, extract_cst, extract_cst_tables
from extract.repo_scan import RepoScanOptions, scan_repo, stable_id
from extract.runtime_inspect_extract import (
    RuntimeInspectOptions,
    RuntimeInspectResult,
    extract_runtime_members,
    extract_runtime_objects,
    extract_runtime_signatures,
    extract_runtime_tables,
)
from extract.scip_extract import (
    SCIPExtractResult,
    SCIPIndexOptions,
    SCIPParseOptions,
    extract_scip_tables,
    parse_index_scip,
    run_scip_python_index,
)
from extract.scip_identity import ScipIdentity, resolve_scip_identity
from extract.scip_indexer import ScipIndexPaths, build_scip_index_options, ensure_scip_build_dir
from extract.symtable_extract import (
    SymtableExtractOptions,
    SymtableExtractResult,
    extract_symtable,
    extract_symtables_table,
)
from extract.tree_sitter_extract import (
    TreeSitterExtractOptions,
    TreeSitterExtractResult,
    extract_ts,
    extract_ts_tables,
)

__all__ = [
    "ASTExtractOptions",
    "ASTExtractResult",
    "BytecodeExtractOptions",
    "BytecodeExtractResult",
    "CSTExtractOptions",
    "CSTExtractResult",
    "RepoScanOptions",
    "RuntimeInspectOptions",
    "RuntimeInspectResult",
    "SCIPExtractResult",
    "SCIPIndexOptions",
    "SCIPParseOptions",
    "ScipIdentity",
    "ScipIndexPaths",
    "SymtableExtractOptions",
    "SymtableExtractResult",
    "TreeSitterExtractOptions",
    "TreeSitterExtractResult",
    "build_scip_index_options",
    "ensure_scip_build_dir",
    "extract_ast",
    "extract_ast_tables",
    "extract_bytecode",
    "extract_bytecode_table",
    "extract_cst",
    "extract_cst_tables",
    "extract_runtime_members",
    "extract_runtime_objects",
    "extract_runtime_signatures",
    "extract_runtime_tables",
    "extract_scip_tables",
    "extract_symtable",
    "extract_symtables_table",
    "extract_ts",
    "extract_ts_tables",
    "parse_index_scip",
    "resolve_scip_identity",
    "run_scip_python_index",
    "scan_repo",
    "stable_id",
]
