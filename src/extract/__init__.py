from __future__ import annotations

"""
Extraction layer.

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

from .ast_extract import ASTExtractOptions, ASTExtractResult, extract_ast
from .bytecode_extract import BytecodeExtractOptions, BytecodeExtractResult, extract_bytecode
from .cst_extract import CSTExtractOptions, CSTExtractResult, extract_cst
from .repo_scan import RepoScanOptions, scan_repo, stable_id
from .scip_extract import (
    SCIPExtractResult,
    SCIPIndexOptions,
    SCIPParseOptions,
    extract_scip_tables,
    parse_index_scip,
    run_scip_python_index,
)
from .symtable_extract import SymtableExtractOptions, SymtableExtractResult, extract_symtable

__all__ = [
    "ASTExtractOptions",
    "ASTExtractResult",
    "BytecodeExtractOptions",
    "BytecodeExtractResult",
    "CSTExtractOptions",
    "CSTExtractResult",
    "RepoScanOptions",
    "SCIPExtractResult",
    "SCIPIndexOptions",
    "SCIPParseOptions",
    "SymtableExtractOptions",
    "SymtableExtractResult",
    "extract_ast",
    "extract_bytecode",
    "extract_cst",
    "extract_scip_tables",
    "extract_symtable",
    "parse_index_scip",
    "run_scip_python_index",
    "scan_repo",
    "stable_id",
]
