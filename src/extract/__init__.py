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

from .repo_scan import RepoScanOptions, scan_repo, stable_id
from .ast_extract import ASTExtractOptions, ASTExtractResult, extract_ast
from .cst_extract import CSTExtractOptions, CSTExtractResult, extract_cst
from .scip_extract import (
    SCIPIndexOptions,
    SCIPParseOptions,
    SCIPExtractResult,
    run_scip_python_index,
    parse_index_scip,
    extract_scip_tables,
)
from .symtable_extract import SymtableExtractOptions, SymtableExtractResult, extract_symtable
from .bytecode_extract import BytecodeExtractOptions, BytecodeExtractResult, extract_bytecode

__all__ = [
    "RepoScanOptions",
    "scan_repo",
    "stable_id",
    "ASTExtractOptions",
    "ASTExtractResult",
    "extract_ast",
    "CSTExtractOptions",
    "CSTExtractResult",
    "extract_cst",
    "SCIPIndexOptions",
    "SCIPParseOptions",
    "SCIPExtractResult",
    "run_scip_python_index",
    "parse_index_scip",
    "extract_scip_tables",
    "SymtableExtractOptions",
    "SymtableExtractResult",
    "extract_symtable",
    "BytecodeExtractOptions",
    "BytecodeExtractResult",
    "extract_bytecode",
]
