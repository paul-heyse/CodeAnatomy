"""Extraction layer.

These modules are "bytes-first" and output PyArrow Tables ("fact tables") that downstream
normalization + relationship rules can join without requiring full intermediate schema
specification.

Exports:
- repo scanning -> repo_files table
- AST extraction -> ast_files
- CST extraction (LibCST) -> libcst_files
- SCIP extraction -> scip tables
- symtable extraction -> symtable_files
- bytecode extraction -> bytecode_files
- tree-sitter extraction -> tree_sitter_files
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from extract.ast_extract import (
        AstExtractOptions,
        AstExtractResult,
        extract_ast,
        extract_ast_tables,
    )
    from extract.bytecode_extract import (
        BytecodeExtractOptions,
        BytecodeExtractResult,
        extract_bytecode,
        extract_bytecode_table,
    )
    from extract.cst_extract import (
        CstExtractOptions,
        CstExtractResult,
        extract_cst,
        extract_cst_tables,
    )
    from extract.python_external_scope import (
        ExternalInterfaceExtractOptions,
        ExternalInterfaceExtractResult,
        extract_python_external,
        extract_python_external_tables,
    )
    from extract.python_imports_extract import (
        PythonImportsExtractOptions,
        PythonImportsExtractResult,
        extract_python_imports,
        extract_python_imports_tables,
    )
    from extract.repo_blobs import RepoBlobOptions, scan_repo_blobs
    from extract.repo_scan import RepoScanOptions, scan_repo
    from extract.scip_extract import (
        SCIPIndexOptions,
        SCIPParseOptions,
        extract_scip_tables,
        parse_index_scip,
        run_scip_python_index,
    )
    from extract.scip_identity import ScipIdentity, resolve_scip_identity
    from extract.scip_indexer import (
        ScipIndexPaths,
        build_scip_index_options,
        ensure_scip_build_dir,
    )
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

_EXPORTS: dict[str, tuple[str, str]] = {
    "AstExtractOptions": ("extract.ast_extract", "AstExtractOptions"),
    "AstExtractResult": ("extract.ast_extract", "AstExtractResult"),
    "BytecodeExtractOptions": ("extract.bytecode_extract", "BytecodeExtractOptions"),
    "BytecodeExtractResult": ("extract.bytecode_extract", "BytecodeExtractResult"),
    "CstExtractOptions": ("extract.cst_extract", "CstExtractOptions"),
    "CstExtractResult": ("extract.cst_extract", "CstExtractResult"),
    "PythonImportsExtractOptions": (
        "extract.python_imports_extract",
        "PythonImportsExtractOptions",
    ),
    "PythonImportsExtractResult": ("extract.python_imports_extract", "PythonImportsExtractResult"),
    "RepoBlobOptions": ("extract.repo_blobs", "RepoBlobOptions"),
    "RepoScanOptions": ("extract.repo_scan", "RepoScanOptions"),
    "SCIPIndexOptions": ("extract.scip_extract", "SCIPIndexOptions"),
    "SCIPParseOptions": ("extract.scip_extract", "SCIPParseOptions"),
    "ScipIdentity": ("extract.scip_identity", "ScipIdentity"),
    "ScipIndexPaths": ("extract.scip_indexer", "ScipIndexPaths"),
    "SymtableExtractOptions": ("extract.symtable_extract", "SymtableExtractOptions"),
    "SymtableExtractResult": ("extract.symtable_extract", "SymtableExtractResult"),
    "TreeSitterExtractOptions": ("extract.tree_sitter_extract", "TreeSitterExtractOptions"),
    "TreeSitterExtractResult": ("extract.tree_sitter_extract", "TreeSitterExtractResult"),
    "build_scip_index_options": ("extract.scip_indexer", "build_scip_index_options"),
    "ensure_scip_build_dir": ("extract.scip_indexer", "ensure_scip_build_dir"),
    "extract_ast": ("extract.ast_extract", "extract_ast"),
    "extract_ast_tables": ("extract.ast_extract", "extract_ast_tables"),
    "extract_bytecode": ("extract.bytecode_extract", "extract_bytecode"),
    "extract_bytecode_table": ("extract.bytecode_extract", "extract_bytecode_table"),
    "extract_cst": ("extract.cst_extract", "extract_cst"),
    "extract_cst_tables": ("extract.cst_extract", "extract_cst_tables"),
    "extract_python_external": ("extract.python_external_scope", "extract_python_external"),
    "extract_python_external_tables": (
        "extract.python_external_scope",
        "extract_python_external_tables",
    ),
    "extract_python_imports": ("extract.python_imports_extract", "extract_python_imports"),
    "extract_python_imports_tables": (
        "extract.python_imports_extract",
        "extract_python_imports_tables",
    ),
    "scan_repo_blobs": ("extract.repo_blobs", "scan_repo_blobs"),
    "extract_scip_tables": ("extract.scip_extract", "extract_scip_tables"),
    "extract_symtable": ("extract.symtable_extract", "extract_symtable"),
    "extract_symtables_table": ("extract.symtable_extract", "extract_symtables_table"),
    "extract_ts": ("extract.tree_sitter_extract", "extract_ts"),
    "extract_ts_tables": ("extract.tree_sitter_extract", "extract_ts_tables"),
    "parse_index_scip": ("extract.scip_extract", "parse_index_scip"),
    "resolve_scip_identity": ("extract.scip_identity", "resolve_scip_identity"),
    "run_scip_python_index": ("extract.scip_extract", "run_scip_python_index"),
    "scan_repo": ("extract.repo_scan", "scan_repo"),
}


def __getattr__(name: str) -> object:
    target = _EXPORTS.get(name)
    if target is None:
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg)
    module_name, attr = target
    module = importlib.import_module(module_name)
    return getattr(module, attr)


def __dir__() -> list[str]:
    return sorted(list(globals()) + list(_EXPORTS))


__all__ = (
    "AstExtractOptions",
    "AstExtractResult",
    "BytecodeExtractOptions",
    "BytecodeExtractResult",
    "CstExtractOptions",
    "CstExtractResult",
    "ExternalInterfaceExtractOptions",
    "ExternalInterfaceExtractResult",
    "PythonImportsExtractOptions",
    "PythonImportsExtractResult",
    "RepoBlobOptions",
    "RepoScanOptions",
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
    "extract_python_external",
    "extract_python_external_tables",
    "extract_python_imports",
    "extract_python_imports_tables",
    "extract_scip_tables",
    "extract_symtable",
    "extract_symtables_table",
    "extract_ts",
    "extract_ts_tables",
    "parse_index_scip",
    "resolve_scip_identity",
    "run_scip_python_index",
    "scan_repo",
    "scan_repo_blobs",
)
