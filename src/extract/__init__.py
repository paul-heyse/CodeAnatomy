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

Module Organization:
- extractors/ - Evidence layer implementations
- coordination/ - Execution context and materialization
- scanning/ - Repository scanning and scope filtering
- git/ - Git repository integration
- python/ - Python-specific scope and environment
- infrastructure/ - Caching, parallelization, utilities
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from extract.extractors.ast_extract import (
        AstExtractOptions,
        extract_ast,
        extract_ast_tables,
    )
    from extract.extractors.bytecode_extract import (
        BytecodeExtractOptions,
        extract_bytecode,
        extract_bytecode_table,
    )
    from extract.extractors.cst_extract import (
        CstExtractOptions,
        extract_cst,
        extract_cst_tables,
    )
    from extract.extractors.external_scope import (
        ExternalInterfaceExtractOptions,
        extract_python_external,
        extract_python_external_tables,
    )
    from extract.extractors.imports_extract import (
        PythonImportsExtractOptions,
        extract_python_imports,
        extract_python_imports_tables,
    )
    from extract.extractors.scip.extract import (
        SCIPIndexOptions,
        SCIPParseOptions,
        extract_scip_tables,
        parse_index_scip,
        run_scip_python_index,
    )
    from extract.extractors.scip.identity import ScipIdentity, resolve_scip_identity
    from extract.extractors.scip.setup import (
        ScipIndexPaths,
        build_scip_index_options,
        ensure_scip_build_dir,
    )
    from extract.extractors.symtable_extract import (
        SymtableExtractOptions,
        extract_symtable,
        extract_symtables_table,
    )
    from extract.extractors.tree_sitter.extract import (
        TreeSitterExtractOptions,
        extract_ts,
        extract_ts_tables,
    )
    from extract.git.blobs import RepoBlobOptions, scan_repo_blobs
    from extract.infrastructure.result_types import ExtractResult
    from extract.scanning.repo_scan import RepoScanOptions, scan_repo

# Map of export names to (module_path, attribute_name) for lazy loading
_EXPORTS: dict[str, tuple[str, str]] = {
    # Extractor options and functions
    "AstExtractOptions": ("extract.extractors.ast_extract", "AstExtractOptions"),
    "BytecodeExtractOptions": ("extract.extractors.bytecode_extract", "BytecodeExtractOptions"),
    "CstExtractOptions": ("extract.extractors.cst_extract", "CstExtractOptions"),
    "ExtractResult": ("extract.infrastructure.result_types", "ExtractResult"),
    "PythonImportsExtractOptions": (
        "extract.extractors.imports_extract",
        "PythonImportsExtractOptions",
    ),
    "RepoBlobOptions": ("extract.git.blobs", "RepoBlobOptions"),
    "RepoScanOptions": ("extract.scanning.repo_scan", "RepoScanOptions"),
    "SCIPIndexOptions": ("extract.extractors.scip.extract", "SCIPIndexOptions"),
    "SCIPParseOptions": ("extract.extractors.scip.extract", "SCIPParseOptions"),
    "ScipIdentity": ("extract.extractors.scip.identity", "ScipIdentity"),
    "ScipIndexPaths": ("extract.extractors.scip.setup", "ScipIndexPaths"),
    "SymtableExtractOptions": ("extract.extractors.symtable_extract", "SymtableExtractOptions"),
    "TreeSitterExtractOptions": (
        "extract.extractors.tree_sitter.extract",
        "TreeSitterExtractOptions",
    ),
    "build_scip_index_options": ("extract.extractors.scip.setup", "build_scip_index_options"),
    "ensure_scip_build_dir": ("extract.extractors.scip.setup", "ensure_scip_build_dir"),
    "extract_ast": ("extract.extractors.ast_extract", "extract_ast"),
    "extract_ast_tables": ("extract.extractors.ast_extract", "extract_ast_tables"),
    "extract_bytecode": ("extract.extractors.bytecode_extract", "extract_bytecode"),
    "extract_bytecode_table": ("extract.extractors.bytecode_extract", "extract_bytecode_table"),
    "extract_cst": ("extract.extractors.cst_extract", "extract_cst"),
    "extract_cst_tables": ("extract.extractors.cst_extract", "extract_cst_tables"),
    "extract_python_external": ("extract.extractors.external_scope", "extract_python_external"),
    "extract_python_external_tables": (
        "extract.extractors.external_scope",
        "extract_python_external_tables",
    ),
    "extract_python_imports": ("extract.extractors.imports_extract", "extract_python_imports"),
    "extract_python_imports_tables": (
        "extract.extractors.imports_extract",
        "extract_python_imports_tables",
    ),
    "extract_scip_tables": ("extract.extractors.scip.extract", "extract_scip_tables"),
    "extract_symtable": ("extract.extractors.symtable_extract", "extract_symtable"),
    "extract_symtables_table": ("extract.extractors.symtable_extract", "extract_symtables_table"),
    "extract_ts": ("extract.extractors.tree_sitter.extract", "extract_ts"),
    "extract_ts_tables": ("extract.extractors.tree_sitter.extract", "extract_ts_tables"),
    "parse_index_scip": ("extract.extractors.scip.extract", "parse_index_scip"),
    "resolve_scip_identity": ("extract.extractors.scip.identity", "resolve_scip_identity"),
    "run_scip_python_index": ("extract.extractors.scip.extract", "run_scip_python_index"),
    "scan_repo": ("extract.scanning.repo_scan", "scan_repo"),
    "scan_repo_blobs": ("extract.git.blobs", "scan_repo_blobs"),
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
    "BytecodeExtractOptions",
    "CstExtractOptions",
    "ExternalInterfaceExtractOptions",
    "ExtractResult",
    "PythonImportsExtractOptions",
    "RepoBlobOptions",
    "RepoScanOptions",
    "SCIPIndexOptions",
    "SCIPParseOptions",
    "ScipIdentity",
    "ScipIndexPaths",
    "SymtableExtractOptions",
    "TreeSitterExtractOptions",
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
