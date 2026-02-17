"""Extraction-to-semantic dataset contract mapping.

This module defines the compatibility bridge between extraction dataset names
and canonical semantic input names expected by the semantic compiler.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol

import msgspec

from extraction.options import ExtractionRunOptions


class ScipIndexConfig(Protocol):
    """Structural contract for SCIP index configuration payloads."""


class RunExtractionRequestV1(msgspec.Struct, frozen=True):
    """Request envelope for staged extraction orchestration."""

    repo_root: str
    work_dir: str
    scip_index_config: ScipIndexConfig | Mapping[str, object] | None = None
    scip_identity_overrides: object | None = None
    tree_sitter_enabled: bool = True
    max_workers: int = 6
    options: ExtractionRunOptions | Mapping[str, object] | None = None


# Legacy compatibility aliases retained during migration.
_COMPAT_ALIASES: dict[str, str] = {
    "repo_files": "repo_files_v1",
    "ast_imports": "ast_files",
    "ast_symbols": "ast_files",
    "cst_imports": "libcst_files",
    "cst_symbols": "libcst_files",
    "ts_imports": "tree_sitter_files",
    "ts_symbols": "tree_sitter_files",
    "symtable_files": "symtable_files_v1",
    "bytecode_files": "bytecode_files_v1",
    "python_external": "python_external_interfaces",
    "scip_symbols": "scip_index",
}

# Canonical semantic input -> preferred extraction dataset candidates.
_SEMANTIC_INPUT_CANDIDATES: dict[str, tuple[str, ...]] = {
    "cst_refs": ("cst_refs", "libcst_files", "libcst_files_v1"),
    "cst_defs": ("cst_defs", "libcst_files", "libcst_files_v1"),
    "cst_imports": ("cst_imports", "libcst_files", "libcst_files_v1"),
    "cst_callsites": ("cst_callsites", "libcst_files", "libcst_files_v1"),
    "cst_call_args": ("cst_call_args", "libcst_files", "libcst_files_v1"),
    "cst_docstrings": ("cst_docstrings", "libcst_files", "libcst_files_v1"),
    "cst_decorators": ("cst_decorators", "libcst_files", "libcst_files_v1"),
    "repo_files_v1": ("repo_files_v1",),
    "file_line_index_v1": ("file_line_index_v1",),
    "scip_occurrences": ("scip_occurrences", "scip_index", "scip_index_v1"),
    "scip_diagnostics": ("scip_diagnostics", "scip_index", "scip_index_v1"),
    "symtable_scopes": ("symtable_scopes", "symtable_files_v1", "symtable_files"),
    "symtable_symbols": ("symtable_symbols", "symtable_files_v1", "symtable_files"),
}


def with_compat_aliases(delta_locations: Mapping[str, str]) -> dict[str, str]:
    """Return extraction locations extended with legacy aliases.

    Returns:
    -------
    dict[str, str]
        Input mapping plus compatibility aliases.
    """
    resolved = dict(delta_locations)
    for alias, canonical in _COMPAT_ALIASES.items():
        location = resolved.get(canonical)
        if location is not None:
            resolved.setdefault(alias, location)
    return resolved


def resolve_semantic_input_locations(delta_locations: Mapping[str, str]) -> dict[str, str]:
    """Resolve canonical semantic input table locations from extraction outputs.

    Returns:
    -------
    dict[str, str]
        Canonical semantic input names resolved to concrete Delta table locations.
    """
    resolved: dict[str, str] = {}
    for semantic_name, candidates in _SEMANTIC_INPUT_CANDIDATES.items():
        for candidate in candidates:
            location = delta_locations.get(candidate)
            if location is not None:
                resolved[semantic_name] = location
                break
    return resolved


__all__ = [
    "RunExtractionRequestV1",
    "ScipIndexConfig",
    "resolve_semantic_input_locations",
    "with_compat_aliases",
]
