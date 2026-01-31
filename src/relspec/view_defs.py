"""Canonical view names and defaults for relationship outputs."""

from __future__ import annotations

from typing import Final

REL_NAME_SYMBOL_OUTPUT: Final[str] = "rel_name_symbol_v1"
REL_IMPORT_SYMBOL_OUTPUT: Final[str] = "rel_import_symbol_v1"
REL_DEF_SYMBOL_OUTPUT: Final[str] = "rel_def_symbol_v1"
REL_CALLSITE_SYMBOL_OUTPUT: Final[str] = "rel_callsite_symbol_v1"
REL_CALLSITE_QNAME_OUTPUT: Final[str] = "rel_callsite_qname_v1"
RELATION_OUTPUT_NAME: Final[str] = "relation_output_v1"

RELATION_VIEW_NAMES: Final[tuple[str, ...]] = (
    REL_NAME_SYMBOL_OUTPUT,
    REL_IMPORT_SYMBOL_OUTPUT,
    REL_DEF_SYMBOL_OUTPUT,
    REL_CALLSITE_SYMBOL_OUTPUT,
    REL_CALLSITE_QNAME_OUTPUT,
    RELATION_OUTPUT_NAME,
)

# Semantic intermediate views (from semantics.naming canonical names)
SCIP_OCCURRENCES_NORM_OUTPUT: Final[str] = "scip_occurrences_norm_v1"
CST_REFS_NORM_OUTPUT: Final[str] = "cst_refs_norm_v1"
CST_DEFS_NORM_OUTPUT: Final[str] = "cst_defs_norm_v1"
CST_IMPORTS_NORM_OUTPUT: Final[str] = "cst_imports_norm_v1"
CST_CALLS_NORM_OUTPUT: Final[str] = "cst_calls_norm_v1"

SEMANTIC_INTERMEDIATE_VIEWS: Final[tuple[str, ...]] = (
    SCIP_OCCURRENCES_NORM_OUTPUT,
    CST_REFS_NORM_OUTPUT,
    CST_DEFS_NORM_OUTPUT,
    CST_IMPORTS_NORM_OUTPUT,
    CST_CALLS_NORM_OUTPUT,
)

DEFAULT_REL_TASK_PRIORITY: Final[int] = 100

__all__ = [
    "CST_CALLS_NORM_OUTPUT",
    "CST_DEFS_NORM_OUTPUT",
    "CST_IMPORTS_NORM_OUTPUT",
    "CST_REFS_NORM_OUTPUT",
    "DEFAULT_REL_TASK_PRIORITY",
    "RELATION_OUTPUT_NAME",
    "RELATION_VIEW_NAMES",
    "REL_CALLSITE_QNAME_OUTPUT",
    "REL_CALLSITE_SYMBOL_OUTPUT",
    "REL_DEF_SYMBOL_OUTPUT",
    "REL_IMPORT_SYMBOL_OUTPUT",
    "REL_NAME_SYMBOL_OUTPUT",
    "SCIP_OCCURRENCES_NORM_OUTPUT",
    "SEMANTIC_INTERMEDIATE_VIEWS",
]
