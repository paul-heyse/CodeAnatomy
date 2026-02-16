"""Canonical view names and defaults for relationship outputs."""

from __future__ import annotations

from typing import Final

from semantics.output_names import RELATION_OUTPUT_NAME

REL_NAME_SYMBOL_OUTPUT: Final[str] = "rel_name_symbol"
REL_IMPORT_SYMBOL_OUTPUT: Final[str] = "rel_import_symbol"
REL_DEF_SYMBOL_OUTPUT: Final[str] = "rel_def_symbol"
REL_CALLSITE_SYMBOL_OUTPUT: Final[str] = "rel_callsite_symbol"

RELATION_VIEW_NAMES: Final[tuple[str, ...]] = (
    REL_NAME_SYMBOL_OUTPUT,
    REL_IMPORT_SYMBOL_OUTPUT,
    REL_DEF_SYMBOL_OUTPUT,
    REL_CALLSITE_SYMBOL_OUTPUT,
    RELATION_OUTPUT_NAME,
)

# Semantic intermediate views (from semantics.naming canonical names)
SCIP_OCCURRENCES_NORM_OUTPUT: Final[str] = "scip_occurrences_norm"
CST_REFS_NORM_OUTPUT: Final[str] = "cst_refs_norm"
CST_DEFS_NORM_OUTPUT: Final[str] = "cst_defs_norm"
CST_IMPORTS_NORM_OUTPUT: Final[str] = "cst_imports_norm"
CST_CALLS_NORM_OUTPUT: Final[str] = "cst_calls_norm"

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
    "REL_CALLSITE_SYMBOL_OUTPUT",
    "REL_DEF_SYMBOL_OUTPUT",
    "REL_IMPORT_SYMBOL_OUTPUT",
    "REL_NAME_SYMBOL_OUTPUT",
    "SCIP_OCCURRENCES_NORM_OUTPUT",
    "SEMANTIC_INTERMEDIATE_VIEWS",
]
