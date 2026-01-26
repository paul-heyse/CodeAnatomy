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

DEFAULT_REL_TASK_PRIORITY: Final[int] = 100

__all__ = [
    "DEFAULT_REL_TASK_PRIORITY",
    "RELATION_OUTPUT_NAME",
    "RELATION_VIEW_NAMES",
    "REL_CALLSITE_QNAME_OUTPUT",
    "REL_CALLSITE_SYMBOL_OUTPUT",
    "REL_DEF_SYMBOL_OUTPUT",
    "REL_IMPORT_SYMBOL_OUTPUT",
    "REL_NAME_SYMBOL_OUTPUT",
]
