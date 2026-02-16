"""Core extraction schema facade."""

from __future__ import annotations

import pyarrow as pa

from datafusion_engine.schema.extraction_schemas import (
    AST_FILES_SCHEMA,
    BYTECODE_FILES_SCHEMA,
    SYMTABLE_FILES_SCHEMA,
    TREE_SITTER_FILES_SCHEMA,
)


def core_extraction_schemas() -> dict[str, pa.Schema]:
    """Return core extraction schemas keyed by dataset name."""
    return {
        "ast_files_v1": AST_FILES_SCHEMA,
        "bytecode_files_v1": BYTECODE_FILES_SCHEMA,
        "symtable_files_v1": SYMTABLE_FILES_SCHEMA,
        "tree_sitter_files_v1": TREE_SITTER_FILES_SCHEMA,
    }


__all__ = ["core_extraction_schemas"]
