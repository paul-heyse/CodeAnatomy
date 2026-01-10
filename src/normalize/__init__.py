from __future__ import annotations

"""
normalize/

This package is where extraction tables become join-ready for relationship rules and Acero plans.

Core responsibilities:
  1) Spans: normalize all source ranges to *byte offsets* (bstart/bend) in UTF-8 bytes.
     - AST: lineno/col_offset -> byte spans
     - SCIP: (line,char) ranges -> byte spans using Document.position_encoding semantics
     - CST: already byte spans, but we add canonical aliases (bstart/bend) for consistent joins
  2) Schema inference: unify schemas across partially-populated tables and align tables permissively.
  3) Stable IDs: deterministic hashing for ids/keys/edges, so downstream graph building is reproducible.
"""

from .spans import (
    FileTextIndex,
    RepoTextIndex,
    build_repo_text_index,
    add_ast_byte_spans,
    add_scip_occurrence_byte_spans,
    normalize_cst_callsites_spans,
    normalize_cst_imports_spans,
    normalize_cst_defs_spans,
)
from .schema_infer import (
    SchemaInferOptions,
    unify_schemas,
    infer_schema_from_tables,
    align_table_to_schema,
    align_tables_to_unified_schema,
)
from .ids import (
    stable_id,
    stable_int64,
    span_id,
    add_span_id_column,
)

__all__ = [
    # spans
    "FileTextIndex",
    "RepoTextIndex",
    "build_repo_text_index",
    "add_ast_byte_spans",
    "add_scip_occurrence_byte_spans",
    "normalize_cst_callsites_spans",
    "normalize_cst_imports_spans",
    "normalize_cst_defs_spans",
    # schema inference
    "SchemaInferOptions",
    "unify_schemas",
    "infer_schema_from_tables",
    "align_table_to_schema",
    "align_tables_to_unified_schema",
    # ids
    "stable_id",
    "stable_int64",
    "span_id",
    "add_span_id_column",
]
