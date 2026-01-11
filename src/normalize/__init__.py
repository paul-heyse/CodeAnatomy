"""Normalization helpers for extracted tables."""

from normalize.ids import add_span_id_column, span_id, stable_id, stable_int64
from normalize.schema_infer import (
    SchemaInferOptions,
    align_table_to_schema,
    align_tables_to_unified_schema,
    infer_schema_from_tables,
    unify_schemas,
)
from normalize.spans import (
    FileTextIndex,
    RepoTextIndex,
    add_ast_byte_spans,
    add_scip_occurrence_byte_spans,
    build_repo_text_index,
    normalize_cst_callsites_spans,
    normalize_cst_defs_spans,
    normalize_cst_imports_spans,
)

__all__ = [
    "FileTextIndex",
    "RepoTextIndex",
    "SchemaInferOptions",
    "add_ast_byte_spans",
    "add_scip_occurrence_byte_spans",
    "add_span_id_column",
    "align_table_to_schema",
    "align_tables_to_unified_schema",
    "build_repo_text_index",
    "infer_schema_from_tables",
    "normalize_cst_callsites_spans",
    "normalize_cst_defs_spans",
    "normalize_cst_imports_spans",
    "span_id",
    "stable_id",
    "stable_int64",
    "unify_schemas",
]
