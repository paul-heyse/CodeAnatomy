"""Normalization helpers for extracted tables."""

from normalize.bytecode_anchor import BytecodeSpanColumns, anchor_instructions
from normalize.bytecode_cfg import build_cfg, build_cfg_blocks, build_cfg_edges
from normalize.bytecode_dfg import build_def_use_events, run_reaching_defs
from normalize.diagnostics import collect_diags
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
from normalize.types import normalize_type_exprs, normalize_types

__all__ = [
    "BytecodeSpanColumns",
    "FileTextIndex",
    "RepoTextIndex",
    "SchemaInferOptions",
    "add_ast_byte_spans",
    "add_scip_occurrence_byte_spans",
    "add_span_id_column",
    "align_table_to_schema",
    "align_tables_to_unified_schema",
    "anchor_instructions",
    "build_cfg",
    "build_cfg_blocks",
    "build_cfg_edges",
    "build_def_use_events",
    "build_repo_text_index",
    "collect_diags",
    "infer_schema_from_tables",
    "normalize_cst_callsites_spans",
    "normalize_cst_defs_spans",
    "normalize_cst_imports_spans",
    "normalize_type_exprs",
    "normalize_types",
    "run_reaching_defs",
    "span_id",
    "stable_id",
    "stable_int64",
    "unify_schemas",
]
