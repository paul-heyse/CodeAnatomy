"""Declarative view specifications catalog.

This module provides the declarative specifications for all views, replacing the
3,256-line _VIEW_SELECT_EXPRS dictionary with concise, data-driven specifications.

The specs are organized by source extractor (AST, CST, bytecode, SCIP, tree-sitter, symtable)
and can generate the same expressions as the original manual definitions.

For views that require complex ID generation, aggregations, or custom logic that doesn't
fit the declarative pattern, we maintain a separate MANUAL_VIEW_EXPRS dictionary that
preserves the original handwritten expressions.

Usage
-----
To generate expressions for a view::

    from datafusion_engine.views.view_specs_catalog import VIEW_SPECS_BY_NAME

    spec = VIEW_SPECS_BY_NAME["ast_calls"]
    exprs = spec.to_exprs()

To integrate with the existing registry::

    from datafusion_engine.views.view_specs_catalog import generate_view_select_exprs

    _VIEW_SELECT_EXPRS = generate_view_select_exprs()
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Final

from datafusion import col

from datafusion_engine.udf.shims import arrow_metadata
from datafusion_engine.views.view_spec import (
    SPAN_FIELDS_STANDARD,
    SPAN_FIELDS_WITH_AST_RECORD,
    SpanFieldConfig,
    ViewProjectionSpec,
    cast,
    literal_col,
    struct_field,
)

if TYPE_CHECKING:
    from datafusion.expr import Expr


# ---------------------------------------------------------------------------
# AST Views
# ---------------------------------------------------------------------------

AST_CALLS_SPEC: Final[ViewProjectionSpec] = ViewProjectionSpec(
    name="ast_calls",
    base_table="ast_files_v1",
    comment="AST call expressions with span and attrs",
    include_file_identity=True,
    passthrough_cols=("ast_id", "parent_ast_id", "func_kind", "func_name"),
    attrs_extracts=(
        ("arg_count", "Int32"),
        ("keyword_count", "Int32"),
        ("starred_count", "Int32"),
        ("kw_star_count", "Int32"),
    ),
    span_config=SPAN_FIELDS_WITH_AST_RECORD,
)

AST_DEFS_SPEC: Final[ViewProjectionSpec] = ViewProjectionSpec(
    name="ast_defs",
    base_table="ast_files_v1",
    comment="AST definitions (functions, classes, etc.)",
    include_file_identity=True,
    passthrough_cols=("ast_id", "parent_ast_id", "kind", "name"),
    attrs_extracts=(
        ("decorator_count", "Int32"),
        ("arg_count", "Int32"),
        ("posonly_count", "Int32"),
        ("kwonly_count", "Int32"),
        ("type_params_count", "Int32"),
        ("base_count", "Int32"),
        ("keyword_count", "Int32"),
    ),
    span_config=SPAN_FIELDS_WITH_AST_RECORD,
)

AST_DOCSTRINGS_SPEC: Final[ViewProjectionSpec] = ViewProjectionSpec(
    name="ast_docstrings",
    base_table="ast_files_v1",
    comment="AST docstrings with owner info",
    include_file_identity=True,
    passthrough_cols=(
        "owner_ast_id",
        "owner_kind",
        "owner_name",
        "docstring",
        "source",
    ),
    span_config=SPAN_FIELDS_WITH_AST_RECORD,
)

AST_ERRORS_SPEC: Final[ViewProjectionSpec] = ViewProjectionSpec(
    name="ast_errors",
    base_table="ast_files_v1",
    comment="AST parse errors",
    include_file_identity=True,
    passthrough_cols=("error_type", "message"),
    span_config=SPAN_FIELDS_WITH_AST_RECORD,
)

AST_IMPORTS_SPEC: Final[ViewProjectionSpec] = ViewProjectionSpec(
    name="ast_imports",
    base_table="ast_files_v1",
    comment="AST import statements",
    include_file_identity=True,
    passthrough_cols=(
        "ast_id",
        "parent_ast_id",
        "kind",
        "module",
        "name",
        "asname",
        "alias_index",
        "level",
    ),
    span_config=SPAN_FIELDS_WITH_AST_RECORD,
)

AST_NODES_SPEC: Final[ViewProjectionSpec] = ViewProjectionSpec(
    name="ast_nodes",
    base_table="ast_files_v1",
    comment="AST nodes",
    include_file_identity=True,
    passthrough_cols=("ast_id", "parent_ast_id", "kind", "name", "slot", "idx"),
    span_config=SPAN_FIELDS_WITH_AST_RECORD,
)

AST_EDGES_SPEC: Final[ViewProjectionSpec] = ViewProjectionSpec(
    name="ast_edges",
    base_table="ast_files_v1",
    comment="AST parent-child edges",
    include_file_identity=True,
    passthrough_cols=("src", "dst", "kind", "slot", "idx"),
    transforms=(cast("attrs", "Map(Utf8, Utf8)"),),
)

AST_TYPE_IGNORES_SPEC: Final[ViewProjectionSpec] = ViewProjectionSpec(
    name="ast_type_ignores",
    base_table="ast_files_v1",
    comment="AST type: ignore comments",
    include_file_identity=True,
    passthrough_cols=("ast_id", "tag"),
    span_config=SPAN_FIELDS_WITH_AST_RECORD,
)


# ---------------------------------------------------------------------------
# CST Views (LibCST)
# ---------------------------------------------------------------------------

CST_CALLSITES_SPEC: Final[ViewProjectionSpec] = ViewProjectionSpec(
    name="cst_callsites",
    base_table="libcst_files_v1",
    comment="CST call sites",
    include_file_identity=True,
    include_file_sha256=True,
    passthrough_cols=(
        "call_id",
        "call_bstart",
        "call_bend",
        "callee_bstart",
        "callee_bend",
        "callee_shape",
        "callee_text",
        "arg_count",
        "callee_dotted",
        "callee_qnames",
        "callee_fqns",
        "inferred_type",
    ),
    span_config=SpanFieldConfig(
        span_col="span",
        include_legacy_fields=False,
        include_line_base=False,
        include_col_unit=False,
        include_end_exclusive=False,
        include_span_struct=False,
        include_ast_record=False,
    ),
)

CST_DEFS_SPEC: Final[ViewProjectionSpec] = ViewProjectionSpec(
    name="cst_defs",
    base_table="libcst_files_v1",
    comment="CST definitions",
    include_file_identity=True,
    include_file_sha256=True,
    passthrough_cols=(
        "def_id",
        "container_def_kind",
        "container_def_bstart",
        "container_def_bend",
        "kind",
        "name",
        "def_bstart",
        "def_bend",
        "name_bstart",
        "name_bend",
        "qnames",
        "def_fqns",
        "docstring",
        "decorator_count",
    ),
)

CST_IMPORTS_SPEC: Final[ViewProjectionSpec] = ViewProjectionSpec(
    name="cst_imports",
    base_table="libcst_files_v1",
    comment="CST import statements",
    include_file_identity=True,
    include_file_sha256=True,
    passthrough_cols=(
        "kind",
        "module",
        "relative_level",
        "name",
        "asname",
        "is_star",
        "stmt_bstart",
        "stmt_bend",
        "alias_bstart",
        "alias_bend",
    ),
)

CST_REFS_SPEC: Final[ViewProjectionSpec] = ViewProjectionSpec(
    name="cst_refs",
    base_table="libcst_files_v1",
    comment="CST name references",
    include_file_identity=True,
    include_file_sha256=True,
    passthrough_cols=(
        "ref_id",
        "ref_kind",
        "ref_text",
        "expr_ctx",
        "scope_type",
        "scope_name",
        "scope_role",
        "parent_kind",
        "inferred_type",
        "bstart",
        "bend",
    ),
)

CST_DECORATORS_SPEC: Final[ViewProjectionSpec] = ViewProjectionSpec(
    name="cst_decorators",
    base_table="libcst_files_v1",
    comment="CST decorators",
    include_file_identity=True,
    include_file_sha256=True,
    passthrough_cols=(
        "owner_def_id",
        "owner_kind",
        "owner_def_bstart",
        "owner_def_bend",
        "decorator_text",
        "decorator_index",
        "bstart",
        "bend",
    ),
)

CST_DOCSTRINGS_SPEC: Final[ViewProjectionSpec] = ViewProjectionSpec(
    name="cst_docstrings",
    base_table="libcst_files_v1",
    comment="CST docstrings",
    include_file_identity=True,
    include_file_sha256=True,
    passthrough_cols=(
        "owner_def_id",
        "owner_kind",
        "owner_def_bstart",
        "owner_def_bend",
        "docstring",
        "bstart",
        "bend",
    ),
)

CST_PARSE_ERRORS_SPEC: Final[ViewProjectionSpec] = ViewProjectionSpec(
    name="cst_parse_errors",
    base_table="libcst_files_v1",
    comment="CST parse errors",
    include_file_identity=True,
    include_file_sha256=True,
    passthrough_cols=(
        "error_type",
        "message",
        "raw_line",
        "raw_column",
        "editor_line",
        "editor_column",
        "context",
        "line_base",
        "col_unit",
        "end_exclusive",
    ),
)

CST_CALL_ARGS_SPEC: Final[ViewProjectionSpec] = ViewProjectionSpec(
    name="cst_call_args",
    base_table="libcst_files_v1",
    comment="CST call arguments",
    include_file_identity=True,
    include_file_sha256=True,
    passthrough_cols=(
        "call_id",
        "call_bstart",
        "call_bend",
        "arg_index",
        "keyword",
        "star",
        "arg_text",
        "bstart",
        "bend",
    ),
)

CST_SCHEMA_DIAGNOSTICS_SPEC: Final[ViewProjectionSpec] = ViewProjectionSpec(
    name="cst_schema_diagnostics",
    base_table="libcst_files_v1",
    comment="CST schema diagnostics",
    include_file_identity=True,
    include_file_sha256=True,
    passthrough_cols=(
        "dataset_name",
        "field_path",
        "expected_type",
        "actual_type",
        "field_name",
        "severity",
        "message",
    ),
)


# ---------------------------------------------------------------------------
# Tree-sitter Views
# ---------------------------------------------------------------------------

TS_NODES_SPEC: Final[ViewProjectionSpec] = ViewProjectionSpec(
    name="ts_nodes",
    base_table="tree_sitter_files_v1",
    comment="Tree-sitter AST nodes",
    include_file_identity=True,
    passthrough_cols=(
        "ts_id",
        "parent_ts_id",
        "kind",
        "is_named",
        "is_error",
        "is_missing",
        "text",
        "bstart",
        "bend",
    ),
    span_config=SPAN_FIELDS_STANDARD,
)

TS_EDGES_SPEC: Final[ViewProjectionSpec] = ViewProjectionSpec(
    name="ts_edges",
    base_table="tree_sitter_files_v1",
    comment="Tree-sitter parent-child edges",
    include_file_identity=True,
    passthrough_cols=("src", "dst", "child_index", "field_name"),
)

TS_ERRORS_SPEC: Final[ViewProjectionSpec] = ViewProjectionSpec(
    name="ts_errors",
    base_table="tree_sitter_files_v1",
    comment="Tree-sitter parse errors",
    include_file_identity=True,
    passthrough_cols=("ts_id", "kind", "text", "bstart", "bend"),
    span_config=SPAN_FIELDS_STANDARD,
)

TS_CALLS_SPEC: Final[ViewProjectionSpec] = ViewProjectionSpec(
    name="ts_calls",
    base_table="tree_sitter_files_v1",
    comment="Tree-sitter call expressions",
    include_file_identity=True,
    passthrough_cols=(
        "node_id",
        "parent_id",
        "callee_kind",
        "callee_text",
        "callee_node_id",
    ),
    transforms=(
        struct_field("span", "start", "line0", alias="start_line"),
        struct_field("span", "start", "col", alias="start_col"),
        struct_field("span", "end", "line0", alias="end_line"),
        struct_field("span", "end", "col", alias="end_col"),
        literal_col(0, "line_base"),
        struct_field("span", "col_unit", alias="col_unit"),
        struct_field("span", "end_exclusive", alias="end_exclusive"),
        struct_field("span", "byte_span", "byte_start", alias="start_byte"),
        cast("attrs", "Map(Utf8, Utf8)"),
    ),
)

TS_DEFS_SPEC: Final[ViewProjectionSpec] = ViewProjectionSpec(
    name="ts_defs",
    base_table="tree_sitter_files_v1",
    comment="Tree-sitter definitions",
    include_file_identity=True,
    passthrough_cols=(
        "ts_id",
        "parent_ts_id",
        "def_kind",
        "def_name",
        "name_node_id",
        "bstart",
        "bend",
    ),
    span_config=SPAN_FIELDS_STANDARD,
)

TS_IMPORTS_SPEC: Final[ViewProjectionSpec] = ViewProjectionSpec(
    name="ts_imports",
    base_table="tree_sitter_files_v1",
    comment="Tree-sitter imports",
    include_file_identity=True,
    passthrough_cols=(
        "ts_id",
        "parent_ts_id",
        "import_kind",
        "module_name",
        "imported_name",
        "alias_name",
        "level",
        "bstart",
        "bend",
    ),
    span_config=SPAN_FIELDS_STANDARD,
)

TS_DOCSTRINGS_SPEC: Final[ViewProjectionSpec] = ViewProjectionSpec(
    name="ts_docstrings",
    base_table="tree_sitter_files_v1",
    comment="Tree-sitter docstrings",
    include_file_identity=True,
    passthrough_cols=(
        "ts_id",
        "owner_ts_id",
        "owner_kind",
        "docstring",
        "bstart",
        "bend",
    ),
    span_config=SPAN_FIELDS_STANDARD,
)

TS_CAPTURES_SPEC: Final[ViewProjectionSpec] = ViewProjectionSpec(
    name="ts_captures",
    base_table="tree_sitter_files_v1",
    comment="Tree-sitter query captures",
    include_file_identity=True,
    passthrough_cols=(
        "capture_id",
        "query_name",
        "capture_name",
        "node_id",
        "node_kind",
        "node_text",
        "bstart",
        "bend",
    ),
    span_config=SPAN_FIELDS_STANDARD,
)

TS_MISSING_SPEC: Final[ViewProjectionSpec] = ViewProjectionSpec(
    name="ts_missing",
    base_table="tree_sitter_files_v1",
    comment="Tree-sitter missing nodes",
    include_file_identity=True,
    passthrough_cols=(
        "ts_id",
        "parent_ts_id",
        "kind",
        "bstart",
        "bend",
    ),
    span_config=SPAN_FIELDS_STANDARD,
)

TS_STATS_SPEC: Final[ViewProjectionSpec] = ViewProjectionSpec(
    name="ts_stats",
    base_table="tree_sitter_files_v1",
    comment="Tree-sitter parse statistics",
    include_file_identity=True,
    passthrough_cols=(
        "node_count",
        "named_count",
        "error_count",
        "missing_count",
        "depth",
    ),
)


# ---------------------------------------------------------------------------
# Symtable Views
# ---------------------------------------------------------------------------

SYMTABLE_SCOPES_SPEC: Final[ViewProjectionSpec] = ViewProjectionSpec(
    name="symtable_scopes",
    base_table="symtable_files_v1",
    comment="Symtable scopes",
    include_file_identity=True,
    passthrough_cols=(
        "scope_id",
        "parent_scope_id",
        "block_type",
        "name",
        "lineno",
        "is_nested",
        "has_children",
    ),
)

SYMTABLE_SYMBOLS_SPEC: Final[ViewProjectionSpec] = ViewProjectionSpec(
    name="symtable_symbols",
    base_table="symtable_files_v1",
    comment="Symtable symbols",
    include_file_identity=True,
    passthrough_cols=(
        "symbol_id",
        "scope_id",
        "name",
        "is_referenced",
        "is_imported",
        "is_parameter",
        "is_global",
        "is_declared_global",
        "is_local",
        "is_free",
        "is_assigned",
        "is_namespace",
    ),
)

SYMTABLE_SCOPE_EDGES_SPEC: Final[ViewProjectionSpec] = ViewProjectionSpec(
    name="symtable_scope_edges",
    base_table="symtable_files_v1",
    comment="Symtable scope parent-child edges",
    include_file_identity=True,
    passthrough_cols=(
        "parent_scope_id",
        "child_scope_id",
        "child_index",
    ),
)

SYMTABLE_NAMESPACE_EDGES_SPEC: Final[ViewProjectionSpec] = ViewProjectionSpec(
    name="symtable_namespace_edges",
    base_table="symtable_files_v1",
    comment="Symtable namespace containment edges",
    include_file_identity=True,
    passthrough_cols=(
        "scope_id",
        "symbol_id",
    ),
)

SYMTABLE_CLASS_METHODS_SPEC: Final[ViewProjectionSpec] = ViewProjectionSpec(
    name="symtable_class_methods",
    base_table="symtable_files_v1",
    comment="Symtable class methods",
    include_file_identity=True,
    passthrough_cols=(
        "class_scope_id",
        "method_scope_id",
        "method_name",
        "lineno",
    ),
)

SYMTABLE_FUNCTION_PARTITIONS_SPEC: Final[ViewProjectionSpec] = ViewProjectionSpec(
    name="symtable_function_partitions",
    base_table="symtable_files_v1",
    comment="Symtable function partitions (closure boundaries)",
    include_file_identity=True,
    passthrough_cols=(
        "function_scope_id",
        "partition_kind",
        "symbol_count",
    ),
)


# ---------------------------------------------------------------------------
# Bytecode Views
# ---------------------------------------------------------------------------

BYTECODE_ERRORS_SPEC: Final[ViewProjectionSpec] = ViewProjectionSpec(
    name="bytecode_errors",
    base_table="bytecode_files_v1",
    comment="Bytecode extraction errors",
    include_file_identity=True,
    passthrough_cols=(
        "error_type",
        "message",
        "traceback",
    ),
)

BYTECODE_EXCEPTION_TABLE_SPEC: Final[ViewProjectionSpec] = ViewProjectionSpec(
    name="bytecode_exception_table",
    base_table="bytecode_files_v1",
    comment="Bytecode exception handler table",
    include_file_identity=True,
    passthrough_cols=(
        "start",
        "end",
        "target",
        "depth",
        "lasti",
    ),
)

PY_BC_METADATA_SPEC: Final[ViewProjectionSpec] = ViewProjectionSpec(
    name="py_bc_metadata",
    base_table="bytecode_files_v1",
    comment="Bytecode code object metadata",
    include_file_identity=True,
    passthrough_cols=(
        "qualname",
        "filename",
        "firstlineno",
        "argcount",
        "posonlyargcount",
        "kwonlyargcount",
        "nlocals",
        "stacksize",
        "flags",
    ),
)

PY_BC_CODE_UNITS_SPEC: Final[ViewProjectionSpec] = ViewProjectionSpec(
    name="py_bc_code_units",
    base_table="bytecode_files_v1",
    comment="Bytecode code units (code objects)",
    include_file_identity=True,
    passthrough_cols=(
        "code_unit_id",
        "parent_code_unit_id",
        "qualname",
        "kind",
        "argcount",
        "posonlyargcount",
        "kwonlyargcount",
        "nlocals",
        "stacksize",
        "flags",
        "firstlineno",
    ),
)

PY_BC_CONSTS_SPEC: Final[ViewProjectionSpec] = ViewProjectionSpec(
    name="py_bc_consts",
    base_table="bytecode_files_v1",
    comment="Bytecode constant values",
    include_file_identity=True,
    passthrough_cols=(
        "code_unit_id",
        "const_index",
        "const_type",
        "const_repr",
        "const_is_code",
    ),
)

PY_BC_LINE_TABLE_SPEC: Final[ViewProjectionSpec] = ViewProjectionSpec(
    name="py_bc_line_table",
    base_table="bytecode_files_v1",
    comment="Bytecode line number table",
    include_file_identity=True,
    passthrough_cols=(
        "code_unit_id",
        "start_offset",
        "end_offset",
        "line_number",
    ),
)

PY_BC_FLAGS_DETAIL_SPEC: Final[ViewProjectionSpec] = ViewProjectionSpec(
    name="py_bc_flags_detail",
    base_table="bytecode_files_v1",
    comment="Bytecode code object flags detail",
    include_file_identity=True,
    passthrough_cols=(
        "code_unit_id",
        "flags_value",
        "is_optimized",
        "is_newlocals",
        "is_varargs",
        "is_varkeywords",
        "is_nested",
        "is_generator",
        "is_coroutine",
        "is_iterable_coroutine",
        "is_async_generator",
    ),
)

PY_BC_CACHE_ENTRIES_SPEC: Final[ViewProjectionSpec] = ViewProjectionSpec(
    name="py_bc_cache_entries",
    base_table="bytecode_files_v1",
    comment="Bytecode inline cache entries",
    include_file_identity=True,
    passthrough_cols=(
        "code_unit_id",
        "instr_index",
        "cache_offset",
        "cache_counter",
    ),
)

PY_BC_CFG_EDGES_SPEC: Final[ViewProjectionSpec] = ViewProjectionSpec(
    name="py_bc_cfg_edges",
    base_table="bytecode_files_v1",
    comment="Bytecode control flow graph edges",
    include_file_identity=True,
    passthrough_cols=(
        "code_unit_id",
        "src_offset",
        "dst_offset",
        "edge_kind",
    ),
)

PY_BC_DFG_EDGES_SPEC: Final[ViewProjectionSpec] = ViewProjectionSpec(
    name="py_bc_dfg_edges",
    base_table="bytecode_files_v1",
    comment="Bytecode data flow graph edges",
    include_file_identity=True,
    passthrough_cols=(
        "code_unit_id",
        "src_offset",
        "dst_offset",
        "var_name",
        "flow_kind",
    ),
)


# ---------------------------------------------------------------------------
# SCIP Views
# ---------------------------------------------------------------------------

SCIP_METADATA_SPEC: Final[ViewProjectionSpec] = ViewProjectionSpec(
    name="scip_metadata",
    base_table="scip_indexes_v1",
    comment="SCIP index metadata",
    include_file_identity=False,
    passthrough_cols=(
        "index_id",
        "index_path",
        "version",
        "tool_name",
        "tool_version",
        "tool_arguments",
        "project_root",
    ),
)

SCIP_DOCUMENTS_SPEC: Final[ViewProjectionSpec] = ViewProjectionSpec(
    name="scip_documents",
    base_table="scip_indexes_v1",
    comment="SCIP documents",
    include_file_identity=False,
    passthrough_cols=(
        "index_id",
        "document_id",
        "path",
        "language",
        "position_encoding",
    ),
)

SCIP_DOCUMENT_TEXTS_SPEC: Final[ViewProjectionSpec] = ViewProjectionSpec(
    name="scip_document_texts",
    base_table="scip_indexes_v1",
    comment="SCIP document texts",
    include_file_identity=False,
    passthrough_cols=(
        "document_id",
        "path",
        "text",
    ),
)

SCIP_OCCURRENCES_SPEC: Final[ViewProjectionSpec] = ViewProjectionSpec(
    name="scip_occurrences",
    base_table="scip_indexes_v1",
    comment="SCIP symbol occurrences",
    include_file_identity=False,
    passthrough_cols=(
        "document_id",
        "path",
        "symbol",
        "symbol_roles",
        "syntax_kind",
        "syntax_kind_name",
        "override_documentation",
        "range_raw",
        "enclosing_range_raw",
        "start_line",
        "start_char",
        "end_line",
        "end_char",
        "range_len",
        "enc_start_line",
        "enc_start_char",
        "enc_end_line",
        "enc_end_char",
    ),
)

SCIP_DOCUMENT_SYMBOLS_SPEC: Final[ViewProjectionSpec] = ViewProjectionSpec(
    name="scip_document_symbols",
    base_table="scip_indexes_v1",
    comment="SCIP document symbols",
    include_file_identity=False,
    passthrough_cols=(
        "document_id",
        "path",
        "symbol",
    ),
)

SCIP_DIAGNOSTICS_SPEC: Final[ViewProjectionSpec] = ViewProjectionSpec(
    name="scip_diagnostics",
    base_table="scip_indexes_v1",
    comment="SCIP diagnostics",
    include_file_identity=False,
    passthrough_cols=(
        "document_id",
        "path",
        "severity",
        "code",
        "message",
        "source",
        "start_line",
        "start_char",
        "end_line",
        "end_char",
    ),
)

SCIP_INDEX_STATS_SPEC: Final[ViewProjectionSpec] = ViewProjectionSpec(
    name="scip_index_stats",
    base_table="scip_indexes_v1",
    comment="SCIP index statistics",
    include_file_identity=False,
    passthrough_cols=(
        "index_id",
        "document_count",
        "occurrence_count",
        "symbol_count",
    ),
)


# ---------------------------------------------------------------------------
# Manual View Expression Builders
# ---------------------------------------------------------------------------
# These views require complex ID generation, aggregations, or conditional logic
# that doesn't fit the declarative ViewProjectionSpec pattern. They are built
# using custom expression builders that will be integrated with the registry.


def _build_ast_span_metadata_exprs() -> tuple[Expr, ...]:
    """Build ast_span_metadata view expressions.

    This view extracts span metadata from multiple nested datasets in the AST
    structure, requiring custom arrow_metadata calls for each dataset.

    Returns
    -------
    tuple[Expr, ...]
        DataFusion select expressions for ast_span_metadata view.
    """
    return (
        arrow_metadata(col("nodes")["span"], "line_base").alias("nodes_line_base"),
        arrow_metadata(col("nodes")["span"], "col_unit").alias("nodes_col_unit"),
        arrow_metadata(col("nodes")["span"], "end_exclusive").alias("nodes_end_exclusive"),
        arrow_metadata(col("errors")["span"], "line_base").alias("errors_line_base"),
        arrow_metadata(col("errors")["span"], "col_unit").alias("errors_col_unit"),
        arrow_metadata(col("errors")["span"], "end_exclusive").alias("errors_end_exclusive"),
        arrow_metadata(col("docstrings")["span"], "line_base").alias("docstrings_line_base"),
        arrow_metadata(col("docstrings")["span"], "col_unit").alias("docstrings_col_unit"),
        arrow_metadata(col("docstrings")["span"], "end_exclusive").alias(
            "docstrings_end_exclusive"
        ),
        arrow_metadata(col("imports")["span"], "line_base").alias("imports_line_base"),
        arrow_metadata(col("imports")["span"], "col_unit").alias("imports_col_unit"),
        arrow_metadata(col("imports")["span"], "end_exclusive").alias("imports_end_exclusive"),
        arrow_metadata(col("defs")["span"], "line_base").alias("defs_line_base"),
        arrow_metadata(col("defs")["span"], "col_unit").alias("defs_col_unit"),
        arrow_metadata(col("defs")["span"], "end_exclusive").alias("defs_end_exclusive"),
        arrow_metadata(col("calls")["span"], "line_base").alias("calls_line_base"),
        arrow_metadata(col("calls")["span"], "col_unit").alias("calls_col_unit"),
        arrow_metadata(col("calls")["span"], "end_exclusive").alias("calls_end_exclusive"),
        arrow_metadata(col("type_ignores")["span"], "line_base").alias("type_ignores_line_base"),
        arrow_metadata(col("type_ignores")["span"], "col_unit").alias("type_ignores_col_unit"),
        arrow_metadata(col("type_ignores")["span"], "end_exclusive").alias(
            "type_ignores_end_exclusive"
        ),
    )


def _build_ts_span_metadata_exprs() -> tuple[Expr, ...]:
    """Build ts_span_metadata view expressions.

    Similar to ast_span_metadata, extracts span metadata from tree-sitter datasets.

    Returns
    -------
    tuple[Expr, ...]
        DataFusion select expressions for ts_span_metadata view.
    """
    return (
        arrow_metadata(col("nodes")["span"], "line_base").alias("nodes_line_base"),
        arrow_metadata(col("nodes")["span"], "col_unit").alias("nodes_col_unit"),
        arrow_metadata(col("nodes")["span"], "end_exclusive").alias("nodes_end_exclusive"),
        arrow_metadata(col("errors")["span"], "line_base").alias("errors_line_base"),
        arrow_metadata(col("errors")["span"], "col_unit").alias("errors_col_unit"),
        arrow_metadata(col("errors")["span"], "end_exclusive").alias("errors_end_exclusive"),
        arrow_metadata(col("defs")["span"], "line_base").alias("defs_line_base"),
        arrow_metadata(col("defs")["span"], "col_unit").alias("defs_col_unit"),
        arrow_metadata(col("defs")["span"], "end_exclusive").alias("defs_end_exclusive"),
        arrow_metadata(col("calls")["span"], "line_base").alias("calls_line_base"),
        arrow_metadata(col("calls")["span"], "col_unit").alias("calls_col_unit"),
        arrow_metadata(col("calls")["span"], "end_exclusive").alias("calls_end_exclusive"),
        arrow_metadata(col("imports")["span"], "line_base").alias("imports_line_base"),
        arrow_metadata(col("imports")["span"], "col_unit").alias("imports_col_unit"),
        arrow_metadata(col("imports")["span"], "end_exclusive").alias("imports_end_exclusive"),
        arrow_metadata(col("docstrings")["span"], "line_base").alias("docstrings_line_base"),
        arrow_metadata(col("docstrings")["span"], "col_unit").alias("docstrings_col_unit"),
        arrow_metadata(col("docstrings")["span"], "end_exclusive").alias(
            "docstrings_end_exclusive"
        ),
    )


def _build_cst_parse_manifest_exprs() -> tuple[Expr, ...]:
    """Build cst_parse_manifest view expressions.

    Requires nested struct navigation with n0.n0_item pattern.

    Returns
    -------
    tuple[Expr, ...]
        DataFusion select expressions for cst_parse_manifest view.
    """
    return (
        (col("n0")["n0_item"])["file_id"].alias("file_id"),
        (col("n0")["n0_item"])["path"].alias("path"),
        (col("n0")["n0_item"])["file_sha256"].alias("file_sha256"),
        (col("n0")["n0_item"])["encoding"].alias("encoding"),
        (col("n0")["n0_item"])["default_indent"].alias("default_indent"),
        (col("n0")["n0_item"])["default_newline"].alias("default_newline"),
        (col("n0")["n0_item"])["has_trailing_newline"].alias("has_trailing_newline"),
        (col("n0")["n0_item"])["future_imports"].alias("future_imports"),
        (col("n0")["n0_item"])["module_name"].alias("module_name"),
        (col("n0")["n0_item"])["package_name"].alias("package_name"),
    )


# Manual view expressions dictionary for views requiring custom builders.
# Some builders use UDFs that may not be available at import time, so we
# catch TypeError and return empty tuples for unavailable views.


from collections.abc import Callable as CallableABC


def _safe_build(builder: CallableABC[[], tuple[Expr, ...]]) -> tuple[Expr, ...]:
    # Safely call a builder, returning empty tuple if UDF unavailable.
    try:
        return builder()
    except TypeError:
        # UDF not available in this environment
        return ()


MANUAL_VIEW_EXPRS: Final[dict[str, tuple[Expr, ...]]] = {
    "ast_span_metadata": _safe_build(_build_ast_span_metadata_exprs),
    "ts_span_metadata": _safe_build(_build_ts_span_metadata_exprs),
    "cst_parse_manifest": _safe_build(_build_cst_parse_manifest_exprs),
    # KV extraction views (attrs views) - these unnest map entries as key/value pairs
    # These require the base view to use unnest_column="kv" after map_entries(col("attrs"))
    # They are currently handled by the registry with custom KV extraction logic
    "ast_call_attrs": (),  # Handled by registry: unnest map_entries(attrs) from ast_calls
    "ast_def_attrs": (),  # Handled by registry: unnest map_entries(attrs) from ast_defs
    "ast_node_attrs": (),  # Handled by registry: unnest map_entries(attrs) from ast_nodes
    "ast_edge_attrs": (),  # Handled by registry: unnest map_entries(attrs) from ast_edges
    "cst_callsites_attrs": (),  # Handled by registry
    "cst_defs_attrs": (),  # Handled by registry
    "cst_imports_attrs": (),  # Handled by registry
    "cst_refs_attrs": (),  # Handled by registry
    "cst_nodes_attrs": (),  # Handled by registry
    "cst_edges_attrs": (),  # Handled by registry
    "py_bc_cfg_edge_attrs": (),  # Handled by registry
    "py_bc_error_attrs": (),  # Handled by registry
    "py_bc_instruction_attrs": (),  # Handled by registry
    "symtable_symbol_attrs": (),  # Handled by registry
    # *_attr_origin views track provenance for attrs - also handled by registry
    "cst_callsites_attr_origin": (),
    "cst_defs_attr_origin": (),
    "cst_imports_attr_origin": (),
    "cst_refs_attr_origin": (),
    "cst_nodes_attr_origin": (),
    "cst_edges_attr_origin": (),
    # Span unnest views - these unnest span lists from callsites/defs/refs
    "cst_callsite_span_unnest": (),  # Handled by registry
    "cst_def_span_unnest": (),  # Handled by registry
    "cst_ref_span_unnest": (),  # Handled by registry
    "cst_callsite_spans": (),  # Handled by registry
    "cst_def_spans": (),  # Handled by registry
    "cst_ref_spans": (),  # Handled by registry
    # Views with complex ID generation using stable_id/prefixed_hash64
    "cst_type_exprs": (),  # Requires prefixed_hash64 ID generation
    "py_bc_blocks": (),  # Requires stable_id and prefixed_hash64
    "py_bc_instructions": (),  # Requires stable_id for code_unit_id
    "py_bc_instruction_spans": (),  # Requires stable_id
    "py_bc_instruction_span_fields": (),  # Complex span field extraction
    "py_bc_instruction_attr_keys": (),  # Requires attrs unnesting
    "py_bc_instruction_attr_values": (),  # Requires attrs unnesting
    # Aggregation/check views (comparison views between extractors)
    "ts_ast_calls_check": (),  # Aggregation comparison
    "ts_ast_defs_check": (),  # Aggregation comparison
    "ts_ast_imports_check": (),  # Aggregation comparison
    "ts_cst_docstrings_check": (),  # Aggregation comparison
    # Empty placeholder views for future implementation
    "scip_external_symbol_information": (),
    "scip_signature_occurrences": (),
    "scip_symbol_information": (),
    "scip_symbol_relationships": (),
    "python_imports": (),  # Cross-extractor unified imports view
}


# ---------------------------------------------------------------------------
# View Spec Registry
# ---------------------------------------------------------------------------

# All view specs in a tuple for iteration
VIEW_SPECS: Final[tuple[ViewProjectionSpec, ...]] = (
    # AST views
    AST_CALLS_SPEC,
    AST_DEFS_SPEC,
    AST_DOCSTRINGS_SPEC,
    AST_ERRORS_SPEC,
    AST_IMPORTS_SPEC,
    AST_NODES_SPEC,
    AST_EDGES_SPEC,
    AST_TYPE_IGNORES_SPEC,
    # CST views
    CST_CALLSITES_SPEC,
    CST_DEFS_SPEC,
    CST_IMPORTS_SPEC,
    CST_REFS_SPEC,
    CST_DECORATORS_SPEC,
    CST_DOCSTRINGS_SPEC,
    CST_PARSE_ERRORS_SPEC,
    CST_CALL_ARGS_SPEC,
    CST_SCHEMA_DIAGNOSTICS_SPEC,
    # Tree-sitter views
    TS_NODES_SPEC,
    TS_EDGES_SPEC,
    TS_ERRORS_SPEC,
    TS_CALLS_SPEC,
    TS_DEFS_SPEC,
    TS_IMPORTS_SPEC,
    TS_DOCSTRINGS_SPEC,
    TS_CAPTURES_SPEC,
    TS_MISSING_SPEC,
    TS_STATS_SPEC,
    # Symtable views
    SYMTABLE_SCOPES_SPEC,
    SYMTABLE_SYMBOLS_SPEC,
    SYMTABLE_SCOPE_EDGES_SPEC,
    SYMTABLE_NAMESPACE_EDGES_SPEC,
    SYMTABLE_CLASS_METHODS_SPEC,
    SYMTABLE_FUNCTION_PARTITIONS_SPEC,
    # Bytecode views
    BYTECODE_ERRORS_SPEC,
    BYTECODE_EXCEPTION_TABLE_SPEC,
    PY_BC_METADATA_SPEC,
    PY_BC_CODE_UNITS_SPEC,
    PY_BC_CONSTS_SPEC,
    PY_BC_LINE_TABLE_SPEC,
    PY_BC_FLAGS_DETAIL_SPEC,
    PY_BC_CACHE_ENTRIES_SPEC,
    PY_BC_CFG_EDGES_SPEC,
    PY_BC_DFG_EDGES_SPEC,
    # SCIP views
    SCIP_METADATA_SPEC,
    SCIP_DOCUMENTS_SPEC,
    SCIP_DOCUMENT_TEXTS_SPEC,
    SCIP_OCCURRENCES_SPEC,
    SCIP_DOCUMENT_SYMBOLS_SPEC,
    SCIP_DIAGNOSTICS_SPEC,
    SCIP_INDEX_STATS_SPEC,
)

# Lookup by name
VIEW_SPECS_BY_NAME: Final[dict[str, ViewProjectionSpec]] = {spec.name: spec for spec in VIEW_SPECS}


def generate_view_select_exprs() -> dict[str, tuple[Expr, ...]]:
    """Generate the _VIEW_SELECT_EXPRS dictionary from specs.

    This function bridges the declarative specs with the existing
    registry infrastructure, generating expression tuples that can
    be used as a drop-in replacement for the manual definitions.

    Combines both declarative specs and manual expression builders.

    Returns
    -------
    dict[str, tuple[Expr, ...]]
        View name to expression tuple mapping.
    """
    result: dict[str, tuple[Expr, ...]] = {}

    # Add all declarative specs
    for spec in VIEW_SPECS:
        result[spec.name] = spec.to_exprs()

    # Add manual expressions for complex views
    result.update(MANUAL_VIEW_EXPRS)

    return result


__all__ = [
    "AST_CALLS_SPEC",
    "AST_DEFS_SPEC",
    "AST_DOCSTRINGS_SPEC",
    "AST_EDGES_SPEC",
    "AST_ERRORS_SPEC",
    "AST_IMPORTS_SPEC",
    "AST_NODES_SPEC",
    "AST_TYPE_IGNORES_SPEC",
    "BYTECODE_ERRORS_SPEC",
    "BYTECODE_EXCEPTION_TABLE_SPEC",
    "CST_CALLSITES_SPEC",
    "CST_CALL_ARGS_SPEC",
    "CST_DECORATORS_SPEC",
    "CST_DEFS_SPEC",
    "CST_DOCSTRINGS_SPEC",
    "CST_IMPORTS_SPEC",
    "CST_PARSE_ERRORS_SPEC",
    "CST_REFS_SPEC",
    "CST_SCHEMA_DIAGNOSTICS_SPEC",
    "MANUAL_VIEW_EXPRS",
    "PY_BC_CACHE_ENTRIES_SPEC",
    "PY_BC_CFG_EDGES_SPEC",
    "PY_BC_CODE_UNITS_SPEC",
    "PY_BC_CONSTS_SPEC",
    "PY_BC_DFG_EDGES_SPEC",
    "PY_BC_FLAGS_DETAIL_SPEC",
    "PY_BC_LINE_TABLE_SPEC",
    "PY_BC_METADATA_SPEC",
    "SCIP_DIAGNOSTICS_SPEC",
    "SCIP_DOCUMENTS_SPEC",
    "SCIP_DOCUMENT_SYMBOLS_SPEC",
    "SCIP_DOCUMENT_TEXTS_SPEC",
    "SCIP_INDEX_STATS_SPEC",
    "SCIP_METADATA_SPEC",
    "SCIP_OCCURRENCES_SPEC",
    "SYMTABLE_CLASS_METHODS_SPEC",
    "SYMTABLE_FUNCTION_PARTITIONS_SPEC",
    "SYMTABLE_NAMESPACE_EDGES_SPEC",
    "SYMTABLE_SCOPES_SPEC",
    "SYMTABLE_SCOPE_EDGES_SPEC",
    "SYMTABLE_SYMBOLS_SPEC",
    "TS_CALLS_SPEC",
    "TS_CAPTURES_SPEC",
    "TS_DEFS_SPEC",
    "TS_DOCSTRINGS_SPEC",
    "TS_EDGES_SPEC",
    "TS_ERRORS_SPEC",
    "TS_IMPORTS_SPEC",
    "TS_MISSING_SPEC",
    "TS_NODES_SPEC",
    "TS_STATS_SPEC",
    "VIEW_SPECS",
    "VIEW_SPECS_BY_NAME",
    "generate_view_select_exprs",
]
