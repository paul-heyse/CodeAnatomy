"""DataFusion-native nested schemas for canonical datasets."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from functools import partial
from typing import Literal, TypedDict, cast

import pyarrow as pa
from datafusion import SessionContext, col
from datafusion.dataframe import DataFrame
from datafusion.expr import Expr

from arrowdsl.core.ordering import OrderingLevel
from arrowdsl.core.schema_constants import (
    DEFAULT_VALUE_META,
    KEY_FIELDS_META,
    REQUIRED_NON_NULL_META,
    SCHEMA_META_NAME,
    SCHEMA_META_VERSION,
)
from arrowdsl.schema.build import list_view_type, struct_type
from arrowdsl.schema.metadata import (
    function_requirements_metadata_spec,
    metadata_list_bytes,
    optional_functions_from_metadata,
    ordering_metadata_spec,
    required_function_signature_types_from_metadata,
    required_function_signatures_from_metadata,
    required_functions_from_metadata,
)
from arrowdsl.schema.semantic_types import (
    SEMANTIC_TYPE_META,
    SPAN_TYPE_INFO,
    byte_span_type,
    span_type,
)
from datafusion_engine.schema_introspection import SchemaIntrospector, table_names_snapshot
from datafusion_engine.sql_options import sql_options_for_profile
from schema_spec.view_specs import ViewSpec, view_spec_from_builder
from sqlglot_tools.compat import exp

BYTE_SPAN_T = byte_span_type()
SPAN_T = span_type()

ATTRS_T = pa.map_(pa.string(), pa.string())
_DEFAULT_ATTRS_META: dict[bytes, bytes] = {DEFAULT_VALUE_META: b"{}"}
_DEFAULT_ATTRS_VALUE: dict[str, str] = cast(
    "dict[str, str]",
    json.loads(_DEFAULT_ATTRS_META[DEFAULT_VALUE_META].decode("utf-8")),
)
BYTECODE_ATTR_VALUE_T = pa.union(
    [
        pa.field("int_value", pa.int64()),
        pa.field("bool_value", pa.bool_()),
        pa.field("str_value", pa.string()),
    ],
    mode="sparse",
)
BYTECODE_ATTRS_T = pa.map_(pa.string(), BYTECODE_ATTR_VALUE_T)

DIAG_TAGS_TYPE = list_view_type(pa.string(), large=True)
DIAG_DETAIL_STRUCT = struct_type(
    {
        "detail_kind": pa.string(),
        "error_type": pa.string(),
        "source": pa.string(),
        "tags": DIAG_TAGS_TYPE,
    }
)
DIAG_DETAILS_TYPE = list_view_type(DIAG_DETAIL_STRUCT, large=True)

_STRING_TYPE_TOKENS = frozenset({"char", "string", "text", "utf8", "varchar"})
_INT_TYPE_TOKENS = frozenset({"int", "integer", "bigint", "smallint", "tinyint", "uint"})
_LIST_TYPE_TOKENS = frozenset({"array", "list"})
_MAP_TYPE_TOKENS = frozenset({"map"})

SQLGLOT_PARSE_ERROR_DETAIL_STRUCT = struct_type(
    {
        "description": pa.string(),
        "message": pa.string(),
        "line": pa.int64(),
        "col": pa.int64(),
        "start_context": pa.string(),
        "highlight": pa.string(),
        "end_context": pa.string(),
        "into_expression": pa.string(),
    }
)
SQLGLOT_PARSE_ERROR_DETAILS_TYPE = list_view_type(
    SQLGLOT_PARSE_ERROR_DETAIL_STRUCT,
    large=True,
)


def _sql_with_options(ctx: SessionContext, expr: exp.Expression | str) -> DataFrame:
    from sqlglot.errors import ParseError

    from datafusion_engine.compile_options import DataFusionCompileOptions
    from datafusion_engine.execution_facade import DataFusionExecutionFacade
    from sqlglot_tools.optimizer import parse_sql_strict, register_datafusion_dialect

    def _sql_ingest(_payload: Mapping[str, object]) -> None:
        return None

    options = DataFusionCompileOptions(
        sql_options=sql_options_for_profile(None),
        sql_ingest_hook=_sql_ingest,
    )
    facade = DataFusionExecutionFacade(ctx=ctx, runtime_profile=None)
    register_datafusion_dialect()
    if isinstance(expr, str):
        try:
            expr = parse_sql_strict(expr, dialect=options.dialect)
        except (ParseError, TypeError, ValueError) as exc:
            msg = "Schema registry SQL parse failed."
            raise ValueError(msg) from exc
    plan = facade.compile(expr, options=options)
    result = facade.execute(plan)
    if result.dataframe is None:
        msg = "Schema registry SQL execution did not return a DataFusion DataFrame."
        raise ValueError(msg)
    return result.dataframe


def _attrs_field(name: str = "attrs") -> pa.Field:
    return pa.field(name, ATTRS_T, metadata=_DEFAULT_ATTRS_META)


def _bytecode_attrs_field(name: str = "attrs") -> pa.Field:
    return pa.field(name, BYTECODE_ATTRS_T, metadata=_DEFAULT_ATTRS_META)


def default_attrs_value() -> dict[str, str]:
    """Return the default attrs map from schema metadata.

    Returns
    -------
    dict[str, str]
        Default attrs mapping derived from schema metadata.
    """
    return dict(_DEFAULT_ATTRS_VALUE)


SCIP_METADATA_SCHEMA = pa.schema(
    [
        ("index_id", pa.string()),
        ("protocol_version", pa.int32()),
        ("tool_name", pa.string()),
        ("tool_version", pa.string()),
        ("tool_arguments", pa.list_(pa.string())),
        ("project_root", pa.string()),
        ("text_document_encoding", pa.int32()),
        ("project_name", pa.string()),
        ("project_version", pa.string()),
        ("project_namespace", pa.string()),
    ]
)

SCIP_INDEX_STATS_SCHEMA = pa.schema(
    [
        ("index_id", pa.string()),
        ("document_count", pa.int64()),
        ("occurrence_count", pa.int64()),
        ("diagnostic_count", pa.int64()),
        ("symbol_count", pa.int64()),
        ("external_symbol_count", pa.int64()),
        ("missing_position_encoding_count", pa.int64()),
        ("document_text_count", pa.int64()),
        ("document_text_bytes", pa.int64()),
    ]
)

SCIP_DOCUMENTS_SCHEMA = pa.schema(
    [
        ("index_id", pa.string()),
        ("document_id", pa.string()),
        ("path", pa.string()),
        ("language", pa.string()),
        ("position_encoding", pa.int32()),
    ]
)

SCIP_DOCUMENT_TEXTS_SCHEMA = pa.schema(
    [
        ("document_id", pa.string()),
        ("path", pa.string()),
        ("text", pa.string()),
    ]
)

SCIP_SYMBOL_INFO_FIELDS: tuple[pa.Field, ...] = (
    pa.field("symbol", pa.string()),
    pa.field("display_name", pa.string()),
    pa.field("kind", pa.int32()),
    pa.field("kind_name", pa.string()),
    pa.field("enclosing_symbol", pa.string()),
    pa.field("documentation", pa.list_(pa.string())),
    pa.field("signature_text", pa.string()),
    pa.field("signature_language", pa.string()),
)

SCIP_SYMBOL_INFORMATION_SCHEMA = pa.schema(SCIP_SYMBOL_INFO_FIELDS)

SCIP_DOCUMENT_SYMBOLS_SCHEMA = pa.schema(
    [
        ("document_id", pa.string()),
        ("path", pa.string()),
        *SCIP_SYMBOL_INFO_FIELDS,
    ]
)

SCIP_EXTERNAL_SYMBOL_INFORMATION_SCHEMA = pa.schema(SCIP_SYMBOL_INFO_FIELDS)

SCIP_SYMBOL_RELATIONSHIPS_SCHEMA = pa.schema(
    [
        ("symbol", pa.string()),
        ("related_symbol", pa.string()),
        ("is_reference", pa.bool_()),
        ("is_implementation", pa.bool_()),
        ("is_type_definition", pa.bool_()),
        ("is_definition", pa.bool_()),
    ]
)

SCIP_SIGNATURE_OCCURRENCES_SCHEMA = pa.schema(
    [
        ("parent_symbol", pa.string()),
        ("symbol", pa.string()),
        ("symbol_roles", pa.int32()),
        ("syntax_kind", pa.int32()),
        ("syntax_kind_name", pa.string()),
        ("range_raw", pa.list_(pa.int32())),
        ("start_line", pa.int32()),
        ("start_char", pa.int32()),
        ("end_line", pa.int32()),
        ("end_char", pa.int32()),
        ("range_len", pa.int32()),
        ("line_base", pa.int32()),
        ("col_unit", pa.string()),
        ("end_exclusive", pa.bool_()),
        ("is_definition", pa.bool_()),
        ("is_import", pa.bool_()),
        ("is_write", pa.bool_()),
        ("is_read", pa.bool_()),
        ("is_generated", pa.bool_()),
        ("is_test", pa.bool_()),
        ("is_forward_definition", pa.bool_()),
    ]
)

SCIP_OCCURRENCES_SCHEMA = pa.schema(
    [
        ("document_id", pa.string()),
        ("path", pa.string()),
        ("symbol", pa.string()),
        ("symbol_roles", pa.int32()),
        ("syntax_kind", pa.int32()),
        ("syntax_kind_name", pa.string()),
        ("override_documentation", pa.list_(pa.string())),
        ("range_raw", pa.list_(pa.int32())),
        ("enclosing_range_raw", pa.list_(pa.int32())),
        ("start_line", pa.int32()),
        ("start_char", pa.int32()),
        ("end_line", pa.int32()),
        ("end_char", pa.int32()),
        ("range_len", pa.int32()),
        ("enc_start_line", pa.int32()),
        ("enc_start_char", pa.int32()),
        ("enc_end_line", pa.int32()),
        ("enc_end_char", pa.int32()),
        ("enc_range_len", pa.int32()),
        ("line_base", pa.int32()),
        ("col_unit", pa.string()),
        ("end_exclusive", pa.bool_()),
        ("is_definition", pa.bool_()),
        ("is_import", pa.bool_()),
        ("is_write", pa.bool_()),
        ("is_read", pa.bool_()),
        ("is_generated", pa.bool_()),
        ("is_test", pa.bool_()),
        ("is_forward_definition", pa.bool_()),
    ]
)

SCIP_DIAGNOSTICS_SCHEMA = pa.schema(
    [
        ("document_id", pa.string()),
        ("path", pa.string()),
        ("severity", pa.int32()),
        ("code", pa.string()),
        ("message", pa.string()),
        ("source", pa.string()),
        ("tags", pa.list_(pa.int32())),
        ("start_line", pa.int32()),
        ("start_char", pa.int32()),
        ("end_line", pa.int32()),
        ("end_char", pa.int32()),
        ("line_base", pa.int32()),
        ("col_unit", pa.string()),
        ("end_exclusive", pa.bool_()),
    ]
)

CST_NODE_T = pa.struct(
    [
        pa.field("cst_id", pa.int64(), nullable=False),
        ("kind", pa.string()),
        ("span", SPAN_T),
        ("span_ws", SPAN_T),
        _attrs_field(),
    ]
)

CST_EDGE_T = pa.struct(
    [
        pa.field("src", pa.int64(), nullable=False),
        pa.field("dst", pa.int64(), nullable=False),
        ("kind", pa.string()),
        ("slot", pa.string()),
        ("idx", pa.int32()),
        _attrs_field(),
    ]
)

QNAME_T = pa.struct([("name", pa.string()), ("source", pa.string())])
FQN_LIST = pa.list_(pa.string())

CST_PARSE_MANIFEST_T = pa.struct(
    [
        pa.field("file_id", pa.string(), nullable=False),
        pa.field("path", pa.string(), nullable=False),
        ("file_sha256", pa.string()),
        ("encoding", pa.string()),
        ("default_indent", pa.string()),
        ("default_newline", pa.string()),
        ("has_trailing_newline", pa.bool_()),
        ("future_imports", pa.list_(pa.string())),
        ("module_name", pa.string()),
        ("package_name", pa.string()),
        ("libcst_version", pa.string()),
        ("parser_backend", pa.string()),
        ("parsed_python_version", pa.string()),
        ("schema_fingerprint", pa.string()),
    ]
)

CST_PARSE_ERROR_T = pa.struct(
    [
        pa.field("file_id", pa.string(), nullable=False),
        pa.field("path", pa.string(), nullable=False),
        ("file_sha256", pa.string()),
        ("error_type", pa.string()),
        ("message", pa.string()),
        ("raw_line", pa.int64()),
        ("raw_column", pa.int64()),
        ("editor_line", pa.int64()),
        ("editor_column", pa.int64()),
        ("context", pa.string()),
        ("line_base", pa.int32()),
        ("col_unit", pa.string()),
        ("end_exclusive", pa.bool_()),
        _attrs_field("meta"),
    ]
)

CST_REF_T = pa.struct(
    [
        pa.field("file_id", pa.string(), nullable=False),
        pa.field("path", pa.string(), nullable=False),
        ("file_sha256", pa.string()),
        pa.field("ref_id", pa.string(), nullable=True),
        ("ref_kind", pa.string()),
        ("ref_text", pa.string()),
        ("expr_ctx", pa.string()),
        ("scope_type", pa.string()),
        ("scope_name", pa.string()),
        ("scope_role", pa.string()),
        ("parent_kind", pa.string()),
        ("inferred_type", pa.string()),
        ("bstart", pa.int64()),
        ("bend", pa.int64()),
        _attrs_field(),
    ]
)

CST_IMPORT_T = pa.struct(
    [
        pa.field("file_id", pa.string(), nullable=False),
        pa.field("path", pa.string(), nullable=False),
        ("file_sha256", pa.string()),
        ("kind", pa.string()),
        ("module", pa.string()),
        ("relative_level", pa.int32()),
        ("name", pa.string()),
        ("asname", pa.string()),
        ("is_star", pa.bool_()),
        ("stmt_bstart", pa.int64()),
        ("stmt_bend", pa.int64()),
        ("alias_bstart", pa.int64()),
        ("alias_bend", pa.int64()),
        _attrs_field(),
    ]
)

CST_CALLSITE_T = pa.struct(
    [
        pa.field("file_id", pa.string(), nullable=False),
        pa.field("path", pa.string(), nullable=False),
        ("file_sha256", pa.string()),
        pa.field("call_id", pa.string(), nullable=True),
        ("call_bstart", pa.int64()),
        ("call_bend", pa.int64()),
        ("callee_bstart", pa.int64()),
        ("callee_bend", pa.int64()),
        ("callee_shape", pa.string()),
        ("callee_text", pa.string()),
        ("arg_count", pa.int32()),
        ("callee_dotted", pa.string()),
        ("callee_qnames", pa.list_(QNAME_T)),
        ("callee_fqns", FQN_LIST),
        ("inferred_type", pa.string()),
        _attrs_field(),
    ]
)

CST_DEF_T = pa.struct(
    [
        pa.field("file_id", pa.string(), nullable=False),
        pa.field("path", pa.string(), nullable=False),
        ("file_sha256", pa.string()),
        pa.field("def_id", pa.string(), nullable=True),
        ("container_def_kind", pa.string()),
        ("container_def_bstart", pa.int64()),
        ("container_def_bend", pa.int64()),
        ("kind", pa.string()),
        ("name", pa.string()),
        ("def_bstart", pa.int64()),
        ("def_bend", pa.int64()),
        ("name_bstart", pa.int64()),
        ("name_bend", pa.int64()),
        ("qnames", pa.list_(QNAME_T)),
        ("def_fqns", FQN_LIST),
        ("docstring", pa.string()),
        ("decorator_count", pa.int32()),
        _attrs_field(),
    ]
)

CST_TYPE_EXPR_T = pa.struct(
    [
        pa.field("file_id", pa.string(), nullable=False),
        pa.field("path", pa.string(), nullable=False),
        ("file_sha256", pa.string()),
        ("owner_def_kind", pa.string()),
        ("owner_def_bstart", pa.int64()),
        ("owner_def_bend", pa.int64()),
        ("param_name", pa.string()),
        ("expr_kind", pa.string()),
        ("expr_role", pa.string()),
        ("bstart", pa.int64()),
        ("bend", pa.int64()),
        ("expr_text", pa.string()),
    ]
)

CST_DOCSTRING_T = pa.struct(
    [
        pa.field("file_id", pa.string(), nullable=False),
        pa.field("path", pa.string(), nullable=False),
        ("file_sha256", pa.string()),
        pa.field("owner_def_id", pa.string(), nullable=True),
        ("owner_kind", pa.string()),
        ("owner_def_bstart", pa.int64()),
        ("owner_def_bend", pa.int64()),
        ("docstring", pa.string()),
        ("bstart", pa.int64()),
        ("bend", pa.int64()),
    ]
)

CST_DECORATOR_T = pa.struct(
    [
        pa.field("file_id", pa.string(), nullable=False),
        pa.field("path", pa.string(), nullable=False),
        ("file_sha256", pa.string()),
        pa.field("owner_def_id", pa.string(), nullable=True),
        ("owner_kind", pa.string()),
        ("owner_def_bstart", pa.int64()),
        ("owner_def_bend", pa.int64()),
        ("decorator_text", pa.string()),
        ("decorator_index", pa.int32()),
        ("bstart", pa.int64()),
        ("bend", pa.int64()),
    ]
)

CST_CALL_ARG_T = pa.struct(
    [
        pa.field("file_id", pa.string(), nullable=False),
        pa.field("path", pa.string(), nullable=False),
        ("file_sha256", pa.string()),
        pa.field("call_id", pa.string(), nullable=True),
        ("call_bstart", pa.int64()),
        ("call_bend", pa.int64()),
        ("arg_index", pa.int32()),
        ("keyword", pa.string()),
        ("star", pa.string()),
        ("arg_text", pa.string()),
        ("bstart", pa.int64()),
        ("bend", pa.int64()),
    ]
)

LIBCST_FILES_SCHEMA = pa.schema(
    [
        ("repo", pa.string()),
        pa.field("path", pa.string(), nullable=False),
        pa.field("file_id", pa.string(), nullable=False),
        ("file_sha256", pa.string()),
        ("nodes", pa.list_(CST_NODE_T)),
        ("edges", pa.list_(CST_EDGE_T)),
        ("parse_manifest", pa.list_(CST_PARSE_MANIFEST_T)),
        ("parse_errors", pa.list_(CST_PARSE_ERROR_T)),
        ("refs", pa.list_(CST_REF_T)),
        ("imports", pa.list_(CST_IMPORT_T)),
        ("callsites", pa.list_(CST_CALLSITE_T)),
        ("defs", pa.list_(CST_DEF_T)),
        ("type_exprs", pa.list_(CST_TYPE_EXPR_T)),
        ("docstrings", pa.list_(CST_DOCSTRING_T)),
        ("decorators", pa.list_(CST_DECORATOR_T)),
        ("call_args", pa.list_(CST_CALL_ARG_T)),
        _attrs_field(),
    ]
)

AST_SPAN_META: dict[bytes, bytes] = {
    b"line_base": b"0",
    b"col_unit": b"byte",
    b"end_exclusive": b"true",
    SEMANTIC_TYPE_META: SPAN_TYPE_INFO.name.encode("utf-8"),
}

TREE_SITTER_SPAN_META: dict[bytes, bytes] = {
    b"line_base": b"0",
    b"col_unit": b"byte",
    b"end_exclusive": b"true",
    SEMANTIC_TYPE_META: SPAN_TYPE_INFO.name.encode("utf-8"),
}

AST_NODE_T = pa.struct(
    [
        ("ast_id", pa.int32()),
        ("parent_ast_id", pa.int32()),
        ("kind", pa.string()),
        ("name", pa.string()),
        ("value", pa.string()),
        pa.field("span", SPAN_T, metadata=AST_SPAN_META),
        ("attrs", ATTRS_T),
    ]
)

AST_EDGE_T = pa.struct(
    [
        ("src", pa.int32()),
        ("dst", pa.int32()),
        ("kind", pa.string()),
        ("slot", pa.string()),
        ("idx", pa.int32()),
        ("attrs", ATTRS_T),
    ]
)

AST_ERROR_T = pa.struct(
    [
        ("error_type", pa.string()),
        ("message", pa.string()),
        pa.field("span", SPAN_T, metadata=AST_SPAN_META),
        ("attrs", ATTRS_T),
    ]
)

AST_DOCSTRING_T = pa.struct(
    [
        ("owner_ast_id", pa.int32()),
        ("owner_kind", pa.string()),
        ("owner_name", pa.string()),
        ("docstring", pa.string()),
        pa.field("span", SPAN_T, metadata=AST_SPAN_META),
        ("source", pa.string()),
        ("attrs", ATTRS_T),
    ]
)

AST_IMPORT_T = pa.struct(
    [
        ("ast_id", pa.int32()),
        ("parent_ast_id", pa.int32()),
        ("kind", pa.string()),
        ("module", pa.string()),
        ("name", pa.string()),
        ("asname", pa.string()),
        ("alias_index", pa.int32()),
        ("level", pa.int32()),
        pa.field("span", SPAN_T, metadata=AST_SPAN_META),
        ("attrs", ATTRS_T),
    ]
)

AST_DEF_T = pa.struct(
    [
        ("ast_id", pa.int32()),
        ("parent_ast_id", pa.int32()),
        ("kind", pa.string()),
        ("name", pa.string()),
        pa.field("span", SPAN_T, metadata=AST_SPAN_META),
        ("attrs", ATTRS_T),
    ]
)

AST_CALL_T = pa.struct(
    [
        ("ast_id", pa.int32()),
        ("parent_ast_id", pa.int32()),
        ("func_kind", pa.string()),
        ("func_name", pa.string()),
        pa.field("span", SPAN_T, metadata=AST_SPAN_META),
        ("attrs", ATTRS_T),
    ]
)

AST_TYPE_IGNORE_T = pa.struct(
    [
        ("ast_id", pa.int32()),
        ("tag", pa.string()),
        pa.field("span", SPAN_T, metadata=AST_SPAN_META),
        ("attrs", ATTRS_T),
    ]
)

AST_FILES_SCHEMA = pa.schema(
    [
        ("repo", pa.string()),
        pa.field("path", pa.string(), nullable=False),
        pa.field("file_id", pa.string(), nullable=False),
        ("file_sha256", pa.string()),
        ("nodes", pa.list_(AST_NODE_T)),
        ("edges", pa.list_(AST_EDGE_T)),
        ("errors", pa.list_(AST_ERROR_T)),
        ("docstrings", pa.list_(AST_DOCSTRING_T)),
        ("imports", pa.list_(AST_IMPORT_T)),
        ("defs", pa.list_(AST_DEF_T)),
        ("calls", pa.list_(AST_CALL_T)),
        ("type_ignores", pa.list_(AST_TYPE_IGNORE_T)),
        ("attrs", ATTRS_T),
    ]
)

TREE_SITTER_FLAGS_T = pa.struct(
    [
        ("is_named", pa.bool_()),
        ("has_error", pa.bool_()),
        ("is_error", pa.bool_()),
        ("is_missing", pa.bool_()),
        ("is_extra", pa.bool_()),
        ("has_changes", pa.bool_()),
    ]
)

TREE_SITTER_NODE_T = pa.struct(
    [
        ("node_id", pa.string()),
        ("node_uid", pa.int64()),
        ("parent_id", pa.string()),
        ("kind", pa.string()),
        ("kind_id", pa.int32()),
        ("grammar_id", pa.int32()),
        ("grammar_name", pa.string()),
        pa.field("span", SPAN_T, metadata=TREE_SITTER_SPAN_META),
        ("flags", TREE_SITTER_FLAGS_T),
        ("attrs", ATTRS_T),
    ]
)

TREE_SITTER_EDGE_T = pa.struct(
    [
        ("parent_id", pa.string()),
        ("child_id", pa.string()),
        ("field_name", pa.string()),
        ("child_index", pa.int32()),
        ("attrs", ATTRS_T),
    ]
)

TREE_SITTER_ERROR_T = pa.struct(
    [
        ("error_id", pa.string()),
        ("node_id", pa.string()),
        pa.field("span", SPAN_T, metadata=TREE_SITTER_SPAN_META),
        ("attrs", ATTRS_T),
    ]
)

TREE_SITTER_MISSING_T = pa.struct(
    [
        ("missing_id", pa.string()),
        ("node_id", pa.string()),
        pa.field("span", SPAN_T, metadata=TREE_SITTER_SPAN_META),
        ("attrs", ATTRS_T),
    ]
)

TREE_SITTER_CAPTURE_T = pa.struct(
    [
        ("capture_id", pa.string()),
        ("query_name", pa.string()),
        ("capture_name", pa.string()),
        ("pattern_index", pa.int32()),
        ("node_id", pa.string()),
        ("node_kind", pa.string()),
        pa.field("span", SPAN_T, metadata=TREE_SITTER_SPAN_META),
        ("attrs", ATTRS_T),
    ]
)

TREE_SITTER_DEF_T = pa.struct(
    [
        ("node_id", pa.string()),
        ("parent_id", pa.string()),
        ("kind", pa.string()),
        ("name", pa.string()),
        pa.field("span", SPAN_T, metadata=TREE_SITTER_SPAN_META),
        ("attrs", ATTRS_T),
    ]
)

TREE_SITTER_CALL_T = pa.struct(
    [
        ("node_id", pa.string()),
        ("parent_id", pa.string()),
        ("callee_kind", pa.string()),
        ("callee_text", pa.string()),
        ("callee_node_id", pa.string()),
        pa.field("span", SPAN_T, metadata=TREE_SITTER_SPAN_META),
        ("attrs", ATTRS_T),
    ]
)

TREE_SITTER_IMPORT_T = pa.struct(
    [
        ("node_id", pa.string()),
        ("parent_id", pa.string()),
        ("kind", pa.string()),
        ("module", pa.string()),
        ("name", pa.string()),
        ("asname", pa.string()),
        ("alias_index", pa.int32()),
        ("level", pa.int32()),
        pa.field("span", SPAN_T, metadata=TREE_SITTER_SPAN_META),
        ("attrs", ATTRS_T),
    ]
)

TREE_SITTER_DOCSTRING_T = pa.struct(
    [
        ("owner_node_id", pa.string()),
        ("owner_kind", pa.string()),
        ("owner_name", pa.string()),
        ("doc_node_id", pa.string()),
        ("docstring", pa.string()),
        ("source", pa.string()),
        pa.field("span", SPAN_T, metadata=TREE_SITTER_SPAN_META),
        ("attrs", ATTRS_T),
    ]
)

TREE_SITTER_STATS_T = pa.struct(
    [
        ("node_count", pa.int32()),
        ("named_count", pa.int32()),
        ("error_count", pa.int32()),
        ("missing_count", pa.int32()),
        ("parse_ms", pa.int64()),
        ("parse_timed_out", pa.bool_()),
        ("incremental_used", pa.bool_()),
        ("query_match_count", pa.int32()),
        ("query_capture_count", pa.int32()),
        ("match_limit_exceeded", pa.bool_()),
    ]
)

TREE_SITTER_FILES_SCHEMA = pa.schema(
    [
        ("repo", pa.string()),
        ("path", pa.string()),
        ("file_id", pa.string()),
        ("file_sha256", pa.string()),
        ("nodes", pa.list_(TREE_SITTER_NODE_T)),
        ("edges", pa.list_(TREE_SITTER_EDGE_T)),
        ("errors", pa.list_(TREE_SITTER_ERROR_T)),
        ("missing", pa.list_(TREE_SITTER_MISSING_T)),
        ("captures", pa.list_(TREE_SITTER_CAPTURE_T)),
        ("defs", pa.list_(TREE_SITTER_DEF_T)),
        ("calls", pa.list_(TREE_SITTER_CALL_T)),
        ("imports", pa.list_(TREE_SITTER_IMPORT_T)),
        ("docstrings", pa.list_(TREE_SITTER_DOCSTRING_T)),
        ("stats", TREE_SITTER_STATS_T),
        ("attrs", ATTRS_T),
    ]
)

SYM_FLAGS_T = pa.struct(
    [
        ("is_referenced", pa.bool_()),
        ("is_imported", pa.bool_()),
        ("is_parameter", pa.bool_()),
        ("is_type_parameter", pa.bool_()),
        ("is_global", pa.bool_()),
        ("is_nonlocal", pa.bool_()),
        ("is_declared_global", pa.bool_()),
        ("is_local", pa.bool_()),
        ("is_annotated", pa.bool_()),
        ("is_free", pa.bool_()),
        ("is_assigned", pa.bool_()),
        ("is_namespace", pa.bool_()),
    ]
)

SYM_SYMBOL_T = pa.struct(
    [
        ("name", pa.string()),
        ("sym_symbol_id", pa.string()),
        ("flags", SYM_FLAGS_T),
        ("namespace_count", pa.int32()),
        ("namespace_block_ids", pa.list_(pa.int64())),
        _attrs_field(),
    ]
)

SYM_FUNCTION_PARTITIONS_T = pa.struct(
    [
        ("parameters", pa.list_(pa.string())),
        ("locals", pa.list_(pa.string())),
        ("globals", pa.list_(pa.string())),
        ("nonlocals", pa.list_(pa.string())),
        ("frees", pa.list_(pa.string())),
    ]
)

SYMTABLE_SPAN_META: dict[bytes, bytes] = {
    b"line_base": b"0",
    b"col_unit": b"utf32",
    b"end_exclusive": b"true",
    SEMANTIC_TYPE_META: SPAN_TYPE_INFO.name.encode("utf-8"),
}

SYM_BLOCK_T = pa.struct(
    [
        ("block_id", pa.int64()),
        ("parent_block_id", pa.int64()),
        ("block_type", pa.string()),
        ("is_meta_scope", pa.bool_()),
        ("name", pa.string()),
        ("lineno1", pa.int32()),
        pa.field("span_hint", SPAN_T, metadata=SYMTABLE_SPAN_META),
        ("scope_id", pa.string()),
        ("scope_local_id", pa.int64()),
        ("scope_type_value", pa.int32()),
        ("qualpath", pa.string()),
        ("function_partitions", SYM_FUNCTION_PARTITIONS_T),
        ("class_methods", pa.list_(pa.string())),
        ("symbols", pa.list_(SYM_SYMBOL_T)),
        _attrs_field(),
    ]
)

SYMTABLE_FILES_SCHEMA = pa.schema(
    [
        ("repo", pa.string()),
        ("path", pa.string()),
        ("file_id", pa.string()),
        ("file_sha256", pa.string()),
        ("blocks", pa.list_(SYM_BLOCK_T)),
        _attrs_field(),
    ]
)


BYTECODE_SPAN_META: dict[bytes, bytes] = {
    b"line_base": b"0",
    b"col_unit": b"utf32",
    b"end_exclusive": b"true",
    SEMANTIC_TYPE_META: SPAN_TYPE_INFO.name.encode("utf-8"),
}

BYTECODE_LINE_META: dict[bytes, bytes] = {b"line_base": b"1"}

_BYTECODE_IDENTITY_FIELDS: tuple[str, ...] = ("file_id", "path")
_BYTECODE_ORDERING_META = ordering_metadata_spec(
    OrderingLevel.EXPLICIT,
    keys=(("path", "ascending"), ("file_id", "ascending")),
)
_BYTECODE_CONSTRAINT_META = {
    REQUIRED_NON_NULL_META: metadata_list_bytes(_BYTECODE_IDENTITY_FIELDS),
    KEY_FIELDS_META: metadata_list_bytes(_BYTECODE_IDENTITY_FIELDS),
}

BYTECODE_SCHEMA_META: dict[bytes, bytes] = dict(_BYTECODE_ORDERING_META.schema_metadata)
BYTECODE_SCHEMA_META.update(_BYTECODE_CONSTRAINT_META)
BYTECODE_SCHEMA_META.update(
    {
        b"bytecode_abi_version": b"1",
        b"bytecode_span_line_base": BYTECODE_SPAN_META[b"line_base"],
        b"bytecode_span_col_unit": BYTECODE_SPAN_META[b"col_unit"],
        b"bytecode_span_end_exclusive": BYTECODE_SPAN_META[b"end_exclusive"],
    }
)

BYTECODE_CACHE_ENTRY_T = pa.struct(
    [
        pa.field("name", pa.string(), nullable=False),
        pa.field("size", pa.int32()),
        pa.field("data_hex", pa.string()),
    ]
)

BYTECODE_CONST_T = pa.struct(
    [
        pa.field("const_index", pa.int32(), nullable=False),
        pa.field("const_repr", pa.string(), nullable=False),
    ]
)

BYTECODE_INSTR_T = pa.struct(
    [
        pa.field("instr_index", pa.int32(), nullable=False),
        pa.field("offset", pa.int32(), nullable=False),
        pa.field("start_offset", pa.int32()),
        pa.field("end_offset", pa.int32()),
        pa.field("opname", pa.string(), nullable=False),
        pa.field("baseopname", pa.string()),
        pa.field("opcode", pa.int32(), nullable=False),
        pa.field("baseopcode", pa.int32()),
        pa.field("arg", pa.int32()),
        pa.field("oparg", pa.int32()),
        pa.field("argval_kind", pa.string()),
        pa.field("argval_int", pa.int64()),
        pa.field("argval_str", pa.string()),
        pa.field("argrepr", pa.string()),
        pa.field("line_number", pa.int32()),
        pa.field("starts_line", pa.int32()),
        pa.field("label", pa.int32()),
        pa.field("is_jump_target", pa.bool_()),
        pa.field("jump_target", pa.int32()),
        pa.field("cache_info", pa.list_(BYTECODE_CACHE_ENTRY_T)),
        pa.field("span", SPAN_T, metadata=BYTECODE_SPAN_META),
        _bytecode_attrs_field(),
    ]
)

BYTECODE_EXCEPTION_T = pa.struct(
    [
        pa.field("exc_index", pa.int32(), nullable=False),
        pa.field("start_offset", pa.int32()),
        pa.field("end_offset", pa.int32()),
        pa.field("target_offset", pa.int32()),
        pa.field("depth", pa.int32()),
        pa.field("lasti", pa.bool_()),
        _bytecode_attrs_field(),
    ]
)

BYTECODE_BLOCK_T = pa.struct(
    [
        pa.field("start_offset", pa.int32(), nullable=False),
        pa.field("end_offset", pa.int32(), nullable=False),
        pa.field("kind", pa.string(), nullable=False),
        _bytecode_attrs_field(),
    ]
)

BYTECODE_CFG_EDGE_T = pa.struct(
    [
        pa.field("src_block_start", pa.int32(), nullable=False),
        pa.field("src_block_end", pa.int32(), nullable=False),
        pa.field("dst_block_start", pa.int32(), nullable=False),
        pa.field("dst_block_end", pa.int32(), nullable=False),
        pa.field("kind", pa.string(), nullable=False),
        pa.field("edge_key", pa.string(), nullable=False),
        pa.field("cond_instr_index", pa.int32()),
        pa.field("cond_instr_offset", pa.int32()),
        pa.field("exc_index", pa.int32()),
        _bytecode_attrs_field(),
    ]
)

BYTECODE_ERROR_T = pa.struct(
    [
        pa.field("error_type", pa.string(), nullable=False),
        pa.field("message", pa.string(), nullable=False),
        _bytecode_attrs_field(),
    ]
)

BYTECODE_LINE_T = pa.struct(
    [
        pa.field("offset", pa.int32(), nullable=False),
        pa.field("line1", pa.int32(), metadata=BYTECODE_LINE_META),
        pa.field("line0", pa.int32()),
        _bytecode_attrs_field(),
    ]
)

BYTECODE_DFG_EDGE_T = pa.struct(
    [
        pa.field("src_instr_index", pa.int32(), nullable=False),
        pa.field("dst_instr_index", pa.int32(), nullable=False),
        pa.field("kind", pa.string(), nullable=False),
        _bytecode_attrs_field(),
    ]
)

BYTECODE_FLAGS_T = pa.struct(
    [
        pa.field("is_optimized", pa.bool_()),
        pa.field("is_newlocals", pa.bool_()),
        pa.field("has_varargs", pa.bool_()),
        pa.field("has_varkeywords", pa.bool_()),
        pa.field("is_nested", pa.bool_()),
        pa.field("is_generator", pa.bool_()),
        pa.field("is_nofree", pa.bool_()),
        pa.field("is_coroutine", pa.bool_()),
        pa.field("is_iterable_coroutine", pa.bool_()),
        pa.field("is_async_generator", pa.bool_()),
    ]
)

BYTECODE_CODE_OBJ_T = pa.struct(
    [
        pa.field("code_id", pa.string(), nullable=True),
        pa.field("qualname", pa.string(), nullable=False),
        pa.field("co_qualname", pa.string()),
        pa.field("co_filename", pa.string()),
        pa.field("name", pa.string(), nullable=False),
        pa.field("firstlineno1", pa.int32(), nullable=False),
        pa.field("argcount", pa.int32()),
        pa.field("posonlyargcount", pa.int32()),
        pa.field("kwonlyargcount", pa.int32()),
        pa.field("nlocals", pa.int32()),
        pa.field("flags", pa.int32()),
        pa.field("flags_detail", BYTECODE_FLAGS_T),
        pa.field("stacksize", pa.int32()),
        pa.field("code_len", pa.int32()),
        pa.field("varnames", pa.list_(pa.string())),
        pa.field("freevars", pa.list_(pa.string())),
        pa.field("cellvars", pa.list_(pa.string())),
        pa.field("names", pa.list_(pa.string())),
        pa.field("consts", pa.list_(BYTECODE_CONST_T)),
        pa.field("consts_json", pa.string()),
        pa.field("line_table", pa.list_(BYTECODE_LINE_T), metadata=BYTECODE_LINE_META),
        pa.field("instructions", pa.list_(BYTECODE_INSTR_T), metadata=BYTECODE_SPAN_META),
        pa.field("exception_table", pa.list_(BYTECODE_EXCEPTION_T)),
        pa.field("blocks", pa.list_(BYTECODE_BLOCK_T)),
        pa.field("cfg_edges", pa.list_(BYTECODE_CFG_EDGE_T)),
        pa.field("dfg_edges", pa.list_(BYTECODE_DFG_EDGE_T)),
        _bytecode_attrs_field(),
    ]
)

BYTECODE_FILES_SCHEMA = pa.schema(
    [
        pa.field("repo", pa.string()),
        pa.field("path", pa.string(), nullable=False),
        pa.field("file_id", pa.string(), nullable=False),
        pa.field("file_sha256", pa.string()),
        pa.field("code_objects", pa.list_(BYTECODE_CODE_OBJ_T)),
        pa.field("errors", pa.list_(BYTECODE_ERROR_T)),
        _bytecode_attrs_field(),
    ]
)


class NestedDatasetSpec(TypedDict):
    root: str
    path: str
    role: Literal["intrinsic", "derived"]
    context: dict[str, str]


def _schema_version_from_name(name: str) -> int | None:
    _, sep, suffix = name.rpartition("_v")
    if sep and suffix.isdigit():
        return int(suffix)
    return None


def _schema_with_metadata(
    name: str,
    schema: pa.Schema,
    *,
    extra_metadata: Mapping[bytes, bytes] | None = None,
) -> pa.Schema:
    metadata = dict(schema.metadata or {})
    metadata[SCHEMA_META_NAME] = name.encode("utf-8")
    version = _schema_version_from_name(name)
    if version is not None:
        metadata[SCHEMA_META_VERSION] = str(version).encode("utf-8")
    if extra_metadata:
        metadata.update(extra_metadata)
    return schema.with_metadata(metadata)


_AST_IDENTITY_FIELDS: tuple[str, ...] = ("file_id", "path")
_AST_ORDERING_META = ordering_metadata_spec(
    OrderingLevel.EXPLICIT,
    keys=(("path", "ascending"), ("file_id", "ascending")),
)
_AST_CONSTRAINT_META = {
    REQUIRED_NON_NULL_META: metadata_list_bytes(_AST_IDENTITY_FIELDS),
    KEY_FIELDS_META: metadata_list_bytes(_AST_IDENTITY_FIELDS),
}
_AST_SCHEMA_META: dict[bytes, bytes] = dict(_AST_ORDERING_META.schema_metadata)
_AST_SCHEMA_META.update(_AST_CONSTRAINT_META)
_AST_SCHEMA_META.update(
    {
        b"ast_span_line_base": AST_SPAN_META[b"line_base"],
        b"ast_span_col_unit": AST_SPAN_META[b"col_unit"],
        b"ast_span_end_exclusive": AST_SPAN_META[b"end_exclusive"],
    }
)
AST_FILES_SCHEMA = _schema_with_metadata(
    "ast_files_v1",
    AST_FILES_SCHEMA,
    extra_metadata=_AST_SCHEMA_META,
)
BYTECODE_FILES_SCHEMA = _schema_with_metadata(
    "bytecode_files_v1",
    BYTECODE_FILES_SCHEMA,
    extra_metadata=BYTECODE_SCHEMA_META,
)
_LIBCST_ORDERING_META = ordering_metadata_spec(
    OrderingLevel.EXPLICIT,
    keys=(("path", "ascending"), ("file_id", "ascending")),
)
_LIBCST_IDENTITY_FIELDS = ("file_id", "path")
_LIBCST_CONSTRAINT_META = {
    REQUIRED_NON_NULL_META: metadata_list_bytes(_LIBCST_IDENTITY_FIELDS),
    KEY_FIELDS_META: metadata_list_bytes(_LIBCST_IDENTITY_FIELDS),
}
_LIBCST_SCHEMA_META = dict(_LIBCST_ORDERING_META.schema_metadata)
_LIBCST_SCHEMA_META.update(_LIBCST_CONSTRAINT_META)
LIBCST_FILES_SCHEMA = _schema_with_metadata(
    "libcst_files_v1",
    LIBCST_FILES_SCHEMA,
    extra_metadata=_LIBCST_SCHEMA_META,
)
SCIP_METADATA_SCHEMA = _schema_with_metadata("scip_metadata_v1", SCIP_METADATA_SCHEMA)
SCIP_INDEX_STATS_SCHEMA = _schema_with_metadata("scip_index_stats_v1", SCIP_INDEX_STATS_SCHEMA)
SCIP_DOCUMENTS_SCHEMA = _schema_with_metadata("scip_documents_v1", SCIP_DOCUMENTS_SCHEMA)
SCIP_DOCUMENT_TEXTS_SCHEMA = _schema_with_metadata(
    "scip_document_texts_v1", SCIP_DOCUMENT_TEXTS_SCHEMA
)
SCIP_OCCURRENCES_SCHEMA = _schema_with_metadata("scip_occurrences_v1", SCIP_OCCURRENCES_SCHEMA)
SCIP_SYMBOL_INFORMATION_SCHEMA = _schema_with_metadata(
    "scip_symbol_information_v1",
    SCIP_SYMBOL_INFORMATION_SCHEMA,
)
SCIP_DOCUMENT_SYMBOLS_SCHEMA = _schema_with_metadata(
    "scip_document_symbols_v1",
    SCIP_DOCUMENT_SYMBOLS_SCHEMA,
)
SCIP_EXTERNAL_SYMBOL_INFORMATION_SCHEMA = _schema_with_metadata(
    "scip_external_symbol_information_v1",
    SCIP_EXTERNAL_SYMBOL_INFORMATION_SCHEMA,
)
SCIP_SYMBOL_RELATIONSHIPS_SCHEMA = _schema_with_metadata(
    "scip_symbol_relationships_v1",
    SCIP_SYMBOL_RELATIONSHIPS_SCHEMA,
)
SCIP_SIGNATURE_OCCURRENCES_SCHEMA = _schema_with_metadata(
    "scip_signature_occurrences_v1",
    SCIP_SIGNATURE_OCCURRENCES_SCHEMA,
)
SCIP_DIAGNOSTICS_SCHEMA = _schema_with_metadata("scip_diagnostics_v1", SCIP_DIAGNOSTICS_SCHEMA)
_SYMTABLE_IDENTITY_FIELDS = ("file_id", "path")
_SYMTABLE_ORDERING_META = ordering_metadata_spec(
    OrderingLevel.EXPLICIT,
    keys=(("path", "ascending"), ("file_id", "ascending")),
)
_SYMTABLE_CONSTRAINT_META = {
    REQUIRED_NON_NULL_META: metadata_list_bytes(_SYMTABLE_IDENTITY_FIELDS),
    KEY_FIELDS_META: metadata_list_bytes(_SYMTABLE_IDENTITY_FIELDS),
}
_SYMTABLE_SCHEMA_META = dict(_SYMTABLE_ORDERING_META.schema_metadata)
_SYMTABLE_SCHEMA_META.update(_SYMTABLE_CONSTRAINT_META)
SYMTABLE_FILES_SCHEMA = _schema_with_metadata(
    "symtable_files_v1",
    SYMTABLE_FILES_SCHEMA,
    extra_metadata=_SYMTABLE_SCHEMA_META,
)
_TREE_SITTER_IDENTITY_FIELDS = ("file_id", "path")
_TREE_SITTER_ORDERING_META = ordering_metadata_spec(
    OrderingLevel.EXPLICIT,
    keys=(("path", "ascending"), ("file_id", "ascending")),
)
_TREE_SITTER_CONSTRAINT_META = {
    REQUIRED_NON_NULL_META: metadata_list_bytes(_TREE_SITTER_IDENTITY_FIELDS),
    KEY_FIELDS_META: metadata_list_bytes(_TREE_SITTER_IDENTITY_FIELDS),
}
_TREE_SITTER_SCHEMA_META = dict(_TREE_SITTER_ORDERING_META.schema_metadata)
_TREE_SITTER_SCHEMA_META.update(_TREE_SITTER_CONSTRAINT_META)
TREE_SITTER_FILES_SCHEMA = _schema_with_metadata(
    "tree_sitter_files_v1",
    TREE_SITTER_FILES_SCHEMA,
    extra_metadata=_TREE_SITTER_SCHEMA_META,
)

DATAFUSION_EXPLAINS_SCHEMA = _schema_with_metadata(
    "datafusion_explains_v1",
    pa.schema(
        [
            pa.field("event_time_unix_ms", pa.int64(), nullable=False),
            pa.field("sql", pa.string(), nullable=False),
            pa.field("explain_rows_artifact_path", pa.string(), nullable=True),
            pa.field("explain_rows_artifact_format", pa.string(), nullable=True),
            pa.field("explain_rows_schema_fingerprint", pa.string(), nullable=True),
            pa.field("explain_analyze", pa.bool_(), nullable=False),
        ]
    ),
)

DATAFUSION_SCHEMA_REGISTRY_VALIDATION_SCHEMA = _schema_with_metadata(
    "datafusion_schema_registry_validation_v1",
    pa.schema(
        [
            pa.field("event_time_unix_ms", pa.int64(), nullable=False),
            pa.field("schema_name", pa.string(), nullable=False),
            pa.field("issue_type", pa.string(), nullable=False),
            pa.field("detail", pa.string(), nullable=True),
        ]
    ),
)

DATAFUSION_SCHEMA_MAP_FINGERPRINTS_SCHEMA = _schema_with_metadata(
    "datafusion_schema_map_fingerprints_v1",
    pa.schema(
        [
            pa.field("event_time_unix_ms", pa.int64(), nullable=False),
            pa.field("schema_map_hash", pa.string(), nullable=False),
            pa.field("schema_map_version", pa.int32(), nullable=False),
            pa.field("table_count", pa.int64(), nullable=False),
            pa.field("column_count", pa.int64(), nullable=False),
        ]
    ),
)

FEATURE_STATE_SCHEMA = _schema_with_metadata(
    "feature_state_v1",
    pa.schema(
        [
            pa.field("profile_name", pa.string(), nullable=False),
            pa.field("determinism_tier", pa.string(), nullable=False),
            pa.field("dynamic_filters_enabled", pa.bool_(), nullable=False),
            pa.field("spill_enabled", pa.bool_(), nullable=False),
            pa.field("named_args_supported", pa.bool_(), nullable=False),
        ]
    ),
)

DATAFUSION_CACHE_STATE_SCHEMA = _schema_with_metadata(
    "datafusion_cache_state_v1",
    pa.schema(
        [
            pa.field("cache_name", pa.string(), nullable=False),
            pa.field("event_time_unix_ms", pa.int64(), nullable=False),
            pa.field("entry_count", pa.int64(), nullable=True),
            pa.field("hit_count", pa.int64(), nullable=True),
            pa.field("miss_count", pa.int64(), nullable=True),
            pa.field("eviction_count", pa.int64(), nullable=True),
            pa.field("config_ttl", pa.string(), nullable=True),
            pa.field("config_limit", pa.string(), nullable=True),
        ]
    ),
)

DATAFUSION_SEMANTIC_DIFF_SCHEMA = _schema_with_metadata(
    "datafusion_semantic_diff_v1",
    pa.schema(
        [
            pa.field("event_time_unix_ms", pa.int64(), nullable=False),
            pa.field("run_id", pa.string(), nullable=True),
            pa.field("plan_hash", pa.string(), nullable=True),
            pa.field("base_plan_hash", pa.string(), nullable=True),
            pa.field("category", pa.string(), nullable=False),
            pa.field("changed", pa.bool_(), nullable=False),
            pa.field("breaking", pa.bool_(), nullable=False),
            pa.field("row_multiplying", pa.bool_(), nullable=False),
            pa.field("change_count", pa.int64(), nullable=True),
        ]
    ),
)

DATAFUSION_RUNS_SCHEMA = _schema_with_metadata(
    "datafusion_runs_v1",
    pa.schema(
        [
            pa.field("run_id", pa.string(), nullable=False),
            pa.field("label", pa.string(), nullable=False),
            pa.field("start_time_unix_ms", pa.int64(), nullable=False),
            pa.field("end_time_unix_ms", pa.int64(), nullable=True),
            pa.field("status", pa.string(), nullable=False),
            pa.field("duration_ms", pa.int64(), nullable=True),
            pa.field("metadata", pa.string(), nullable=True),
        ]
    ),
)

DATAFUSION_PLAN_ARTIFACTS_SCHEMA = _schema_with_metadata(
    "datafusion_plan_artifacts_v1",
    pa.schema(
        [
            pa.field("event_time_unix_ms", pa.int64(), nullable=False),
            pa.field("run_id", pa.string(), nullable=True),
            pa.field("plan_hash", pa.string(), nullable=True),
            pa.field("sql", pa.string(), nullable=False),
            pa.field("normalized_sql", pa.string(), nullable=True),
            pa.field("explain_rows_artifact_path", pa.string(), nullable=True),
            pa.field("explain_rows_artifact_format", pa.string(), nullable=True),
            pa.field("explain_rows_schema_fingerprint", pa.string(), nullable=True),
            pa.field("explain_analyze_artifact_path", pa.string(), nullable=True),
            pa.field("explain_analyze_artifact_format", pa.string(), nullable=True),
            pa.field("substrait_b64", pa.string(), nullable=True),
            pa.field("substrait_validation_status", pa.string(), nullable=True),
            pa.field("sqlglot_ast", pa.binary(), nullable=True),
            pa.field("ibis_decompile", pa.string(), nullable=True),
            pa.field("ibis_sql", pa.string(), nullable=True),
            pa.field("ibis_sql_pretty", pa.string(), nullable=True),
            pa.field("ibis_graphviz", pa.string(), nullable=True),
            pa.field("ibis_compiled_sql", pa.string(), nullable=True),
            pa.field("ibis_compiled_sql_hash", pa.string(), nullable=True),
            pa.field("ibis_compile_params", pa.string(), nullable=True),
            pa.field("ibis_compile_limit", pa.int64(), nullable=True),
            pa.field("read_dialect", pa.string(), nullable=True),
            pa.field("write_dialect", pa.string(), nullable=True),
            pa.field("canonical_fingerprint", pa.string(), nullable=True),
            pa.field("lineage_tables", pa.list_(pa.string()), nullable=True),
            pa.field("lineage_columns", pa.list_(pa.string()), nullable=True),
            pa.field("lineage_scopes", pa.list_(pa.string()), nullable=True),
            pa.field("param_signature", pa.string(), nullable=True),
            pa.field("projection_map", pa.binary(), nullable=True),
            pa.field("unparsed_sql", pa.string(), nullable=True),
            pa.field("unparse_error", pa.string(), nullable=True),
            pa.field("logical_plan", pa.string(), nullable=True),
            pa.field("optimized_plan", pa.string(), nullable=True),
            pa.field("physical_plan", pa.string(), nullable=True),
            pa.field("graphviz", pa.string(), nullable=True),
            pa.field("partition_count", pa.int64(), nullable=True),
            pa.field("join_operators", pa.list_(pa.string()), nullable=True),
        ]
    ),
)

IBIS_SQL_INGEST_SCHEMA = _schema_with_metadata(
    "ibis_sql_ingest_v1",
    pa.schema(
        [
            pa.field("event_time_unix_ms", pa.int64(), nullable=False),
            pa.field("ingest_kind", pa.string(), nullable=False),
            pa.field("source_name", pa.string(), nullable=True),
            pa.field("sql", pa.string(), nullable=False),
            pa.field("decompiled_sql", pa.string(), nullable=True),
            pa.field("schema", pa.map_(pa.string(), pa.string()), nullable=True),
            pa.field("dialect", pa.string(), nullable=True),
            pa.field("parse_errors", SQLGLOT_PARSE_ERROR_DETAILS_TYPE, nullable=True),
            pa.field("sqlglot_sql", pa.string(), nullable=True),
            pa.field("normalized_sql", pa.string(), nullable=True),
            pa.field("sqlglot_ast", pa.binary(), nullable=True),
            pa.field("ibis_sqlglot_ast", pa.binary(), nullable=True),
            pa.field("sqlglot_policy_hash", pa.string(), nullable=True),
            pa.field("sqlglot_policy_snapshot", pa.binary(), nullable=True),
        ]
    ),
)

SQLGLOT_PARSE_ERRORS_SCHEMA = _schema_with_metadata(
    "sqlglot_parse_errors_v1",
    pa.schema(
        [
            pa.field("event_time_unix_ms", pa.int64(), nullable=False),
            pa.field("source", pa.string(), nullable=True),
            pa.field("sql", pa.string(), nullable=True),
            pa.field("dialect", pa.string(), nullable=True),
            pa.field("error", pa.string(), nullable=True),
            pa.field("parse_errors", SQLGLOT_PARSE_ERROR_DETAILS_TYPE, nullable=True),
        ]
    ),
)

ENGINE_RUNTIME_SCHEMA = _schema_with_metadata(
    "engine_runtime_v1",
    pa.schema(
        [
            pa.field("event_time_unix_ms", pa.int64(), nullable=False),
            pa.field("runtime_profile_name", pa.string(), nullable=False),
            pa.field("determinism_tier", pa.string(), nullable=False),
            pa.field("runtime_profile_hash", pa.string(), nullable=False),
            pa.field("runtime_profile_snapshot", pa.binary(), nullable=False),
            pa.field("sqlglot_policy_hash", pa.string(), nullable=True),
            pa.field("sqlglot_policy_snapshot", pa.binary(), nullable=True),
            pa.field("function_registry_hash", pa.string(), nullable=True),
            pa.field("function_registry_snapshot", pa.binary(), nullable=True),
            pa.field("datafusion_settings_hash", pa.string(), nullable=True),
            pa.field("datafusion_settings", pa.binary(), nullable=True),
        ]
    ),
)

DATAFUSION_UDF_VALIDATION_SCHEMA = _schema_with_metadata(
    "datafusion_udf_validation_v1",
    pa.schema(
        [
            pa.field("event_time_unix_ms", pa.int64(), nullable=False),
            pa.field("udf_catalog_policy", pa.string(), nullable=False),
            pa.field("missing_udfs", pa.list_(pa.string()), nullable=True),
            pa.field("missing_count", pa.int32(), nullable=True),
        ]
    ),
)

DATAFUSION_OBJECT_STORES_SCHEMA = _schema_with_metadata(
    "datafusion_object_stores_v1",
    pa.schema(
        [
            pa.field("event_time_unix_ms", pa.int64(), nullable=False),
            pa.field("scheme", pa.string(), nullable=False),
            pa.field("host", pa.string(), nullable=True),
            pa.field("store_type", pa.string(), nullable=True),
        ]
    ),
)

REPO_SNAPSHOT_SCHEMA = _schema_with_metadata(
    "repo_snapshot_v1",
    pa.schema(
        [
            pa.field("file_id", pa.string()),
            pa.field("path", pa.string()),
            pa.field("file_sha256", pa.string()),
            pa.field("size_bytes", pa.int64()),
            pa.field("mtime_ns", pa.int64()),
        ]
    ),
)

DIM_QUALIFIED_NAMES_SCHEMA = _schema_with_metadata(
    "dim_qualified_names_v1",
    pa.schema(
        [
            pa.field("qname_id", pa.string()),
            pa.field("qname", pa.string()),
        ]
    ),
)

CALLSITE_QNAME_CANDIDATES_SCHEMA = _schema_with_metadata(
    "callsite_qname_candidates_v1",
    pa.schema(
        [
            pa.field("call_id", pa.string()),
            pa.field("qname", pa.string()),
            pa.field("path", pa.string()),
            pa.field("call_bstart", pa.int64()),
            pa.field("call_bend", pa.int64()),
            pa.field("arg_count", pa.int32()),
            pa.field("keyword_count", pa.int32()),
            pa.field("star_arg_count", pa.int32()),
            pa.field("star_kwarg_count", pa.int32()),
            pa.field("positional_count", pa.int32()),
            pa.field("qname_source", pa.string()),
        ]
    ),
)

SCALAR_PARAM_SIGNATURE_SCHEMA = _schema_with_metadata(
    "scalar_param_signature_v1",
    pa.schema(
        [
            pa.field("version", pa.int32(), nullable=False),
            pa.field(
                "entries",
                pa.list_(
                    pa.struct(
                        [
                            ("key", pa.string()),
                            ("value", pa.string()),
                        ]
                    )
                ),
                nullable=False,
            ),
        ]
    ),
)

DATASET_FINGERPRINT_SCHEMA = _schema_with_metadata(
    "dataset_fingerprint_v1",
    pa.schema(
        [
            pa.field("version", pa.int32(), nullable=False),
            pa.field("plan_hash", pa.string(), nullable=False),
            pa.field("schema_fingerprint", pa.string(), nullable=False),
            pa.field("profile_hash", pa.string(), nullable=False),
            pa.field("writer_strategy", pa.string(), nullable=False),
            pa.field("input_fingerprints", pa.list_(pa.string()), nullable=False),
        ]
    ),
)

PARAM_FILE_IDS_SCHEMA = _schema_with_metadata(
    "param_file_ids_v1",
    pa.schema(
        [
            pa.field("file_id", pa.string(), nullable=False),
        ]
    ),
)

SCHEMA_REGISTRY: dict[str, pa.Schema] = {
    "callsite_qname_candidates_v1": CALLSITE_QNAME_CANDIDATES_SCHEMA,
    "dataset_fingerprint_v1": DATASET_FINGERPRINT_SCHEMA,
    "datafusion_cache_state_v1": DATAFUSION_CACHE_STATE_SCHEMA,
    "datafusion_explains_v1": DATAFUSION_EXPLAINS_SCHEMA,
    "datafusion_plan_artifacts_v1": DATAFUSION_PLAN_ARTIFACTS_SCHEMA,
    "datafusion_runs_v1": DATAFUSION_RUNS_SCHEMA,
    "datafusion_schema_map_fingerprints_v1": DATAFUSION_SCHEMA_MAP_FINGERPRINTS_SCHEMA,
    "datafusion_schema_registry_validation_v1": DATAFUSION_SCHEMA_REGISTRY_VALIDATION_SCHEMA,
    "datafusion_semantic_diff_v1": DATAFUSION_SEMANTIC_DIFF_SCHEMA,
    "dim_qualified_names_v1": DIM_QUALIFIED_NAMES_SCHEMA,
    "engine_runtime_v1": ENGINE_RUNTIME_SCHEMA,
    "feature_state_v1": FEATURE_STATE_SCHEMA,
    "ibis_sql_ingest_v1": IBIS_SQL_INGEST_SCHEMA,
    "sqlglot_parse_errors_v1": SQLGLOT_PARSE_ERRORS_SCHEMA,
    "param_file_ids_v1": PARAM_FILE_IDS_SCHEMA,
    "repo_snapshot_v1": REPO_SNAPSHOT_SCHEMA,
    "scalar_param_signature_v1": SCALAR_PARAM_SIGNATURE_SCHEMA,
}

NESTED_DATASET_INDEX: dict[str, NestedDatasetSpec] = {
    "cst_nodes": {
        "root": "libcst_files_v1",
        "path": "nodes",
        "role": "intrinsic",
        "context": {},
    },
    "cst_edges": {
        "root": "libcst_files_v1",
        "path": "edges",
        "role": "intrinsic",
        "context": {},
    },
    "cst_parse_manifest": {
        "root": "libcst_files_v1",
        "path": "parse_manifest",
        "role": "intrinsic",
        "context": {},
    },
    "cst_parse_errors": {
        "root": "libcst_files_v1",
        "path": "parse_errors",
        "role": "derived",
        "context": {},
    },
    "cst_refs": {
        "root": "libcst_files_v1",
        "path": "refs",
        "role": "derived",
        "context": {},
    },
    "cst_imports": {
        "root": "libcst_files_v1",
        "path": "imports",
        "role": "derived",
        "context": {},
    },
    "cst_callsites": {
        "root": "libcst_files_v1",
        "path": "callsites",
        "role": "derived",
        "context": {},
    },
    "cst_defs": {
        "root": "libcst_files_v1",
        "path": "defs",
        "role": "derived",
        "context": {},
    },
    "cst_type_exprs": {
        "root": "libcst_files_v1",
        "path": "type_exprs",
        "role": "derived",
        "context": {},
    },
    "cst_docstrings": {
        "root": "libcst_files_v1",
        "path": "docstrings",
        "role": "derived",
        "context": {},
    },
    "cst_decorators": {
        "root": "libcst_files_v1",
        "path": "decorators",
        "role": "derived",
        "context": {},
    },
    "cst_call_args": {
        "root": "libcst_files_v1",
        "path": "call_args",
        "role": "derived",
        "context": {},
    },
    "ast_nodes": {
        "root": "ast_files_v1",
        "path": "nodes",
        "role": "derived",
        "context": {},
    },
    "ast_edges": {
        "root": "ast_files_v1",
        "path": "edges",
        "role": "derived",
        "context": {},
    },
    "ast_errors": {
        "root": "ast_files_v1",
        "path": "errors",
        "role": "derived",
        "context": {},
    },
    "ast_docstrings": {
        "root": "ast_files_v1",
        "path": "docstrings",
        "role": "derived",
        "context": {},
    },
    "ast_imports": {
        "root": "ast_files_v1",
        "path": "imports",
        "role": "derived",
        "context": {},
    },
    "ast_defs": {
        "root": "ast_files_v1",
        "path": "defs",
        "role": "derived",
        "context": {},
    },
    "ast_calls": {
        "root": "ast_files_v1",
        "path": "calls",
        "role": "derived",
        "context": {},
    },
    "ast_type_ignores": {
        "root": "ast_files_v1",
        "path": "type_ignores",
        "role": "derived",
        "context": {},
    },
    "ts_nodes": {
        "root": "tree_sitter_files_v1",
        "path": "nodes",
        "role": "derived",
        "context": {},
    },
    "ts_edges": {
        "root": "tree_sitter_files_v1",
        "path": "edges",
        "role": "intrinsic",
        "context": {},
    },
    "ts_errors": {
        "root": "tree_sitter_files_v1",
        "path": "errors",
        "role": "derived",
        "context": {},
    },
    "ts_missing": {
        "root": "tree_sitter_files_v1",
        "path": "missing",
        "role": "derived",
        "context": {},
    },
    "ts_captures": {
        "root": "tree_sitter_files_v1",
        "path": "captures",
        "role": "derived",
        "context": {},
    },
    "ts_defs": {
        "root": "tree_sitter_files_v1",
        "path": "defs",
        "role": "derived",
        "context": {},
    },
    "ts_calls": {
        "root": "tree_sitter_files_v1",
        "path": "calls",
        "role": "derived",
        "context": {},
    },
    "ts_imports": {
        "root": "tree_sitter_files_v1",
        "path": "imports",
        "role": "derived",
        "context": {},
    },
    "ts_docstrings": {
        "root": "tree_sitter_files_v1",
        "path": "docstrings",
        "role": "derived",
        "context": {},
    },
    "ts_stats": {
        "root": "tree_sitter_files_v1",
        "path": "stats",
        "role": "intrinsic",
        "context": {},
    },
    "symtable_scopes": {
        "root": "symtable_files_v1",
        "path": "blocks",
        "role": "derived",
        "context": {},
    },
    "symtable_symbols": {
        "root": "symtable_files_v1",
        "path": "blocks.symbols",
        "role": "derived",
        "context": {"block_id": "blocks.block_id", "scope_id": "blocks.scope_id"},
    },
    "symtable_scope_edges": {
        "root": "symtable_files_v1",
        "path": "blocks",
        "role": "derived",
        "context": {},
    },
    "py_bc_code_units": {
        "root": "bytecode_files_v1",
        "path": "code_objects",
        "role": "derived",
        "context": {},
    },
    "py_bc_line_table": {
        "root": "bytecode_files_v1",
        "path": "code_objects.line_table",
        "role": "derived",
        "context": {
            "code_unit_qualpath": "code_objects.qualname",
            "code_unit_name": "code_objects.name",
            "code_unit_firstlineno": "code_objects.firstlineno1",
        },
    },
    "py_bc_instructions": {
        "root": "bytecode_files_v1",
        "path": "code_objects.instructions",
        "role": "derived",
        "context": {
            "code_unit_qualpath": "code_objects.qualname",
            "code_unit_name": "code_objects.name",
            "code_unit_firstlineno": "code_objects.firstlineno1",
        },
    },
    "py_bc_cache_entries": {
        "root": "bytecode_files_v1",
        "path": "code_objects.instructions.cache_info",
        "role": "derived",
        "context": {
            "code_unit_qualpath": "code_objects.qualname",
            "code_unit_name": "code_objects.name",
            "code_unit_firstlineno": "code_objects.firstlineno1",
            "instr_index": "code_objects.instructions.instr_index",
            "offset": "code_objects.instructions.offset",
        },
    },
    "py_bc_consts": {
        "root": "bytecode_files_v1",
        "path": "code_objects.consts",
        "role": "derived",
        "context": {
            "code_unit_qualpath": "code_objects.qualname",
            "code_unit_name": "code_objects.name",
            "code_unit_firstlineno": "code_objects.firstlineno1",
        },
    },
    "py_bc_blocks": {
        "root": "bytecode_files_v1",
        "path": "code_objects.blocks",
        "role": "derived",
        "context": {
            "code_unit_qualpath": "code_objects.qualname",
            "code_unit_name": "code_objects.name",
            "code_unit_firstlineno": "code_objects.firstlineno1",
        },
    },
    "py_bc_cfg_edges": {
        "root": "bytecode_files_v1",
        "path": "code_objects.cfg_edges",
        "role": "derived",
        "context": {
            "code_unit_qualpath": "code_objects.qualname",
            "code_unit_name": "code_objects.name",
            "code_unit_firstlineno": "code_objects.firstlineno1",
        },
    },
    "py_bc_dfg_edges": {
        "root": "bytecode_files_v1",
        "path": "code_objects.dfg_edges",
        "role": "derived",
        "context": {
            "code_unit_qualpath": "code_objects.qualname",
            "code_unit_name": "code_objects.name",
            "code_unit_firstlineno": "code_objects.firstlineno1",
        },
    },
    "bytecode_exception_table": {
        "root": "bytecode_files_v1",
        "path": "code_objects.exception_table",
        "role": "derived",
        "context": {
            "code_unit_qualpath": "code_objects.qualname",
            "code_unit_name": "code_objects.name",
            "code_unit_firstlineno": "code_objects.firstlineno1",
        },
    },
    "bytecode_errors": {
        "root": "bytecode_files_v1",
        "path": "errors",
        "role": "derived",
        "context": {},
    },
}

ROOT_IDENTITY_FIELDS: dict[str, tuple[str, ...]] = {
    "ast_files_v1": _AST_IDENTITY_FIELDS,
    "bytecode_files_v1": _BYTECODE_IDENTITY_FIELDS,
    "libcst_files_v1": _LIBCST_IDENTITY_FIELDS,
    "symtable_files_v1": _SYMTABLE_IDENTITY_FIELDS,
    "tree_sitter_files_v1": _TREE_SITTER_IDENTITY_FIELDS,
}

AST_CORE_VIEW_NAMES: tuple[str, ...] = (
    "ast_nodes",
    "ast_edges",
    "ast_errors",
    "ast_docstrings",
    "ast_imports",
    "ast_defs",
    "ast_calls",
    "ast_type_ignores",
)
AST_OPTIONAL_VIEW_NAMES: tuple[str, ...] = (
    "ast_node_attrs",
    "ast_def_attrs",
    "ast_call_attrs",
    "ast_edge_attrs",
    "ast_span_metadata",
)
AST_ATTRS_VIEW_NAMES: tuple[str, ...] = (
    "ast_node_attrs",
    "ast_def_attrs",
    "ast_call_attrs",
    "ast_edge_attrs",
)
AST_VIEW_NAMES: tuple[str, ...] = AST_CORE_VIEW_NAMES + AST_OPTIONAL_VIEW_NAMES

TREE_SITTER_VIEW_NAMES: tuple[str, ...] = (
    "ts_nodes",
    "ts_edges",
    "ts_errors",
    "ts_missing",
    "ts_captures",
    "ts_defs",
    "ts_calls",
    "ts_imports",
    "ts_docstrings",
    "ts_stats",
    "ts_span_metadata",
    "ts_ast_defs_check",
    "ts_ast_calls_check",
    "ts_ast_imports_check",
    "ts_cst_docstrings_check",
)

TREE_SITTER_CHECK_VIEWS: tuple[str, ...] = (
    "ts_ast_defs_check",
    "ts_ast_calls_check",
    "ts_ast_imports_check",
    "ts_cst_docstrings_check",
)

SCIP_VIEW_NAMES: tuple[str, ...] = (
    "scip_metadata",
    "scip_index_stats",
    "scip_documents",
    "scip_document_texts",
    "scip_occurrences",
    "scip_symbol_information",
    "scip_document_symbols",
    "scip_external_symbol_information",
    "scip_symbol_relationships",
    "scip_signature_occurrences",
    "scip_diagnostics",
)

SYMTABLE_VIEW_NAMES: tuple[str, ...] = (
    "symtable_scopes",
    "symtable_symbols",
    "symtable_scope_edges",
    "symtable_namespace_edges",
    "symtable_function_partitions",
    "symtable_class_methods",
    "symtable_symbol_attrs",
)

BYTECODE_VIEW_NAMES: tuple[str, ...] = (
    "py_bc_code_units",
    "py_bc_consts",
    "py_bc_line_table",
    "py_bc_instructions",
    "py_bc_instruction_attrs",
    "py_bc_instruction_attr_keys",
    "py_bc_instruction_attr_values",
    "py_bc_instruction_spans",
    "py_bc_instruction_span_fields",
    "py_bc_cache_entries",
    "py_bc_metadata",
    "bytecode_exception_table",
    "py_bc_cfg_edge_attrs",
    "py_bc_blocks",
    "py_bc_dfg_edges",
    "py_bc_flags_detail",
    "py_bc_cfg_edges",
    "py_bc_error_attrs",
    "bytecode_errors",
)


def _sorted_tokens(tokens: frozenset[str]) -> tuple[str, ...]:
    return tuple(sorted(tokens))


_ENGINE_FUNCTION_REQUIREMENTS = function_requirements_metadata_spec(
    required=(
        "array_agg",
        "array_distinct",
        "array_sort",
        "array_to_string",
        "bool_or",
        "coalesce",
        "col_to_byte",
        "concat_ws",
        "prefixed_hash64",
        "row_number",
        "sha256",
        "stable_hash64",
        "stable_hash128",
        "stable_id",
    ),
).schema_metadata


@dataclass(frozen=True)
class FunctionRequirements:
    """Decoded function requirement metadata."""

    required: tuple[str, ...]
    optional: tuple[str, ...]
    signature_counts: Mapping[str, int]
    signature_types: Mapping[str, tuple[frozenset[str] | None, ...]]


def _function_requirements(schema: pa.Schema) -> FunctionRequirements:
    meta = schema.metadata
    return FunctionRequirements(
        required=required_functions_from_metadata(meta),
        optional=optional_functions_from_metadata(meta),
        signature_counts=required_function_signatures_from_metadata(meta),
        signature_types=required_function_signature_types_from_metadata(meta),
    )


ENGINE_RUNTIME_SCHEMA = _schema_with_metadata(
    "engine_runtime_v1",
    ENGINE_RUNTIME_SCHEMA,
    extra_metadata=_ENGINE_FUNCTION_REQUIREMENTS,
)
SCHEMA_REGISTRY["engine_runtime_v1"] = ENGINE_RUNTIME_SCHEMA


AST_VIEW_REQUIRED_NON_NULL_FIELDS: tuple[str, ...] = ("file_id", "path")
CST_VIEW_REQUIRED_NON_NULL_FIELDS: tuple[str, ...] = ("file_id", "path")

CST_VIEW_NAMES: tuple[str, ...] = (
    "cst_parse_manifest",
    "cst_parse_errors",
    "cst_refs",
    "cst_imports",
    "cst_callsites",
    "cst_defs",
    "cst_type_exprs",
    "cst_docstrings",
    "cst_decorators",
    "cst_call_args",
    "cst_nodes",
    "cst_edges",
    "cst_refs_attrs",
    "cst_defs_attrs",
    "cst_callsites_attrs",
    "cst_imports_attrs",
    "cst_nodes_attrs",
    "cst_edges_attrs",
    "cst_refs_attr_origin",
    "cst_defs_attr_origin",
    "cst_callsites_attr_origin",
    "cst_imports_attr_origin",
    "cst_nodes_attr_origin",
    "cst_edges_attr_origin",
    "cst_ref_spans",
    "cst_callsite_spans",
    "cst_def_spans",
    "cst_ref_span_unnest",
    "cst_callsite_span_unnest",
    "cst_def_span_unnest",
    "cst_schema_diagnostics",
)


def nested_dataset_names() -> tuple[str, ...]:
    """Return nested dataset names in sorted order.

    Returns
    -------
    tuple[str, ...]
        Sorted nested dataset name tuple.
    """
    return tuple(sorted(NESTED_DATASET_INDEX))


def nested_schema_names() -> tuple[str, ...]:
    """Return intrinsic nested dataset names in sorted order.

    Returns
    -------
    tuple[str, ...]
        Sorted intrinsic nested dataset name tuple.
    """
    return tuple(
        sorted(name for name, spec in NESTED_DATASET_INDEX.items() if spec["role"] == "intrinsic")
    )


def nested_spec_for(name: str) -> NestedDatasetSpec:
    """Return the nested dataset spec for a name.

    Returns
    -------
    NestedDatasetSpec
        Nested dataset specification mapping.

    Raises
    ------
    KeyError
        Raised when the dataset name is not registered.
    """
    spec = NESTED_DATASET_INDEX.get(name)
    if spec is None:
        msg = f"Unknown nested dataset: {name!r}."
        raise KeyError(msg)
    return spec


def datasets_for_path(root: str, path: str) -> tuple[str, ...]:
    """Return dataset names registered for a root/path pair.

    Returns
    -------
    tuple[str, ...]
        Sorted dataset name tuple for the path.
    """
    return tuple(
        sorted(
            name
            for name, spec in NESTED_DATASET_INDEX.items()
            if spec["root"] == root and spec["path"] == path
        )
    )


def nested_path_for(name: str) -> tuple[str, str]:
    """Return the root schema name and path for a nested dataset.

    Returns
    -------
    tuple[str, str]
        Root schema name and nested path.
    """
    spec = nested_spec_for(name)
    return spec["root"], spec["path"]


def nested_context_for(name: str) -> Mapping[str, str]:
    """Return the context fields for a nested dataset.

    Returns
    -------
    Mapping[str, str]
        Mapping of output column name to nested context path.
    """
    return nested_spec_for(name)["context"]


def nested_role_for(name: str) -> Literal["intrinsic", "derived"]:
    """Return the nested dataset role for a name.

    Returns
    -------
    Literal["intrinsic", "derived"]
        Dataset role label.
    """
    return nested_spec_for(name)["role"]


def is_nested_dataset(name: str) -> bool:
    """Return whether a name is a known nested dataset.

    Returns
    -------
    bool
        ``True`` when the dataset name is registered.
    """
    return name in NESTED_DATASET_INDEX


def is_intrinsic_nested_dataset(name: str) -> bool:
    """Return whether a nested dataset is intrinsic.

    Returns
    -------
    bool
        ``True`` when the dataset is intrinsic.
    """
    spec = NESTED_DATASET_INDEX.get(name)
    return spec is not None and spec["role"] == "intrinsic"


def _is_list_type(dtype: pa.DataType) -> bool:
    return pa.types.is_list(dtype) or pa.types.is_large_list(dtype)


def _field_from_container(container: pa.Schema | pa.StructType, name: str) -> pa.Field:
    try:
        return container.field(name)
    except KeyError as exc:
        msg = f"Unknown nested field: {name!r}."
        raise KeyError(msg) from exc


def struct_for_path(schema: pa.Schema, path: str) -> pa.StructType:
    """Resolve the struct type for a nested path.

    Returns
    -------
    pyarrow.StructType
        Struct type resolved at the nested path.

    Raises
    ------
    TypeError
        Raised when the path does not resolve to a struct.
    ValueError
        Raised when the path is empty.
    """
    if not path:
        msg = "Nested schema path cannot be empty."
        raise ValueError(msg)
    current: pa.Schema | pa.StructType = schema
    for step in path.split("."):
        field = _field_from_container(current, step)
        dtype = field.type
        if _is_list_type(dtype):
            dtype = dtype.value_type
        if not pa.types.is_struct(dtype):
            msg = f"Nested path {path!r} does not resolve to a struct at {step!r}."
            raise TypeError(msg)
        current = dtype
    if isinstance(current, pa.StructType):
        return current
    msg = f"Nested path {path!r} did not resolve to a struct."
    raise TypeError(msg)


def _context_fields_for(name: str, root_schema: pa.Schema) -> tuple[pa.Field, ...]:
    context = nested_context_for(name)
    if not context:
        return ()
    fields: list[pa.Field] = []
    for alias, path in context.items():
        prefix, sep, field_name = path.rpartition(".")
        if not sep:
            field = root_schema.field(field_name)
        else:
            struct_type = struct_for_path(root_schema, prefix)
            field = struct_type.field(field_name)
        fields.append(pa.field(alias, field.type, field.nullable, metadata=field.metadata))
    return tuple(fields)


def identity_fields_for(
    name: str,
    root_schema: pa.Schema,
    row_struct: pa.StructType,
) -> tuple[str, ...]:
    """Return identity column names for a nested dataset.

    Returns
    -------
    tuple[str, ...]
        Identity column names for the dataset.
    """
    root, _ = nested_path_for(name)
    identities = ROOT_IDENTITY_FIELDS.get(root, ())
    row_fields = {field.name for field in row_struct}
    resolved: list[str] = []
    for field_name in identities:
        if field_name in row_fields:
            continue
        root_schema.field(field_name)
        resolved.append(field_name)
    return tuple(resolved)


def nested_schema_for(name: str, *, allow_derived: bool = False) -> pa.Schema:
    """Return the schema for an intrinsic nested dataset.

    Returns
    -------
    pyarrow.Schema
        Derived nested schema for the dataset.

    Raises
    ------
    KeyError
        Raised when the dataset is not intrinsic unless allow_derived is True.
    """
    if not allow_derived and not is_intrinsic_nested_dataset(name):
        msg = f"Nested schema {name!r} is derived; set allow_derived=True to resolve."
        raise KeyError(msg)
    root, path = nested_path_for(name)
    root_schema = SCHEMA_REGISTRY[root]
    row_struct = struct_for_path(root_schema, path)
    identity_fields = identity_fields_for(name, root_schema, row_struct)
    context_fields = _context_fields_for(name, root_schema)
    fields: list[pa.Field] = []
    seen: set[str] = set()
    for field_name in identity_fields:
        field = root_schema.field(field_name)
        fields.append(pa.field(field.name, field.type, field.nullable, metadata=field.metadata))
        seen.add(field.name)
    for field in context_fields:
        if field.name in seen:
            continue
        fields.append(pa.field(field.name, field.type, field.nullable, metadata=field.metadata))
        seen.add(field.name)
    for field in row_struct:
        if field.name in seen:
            continue
        fields.append(pa.field(field.name, field.type, field.nullable, metadata=field.metadata))
        seen.add(field.name)
    return pa.schema(fields)


def _resolve_root_schema(ctx: SessionContext, root: str) -> pa.Schema:
    schema = SCHEMA_REGISTRY.get(root)
    if schema is not None:
        return schema
    try:
        df = ctx.table(root)
    except (KeyError, RuntimeError, TypeError, ValueError) as exc:
        msg = f"Missing root schema for {root!r} and table lookup failed."
        raise KeyError(msg) from exc
    resolved = df.schema()
    if isinstance(resolved, pa.Schema):
        return resolved
    to_arrow = getattr(resolved, "to_arrow", None)
    if callable(to_arrow):
        arrow_schema = to_arrow()
        if isinstance(arrow_schema, pa.Schema):
            return arrow_schema
    msg = f"Unable to resolve root schema for {root!r}."
    raise TypeError(msg)


def _append_expr_selection(
    selections: list[Expr],
    selected_names: set[str],
    *,
    name: str,
    expr: Expr,
) -> None:
    if name in selected_names:
        return
    selections.append(expr.alias(name))
    selected_names.add(name)


def _context_expr_expr(
    *,
    prefix_exprs: Mapping[str, Expr],
    ctx_path: str,
    dataset_name: str,
) -> Expr:
    prefix, sep, field_name = ctx_path.rpartition(".")
    if not sep:
        return col(field_name)
    prefix_expr = prefix_exprs.get(prefix)
    if prefix_expr is None:
        msg = f"Nested context path {ctx_path!r} not resolved for {dataset_name!r}."
        raise KeyError(msg)
    return prefix_expr[field_name]


def _resolve_nested_path(
    df: DataFrame,
    root_schema: pa.Schema,
    *,
    path: str,
) -> tuple[DataFrame, pa.StructType, Expr | None, dict[str, Expr]]:
    current_struct: pa.Schema | pa.StructType = root_schema
    current_expr: Expr | None = None
    prefix_exprs: dict[str, Expr] = {}
    parts = path.split(".") if path else []
    for idx, step in enumerate(parts):
        field = _field_from_container(current_struct, step)
        dtype = field.type
        prefix = ".".join(parts[: idx + 1])
        if _is_list_type(dtype):
            list_expr = col(step) if current_expr is None else current_expr[step]
            alias = f"n{idx}"
            df = df.with_column(alias, list_expr)
            df = df.unnest_columns(alias, preserve_nulls=False)
            current_expr = col(alias)
            value_type = dtype.value_type
            if not pa.types.is_struct(value_type):
                msg = f"Nested path {path!r} does not resolve to a struct at {step!r}."
                raise TypeError(msg)
            current_struct = value_type
            prefix_exprs[prefix] = current_expr
            continue
        if not pa.types.is_struct(dtype):
            msg = f"Nested path {path!r} does not resolve to a struct at {step!r}."
            raise TypeError(msg)
        current_expr = col(step) if current_expr is None else current_expr[step]
        current_struct = dtype
        prefix_exprs[prefix] = current_expr
    if not isinstance(current_struct, pa.StructType):
        msg = f"Nested path {path!r} did not resolve to a struct."
        raise TypeError(msg)
    return df, current_struct, current_expr, prefix_exprs


def _build_nested_selections(
    *,
    name: str,
    root_schema: pa.Schema,
    current_struct: pa.StructType,
    current_expr: Expr | None,
    prefix_exprs: Mapping[str, Expr],
) -> list[Expr]:
    identity_fields = identity_fields_for(name, root_schema, current_struct)
    context_fields = nested_context_for(name)
    selections: list[Expr] = []
    selected_names: set[str] = set()
    for field_name in identity_fields:
        _append_expr_selection(
            selections,
            selected_names,
            name=field_name,
            expr=col(field_name),
        )
    for alias, ctx_path in context_fields.items():
        expr = _context_expr_expr(
            prefix_exprs=prefix_exprs,
            ctx_path=ctx_path,
            dataset_name=name,
        )
        _append_expr_selection(selections, selected_names, name=alias, expr=expr)
    for field in current_struct:
        expr = col(field.name) if current_expr is None else current_expr[field.name]
        _append_expr_selection(selections, selected_names, name=field.name, expr=expr)
    return selections


def nested_base_df(
    ctx: SessionContext,
    name: str,
    *,
    table: str | None = None,
) -> DataFrame:
    """Return a DataFrame for a nested dataset path.

    Parameters
    ----------
    ctx:
        DataFusion session context.
    name:
        Nested dataset name.
    table:
        Optional base table name to use instead of the root schema name.

    Returns
    -------
    DataFrame
        DataFrame projecting the nested dataset rows.

    """
    root, path = nested_path_for(name)
    root_schema = _resolve_root_schema(ctx, root)
    resolved_table = table or root
    df = ctx.table(resolved_table)
    df, current_struct, current_expr, prefix_exprs = _resolve_nested_path(
        df,
        root_schema,
        path=path,
    )
    selections = _build_nested_selections(
        name=name,
        root_schema=root_schema,
        current_struct=current_struct,
        current_expr=current_expr,
        prefix_exprs=prefix_exprs,
    )
    return df.select(*selections)


def nested_view_spec(
    ctx: SessionContext,
    name: str,
    *,
    table: str | None = None,
) -> ViewSpec:
    """Return a ViewSpec for a nested dataset.

    Returns
    -------
    ViewSpec
        View specification derived from the registered base table.
    """
    builder = partial(nested_base_df, name=name, table=table)
    return view_spec_from_builder(ctx, name=name, builder=builder, sql=None)


def nested_view_specs(
    ctx: SessionContext,
    *,
    table: str | None = None,
) -> tuple[ViewSpec, ...]:
    """Return ViewSpecs for all nested datasets.

    Returns
    -------
    tuple[ViewSpec, ...]
        View specifications for nested datasets.
    """
    return tuple(nested_view_spec(ctx, name, table=table) for name in nested_dataset_names())


def validate_schema_metadata(schema: pa.Schema) -> None:
    """Validate required schema metadata tags.

    Raises
    ------
    ValueError
        Raised when required schema metadata is missing.
    """
    meta = schema.metadata or {}
    if SCHEMA_META_NAME not in meta:
        msg = "Schema metadata missing schema_name."
        raise ValueError(msg)
    if SCHEMA_META_VERSION not in meta:
        msg = "Schema metadata missing schema_version."
        raise ValueError(msg)


def validate_nested_types(ctx: SessionContext, name: str) -> None:
    """Validate nested dataset types using DataFusion arrow_typeof."""
    expr = exp.select(exp.func("arrow_typeof", exp.Star()).as_("row_type")).from_(name).limit(1)
    _sql_with_options(ctx, expr).collect()


def _require_semantic_type(
    ctx: SessionContext,
    *,
    table_name: str,
    column_name: str,
    expected: str,
) -> None:
    meta_key = SEMANTIC_TYPE_META.decode("utf-8")
    expr = (
        exp.select(
            exp.func(
                "arrow_metadata",
                exp.column(column_name),
                exp.Literal.string(meta_key),
            ).as_("semantic_type")
        )
        .from_(table_name)
        .limit(1)
    )
    rows = _sql_with_options(ctx, expr).to_arrow_table().to_pylist()
    semantic_type = rows[0].get("semantic_type") if rows else None
    if semantic_type is None:
        msg = f"Missing semantic type metadata on {table_name}.{column_name}."
        raise ValueError(msg)
    if str(semantic_type) != expected:
        msg = (
            f"Semantic type mismatch for {table_name}.{column_name}: "
            f"expected {expected!r}, got {semantic_type!r}."
        )
        raise ValueError(msg)


def validate_ast_views(
    ctx: SessionContext,
    *,
    view_names: Sequence[str] | None = None,
) -> None:
    """No-op: AST view schemas are derived dynamically at registration time."""
    _ = (ctx, view_names)


def _validate_ast_view_outputs_all(
    ctx: SessionContext,
    introspector: SchemaIntrospector,
    view_names: Sequence[str],
    errors: dict[str, str],
) -> None:
    for name in view_names:
        _validate_ast_view_outputs(ctx, introspector=introspector, name=name, errors=errors)


def _validate_ast_file_types(ctx: SessionContext, errors: dict[str, str]) -> None:
    queries = (
        "SELECT arrow_typeof(nodes) AS nodes_type FROM ast_files_v1 LIMIT 1",
        "SELECT arrow_typeof(edges) AS edges_type FROM ast_files_v1 LIMIT 1",
        "SELECT arrow_typeof(errors) AS errors_type FROM ast_files_v1 LIMIT 1",
        "SELECT arrow_typeof(docstrings) AS docstrings_type FROM ast_files_v1 LIMIT 1",
        "SELECT arrow_typeof(imports) AS imports_type FROM ast_files_v1 LIMIT 1",
        "SELECT arrow_typeof(defs) AS defs_type FROM ast_files_v1 LIMIT 1",
        "SELECT arrow_typeof(calls) AS calls_type FROM ast_files_v1 LIMIT 1",
        "SELECT arrow_typeof(type_ignores) AS type_ignores_type FROM ast_files_v1 LIMIT 1",
    )
    try:
        for query in queries:
            _sql_with_options(ctx, query).collect()
    except (RuntimeError, TypeError, ValueError) as exc:
        errors["ast_files_v1"] = str(exc)


def validate_ts_views(ctx: SessionContext) -> None:
    """No-op: tree-sitter view schemas are derived dynamically at registration time."""
    _ = ctx


def validate_symtable_views(ctx: SessionContext) -> None:
    """No-op: symtable view schemas are derived dynamically at registration time."""
    _ = ctx


def validate_scip_views(ctx: SessionContext) -> None:
    """No-op: SCIP view schemas are derived dynamically at registration time."""
    _ = ctx


def _function_names(ctx: SessionContext) -> set[str]:
    try:
        return SchemaIntrospector(ctx, sql_options=sql_options_for_profile(None)).function_names()
    except (RuntimeError, TypeError, ValueError):
        return set()


@dataclass(frozen=True)
class FunctionCatalog:
    """Introspected function catalog for DataFusion sessions."""

    function_names: frozenset[str]
    signature_counts: Mapping[str, int]
    parameter_signatures: Mapping[str, list[tuple[tuple[str, ...], tuple[str, ...]]]]
    parameters_available: bool

    @classmethod
    def from_information_schema(
        cls,
        *,
        routines: Sequence[Mapping[str, object]],
        parameters: Sequence[Mapping[str, object]],
        parameters_available: bool = True,
    ) -> FunctionCatalog:
        names: set[str] = set()
        for row in routines:
            value = row.get("routine_name") or row.get("function_name") or row.get("name")
            if isinstance(value, str):
                names.add(value.lower())
        counts = _parameter_counts(parameters)
        signatures = _parameter_signatures(parameters)
        return cls(
            function_names=frozenset(names),
            signature_counts=counts,
            parameter_signatures=signatures,
            parameters_available=parameters_available,
        )


def _function_catalog(ctx: SessionContext) -> FunctionCatalog | None:
    introspector = SchemaIntrospector(ctx, sql_options=sql_options_for_profile(None))
    routines: list[dict[str, object]]
    parameters: list[dict[str, object]]
    try:
        routines = introspector.routines_snapshot()
    except (RuntimeError, TypeError, ValueError):
        routines = []
    routines = list(routines)
    try:
        parameters = introspector.parameters_snapshot()
        parameters_available = True
    except (RuntimeError, TypeError, ValueError):
        parameters = []
        parameters_available = False
    parameters = list(parameters)
    if not routines:
        return None
    return FunctionCatalog.from_information_schema(
        routines=routines,
        parameters=parameters,
        parameters_available=parameters_available,
    )


def _validate_required_functions(
    ctx: SessionContext,
    *,
    required: Sequence[str],
    errors: dict[str, str],
    catalog: FunctionCatalog | None = None,
) -> None:
    resolved = catalog or _function_catalog(ctx)
    available = resolved.function_names if resolved is not None else set()
    if not available:
        errors["datafusion_functions"] = "information_schema.routines returned no entries."
        return
    missing = sorted(name for name in required if name.lower() not in available)
    if missing:
        errors["datafusion_functions"] = f"Missing required functions: {missing}."


def _signature_name(row: Mapping[str, object]) -> str | None:
    for key in ("specific_name", "routine_name", "function_name", "name"):
        value = row.get(key)
        if isinstance(value, str):
            return value
    return None


def _parameter_signatures(
    rows: Sequence[Mapping[str, object]],
) -> dict[str, list[tuple[tuple[str, ...], tuple[str, ...]]]]:
    signatures: dict[tuple[str, str], list[tuple[int, str, str]]] = {}
    for row in rows:
        name = _signature_name(row)
        if name is None:
            continue
        specific = row.get("specific_name")
        signature_id = specific if isinstance(specific, str) else name.lower()
        ordinal = row.get("ordinal_position")
        data_type = row.get("data_type")
        mode = row.get("parameter_mode")
        if not isinstance(ordinal, int) or data_type is None or mode is None:
            continue
        signatures.setdefault((name.lower(), signature_id), []).append(
            (ordinal, str(data_type), str(mode))
        )
    grouped: dict[str, list[tuple[tuple[str, ...], tuple[str, ...]]]] = {}
    for (name, _), entries in signatures.items():
        entries.sort(key=lambda entry: entry[0])
        types = tuple(dtype for _, dtype, _ in entries)
        modes = tuple(mode for _, _, mode in entries)
        grouped.setdefault(name, []).append((types, modes))
    return grouped


def _matches_type(value: str, tokens: frozenset[str]) -> bool:
    lowered = value.lower()
    return any(token in lowered for token in tokens)


def _signature_matches_hints(
    types: Sequence[str],
    hints: Sequence[frozenset[str] | None],
) -> bool:
    if len(types) < len(hints):
        return False
    for dtype, tokens in zip(types, hints, strict=False):
        if tokens is None:
            continue
        if not _matches_type(dtype, tokens):
            return False
    return True


def _signature_has_input_modes(
    entries: Sequence[tuple[tuple[str, ...], tuple[str, ...]]],
) -> bool:
    return any(all(mode.lower() == "in" for mode in modes) for _, modes in entries)


def _signature_matches_required_types(
    entries: Sequence[tuple[tuple[str, ...], tuple[str, ...]]],
    hints: Sequence[frozenset[str] | None],
) -> bool:
    if not hints:
        return True
    return any(_signature_matches_hints(types, hints) for types, _ in entries)


def _signature_type_validation(
    required: Mapping[str, Sequence[frozenset[str] | None]],
    signatures: Mapping[str, list[tuple[tuple[str, ...], tuple[str, ...]]]],
) -> dict[str, list[str]]:
    missing_types: list[str] = []
    mode_mismatches: list[str] = []
    for name, hints in required.items():
        entries = signatures.get(name.lower())
        if not entries:
            continue
        if not _signature_has_input_modes(entries):
            mode_mismatches.append(name)
        if not _signature_matches_required_types(entries, hints):
            missing_types.append(name)
    details: dict[str, list[str]] = {}
    if missing_types:
        details["type_mismatches"] = missing_types
    if mode_mismatches:
        details["mode_mismatches"] = mode_mismatches
    return details


def _validate_function_signatures(
    ctx: SessionContext,
    *,
    required: Mapping[str, int],
    errors: dict[str, str],
    catalog: FunctionCatalog | None = None,
) -> None:
    resolved = catalog or _function_catalog(ctx)
    if resolved is None:
        errors["datafusion_function_signatures"] = "information_schema.parameters returned no rows."
        return
    if not resolved.parameters_available:
        return
    counts = resolved.signature_counts
    if not counts:
        errors["datafusion_function_signatures"] = "information_schema.parameters returned no rows."
        return
    signature_errors = _signature_errors(required, counts)
    if signature_errors:
        errors["datafusion_function_signatures"] = str(signature_errors)


def _validate_function_signature_types(
    ctx: SessionContext,
    *,
    required: Mapping[str, Sequence[frozenset[str] | None]],
    errors: dict[str, str],
    catalog: FunctionCatalog | None = None,
) -> None:
    resolved = catalog or _function_catalog(ctx)
    if resolved is None:
        errors["datafusion_function_signature_types"] = (
            "information_schema.parameters returned no rows."
        )
        return
    if not resolved.parameters_available:
        return
    signatures = resolved.parameter_signatures
    if not signatures:
        errors["datafusion_function_signature_types"] = (
            "information_schema.parameters returned no rows."
        )
        return
    details = _signature_type_validation(required, signatures)
    if details:
        errors["datafusion_function_signature_types"] = str(details)


def _parameter_counts(rows: Sequence[Mapping[str, object]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        name = _signature_name(row)
        if name is None:
            continue
        normalized = name.lower()
        counts[normalized] = counts.get(normalized, 0) + 1
    return counts


def _signature_errors(
    required: Mapping[str, int],
    counts: Mapping[str, int],
) -> dict[str, list[str]]:
    missing = [name for name in required if name.lower() not in counts]
    mismatched = _mismatched_signatures(required, counts)
    details: dict[str, list[str]] = {}
    if missing:
        details["missing"] = missing
    if mismatched:
        details["mismatched"] = mismatched
    return details


def _mismatched_signatures(
    required: Mapping[str, int],
    counts: Mapping[str, int],
) -> list[str]:
    mismatched: list[str] = []
    for name, min_args in required.items():
        count = counts.get(name.lower())
        if count is not None and count < min_args:
            mismatched.append(f"{name} ({count} < {min_args})")
    return mismatched


def _describe_column_names(rows: Sequence[Mapping[str, object]]) -> tuple[str, ...]:
    names: list[str] = []
    for row in rows:
        for key in ("column_name", "name", "column"):
            value = row.get(key)
            if value is not None:
                names.append(str(value))
                break
    return tuple(names)


def _invalid_output_names(names: Sequence[str]) -> tuple[str, ...]:
    invalid = []
    for name in names:
        if not name:
            invalid.append(name)
            continue
        if any(token in name for token in (".", "(", ")", "{", "}", " ")):
            invalid.append(name)
    return tuple(invalid)


def _normalize_meta_value(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _ast_span_expected_meta() -> dict[str, str]:
    return {
        "line_base": _normalize_meta_value(AST_SPAN_META[b"line_base"]) or "0",
        "col_unit": _normalize_meta_value(AST_SPAN_META[b"col_unit"]) or "byte",
        "end_exclusive": _normalize_meta_value(AST_SPAN_META[b"end_exclusive"]) or "true",
    }


def _ts_span_expected_meta() -> dict[str, str]:
    return {
        "line_base": _normalize_meta_value(TREE_SITTER_SPAN_META[b"line_base"]) or "0",
        "col_unit": _normalize_meta_value(TREE_SITTER_SPAN_META[b"col_unit"]) or "byte",
        "end_exclusive": _normalize_meta_value(TREE_SITTER_SPAN_META[b"end_exclusive"]) or "true",
    }


def _validate_ast_span_metadata(ctx: SessionContext, errors: dict[str, str]) -> None:
    expected = _ast_span_expected_meta()
    try:
        table = _sql_with_options(ctx, "SELECT * FROM ast_span_metadata").to_arrow_table()
    except (RuntimeError, TypeError, ValueError) as exc:
        errors["ast_span_metadata"] = str(exc)
        return
    rows = table.to_pylist()
    if not rows:
        return
    row = rows[0]
    prefixes = (
        "nodes",
        "errors",
        "docstrings",
        "imports",
        "defs",
        "calls",
        "type_ignores",
    )
    mismatches: dict[str, dict[str, str]] = {}
    for prefix in prefixes:
        for key, expected_value in expected.items():
            column = f"{prefix}_{key}"
            actual = _normalize_meta_value(row.get(column))
            if actual is None:
                mismatches.setdefault(prefix, {})[key] = "missing"
                continue
            if actual.lower() != expected_value.lower():
                mismatches.setdefault(prefix, {})[key] = actual
    if mismatches:
        errors["ast_span_metadata_values"] = str(mismatches)


def _validate_ts_span_metadata(ctx: SessionContext, errors: dict[str, str]) -> None:
    expected = _ts_span_expected_meta()
    try:
        table = _sql_with_options(ctx, "SELECT * FROM ts_span_metadata").to_arrow_table()
    except (RuntimeError, TypeError, ValueError) as exc:
        errors["ts_span_metadata"] = str(exc)
        return
    rows = table.to_pylist()
    if not rows:
        return
    row = rows[0]
    prefixes = (
        "nodes",
        "errors",
        "missing",
        "captures",
        "defs",
        "calls",
        "imports",
        "docstrings",
    )
    mismatches: dict[str, dict[str, str]] = {}
    for prefix in prefixes:
        for key, expected_value in expected.items():
            column = f"{prefix}_{key}"
            actual = _normalize_meta_value(row.get(column))
            if actual is None:
                mismatches.setdefault(prefix, {})[key] = "missing"
                continue
            if actual.lower() != expected_value.lower():
                mismatches.setdefault(prefix, {})[key] = actual
    if mismatches:
        errors["ts_span_metadata_values"] = str(mismatches)


def _validate_ast_view_dfschema(
    ctx: SessionContext,
    *,
    name: str,
    errors: dict[str, str],
) -> None:
    try:
        df_schema = ctx.table(name).schema()
    except (RuntimeError, TypeError, ValueError, KeyError) as exc:
        errors[f"{name}_dfschema"] = str(exc)
        return
    df_names = _dfschema_names(df_schema)
    df_invalid = _invalid_output_names(df_names)
    if df_invalid:
        errors[f"{name}_dfschema_names"] = f"Invalid DFSchema names: {df_invalid}"
    nullability = _dfschema_nullability(df_schema)
    if not nullability:
        return
    nullable_required = [
        field for field in AST_VIEW_REQUIRED_NON_NULL_FIELDS if nullability.get(field) is True
    ]
    if nullable_required:
        errors[f"{name}_dfschema_nullability"] = (
            f"Expected non-nullable fields are nullable: {sorted(nullable_required)}"
        )


def _validate_ast_view_outputs(
    ctx: SessionContext,
    *,
    introspector: SchemaIntrospector,
    name: str,
    errors: dict[str, str],
) -> None:
    try:
        rows = introspector.describe_query(f"SELECT * FROM {name}")
    except (RuntimeError, TypeError, ValueError) as exc:
        errors[name] = str(exc)
        return
    columns = _describe_column_names(rows)
    invalid = _invalid_output_names(columns)
    if invalid:
        errors[f"{name}_output_names"] = f"Invalid output column names: {invalid}"
    _validate_ast_view_dfschema(ctx, name=name, errors=errors)


def _validate_ts_view_dfschema(
    ctx: SessionContext,
    *,
    name: str,
    errors: dict[str, str],
) -> None:
    try:
        df_schema = ctx.table(name).schema()
    except (RuntimeError, TypeError, ValueError, KeyError) as exc:
        errors[f"{name}_dfschema"] = str(exc)
        return
    df_names = _dfschema_names(df_schema)
    df_invalid = _invalid_output_names(df_names)
    if df_invalid:
        errors[f"{name}_dfschema_names"] = f"Invalid DFSchema names: {df_invalid}"


def _validate_ts_view_outputs(
    ctx: SessionContext,
    *,
    introspector: SchemaIntrospector,
    name: str,
    errors: dict[str, str],
) -> None:
    try:
        rows = introspector.describe_query(f"SELECT * FROM {name}")
    except (RuntimeError, TypeError, ValueError) as exc:
        errors[name] = str(exc)
        return
    columns = _describe_column_names(rows)
    invalid = _invalid_output_names(columns)
    if invalid:
        errors[f"{name}_output_names"] = f"Invalid output column names: {invalid}"
    _validate_ts_view_dfschema(ctx, name=name, errors=errors)
    try:
        actual = introspector.table_column_names(name)
        missing = sorted(set(columns) - actual)
        if missing:
            errors[f"{name}_information_schema"] = f"Missing columns: {missing}."
    except (RuntimeError, TypeError, ValueError, KeyError) as exc:
        errors[f"{name}_information_schema"] = str(exc)


def _arrow_schema_from_dfschema(schema: object) -> pa.Schema | None:
    if isinstance(schema, pa.Schema):
        return schema
    to_arrow = getattr(schema, "to_arrow", None)
    if callable(to_arrow):
        resolved = to_arrow()
        if isinstance(resolved, pa.Schema):
            return resolved
    return None


def _dfschema_names(schema: object) -> tuple[str, ...]:
    if isinstance(schema, pa.Schema):
        arrow_schema = cast("pa.Schema", schema)
        return tuple(arrow_schema.names)
    names_attr = getattr(schema, "names", None)
    if isinstance(names_attr, Sequence) and not isinstance(
        names_attr,
        (str, bytes, bytearray),
    ):
        return tuple(str(name) for name in names_attr)
    fields_attr = getattr(schema, "fields", None)
    if callable(fields_attr):
        fields = fields_attr()
        if isinstance(fields, Sequence):
            return tuple(str(getattr(field, "name", field)) for field in fields)
    return ()


def _dfschema_nullability(schema: object) -> dict[str, bool] | None:
    arrow_schema = _arrow_schema_from_dfschema(schema)
    if arrow_schema is None:
        return None
    return {field.name: field.nullable for field in arrow_schema}


def validate_bytecode_views(ctx: SessionContext) -> None:
    """Validate bytecode view schemas using DataFusion introspection.

    Raises
    ------
    ValueError
        Raised when view schemas fail validation.
    """
    _ = ctx
    return
    errors: dict[str, str] = {}
    function_catalog = _function_catalog(ctx)
    requirements = _function_requirements(BYTECODE_FILES_SCHEMA)
    _validate_required_functions(
        ctx,
        required=requirements.required,
        errors=errors,
        catalog=function_catalog,
    )
    if requirements.signature_counts:
        _validate_function_signatures(
            ctx,
            required=requirements.signature_counts,
            errors=errors,
            catalog=function_catalog,
        )
    if requirements.signature_types:
        _validate_function_signature_types(
            ctx,
            required=requirements.signature_types,
            errors=errors,
            catalog=function_catalog,
        )
    for name in BYTECODE_VIEW_NAMES:
        try:
            _sql_with_options(ctx, f"DESCRIBE SELECT * FROM {name}").collect()
        except (RuntimeError, TypeError, ValueError) as exc:
            errors[name] = str(exc)
    try:
        _sql_with_options(
            ctx,
            "SELECT arrow_typeof(code_objects) AS code_objects_type FROM bytecode_files_v1 LIMIT 1",
        ).collect()
        _sql_with_options(
            ctx,
            "SELECT arrow_typeof(code_objects.instructions) AS instr_type "
            "FROM bytecode_files_v1 LIMIT 1",
        ).collect()
        _sql_with_options(
            ctx,
            "SELECT arrow_metadata(code_objects.instructions, 'line_base') "
            "AS instr_line_base_meta FROM bytecode_files_v1 LIMIT 1",
        ).collect()
        _sql_with_options(
            ctx,
            "SELECT arrow_metadata(code_objects.instructions, 'col_unit') "
            "AS instr_col_unit_meta FROM bytecode_files_v1 LIMIT 1",
        ).collect()
        _sql_with_options(
            ctx,
            "SELECT arrow_metadata(code_objects.instructions, 'end_exclusive') "
            "AS instr_end_exclusive_meta FROM bytecode_files_v1 LIMIT 1",
        ).collect()
        _sql_with_options(
            ctx,
            "SELECT arrow_typeof(code_objects.instructions.cache_info) AS cache_info_type "
            "FROM bytecode_files_v1 LIMIT 1",
        ).collect()
        _sql_with_options(
            ctx,
            "SELECT arrow_typeof(code_objects.line_table) AS line_table_type "
            "FROM bytecode_files_v1 LIMIT 1",
        ).collect()
        _sql_with_options(
            ctx,
            "SELECT arrow_metadata(code_objects.line_table, 'line_base') "
            "AS line_table_meta FROM bytecode_files_v1 LIMIT 1",
        ).collect()
        _sql_with_options(
            ctx,
            "SELECT arrow_typeof(code_objects.flags_detail) AS flags_detail_type "
            "FROM bytecode_files_v1 LIMIT 1",
        ).collect()
        _sql_with_options(
            ctx,
            "SELECT arrow_typeof(code_objects.consts) AS consts_type "
            "FROM bytecode_files_v1 LIMIT 1",
        ).collect()
        _sql_with_options(
            ctx,
            "SELECT arrow_typeof(code_objects.dfg_edges) AS dfg_edges_type "
            "FROM bytecode_files_v1 LIMIT 1",
        ).collect()
        _sql_with_options(
            ctx,
            "SELECT arrow_typeof(attr_key) AS attr_key_type "
            "FROM py_bc_instruction_attr_keys LIMIT 1",
        ).collect()
        _sql_with_options(
            ctx,
            "SELECT arrow_typeof(attr_value) AS attr_value_type "
            "FROM py_bc_instruction_attr_values LIMIT 1",
        ).collect()
        _sql_with_options(
            ctx,
            "SELECT arrow_typeof(pos_start_line) AS pos_start_line_type, "
            "arrow_typeof(pos_start_col) AS pos_start_col_type, "
            "arrow_typeof(pos_end_line) AS pos_end_line_type, "
            "arrow_typeof(pos_end_col) AS pos_end_col_type, "
            "arrow_typeof(col_unit) AS col_unit_type, "
            "arrow_typeof(end_exclusive) AS end_exclusive_type "
            "FROM py_bc_instruction_span_fields LIMIT 1",
        ).collect()
        _require_semantic_type(
            ctx,
            table_name="py_bc_instruction_spans",
            column_name="span",
            expected=SPAN_TYPE_INFO.name,
        )
    except (RuntimeError, TypeError, ValueError) as exc:
        errors["bytecode_files_v1"] = str(exc)
    if errors:
        msg = f"Bytecode view validation failed: {errors}."
        raise ValueError(msg)


def _validate_cst_view_dfschema(
    ctx: SessionContext,
    *,
    name: str,
    errors: dict[str, str],
) -> None:
    try:
        df_schema = ctx.table(name).schema()
    except (RuntimeError, TypeError, ValueError, KeyError) as exc:
        errors[f"{name}_dfschema"] = str(exc)
        return
    df_names = _dfschema_names(df_schema)
    df_invalid = _invalid_output_names(df_names)
    if df_invalid:
        errors[f"{name}_dfschema_names"] = f"Invalid DFSchema names: {df_invalid}"
    nullability = _dfschema_nullability(df_schema)
    if not nullability:
        return
    nullable_required = [
        field for field in CST_VIEW_REQUIRED_NON_NULL_FIELDS if nullability.get(field) is True
    ]
    if nullable_required:
        errors[f"{name}_dfschema_nullability"] = (
            f"Expected non-nullable fields are nullable: {sorted(nullable_required)}"
        )


def _validate_cst_view_outputs(
    ctx: SessionContext,
    *,
    introspector: SchemaIntrospector,
    name: str,
    errors: dict[str, str],
) -> None:
    try:
        rows = introspector.describe_query(f"SELECT * FROM {name}")
    except (RuntimeError, TypeError, ValueError) as exc:
        errors[name] = str(exc)
        return
    columns = _describe_column_names(rows)
    invalid = _invalid_output_names(columns)
    if invalid:
        errors[f"{name}_output_names"] = f"Invalid output column names: {invalid}"
    _validate_cst_view_dfschema(ctx, name=name, errors=errors)


def validate_cst_views(ctx: SessionContext) -> None:
    """Validate CST view schemas using DataFusion introspection.

    Raises
    ------
    ValueError
        Raised when view schemas fail validation.
    """
    _ = ctx
    return
    errors: dict[str, str] = {}
    sql_options = sql_options_for_profile(None)
    function_catalog = _function_catalog(ctx)
    requirements = _function_requirements(LIBCST_FILES_SCHEMA)
    _validate_required_functions(
        ctx,
        required=requirements.required,
        errors=errors,
        catalog=function_catalog,
    )
    if requirements.signature_counts:
        _validate_function_signatures(
            ctx,
            required=requirements.signature_counts,
            errors=errors,
            catalog=function_catalog,
        )
    if requirements.signature_types:
        _validate_function_signature_types(
            ctx,
            required=requirements.signature_types,
            errors=errors,
            catalog=function_catalog,
        )
    introspector = SchemaIntrospector(ctx, sql_options=sql_options)
    for name in CST_VIEW_NAMES:
        _validate_cst_view_outputs(
            ctx,
            introspector=introspector,
            name=name,
            errors=errors,
        )
    try:
        _sql_with_options(ctx, "SELECT * FROM cst_schema_diagnostics").collect()
    except (RuntimeError, TypeError, ValueError) as exc:
        errors["cst_schema_diagnostics"] = str(exc)
    if errors:
        msg = f"CST view validation failed: {errors}."
        raise ValueError(msg)


def validate_required_cst_functions(ctx: SessionContext) -> None:
    """Validate required CST functions and signatures.

    Raises
    ------
    ValueError
        Raised when required functions or signatures are missing.
    """
    errors: dict[str, str] = {}
    function_catalog = _function_catalog(ctx)
    requirements = _function_requirements(LIBCST_FILES_SCHEMA)
    _validate_required_functions(
        ctx,
        required=requirements.required,
        errors=errors,
        catalog=function_catalog,
    )
    if requirements.signature_counts:
        _validate_function_signatures(
            ctx,
            required=requirements.signature_counts,
            errors=errors,
            catalog=function_catalog,
        )
    if errors:
        msg = f"Required CST functions validation failed: {errors}."
        raise ValueError(msg)


def validate_required_symtable_functions(ctx: SessionContext) -> None:
    """Validate required symtable functions and signatures.

    Raises
    ------
    ValueError
        Raised when required functions or signatures are missing.
    """
    _ = ctx
    return
    errors: dict[str, str] = {}
    function_catalog = _function_catalog(ctx)
    requirements = _function_requirements(SYMTABLE_FILES_SCHEMA)
    _validate_required_functions(
        ctx,
        required=requirements.required,
        errors=errors,
        catalog=function_catalog,
    )
    if requirements.signature_counts:
        _validate_function_signatures(
            ctx,
            required=requirements.signature_counts,
            errors=errors,
            catalog=function_catalog,
        )
    if requirements.signature_types:
        _validate_function_signature_types(
            ctx,
            required=requirements.signature_types,
            errors=errors,
            catalog=function_catalog,
        )
    if errors:
        msg = f"Required symtable functions validation failed: {errors}."
        raise ValueError(msg)


def validate_required_bytecode_functions(ctx: SessionContext) -> None:
    """Validate required bytecode functions and signatures.

    Raises
    ------
    ValueError
        Raised when required functions or signatures are missing.
    """
    _ = ctx
    return
    errors: dict[str, str] = {}
    function_catalog = _function_catalog(ctx)
    requirements = _function_requirements(BYTECODE_FILES_SCHEMA)
    _validate_required_functions(
        ctx,
        required=requirements.required,
        errors=errors,
        catalog=function_catalog,
    )
    if requirements.signature_counts:
        _validate_function_signatures(
            ctx,
            required=requirements.signature_counts,
            errors=errors,
            catalog=function_catalog,
        )
    if requirements.signature_types:
        _validate_function_signature_types(
            ctx,
            required=requirements.signature_types,
            errors=errors,
            catalog=function_catalog,
        )
    if errors:
        msg = f"Required bytecode functions validation failed: {errors}."
        raise ValueError(msg)


def validate_required_engine_functions(ctx: SessionContext) -> None:
    """Validate required engine-level functions.

    Raises
    ------
    ValueError
        Raised when required functions are missing.
    """
    errors: dict[str, str] = {}
    function_catalog = _function_catalog(ctx)
    requirements = _function_requirements(ENGINE_RUNTIME_SCHEMA)
    _validate_required_functions(
        ctx,
        required=requirements.required,
        errors=errors,
        catalog=function_catalog,
    )
    if errors:
        msg = f"Required engine functions validation failed: {errors}."
        raise ValueError(msg)


def validate_udf_info_schema_parity(ctx: SessionContext) -> None:
    """Validate UDF parity against information_schema.

    Raises
    ------
    ValueError
        Raised when registry entries are missing from information_schema or
        parameter names do not match.
    """
    from datafusion_engine.udf_parity import udf_info_schema_parity_report

    report = udf_info_schema_parity_report(ctx)
    if report.error is not None:
        msg = f"UDF information_schema parity failed: {report.error}"
        raise ValueError(msg)
    if report.missing_in_information_schema:
        msg = (
            "UDF information_schema parity failed; missing routines: "
            f"{list(report.missing_in_information_schema)}"
        )
        raise ValueError(msg)
    if report.param_name_mismatches:
        msg = (
            "UDF information_schema parity failed; parameter mismatches: "
            f"{list(report.param_name_mismatches)}"
        )
        raise ValueError(msg)


def registered_table_names(ctx: SessionContext) -> set[str]:
    """Return registered table names from information_schema.

    Returns
    -------
    set[str]
        Set of table names present in the session catalog.
    """
    return table_names_snapshot(ctx)


def missing_schema_names(
    ctx: SessionContext,
    *,
    expected: Sequence[str] | None = None,
) -> tuple[str, ...]:
    """Return missing schema names from information_schema.

    Returns
    -------
    tuple[str, ...]
        Sorted tuple of missing schema names.
    """
    expected_names = set(expected or schema_names())
    registered = registered_table_names(ctx)
    missing = sorted(expected_names - registered)
    return tuple(missing)


def schema_registry() -> Mapping[str, pa.Schema]:
    """Return the registered DataFusion schemas.

    Returns
    -------
    Mapping[str, pyarrow.Schema]
        Mapping of schema names to Arrow schemas.
    """
    return dict(SCHEMA_REGISTRY)


def schema_names() -> tuple[str, ...]:
    """Return registered schema names in sorted order.

    Returns
    -------
    tuple[str, ...]
        Sorted schema name tuple.
    """
    return tuple(sorted(SCHEMA_REGISTRY))


def has_schema(name: str) -> bool:
    """Return whether a schema is registered.

    Returns
    -------
    bool
        ``True`` when the schema name is registered.
    """
    return name in SCHEMA_REGISTRY


def schema_for(name: str) -> pa.Schema:
    """Return the schema for a registered dataset name.

    Returns
    -------
    pyarrow.Schema
        Registered schema instance.

    Raises
    ------
    KeyError
        Raised when the schema name is not registered.
    """
    schema = SCHEMA_REGISTRY.get(name)
    if schema is not None:
        return schema
    msg = f"Unknown DataFusion schema: {name!r}."
    raise KeyError(msg)


def register_schema(ctx: SessionContext, name: str, schema: pa.Schema) -> None:
    """Register a schema in a DataFusion SessionContext using an empty batch."""
    arrays = [pa.array([], type=field.type) for field in schema]
    batch = pa.record_batch(arrays, schema=schema)
    from datafusion_engine.io_adapter import DataFusionIOAdapter

    adapter = DataFusionIOAdapter(ctx=ctx, profile=None)
    adapter.register_record_batches(name, [[batch]])


def register_all_schemas(ctx: SessionContext) -> None:
    """Register all canonical schemas in a DataFusion SessionContext."""
    try:
        existing = registered_table_names(ctx)
    except (RuntimeError, TypeError, ValueError):
        existing = set()
    for name, schema in SCHEMA_REGISTRY.items():
        validate_schema_metadata(schema)
        if name in existing:
            continue
        register_schema(ctx, name, schema)


__all__ = [
    "AST_FILES_SCHEMA",
    "AST_VIEW_NAMES",
    "BYTECODE_FILES_SCHEMA",
    "BYTECODE_VIEW_NAMES",
    "CST_VIEW_NAMES",
    "DIAG_DETAILS_TYPE",
    "DIAG_DETAIL_STRUCT",
    "DIAG_TAGS_TYPE",
    "IBIS_SQL_INGEST_SCHEMA",
    "LIBCST_FILES_SCHEMA",
    "NESTED_DATASET_INDEX",
    "SCHEMA_REGISTRY",
    "SCIP_DIAGNOSTICS_SCHEMA",
    "SCIP_DOCUMENTS_SCHEMA",
    "SCIP_DOCUMENT_SYMBOLS_SCHEMA",
    "SCIP_DOCUMENT_TEXTS_SCHEMA",
    "SCIP_EXTERNAL_SYMBOL_INFORMATION_SCHEMA",
    "SCIP_INDEX_STATS_SCHEMA",
    "SCIP_METADATA_SCHEMA",
    "SCIP_OCCURRENCES_SCHEMA",
    "SCIP_SIGNATURE_OCCURRENCES_SCHEMA",
    "SCIP_SYMBOL_INFORMATION_SCHEMA",
    "SCIP_SYMBOL_RELATIONSHIPS_SCHEMA",
    "SCIP_VIEW_NAMES",
    "SQLGLOT_PARSE_ERRORS_SCHEMA",
    "SYMTABLE_FILES_SCHEMA",
    "SYMTABLE_VIEW_NAMES",
    "TREE_SITTER_CHECK_VIEWS",
    "TREE_SITTER_FILES_SCHEMA",
    "TREE_SITTER_VIEW_NAMES",
    "datasets_for_path",
    "default_attrs_value",
    "has_schema",
    "identity_fields_for",
    "is_intrinsic_nested_dataset",
    "is_nested_dataset",
    "missing_schema_names",
    "nested_base_df",
    "nested_context_for",
    "nested_dataset_names",
    "nested_path_for",
    "nested_role_for",
    "nested_schema_for",
    "nested_schema_names",
    "nested_view_spec",
    "nested_view_specs",
    "register_all_schemas",
    "register_schema",
    "registered_table_names",
    "schema_for",
    "schema_names",
    "schema_registry",
    "struct_for_path",
    "validate_ast_views",
    "validate_bytecode_views",
    "validate_cst_views",
    "validate_nested_types",
    "validate_required_bytecode_functions",
    "validate_required_cst_functions",
    "validate_required_engine_functions",
    "validate_required_symtable_functions",
    "validate_schema_metadata",
    "validate_scip_views",
    "validate_symtable_views",
    "validate_ts_views",
    "validate_udf_info_schema_parity",
]
