"""DataFusion-native nested schemas for canonical datasets."""

from __future__ import annotations

import importlib
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Literal, TypedDict, cast

import pyarrow as pa
from datafusion import SessionContext
from datafusion.dataframe import DataFrame

from arrowdsl.core.ordering import OrderingLevel
from arrowdsl.core.schema_constants import (
    DEFAULT_VALUE_META,
    KEY_FIELDS_META,
    REQUIRED_NON_NULL_META,
    SCHEMA_META_NAME,
    SCHEMA_META_VERSION,
)
from arrowdsl.schema.build import list_view_type, struct_type
from arrowdsl.schema.metadata import metadata_list_bytes, ordering_metadata_spec
from arrowdsl.schema.semantic_types import (
    SEMANTIC_TYPE_META,
    SPAN_TYPE_INFO,
    byte_span_type,
    span_type,
)
from datafusion_engine.schema_introspection import SchemaIntrospector, table_names_snapshot
from datafusion_engine.sql_options import sql_options_for_profile
from schema_spec.view_specs import ViewSpec, view_spec_from_builder
from sqlglot_tools.optimizer import parse_sql_strict, resolve_sqlglot_policy

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


def _sql_with_options(ctx: SessionContext, sql: str) -> DataFrame:
    return ctx.sql_with_options(sql, sql_options_for_profile(None))


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

DATAFUSION_DDL_FINGERPRINTS_SCHEMA = _schema_with_metadata(
    "datafusion_ddl_fingerprints_v1",
    pa.schema(
        [
            pa.field("event_time_unix_ms", pa.int64(), nullable=False),
            pa.field("table_catalog", pa.string(), nullable=False),
            pa.field("table_schema", pa.string(), nullable=False),
            pa.field("table_name", pa.string(), nullable=False),
            pa.field("table_type", pa.string(), nullable=True),
            pa.field("ddl_fingerprint", pa.string(), nullable=True),
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

REL_NAME_SYMBOL_SCHEMA = _schema_with_metadata(
    "rel_name_symbol_v1",
    pa.schema(
        [
            pa.field("ref_id", pa.string()),
            pa.field("symbol", pa.string()),
            pa.field("symbol_roles", pa.int32()),
            pa.field("path", pa.string()),
            pa.field("edge_owner_file_id", pa.string()),
            pa.field("bstart", pa.int64()),
            pa.field("bend", pa.int64()),
            pa.field("resolution_method", pa.string()),
            pa.field("confidence", pa.float32()),
            pa.field("score", pa.float32()),
            pa.field("rule_name", pa.string()),
            pa.field("rule_priority", pa.int32()),
        ]
    ),
)

REL_IMPORT_SYMBOL_SCHEMA = _schema_with_metadata(
    "rel_import_symbol_v1",
    pa.schema(
        [
            pa.field("import_alias_id", pa.string()),
            pa.field("symbol", pa.string()),
            pa.field("symbol_roles", pa.int32()),
            pa.field("path", pa.string()),
            pa.field("edge_owner_file_id", pa.string()),
            pa.field("bstart", pa.int64()),
            pa.field("bend", pa.int64()),
            pa.field("resolution_method", pa.string()),
            pa.field("confidence", pa.float32()),
            pa.field("score", pa.float32()),
            pa.field("rule_name", pa.string()),
            pa.field("rule_priority", pa.int32()),
        ]
    ),
)

REL_DEF_SYMBOL_SCHEMA = _schema_with_metadata(
    "rel_def_symbol_v1",
    pa.schema(
        [
            pa.field("def_id", pa.string()),
            pa.field("symbol", pa.string()),
            pa.field("symbol_roles", pa.int32()),
            pa.field("path", pa.string()),
            pa.field("edge_owner_file_id", pa.string()),
            pa.field("bstart", pa.int64()),
            pa.field("bend", pa.int64()),
            pa.field("resolution_method", pa.string()),
            pa.field("confidence", pa.float32()),
            pa.field("score", pa.float32()),
            pa.field("rule_name", pa.string()),
            pa.field("rule_priority", pa.int32()),
        ]
    ),
)

REL_CALLSITE_SYMBOL_SCHEMA = _schema_with_metadata(
    "rel_callsite_symbol_v1",
    pa.schema(
        [
            pa.field("call_id", pa.string()),
            pa.field("symbol", pa.string()),
            pa.field("symbol_roles", pa.int32()),
            pa.field("path", pa.string()),
            pa.field("edge_owner_file_id", pa.string()),
            pa.field("call_bstart", pa.int64()),
            pa.field("call_bend", pa.int64()),
            pa.field("resolution_method", pa.string()),
            pa.field("confidence", pa.float32()),
            pa.field("score", pa.float32()),
            pa.field("rule_name", pa.string()),
            pa.field("rule_priority", pa.int32()),
        ]
    ),
)

REL_CALLSITE_QNAME_SCHEMA = _schema_with_metadata(
    "rel_callsite_qname_v1",
    pa.schema(
        [
            pa.field("call_id", pa.string()),
            pa.field("qname_id", pa.string()),
            pa.field("qname_source", pa.string()),
            pa.field("path", pa.string()),
            pa.field("edge_owner_file_id", pa.string()),
            pa.field("call_bstart", pa.int64()),
            pa.field("call_bend", pa.int64()),
            pa.field("confidence", pa.float32()),
            pa.field("score", pa.float32()),
            pa.field("ambiguity_group_id", pa.string()),
            pa.field("rule_name", pa.string()),
            pa.field("rule_priority", pa.int32()),
        ]
    ),
)

RELATION_OUTPUT_SCHEMA = _schema_with_metadata(
    "relation_output_v1",
    pa.schema(
        [
            pa.field("src", pa.string(), nullable=True),
            pa.field("dst", pa.string(), nullable=True),
            pa.field("path", pa.string(), nullable=True),
            pa.field("edge_owner_file_id", pa.string(), nullable=True),
            pa.field("bstart", pa.int64(), nullable=True),
            pa.field("bend", pa.int64(), nullable=True),
            pa.field("origin", pa.string(), nullable=True),
            pa.field("resolution_method", pa.string(), nullable=True),
            pa.field("binding_kind", pa.string(), nullable=True),
            pa.field("def_site_kind", pa.string(), nullable=True),
            pa.field("use_kind", pa.string(), nullable=True),
            pa.field("kind", pa.string(), nullable=True),
            pa.field("reason", pa.string(), nullable=True),
            pa.field("confidence", pa.float32(), nullable=True),
            pa.field("score", pa.float32(), nullable=True),
            pa.field("symbol_roles", pa.int32(), nullable=True),
            pa.field("qname_source", pa.string(), nullable=True),
            pa.field("ambiguity_group_id", pa.string(), nullable=True),
            pa.field("diag_source", pa.string(), nullable=True),
            pa.field("severity", pa.string(), nullable=True),
            pa.field("rule_name", pa.string(), nullable=True),
            pa.field("rule_priority", pa.int32(), nullable=True),
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
    "ast_files_v1": AST_FILES_SCHEMA,
    "bytecode_files_v1": BYTECODE_FILES_SCHEMA,
    "callsite_qname_candidates_v1": CALLSITE_QNAME_CANDIDATES_SCHEMA,
    "dataset_fingerprint_v1": DATASET_FINGERPRINT_SCHEMA,
    "datafusion_cache_state_v1": DATAFUSION_CACHE_STATE_SCHEMA,
    "datafusion_ddl_fingerprints_v1": DATAFUSION_DDL_FINGERPRINTS_SCHEMA,
    "datafusion_explains_v1": DATAFUSION_EXPLAINS_SCHEMA,
    "datafusion_plan_artifacts_v1": DATAFUSION_PLAN_ARTIFACTS_SCHEMA,
    "datafusion_runs_v1": DATAFUSION_RUNS_SCHEMA,
    "datafusion_schema_map_fingerprints_v1": DATAFUSION_SCHEMA_MAP_FINGERPRINTS_SCHEMA,
    "datafusion_schema_registry_validation_v1": DATAFUSION_SCHEMA_REGISTRY_VALIDATION_SCHEMA,
    "dim_qualified_names_v1": DIM_QUALIFIED_NAMES_SCHEMA,
    "engine_runtime_v1": ENGINE_RUNTIME_SCHEMA,
    "feature_state_v1": FEATURE_STATE_SCHEMA,
    "ibis_sql_ingest_v1": IBIS_SQL_INGEST_SCHEMA,
    "libcst_files_v1": LIBCST_FILES_SCHEMA,
    "sqlglot_parse_errors_v1": SQLGLOT_PARSE_ERRORS_SCHEMA,
    "param_file_ids_v1": PARAM_FILE_IDS_SCHEMA,
    "rel_callsite_qname_v1": REL_CALLSITE_QNAME_SCHEMA,
    "rel_callsite_symbol_v1": REL_CALLSITE_SYMBOL_SCHEMA,
    "rel_def_symbol_v1": REL_DEF_SYMBOL_SCHEMA,
    "rel_import_symbol_v1": REL_IMPORT_SYMBOL_SCHEMA,
    "rel_name_symbol_v1": REL_NAME_SYMBOL_SCHEMA,
    "relation_output_v1": RELATION_OUTPUT_SCHEMA,
    "repo_snapshot_v1": REPO_SNAPSHOT_SCHEMA,
    "scalar_param_signature_v1": SCALAR_PARAM_SIGNATURE_SCHEMA,
    "scip_metadata_v1": SCIP_METADATA_SCHEMA,
    "scip_index_stats_v1": SCIP_INDEX_STATS_SCHEMA,
    "scip_documents_v1": SCIP_DOCUMENTS_SCHEMA,
    "scip_document_texts_v1": SCIP_DOCUMENT_TEXTS_SCHEMA,
    "scip_occurrences_v1": SCIP_OCCURRENCES_SCHEMA,
    "scip_symbol_information_v1": SCIP_SYMBOL_INFORMATION_SCHEMA,
    "scip_document_symbols_v1": SCIP_DOCUMENT_SYMBOLS_SCHEMA,
    "scip_external_symbol_information_v1": SCIP_EXTERNAL_SYMBOL_INFORMATION_SCHEMA,
    "scip_symbol_relationships_v1": SCIP_SYMBOL_RELATIONSHIPS_SCHEMA,
    "scip_signature_occurrences_v1": SCIP_SIGNATURE_OCCURRENCES_SCHEMA,
    "scip_diagnostics_v1": SCIP_DIAGNOSTICS_SCHEMA,
    "symtable_files_v1": SYMTABLE_FILES_SCHEMA,
    "tree_sitter_files_v1": TREE_SITTER_FILES_SCHEMA,
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
AST_VIEW_COLUMN_MAP: dict[str, tuple[str, ...]] = {
    "ast_nodes": (
        "file_id",
        "path",
        "ast_id",
        "parent_ast_id",
        "kind",
        "name",
        "value_repr",
        "lineno",
        "col_offset",
        "end_lineno",
        "end_col_offset",
        "line_base",
        "col_unit",
        "end_exclusive",
        "bstart",
        "bend",
        "span",
        "attrs",
        "ast_record",
    ),
    "ast_edges": (
        "file_id",
        "path",
        "src",
        "dst",
        "kind",
        "slot",
        "idx",
        "attrs",
    ),
    "ast_errors": (
        "file_id",
        "path",
        "error_type",
        "message",
        "lineno",
        "col_offset",
        "end_lineno",
        "end_col_offset",
        "line_base",
        "col_unit",
        "end_exclusive",
        "span",
        "attrs",
        "ast_record",
    ),
    "ast_docstrings": (
        "file_id",
        "path",
        "owner_ast_id",
        "owner_kind",
        "owner_name",
        "docstring",
        "source",
        "lineno",
        "col_offset",
        "end_lineno",
        "end_col_offset",
        "line_base",
        "col_unit",
        "end_exclusive",
        "span",
        "attrs",
        "ast_record",
    ),
    "ast_imports": (
        "file_id",
        "path",
        "ast_id",
        "parent_ast_id",
        "kind",
        "module",
        "name",
        "asname",
        "alias_index",
        "level",
        "lineno",
        "col_offset",
        "end_lineno",
        "end_col_offset",
        "line_base",
        "col_unit",
        "end_exclusive",
        "span",
        "attrs",
        "ast_record",
    ),
    "ast_defs": (
        "file_id",
        "path",
        "ast_id",
        "parent_ast_id",
        "kind",
        "name",
        "decorator_count",
        "arg_count",
        "posonly_count",
        "kwonly_count",
        "type_params_count",
        "base_count",
        "keyword_count",
        "lineno",
        "col_offset",
        "end_lineno",
        "end_col_offset",
        "line_base",
        "col_unit",
        "end_exclusive",
        "span",
        "attrs",
        "ast_record",
    ),
    "ast_calls": (
        "file_id",
        "path",
        "ast_id",
        "parent_ast_id",
        "func_kind",
        "func_name",
        "arg_count",
        "keyword_count",
        "starred_count",
        "kw_star_count",
        "lineno",
        "col_offset",
        "end_lineno",
        "end_col_offset",
        "line_base",
        "col_unit",
        "end_exclusive",
        "span",
        "attrs",
        "ast_record",
    ),
    "ast_type_ignores": (
        "file_id",
        "path",
        "ast_id",
        "tag",
        "lineno",
        "col_offset",
        "end_lineno",
        "end_col_offset",
        "line_base",
        "col_unit",
        "end_exclusive",
        "span",
        "attrs",
        "ast_record",
    ),
    "ast_node_attrs": (
        "file_id",
        "path",
        "ast_id",
        "parent_ast_id",
        "kind",
        "name",
        "attr_key",
        "attr_value",
    ),
    "ast_def_attrs": (
        "file_id",
        "path",
        "ast_id",
        "parent_ast_id",
        "kind",
        "name",
        "attr_key",
        "attr_value",
    ),
    "ast_call_attrs": (
        "file_id",
        "path",
        "ast_id",
        "parent_ast_id",
        "func_kind",
        "func_name",
        "attr_key",
        "attr_value",
    ),
    "ast_edge_attrs": (
        "file_id",
        "path",
        "src",
        "dst",
        "kind",
        "slot",
        "idx",
        "attr_key",
        "attr_value",
    ),
    "ast_span_metadata": (
        "nodes_line_base",
        "nodes_col_unit",
        "nodes_end_exclusive",
        "errors_line_base",
        "errors_col_unit",
        "errors_end_exclusive",
        "docstrings_line_base",
        "docstrings_col_unit",
        "docstrings_end_exclusive",
        "imports_line_base",
        "imports_col_unit",
        "imports_end_exclusive",
        "defs_line_base",
        "defs_col_unit",
        "defs_end_exclusive",
        "calls_line_base",
        "calls_col_unit",
        "calls_end_exclusive",
        "type_ignores_line_base",
        "type_ignores_col_unit",
        "type_ignores_end_exclusive",
    ),
}

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

SCIP_VIEW_SCHEMA_MAP: dict[str, str] = {
    "scip_metadata": "scip_metadata_v1",
    "scip_index_stats": "scip_index_stats_v1",
    "scip_documents": "scip_documents_v1",
    "scip_document_texts": "scip_document_texts_v1",
    "scip_occurrences": "scip_occurrences_v1",
    "scip_symbol_information": "scip_symbol_information_v1",
    "scip_document_symbols": "scip_document_symbols_v1",
    "scip_external_symbol_information": "scip_external_symbol_information_v1",
    "scip_symbol_relationships": "scip_symbol_relationships_v1",
    "scip_signature_occurrences": "scip_signature_occurrences_v1",
    "scip_diagnostics": "scip_diagnostics_v1",
}

SCIP_VIEW_NAMES: tuple[str, ...] = tuple(SCIP_VIEW_SCHEMA_MAP)

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

AST_REQUIRED_FUNCTIONS: tuple[str, ...] = (
    "arrow_cast",
    "arrow_metadata",
    "arrow_typeof",
    "get_field",
    "list_extract",
    "map_entries",
    "map_extract",
    "named_struct",
    "unnest",
)

TREE_SITTER_REQUIRED_FUNCTIONS: tuple[str, ...] = (
    "arrow_cast",
    "arrow_metadata",
    "arrow_typeof",
    "get_field",
)

AST_REQUIRED_FUNCTION_SIGNATURES: dict[str, int] = {
    "arrow_cast": 2,
    "arrow_metadata": 1,
    "arrow_typeof": 1,
    "get_field": 2,
    "list_extract": 2,
    "map_entries": 1,
    "map_extract": 2,
    "named_struct": 2,
    "unnest": 1,
}

TREE_SITTER_REQUIRED_FUNCTION_SIGNATURES: dict[str, int] = {
    "arrow_cast": 2,
    "arrow_metadata": 1,
    "arrow_typeof": 1,
    "get_field": 2,
}

BYTECODE_REQUIRED_FUNCTIONS: tuple[str, ...] = (
    "arrow_cast",
    "arrow_metadata",
    "arrow_typeof",
    "concat_ws",
    "get_field",
    "map_entries",
    "map_extract",
    "map_keys",
    "map_values",
    "list_extract",
    "named_struct",
    "prefixed_hash64",
    "stable_id",
    "unnest",
)

BYTECODE_REQUIRED_FUNCTION_SIGNATURES: dict[str, int] = {
    "arrow_cast": 2,
    "arrow_metadata": 1,
    "arrow_typeof": 1,
    "get_field": 2,
    "list_extract": 2,
    "map_entries": 1,
    "map_extract": 2,
    "map_keys": 1,
    "map_values": 1,
    "named_struct": 2,
    "prefixed_hash64": 2,
    "stable_id": 2,
    "unnest": 1,
}

CST_REQUIRED_FUNCTIONS: tuple[str, ...] = (
    "arrow_cast",
    "arrow_metadata",
    "arrow_typeof",
    "concat_ws",
    "get_field",
    "map_entries",
    "map_extract",
    "named_struct",
    "prefixed_hash64",
    "stable_id",
    "unnest",
)

CST_REQUIRED_FUNCTION_SIGNATURES: dict[str, int] = {
    "arrow_cast": 2,
    "arrow_metadata": 1,
    "arrow_typeof": 1,
    "get_field": 2,
    "map_entries": 1,
    "map_extract": 2,
    "named_struct": 2,
    "prefixed_hash64": 2,
    "stable_id": 2,
    "unnest": 1,
}

SYMTABLE_REQUIRED_FUNCTIONS: tuple[str, ...] = (
    "arrow_metadata",
    "arrow_typeof",
    "concat_ws",
    "get_field",
    "map_entries",
    "prefixed_hash64",
    "unnest",
)

SYMTABLE_REQUIRED_FUNCTION_SIGNATURES: dict[str, int] = {
    "arrow_metadata": 1,
    "arrow_typeof": 1,
    "get_field": 2,
    "map_entries": 1,
    "prefixed_hash64": 2,
    "unnest": 1,
}

ENGINE_REQUIRED_FUNCTIONS: tuple[str, ...] = (
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
)

_STRING_TYPE_TOKENS = frozenset({"char", "string", "text", "utf8", "varchar"})
_INT_TYPE_TOKENS = frozenset({"int", "integer", "bigint", "smallint", "tinyint", "uint"})
_LIST_TYPE_TOKENS = frozenset({"array", "list"})
_MAP_TYPE_TOKENS = frozenset({"map"})

AST_REQUIRED_FUNCTION_SIGNATURE_TYPES: dict[str, tuple[frozenset[str] | None, ...]] = {
    "arrow_cast": (None, _STRING_TYPE_TOKENS),
    "arrow_metadata": (None, _STRING_TYPE_TOKENS),
    "get_field": (None, _STRING_TYPE_TOKENS),
    "list_extract": (_LIST_TYPE_TOKENS, _INT_TYPE_TOKENS),
    "map_entries": (_MAP_TYPE_TOKENS,),
    "map_extract": (_MAP_TYPE_TOKENS, _STRING_TYPE_TOKENS),
}


def _ast_optional_functions(view_names: Sequence[str]) -> set[str]:
    optional: set[str] = set()
    if any(name in view_names for name in AST_ATTRS_VIEW_NAMES):
        optional.add("map_entries")
    if "ast_span_metadata" in view_names:
        optional.add("arrow_metadata")
    return optional


def _ast_required_functions(view_names: Sequence[str]) -> tuple[str, ...]:
    optional = _ast_optional_functions(view_names)
    required: list[str] = []
    for name in AST_REQUIRED_FUNCTIONS:
        if name in {"map_entries", "arrow_metadata"} and name not in optional:
            continue
        required.append(name)
    return tuple(required)


def _ast_required_function_signatures(view_names: Sequence[str]) -> dict[str, int]:
    optional = _ast_optional_functions(view_names)
    required = dict(AST_REQUIRED_FUNCTION_SIGNATURES)
    if "map_entries" not in optional:
        required.pop("map_entries", None)
    if "arrow_metadata" not in optional:
        required.pop("arrow_metadata", None)
    return required


def _ast_required_function_signature_types(
    view_names: Sequence[str],
) -> dict[str, tuple[frozenset[str] | None, ...]]:
    optional = _ast_optional_functions(view_names)
    required = dict(AST_REQUIRED_FUNCTION_SIGNATURE_TYPES)
    if "map_entries" not in optional:
        required.pop("map_entries", None)
    if "arrow_metadata" not in optional:
        required.pop("arrow_metadata", None)
    return required


TREE_SITTER_REQUIRED_FUNCTION_SIGNATURE_TYPES: dict[str, tuple[frozenset[str] | None, ...]] = {
    "arrow_cast": (None, _STRING_TYPE_TOKENS),
    "arrow_metadata": (None, _STRING_TYPE_TOKENS),
    "get_field": (None, _STRING_TYPE_TOKENS),
}

CST_REQUIRED_FUNCTION_SIGNATURE_TYPES: dict[str, tuple[frozenset[str] | None, ...]] = {
    "arrow_cast": (None, _STRING_TYPE_TOKENS),
    "arrow_metadata": (None, _STRING_TYPE_TOKENS),
    "get_field": (None, _STRING_TYPE_TOKENS),
    "map_entries": (_MAP_TYPE_TOKENS,),
    "map_extract": (_MAP_TYPE_TOKENS, _STRING_TYPE_TOKENS),
    "stable_id": (_STRING_TYPE_TOKENS, _STRING_TYPE_TOKENS),
}

SYMTABLE_REQUIRED_FUNCTION_SIGNATURE_TYPES: dict[str, tuple[frozenset[str] | None, ...]] = {
    "arrow_metadata": (None, _STRING_TYPE_TOKENS),
    "get_field": (None, _STRING_TYPE_TOKENS),
    "map_entries": (_MAP_TYPE_TOKENS,),
}

BYTECODE_REQUIRED_FUNCTION_SIGNATURE_TYPES: dict[str, tuple[frozenset[str] | None, ...]] = {
    "arrow_cast": (None, _STRING_TYPE_TOKENS),
    "arrow_metadata": (None, _STRING_TYPE_TOKENS),
    "get_field": (None, _STRING_TYPE_TOKENS),
    "list_extract": (_LIST_TYPE_TOKENS, _INT_TYPE_TOKENS),
    "map_entries": (_MAP_TYPE_TOKENS,),
    "map_extract": (_MAP_TYPE_TOKENS, _STRING_TYPE_TOKENS),
    "map_keys": (_MAP_TYPE_TOKENS,),
    "map_values": (_MAP_TYPE_TOKENS,),
    "stable_id": (_STRING_TYPE_TOKENS, _STRING_TYPE_TOKENS),
}

AST_VIEW_REQUIRED_NON_NULL_FIELDS: tuple[str, ...] = ("file_id", "path")
CST_VIEW_REQUIRED_NON_NULL_FIELDS: tuple[str, ...] = ("file_id", "path")

SCIP_REQUIRED_FUNCTIONS: tuple[str, ...] = (
    "concat_ws",
    "stable_id",
)

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
        fields.append(pa.field(alias, field.type, field.nullable))
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
        fields.append(pa.field(field.name, field.type, field.nullable))
        seen.add(field.name)
    for field in context_fields:
        if field.name in seen:
            continue
        fields.append(field)
        seen.add(field.name)
    for field in row_struct:
        if field.name in seen:
            continue
        fields.append(pa.field(field.name, field.type, field.nullable))
        seen.add(field.name)
    return pa.schema(fields)


def _append_selection(
    selections: list[str],
    selected_names: set[str],
    *,
    name: str,
    expr: str,
) -> None:
    if name in selected_names:
        return
    selections.append(f"{expr} AS {name}")
    selected_names.add(name)


def _get_field_expr(expr: str, field_name: str) -> str:
    return f"get_field({expr}, '{field_name}')"


def _context_expr(
    *,
    root_alias: str,
    prefix_exprs: Mapping[str, str],
    ctx_path: str,
    dataset_name: str,
) -> str:
    prefix, sep, field_name = ctx_path.rpartition(".")
    if not sep:
        return f"{root_alias}.{field_name}"
    prefix_expr = prefix_exprs.get(prefix)
    if prefix_expr is None:
        msg = f"Nested context path {ctx_path!r} not resolved for {dataset_name!r}."
        raise KeyError(msg)
    return _get_field_expr(prefix_expr, field_name)


def _resolve_nested_path(
    *,
    root_schema: pa.Schema,
    path: str,
    root_alias: str,
    table: str,
) -> tuple[str, str, pa.StructType, dict[str, str]]:
    from_clause = f"FROM {table} AS {root_alias}"
    current_expr = root_alias
    current_is_root = True
    current_struct: pa.Schema | pa.StructType = root_schema
    prefix_exprs: dict[str, str] = {}
    parts = path.split(".")
    for idx, step in enumerate(parts):
        field = _field_from_container(current_struct, step)
        dtype = field.type
        prefix = ".".join(parts[: idx + 1])
        if _is_list_type(dtype):
            list_expr = (
                f"{current_expr}.{step}" if current_is_root else _get_field_expr(current_expr, step)
            )
            alias = f"n{idx}"
            item_alias = f"{alias}_item"
            from_clause += f"\nCROSS JOIN unnest({list_expr}) AS {alias}({item_alias})"
            current_expr = f"{alias}.{item_alias}"
            current_is_root = False
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
        current_expr = (
            f"{current_expr}.{step}" if current_is_root else _get_field_expr(current_expr, step)
        )
        current_is_root = False
        current_struct = dtype
        prefix_exprs[prefix] = current_expr
    if not isinstance(current_struct, pa.StructType):
        msg = f"Nested path {path!r} did not resolve to a struct."
        raise TypeError(msg)
    return from_clause, current_expr, current_struct, prefix_exprs


def nested_base_sql(name: str, *, table: str | None = None) -> str:
    """Return base SQL for a nested dataset path.

    Returns
    -------
    str
        Base SQL query string.
    """
    root, path = nested_path_for(name)
    root_schema = SCHEMA_REGISTRY[root]
    root_alias = "root"
    resolved_table = table or root
    from_clause, row_expr, row_struct, prefix_exprs = _resolve_nested_path(
        root_schema=root_schema,
        path=path,
        root_alias=root_alias,
        table=resolved_table,
    )
    identity_fields = identity_fields_for(name, root_schema, row_struct)
    context_fields = nested_context_for(name)
    selections: list[str] = []
    selected_names: set[str] = set()
    for field_name in identity_fields:
        _append_selection(
            selections,
            selected_names,
            name=field_name,
            expr=f"{root_alias}.{field_name}",
        )
    for alias, ctx_path in context_fields.items():
        expr = _context_expr(
            root_alias=root_alias,
            prefix_exprs=prefix_exprs,
            ctx_path=ctx_path,
            dataset_name=name,
        )
        _append_selection(selections, selected_names, name=alias, expr=expr)
    for field in row_struct:
        _append_selection(
            selections,
            selected_names,
            name=field.name,
            expr=_get_field_expr(row_expr, field.name),
        )
    select_sql = ",\n      ".join(selections)
    return f"SELECT\n      {select_sql}\n{from_clause}"


def _nested_df_builder(sql: str) -> Callable[[SessionContext], DataFrame]:
    def _build(ctx: SessionContext) -> DataFrame:
        module = importlib.import_module("datafusion_engine.df_builder")
        df_from_sqlglot = getattr(module, "df_from_sqlglot", None)
        if not callable(df_from_sqlglot):
            msg = "DataFrame builder is unavailable for nested view registration."
            raise TypeError(msg)
        udf_module = importlib.import_module("datafusion_engine.udf_registry")
        register_udfs = getattr(udf_module, "register_datafusion_udfs", None)
        if callable(register_udfs):
            register_udfs(ctx)
        policy = resolve_sqlglot_policy(name="datafusion_compile")
        expr = parse_sql_strict(sql, dialect=policy.read_dialect)
        df = df_from_sqlglot(ctx, expr)
        return cast("DataFrame", df)

    return _build


def nested_view_spec(name: str, *, table: str | None = None) -> ViewSpec:
    """Return a ViewSpec for a nested dataset.

    Returns
    -------
    ViewSpec
        View specification with schema and base SQL.
    """
    schema = nested_schema_for(name, allow_derived=True)
    sql = nested_base_sql(name, table=table)
    return ViewSpec(
        name=name,
        sql=None,
        schema=schema,
        builder=_nested_df_builder(sql),
    )


def nested_view_specs(*, table: str | None = None) -> tuple[ViewSpec, ...]:
    """Return ViewSpecs for all nested datasets.

    Returns
    -------
    tuple[ViewSpec, ...]
        View specifications for nested datasets.
    """
    return tuple(nested_view_spec(name, table=table) for name in nested_dataset_names())


def symtable_derived_view_specs(ctx: SessionContext) -> tuple[ViewSpec, ...]:
    """Return ViewSpecs for symtable-derived views.

    Returns
    -------
    tuple[ViewSpec, ...]
        View specifications for symtable-derived views.
    """
    from datafusion_engine.symtable_views import (
        symtable_bindings_df,
        symtable_def_sites_df,
        symtable_type_param_edges_df,
        symtable_type_params_df,
        symtable_use_sites_df,
    )

    return (
        view_spec_from_builder(
            ctx,
            name="symtable_bindings",
            builder=symtable_bindings_df,
            sql=None,
        ),
        view_spec_from_builder(
            ctx,
            name="symtable_def_sites",
            builder=symtable_def_sites_df,
            sql=None,
        ),
        view_spec_from_builder(
            ctx,
            name="symtable_use_sites",
            builder=symtable_use_sites_df,
            sql=None,
        ),
        view_spec_from_builder(
            ctx,
            name="symtable_type_params",
            builder=symtable_type_params_df,
            sql=None,
        ),
        view_spec_from_builder(
            ctx,
            name="symtable_type_param_edges",
            builder=symtable_type_param_edges_df,
            sql=None,
        ),
    )


def symtable_binding_resolution_view_specs(ctx: SessionContext) -> tuple[ViewSpec, ...]:
    """Return ViewSpecs for symtable binding resolution views.

    Returns
    -------
    tuple[ViewSpec, ...]
        View specifications for binding resolution views.
    """
    from datafusion_engine.symtable_views import symtable_binding_resolutions_df

    return (
        view_spec_from_builder(
            ctx,
            name="symtable_binding_resolutions",
            builder=symtable_binding_resolutions_df,
            sql=None,
        ),
    )


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
    _sql_with_options(ctx, f"SELECT arrow_typeof(*) AS row_type FROM {name} LIMIT 1").collect()


def _require_semantic_type(
    ctx: SessionContext,
    *,
    table_name: str,
    column_name: str,
    expected: str,
) -> None:
    rows = (
        _sql_with_options(
            ctx,
            "SELECT arrow_metadata("
            f"{column_name}, '{SEMANTIC_TYPE_META.decode('utf-8')}'"
            f") AS semantic_type FROM {table_name} LIMIT 1",
        )
        .to_arrow_table()
        .to_pylist()
    )
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
    """Validate AST view schemas using DataFusion introspection.

    Raises
    ------
    ValueError
        Raised when view schemas fail validation.
    """
    resolved_views = tuple(dict.fromkeys(view_names)) if view_names is not None else AST_VIEW_NAMES
    sql_options = sql_options_for_profile(None)
    errors: dict[str, str] = {}
    function_catalog = _function_catalog(ctx)
    _validate_required_functions(
        ctx,
        required=_ast_required_functions(resolved_views),
        errors=errors,
        catalog=function_catalog,
    )
    _validate_function_signatures(
        ctx,
        required=_ast_required_function_signatures(resolved_views),
        errors=errors,
        catalog=function_catalog,
    )
    _validate_function_signature_types(
        ctx,
        required=_ast_required_function_signature_types(resolved_views),
        errors=errors,
        catalog=function_catalog,
    )
    introspector = SchemaIntrospector(ctx, sql_options=sql_options)
    for name in resolved_views:
        _validate_ast_view_outputs(ctx, introspector=introspector, name=name, errors=errors)
    for name in resolved_views:
        expected = AST_VIEW_COLUMN_MAP.get(name)
        if not expected:
            continue
        try:
            actual = introspector.table_column_names(name)
            missing = sorted(set(expected) - actual)
            if missing:
                errors[f"{name}_information_schema"] = f"Missing columns: {missing}."
        except (RuntimeError, TypeError, ValueError, KeyError) as exc:
            errors[f"{name}_information_schema"] = str(exc)
    try:
        _sql_with_options(
            ctx,
            "SELECT arrow_typeof(nodes) AS nodes_type FROM ast_files_v1 LIMIT 1",
        ).collect()
        _sql_with_options(
            ctx,
            "SELECT arrow_typeof(edges) AS edges_type FROM ast_files_v1 LIMIT 1",
        ).collect()
        _sql_with_options(
            ctx,
            "SELECT arrow_typeof(errors) AS errors_type FROM ast_files_v1 LIMIT 1",
        ).collect()
        _sql_with_options(
            ctx,
            "SELECT arrow_typeof(docstrings) AS docstrings_type FROM ast_files_v1 LIMIT 1",
        ).collect()
        _sql_with_options(
            ctx,
            "SELECT arrow_typeof(imports) AS imports_type FROM ast_files_v1 LIMIT 1",
        ).collect()
        _sql_with_options(
            ctx,
            "SELECT arrow_typeof(defs) AS defs_type FROM ast_files_v1 LIMIT 1",
        ).collect()
        _sql_with_options(
            ctx,
            "SELECT arrow_typeof(calls) AS calls_type FROM ast_files_v1 LIMIT 1",
        ).collect()
        _sql_with_options(
            ctx,
            "SELECT arrow_typeof(type_ignores) AS type_ignores_type FROM ast_files_v1 LIMIT 1",
        ).collect()
    except (RuntimeError, TypeError, ValueError) as exc:
        errors["ast_files_v1"] = str(exc)
    if "ast_span_metadata" in resolved_views:
        _validate_ast_span_metadata(ctx, errors)
    if errors:
        msg = f"AST view validation failed: {errors}."
        raise ValueError(msg)


def validate_ts_views(ctx: SessionContext) -> None:
    """Validate tree-sitter view schemas using DataFusion introspection.

    Raises
    ------
    ValueError
        Raised when view schemas fail validation.
    """
    errors: dict[str, str] = {}
    sql_options = sql_options_for_profile(None)
    function_catalog = _function_catalog(ctx)
    _validate_required_functions(
        ctx,
        required=TREE_SITTER_REQUIRED_FUNCTIONS,
        errors=errors,
        catalog=function_catalog,
    )
    _validate_function_signatures(
        ctx,
        required=TREE_SITTER_REQUIRED_FUNCTION_SIGNATURES,
        errors=errors,
        catalog=function_catalog,
    )
    _validate_function_signature_types(
        ctx,
        required=TREE_SITTER_REQUIRED_FUNCTION_SIGNATURE_TYPES,
        errors=errors,
        catalog=function_catalog,
    )
    introspector = SchemaIntrospector(ctx, sql_options=sql_options)
    for name in TREE_SITTER_VIEW_NAMES:
        _validate_ts_view_outputs(ctx, introspector=introspector, name=name, errors=errors)
    for table_name in (
        "ts_nodes",
        "ts_errors",
        "ts_missing",
        "ts_captures",
        "ts_defs",
        "ts_calls",
        "ts_imports",
        "ts_docstrings",
    ):
        try:
            _require_semantic_type(
                ctx,
                table_name=table_name,
                column_name="span",
                expected=SPAN_TYPE_INFO.name,
            )
        except (RuntimeError, TypeError, ValueError) as exc:
            errors[f"{table_name}_span_type"] = str(exc)
    try:
        _sql_with_options(
            ctx,
            "SELECT arrow_typeof(nodes) AS nodes_type FROM tree_sitter_files_v1 LIMIT 1",
        ).collect()
        _sql_with_options(
            ctx,
            "SELECT arrow_typeof(edges) AS edges_type FROM tree_sitter_files_v1 LIMIT 1",
        ).collect()
        _sql_with_options(
            ctx,
            "SELECT arrow_typeof(errors) AS errors_type FROM tree_sitter_files_v1 LIMIT 1",
        ).collect()
        _sql_with_options(
            ctx,
            "SELECT arrow_typeof(missing) AS missing_type FROM tree_sitter_files_v1 LIMIT 1",
        ).collect()
        _sql_with_options(
            ctx,
            "SELECT arrow_typeof(captures) AS captures_type FROM tree_sitter_files_v1 LIMIT 1",
        ).collect()
        _sql_with_options(
            ctx,
            "SELECT arrow_typeof(defs) AS defs_type FROM tree_sitter_files_v1 LIMIT 1",
        ).collect()
        _sql_with_options(
            ctx,
            "SELECT arrow_typeof(calls) AS calls_type FROM tree_sitter_files_v1 LIMIT 1",
        ).collect()
        _sql_with_options(
            ctx,
            "SELECT arrow_typeof(imports) AS imports_type FROM tree_sitter_files_v1 LIMIT 1",
        ).collect()
        _sql_with_options(
            ctx,
            "SELECT arrow_typeof(docstrings) AS docstrings_type FROM tree_sitter_files_v1 LIMIT 1",
        ).collect()
        _sql_with_options(
            ctx,
            "SELECT arrow_typeof(stats) AS stats_type FROM tree_sitter_files_v1 LIMIT 1",
        ).collect()
    except (RuntimeError, TypeError, ValueError) as exc:
        errors["tree_sitter_files_v1"] = str(exc)
    _validate_ts_span_metadata(ctx, errors)
    if errors:
        msg = f"Tree-sitter view validation failed: {errors}."
        raise ValueError(msg)


def validate_symtable_views(ctx: SessionContext) -> None:
    """Validate symtable view schemas using DataFusion introspection.

    Raises
    ------
    ValueError
        Raised when view schemas fail validation.
    """
    errors: dict[str, str] = {}
    sql_options = sql_options_for_profile(None)
    function_catalog = _function_catalog(ctx)
    _validate_required_functions(
        ctx,
        required=SYMTABLE_REQUIRED_FUNCTIONS,
        errors=errors,
        catalog=function_catalog,
    )
    _validate_function_signatures(
        ctx,
        required=SYMTABLE_REQUIRED_FUNCTION_SIGNATURES,
        errors=errors,
        catalog=function_catalog,
    )
    _validate_function_signature_types(
        ctx,
        required=SYMTABLE_REQUIRED_FUNCTION_SIGNATURE_TYPES,
        errors=errors,
        catalog=function_catalog,
    )
    introspector = SchemaIntrospector(ctx, sql_options=sql_options)
    for name in SYMTABLE_VIEW_NAMES:
        try:
            _sql_with_options(ctx, f"DESCRIBE SELECT * FROM {name}").collect()
        except (RuntimeError, TypeError, ValueError) as exc:
            errors[name] = str(exc)
    for name in SYMTABLE_VIEW_NAMES:
        try:
            expected = set(schema_for(name).names)
            actual = introspector.table_column_names(name)
            missing = sorted(expected - actual)
            if missing:
                errors[f"{name}_information_schema"] = f"Missing columns: {missing}."
        except (RuntimeError, TypeError, ValueError, KeyError) as exc:
            errors[f"{name}_information_schema"] = str(exc)
    try:
        _sql_with_options(
            ctx,
            "SELECT arrow_typeof(blocks) AS blocks_type FROM symtable_files_v1 LIMIT 1",
        ).collect()
        _sql_with_options(
            ctx,
            "SELECT arrow_typeof(blocks.symbols) AS symbols_type FROM symtable_files_v1 LIMIT 1",
        ).collect()
        _sql_with_options(
            ctx,
            "SELECT arrow_metadata(blocks.span_hint, 'line_base') AS span_line_base "
            "FROM symtable_files_v1 LIMIT 1",
        ).collect()
        _sql_with_options(
            ctx,
            "SELECT arrow_metadata(blocks.span_hint, 'col_unit') AS span_col_unit "
            "FROM symtable_files_v1 LIMIT 1",
        ).collect()
        _sql_with_options(
            ctx,
            "SELECT arrow_metadata(blocks.span_hint, 'end_exclusive') AS span_end_exclusive "
            "FROM symtable_files_v1 LIMIT 1",
        ).collect()
    except (RuntimeError, TypeError, ValueError) as exc:
        errors["symtable_files_v1"] = str(exc)
    if errors:
        msg = f"Symtable view validation failed: {errors}."
        raise ValueError(msg)


def validate_scip_views(ctx: SessionContext) -> None:
    """Validate SCIP view schemas using DataFusion introspection.

    Raises
    ------
    ValueError
        Raised when view schemas fail validation.
    """
    errors: dict[str, str] = {}
    sql_options = sql_options_for_profile(None)
    function_catalog = _function_catalog(ctx)
    _validate_required_functions(
        ctx,
        required=SCIP_REQUIRED_FUNCTIONS,
        errors=errors,
        catalog=function_catalog,
    )
    introspector = SchemaIntrospector(ctx, sql_options=sql_options)
    for view_name in SCIP_VIEW_NAMES:
        try:
            rows = introspector.describe_query(f"SELECT * FROM {view_name}")
        except (RuntimeError, TypeError, ValueError) as exc:
            errors[view_name] = str(exc)
            continue
        columns = _describe_column_names(rows)
        invalid = _invalid_output_names(columns)
        if invalid:
            errors[f"{view_name}_invalid_columns"] = f"Invalid column names: {invalid}."
    for view_name, schema_name in SCIP_VIEW_SCHEMA_MAP.items():
        try:
            expected = set(schema_for(schema_name).names)
            actual = introspector.table_column_names(view_name)
            missing = sorted(expected - actual)
            if missing:
                errors[f"{view_name}_information_schema"] = f"Missing columns: {missing}."
        except (RuntimeError, TypeError, ValueError, KeyError) as exc:
            errors[f"{view_name}_information_schema"] = str(exc)
    if errors:
        msg = f"SCIP view validation failed: {errors}."
        raise ValueError(msg)


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
    errors: dict[str, str] = {}
    function_catalog = _function_catalog(ctx)
    _validate_required_functions(
        ctx,
        required=BYTECODE_REQUIRED_FUNCTIONS,
        errors=errors,
        catalog=function_catalog,
    )
    _validate_function_signatures(
        ctx,
        required=BYTECODE_REQUIRED_FUNCTION_SIGNATURES,
        errors=errors,
        catalog=function_catalog,
    )
    _validate_function_signature_types(
        ctx,
        required=BYTECODE_REQUIRED_FUNCTION_SIGNATURE_TYPES,
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
    errors: dict[str, str] = {}
    sql_options = sql_options_for_profile(None)
    function_catalog = _function_catalog(ctx)
    _validate_required_functions(
        ctx,
        required=CST_REQUIRED_FUNCTIONS,
        errors=errors,
        catalog=function_catalog,
    )
    _validate_function_signatures(
        ctx,
        required=CST_REQUIRED_FUNCTION_SIGNATURES,
        errors=errors,
        catalog=function_catalog,
    )
    _validate_function_signature_types(
        ctx,
        required=CST_REQUIRED_FUNCTION_SIGNATURE_TYPES,
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
    _validate_required_functions(
        ctx,
        required=CST_REQUIRED_FUNCTIONS,
        errors=errors,
        catalog=function_catalog,
    )
    _validate_function_signatures(
        ctx,
        required=CST_REQUIRED_FUNCTION_SIGNATURES,
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
    errors: dict[str, str] = {}
    function_catalog = _function_catalog(ctx)
    _validate_required_functions(
        ctx,
        required=SYMTABLE_REQUIRED_FUNCTIONS,
        errors=errors,
        catalog=function_catalog,
    )
    _validate_function_signatures(
        ctx,
        required=SYMTABLE_REQUIRED_FUNCTION_SIGNATURES,
        errors=errors,
        catalog=function_catalog,
    )
    _validate_function_signature_types(
        ctx,
        required=SYMTABLE_REQUIRED_FUNCTION_SIGNATURE_TYPES,
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
    errors: dict[str, str] = {}
    function_catalog = _function_catalog(ctx)
    _validate_required_functions(
        ctx,
        required=BYTECODE_REQUIRED_FUNCTIONS,
        errors=errors,
        catalog=function_catalog,
    )
    _validate_function_signatures(
        ctx,
        required=BYTECODE_REQUIRED_FUNCTION_SIGNATURES,
        errors=errors,
        catalog=function_catalog,
    )
    _validate_function_signature_types(
        ctx,
        required=BYTECODE_REQUIRED_FUNCTION_SIGNATURE_TYPES,
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
    _validate_required_functions(
        ctx,
        required=ENGINE_REQUIRED_FUNCTIONS,
        errors=errors,
        catalog=function_catalog,
    )
    if errors:
        msg = f"Required engine functions validation failed: {errors}."
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
    return tuple(sorted({*SCHEMA_REGISTRY, *nested_schema_names()}))


def has_schema(name: str) -> bool:
    """Return whether a schema is registered.

    Returns
    -------
    bool
        ``True`` when the schema name is registered.
    """
    return name in SCHEMA_REGISTRY or is_intrinsic_nested_dataset(name)


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
    if is_intrinsic_nested_dataset(name):
        return nested_schema_for(name)
    msg = f"Unknown DataFusion schema: {name!r}."
    raise KeyError(msg)


def register_schema(ctx: SessionContext, name: str, schema: pa.Schema) -> None:
    """Register a schema in a DataFusion SessionContext using an empty batch."""
    arrays = [pa.array([], type=field.type) for field in schema]
    batch = pa.record_batch(arrays, schema=schema)
    ctx.register_record_batches(name, [[batch]])


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
    for name in nested_schema_names():
        register_schema(ctx, name, nested_schema_for(name))


__all__ = [
    "AST_FILES_SCHEMA",
    "AST_VIEW_NAMES",
    "BYTECODE_FILES_SCHEMA",
    "BYTECODE_VIEW_NAMES",
    "CST_VIEW_NAMES",
    "DIAG_DETAILS_TYPE",
    "DIAG_DETAIL_STRUCT",
    "DIAG_TAGS_TYPE",
    "ENGINE_REQUIRED_FUNCTIONS",
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
    "SCIP_VIEW_SCHEMA_MAP",
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
    "nested_base_sql",
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
    "symtable_binding_resolution_view_specs",
    "symtable_derived_view_specs",
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
]
