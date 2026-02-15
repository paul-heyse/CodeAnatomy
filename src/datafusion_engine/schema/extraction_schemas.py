"""Schema definitions for extraction evidence tables.

Arrow schema contracts for extraction outputs across all evidence sources
(AST, LibCST, bytecode, SCIP, symtable, tree-sitter, repo files).

DataFusion best practice: Use explicit schemas rather than inference
for stability. Schemas should declare NOT NULL constraints via Arrow
Field.nullable=False and use schema metadata for semantic annotations.

All extraction schemas use byte span types (bstart/bend) as canonical
coordinates. These are defined in datafusion_engine.arrow.semantic.

Schema drift and evolution:
Schemas registered in this module can leverage scan-time schema adapters when
the runtime profile enables ``enable_schema_evolution_adapter=True``. Schema
adapters normalize physical batches at the TableProvider boundary, handling
column reordering, type coercion, and missing/extra columns during physical
plan execution.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from typing import TYPE_CHECKING, cast

import pyarrow as pa

from arrow_utils.core.ordering import OrderingLevel
from arrow_utils.core.schema_constants import (
    DEFAULT_VALUE_META,
    KEY_FIELDS_META,
    REQUIRED_NON_NULL_META,
    SCHEMA_META_NAME,
    SCHEMA_META_VERSION,
)
from datafusion_engine.arrow.build import list_view_type, struct_type
from datafusion_engine.arrow.field_builders import (
    bool_field,
    int32_field,
    int64_field,
    list_field,
    string_field,
)
from datafusion_engine.arrow.metadata import (
    function_requirements_metadata_spec,
    optional_functions_from_metadata,
    ordering_metadata_spec,
    required_function_signature_types_from_metadata,
    required_function_signatures_from_metadata,
    required_functions_from_metadata,
)
from datafusion_engine.arrow.metadata_codec import encode_metadata_list
from datafusion_engine.arrow.schema import (
    version_field,
)
from datafusion_engine.arrow.semantic import (
    apply_semantic_types,
    byte_span_type,
    span_metadata,
    span_type,
)
from schema_spec.file_identity import FILE_ID_FIELD, FILE_SHA256_FIELD, PATH_FIELD
from utils.registry_protocol import ImmutableRegistry

_LOGGER = logging.getLogger(__name__)
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


def _attrs_field(name: str = "attrs") -> pa.Field:
    return pa.field(name, ATTRS_T, metadata=_DEFAULT_ATTRS_META)


def _bytecode_attrs_field(name: str = "attrs") -> pa.Field:
    return pa.field(name, ATTRS_T, metadata=_DEFAULT_ATTRS_META)


def default_attrs_value() -> dict[str, str]:
    """Return the default attrs map from schema metadata.

    Returns:
    -------
    dict[str, str]
        Default attrs mapping derived from schema metadata.
    """
    return dict(_DEFAULT_ATTRS_VALUE)


# ---------------------------------------------------------------------------
# SCIP extraction schemas
# ---------------------------------------------------------------------------

SCIP_METADATA_SCHEMA = pa.schema(
    [
        string_field("index_id"),
        int32_field("protocol_version"),
        string_field("tool_name"),
        string_field("tool_version"),
        list_field("tool_arguments", pa.string()),
        string_field("project_root"),
        int32_field("text_document_encoding"),
        string_field("project_name"),
        string_field("project_version"),
        string_field("project_namespace"),
    ]
)

SCIP_INDEX_STATS_SCHEMA = pa.schema(
    [
        string_field("index_id"),
        int64_field("document_count"),
        int64_field("occurrence_count"),
        int64_field("diagnostic_count"),
        int64_field("symbol_count"),
        int64_field("external_symbol_count"),
        int64_field("missing_position_encoding_count"),
        int64_field("document_text_count"),
        int64_field("document_text_bytes"),
    ]
)

SCIP_DOCUMENTS_SCHEMA = pa.schema(
    [
        string_field("index_id"),
        string_field("document_id"),
        string_field("path"),
        string_field("language"),
        int32_field("position_encoding"),
    ]
)

SCIP_DOCUMENT_TEXTS_SCHEMA = pa.schema(
    [
        string_field("document_id"),
        string_field("path"),
        string_field("text"),
    ]
)

SCIP_SYMBOL_INFO_FIELDS: tuple[pa.Field, ...] = (
    string_field("symbol"),
    string_field("display_name"),
    int32_field("kind"),
    string_field("kind_name"),
    string_field("enclosing_symbol"),
    list_field("documentation", pa.string()),
    string_field("signature_text"),
    string_field("signature_language"),
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
        string_field("symbol"),
        string_field("related_symbol"),
        bool_field("is_reference"),
        bool_field("is_implementation"),
        bool_field("is_type_definition"),
        bool_field("is_definition"),
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

SCIP_OCCURRENCES_NORM_SCHEMA = pa.schema(
    [
        *SCIP_OCCURRENCES_SCHEMA,
        ("bstart", pa.int64()),
        ("bend", pa.int64()),
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

SCIP_METADATA_T = pa.struct(
    [field for field in SCIP_METADATA_SCHEMA if field.name not in {"index_id", "index_path"}]
)
SCIP_INDEX_STATS_T = pa.struct(
    [field for field in SCIP_INDEX_STATS_SCHEMA if field.name not in {"index_id", "index_path"}]
)
SCIP_SYMBOL_INFO_T = pa.struct(SCIP_SYMBOL_INFO_FIELDS)
SCIP_OCCURRENCE_T = pa.struct(
    [field for field in SCIP_OCCURRENCES_SCHEMA if field.name not in {"document_id", "path"}]
)
SCIP_DIAGNOSTIC_T = pa.struct(
    [field for field in SCIP_DIAGNOSTICS_SCHEMA if field.name not in {"document_id", "path"}]
)
SCIP_SYMBOL_RELATIONSHIP_T = pa.struct(list(SCIP_SYMBOL_RELATIONSHIPS_SCHEMA))
SCIP_SIGNATURE_OCCURRENCE_T = pa.struct(list(SCIP_SIGNATURE_OCCURRENCES_SCHEMA))
SCIP_DOCUMENT_T = pa.struct(
    [
        pa.field("document_id", pa.string()),
        pa.field("path", pa.string()),
        pa.field("language", pa.string()),
        pa.field("position_encoding", pa.int32()),
        pa.field("text", pa.string()),
        pa.field("symbols", pa.list_(SCIP_SYMBOL_INFO_T)),
        pa.field("occurrences", pa.list_(SCIP_OCCURRENCE_T)),
        pa.field("diagnostics", pa.list_(SCIP_DIAGNOSTIC_T)),
    ]
)
SCIP_INDEX_SCHEMA = pa.schema(
    [
        pa.field("index_id", pa.string()),
        pa.field("index_path", pa.string()),
        pa.field("metadata", SCIP_METADATA_T),
        pa.field("index_stats", SCIP_INDEX_STATS_T),
        pa.field("documents", pa.list_(SCIP_DOCUMENT_T)),
        pa.field("symbol_information", pa.list_(SCIP_SYMBOL_INFO_T)),
        pa.field("external_symbol_information", pa.list_(SCIP_SYMBOL_INFO_T)),
        pa.field("symbol_relationships", pa.list_(SCIP_SYMBOL_RELATIONSHIP_T)),
        pa.field("signature_occurrences", pa.list_(SCIP_SIGNATURE_OCCURRENCE_T)),
    ]
)

# ---------------------------------------------------------------------------
# CST (LibCST) extraction schemas
# ---------------------------------------------------------------------------

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
        FILE_ID_FIELD,
        PATH_FIELD,
        FILE_SHA256_FIELD,
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
        ("schema_identity_hash", pa.string()),
    ]
)

CST_PARSE_ERROR_T = pa.struct(
    [
        FILE_ID_FIELD,
        PATH_FIELD,
        FILE_SHA256_FIELD,
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
        FILE_ID_FIELD,
        PATH_FIELD,
        FILE_SHA256_FIELD,
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
        FILE_ID_FIELD,
        PATH_FIELD,
        FILE_SHA256_FIELD,
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
        FILE_ID_FIELD,
        PATH_FIELD,
        FILE_SHA256_FIELD,
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
        FILE_ID_FIELD,
        PATH_FIELD,
        FILE_SHA256_FIELD,
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
        FILE_ID_FIELD,
        PATH_FIELD,
        FILE_SHA256_FIELD,
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
        FILE_ID_FIELD,
        PATH_FIELD,
        FILE_SHA256_FIELD,
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
        FILE_ID_FIELD,
        PATH_FIELD,
        FILE_SHA256_FIELD,
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
        FILE_ID_FIELD,
        PATH_FIELD,
        FILE_SHA256_FIELD,
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
        PATH_FIELD,
        FILE_ID_FIELD,
        FILE_SHA256_FIELD,
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

# ---------------------------------------------------------------------------
# Repo files schema
# ---------------------------------------------------------------------------

REPO_FILES_SCHEMA = pa.schema(
    [
        FILE_ID_FIELD,
        PATH_FIELD,
        FILE_SHA256_FIELD,
        ("abs_path", pa.string()),
        ("size_bytes", pa.int64()),
        ("mtime_ns", pa.int64()),
        ("encoding", pa.string()),
    ]
)

# ---------------------------------------------------------------------------
# AST extraction schemas
# ---------------------------------------------------------------------------

AST_SPAN_META: dict[bytes, bytes] = span_metadata()

TREE_SITTER_SPAN_META: dict[bytes, bytes] = span_metadata()

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
        PATH_FIELD,
        FILE_ID_FIELD,
        FILE_SHA256_FIELD,
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

# ---------------------------------------------------------------------------
# Tree-sitter extraction schemas
# ---------------------------------------------------------------------------

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

# ---------------------------------------------------------------------------
# Symtable extraction schemas
# ---------------------------------------------------------------------------

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

SYMTABLE_SPAN_META: dict[bytes, bytes] = span_metadata(col_unit="byte")

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

# ---------------------------------------------------------------------------
# Bytecode extraction schemas
# ---------------------------------------------------------------------------

BYTECODE_SPAN_META: dict[bytes, bytes] = span_metadata(col_unit="byte")

BYTECODE_LINE_META: dict[bytes, bytes] = {b"line_base": b"1"}

_BYTECODE_IDENTITY_FIELDS: tuple[str, ...] = ("file_id", "path")
_BYTECODE_ORDERING_META = ordering_metadata_spec(
    OrderingLevel.EXPLICIT,
    keys=(("path", "ascending"), ("file_id", "ascending")),
)
_BYTECODE_CONSTRAINT_META = {
    REQUIRED_NON_NULL_META: encode_metadata_list(_BYTECODE_IDENTITY_FIELDS),
    KEY_FIELDS_META: encode_metadata_list(_BYTECODE_IDENTITY_FIELDS),
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
        PATH_FIELD,
        FILE_ID_FIELD,
        FILE_SHA256_FIELD,
        pa.field("code_objects", pa.list_(BYTECODE_CODE_OBJ_T)),
        pa.field("errors", pa.list_(BYTECODE_ERROR_T)),
        _bytecode_attrs_field(),
    ]
)

# ---------------------------------------------------------------------------
# Schema metadata utilities
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Apply schema-level metadata to file schemas
# ---------------------------------------------------------------------------

_AST_IDENTITY_FIELDS: tuple[str, ...] = ("file_id", "path")
_AST_ORDERING_META = ordering_metadata_spec(
    OrderingLevel.EXPLICIT,
    keys=(("path", "ascending"), ("file_id", "ascending")),
)
_AST_CONSTRAINT_META = {
    REQUIRED_NON_NULL_META: encode_metadata_list(_AST_IDENTITY_FIELDS),
    KEY_FIELDS_META: encode_metadata_list(_AST_IDENTITY_FIELDS),
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
    apply_semantic_types(AST_FILES_SCHEMA),
    extra_metadata=_AST_SCHEMA_META,
)
BYTECODE_FILES_SCHEMA = _schema_with_metadata(
    "bytecode_files_v1",
    apply_semantic_types(BYTECODE_FILES_SCHEMA),
    extra_metadata=BYTECODE_SCHEMA_META,
)
_LIBCST_ORDERING_META = ordering_metadata_spec(
    OrderingLevel.EXPLICIT,
    keys=(("path", "ascending"), ("file_id", "ascending")),
)
_LIBCST_IDENTITY_FIELDS = ("file_id", "path")
_LIBCST_CONSTRAINT_META = {
    REQUIRED_NON_NULL_META: encode_metadata_list(_LIBCST_IDENTITY_FIELDS),
    KEY_FIELDS_META: encode_metadata_list(_LIBCST_IDENTITY_FIELDS),
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
SCIP_OCCURRENCES_NORM_SCHEMA = _schema_with_metadata(
    "scip_occurrences_norm_v1",
    SCIP_OCCURRENCES_NORM_SCHEMA,
)
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
_SCIP_INDEX_IDENTITY_FIELDS = ("index_id", "index_path")
_SCIP_INDEX_ORDERING_META = ordering_metadata_spec(
    OrderingLevel.EXPLICIT,
    keys=(("index_path", "ascending"), ("index_id", "ascending")),
)
_SCIP_INDEX_CONSTRAINT_META = {
    REQUIRED_NON_NULL_META: encode_metadata_list(("index_path",)),
    KEY_FIELDS_META: encode_metadata_list(_SCIP_INDEX_IDENTITY_FIELDS),
}
_SCIP_INDEX_SCHEMA_META = dict(_SCIP_INDEX_ORDERING_META.schema_metadata)
_SCIP_INDEX_SCHEMA_META.update(_SCIP_INDEX_CONSTRAINT_META)
SCIP_INDEX_SCHEMA = _schema_with_metadata(
    "scip_index_v1",
    SCIP_INDEX_SCHEMA,
    extra_metadata=_SCIP_INDEX_SCHEMA_META,
)
_SYMTABLE_IDENTITY_FIELDS = ("file_id", "path")
_SYMTABLE_ORDERING_META = ordering_metadata_spec(
    OrderingLevel.EXPLICIT,
    keys=(("path", "ascending"), ("file_id", "ascending")),
)
_SYMTABLE_CONSTRAINT_META = {
    REQUIRED_NON_NULL_META: encode_metadata_list(_SYMTABLE_IDENTITY_FIELDS),
    KEY_FIELDS_META: encode_metadata_list(_SYMTABLE_IDENTITY_FIELDS),
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
    REQUIRED_NON_NULL_META: encode_metadata_list(_TREE_SITTER_IDENTITY_FIELDS),
    KEY_FIELDS_META: encode_metadata_list(_TREE_SITTER_IDENTITY_FIELDS),
}
_TREE_SITTER_SCHEMA_META = dict(_TREE_SITTER_ORDERING_META.schema_metadata)
_TREE_SITTER_SCHEMA_META.update(_TREE_SITTER_CONSTRAINT_META)
TREE_SITTER_FILES_SCHEMA = _schema_with_metadata(
    "tree_sitter_files_v1",
    apply_semantic_types(TREE_SITTER_FILES_SCHEMA),
    extra_metadata=_TREE_SITTER_SCHEMA_META,
)

# ---------------------------------------------------------------------------
# Relationship schemas
# ---------------------------------------------------------------------------

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
            version_field(),
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
            version_field(),
            pa.field("plan_fingerprint", pa.string(), nullable=False),
            pa.field("schema_identity_hash", pa.string(), nullable=False),
            pa.field("profile_hash", pa.string(), nullable=False),
            pa.field("writer_strategy", pa.string(), nullable=False),
            pa.field("input_fingerprints", pa.list_(pa.string()), nullable=False),
        ]
    ),
)


def _relationship_schema(name: str, entity_id_col: str) -> pa.Schema:
    return _schema_with_metadata(
        name,
        pa.schema(
            [
                pa.field(entity_id_col, pa.string()),
                pa.field("symbol", pa.string()),
                pa.field("symbol_roles", pa.int32()),
                pa.field("path", pa.string()),
                pa.field("edge_owner_file_id", pa.string()),
                pa.field("bstart", pa.int64()),
                pa.field("bend", pa.int64()),
                pa.field("resolution_method", pa.string()),
                pa.field("confidence", pa.float64()),
                pa.field("score", pa.float64()),
                pa.field("task_name", pa.string()),
                pa.field("task_priority", pa.int32()),
            ]
        ),
    )


REL_NAME_SYMBOL_SCHEMA = _relationship_schema("rel_name_symbol", "entity_id")
REL_IMPORT_SYMBOL_SCHEMA = _relationship_schema("rel_import_symbol", "entity_id")
REL_DEF_SYMBOL_SCHEMA = _relationship_schema("rel_def_symbol", "entity_id")
REL_CALLSITE_SYMBOL_SCHEMA = _relationship_schema("rel_callsite_symbol", "entity_id")

RELATIONSHIP_SCHEMA_BY_NAME: ImmutableRegistry[str, pa.Schema] = ImmutableRegistry.from_dict(
    {
        "rel_name_symbol": REL_NAME_SYMBOL_SCHEMA,
        "rel_import_symbol": REL_IMPORT_SYMBOL_SCHEMA,
        "rel_def_symbol": REL_DEF_SYMBOL_SCHEMA,
        "rel_callsite_symbol": REL_CALLSITE_SYMBOL_SCHEMA,
    }
)

PARAM_FILE_IDS_SCHEMA = _schema_with_metadata(
    "param_file_ids_v1",
    pa.schema(
        [
            FILE_ID_FIELD,
        ]
    ),
)

# ---------------------------------------------------------------------------
# View name constants
# ---------------------------------------------------------------------------

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
    "ast_span_unnest",
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
    "ts_span_unnest",
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
    "symtable_span_unnest",
)

BYTECODE_VIEW_NAMES: tuple[str, ...] = (
    "py_bc_code_units",
    "py_bc_consts",
    "py_bc_line_table",
    "py_bc_line_table_with_bytes",
    "py_bc_instructions",
    "py_bc_instruction_attrs",
    "py_bc_instruction_attr_keys",
    "py_bc_instruction_attr_values",
    "py_bc_instruction_spans",
    "py_bc_instruction_span_fields",
    "py_bc_instruction_span_unnest",
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
)

# ---------------------------------------------------------------------------
# Function requirements metadata
# ---------------------------------------------------------------------------


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


from dataclasses import dataclass


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


# ---------------------------------------------------------------------------
# Schema resolution for extraction datasets
# ---------------------------------------------------------------------------


def extract_base_schema_names() -> tuple[str, ...]:
    """Return derived extract base schema names in sorted order.

    Returns:
    -------
    tuple[str, ...]
        Sorted derived extract base schema name tuple.
    """
    from datafusion_engine.extract.metadata import extract_metadata_by_name

    names = [
        name
        for name in extract_metadata_by_name()
        if _derived_extract_base_schema_for(name) is not None
    ]
    return tuple(sorted(names))


def extract_base_schema_for(name: str) -> pa.Schema:
    """Return schema for an extract base dataset from derived authority.

    Args:
        name: Description.

    Raises:
        KeyError: If the operation cannot be completed.
    """
    from datafusion_engine.schema.nested_views import NESTED_DATASET_INDEX

    if name in NESTED_DATASET_INDEX:
        msg = f"{name!r} is a nested dataset, not a base extract dataset."
        raise KeyError(msg)
    return extract_schema_for(name)


def relationship_schema_names() -> tuple[str, ...]:
    """Return relationship schema names in sorted order.

    Returns:
    -------
    tuple[str, ...]
        Sorted relationship schema names.
    """
    return tuple(sorted(RELATIONSHIP_SCHEMA_BY_NAME))


def relationship_schema_for(name: str) -> pa.Schema:
    """Return the schema for a relationship dataset.

    Args:
        name: Description.

    Raises:
        KeyError: If the operation cannot be completed.
    """
    schema = RELATIONSHIP_SCHEMA_BY_NAME.get(name)
    if schema is None:
        msg = f"Unknown relationship schema: {name!r}."
        raise KeyError(msg)
    return schema


def _derived_extract_base_schema_for(name: str) -> pa.Schema | None:
    from datafusion_engine.extract.metadata import extract_metadata_by_name
    from datafusion_engine.schema.derivation import derive_extract_schema

    metadata = extract_metadata_by_name().get(name)
    if metadata is None:
        return None
    try:
        return derive_extract_schema(metadata)
    except (TypeError, ValueError):
        return None


def _static_extract_base_schema_for(name: str) -> pa.Schema | None:
    static_root_schemas: Mapping[str, pa.Schema] = {
        "repo_files_v1": REPO_FILES_SCHEMA,
        "libcst_files_v1": LIBCST_FILES_SCHEMA,
        "ast_files_v1": AST_FILES_SCHEMA,
        "symtable_files_v1": SYMTABLE_FILES_SCHEMA,
        "tree_sitter_files_v1": TREE_SITTER_FILES_SCHEMA,
        "bytecode_files_v1": BYTECODE_FILES_SCHEMA,
        "scip_index_v1": SCIP_INDEX_SCHEMA,
    }
    return static_root_schemas.get(name)


def _derived_extract_nested_schema_for(name: str) -> pa.Schema | None:
    from datafusion_engine.extract.metadata import extract_metadata_by_name
    from datafusion_engine.schema.derivation import derive_nested_dataset_schema
    from datafusion_engine.schema.nested_views import (
        NESTED_DATASET_INDEX,
    )
    from utils.schema_from_struct import schema_from_struct

    spec = NESTED_DATASET_INDEX.get(name)
    if spec is None:
        return None
    root_name = spec["root"]
    nested_path = spec["path"]
    metadata = extract_metadata_by_name().get(root_name)
    if metadata is None:
        return None

    candidate_info = _resolve_nested_dataset_context(
        root_name=root_name,
        nested_path=nested_path,
        metadata=metadata,
    )
    if candidate_info is None:
        return None
    root_schema, row_struct = candidate_info

    row_schema = _resolve_nested_row_schema_authority(
        dataset_name=name,
        nested_path=nested_path,
        derived_schema=derive_nested_dataset_schema(
            metadata,
            nested_path,
        ),
        struct_schema=schema_from_struct(row_struct),
    )
    selections = _collect_nested_schema_fields(
        name=name,
        root_schema=root_schema,
        row_struct=row_struct,
        row_schema=row_schema,
    )
    return pa.schema(selections)


def _resolve_nested_dataset_context(
    *,
    metadata: object,
    root_name: str,
    nested_path: str,
) -> tuple[pa.Schema, pa.StructType] | None:
    candidate_root_schemas = _nested_dataset_root_schemas(root_name)
    if not candidate_root_schemas:
        return None
    candidate = _resolve_nested_row_struct(
        candidate_root_schemas=candidate_root_schemas,
        nested_path=nested_path,
    )
    if candidate is None:
        return None
    if metadata is None:
        return None
    return candidate


def _nested_dataset_root_schemas(root_name: str) -> tuple[pa.Schema, ...]:
    derived_root_schema = _derived_extract_base_schema_for(root_name)
    static_root_schema = _static_extract_base_schema_for(root_name)
    if derived_root_schema is None:
        return (static_root_schema,) if static_root_schema is not None else ()
    if static_root_schema is None or static_root_schema == derived_root_schema:
        return (derived_root_schema,)
    return (derived_root_schema, static_root_schema)


def _resolve_nested_row_struct(
    *,
    candidate_root_schemas: tuple[pa.Schema, ...],
    nested_path: str,
) -> tuple[pa.Schema, pa.StructType] | None:
    from datafusion_engine.schema.nested_views import struct_for_path

    for candidate_schema in candidate_root_schemas:
        try:
            row_struct = struct_for_path(candidate_schema, nested_path)
        except (KeyError, TypeError, ValueError):
            continue
        return candidate_schema, row_struct
    return None


def _collect_nested_schema_fields(
    *,
    name: str,
    root_schema: pa.Schema,
    row_struct: pa.StructType,
    row_schema: pa.Schema,
) -> list[pa.Field]:
    from datafusion_engine.schema.nested_views import (
        extract_nested_context_for,
        identity_fields_for,
    )

    selections: list[pa.Field] = []
    selected_names: set[str] = set()
    for field_name in identity_fields_for(name, root_schema, row_struct):
        field = _field_from_container(root_schema, field_name)
        _append_schema_field(selections, selected_names, field=field)
    for alias, ctx_path in extract_nested_context_for(name).items():
        field = _field_for_path(root_schema, ctx_path)
        _append_schema_field(
            selections,
            selected_names,
            field=_clone_field(field, name=alias),
        )
    for field in row_schema:
        _append_schema_field(selections, selected_names, field=field)
    return selections


def _derived_extract_schema_for(name: str) -> pa.Schema | None:
    from datafusion_engine.schema.nested_views import NESTED_DATASET_INDEX

    if name in NESTED_DATASET_INDEX:
        return _derived_extract_nested_schema_for(name)
    return _derived_extract_base_schema_for(name)


def extract_schema_for(name: str) -> pa.Schema:
    """Resolve schema for an extract base or nested dataset from metadata.

    Args:
        name:
            Extract dataset name (base or nested).

    Returns:
    -------
    pyarrow.Schema
        Derived schema for the requested extract dataset.

    Raises:
        KeyError: If no derived schema is available for the dataset name.
    """
    derived_schema = _derived_extract_schema_for(name)
    if derived_schema is None:
        _LOGGER.debug(
            "extract_schema_authority",
            extra={"dataset_name": name, "authority": "missing"},
        )
        msg = f"No derived extract schema available for {name!r}."
        raise KeyError(msg)
    _LOGGER.debug(
        "extract_schema_authority",
        extra={"dataset_name": name, "authority": "derived"},
    )
    return derived_schema


def extract_schema_contract_for(name: str) -> SchemaContract:
    """Return the SchemaContract for an extract base or nested dataset.

    Returns:
    -------
    SchemaContract
        Schema contract derived from metadata-backed schema authority.
    """
    from datafusion_engine.schema.contracts import SchemaContract

    schema = extract_schema_for(name)
    return SchemaContract.from_arrow_schema(name, schema)


def extract_nested_schema_for(name: str) -> pa.Schema:
    """Return schema for an extract nested dataset from derived authority.

    Returns:
    -------
    pyarrow.Schema
        Arrow schema derived from extract metadata.

    Raises:
        KeyError: If ``name`` is not a registered nested extract dataset.
    """
    from datafusion_engine.schema.nested_views import NESTED_DATASET_INDEX

    if name not in NESTED_DATASET_INDEX:
        msg = f"{name!r} is not a registered extract nested dataset."
        raise KeyError(msg)
    return extract_schema_for(name)


# ---------------------------------------------------------------------------
# Schema-level helpers (shared across domains)
# ---------------------------------------------------------------------------


def _schema_signature(schema: pa.Schema) -> tuple[tuple[str, str, bool], ...]:
    return tuple((field.name, str(field.type), bool(field.nullable)) for field in schema)


def _resolve_nested_row_schema_authority(
    *,
    dataset_name: str,
    nested_path: str,
    derived_schema: pa.Schema | None,
    struct_schema: pa.Schema,
) -> pa.Schema:
    """Resolve nested row schema authority with deterministic conflict handling.

    Returns:
    -------
    pyarrow.Schema
        Nested row schema to use for downstream extraction.
    """
    if derived_schema is None:
        return struct_schema
    if _schema_signature(derived_schema) == _schema_signature(struct_schema):
        return derived_schema
    _LOGGER.warning(
        "extract_schema_authority_conflict",
        extra={
            "dataset_name": dataset_name,
            "nested_path": nested_path,
            "authority": "derived",
            "fallback_authority": "struct",
            "derived_fields": list(derived_schema.names),
            "struct_fields": list(struct_schema.names),
        },
    )
    return derived_schema


def _field_from_container(container: pa.Schema | pa.StructType, name: str) -> pa.Field:
    try:
        return container.field(name)
    except KeyError as exc:
        msg = f"Unknown nested field: {name!r}."
        raise KeyError(msg) from exc


def _clone_field(field: pa.Field, *, name: str | None = None) -> pa.Field:
    resolved_name = field.name if name is None else name
    return pa.field(
        resolved_name,
        field.type,
        nullable=field.nullable,
        metadata=field.metadata,
    )


def _append_schema_field(
    selections: list[pa.Field],
    selected_names: set[str],
    *,
    field: pa.Field,
) -> None:
    if field.name in selected_names:
        return
    selections.append(field)
    selected_names.add(field.name)


def _field_for_path(schema: pa.Schema, path: str) -> pa.Field:
    from datafusion_engine.schema.nested_views import struct_for_path

    if not path:
        msg = "Nested schema path cannot be empty."
        raise ValueError(msg)
    prefix, sep, field_name = path.rpartition(".")
    if not sep:
        return _field_from_container(schema, field_name)
    struct_type = struct_for_path(schema, prefix)
    return _field_from_container(struct_type, field_name)


def base_extract_schema_registry() -> MappingRegistryAdapter[str, pa.Schema]:
    """Return a registry adapter for derived base extract schemas.

    Returns:
    -------
    MappingRegistryAdapter[str, pa.Schema]
        Read-only registry adapter for derived base extract schemas.
    """
    from utils.registry_protocol import MappingRegistryAdapter

    mapping: dict[str, pa.Schema] = {}
    for name in extract_base_schema_names():
        try:
            mapping[name] = extract_schema_for(name)
        except KeyError:
            continue
    return MappingRegistryAdapter.from_mapping(
        mapping,
        read_only=True,
    )


def relationship_schema_registry() -> MappingRegistryAdapter[str, pa.Schema]:
    """Return a registry adapter for relationship schemas.

    Returns:
    -------
    MappingRegistryAdapter[str, pa.Schema]
        Read-only registry adapter for relationship schemas.
    """
    from utils.registry_protocol import MappingRegistryAdapter

    return MappingRegistryAdapter.from_mapping(
        _mapping_from_registry(RELATIONSHIP_SCHEMA_BY_NAME),
        read_only=True,
    )


def _mapping_from_registry[T](registry: ImmutableRegistry[str, T]) -> dict[str, T]:
    return {key: value for key in registry if (value := registry.get(key)) is not None}


if TYPE_CHECKING:
    from datafusion_engine.schema.contracts import SchemaContract
    from utils.registry_protocol import MappingRegistryAdapter
