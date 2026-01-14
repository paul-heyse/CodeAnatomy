"""Field catalog for extract dataset schemas and row schemas."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import pyarrow as pa

from schema_spec.specs import ArrowFieldSpec

ENCODING_META: dict[str, str] = {"encoding": "dictionary"}

QNAME_STRUCT = pa.struct([("name", pa.string()), ("source", pa.string())])
QNAME_LIST = pa.large_list_view(QNAME_STRUCT)

SCIP_SIGNATURE_OCCURRENCE_TYPE = pa.struct(
    [
        ("symbol", pa.string()),
        ("symbol_roles", pa.int32()),
        ("range", pa.list_(pa.int32())),
    ]
)
SCIP_SIGNATURE_DOCUMENTATION_TYPE = pa.struct(
    [
        ("text", pa.string()),
        ("language", pa.string()),
        ("occurrences", pa.large_list_view(SCIP_SIGNATURE_OCCURRENCE_TYPE)),
    ]
)


def _spec(
    name: str,
    dtype: pa.DataType,
    *,
    nullable: bool = True,
    metadata: Mapping[str, str] | None = None,
) -> ArrowFieldSpec:
    return ArrowFieldSpec(
        name=name,
        dtype=dtype,
        nullable=nullable,
        metadata=dict(metadata or {}),
    )


def _dict_string(name: str) -> ArrowFieldSpec:
    return _spec(name, pa.string(), metadata=ENCODING_META)


_FIELD_CATALOG: dict[str, ArrowFieldSpec] = {}


def _register(key: str, spec: ArrowFieldSpec) -> None:
    existing = _FIELD_CATALOG.get(key)
    if existing is not None and existing != spec:
        msg = f"Field spec conflict for key {key!r}: {existing} vs {spec}"
        raise ValueError(msg)
    _FIELD_CATALOG[key] = spec


def _register_many(entries: Mapping[str, ArrowFieldSpec]) -> None:
    for key, spec in entries.items():
        _register(key, spec)


_register_many(
    {
        "file_id": _spec("file_id", pa.string()),
        "path": _spec("path", pa.string()),
        "path_dict": _dict_string("path"),
        "file_sha256": _spec("file_sha256", pa.string()),
        "abs_path": _spec("abs_path", pa.string()),
        "size_bytes": _spec("size_bytes", pa.int64()),
        "encoding": _spec("encoding", pa.string()),
        "text": _spec("text", pa.string()),
        "bytes": _spec("bytes", pa.binary()),
        "evidence_rank": _spec("evidence_rank", pa.int16()),
        "confidence": _spec("confidence", pa.float32()),
        "ast_idx": _spec("ast_idx", pa.int32()),
        "parent_ast_idx": _spec("parent_ast_idx", pa.int32()),
        "child_ast_idx": _spec("child_ast_idx", pa.int32()),
        "field_name": _spec("field_name", pa.string()),
        "field_pos": _spec("field_pos", pa.int32()),
        "kind": _spec("kind", pa.string()),
        "kind_dict": _dict_string("kind"),
        "name": _spec("name", pa.string()),
        "value_repr": _spec("value_repr", pa.string()),
        "lineno": _spec("lineno", pa.int32()),
        "col_offset": _spec("col_offset", pa.int32()),
        "end_lineno": _spec("end_lineno", pa.int32()),
        "end_col_offset": _spec("end_col_offset", pa.int32()),
        "line_base": _spec("line_base", pa.int32()),
        "col_unit": _spec("col_unit", pa.string()),
        "end_exclusive": _spec("end_exclusive", pa.bool_()),
        "offset": _spec("offset", pa.int32()),
        "end_offset": _spec("end_offset", pa.int32()),
        "error_type": _spec("error_type", pa.string()),
        "message": _spec("message", pa.string()),
        "default_indent": _spec("default_indent", pa.string()),
        "default_newline": _spec("default_newline", pa.string()),
        "has_trailing_newline": _spec("has_trailing_newline", pa.bool_()),
        "future_imports": _spec("future_imports", pa.large_list_view(pa.string())),
        "module_name": _spec("module_name", pa.string()),
        "package_name": _spec("package_name", pa.string()),
        "raw_line": _spec("raw_line", pa.int32()),
        "raw_column": _spec("raw_column", pa.int32()),
        "meta": _spec("meta", pa.map_(pa.string(), pa.string())),
        "name_ref_id": _spec("name_ref_id", pa.string()),
        "span_id": _spec("span_id", pa.string()),
        "expr_ctx": _spec("expr_ctx", pa.string()),
        "bstart": _spec("bstart", pa.int64()),
        "bend": _spec("bend", pa.int64()),
        "import_id": _spec("import_id", pa.string()),
        "module": _spec("module", pa.string()),
        "relative_level": _spec("relative_level", pa.int32()),
        "asname": _spec("asname", pa.string()),
        "is_star": _spec("is_star", pa.bool_()),
        "stmt_bstart": _spec("stmt_bstart", pa.int64()),
        "stmt_bend": _spec("stmt_bend", pa.int64()),
        "alias_bstart": _spec("alias_bstart", pa.int64()),
        "alias_bend": _spec("alias_bend", pa.int64()),
        "call_id": _spec("call_id", pa.string()),
        "call_bstart": _spec("call_bstart", pa.int64()),
        "call_bend": _spec("call_bend", pa.int64()),
        "callee_bstart": _spec("callee_bstart", pa.int64()),
        "callee_bend": _spec("callee_bend", pa.int64()),
        "callee_shape": _spec("callee_shape", pa.string()),
        "callee_text": _spec("callee_text", pa.string()),
        "arg_count": _spec("arg_count", pa.int32()),
        "callee_dotted": _spec("callee_dotted", pa.string()),
        "callee_qnames": _spec("callee_qnames", QNAME_LIST),
        "def_id": _spec("def_id", pa.string()),
        "container_def_id": _spec("container_def_id", pa.string()),
        "def_bstart": _spec("def_bstart", pa.int64()),
        "def_bend": _spec("def_bend", pa.int64()),
        "name_bstart": _spec("name_bstart", pa.int64()),
        "name_bend": _spec("name_bend", pa.int64()),
        "qnames": _spec("qnames", QNAME_LIST),
        "type_expr_id": _spec("type_expr_id", pa.string()),
        "owner_def_id": _spec("owner_def_id", pa.string()),
        "param_name": _spec("param_name", pa.string()),
        "expr_kind": _spec("expr_kind", pa.string()),
        "expr_role": _spec("expr_role", pa.string()),
        "expr_text": _spec("expr_text", pa.string()),
        "container_def_kind": _spec("container_def_kind", pa.string()),
        "container_def_bstart": _spec("container_def_bstart", pa.int64()),
        "container_def_bend": _spec("container_def_bend", pa.int64()),
        "owner_def_kind": _spec("owner_def_kind", pa.string()),
        "owner_def_bstart": _spec("owner_def_bstart", pa.int64()),
        "owner_def_bend": _spec("owner_def_bend", pa.int64()),
        "ts_node_id": _spec("ts_node_id", pa.string()),
        "parent_ts_id": _spec("parent_ts_id", pa.string()),
        "ts_type": _spec("ts_type", pa.string()),
        "start_byte": _spec("start_byte", pa.int64()),
        "end_byte": _spec("end_byte", pa.int64()),
        "is_named": _spec("is_named", pa.bool_()),
        "has_error": _spec("has_error", pa.bool_()),
        "is_error": _spec("is_error", pa.bool_()),
        "is_missing": _spec("is_missing", pa.bool_()),
        "ts_error_id": _spec("ts_error_id", pa.string()),
        "ts_missing_id": _spec("ts_missing_id", pa.string()),
        "parent_ts_type": _spec("parent_ts_type", pa.string()),
        "parent_start_byte": _spec("parent_start_byte", pa.int64()),
        "parent_end_byte": _spec("parent_end_byte", pa.int64()),
        "scope_id": _spec("scope_id", pa.string()),
        "table_id": _spec("table_id", pa.int64()),
        "scope_type": _spec("scope_type", pa.string()),
        "scope_name": _spec("scope_name", pa.string()),
        "is_nested": _spec("is_nested", pa.bool_()),
        "is_optimized": _spec("is_optimized", pa.bool_()),
        "has_children": _spec("has_children", pa.bool_()),
        "scope_role": _spec("scope_role", pa.string()),
        "symbol_row_id": _spec("symbol_row_id", pa.string()),
        "is_referenced": _spec("is_referenced", pa.bool_()),
        "is_assigned": _spec("is_assigned", pa.bool_()),
        "is_imported": _spec("is_imported", pa.bool_()),
        "is_annotated": _spec("is_annotated", pa.bool_()),
        "is_parameter": _spec("is_parameter", pa.bool_()),
        "is_global": _spec("is_global", pa.bool_()),
        "is_declared_global": _spec("is_declared_global", pa.bool_()),
        "is_nonlocal": _spec("is_nonlocal", pa.bool_()),
        "is_local": _spec("is_local", pa.bool_()),
        "is_free": _spec("is_free", pa.bool_()),
        "is_namespace": _spec("is_namespace", pa.bool_()),
        "edge_id": _spec("edge_id", pa.string()),
        "parent_scope_id": _spec("parent_scope_id", pa.string()),
        "child_scope_id": _spec("child_scope_id", pa.string()),
        "parameters": _spec("parameters", pa.large_list_view(pa.string())),
        "locals": _spec("locals", pa.large_list_view(pa.string())),
        "globals": _spec("globals", pa.large_list_view(pa.string())),
        "nonlocals": _spec("nonlocals", pa.large_list_view(pa.string())),
        "frees": _spec("frees", pa.large_list_view(pa.string())),
        "scope_table_id": _spec("scope_table_id", pa.int64()),
        "parent_table_id": _spec("parent_table_id", pa.int64()),
        "child_table_id": _spec("child_table_id", pa.int64()),
        "symbol_name": _spec("symbol_name", pa.string()),
        "code_unit_id": _spec("code_unit_id", pa.string()),
        "parent_code_unit_id": _spec("parent_code_unit_id", pa.string()),
        "qualpath": _spec("qualpath", pa.string()),
        "co_name": _spec("co_name", pa.string()),
        "firstlineno": _spec("firstlineno", pa.int32()),
        "argcount": _spec("argcount", pa.int32()),
        "line_no": _spec("line_no", pa.int32()),
        "line_start_byte": _spec("line_start_byte", pa.int64()),
        "line_end_byte": _spec("line_end_byte", pa.int64()),
        "line_text": _spec("line_text", pa.string()),
        "newline_kind": _spec("newline_kind", pa.string()),
        "posonlyargcount": _spec("posonlyargcount", pa.int32()),
        "kwonlyargcount": _spec("kwonlyargcount", pa.int32()),
        "nlocals": _spec("nlocals", pa.int32()),
        "flags": _spec("flags", pa.int32()),
        "stacksize": _spec("stacksize", pa.int32()),
        "code_len": _spec("code_len", pa.int32()),
        "instr_id": _spec("instr_id", pa.string()),
        "instr_index": _spec("instr_index", pa.int32()),
        "opname": _spec("opname", pa.string()),
        "opcode": _spec("opcode", pa.int32()),
        "arg": _spec("arg", pa.int32()),
        "argval_str": _spec("argval_str", pa.string()),
        "argrepr": _spec("argrepr", pa.string()),
        "is_jump_target": _spec("is_jump_target", pa.bool_()),
        "jump_target_offset": _spec("jump_target_offset", pa.int32()),
        "starts_line": _spec("starts_line", pa.int32()),
        "pos_start_line": _spec("pos_start_line", pa.int32()),
        "pos_end_line": _spec("pos_end_line", pa.int32()),
        "pos_start_col": _spec("pos_start_col", pa.int32()),
        "pos_end_col": _spec("pos_end_col", pa.int32()),
        "exc_entry_id": _spec("exc_entry_id", pa.string()),
        "exc_index": _spec("exc_index", pa.int32()),
        "start_offset": _spec("start_offset", pa.int32()),
        "target_offset": _spec("target_offset", pa.int32()),
        "depth": _spec("depth", pa.int32()),
        "lasti": _spec("lasti", pa.bool_()),
        "block_id": _spec("block_id", pa.string()),
        "src_block_id": _spec("src_block_id", pa.string()),
        "dst_block_id": _spec("dst_block_id", pa.string()),
        "cond_instr_id": _spec("cond_instr_id", pa.string()),
        "edge_key": _spec("edge_key", pa.string()),
        "cond_instr_index": _spec("cond_instr_index", pa.int32()),
        "cond_instr_offset": _spec("cond_instr_offset", pa.int32()),
        "src_block_start": _spec("src_block_start", pa.int32()),
        "src_block_end": _spec("src_block_end", pa.int32()),
        "dst_block_start": _spec("dst_block_start", pa.int32()),
        "dst_block_end": _spec("dst_block_end", pa.int32()),
        "parent_qualpath": _spec("parent_qualpath", pa.string()),
        "parent_co_name": _spec("parent_co_name", pa.string()),
        "parent_firstlineno": _spec("parent_firstlineno", pa.int32()),
        "rt_id": _spec("rt_id", pa.string()),
        "sig_id": _spec("sig_id", pa.string()),
        "param_id": _spec("param_id", pa.string()),
        "member_id": _spec("member_id", pa.string()),
        "qualname": _spec("qualname", pa.string()),
        "obj_type": _spec("obj_type", pa.string()),
        "source_path": _spec("source_path", pa.string()),
        "source_line": _spec("source_line", pa.int32()),
        "signature": _spec("signature", pa.string()),
        "return_annotation": _spec("return_annotation", pa.string()),
        "default_repr": _spec("default_repr", pa.string()),
        "annotation_repr": _spec("annotation_repr", pa.string()),
        "position": _spec("position", pa.int32()),
        "member_kind": _spec("member_kind", pa.string()),
        "value_module": _spec("value_module", pa.string()),
        "value_qualname": _spec("value_qualname", pa.string()),
        "object_key": _spec("object_key", pa.string()),
        "tool_name_dict": _dict_string("tool_name"),
        "tool_version_dict": _dict_string("tool_version"),
        "project_root_dict": _dict_string("project_root"),
        "text_document_encoding_dict": _dict_string("text_document_encoding"),
        "protocol_version_dict": _dict_string("protocol_version"),
        "document_id": _spec("document_id", pa.string()),
        "language_dict": _dict_string("language"),
        "position_encoding_dict": _dict_string("position_encoding"),
        "occurrence_id": _spec("occurrence_id", pa.string()),
        "symbol_dict": _dict_string("symbol"),
        "symbol_roles": _spec("symbol_roles", pa.int32()),
        "syntax_kind_dict": _dict_string("syntax_kind"),
        "override_documentation": _spec("override_documentation", pa.large_list(pa.string())),
        "symbol_info_id": _spec("symbol_info_id", pa.string()),
        "display_name_dict": _dict_string("display_name"),
        "enclosing_symbol_dict": _dict_string("enclosing_symbol"),
        "documentation": _spec("documentation", pa.large_list(pa.string())),
        "signature_documentation": _spec(
            "signature_documentation",
            SCIP_SIGNATURE_DOCUMENTATION_TYPE,
        ),
        "relationship_id": _spec("relationship_id", pa.string()),
        "related_symbol_dict": _dict_string("related_symbol"),
        "is_reference": _spec("is_reference", pa.bool_()),
        "is_implementation": _spec("is_implementation", pa.bool_()),
        "is_type_definition": _spec("is_type_definition", pa.bool_()),
        "is_definition": _spec("is_definition", pa.bool_()),
        "severity_dict": _dict_string("severity"),
        "code_dict": _dict_string("code"),
        "source_dict": _dict_string("source"),
        "tags": _spec("tags", pa.large_list(pa.string())),
    }
)


def field(key: str) -> ArrowFieldSpec:
    """Return the ArrowFieldSpec for a field key.

    Returns
    -------
    ArrowFieldSpec
        Field specification for the key.
    """
    return _FIELD_CATALOG[key]


def fields(keys: Sequence[str]) -> list[ArrowFieldSpec]:
    """Return ArrowFieldSpec instances for field keys.

    Returns
    -------
    list[ArrowFieldSpec]
        Field specifications for the keys.
    """
    return [field(key) for key in keys]


def field_name(key: str) -> str:
    """Return the column name for a field key.

    Returns
    -------
    str
        Column name for the key.
    """
    return field(key).name


__all__ = [
    "ENCODING_META",
    "QNAME_LIST",
    "QNAME_STRUCT",
    "SCIP_SIGNATURE_DOCUMENTATION_TYPE",
    "SCIP_SIGNATURE_OCCURRENCE_TYPE",
    "field",
    "field_name",
    "fields",
]
