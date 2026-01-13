"""Dataset rows describing extract schemas and derivations."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Literal

from arrowdsl.core.context import OrderingKey
from extract.registry_templates import flag_default

DerivedKind = Literal["masked_hash", "hash"]


def _flag_enabled(flag: str, *, default: bool = True) -> Callable[[object | None], bool]:
    def _predicate(options: object | None) -> bool:
        resolved_default = flag_default(flag, fallback=default)
        if options is None:
            return resolved_default
        value = getattr(options, flag, resolved_default)
        return value if isinstance(value, bool) else resolved_default

    return _predicate


def _allowlist_enabled(options: object | None) -> bool:
    if options is None:
        return False
    allowlist = getattr(options, "module_allowlist", ())
    return bool(allowlist)


@dataclass(frozen=True)
class DerivedIdSpec:
    """Derived identifier specification for dataset rows."""

    name: str
    spec: str
    kind: DerivedKind = "masked_hash"
    required: tuple[str, ...] = ()


@dataclass(frozen=True)
class DatasetRow:
    """Row spec describing a dataset schema and derivations."""

    name: str
    version: int
    bundles: tuple[str, ...]
    fields: tuple[str, ...]
    derived: tuple[DerivedIdSpec, ...] = ()
    row_fields: tuple[str, ...] = ()
    row_extras: tuple[str, ...] = ()
    template: str | None = None
    ordering_keys: tuple[OrderingKey, ...] = ()
    join_keys: tuple[str, ...] = ()
    enabled_when: Callable[[object | None], bool] | None = None
    postprocess: str | None = None
    metadata_extra: dict[bytes, bytes] = field(default_factory=dict)
    evidence_required_columns: tuple[str, ...] = ()

    def output_name(self) -> str:
        """Return the canonical output alias for the dataset row.

        Returns
        -------
        str
            Output alias used by extract bundles.
        """
        suffix = f"_v{self.version}"
        base = self.name[: -len(suffix)] if self.name.endswith(suffix) else self.name
        if self.template in {"ast", "cst"} and base.startswith("py_"):
            return base[3:]
        return base


DATASET_ROWS: tuple[DatasetRow, ...] = (
    DatasetRow(
        name="repo_files_v1",
        version=1,
        bundles=("file_identity",),
        fields=("abs_path", "size_bytes", "encoding", "text", "bytes"),
        derived=(DerivedIdSpec(name="file_id", spec="repo_file_id", kind="hash"),),
        template="repo_scan",
        ordering_keys=(("path", "ascending"),),
        join_keys=("path",),
    ),
    DatasetRow(
        name="py_ast_nodes_v1",
        version=1,
        bundles=("file_identity",),
        fields=(
            "ast_idx",
            "parent_ast_idx",
            "field_name",
            "field_pos",
            "kind",
            "name",
            "value_repr",
            "lineno",
            "col_offset",
            "end_lineno",
            "end_col_offset",
        ),
        template="ast",
        join_keys=("file_id", "ast_idx"),
    ),
    DatasetRow(
        name="py_ast_edges_v1",
        version=1,
        bundles=("file_identity",),
        fields=("parent_ast_idx", "child_ast_idx", "field_name", "field_pos"),
        template="ast",
        join_keys=("file_id", "parent_ast_idx", "child_ast_idx", "field_name", "field_pos"),
    ),
    DatasetRow(
        name="py_ast_errors_v1",
        version=1,
        bundles=("file_identity",),
        fields=("error_type", "message", "lineno", "offset", "end_lineno", "end_offset"),
        template="ast",
        join_keys=("file_id", "lineno", "offset", "end_lineno", "end_offset"),
    ),
    DatasetRow(
        name="py_cst_parse_manifest_v1",
        version=1,
        bundles=("file_identity",),
        fields=(
            "encoding",
            "default_indent",
            "default_newline",
            "has_trailing_newline",
            "future_imports",
            "module_name",
            "package_name",
        ),
        template="cst",
        join_keys=("file_id",),
        enabled_when=_flag_enabled("include_parse_manifest"),
    ),
    DatasetRow(
        name="py_cst_parse_errors_v1",
        version=1,
        bundles=("file_identity",),
        fields=("error_type", "message", "raw_line", "raw_column", "meta"),
        template="cst",
        join_keys=("file_id", "raw_line", "raw_column"),
        enabled_when=_flag_enabled("include_parse_errors"),
    ),
    DatasetRow(
        name="py_cst_name_refs_v1",
        version=1,
        bundles=("file_identity",),
        fields=("name_ref_id", "span_id", "name", "expr_ctx", "bstart", "bend"),
        derived=(
            DerivedIdSpec(name="name_ref_id", spec="cst_name_ref_id"),
            DerivedIdSpec(name="span_id", spec="span_id"),
        ),
        template="cst",
        join_keys=("file_id", "bstart", "bend"),
        enabled_when=_flag_enabled("include_name_refs"),
    ),
    DatasetRow(
        name="py_cst_imports_v1",
        version=1,
        bundles=("file_identity",),
        fields=(
            "import_id",
            "span_id",
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
        derived=(
            DerivedIdSpec(name="import_id", spec="cst_import_id"),
            DerivedIdSpec(name="span_id", spec="cst_import_span_id"),
        ),
        template="cst",
        join_keys=("file_id", "alias_bstart", "alias_bend"),
        enabled_when=_flag_enabled("include_imports"),
    ),
    DatasetRow(
        name="py_cst_callsites_v1",
        version=1,
        bundles=("file_identity", "call_span"),
        fields=(
            "call_id",
            "span_id",
            "callee_bstart",
            "callee_bend",
            "callee_shape",
            "callee_text",
            "arg_count",
            "callee_dotted",
            "callee_qnames",
        ),
        derived=(
            DerivedIdSpec(name="call_id", spec="cst_call_id"),
            DerivedIdSpec(name="span_id", spec="cst_call_span_id"),
        ),
        template="cst",
        join_keys=("file_id", "call_bstart", "call_bend"),
        enabled_when=_flag_enabled("include_callsites"),
    ),
    DatasetRow(
        name="py_cst_defs_v1",
        version=1,
        bundles=("file_identity",),
        fields=(
            "def_id",
            "container_def_id",
            "span_id",
            "kind",
            "name",
            "def_bstart",
            "def_bend",
            "name_bstart",
            "name_bend",
            "qnames",
        ),
        derived=(
            DerivedIdSpec(name="def_id", spec="cst_def_id"),
            DerivedIdSpec(name="container_def_id", spec="cst_container_def_id"),
            DerivedIdSpec(name="span_id", spec="cst_def_span_id"),
        ),
        row_fields=(
            "def_id",
            "container_def_id",
            "kind",
            "name",
            "def_bstart",
            "def_bend",
            "name_bstart",
            "name_bend",
            "qnames",
            "container_def_kind",
            "container_def_bstart",
            "container_def_bend",
        ),
        template="cst",
        join_keys=("file_id", "def_bstart", "def_bend"),
        enabled_when=_flag_enabled("include_defs"),
    ),
    DatasetRow(
        name="py_cst_type_exprs_v1",
        version=1,
        bundles=("file_identity",),
        fields=(
            "type_expr_id",
            "owner_def_id",
            "span_id",
            "param_name",
            "expr_kind",
            "expr_role",
            "bstart",
            "bend",
            "expr_text",
        ),
        derived=(
            DerivedIdSpec(name="type_expr_id", spec="cst_type_expr_id"),
            DerivedIdSpec(name="owner_def_id", spec="cst_owner_def_id"),
            DerivedIdSpec(name="span_id", spec="span_id"),
        ),
        row_fields=(
            "type_expr_id",
            "owner_def_id",
            "param_name",
            "expr_kind",
            "expr_role",
            "bstart",
            "bend",
            "expr_text",
            "owner_def_kind",
            "owner_def_bstart",
            "owner_def_bend",
        ),
        template="cst",
        join_keys=("file_id", "bstart", "bend"),
        enabled_when=_flag_enabled("include_type_exprs"),
    ),
    DatasetRow(
        name="ts_nodes_v1",
        version=1,
        bundles=("file_identity",),
        fields=(
            "ts_node_id",
            "parent_ts_id",
            "span_id",
            "ts_type",
            "start_byte",
            "end_byte",
            "is_named",
            "has_error",
            "is_error",
            "is_missing",
        ),
        derived=(
            DerivedIdSpec(name="ts_node_id", spec="ts_node_id"),
            DerivedIdSpec(name="parent_ts_id", spec="ts_parent_node_id"),
            DerivedIdSpec(name="span_id", spec="ts_span_id"),
        ),
        row_fields=(
            "ts_node_id",
            "parent_ts_id",
            "ts_type",
            "start_byte",
            "end_byte",
            "is_named",
            "has_error",
            "is_error",
            "is_missing",
            "parent_ts_type",
            "parent_start_byte",
            "parent_end_byte",
        ),
        template="tree_sitter",
        join_keys=("file_id", "start_byte", "end_byte", "ts_type"),
        enabled_when=_flag_enabled("include_nodes"),
    ),
    DatasetRow(
        name="ts_errors_v1",
        version=1,
        bundles=("file_identity",),
        fields=(
            "ts_error_id",
            "ts_node_id",
            "span_id",
            "ts_type",
            "start_byte",
            "end_byte",
            "is_error",
        ),
        derived=(
            DerivedIdSpec(name="ts_node_id", spec="ts_node_id"),
            DerivedIdSpec(name="ts_error_id", spec="ts_error_id"),
            DerivedIdSpec(name="span_id", spec="ts_span_id"),
        ),
        template="tree_sitter",
        join_keys=("file_id", "start_byte", "end_byte"),
        enabled_when=_flag_enabled("include_errors"),
    ),
    DatasetRow(
        name="ts_missing_v1",
        version=1,
        bundles=("file_identity",),
        fields=(
            "ts_missing_id",
            "ts_node_id",
            "span_id",
            "ts_type",
            "start_byte",
            "end_byte",
            "is_missing",
        ),
        derived=(
            DerivedIdSpec(name="ts_node_id", spec="ts_node_id"),
            DerivedIdSpec(name="ts_missing_id", spec="ts_missing_id"),
            DerivedIdSpec(name="span_id", spec="ts_span_id"),
        ),
        template="tree_sitter",
        join_keys=("file_id", "start_byte", "end_byte"),
        enabled_when=_flag_enabled("include_missing"),
    ),
    DatasetRow(
        name="py_bc_code_units_v1",
        version=1,
        bundles=("file_identity",),
        fields=(
            "code_unit_id",
            "parent_code_unit_id",
            "qualpath",
            "co_name",
            "firstlineno",
            "argcount",
            "posonlyargcount",
            "kwonlyargcount",
            "nlocals",
            "flags",
            "stacksize",
            "code_len",
        ),
        derived=(
            DerivedIdSpec(name="code_unit_id", spec="bc_code_unit_id"),
            DerivedIdSpec(name="parent_code_unit_id", spec="bc_parent_code_unit_id"),
        ),
        row_fields=(
            "qualpath",
            "co_name",
            "firstlineno",
            "parent_qualpath",
            "parent_co_name",
            "parent_firstlineno",
            "argcount",
            "posonlyargcount",
            "kwonlyargcount",
            "nlocals",
            "flags",
            "stacksize",
            "code_len",
        ),
        template="bytecode",
        join_keys=("file_id", "qualpath", "firstlineno", "co_name"),
    ),
    DatasetRow(
        name="py_bc_instructions_v1",
        version=1,
        bundles=("file_identity",),
        fields=(
            "instr_id",
            "code_unit_id",
            "instr_index",
            "offset",
            "opname",
            "opcode",
            "arg",
            "argval_str",
            "argrepr",
            "is_jump_target",
            "jump_target_offset",
            "starts_line",
            "pos_start_line",
            "pos_end_line",
            "pos_start_col",
            "pos_end_col",
        ),
        derived=(
            DerivedIdSpec(name="code_unit_id", spec="bc_code_unit_id"),
            DerivedIdSpec(name="instr_id", spec="bc_instr_id"),
        ),
        row_fields=(
            "qualpath",
            "co_name",
            "firstlineno",
            "instr_index",
            "offset",
            "opname",
            "opcode",
            "arg",
            "argval_str",
            "argrepr",
            "is_jump_target",
            "jump_target_offset",
            "starts_line",
            "pos_start_line",
            "pos_end_line",
            "pos_start_col",
            "pos_end_col",
        ),
        template="bytecode",
        join_keys=("code_unit_id", "instr_index", "offset"),
    ),
    DatasetRow(
        name="py_bc_exception_table_v1",
        version=1,
        bundles=("file_identity",),
        fields=(
            "exc_entry_id",
            "code_unit_id",
            "exc_index",
            "start_offset",
            "end_offset",
            "target_offset",
            "depth",
            "lasti",
        ),
        derived=(
            DerivedIdSpec(name="code_unit_id", spec="bc_code_unit_id"),
            DerivedIdSpec(name="exc_entry_id", spec="bc_exc_entry_id"),
        ),
        row_fields=(
            "qualpath",
            "co_name",
            "firstlineno",
            "exc_index",
            "start_offset",
            "end_offset",
            "target_offset",
            "depth",
            "lasti",
        ),
        template="bytecode",
        join_keys=("code_unit_id", "exc_index", "start_offset", "end_offset", "target_offset"),
    ),
    DatasetRow(
        name="py_bc_blocks_v1",
        version=1,
        bundles=("file_identity",),
        fields=("block_id", "code_unit_id", "start_offset", "end_offset", "kind"),
        derived=(
            DerivedIdSpec(name="code_unit_id", spec="bc_code_unit_id"),
            DerivedIdSpec(name="block_id", spec="bc_block_id"),
        ),
        row_fields=(
            "qualpath",
            "co_name",
            "firstlineno",
            "start_offset",
            "end_offset",
            "kind",
        ),
        template="bytecode",
        join_keys=("code_unit_id", "start_offset", "end_offset"),
        enabled_when=_flag_enabled("include_cfg_derivations"),
    ),
    DatasetRow(
        name="py_bc_cfg_edges_v1",
        version=1,
        bundles=("file_identity",),
        fields=(
            "edge_id",
            "code_unit_id",
            "src_block_id",
            "dst_block_id",
            "kind",
            "cond_instr_id",
            "exc_index",
        ),
        derived=(
            DerivedIdSpec(name="code_unit_id", spec="bc_code_unit_id"),
            DerivedIdSpec(name="src_block_id", spec="bc_src_block_id"),
            DerivedIdSpec(name="dst_block_id", spec="bc_dst_block_id"),
            DerivedIdSpec(name="cond_instr_id", spec="bc_cond_instr_id"),
            DerivedIdSpec(name="edge_id", spec="bc_edge_id"),
        ),
        row_fields=(
            "qualpath",
            "co_name",
            "firstlineno",
            "src_block_start",
            "src_block_end",
            "dst_block_start",
            "dst_block_end",
            "kind",
            "edge_key",
            "cond_instr_index",
            "cond_instr_offset",
            "exc_index",
        ),
        template="bytecode",
        join_keys=("code_unit_id", "src_block_id", "dst_block_id", "kind", "edge_key", "exc_index"),
        enabled_when=_flag_enabled("include_cfg_derivations"),
    ),
    DatasetRow(
        name="py_bc_errors_v1",
        version=1,
        bundles=("file_identity",),
        fields=("error_type", "message"),
        template="bytecode",
        join_keys=("file_id", "message"),
    ),
    DatasetRow(
        name="py_sym_scopes_v1",
        version=1,
        bundles=("file_identity",),
        fields=(
            "scope_id",
            "table_id",
            "scope_type",
            "scope_name",
            "lineno",
            "is_nested",
            "is_optimized",
            "has_children",
            "scope_role",
        ),
        derived=(DerivedIdSpec(name="scope_id", spec="sym_scope_id"),),
        row_fields=(
            "table_id",
            "scope_type",
            "scope_name",
            "lineno",
            "is_nested",
            "is_optimized",
            "has_children",
            "scope_role",
        ),
        template="symtable",
        join_keys=("file_id", "table_id"),
    ),
    DatasetRow(
        name="py_sym_symbols_v1",
        version=1,
        bundles=("file_identity",),
        fields=(
            "symbol_row_id",
            "scope_id",
            "name",
            "is_referenced",
            "is_assigned",
            "is_imported",
            "is_annotated",
            "is_parameter",
            "is_global",
            "is_declared_global",
            "is_nonlocal",
            "is_local",
            "is_free",
            "is_namespace",
        ),
        derived=(DerivedIdSpec(name="symbol_row_id", spec="sym_symbol_row_id"),),
        row_fields=(
            "scope_table_id",
            "name",
            "is_referenced",
            "is_assigned",
            "is_imported",
            "is_annotated",
            "is_parameter",
            "is_global",
            "is_declared_global",
            "is_nonlocal",
            "is_local",
            "is_free",
            "is_namespace",
        ),
        template="symtable",
        join_keys=("file_id", "scope_id", "name"),
    ),
    DatasetRow(
        name="py_sym_scope_edges_v1",
        version=1,
        bundles=("file_identity",),
        fields=("edge_id", "parent_scope_id", "child_scope_id"),
        derived=(DerivedIdSpec(name="edge_id", spec="sym_scope_edge_id"),),
        row_fields=("parent_table_id", "child_table_id"),
        template="symtable",
        join_keys=("file_id", "parent_scope_id", "child_scope_id"),
    ),
    DatasetRow(
        name="py_sym_namespace_edges_v1",
        version=1,
        bundles=("file_identity",),
        fields=("edge_id", "scope_id", "symbol_row_id", "child_scope_id"),
        derived=(
            DerivedIdSpec(name="symbol_row_id", spec="sym_ns_symbol_row_id"),
            DerivedIdSpec(name="edge_id", spec="sym_ns_edge_id"),
        ),
        row_fields=("scope_table_id", "symbol_name", "child_table_id"),
        template="symtable",
        join_keys=("file_id", "scope_id", "symbol_row_id", "child_scope_id"),
    ),
    DatasetRow(
        name="py_sym_function_partitions_v1",
        version=1,
        bundles=("file_identity",),
        fields=("scope_id", "parameters", "locals", "globals", "nonlocals", "frees"),
        row_fields=("scope_table_id", "parameters", "locals", "globals", "nonlocals", "frees"),
        template="symtable",
        join_keys=("file_id", "scope_id"),
    ),
    DatasetRow(
        name="rt_objects_v1",
        version=1,
        bundles=(),
        fields=(
            "rt_id",
            "module",
            "qualname",
            "name",
            "obj_type",
            "source_path",
            "source_line",
            "meta",
        ),
        derived=(DerivedIdSpec(name="rt_id", spec="rt_object_id"),),
        row_fields=(
            "object_key",
            "module",
            "qualname",
            "name",
            "obj_type",
            "source_path",
            "source_line",
            "meta",
        ),
        template="runtime_inspect",
        join_keys=("module", "qualname"),
        enabled_when=_allowlist_enabled,
    ),
    DatasetRow(
        name="rt_signatures_v1",
        version=1,
        bundles=(),
        fields=("sig_id", "rt_id", "signature", "return_annotation"),
        derived=(DerivedIdSpec(name="sig_id", spec="rt_signature_id"),),
        row_fields=("object_key", "signature", "return_annotation"),
        template="runtime_inspect",
        join_keys=("rt_id", "signature"),
        enabled_when=_allowlist_enabled,
    ),
    DatasetRow(
        name="rt_signature_params_v1",
        version=1,
        bundles=(),
        fields=(
            "param_id",
            "sig_id",
            "name",
            "kind",
            "default_repr",
            "annotation_repr",
            "position",
        ),
        derived=(DerivedIdSpec(name="param_id", spec="rt_param_id"),),
        row_fields=(
            "object_key",
            "signature",
            "name",
            "kind",
            "default_repr",
            "annotation_repr",
            "position",
        ),
        template="runtime_inspect",
        join_keys=("sig_id", "name", "position"),
        enabled_when=_allowlist_enabled,
    ),
    DatasetRow(
        name="rt_members_v1",
        version=1,
        bundles=(),
        fields=(
            "member_id",
            "rt_id",
            "name",
            "member_kind",
            "value_repr",
            "value_module",
            "value_qualname",
        ),
        derived=(DerivedIdSpec(name="member_id", spec="rt_member_id"),),
        row_fields=(
            "object_key",
            "name",
            "member_kind",
            "value_repr",
            "value_module",
            "value_qualname",
        ),
        template="runtime_inspect",
        join_keys=("rt_id", "name"),
        enabled_when=_allowlist_enabled,
    ),
    DatasetRow(
        name="scip_metadata_v1",
        version=1,
        bundles=(),
        fields=(
            "tool_name_dict",
            "tool_version_dict",
            "project_root_dict",
            "text_document_encoding_dict",
            "protocol_version_dict",
            "meta",
        ),
        template="scip",
        join_keys=("project_root_dict", "tool_name_dict", "tool_version_dict"),
    ),
    DatasetRow(
        name="scip_documents_v1",
        version=1,
        bundles=(),
        fields=("document_id", "path_dict", "language_dict", "position_encoding_dict"),
        template="scip",
        postprocess="scip_documents",
        join_keys=("path_dict",),
    ),
    DatasetRow(
        name="scip_occurrences_v1",
        version=1,
        bundles=("scip_range_len", "scip_range_enc_len"),
        fields=(
            "occurrence_id",
            "document_id",
            "path_dict",
            "symbol_dict",
            "symbol_roles",
            "syntax_kind_dict",
            "override_documentation",
        ),
        template="scip",
        postprocess="scip_occurrences",
        join_keys=("document_id", "start_line", "start_char", "end_line", "end_char"),
    ),
    DatasetRow(
        name="scip_symbol_info_v1",
        version=1,
        bundles=(),
        fields=(
            "symbol_info_id",
            "symbol_dict",
            "display_name_dict",
            "kind_dict",
            "enclosing_symbol_dict",
            "documentation",
            "signature_documentation",
        ),
        template="scip",
        postprocess="scip_symbol_info",
        join_keys=("symbol_dict",),
    ),
    DatasetRow(
        name="scip_symbol_relationships_v1",
        version=1,
        bundles=(),
        fields=(
            "relationship_id",
            "symbol_dict",
            "related_symbol_dict",
            "is_reference",
            "is_implementation",
            "is_type_definition",
            "is_definition",
        ),
        template="scip",
        postprocess="scip_symbol_relationships",
        join_keys=(
            "symbol_dict",
            "related_symbol_dict",
            "is_reference",
            "is_implementation",
            "is_type_definition",
            "is_definition",
        ),
    ),
    DatasetRow(
        name="scip_external_symbol_info_v1",
        version=1,
        bundles=(),
        fields=(
            "symbol_info_id",
            "symbol_dict",
            "display_name_dict",
            "kind_dict",
            "enclosing_symbol_dict",
            "documentation",
            "signature_documentation",
        ),
        template="scip",
        postprocess="scip_external_symbol_info",
        join_keys=("symbol_dict",),
    ),
    DatasetRow(
        name="scip_diagnostics_v1",
        version=1,
        bundles=("scip_range",),
        fields=(
            "diagnostic_id",
            "document_id",
            "path_dict",
            "severity_dict",
            "code_dict",
            "message",
            "source_dict",
            "tags",
        ),
        template="scip",
        postprocess="scip_diagnostics",
        join_keys=("document_id", "start_line", "start_char", "end_line", "end_char"),
    ),
)


def dataset_row(name: str) -> DatasetRow:
    """Return the dataset row spec by name.

    Returns
    -------
    DatasetRow
        Dataset row spec for the requested name.
    """
    by_name = {row.name: row for row in DATASET_ROWS}
    return by_name[name]


__all__ = ["DATASET_ROWS", "DatasetRow", "DerivedIdSpec", "dataset_row"]
