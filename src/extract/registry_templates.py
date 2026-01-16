"""Extractor templates for metadata defaults."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field

from arrowdsl.core.context import OrderingLevel
from extract.registry_template_specs import DatasetTemplateSpec


@dataclass(frozen=True)
class ExtractorTemplate:
    """Template for extractor metadata and ordering defaults."""

    extractor_name: str
    evidence_rank: int
    ordering_level: OrderingLevel = OrderingLevel.IMPLICIT
    metadata_extra: dict[bytes, bytes] = field(default_factory=dict)
    confidence: float = 1.0


@dataclass(frozen=True)
class ExtractorConfigSpec:
    """Programmatic default configuration for extractors."""

    extractor_name: str
    feature_flags: tuple[str, ...] = ()
    defaults: dict[str, object] = field(default_factory=dict)


def _param_int(spec: DatasetTemplateSpec, key: str, *, default: int) -> int:
    value = spec.params.get(key, default)
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, (str, bytes, bytearray)):
        try:
            return int(value)
        except ValueError as exc:
            msg = f"DatasetTemplateSpec {spec.name!r} param {key!r} must be an int."
            raise TypeError(msg) from exc
    msg = f"DatasetTemplateSpec {spec.name!r} param {key!r} must be an int."
    raise TypeError(msg)


TEMPLATES: dict[str, ExtractorTemplate] = {
    "ast": ExtractorTemplate(
        extractor_name="ast",
        evidence_rank=4,
        metadata_extra={
            b"evidence_family": b"ast",
            b"coordinate_system": b"line_col",
            b"ambiguity_policy": b"preserve",
            b"superior_rank": b"4",
            b"streaming_safe": b"true",
            b"pipeline_breaker": b"false",
        },
    ),
    "cst": ExtractorTemplate(
        extractor_name="cst",
        evidence_rank=3,
        metadata_extra={
            b"evidence_family": b"cst",
            b"coordinate_system": b"bytes",
            b"ambiguity_policy": b"preserve",
            b"superior_rank": b"3",
            b"streaming_safe": b"true",
            b"pipeline_breaker": b"false",
        },
    ),
    "tree_sitter": ExtractorTemplate(
        extractor_name="tree_sitter",
        evidence_rank=6,
        metadata_extra={
            b"evidence_family": b"tree_sitter",
            b"coordinate_system": b"bytes",
            b"ambiguity_policy": b"preserve",
            b"superior_rank": b"6",
            b"streaming_safe": b"true",
            b"pipeline_breaker": b"false",
        },
    ),
    "bytecode": ExtractorTemplate(
        extractor_name="bytecode",
        evidence_rank=5,
        metadata_extra={
            b"evidence_family": b"bytecode",
            b"coordinate_system": b"offsets",
            b"ambiguity_policy": b"preserve",
            b"superior_rank": b"5",
            b"streaming_safe": b"true",
            b"pipeline_breaker": b"false",
        },
    ),
    "symtable": ExtractorTemplate(
        extractor_name="symtable",
        evidence_rank=2,
        metadata_extra={
            b"evidence_family": b"symtable",
            b"coordinate_system": b"line",
            b"ambiguity_policy": b"preserve",
            b"superior_rank": b"2",
            b"streaming_safe": b"true",
            b"pipeline_breaker": b"false",
        },
    ),
    "repo_scan": ExtractorTemplate(
        extractor_name="repo_scan",
        evidence_rank=8,
        metadata_extra={
            b"evidence_family": b"repo_scan",
            b"coordinate_system": b"path",
            b"ambiguity_policy": b"preserve",
            b"superior_rank": b"8",
            b"streaming_safe": b"true",
            b"pipeline_breaker": b"false",
        },
    ),
    "runtime_inspect": ExtractorTemplate(
        extractor_name="runtime_inspect",
        evidence_rank=7,
        metadata_extra={
            b"evidence_family": b"runtime_inspect",
            b"coordinate_system": b"none",
            b"ambiguity_policy": b"preserve",
            b"superior_rank": b"7",
            b"streaming_safe": b"true",
            b"pipeline_breaker": b"false",
        },
    ),
    "scip": ExtractorTemplate(
        extractor_name="scip",
        evidence_rank=1,
        metadata_extra={
            b"evidence_family": b"scip",
            b"coordinate_system": b"line_col",
            b"ambiguity_policy": b"preserve",
            b"superior_rank": b"1",
            b"streaming_safe": b"true",
            b"pipeline_breaker": b"false",
        },
    ),
}


CONFIGS: dict[str, ExtractorConfigSpec] = {
    "ast": ExtractorConfigSpec(
        extractor_name="ast",
        defaults={
            "type_comments": True,
            "feature_version": None,
        },
    ),
    "cst": ExtractorConfigSpec(
        extractor_name="cst",
        feature_flags=(
            "include_parse_manifest",
            "include_parse_errors",
            "include_name_refs",
            "include_imports",
            "include_callsites",
            "include_defs",
            "include_type_exprs",
        ),
        defaults={
            "repo_id": None,
            "repo_root": None,
            "include_parse_manifest": True,
            "include_parse_errors": True,
            "include_name_refs": True,
            "include_imports": True,
            "include_callsites": True,
            "include_defs": True,
            "include_type_exprs": True,
            "compute_expr_context": True,
            "compute_qualified_names": True,
        },
    ),
    "tree_sitter": ExtractorConfigSpec(
        extractor_name="tree_sitter",
        feature_flags=("include_nodes", "include_errors", "include_missing"),
        defaults={
            "include_nodes": True,
            "include_errors": True,
            "include_missing": True,
        },
    ),
    "bytecode": ExtractorConfigSpec(
        extractor_name="bytecode",
        feature_flags=("include_cfg_derivations",),
        defaults={
            "optimize": 0,
            "dont_inherit": True,
            "adaptive": False,
            "include_cfg_derivations": True,
            "terminator_opnames": (
                "RETURN_VALUE",
                "RETURN_CONST",
                "RAISE_VARARGS",
                "RERAISE",
            ),
        },
    ),
    "symtable": ExtractorConfigSpec(
        extractor_name="symtable",
        defaults={
            "compile_type": "exec",
        },
    ),
    "repo_scan": ExtractorConfigSpec(
        extractor_name="repo_scan",
        defaults={
            "repo_id": None,
            "include_globs": ("**/*.py",),
            "exclude_dirs": (
                ".git",
                "__pycache__",
                ".venv",
                "venv",
                "node_modules",
                "dist",
                "build",
                ".mypy_cache",
                ".pytest_cache",
            ),
            "exclude_globs": (),
            "follow_symlinks": False,
            "include_bytes": True,
            "include_text": True,
            "max_file_bytes": None,
            "max_files": None,
        },
    ),
    "runtime_inspect": ExtractorConfigSpec(
        extractor_name="runtime_inspect",
        defaults={
            "module_allowlist": (),
            "timeout_s": 15,
        },
    ),
    "scip": ExtractorConfigSpec(
        extractor_name="scip",
        defaults={
            "prefer_protobuf": True,
            "allow_json_fallback": False,
            "scip_pb2_import": None,
            "scip_cli_bin": "scip",
            "build_dir": None,
            "health_check": False,
            "log_counts": False,
            "dictionary_encode_strings": False,
        },
    ),
}

_FLAG_DEFAULTS: dict[str, bool] = {
    flag: value
    for config in CONFIGS.values()
    for flag, value in config.defaults.items()
    if isinstance(value, bool)
}


DatasetRowRecord = Mapping[str, object]


def _derived_specs(
    *specs: tuple[str, str, str | None, Sequence[str] | None],
) -> list[Mapping[str, object]]:
    rows: list[Mapping[str, object]] = []
    for name, spec, kind, required in specs:
        rows.append(
            {
                "name": name,
                "spec": spec,
                "kind": kind or "masked_hash",
                "required": list(required) if required else None,
            }
        )
    return rows


def _repo_scan_records(spec: DatasetTemplateSpec) -> tuple[DatasetRowRecord, ...]:
    version = _param_int(spec, "version", default=1)
    base = {
        "version": version,
        "bundles": ["file_identity"],
        "template": spec.template,
    }
    return (
        {
            **base,
            "name": "repo_files_v1",
            "fields": ["abs_path", "size_bytes", "encoding", "text", "bytes"],
            "derived": _derived_specs(("file_id", "repo_file_id", "hash", None)),
            "row_fields": None,
            "row_extras": None,
            "ordering_keys": [{"column": "path", "order": "ascending"}],
            "join_keys": ["path"],
            "enabled_when": None,
            "feature_flag": None,
            "postprocess": None,
            "metadata_extra": None,
            "evidence_required_columns": None,
        },
        {
            **base,
            "name": "file_line_index_v1",
            "fields": [
                "line_no",
                "line_start_byte",
                "line_end_byte",
                "line_text",
                "newline_kind",
            ],
            "derived": None,
            "row_fields": None,
            "row_extras": None,
            "ordering_keys": [
                {"column": "path", "order": "ascending"},
                {"column": "line_no", "order": "ascending"},
            ],
            "join_keys": ["file_id", "line_no"],
            "enabled_when": None,
            "feature_flag": None,
            "postprocess": None,
            "metadata_extra": None,
            "evidence_required_columns": None,
        },
    )


def _ast_records(spec: DatasetTemplateSpec) -> tuple[DatasetRowRecord, ...]:
    version = _param_int(spec, "version", default=1)
    base = {
        "version": version,
        "bundles": ["file_identity"],
        "template": spec.template,
    }
    return (
        {
            **base,
            "name": "py_ast_nodes_v1",
            "fields": [
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
                "line_base",
                "col_unit",
                "end_exclusive",
            ],
            "derived": None,
            "row_fields": None,
            "row_extras": None,
            "ordering_keys": None,
            "join_keys": ["file_id", "ast_idx"],
            "enabled_when": None,
            "feature_flag": None,
            "postprocess": None,
            "metadata_extra": None,
            "evidence_required_columns": None,
        },
        {
            **base,
            "name": "py_ast_edges_v1",
            "fields": ["parent_ast_idx", "child_ast_idx", "field_name", "field_pos"],
            "derived": None,
            "row_fields": None,
            "row_extras": None,
            "ordering_keys": None,
            "join_keys": ["file_id", "parent_ast_idx", "child_ast_idx", "field_name", "field_pos"],
            "enabled_when": None,
            "feature_flag": None,
            "postprocess": None,
            "metadata_extra": None,
            "evidence_required_columns": None,
        },
        {
            **base,
            "name": "py_ast_errors_v1",
            "fields": [
                "error_type",
                "message",
                "lineno",
                "offset",
                "end_lineno",
                "end_offset",
                "line_base",
                "col_unit",
                "end_exclusive",
            ],
            "derived": None,
            "row_fields": None,
            "row_extras": None,
            "ordering_keys": None,
            "join_keys": ["file_id", "lineno", "offset", "end_lineno", "end_offset"],
            "enabled_when": None,
            "feature_flag": None,
            "postprocess": None,
            "metadata_extra": None,
            "evidence_required_columns": None,
        },
    )


def _cst_records(spec: DatasetTemplateSpec) -> tuple[DatasetRowRecord, ...]:
    version = _param_int(spec, "version", default=1)
    base = {
        "version": version,
        "bundles": ["file_identity"],
        "template": spec.template,
        "enabled_when": "feature_flag",
    }
    return (
        {
            **base,
            "name": "py_cst_parse_manifest_v1",
            "fields": [
                "encoding",
                "default_indent",
                "default_newline",
                "has_trailing_newline",
                "future_imports",
                "module_name",
                "package_name",
            ],
            "derived": None,
            "row_fields": None,
            "row_extras": None,
            "ordering_keys": None,
            "join_keys": ["file_id"],
            "feature_flag": "include_parse_manifest",
            "postprocess": None,
            "metadata_extra": None,
            "evidence_required_columns": None,
        },
        {
            **base,
            "name": "py_cst_parse_errors_v1",
            "fields": [
                "error_type",
                "message",
                "raw_line",
                "raw_column",
                "line_base",
                "col_unit",
                "end_exclusive",
                "meta",
            ],
            "derived": None,
            "row_fields": None,
            "row_extras": None,
            "ordering_keys": None,
            "join_keys": ["file_id", "raw_line", "raw_column"],
            "feature_flag": "include_parse_errors",
            "postprocess": None,
            "metadata_extra": None,
            "evidence_required_columns": None,
        },
        {
            **base,
            "name": "py_cst_name_refs_v1",
            "fields": ["name_ref_id", "span_id", "name", "expr_ctx", "bstart", "bend"],
            "derived": _derived_specs(
                ("name_ref_id", "cst_name_ref_id", "masked_hash", None),
                ("span_id", "span_id", "masked_hash", None),
            ),
            "row_fields": None,
            "row_extras": None,
            "ordering_keys": None,
            "join_keys": ["file_id", "bstart", "bend"],
            "feature_flag": "include_name_refs",
            "postprocess": None,
            "metadata_extra": None,
            "evidence_required_columns": ["name_ref_id", "span_id", "path"],
        },
        {
            **base,
            "name": "py_cst_imports_v1",
            "fields": [
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
            ],
            "derived": _derived_specs(
                ("import_id", "cst_import_id", "masked_hash", None),
                ("span_id", "cst_import_span_id", "masked_hash", None),
            ),
            "row_fields": None,
            "row_extras": None,
            "ordering_keys": None,
            "join_keys": ["file_id", "alias_bstart", "alias_bend"],
            "feature_flag": "include_imports",
            "postprocess": None,
            "metadata_extra": None,
            "evidence_required_columns": ["path"],
        },
        {
            **base,
            "name": "py_cst_callsites_v1",
            "bundles": ["file_identity", "call_span"],
            "fields": [
                "call_id",
                "span_id",
                "callee_bstart",
                "callee_bend",
                "callee_shape",
                "callee_text",
                "arg_count",
                "callee_dotted",
                "callee_qnames",
            ],
            "derived": _derived_specs(
                ("call_id", "cst_call_id", "masked_hash", None),
                ("span_id", "cst_call_span_id", "masked_hash", None),
            ),
            "row_fields": None,
            "row_extras": None,
            "ordering_keys": None,
            "join_keys": ["file_id", "call_bstart", "call_bend"],
            "feature_flag": "include_callsites",
            "postprocess": None,
            "metadata_extra": None,
            "evidence_required_columns": [
                "call_id",
                "span_id",
                "path",
                "callee_bstart",
                "callee_bend",
                "callee_qnames",
            ],
        },
        {
            **base,
            "name": "py_cst_defs_v1",
            "fields": [
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
            ],
            "derived": _derived_specs(
                ("def_id", "cst_def_id", "masked_hash", None),
                ("container_def_id", "cst_container_def_id", "masked_hash", None),
                ("span_id", "cst_def_span_id", "masked_hash", None),
            ),
            "row_fields": [
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
            ],
            "row_extras": None,
            "ordering_keys": None,
            "join_keys": ["file_id", "def_bstart", "def_bend"],
            "feature_flag": "include_defs",
            "postprocess": None,
            "metadata_extra": None,
            "evidence_required_columns": None,
        },
        {
            **base,
            "name": "py_cst_type_exprs_v1",
            "fields": [
                "type_expr_id",
                "owner_def_id",
                "span_id",
                "param_name",
                "expr_kind",
                "expr_role",
                "bstart",
                "bend",
                "expr_text",
            ],
            "derived": _derived_specs(
                ("type_expr_id", "cst_type_expr_id", "masked_hash", None),
                ("owner_def_id", "cst_owner_def_id", "masked_hash", None),
                ("span_id", "span_id", "masked_hash", None),
            ),
            "row_fields": [
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
            ],
            "row_extras": None,
            "ordering_keys": None,
            "join_keys": ["file_id", "bstart", "bend"],
            "feature_flag": "include_type_exprs",
            "postprocess": None,
            "metadata_extra": None,
            "evidence_required_columns": None,
        },
    )


def _bytecode_records(spec: DatasetTemplateSpec) -> tuple[DatasetRowRecord, ...]:
    version = _param_int(spec, "version", default=1)
    base = {
        "version": version,
        "bundles": ["file_identity"],
        "template": spec.template,
    }
    return (
        {
            **base,
            "name": "py_bc_code_units_v1",
            "fields": [
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
            ],
            "derived": _derived_specs(
                ("code_unit_id", "bc_code_unit_id", "masked_hash", None),
                ("parent_code_unit_id", "bc_parent_code_unit_id", "masked_hash", None),
            ),
            "row_fields": [
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
            ],
            "row_extras": None,
            "ordering_keys": None,
            "join_keys": ["file_id", "qualpath", "firstlineno", "co_name"],
            "enabled_when": None,
            "feature_flag": None,
            "postprocess": None,
            "metadata_extra": None,
            "evidence_required_columns": None,
        },
        {
            **base,
            "name": "py_bc_instructions_v1",
            "fields": [
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
                "line_base",
                "col_unit",
                "end_exclusive",
            ],
            "derived": _derived_specs(
                ("code_unit_id", "bc_code_unit_id", "masked_hash", None),
                ("instr_id", "bc_instr_id", "masked_hash", None),
            ),
            "row_fields": [
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
                "line_base",
                "col_unit",
                "end_exclusive",
            ],
            "row_extras": None,
            "ordering_keys": None,
            "join_keys": ["code_unit_id", "instr_index", "offset"],
            "enabled_when": None,
            "feature_flag": None,
            "postprocess": None,
            "metadata_extra": None,
            "evidence_required_columns": None,
        },
        {
            **base,
            "name": "py_bc_exception_table_v1",
            "fields": [
                "exc_entry_id",
                "code_unit_id",
                "exc_index",
                "start_offset",
                "end_offset",
                "target_offset",
                "depth",
                "lasti",
            ],
            "derived": _derived_specs(
                ("code_unit_id", "bc_code_unit_id", "masked_hash", None),
                ("exc_entry_id", "bc_exc_entry_id", "masked_hash", None),
            ),
            "row_fields": [
                "qualpath",
                "co_name",
                "firstlineno",
                "exc_index",
                "start_offset",
                "end_offset",
                "target_offset",
                "depth",
                "lasti",
            ],
            "row_extras": None,
            "ordering_keys": None,
            "join_keys": [
                "code_unit_id",
                "exc_index",
                "start_offset",
                "end_offset",
                "target_offset",
            ],
            "enabled_when": None,
            "feature_flag": None,
            "postprocess": None,
            "metadata_extra": None,
            "evidence_required_columns": None,
        },
        {
            **base,
            "name": "py_bc_blocks_v1",
            "fields": ["block_id", "code_unit_id", "start_offset", "end_offset", "kind"],
            "derived": _derived_specs(
                ("code_unit_id", "bc_code_unit_id", "masked_hash", None),
                ("block_id", "bc_block_id", "masked_hash", None),
            ),
            "row_fields": [
                "qualpath",
                "co_name",
                "firstlineno",
                "start_offset",
                "end_offset",
                "kind",
            ],
            "row_extras": None,
            "ordering_keys": None,
            "join_keys": ["code_unit_id", "start_offset", "end_offset"],
            "enabled_when": "feature_flag",
            "feature_flag": "include_cfg_derivations",
            "postprocess": None,
            "metadata_extra": None,
            "evidence_required_columns": None,
        },
        {
            **base,
            "name": "py_bc_cfg_edges_v1",
            "fields": [
                "edge_id",
                "code_unit_id",
                "src_block_id",
                "dst_block_id",
                "kind",
                "cond_instr_id",
                "exc_index",
            ],
            "derived": _derived_specs(
                ("code_unit_id", "bc_code_unit_id", "masked_hash", None),
                ("src_block_id", "bc_src_block_id", "masked_hash", None),
                ("dst_block_id", "bc_dst_block_id", "masked_hash", None),
                ("cond_instr_id", "bc_cond_instr_id", "masked_hash", None),
                ("edge_id", "bc_edge_id", "masked_hash", None),
            ),
            "row_fields": [
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
            ],
            "row_extras": None,
            "ordering_keys": None,
            "join_keys": [
                "code_unit_id",
                "src_block_id",
                "dst_block_id",
                "kind",
                "edge_key",
                "exc_index",
            ],
            "enabled_when": "feature_flag",
            "feature_flag": "include_cfg_derivations",
            "postprocess": None,
            "metadata_extra": None,
            "evidence_required_columns": None,
        },
        {
            **base,
            "name": "py_bc_errors_v1",
            "fields": ["error_type", "message"],
            "derived": None,
            "row_fields": None,
            "row_extras": None,
            "ordering_keys": None,
            "join_keys": ["file_id", "message"],
            "enabled_when": None,
            "feature_flag": None,
            "postprocess": None,
            "metadata_extra": None,
            "evidence_required_columns": None,
        },
    )


def _symtable_records(spec: DatasetTemplateSpec) -> tuple[DatasetRowRecord, ...]:
    version = _param_int(spec, "version", default=1)
    base = {
        "version": version,
        "bundles": ["file_identity"],
        "template": spec.template,
    }
    return (
        {
            **base,
            "name": "py_sym_scopes_v1",
            "fields": [
                "scope_id",
                "table_id",
                "scope_type",
                "scope_name",
                "lineno",
                "is_nested",
                "is_optimized",
                "has_children",
                "scope_role",
            ],
            "derived": _derived_specs(("scope_id", "sym_scope_id", "masked_hash", None)),
            "row_fields": [
                "table_id",
                "scope_type",
                "scope_name",
                "lineno",
                "is_nested",
                "is_optimized",
                "has_children",
                "scope_role",
            ],
            "row_extras": None,
            "ordering_keys": None,
            "join_keys": ["file_id", "table_id"],
            "enabled_when": None,
            "feature_flag": None,
            "postprocess": None,
            "metadata_extra": None,
            "evidence_required_columns": None,
        },
        {
            **base,
            "name": "py_sym_symbols_v1",
            "fields": [
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
            ],
            "derived": _derived_specs(("symbol_row_id", "sym_symbol_row_id", "masked_hash", None)),
            "row_fields": [
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
            ],
            "row_extras": None,
            "ordering_keys": None,
            "join_keys": ["file_id", "scope_id", "name"],
            "enabled_when": None,
            "feature_flag": None,
            "postprocess": None,
            "metadata_extra": None,
            "evidence_required_columns": None,
        },
        {
            **base,
            "name": "py_sym_scope_edges_v1",
            "fields": ["edge_id", "parent_scope_id", "child_scope_id"],
            "derived": _derived_specs(("edge_id", "sym_scope_edge_id", "masked_hash", None)),
            "row_fields": ["parent_table_id", "child_table_id"],
            "row_extras": None,
            "ordering_keys": None,
            "join_keys": ["file_id", "parent_scope_id", "child_scope_id"],
            "enabled_when": None,
            "feature_flag": None,
            "postprocess": None,
            "metadata_extra": None,
            "evidence_required_columns": None,
        },
        {
            **base,
            "name": "py_sym_namespace_edges_v1",
            "fields": ["edge_id", "scope_id", "symbol_row_id", "child_scope_id"],
            "derived": _derived_specs(
                ("symbol_row_id", "sym_ns_symbol_row_id", "masked_hash", None),
                ("edge_id", "sym_ns_edge_id", "masked_hash", None),
            ),
            "row_fields": ["scope_table_id", "symbol_name", "child_table_id"],
            "row_extras": None,
            "ordering_keys": None,
            "join_keys": ["file_id", "scope_id", "symbol_row_id", "child_scope_id"],
            "enabled_when": None,
            "feature_flag": None,
            "postprocess": None,
            "metadata_extra": None,
            "evidence_required_columns": None,
        },
        {
            **base,
            "name": "py_sym_function_partitions_v1",
            "fields": ["scope_id", "parameters", "locals", "globals", "nonlocals", "frees"],
            "derived": None,
            "row_fields": [
                "scope_table_id",
                "parameters",
                "locals",
                "globals",
                "nonlocals",
                "frees",
            ],
            "row_extras": None,
            "ordering_keys": None,
            "join_keys": ["file_id", "scope_id"],
            "enabled_when": None,
            "feature_flag": None,
            "postprocess": None,
            "metadata_extra": None,
            "evidence_required_columns": None,
        },
    )


def _scip_records(spec: DatasetTemplateSpec) -> tuple[DatasetRowRecord, ...]:
    version = _param_int(spec, "version", default=1)
    base = {
        "version": version,
        "template": spec.template,
    }
    return (
        {
            **base,
            "name": "scip_metadata_v1",
            "bundles": None,
            "fields": [
                "tool_name_dict",
                "tool_version_dict",
                "project_root_dict",
                "text_document_encoding_dict",
                "protocol_version_dict",
                "meta",
            ],
            "derived": None,
            "row_fields": None,
            "row_extras": None,
            "ordering_keys": None,
            "join_keys": ["project_root_dict", "tool_name_dict", "tool_version_dict"],
            "enabled_when": None,
            "feature_flag": None,
            "postprocess": None,
            "metadata_extra": None,
            "evidence_required_columns": None,
        },
        {
            **base,
            "name": "scip_documents_v1",
            "bundles": None,
            "fields": ["document_id", "path_dict", "language_dict", "position_encoding_dict"],
            "derived": None,
            "row_fields": None,
            "row_extras": None,
            "ordering_keys": None,
            "join_keys": ["path_dict"],
            "enabled_when": None,
            "feature_flag": None,
            "postprocess": "scip_documents",
            "metadata_extra": None,
            "evidence_required_columns": None,
        },
        {
            **base,
            "name": "scip_occurrences_v1",
            "bundles": ["scip_range_len", "scip_range_enc_len"],
            "fields": [
                "occurrence_id",
                "document_id",
                "path_dict",
                "symbol_dict",
                "symbol_roles",
                "syntax_kind_dict",
                "override_documentation",
                "line_base",
                "col_unit",
                "end_exclusive",
            ],
            "derived": None,
            "row_fields": None,
            "row_extras": None,
            "ordering_keys": None,
            "join_keys": ["document_id", "start_line", "start_char", "end_line", "end_char"],
            "enabled_when": None,
            "feature_flag": None,
            "postprocess": "scip_occurrences",
            "metadata_extra": None,
            "evidence_required_columns": ["path", "symbol", "symbol_roles"],
        },
        {
            **base,
            "name": "scip_symbol_info_v1",
            "bundles": None,
            "fields": [
                "symbol_info_id",
                "symbol_dict",
                "display_name_dict",
                "kind_dict",
                "enclosing_symbol_dict",
                "documentation",
                "signature_documentation",
            ],
            "derived": None,
            "row_fields": None,
            "row_extras": None,
            "ordering_keys": None,
            "join_keys": ["symbol_dict"],
            "enabled_when": None,
            "feature_flag": None,
            "postprocess": "scip_symbol_info",
            "metadata_extra": None,
            "evidence_required_columns": None,
        },
        {
            **base,
            "name": "scip_symbol_relationships_v1",
            "bundles": None,
            "fields": [
                "relationship_id",
                "symbol_dict",
                "related_symbol_dict",
                "is_reference",
                "is_implementation",
                "is_type_definition",
                "is_definition",
            ],
            "derived": None,
            "row_fields": None,
            "row_extras": None,
            "ordering_keys": None,
            "join_keys": [
                "symbol_dict",
                "related_symbol_dict",
                "is_reference",
                "is_implementation",
                "is_type_definition",
                "is_definition",
            ],
            "enabled_when": None,
            "feature_flag": None,
            "postprocess": "scip_symbol_relationships",
            "metadata_extra": None,
            "evidence_required_columns": None,
        },
        {
            **base,
            "name": "scip_external_symbol_info_v1",
            "bundles": None,
            "fields": [
                "symbol_info_id",
                "symbol_dict",
                "display_name_dict",
                "kind_dict",
                "enclosing_symbol_dict",
                "documentation",
                "signature_documentation",
            ],
            "derived": None,
            "row_fields": None,
            "row_extras": None,
            "ordering_keys": None,
            "join_keys": ["symbol_dict"],
            "enabled_when": None,
            "feature_flag": None,
            "postprocess": "scip_external_symbol_info",
            "metadata_extra": None,
            "evidence_required_columns": None,
        },
        {
            **base,
            "name": "scip_diagnostics_v1",
            "bundles": ["scip_range"],
            "fields": [
                "diagnostic_id",
                "document_id",
                "path_dict",
                "severity_dict",
                "code_dict",
                "message",
                "source_dict",
                "tags",
                "line_base",
                "col_unit",
                "end_exclusive",
            ],
            "derived": None,
            "row_fields": None,
            "row_extras": None,
            "ordering_keys": None,
            "join_keys": ["document_id", "start_line", "start_char", "end_line", "end_char"],
            "enabled_when": None,
            "feature_flag": None,
            "postprocess": "scip_diagnostics",
            "metadata_extra": None,
            "evidence_required_columns": None,
        },
    )


def _tree_sitter_records(spec: DatasetTemplateSpec) -> tuple[DatasetRowRecord, ...]:
    version = _param_int(spec, "version", default=1)
    base = {
        "version": version,
        "bundles": ["file_identity"],
        "template": spec.template,
        "enabled_when": "feature_flag",
    }
    return (
        {
            **base,
            "name": "ts_nodes_v1",
            "fields": [
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
            ],
            "derived": _derived_specs(
                ("ts_node_id", "ts_node_id", None, None),
                ("parent_ts_id", "ts_parent_node_id", None, None),
                ("span_id", "ts_span_id", None, None),
            ),
            "row_fields": [
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
            ],
            "join_keys": ["file_id", "start_byte", "end_byte", "ts_type"],
            "feature_flag": "include_nodes",
        },
        {
            **base,
            "name": "ts_errors_v1",
            "fields": [
                "ts_error_id",
                "ts_node_id",
                "span_id",
                "ts_type",
                "start_byte",
                "end_byte",
                "is_error",
            ],
            "derived": _derived_specs(
                ("ts_node_id", "ts_node_id", None, None),
                ("ts_error_id", "ts_error_id", None, None),
                ("span_id", "ts_span_id", None, None),
            ),
            "join_keys": ["file_id", "start_byte", "end_byte"],
            "feature_flag": "include_errors",
        },
        {
            **base,
            "name": "ts_missing_v1",
            "fields": [
                "ts_missing_id",
                "ts_node_id",
                "span_id",
                "ts_type",
                "start_byte",
                "end_byte",
                "is_missing",
            ],
            "derived": _derived_specs(
                ("ts_node_id", "ts_node_id", None, None),
                ("ts_missing_id", "ts_missing_id", None, None),
                ("span_id", "ts_span_id", None, None),
            ),
            "join_keys": ["file_id", "start_byte", "end_byte"],
            "feature_flag": "include_missing",
        },
    )


def _runtime_inspect_records(spec: DatasetTemplateSpec) -> tuple[DatasetRowRecord, ...]:
    version = _param_int(spec, "version", default=1)
    base = {
        "version": version,
        "bundles": None,
        "template": spec.template,
        "enabled_when": "allowlist",
    }
    return (
        {
            **base,
            "name": "rt_objects_v1",
            "fields": [
                "rt_id",
                "module",
                "qualname",
                "name",
                "obj_type",
                "source_path",
                "source_line",
                "meta",
            ],
            "derived": _derived_specs(("rt_id", "rt_object_id", None, None)),
            "row_fields": [
                "object_key",
                "module",
                "qualname",
                "name",
                "obj_type",
                "source_path",
                "source_line",
                "meta",
            ],
            "join_keys": ["module", "qualname"],
        },
        {
            **base,
            "name": "rt_signatures_v1",
            "fields": ["sig_id", "rt_id", "signature", "return_annotation"],
            "derived": _derived_specs(("sig_id", "rt_signature_id", None, None)),
            "row_fields": ["object_key", "signature", "return_annotation"],
            "join_keys": ["rt_id", "signature"],
        },
        {
            **base,
            "name": "rt_signature_params_v1",
            "fields": [
                "param_id",
                "sig_id",
                "name",
                "kind",
                "default_repr",
                "annotation_repr",
                "position",
            ],
            "derived": _derived_specs(("param_id", "rt_param_id", None, None)),
            "row_fields": [
                "object_key",
                "signature",
                "name",
                "kind",
                "default_repr",
                "annotation_repr",
                "position",
            ],
            "join_keys": ["sig_id", "name", "position"],
        },
        {
            **base,
            "name": "rt_members_v1",
            "fields": [
                "member_id",
                "rt_id",
                "name",
                "member_kind",
                "value_repr",
                "value_module",
                "value_qualname",
            ],
            "derived": _derived_specs(("member_id", "rt_member_id", None, None)),
            "row_fields": [
                "object_key",
                "name",
                "member_kind",
                "value_repr",
                "value_module",
                "value_qualname",
            ],
            "join_keys": ["rt_id", "name"],
        },
    )


_DATASET_TEMPLATE_REGISTRY: Mapping[
    str, Callable[[DatasetTemplateSpec], tuple[DatasetRowRecord, ...]]
] = {
    "repo_scan": _repo_scan_records,
    "ast": _ast_records,
    "cst": _cst_records,
    "bytecode": _bytecode_records,
    "symtable": _symtable_records,
    "scip": _scip_records,
    "tree_sitter": _tree_sitter_records,
    "runtime_inspect": _runtime_inspect_records,
}


def expand_dataset_templates(
    specs: Sequence[DatasetTemplateSpec],
) -> tuple[DatasetRowRecord, ...]:
    """Expand dataset template specs into row record mappings.

    Returns
    -------
    tuple[Mapping[str, object], ...]
        Expanded dataset row records.

    Raises
    ------
    KeyError
        Raised when a template name is unknown.
    """
    records: list[DatasetRowRecord] = []
    for spec in specs:
        handler = _DATASET_TEMPLATE_REGISTRY.get(spec.template)
        if handler is None:
            msg = f"Unknown dataset template: {spec.template!r}."
            raise KeyError(msg)
        records.extend(handler(spec))
    return tuple(records)


def template(name: str) -> ExtractorTemplate:
    """Return the extractor template by name.

    Returns
    -------
    ExtractorTemplate
        Template configuration for the extractor.
    """
    return TEMPLATES[name]


def config(name: str) -> ExtractorConfigSpec:
    """Return the extractor configuration by name.

    Returns
    -------
    ExtractorConfigSpec
        Configuration for the extractor.
    """
    return CONFIGS[name]


def flag_default(flag: str, *, fallback: bool = True) -> bool:
    """Return the default value for a feature flag.

    Returns
    -------
    bool
        Default flag value when configured, else fallback.
    """
    return _FLAG_DEFAULTS.get(flag, fallback)


__all__ = [
    "ExtractorConfigSpec",
    "ExtractorTemplate",
    "config",
    "expand_dataset_templates",
    "flag_default",
    "template",
]
