"""Extractor templates with convention-based discovery.

Each extractor's metadata is defined as a module-level constant pair
(``_<NAME>_TEMPLATE`` / ``_<NAME>_CONFIG``) and collected by
``_discover_templates()`` / ``_discover_configs()`` into the public dicts.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field

from arrow_utils.core.ordering import OrderingLevel
from datafusion_engine.arrow.metadata import EvidenceMetadataSpec, evidence_metadata
from utils.registry_protocol import ImmutableRegistry


@dataclass(frozen=True)
class DatasetTemplateSpec:
    """Minimal template spec for extract dataset metadata expansion."""

    name: str
    template: str
    params: Mapping[str, object] = field(default_factory=dict)


_TEMPLATE_NAMES: tuple[str, ...] = (
    "repo_scan",
    "ast",
    "cst",
    "bytecode",
    "symtable",
    "scip",
    "tree_sitter",
    "python_imports",
    "python_external",
)


def dataset_template_specs() -> tuple[DatasetTemplateSpec, ...]:
    """Return extract dataset template specs in registry order.

    Returns:
    -------
    tuple[DatasetTemplateSpec, ...]
        Dataset template specs for metadata expansion.
    """
    return tuple(DatasetTemplateSpec(name=name, template=name) for name in _TEMPLATE_NAMES)


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


# ---------------------------------------------------------------------------
# Per-extractor TEMPLATE descriptors
# ---------------------------------------------------------------------------

_AST_TEMPLATE = ExtractorTemplate(
    extractor_name="ast",
    evidence_rank=4,
    metadata_extra=evidence_metadata(
        spec=EvidenceMetadataSpec(
            evidence_family="ast",
            coordinate_system="line_col",
            ambiguity_policy="preserve",
            superior_rank=4,
            streaming_safe=True,
            pipeline_breaker=False,
        ),
    ),
)

_CST_TEMPLATE = ExtractorTemplate(
    extractor_name="cst",
    evidence_rank=3,
    metadata_extra=evidence_metadata(
        spec=EvidenceMetadataSpec(
            evidence_family="cst",
            coordinate_system="bytes",
            ambiguity_policy="preserve",
            superior_rank=3,
            streaming_safe=True,
            pipeline_breaker=False,
        ),
        extra={
            b"type_inference_provider": b"pyre",
            b"type_inference_option": b"compute_type_inference",
            b"type_inference_fields": b"refs.inferred_type,callsites.inferred_type",
        },
    ),
)

_TREE_SITTER_TEMPLATE = ExtractorTemplate(
    extractor_name="tree_sitter",
    evidence_rank=6,
    metadata_extra=evidence_metadata(
        spec=EvidenceMetadataSpec(
            evidence_family="tree_sitter",
            coordinate_system="bytes",
            ambiguity_policy="preserve",
            superior_rank=6,
            streaming_safe=True,
            pipeline_breaker=False,
        ),
    ),
)

_BYTECODE_TEMPLATE = ExtractorTemplate(
    extractor_name="bytecode",
    evidence_rank=5,
    metadata_extra=evidence_metadata(
        spec=EvidenceMetadataSpec(
            evidence_family="bytecode",
            coordinate_system="offsets",
            ambiguity_policy="preserve",
            superior_rank=5,
            streaming_safe=True,
            pipeline_breaker=False,
        ),
    ),
)

_SYMTABLE_TEMPLATE = ExtractorTemplate(
    extractor_name="symtable",
    evidence_rank=2,
    metadata_extra=evidence_metadata(
        spec=EvidenceMetadataSpec(
            evidence_family="symtable",
            coordinate_system="line",
            ambiguity_policy="preserve",
            superior_rank=2,
            streaming_safe=True,
            pipeline_breaker=False,
        ),
    ),
)

_REPO_SCAN_TEMPLATE = ExtractorTemplate(
    extractor_name="repo_scan",
    evidence_rank=8,
    metadata_extra=evidence_metadata(
        spec=EvidenceMetadataSpec(
            evidence_family="repo_scan",
            coordinate_system="path",
            ambiguity_policy="preserve",
            superior_rank=8,
            streaming_safe=True,
            pipeline_breaker=False,
        ),
    ),
)

_PYTHON_IMPORTS_TEMPLATE = ExtractorTemplate(
    extractor_name="python_imports",
    evidence_rank=7,
    metadata_extra=evidence_metadata(
        spec=EvidenceMetadataSpec(
            evidence_family="python_imports",
            coordinate_system="module",
            ambiguity_policy="preserve",
            superior_rank=7,
            streaming_safe=True,
            pipeline_breaker=False,
        ),
    ),
)

_PYTHON_EXTERNAL_TEMPLATE = ExtractorTemplate(
    extractor_name="python_external",
    evidence_rank=7,
    metadata_extra=evidence_metadata(
        spec=EvidenceMetadataSpec(
            evidence_family="python_external",
            coordinate_system="module",
            ambiguity_policy="preserve",
            superior_rank=7,
            streaming_safe=True,
            pipeline_breaker=False,
        ),
    ),
)

_SCIP_TEMPLATE = ExtractorTemplate(
    extractor_name="scip",
    evidence_rank=1,
    metadata_extra=evidence_metadata(
        spec=EvidenceMetadataSpec(
            evidence_family="scip",
            coordinate_system="line_col",
            ambiguity_policy="preserve",
            superior_rank=1,
            streaming_safe=True,
            pipeline_breaker=False,
        ),
    ),
)


def _discover_templates() -> dict[str, ExtractorTemplate]:
    """Collect per-extractor TEMPLATE descriptors into a single dict.

    Returns:
        dict[str, ExtractorTemplate]: Templates keyed by extractor name.
    """
    descriptors: tuple[ExtractorTemplate, ...] = (
        _AST_TEMPLATE,
        _CST_TEMPLATE,
        _TREE_SITTER_TEMPLATE,
        _BYTECODE_TEMPLATE,
        _SYMTABLE_TEMPLATE,
        _REPO_SCAN_TEMPLATE,
        _PYTHON_IMPORTS_TEMPLATE,
        _PYTHON_EXTERNAL_TEMPLATE,
        _SCIP_TEMPLATE,
    )
    return {t.extractor_name: t for t in descriptors}


TEMPLATES: dict[str, ExtractorTemplate] = _discover_templates()


# ---------------------------------------------------------------------------
# Per-extractor CONFIG descriptors
# ---------------------------------------------------------------------------

_AST_CONFIG = ExtractorConfigSpec(
    extractor_name="ast",
    defaults={
        "type_comments": True,
        "feature_version": None,
    },
)

_CST_CONFIG = ExtractorConfigSpec(
    extractor_name="cst",
    feature_flags=(
        "include_parse_manifest",
        "include_parse_errors",
        "include_refs",
        "include_imports",
        "include_callsites",
        "include_defs",
        "include_type_exprs",
        "include_docstrings",
        "include_decorators",
        "include_call_args",
    ),
    defaults={
        "repo_id": None,
        "repo_root": None,
        "include_parse_manifest": True,
        "include_parse_errors": True,
        "include_refs": True,
        "include_imports": True,
        "include_callsites": True,
        "include_defs": True,
        "include_type_exprs": True,
        "include_docstrings": True,
        "include_decorators": True,
        "include_call_args": True,
        "compute_expr_context": True,
        "compute_qualified_names": True,
        "compute_fully_qualified_names": True,
        "compute_scope": True,
        "compute_type_inference": False,
    },
)

_TREE_SITTER_CONFIG = ExtractorConfigSpec(
    extractor_name="tree_sitter",
    feature_flags=(
        "include_nodes",
        "include_errors",
        "include_missing",
        "include_edges",
        "include_captures",
        "include_defs",
        "include_calls",
        "include_imports",
        "include_docstrings",
        "include_stats",
    ),
    defaults={
        "repo_id": None,
        "include_nodes": True,
        "include_errors": True,
        "include_missing": True,
        "include_edges": True,
        "include_captures": True,
        "include_defs": True,
        "include_calls": True,
        "include_imports": True,
        "include_docstrings": True,
        "include_stats": True,
        "extensions": None,
        "parser_timeout_micros": None,
        "query_match_limit": 10_000,
        "query_timeout_micros": None,
        "max_text_bytes": 256,
        "max_docstring_bytes": 2048,
        "incremental": False,
        "incremental_cache_size": 256,
    },
)

_BYTECODE_CONFIG = ExtractorConfigSpec(
    extractor_name="bytecode",
    feature_flags=("include_cfg_derivations",),
    defaults={
        "optimize": 0,
        "dont_inherit": True,
        "adaptive": False,
        "include_cfg_derivations": True,
        "max_workers": None,
        "parallel_min_files": 8,
        "parallel_max_bytes": 50_000_000,
        "terminator_opnames": (
            "RETURN_VALUE",
            "RETURN_CONST",
            "RAISE_VARARGS",
            "RERAISE",
        ),
    },
)

_SYMTABLE_CONFIG = ExtractorConfigSpec(
    extractor_name="symtable",
    defaults={
        "compile_type": "exec",
        "max_workers": None,
    },
)

_REPO_SCAN_CONFIG = ExtractorConfigSpec(
    extractor_name="repo_scan",
    defaults={
        "repo_id": None,
        "scope_policy": {},
        "include_sha256": True,
        "include_encoding": True,
        "encoding_sample_bytes": 8192,
        "max_file_bytes": None,
        "max_files": 200_000,
        "diff_base_ref": None,
        "diff_head_ref": None,
        "changed_only": False,
        "update_submodules": False,
        "submodule_update_init": True,
        "submodule_update_depth": None,
        "submodule_use_remote_auth": False,
        "record_blame": False,
        "blame_max_files": None,
        "blame_ref": None,
        "record_pathspec_trace": False,
        "pathspec_trace_limit": 200,
        "pathspec_trace_pattern_limit": 50,
    },
)

_PYTHON_IMPORTS_CONFIG = ExtractorConfigSpec(
    extractor_name="python_imports",
    defaults={
        "repo_id": None,
    },
)

_PYTHON_EXTERNAL_CONFIG = ExtractorConfigSpec(
    extractor_name="python_external",
    defaults={
        "repo_id": None,
        "include_stdlib": True,
        "include_unresolved": True,
        "max_imports": None,
        "depth": "metadata",
    },
)

_REPO_BLOBS_CONFIG = ExtractorConfigSpec(
    extractor_name="repo_blobs",
    defaults={
        "repo_id": None,
        "include_bytes": True,
        "include_text": True,
        "max_file_bytes": None,
        "max_files": None,
        "source_ref": None,
    },
)

_SCIP_CONFIG = ExtractorConfigSpec(
    extractor_name="scip",
    defaults={
        "prefer_protobuf": True,
        "scip_pb2_import": None,
        "build_dir": None,
        "health_check": False,
        "log_counts": False,
    },
)

_FILE_LINE_INDEX_CONFIG = ExtractorConfigSpec(
    extractor_name="file_line_index_v1",
    defaults={
        "repo_id": None,
        "max_files": None,
        "include_text": True,
        "max_line_text_bytes": 10_000,
    },
)


def _discover_configs() -> dict[str, ExtractorConfigSpec]:
    """Collect per-extractor CONFIG descriptors into a single dict.

    Returns:
        dict[str, ExtractorConfigSpec]: Config specs keyed by extractor name.
    """
    descriptors: tuple[ExtractorConfigSpec, ...] = (
        _AST_CONFIG,
        _CST_CONFIG,
        _TREE_SITTER_CONFIG,
        _BYTECODE_CONFIG,
        _SYMTABLE_CONFIG,
        _REPO_SCAN_CONFIG,
        _PYTHON_IMPORTS_CONFIG,
        _PYTHON_EXTERNAL_CONFIG,
        _REPO_BLOBS_CONFIG,
        _SCIP_CONFIG,
        _FILE_LINE_INDEX_CONFIG,
    )
    return {c.extractor_name: c for c in descriptors}


CONFIGS: dict[str, ExtractorConfigSpec] = _discover_configs()

_FLAG_DEFAULTS: dict[str, bool] = {
    flag: value
    for cfg in CONFIGS.values()
    for flag, value in cfg.defaults.items()
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
            "fields": ["abs_path", "size_bytes", "mtime_ns", "encoding"],
            "field_types": {
                "abs_path": "string",
                "size_bytes": "int64",
                "mtime_ns": "int64",
                "encoding": "string",
            },
            "nested_shapes": None,
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
            "bundles": [],
            "name": "python_extensions_v1",
            "fields": ["extension", "source", "repo_root"],
            "derived": None,
            "row_fields": None,
            "row_extras": None,
            "ordering_keys": [{"column": "extension", "order": "ascending"}],
            "join_keys": ["extension", "source", "repo_root"],
            "enabled_when": None,
            "feature_flag": None,
            "postprocess": None,
            "metadata_extra": None,
            "evidence_required_columns": None,
        },
        {
            **base,
            "bundles": [],
            "name": "scope_manifest_v1",
            "fields": [
                "path",
                "included",
                "ignored_by_git",
                "include_index",
                "exclude_index",
                "scope_kind",
                "repo_root",
                "is_untracked",
            ],
            "derived": None,
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
            "name": "repo_file_blobs_v1",
            "fields": ["abs_path", "size_bytes", "mtime_ns", "encoding", "text", "bytes"],
            "derived": _derived_specs(("file_id", "repo_file_id", "hash", None)),
            "row_fields": None,
            "row_extras": None,
            "ordering_keys": [{"column": "path", "order": "ascending"}],
            "join_keys": ["file_id"],
            "enabled_when": None,
            "feature_flag": None,
            "postprocess": None,
            "metadata_extra": None,
            "evidence_required_columns": None,
        },
        {
            **base,
            "name": "file_line_index_v1",
            "bundles": ["file_identity_no_sha"],
            "fields": [
                "file_id",
                "path",
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
        "template": spec.template,
    }
    return (
        {
            **base,
            "name": "ast_files_v1",
            "bundles": None,
            "fields": [
                "nodes",
                "edges",
                "errors",
                "docstrings",
                "imports",
                "defs",
                "calls",
                "type_ignores",
                "attrs",
            ],
            "field_types": {
                "nodes": "list<struct>",
                "edges": "list<struct>",
                "errors": "list<struct>",
                "docstrings": "list<struct>",
                "imports": "list<struct>",
                "defs": "list<struct>",
                "calls": "list<struct>",
                "type_ignores": "list<struct>",
                "attrs": "map<string, string>",
            },
            "nested_shapes": {
                "nodes": [
                    "ast_id",
                    "parent_ast_id",
                    "kind",
                    "name",
                    "value",
                    "span",
                    "attrs",
                ],
                "edges": ["src", "dst", "kind", "slot", "idx", "attrs"],
                "errors": ["error_type", "message", "span", "attrs"],
                "docstrings": [
                    "owner_ast_id",
                    "owner_kind",
                    "owner_name",
                    "docstring",
                    "span",
                    "source",
                    "attrs",
                ],
                "imports": [
                    "ast_id",
                    "parent_ast_id",
                    "kind",
                    "module",
                    "name",
                    "asname",
                    "alias_index",
                    "level",
                    "span",
                    "attrs",
                ],
                "defs": ["ast_id", "parent_ast_id", "kind", "name", "span", "attrs"],
                "calls": [
                    "ast_id",
                    "parent_ast_id",
                    "func_kind",
                    "func_name",
                    "span",
                    "attrs",
                ],
                "type_ignores": ["ast_id", "tag", "span", "attrs"],
            },
            "derived": None,
            "row_fields": None,
            "row_extras": None,
            "ordering_keys": [{"column": "path", "order": "ascending"}],
            "join_keys": ["file_id"],
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
        "template": spec.template,
    }
    return (
        {
            **base,
            "name": "libcst_files_v1",
            "bundles": None,
            "fields": [
                "nodes",
                "edges",
                "parse_manifest",
                "parse_errors",
                "refs",
                "imports",
                "callsites",
                "defs",
                "type_exprs",
                "docstrings",
                "decorators",
                "call_args",
                "attrs",
            ],
            "field_types": {
                "nodes": "list<struct>",
                "edges": "list<struct>",
                "parse_manifest": "list<struct>",
                "parse_errors": "list<struct>",
                "refs": "list<struct>",
                "imports": "list<struct>",
                "callsites": "list<struct>",
                "defs": "list<struct>",
                "type_exprs": "list<struct>",
                "docstrings": "list<struct>",
                "decorators": "list<struct>",
                "call_args": "list<struct>",
                "attrs": "map<string, string>",
            },
            "nested_shapes": {
                "nodes": ["cst_id", "kind", "span", "span_ws", "attrs"],
                "edges": ["src", "dst", "kind", "slot", "idx", "attrs"],
                "parse_manifest": [
                    "file_id",
                    "path",
                    "file_sha256",
                    "encoding",
                    "default_indent",
                    "default_newline",
                    "has_trailing_newline",
                    "future_imports",
                    "module_name",
                    "package_name",
                    "libcst_version",
                    "parser_backend",
                    "parsed_python_version",
                    "schema_identity_hash",
                ],
                "parse_errors": [
                    "file_id",
                    "path",
                    "file_sha256",
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
                    "meta",
                ],
                "refs": [
                    "file_id",
                    "path",
                    "file_sha256",
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
                    "attrs",
                ],
                "imports": [
                    "file_id",
                    "path",
                    "file_sha256",
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
                    "attrs",
                ],
                "callsites": [
                    "file_id",
                    "path",
                    "file_sha256",
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
                    "attrs",
                ],
                "defs": [
                    "file_id",
                    "path",
                    "file_sha256",
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
                    "attrs",
                ],
                "type_exprs": [
                    "file_id",
                    "path",
                    "file_sha256",
                    "owner_def_kind",
                    "owner_def_bstart",
                    "owner_def_bend",
                    "param_name",
                    "expr_kind",
                    "expr_role",
                    "bstart",
                    "bend",
                    "expr_text",
                ],
                "docstrings": [
                    "file_id",
                    "path",
                    "file_sha256",
                    "owner_def_id",
                    "owner_kind",
                    "owner_def_bstart",
                    "owner_def_bend",
                    "docstring",
                    "bstart",
                    "bend",
                ],
                "decorators": [
                    "file_id",
                    "path",
                    "file_sha256",
                    "owner_def_id",
                    "owner_kind",
                    "owner_def_bstart",
                    "owner_def_bend",
                    "decorator_text",
                    "decorator_index",
                    "bstart",
                    "bend",
                ],
                "call_args": [
                    "file_id",
                    "path",
                    "file_sha256",
                    "call_id",
                    "call_bstart",
                    "call_bend",
                    "arg_index",
                    "keyword",
                    "star",
                    "arg_text",
                    "bstart",
                    "bend",
                ],
            },
            "derived": None,
            "row_fields": None,
            "row_extras": None,
            "ordering_keys": [{"column": "path", "order": "ascending"}],
            "join_keys": ["file_id"],
            "enabled_when": None,
            "feature_flag": None,
            "postprocess": None,
            "metadata_extra": None,
            "evidence_required_columns": None,
        },
    )


def _bytecode_records(spec: DatasetTemplateSpec) -> tuple[DatasetRowRecord, ...]:
    version = _param_int(spec, "version", default=1)
    base = {
        "version": version,
        "template": spec.template,
    }
    return (
        {
            **base,
            "name": "bytecode_files_v1",
            "bundles": None,
            "fields": [
                "code_objects",
                "errors",
                "attrs",
            ],
            "field_types": {
                "code_objects": "list<struct>",
                "errors": "list<struct>",
                "attrs": "map<string, string>",
            },
            "nested_shapes": {
                "code_objects": [
                    "code_id",
                    "qualname",
                    "co_qualname",
                    "co_filename",
                    "name",
                    "firstlineno1",
                    "argcount",
                    "posonlyargcount",
                    "kwonlyargcount",
                    "nlocals",
                    "flags",
                    "flags_detail",
                    "stacksize",
                    "code_len",
                    "varnames",
                    "freevars",
                    "cellvars",
                    "names",
                    "consts",
                    "consts_json",
                    "line_table",
                    "instructions",
                    "exception_table",
                    "blocks",
                    "cfg_edges",
                    "dfg_edges",
                    "attrs",
                ],
                "errors": ["error_type", "message", "attrs"],
            },
            "derived": None,
            "row_fields": None,
            "row_extras": None,
            "ordering_keys": [{"column": "path", "order": "ascending"}],
            "join_keys": ["file_id"],
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
        "template": spec.template,
    }
    return (
        {
            **base,
            "name": "symtable_files_v1",
            "bundles": None,
            "fields": [
                "blocks",
                "attrs",
            ],
            "field_types": {
                "blocks": "list<struct>",
                "attrs": "map<string, string>",
            },
            "nested_shapes": {
                "blocks": [
                    "block_id",
                    "parent_block_id",
                    "block_type",
                    "is_meta_scope",
                    "name",
                    "lineno1",
                    "span_hint",
                    "scope_id",
                    "scope_local_id",
                    "scope_type_value",
                    "qualpath",
                    "function_partitions",
                    "class_methods",
                    "symbols",
                    "attrs",
                ],
            },
            "derived": None,
            "row_fields": None,
            "row_extras": None,
            "ordering_keys": [{"column": "path", "order": "ascending"}],
            "join_keys": ["file_id"],
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
            "name": "scip_index_v1",
            "bundles": None,
            "fields": [
                "index_id",
                "index_path",
                "metadata",
                "index_stats",
                "documents",
                "symbol_information",
                "external_symbol_information",
                "symbol_relationships",
                "signature_occurrences",
            ],
            "field_types": {
                "index_id": "string",
                "index_path": "string",
                "metadata": "struct",
                "index_stats": "struct",
                "documents": "list<struct>",
                "symbol_information": "list<struct>",
                "external_symbol_information": "list<struct>",
                "symbol_relationships": "list<struct>",
                "signature_occurrences": "list<struct>",
            },
            "nested_shapes": {
                "metadata": [
                    "protocol_version",
                    "tool_name",
                    "tool_version",
                    "tool_arguments",
                    "project_root",
                    "text_document_encoding",
                    "project_name",
                    "project_version",
                    "project_namespace",
                ],
                "index_stats": [
                    "document_count",
                    "occurrence_count",
                    "diagnostic_count",
                    "symbol_count",
                    "external_symbol_count",
                    "missing_position_encoding_count",
                    "document_text_count",
                    "document_text_bytes",
                ],
                "documents": [
                    "document_id",
                    "path",
                    "language",
                    "position_encoding",
                    "text",
                    "symbols",
                    "occurrences",
                    "diagnostics",
                ],
                "symbol_information": [
                    "symbol",
                    "display_name",
                    "kind",
                    "kind_name",
                    "enclosing_symbol",
                    "documentation",
                    "signature_text",
                    "signature_language",
                ],
                "external_symbol_information": [
                    "symbol",
                    "display_name",
                    "kind",
                    "kind_name",
                    "enclosing_symbol",
                    "documentation",
                    "signature_text",
                    "signature_language",
                ],
                "symbol_relationships": [
                    "symbol",
                    "related_symbol",
                    "is_reference",
                    "is_implementation",
                    "is_type_definition",
                    "is_definition",
                ],
                "signature_occurrences": [
                    "parent_symbol",
                    "symbol",
                    "symbol_roles",
                    "syntax_kind",
                    "syntax_kind_name",
                    "range_raw",
                    "start_line",
                    "start_char",
                    "end_line",
                    "end_char",
                    "range_len",
                    "line_base",
                    "col_unit",
                    "end_exclusive",
                    "is_definition",
                    "is_import",
                    "is_write",
                    "is_read",
                    "is_generated",
                    "is_test",
                    "is_forward_definition",
                ],
            },
            "derived": None,
            "row_fields": None,
            "row_extras": None,
            "ordering_keys": [{"column": "index_path", "order": "ascending"}],
            "join_keys": ["index_path"],
            "enabled_when": None,
            "feature_flag": None,
            "postprocess": None,
            "metadata_extra": None,
            "evidence_required_columns": None,
        },
    )


def _python_imports_records(spec: DatasetTemplateSpec) -> tuple[DatasetRowRecord, ...]:
    version = _param_int(spec, "version", default=1)
    base = {
        "version": version,
        "template": spec.template,
    }
    return (
        {
            **base,
            "name": "python_imports_v1",
            "bundles": None,
            "fields": [
                "file_id",
                "path",
                "source",
                "kind",
                "module",
                "name",
                "asname",
                "level",
                "is_star",
            ],
            "derived": None,
            "row_fields": None,
            "row_extras": None,
            "ordering_keys": [{"column": "path", "order": "ascending"}],
            "join_keys": [
                "file_id",
                "path",
                "source",
                "module",
                "name",
                "asname",
                "level",
                "is_star",
            ],
            "enabled_when": None,
            "feature_flag": None,
            "postprocess": None,
            "metadata_extra": None,
            "evidence_required_columns": None,
        },
    )


def _python_external_records(spec: DatasetTemplateSpec) -> tuple[DatasetRowRecord, ...]:
    version = _param_int(spec, "version", default=1)
    base = {
        "version": version,
        "template": spec.template,
    }
    return (
        {
            **base,
            "name": "python_external_interfaces_v1",
            "bundles": None,
            "fields": [
                "name",
                "status",
                "origin",
                "dist_name",
                "dist_version",
                "is_stdlib",
            ],
            "derived": None,
            "row_fields": None,
            "row_extras": None,
            "ordering_keys": [{"column": "name", "order": "ascending"}],
            "join_keys": ["name"],
            "enabled_when": None,
            "feature_flag": None,
            "postprocess": None,
            "metadata_extra": None,
            "evidence_required_columns": None,
        },
    )


def _tree_sitter_records(spec: DatasetTemplateSpec) -> tuple[DatasetRowRecord, ...]:
    version = _param_int(spec, "version", default=1)
    base = {
        "version": version,
        "template": spec.template,
    }
    return (
        {
            **base,
            "name": "tree_sitter_files_v1",
            "bundles": None,
            "fields": [
                "nodes",
                "edges",
                "errors",
                "missing",
                "captures",
                "defs",
                "calls",
                "imports",
                "docstrings",
                "stats",
                "attrs",
            ],
            "field_types": {
                "nodes": "list<struct>",
                "edges": "list<struct>",
                "errors": "list<struct>",
                "missing": "list<struct>",
                "captures": "list<struct>",
                "defs": "list<struct>",
                "calls": "list<struct>",
                "imports": "list<struct>",
                "docstrings": "list<struct>",
                "stats": "struct",
                "attrs": "map<string, string>",
            },
            "nested_shapes": {
                "nodes": [
                    "node_id",
                    "node_uid",
                    "parent_id",
                    "kind",
                    "kind_id",
                    "grammar_id",
                    "grammar_name",
                    "span",
                    "flags",
                    "attrs",
                ],
                "edges": ["parent_id", "child_id", "field_name", "child_index", "attrs"],
                "errors": ["error_id", "node_id", "span", "attrs"],
                "missing": ["missing_id", "node_id", "span", "attrs"],
                "captures": [
                    "capture_id",
                    "query_name",
                    "capture_name",
                    "pattern_index",
                    "node_id",
                    "node_kind",
                    "span",
                    "attrs",
                ],
                "defs": ["node_id", "parent_id", "kind", "name", "span", "attrs"],
                "calls": [
                    "node_id",
                    "parent_id",
                    "callee_kind",
                    "callee_text",
                    "callee_node_id",
                    "span",
                    "attrs",
                ],
                "imports": [
                    "node_id",
                    "parent_id",
                    "kind",
                    "module",
                    "name",
                    "asname",
                    "alias_index",
                    "level",
                    "span",
                    "attrs",
                ],
                "docstrings": [
                    "owner_node_id",
                    "owner_kind",
                    "owner_name",
                    "doc_node_id",
                    "docstring",
                    "source",
                    "span",
                    "attrs",
                ],
                "stats": [
                    "node_count",
                    "named_count",
                    "error_count",
                    "missing_count",
                    "parse_ms",
                    "parse_timed_out",
                    "incremental_used",
                    "query_match_count",
                    "query_capture_count",
                    "match_limit_exceeded",
                ],
            },
            "derived": None,
            "row_fields": None,
            "row_extras": None,
            "ordering_keys": [{"column": "path", "order": "ascending"}],
            "join_keys": ["file_id"],
            "enabled_when": None,
            "feature_flag": None,
            "postprocess": None,
            "metadata_extra": None,
            "evidence_required_columns": None,
        },
    )


_DATASET_TEMPLATE_REGISTRY: ImmutableRegistry[
    str, Callable[[DatasetTemplateSpec], tuple[DatasetRowRecord, ...]]
] = ImmutableRegistry.from_dict(
    {
        "repo_scan": _repo_scan_records,
        "ast": _ast_records,
        "cst": _cst_records,
        "bytecode": _bytecode_records,
        "symtable": _symtable_records,
        "scip": _scip_records,
        "python_imports": _python_imports_records,
        "python_external": _python_external_records,
        "tree_sitter": _tree_sitter_records,
    }
)


def expand_dataset_templates(
    specs: Sequence[DatasetTemplateSpec],
) -> tuple[DatasetRowRecord, ...]:
    """Expand dataset template specs into row record mappings.

    Args:
        specs: Description.

    Returns:
        tuple[DatasetRowRecord, ...]: Result.

    Raises:
        KeyError: If the operation cannot be completed.
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

    Returns:
    -------
    ExtractorTemplate
        Template configuration for the extractor.
    """
    return TEMPLATES[name]


def config(name: str) -> ExtractorConfigSpec:
    """Return the extractor configuration by name.

    Returns:
    -------
    ExtractorConfigSpec
        Configuration for the extractor.
    """
    return CONFIGS[name]


def flag_default(flag: str, *, fallback: bool = True) -> bool:
    """Return the default value for a feature flag.

    Returns:
    -------
    bool
        Default flag value when configured, else fallback.
    """
    return _FLAG_DEFAULTS.get(flag, fallback)


__all__ = [
    "CONFIGS",
    "TEMPLATES",
    "DatasetTemplateSpec",
    "ExtractorConfigSpec",
    "ExtractorTemplate",
    "config",
    "dataset_template_specs",
    "expand_dataset_templates",
    "flag_default",
    "template",
]
