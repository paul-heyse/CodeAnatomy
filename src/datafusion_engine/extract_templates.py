"""Extractor templates for central extract metadata."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field

from arrow_utils.core.ordering import OrderingLevel
from datafusion_engine.arrow_schema.metadata import EvidenceMetadataSpec, evidence_metadata
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

    Returns
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


TEMPLATES: dict[str, ExtractorTemplate] = {
    "ast": ExtractorTemplate(
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
    ),
    "cst": ExtractorTemplate(
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
    ),
    "tree_sitter": ExtractorTemplate(
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
    ),
    "bytecode": ExtractorTemplate(
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
    ),
    "symtable": ExtractorTemplate(
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
    ),
    "repo_scan": ExtractorTemplate(
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
    ),
    "python_imports": ExtractorTemplate(
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
    ),
    "python_external": ExtractorTemplate(
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
    ),
    "scip": ExtractorTemplate(
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
    ),
    "tree_sitter": ExtractorConfigSpec(
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
    ),
    "bytecode": ExtractorConfigSpec(
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
    ),
    "symtable": ExtractorConfigSpec(
        extractor_name="symtable",
        defaults={
            "compile_type": "exec",
            "max_workers": None,
        },
    ),
    "repo_scan": ExtractorConfigSpec(
        extractor_name="repo_scan",
        defaults={
            "repo_id": None,
            "scope_policy": {},
            "include_sha256": True,
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
    ),
    "python_imports": ExtractorConfigSpec(
        extractor_name="python_imports",
        defaults={
            "repo_id": None,
        },
    ),
    "python_external": ExtractorConfigSpec(
        extractor_name="python_external",
        defaults={
            "repo_id": None,
            "include_stdlib": True,
            "include_unresolved": True,
            "max_imports": None,
            "depth": "metadata",
        },
    ),
    "repo_blobs": ExtractorConfigSpec(
        extractor_name="repo_blobs",
        defaults={
            "repo_id": None,
            "include_bytes": True,
            "include_text": True,
            "max_file_bytes": None,
            "max_files": None,
            "source_ref": None,
        },
    ),
    "scip": ExtractorConfigSpec(
        extractor_name="scip",
        defaults={
            "prefer_protobuf": True,
            "scip_pb2_import": None,
            "build_dir": None,
            "health_check": False,
            "log_counts": False,
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
            "fields": ["abs_path", "size_bytes", "mtime_ns"],
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
        "template": spec.template,
    }
    return (
        {
            **base,
            "name": "ast_files_v1",
            "bundles": None,
            "fields": None,
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
            "fields": None,
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
            "fields": None,
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
            "fields": None,
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
            "fields": None,
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
    "DatasetTemplateSpec",
    "ExtractorConfigSpec",
    "ExtractorTemplate",
    "config",
    "dataset_template_specs",
    "expand_dataset_templates",
    "flag_default",
    "template",
]
