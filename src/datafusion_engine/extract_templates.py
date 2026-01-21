"""Extractor templates for central extract metadata."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field

from arrowdsl.core.ordering import OrderingLevel
from registry_common.metadata import EvidenceMetadataSpec, evidence_metadata
from relspec.extract.registry_template_specs import DatasetTemplateSpec


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
    "runtime_inspect": ExtractorTemplate(
        extractor_name="runtime_inspect",
        evidence_rank=7,
        metadata_extra=evidence_metadata(
            spec=EvidenceMetadataSpec(
                evidence_family="runtime_inspect",
                coordinate_system="none",
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
            "scip_pb2_import": None,
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
            "fields": None,
            "derived": None,
            "row_fields": None,
            "row_extras": None,
            "ordering_keys": [{"column": "index_id", "order": "ascending"}],
            "join_keys": ["index_id"],
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


def _runtime_inspect_records(spec: DatasetTemplateSpec) -> tuple[DatasetRowRecord, ...]:
    version = _param_int(spec, "version", default=1)
    base: dict[str, object] = {
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
