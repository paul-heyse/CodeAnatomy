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
