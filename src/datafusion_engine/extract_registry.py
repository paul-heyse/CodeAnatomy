"""Central accessors for extract registry behavior."""

from __future__ import annotations

import importlib
from collections.abc import Mapping
from dataclasses import asdict, is_dataclass
from functools import cache
from typing import TYPE_CHECKING

from arrowdsl.core.interop import SchemaLike
from arrowdsl.schema.metadata import (
    extractor_option_defaults_from_metadata,
    extractor_option_defaults_spec,
    merge_metadata_specs,
    options_metadata_spec,
)
from arrowdsl.schema.policy import SchemaPolicyOptions, schema_policy_factory
from arrowdsl.schema.schema import EncodingPolicy, SchemaMetadataSpec
from datafusion_engine.extract_builders import QueryContext, build_query_spec
from datafusion_engine.extract_metadata import (
    ExtractMetadata,
    extract_metadata_by_name,
    extract_metadata_specs,
)
from datafusion_engine.extract_templates import config as extractor_config
from datafusion_engine.schema_introspection import SchemaIntrospector
from extract.evidence_specs import evidence_metadata_spec as extract_evidence_metadata_spec
from ibis_engine.query_compiler import IbisQuerySpec
from relspec.extract.registry_policies import policy_row, template_policy_row
from relspec.rules.definitions import RuleStage, stage_enabled
from relspec.rules.options import RuleExecutionOptions
from schema_spec.system import DatasetSpec, dataset_spec_from_schema

if TYPE_CHECKING:
    from datafusion import SessionContext

    from arrowdsl.core.execution_context import ExecutionContext
    from arrowdsl.schema.policy import SchemaPolicy

_OPTIONS_TYPE_ERROR = "Options must be a dataclass instance or dict."


class OptionsTypeError(TypeError):
    """Raised when options payload has an unsupported type."""

    def __init__(self) -> None:
        super().__init__(_OPTIONS_TYPE_ERROR)


def extract_metadata(name: str) -> ExtractMetadata:
    """Return extract metadata for a dataset name.

    Returns
    -------
    ExtractMetadata
        Extract metadata for the dataset.
    """
    return extract_metadata_by_name()[name]


def dataset_spec(name: str) -> DatasetSpec:
    """Return the DatasetSpec for the dataset name.

    Returns
    -------
    DatasetSpec
        Dataset specification for the name.
    """
    schema = dataset_schema(name)
    return dataset_spec_from_schema(name, schema)


def dataset_schema(name: str) -> SchemaLike:
    """Return the schema for the dataset name from DataFusion context.

    Returns
    -------
    SchemaLike
        Arrow schema for the dataset.

    Raises
    ------
    KeyError
        Raised when the dataset is not registered in the DataFusion context.
    """
    ctx = _resolve_session_context(None)
    try:
        return ctx.table(name).schema()
    except (KeyError, RuntimeError, TypeError, ValueError) as exc:
        msg = f"Unknown DataFusion schema: {name!r}."
        raise KeyError(msg) from exc


def _resolve_session_context(ctx: SessionContext | None) -> SessionContext:
    if ctx is not None:
        return ctx
    module = importlib.import_module("datafusion_engine.runtime")
    profile = module.DataFusionRuntimeProfile()
    return profile.session_context()


@cache
def dataset_query(name: str, *, repo_id: str | None = None) -> IbisQuerySpec:
    """Return the IbisQuerySpec for the dataset name.

    Returns
    -------
    IbisQuerySpec
        IbisQuerySpec for the dataset name.
    """
    row = extract_metadata(name)
    return build_query_spec(row, ctx=QueryContext(repo_id=repo_id))


def dataset_metadata_spec(name: str) -> SchemaMetadataSpec:
    """Return the base metadata spec for the dataset name.

    Returns
    -------
    SchemaMetadataSpec
        Base metadata spec for the dataset.
    """
    base = dataset_spec(name).metadata_spec
    return merge_metadata_specs(base, extract_evidence_metadata_spec(name))


def dataset_metadata_with_options(
    name: str,
    *,
    options: object | None = None,
    repo_id: str | None = None,
) -> SchemaMetadataSpec:
    """Return metadata spec merged with runtime options.

    Returns
    -------
    SchemaMetadataSpec
        Merged metadata specification.
    """
    base = dataset_metadata_spec(name)
    if options is None and repo_id is None:
        return base
    run_meta = options_metadata_spec(options=options, repo_id=repo_id)
    return merge_metadata_specs(base, run_meta)


def dataset_enabled(name: str, options: object | None = None) -> bool:
    """Return whether a dataset is enabled for the provided options.

    Returns
    -------
    bool
        ``True`` when the dataset is enabled.
    """
    row = extract_metadata(name)
    if row.enabled_when is None:
        return True
    stage = RuleStage(name=name, mode="source", enabled_when=row.enabled_when)
    return stage_enabled(stage, _options_mapping(options))


def enabled_datasets(
    options: object | None = None,
    *,
    template: str | None = None,
) -> tuple[str, ...]:
    """Return enabled dataset names, optionally filtered by template.

    Returns
    -------
    tuple[str, ...]
        Enabled dataset names.
    """
    enabled: list[str] = []
    for row in extract_metadata_specs():
        if template is not None and row.template != template:
            continue
        if dataset_enabled(row.name, options):
            enabled.append(row.name)
    return tuple(enabled)


def extractor_defaults(name: str) -> dict[str, object]:
    """Return default option values for an extractor.

    Returns
    -------
    dict[str, object]
        Default options for the extractor.
    """
    defaults = extractor_config(name).defaults
    spec = extractor_option_defaults_spec(defaults)
    if spec.schema_metadata:
        return extractor_option_defaults_from_metadata(spec.schema_metadata)
    return dict(defaults)


def _options_dict(options: object) -> dict[str, object]:
    if is_dataclass(options) and not isinstance(options, type):
        return asdict(options)
    if isinstance(options, dict):
        return dict(options)
    raise OptionsTypeError


def _options_mapping(options: object | None) -> Mapping[str, object]:
    if options is None:
        return {}
    if isinstance(options, RuleExecutionOptions):
        return options.as_mapping()
    if is_dataclass(options) and not isinstance(options, type):
        return asdict(options)
    if isinstance(options, Mapping):
        return options
    return {}


def normalize_options[T](name: str, options: object | None, factory: type[T]) -> T:
    """Normalize options by applying defaults for missing fields.

    Returns
    -------
    T
        Options object with defaults merged in.
    """
    defaults = extractor_defaults(name)
    if options is None:
        return factory(**defaults)
    merged = {**defaults, **_options_dict(options)}
    return factory(**merged)


def dataset_schema_policy(
    name: str,
    *,
    ctx: ExecutionContext,
    options: object | None = None,
    repo_id: str | None = None,
    enable_encoding: bool = True,
) -> SchemaPolicy:
    """Return the SchemaPolicy for a dataset name.

    Returns
    -------
    SchemaPolicy
        Schema policy derived from the dataset spec and registry overrides.
    """
    spec = dataset_spec(name).table_spec
    row = policy_row(name)
    template_row = template_policy_row(extract_metadata(name).template)
    safe_cast = row.safe_cast if row and row.safe_cast is not None else None
    if safe_cast is None and template_row is not None:
        safe_cast = template_row.safe_cast
    keep_extra_columns = (
        row.keep_extra_columns if row and row.keep_extra_columns is not None else None
    )
    if keep_extra_columns is None and template_row is not None:
        keep_extra_columns = template_row.keep_extra_columns
    on_error = row.on_error if row and row.on_error is not None else None
    if on_error is None and template_row is not None:
        on_error = template_row.on_error
    policy_options = SchemaPolicyOptions(
        schema=dataset_schema(name),
        metadata=dataset_metadata_with_options(name, options=options, repo_id=repo_id),
        safe_cast=safe_cast,
        keep_extra_columns=keep_extra_columns,
        on_error=on_error,
        encoding=None if enable_encoding else EncodingPolicy(),
    )
    return schema_policy_factory(spec, ctx=ctx, options=policy_options)


def validate_registry(*, repo_id: str | None = None) -> None:
    """Validate registry wiring by building specs, schemas, and queries.

    Raises
    ------
    KeyError
        Raised when extract datasets are missing from information_schema views.
    """
    ctx = _resolve_session_context(None)
    introspector = SchemaIntrospector(ctx)
    tables = {
        row["table_name"]
        for row in introspector.tables_snapshot()
        if isinstance(row.get("table_name"), str)
    }
    for row in extract_metadata_specs():
        if row.name not in tables:
            msg = f"Extract dataset not in information_schema.tables: {row.name!r}."
            raise KeyError(msg)
        _ = dataset_schema(row.name)
        columns = introspector.table_columns(row.name)
        if not columns:
            msg = f"Extract dataset missing information_schema columns: {row.name!r}."
            raise KeyError(msg)
        _ = dataset_query(row.name, repo_id=repo_id)
        _ = dataset_metadata_spec(row.name)


__all__ = [
    "dataset_enabled",
    "dataset_metadata_spec",
    "dataset_metadata_with_options",
    "dataset_query",
    "dataset_schema",
    "dataset_schema_policy",
    "dataset_spec",
    "enabled_datasets",
    "extract_metadata",
    "extractor_defaults",
    "normalize_options",
    "validate_registry",
]
