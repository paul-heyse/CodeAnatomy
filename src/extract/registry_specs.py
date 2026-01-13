"""Programmatic registry accessors for extract datasets."""

from __future__ import annotations

from dataclasses import asdict, is_dataclass
from functools import cache
from typing import TYPE_CHECKING

from arrowdsl.core.interop import SchemaLike, TableLike
from arrowdsl.plan.query import QuerySpec
from arrowdsl.schema.metadata import (
    extractor_option_defaults_from_metadata,
    extractor_option_defaults_spec,
    merge_metadata_specs,
    options_metadata_spec,
)
from arrowdsl.schema.policy import SchemaPolicyOptions, schema_policy_factory
from arrowdsl.schema.schema import EncodingPolicy, SchemaMetadataSpec
from extract.evidence_specs import evidence_metadata_spec as extract_evidence_metadata_spec
from extract.registry_builders import (
    QueryContext,
    build_dataset_spec,
    build_query_spec,
    build_row_schema,
)
from extract.registry_pipelines import pipeline_spec
from extract.registry_policies import policy_row, template_policy_row
from extract.registry_rows import DATASET_ROWS, DatasetRow
from extract.registry_templates import config as extractor_config
from schema_spec.system import DatasetSpec

if TYPE_CHECKING:
    from arrowdsl.core.context import ExecutionContext
    from arrowdsl.schema.policy import SchemaPolicy

_ROWS_BY_NAME: dict[str, DatasetRow] = {row.name: row for row in DATASET_ROWS}
_OPTIONS_TYPE_ERROR = "Options must be a dataclass instance or dict."


class OptionsTypeError(TypeError):
    """Raised when options payload has an unsupported type."""

    def __init__(self) -> None:
        super().__init__(_OPTIONS_TYPE_ERROR)


def dataset_row(name: str) -> DatasetRow:
    """Return the dataset row spec by name.

    Returns
    -------
    DatasetRow
        Row specification for the dataset.
    """
    return _ROWS_BY_NAME[name]


@cache
def dataset_spec(name: str) -> DatasetSpec:
    """Return the DatasetSpec for the dataset name.

    Returns
    -------
    DatasetSpec
        Dataset specification for the name.
    """
    row = dataset_row(name)
    return build_dataset_spec(row, ctx=QueryContext())


@cache
def dataset_schema(name: str) -> SchemaLike:
    """Return the Arrow schema for the dataset name.

    Returns
    -------
    SchemaLike
        Arrow schema for the dataset.
    """
    return dataset_spec(name).schema()


@cache
def dataset_row_schema(name: str) -> SchemaLike:
    """Return the row-ingest schema for the dataset name.

    Returns
    -------
    SchemaLike
        Row-ingest schema for the dataset.
    """
    row = dataset_row(name)
    return build_row_schema(row)


def dataset_query(name: str, *, repo_id: str | None = None) -> QuerySpec:
    """Return the QuerySpec for the dataset name.

    Returns
    -------
    QuerySpec
        QuerySpec for the dataset name.
    """
    row = dataset_row(name)
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
    row = dataset_row(name)
    if row.enabled_when is None:
        return True
    return row.enabled_when(options)


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
    for row in DATASET_ROWS:
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
    template_row = template_policy_row(dataset_row(name).template)
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
        metadata=dataset_metadata_with_options(name, options=options, repo_id=repo_id),
        safe_cast=safe_cast,
        keep_extra_columns=keep_extra_columns,
        on_error=on_error,
        encoding=None if enable_encoding else EncodingPolicy(),
    )
    return schema_policy_factory(spec, ctx=ctx, options=policy_options)


def postprocess_table(name: str, table: TableLike) -> TableLike:
    """Apply any postprocess hook for a dataset name.

    Returns
    -------
    TableLike
        Postprocessed table when a hook exists.
    """
    kernels = pipeline_spec(name).post_kernels
    processed = table
    for kernel in kernels:
        processed = kernel(processed)
    return processed


def validate_registry(*, repo_id: str | None = None) -> None:
    """Validate registry wiring by building specs, schemas, and queries."""
    for row in DATASET_ROWS:
        _ = dataset_spec(row.name)
        _ = dataset_schema(row.name)
        _ = dataset_row_schema(row.name)
        _ = dataset_query(row.name, repo_id=repo_id)
        _ = dataset_metadata_spec(row.name)


__all__ = [
    "dataset_enabled",
    "dataset_metadata_spec",
    "dataset_metadata_with_options",
    "dataset_query",
    "dataset_row",
    "dataset_row_schema",
    "dataset_schema",
    "dataset_schema_policy",
    "dataset_spec",
    "enabled_datasets",
    "extractor_config",
    "extractor_defaults",
    "normalize_options",
    "postprocess_table",
    "validate_registry",
]
