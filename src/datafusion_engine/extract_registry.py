"""Lightweight extract schema accessors for inference-driven planning."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from functools import cache

import pyarrow as pa

from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.interop import SchemaLike
from arrowdsl.schema.policy import SchemaPolicy, SchemaPolicyOptions, schema_policy_factory
from arrowdsl.schema.schema import SchemaMetadataSpec
from datafusion_engine.extract_metadata import ExtractMetadata, extract_metadata_by_name
from datafusion_engine.extract_templates import config
from ibis_engine.query_compiler import IbisQuerySpec
from schema_spec.system import DatasetSpec, dataset_spec_from_schema


def extract_metadata(name: str) -> ExtractMetadata:
    """Return extract metadata for a dataset name.

    Returns
    -------
    ExtractMetadata
        Extract metadata for the dataset.
    """
    return extract_metadata_by_name()[name]


@cache
def dataset_schema(name: str) -> SchemaLike:
    """Return a schema for extract datasets based on metadata fields.

    Returns
    -------
    SchemaLike
        Arrow schema with string-typed columns for metadata fields.
    """
    if name == "scip_index_v1":
        from datafusion_engine.schema_registry import SCIP_INDEX_SCHEMA

        return SCIP_INDEX_SCHEMA
    row = extract_metadata(name)
    fields = [
        pa.field(column, pa.string()) for column in (*row.fields, *row.row_fields, *row.row_extras)
    ]
    field_names = {field.name for field in fields}
    for derived in row.derived:
        if derived.name not in field_names:
            fields.append(pa.field(derived.name, pa.string()))
            field_names.add(derived.name)
    return pa.schema(fields)


def dataset_spec(name: str) -> DatasetSpec:
    """Return the DatasetSpec for the dataset name.

    Returns
    -------
    DatasetSpec
        Dataset specification for the name.
    """
    return dataset_spec_from_schema(name, dataset_schema(name))


def dataset_metadata_spec(name: str) -> SchemaMetadataSpec:
    """Return metadata spec for the dataset name.

    Returns
    -------
    SchemaMetadataSpec
        Metadata specification for the dataset.
    """
    return dataset_spec(name).metadata_spec


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
        Metadata spec with runtime defaults applied.
    """
    _ = (options, repo_id)
    return dataset_metadata_spec(name)


def dataset_schema_policy(
    name: str,
    *,
    ctx: ExecutionContext,
    options: object | None = None,
    repo_id: str | None = None,
    enable_encoding: bool = True,
) -> SchemaPolicy:
    """Return a schema policy derived from the dataset schema.

    Returns
    -------
    SchemaPolicy
        Policy configured from dataset metadata.
    """
    _ = (options, repo_id)
    spec = dataset_spec(name)
    return schema_policy_factory(
        spec.table_spec,
        ctx=ctx,
        options=SchemaPolicyOptions(encoding=spec.encoding_policy() if enable_encoding else None),
    )


def dataset_query(
    name: str,
    *,
    repo_id: str | None = None,
    projection: Sequence[str] | None = None,
) -> IbisQuerySpec:
    """Return a simple query spec for extract datasets.

    Returns
    -------
    IbisQuerySpec
        Query spec projecting all declared columns.
    """
    _ = repo_id
    row = extract_metadata(name)
    columns = [*row.fields, *row.row_fields, *row.row_extras]
    for derived in row.derived:
        if derived.name not in columns:
            columns.append(derived.name)
    if projection:
        projection_set = set(projection)
        schema = dataset_schema(name)
        filtered = [field.name for field in schema if field.name in projection_set]
        if filtered:
            columns = filtered
    return IbisQuerySpec.simple(*columns)


def normalize_options[T](name: str, options: object | None, options_type: type[T]) -> T:
    """Normalize extractor options into a typed options payload.

    Returns
    -------
    T
        Normalized options instance.

    Raises
    ------
    TypeError
        If options are not a mapping or an instance of the expected options type.
    """
    if isinstance(options, options_type):
        return options
    defaults = extractor_defaults(name)
    if options is None:
        return _build_options(name, options_type, defaults)
    if isinstance(options, Mapping):
        merged = {**defaults, **dict(options)}
        return _build_options(name, options_type, merged)
    msg = f"Options for {name!r} must be {options_type.__name__} or mapping."
    raise TypeError(msg)


def extractor_defaults(name: str) -> dict[str, object]:
    """Return extractor defaults for a dataset name.

    Returns
    -------
    dict[str, object]
        Defaults defined by the extract template, if any.
    """
    try:
        defaults = config(name).defaults
    except KeyError:
        return {}
    return dict(defaults)


def _build_options[T](name: str, options_type: type[T], values: Mapping[str, object]) -> T:
    """Build an options payload from normalized values.

    Returns
    -------
    T
        Options instance created from the mapping.

    Raises
    ------
    TypeError
        If the options payload cannot be constructed.
    """
    try:
        return options_type(**dict(values))
    except TypeError as exc:
        msg = f"Failed to build options for {name!r} using {options_type.__name__}."
        raise TypeError(msg) from exc


__all__ = [
    "dataset_metadata_spec",
    "dataset_metadata_with_options",
    "dataset_query",
    "dataset_schema",
    "dataset_schema_policy",
    "dataset_spec",
    "extract_metadata",
    "extractor_defaults",
    "normalize_options",
]
