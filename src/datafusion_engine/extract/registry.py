"""Lightweight extract schema accessors for inference-driven planning."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from functools import cache
from typing import Literal

import msgspec
import pyarrow as pa

from datafusion_engine.arrow.interop import SchemaLike
from datafusion_engine.arrow.metadata import SchemaMetadataSpec
from datafusion_engine.delta.protocol import DeltaFeatureGate
from datafusion_engine.expr.query_spec import QuerySpec
from datafusion_engine.extract.metadata import ExtractMetadata, extract_metadata_by_name
from datafusion_engine.extract.templates import config
from datafusion_engine.schema.policy import SchemaPolicy, SchemaPolicyOptions, schema_policy_factory
from schema_spec.contracts import (
    DatasetSpec,
    DeltaCdfPolicy,
    DeltaMaintenancePolicy,
    DeltaPolicyBundle,
    ValidationPolicySpec,
    dataset_spec_encoding_policy,
    dataset_spec_from_schema,
)
from storage.deltalake.config import DeltaSchemaPolicy, DeltaWritePolicy

_EXTRACT_DELTA_FEATURES: tuple[
    Literal[
        "change_data_feed",
        "column_mapping",
        "deletion_vectors",
        "in_commit_timestamps",
        "row_tracking",
        "v2_checkpoints",
    ],
    ...,
] = (
    "change_data_feed",
    "column_mapping",
    "v2_checkpoints",
)
_EXTRACT_FEATURE_GATE = DeltaFeatureGate(
    required_writer_features=_EXTRACT_DELTA_FEATURES,
)
_EXTRACT_CDF_POLICY = DeltaCdfPolicy(required=True, allow_out_of_range=False)
_EXTRACT_SCHEMA_POLICY = DeltaSchemaPolicy(column_mapping_mode="name")
_EXTRACT_WRITE_POLICY = DeltaWritePolicy(
    stats_policy="auto",
    enable_features=_EXTRACT_DELTA_FEATURES,
)
_EXTRACT_VALIDATION_POLICY = ValidationPolicySpec(enabled=True, lazy=True, sample=1000)
_EXTRACT_OPTIMIZE_TARGET_BYTES = 256 * 1024 * 1024
_EXTRACT_ZORDER_CANDIDATES: tuple[str, ...] = (
    "file_id",
    "path",
    "node_id",
    "edge_id",
    "span_id",
)
_LEGACY_DATASET_ALIASES: dict[str, str] = {}
_FILE_IDENTITY_QUERY_COLUMNS: tuple[str, ...] = (
    "repo",
    "path",
    "file_id",
    "file_sha256",
)

_REPO_FILE_BLOBS_SCHEMA = pa.schema(
    [
        ("file_id", pa.string()),
        ("repo_file_id", pa.string()),
        ("hash", pa.string()),
        ("path", pa.string()),
        ("file_sha256", pa.string()),
        ("abs_path", pa.string()),
        ("size_bytes", pa.int64()),
        ("mtime_ns", pa.int64()),
        ("encoding", pa.string()),
        ("text", pa.string()),
        ("bytes", pa.binary()),
    ]
)
_FILE_LINE_INDEX_SCHEMA = pa.schema(
    [
        ("file_id", pa.string()),
        ("path", pa.string()),
        ("line_no", pa.int64()),
        ("line_start_byte", pa.int64()),
        ("line_end_byte", pa.int64()),
        ("line_text", pa.string()),
        ("newline_kind", pa.string()),
    ]
)

_TYPED_SCHEMA_OVERRIDES: dict[str, pa.Schema] = {
    "repo_file_blobs_v1": _REPO_FILE_BLOBS_SCHEMA,
    "file_line_index_v1": _FILE_LINE_INDEX_SCHEMA,
}


def _static_root_extract_schema(name: str) -> pa.Schema | None:
    """Return static typed schemas for core root extract datasets."""
    from datafusion_engine.schema import (
        AST_FILES_SCHEMA,
        BYTECODE_FILES_SCHEMA,
        LIBCST_FILES_SCHEMA,
        REPO_FILES_SCHEMA,
        SCIP_INDEX_SCHEMA,
        SYMTABLE_FILES_SCHEMA,
        TREE_SITTER_FILES_SCHEMA,
    )

    static_schemas: dict[str, pa.Schema] = {
        "repo_files_v1": REPO_FILES_SCHEMA,
        "libcst_files_v1": LIBCST_FILES_SCHEMA,
        "ast_files_v1": AST_FILES_SCHEMA,
        "symtable_files_v1": SYMTABLE_FILES_SCHEMA,
        "tree_sitter_files_v1": TREE_SITTER_FILES_SCHEMA,
        "bytecode_files_v1": BYTECODE_FILES_SCHEMA,
        "scip_index_v1": SCIP_INDEX_SCHEMA,
    }
    return static_schemas.get(name)


def extract_metadata(name: str) -> ExtractMetadata:
    """Return extract metadata for a dataset name.

    Returns:
    -------
    ExtractMetadata
        Extract metadata for the dataset.
    """
    resolved_name = _LEGACY_DATASET_ALIASES.get(name, name)
    return extract_metadata_by_name()[resolved_name]


@cache
def dataset_schema(name: str) -> SchemaLike:
    """Return a schema for extract datasets based on metadata fields.

    Returns:
    -------
    SchemaLike
        Arrow schema with string-typed columns for metadata fields.
    """
    resolved_name = _LEGACY_DATASET_ALIASES.get(name, name)
    if resolved_name in _TYPED_SCHEMA_OVERRIDES:
        return _TYPED_SCHEMA_OVERRIDES[resolved_name]
    static_schema = _static_root_extract_schema(resolved_name)
    if static_schema is not None:
        return static_schema
    try:
        from datafusion_engine.schema import extract_schema_for

        return extract_schema_for(resolved_name)
    except KeyError:
        pass
    row = extract_metadata(resolved_name)
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

    Returns:
    -------
    DatasetSpec
        Dataset specification for the name.
    """
    schema = dataset_schema(name)
    spec = dataset_spec_from_schema(name, schema)
    delta_bundle = spec.policies.delta
    if delta_bundle is None:
        delta_bundle = DeltaPolicyBundle()
    delta_bundle = msgspec.structs.replace(
        delta_bundle,
        cdf_policy=_EXTRACT_CDF_POLICY,
        maintenance_policy=_extract_maintenance_policy(schema),
        write_policy=_EXTRACT_WRITE_POLICY,
        schema_policy=_EXTRACT_SCHEMA_POLICY,
        feature_gate=_EXTRACT_FEATURE_GATE,
    )
    policies = msgspec.structs.replace(
        spec.policies,
        delta=delta_bundle,
        dataframe_validation=_EXTRACT_VALIDATION_POLICY,
    )
    return msgspec.structs.replace(spec, policies=policies)


def _extract_maintenance_policy(schema: SchemaLike) -> DeltaMaintenancePolicy:
    resolved = schema if isinstance(schema, pa.Schema) else pa.schema(schema)
    z_order_cols = tuple(name for name in _EXTRACT_ZORDER_CANDIDATES if name in resolved.names)
    z_order_when = "after_partition_complete" if z_order_cols else "never"
    return DeltaMaintenancePolicy(
        optimize_on_write=True,
        optimize_target_size=_EXTRACT_OPTIMIZE_TARGET_BYTES,
        z_order_cols=z_order_cols,
        z_order_when=z_order_when,
        vacuum_on_write=False,
        enable_deletion_vectors=True,
        enable_v2_checkpoints=True,
        enable_log_compaction=True,
    )


def dataset_metadata_spec(name: str) -> SchemaMetadataSpec:
    """Return metadata spec for the dataset name.

    Returns:
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

    Returns:
    -------
    SchemaMetadataSpec
        Metadata spec with runtime defaults applied.
    """
    _ = (options, repo_id)
    return dataset_metadata_spec(name)


def dataset_schema_policy(
    name: str,
    *,
    options: object | None = None,
    repo_id: str | None = None,
    enable_encoding: bool = True,
) -> SchemaPolicy:
    """Return a schema policy derived from the dataset schema.

    Returns:
    -------
    SchemaPolicy
        Policy configured from dataset metadata.
    """
    _ = (options, repo_id)
    spec = dataset_spec(name)
    return schema_policy_factory(
        spec.table_spec,
        options=SchemaPolicyOptions(
            encoding=dataset_spec_encoding_policy(spec) if enable_encoding else None
        ),
    )


def dataset_query(
    name: str,
    *,
    repo_id: str | None = None,
    projection: Sequence[str] | None = None,
) -> QuerySpec:
    """Return a simple query spec for extract datasets.

    Returns:
    -------
    QuerySpec
        Query spec projecting all declared columns.
    """
    _ = repo_id
    row = extract_metadata(name)
    schema = dataset_schema(name)
    columns = _base_dataset_query_columns(row, schema)
    if projection:
        columns = _projected_dataset_query_columns(schema, projection, fallback=columns)
    return QuerySpec.simple(*columns)


def _base_dataset_query_columns(row: ExtractMetadata, schema: SchemaLike) -> list[str]:
    columns: list[str] = []
    seen: set[str] = set()
    schema_fields = {field.name for field in schema}

    def _append_column(column: str) -> None:
        if column in seen:
            return
        columns.append(column)
        seen.add(column)

    # Preserve root file-identity columns when present in the dataset schema.
    for identity_column in _FILE_IDENTITY_QUERY_COLUMNS:
        if identity_column in schema_fields:
            _append_column(identity_column)

    if row.bundles:
        from datafusion_engine.extract.bundles import bundle as _bundle

        for bundle_name in row.bundles:
            for field in _bundle(bundle_name).fields:
                _append_column(field.name)
    for field_name in (*row.fields, *row.row_fields, *row.row_extras):
        _append_column(field_name)
    for derived in row.derived:
        _append_column(derived.name)
    return columns


def _projected_dataset_query_columns(
    schema: SchemaLike,
    projection: Sequence[str],
    *,
    fallback: list[str],
) -> list[str]:
    projection_set = set(projection)
    filtered = [field.name for field in schema if field.name in projection_set]
    return filtered if filtered else fallback


def normalize_options[T](name: str, options: object | None, options_type: type[T]) -> T:
    """Normalize extractor options into a typed options payload.

    Args:
        name: Extractor name.
        options: Options object or mapping override.
        options_type: Target options dataclass/type.

    Returns:
        T: Result.

    Raises:
        TypeError: If options cannot be coerced to `options_type`.
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

    Returns:
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

    Args:
        name: Extractor name.
        options_type: Target options dataclass/type.
        values: Normalized values to pass into `options_type`.

    Returns:
        T: Result.

    Raises:
        TypeError: If constructing `options_type` fails.
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
