"""Schema spec serialization helpers."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Literal

import msgspec
import pyarrow as pa

from arrowdsl.schema.schema import SchemaEvolutionSpec, SchemaMetadataSpec
from arrowdsl.schema.serialization import schema_from_msgpack, schema_to_msgpack
from arrowdsl.schema.validation import ArrowValidationOptions
from ibis_engine.query_compiler import IbisProjectionSpec, IbisQuerySpec
from schema_spec.specs import DerivedFieldSpec, TableSchemaSpec
from schema_spec.system import (
    ContractSpec,
    DataFusionScanOptions,
    DatasetSpec,
    DedupeSpecSpec,
    DeltaScanOptions,
    ParquetColumnOptions,
    SortKeySpec,
    TableSchemaContract,
)
from schema_spec.view_specs import ViewSpec
from serde_msgspec import StructBase, dumps_json, dumps_msgpack, loads_msgpack
from sqlglot_tools.expr_spec import SqlExprSpec
from storage.deltalake.config import DeltaSchemaPolicy, DeltaWritePolicy


class SqlExprPayload(StructBase, frozen=True):
    """Serializable SQL expression payload."""

    sql: str
    policy_name: str = "datafusion_compile"
    dialect: str | None = None


class DerivedFieldPayload(StructBase, frozen=True):
    """Serializable derived field payload."""

    name: str
    expr: SqlExprPayload


class TableSchemaPayload(StructBase, frozen=True):
    """Serializable table schema payload."""

    name: str
    schema_msgpack: bytes


class SortKeyPayload(StructBase, frozen=True):
    """Serializable sort key payload."""

    column: str
    order: Literal["ascending", "descending"] = "ascending"


class DedupeSpecPayload(StructBase, frozen=True):
    """Serializable dedupe spec payload."""

    keys: tuple[str, ...]
    tie_breakers: tuple[SortKeyPayload, ...] = ()
    strategy: Literal[
        "KEEP_FIRST_AFTER_SORT",
        "KEEP_BEST_BY_SCORE",
        "COLLAPSE_LIST",
        "KEEP_ARBITRARY",
    ] = "KEEP_FIRST_AFTER_SORT"


class ArrowValidationPayload(StructBase, frozen=True):
    """Serializable Arrow validation options."""

    strict: bool | Literal["filter"] = "filter"
    coerce: bool = False
    max_errors: int | None = None
    emit_invalid_rows: bool = True
    emit_error_table: bool = True


class VirtualFieldPayload(StructBase, frozen=True):
    """Serializable virtual field payload."""

    fields: tuple[str, ...] = ()
    docs: Mapping[str, str] | None = None


class ViewSpecPayload(StructBase, frozen=True):
    """Serializable view spec payload."""

    name: str
    sql: str | None
    schema_msgpack: bytes
    builder_name: str | None = None


class IbisProjectionPayload(StructBase, frozen=True):
    """Serializable Ibis projection payload."""

    base: tuple[str, ...]
    derived: tuple[DerivedFieldPayload, ...] = ()


class IbisQueryPayload(StructBase, frozen=True):
    """Serializable Ibis query payload."""

    projection: IbisProjectionPayload
    predicate: SqlExprPayload | None = None
    pushdown_predicate: SqlExprPayload | None = None


class SchemaEvolutionPayload(StructBase, frozen=True):
    """Serializable schema evolution payload."""

    promote_options: str = "permissive"
    rename_map: Mapping[str, str] = msgspec.field(default_factory=dict)
    allow_missing: bool = False
    allow_extra: bool = True
    allow_casts: bool = True


class SchemaMetadataPayload(StructBase, frozen=True):
    """Serializable schema metadata payload."""

    schema_metadata: Mapping[str, str] = msgspec.field(default_factory=dict)
    field_metadata: Mapping[str, Mapping[str, str]] = msgspec.field(default_factory=dict)


class ParquetColumnOptionsPayload(StructBase, frozen=True):
    """Serializable Parquet column options payload."""

    statistics_enabled: tuple[str, ...] = ()
    bloom_filter_enabled: tuple[str, ...] = ()
    dictionary_enabled: tuple[str, ...] = ()


class TableSchemaContractPayload(StructBase, frozen=True):
    """Serializable TableSchemaContract payload."""

    file_schema_msgpack: bytes
    partition_schema_msgpack: bytes | None = None


class DataFusionScanPayload(StructBase, frozen=True):
    """Serializable DataFusion scan payload."""

    partition_schema_msgpack: bytes | None = None
    file_sort_order: tuple[str, ...] = ()
    parquet_pruning: bool = True
    skip_metadata: bool = True
    skip_arrow_metadata: bool | None = None
    binary_as_string: bool | None = None
    schema_force_view_types: bool | None = None
    listing_table_factory_infer_partitions: bool | None = None
    listing_table_ignore_subdirectory: bool | None = None
    file_extension: str | None = None
    cache: bool = False
    collect_statistics: bool | None = None
    meta_fetch_concurrency: int | None = None
    list_files_cache_ttl: str | None = None
    list_files_cache_limit: str | None = None
    projection_exprs: tuple[str, ...] = ()
    parquet_column_options: ParquetColumnOptionsPayload | None = None
    listing_mutable: bool = False
    unbounded: bool = False
    table_schema_contract: TableSchemaContractPayload | None = None
    expr_adapter_factory_name: str | None = None


class DeltaScanPayload(StructBase, frozen=True):
    """Serializable Delta scan payload."""

    file_column_name: str | None = None
    enable_parquet_pushdown: bool = True
    schema_force_view_types: bool | None = None
    wrap_partition_values: bool = False
    schema_msgpack: bytes | None = None


class DeltaWritePolicyPayload(StructBase, frozen=True):
    """Serializable Delta write policy payload."""

    target_file_size: int | None = None
    stats_columns: tuple[str, ...] | None = None


class DeltaSchemaPolicyPayload(StructBase, frozen=True):
    """Serializable Delta schema policy payload."""

    schema_mode: Literal["merge", "overwrite"] | None = None
    column_mapping_mode: Literal["id", "name"] | None = None


class ContractSpecPayload(StructBase, frozen=True):
    """Serializable contract spec payload."""

    name: str
    table_schema: TableSchemaPayload
    dedupe: DedupeSpecPayload | None = None
    canonical_sort: tuple[SortKeyPayload, ...] = ()
    constraints: tuple[str, ...] = ()
    version: int | None = None
    virtual_fields: tuple[str, ...] = ()
    virtual_field_docs: Mapping[str, str] | None = None
    validation: ArrowValidationPayload | None = None
    view_specs: tuple[ViewSpecPayload, ...] = ()


class DatasetSpecPayload(StructBase, frozen=True):
    """Serializable dataset spec payload."""

    table_schema: TableSchemaPayload
    contract_spec: ContractSpecPayload | None = None
    query_spec: IbisQueryPayload | None = None
    view_specs: tuple[ViewSpecPayload, ...] = ()
    datafusion_scan: DataFusionScanPayload | None = None
    delta_scan: DeltaScanPayload | None = None
    delta_write_policy: DeltaWritePolicyPayload | None = None
    delta_schema_policy: DeltaSchemaPolicyPayload | None = None
    delta_constraints: tuple[str, ...] = ()
    derived_fields: tuple[DerivedFieldPayload, ...] = ()
    predicate: SqlExprPayload | None = None
    pushdown_predicate: SqlExprPayload | None = None
    evolution_spec: SchemaEvolutionPayload | None = None
    metadata_spec: SchemaMetadataPayload | None = None
    validation: ArrowValidationPayload | None = None


def _expr_payload(expr: SqlExprSpec) -> SqlExprPayload:
    return SqlExprPayload(sql=expr.sql, policy_name=expr.policy_name, dialect=expr.dialect)


def _expr_from_payload(payload: SqlExprPayload) -> SqlExprSpec:
    return SqlExprSpec(
        sql=payload.sql,
        policy_name=payload.policy_name,
        dialect=payload.dialect,
    )


def _derived_payload(spec: DerivedFieldSpec) -> DerivedFieldPayload:
    return DerivedFieldPayload(name=spec.name, expr=_expr_payload(spec.expr))


def _derived_from_payload(payload: DerivedFieldPayload) -> DerivedFieldSpec:
    return DerivedFieldSpec(name=payload.name, expr=_expr_from_payload(payload.expr))


def _table_schema_payload(spec: TableSchemaSpec) -> TableSchemaPayload:
    schema = spec.to_arrow_schema()
    return TableSchemaPayload(name=spec.name, schema_msgpack=schema_to_msgpack(schema))


def _table_schema_from_payload(payload: TableSchemaPayload) -> TableSchemaSpec:
    schema = schema_from_msgpack(payload.schema_msgpack)
    return TableSchemaSpec.from_schema(payload.name, schema)


def _sort_key_payload(spec: SortKeySpec) -> SortKeyPayload:
    return SortKeyPayload(column=spec.column, order=spec.order)


def _sort_key_from_payload(payload: SortKeyPayload) -> SortKeySpec:
    return SortKeySpec(column=payload.column, order=payload.order)


def _dedupe_payload(spec: DedupeSpecSpec) -> DedupeSpecPayload:
    return DedupeSpecPayload(
        keys=tuple(spec.keys),
        tie_breakers=tuple(_sort_key_payload(item) for item in spec.tie_breakers),
        strategy=spec.strategy,
    )


def _dedupe_from_payload(payload: DedupeSpecPayload) -> DedupeSpecSpec:
    return DedupeSpecSpec(
        keys=payload.keys,
        tie_breakers=tuple(_sort_key_from_payload(item) for item in payload.tie_breakers),
        strategy=payload.strategy,
    )


def _validation_payload(options: ArrowValidationOptions) -> ArrowValidationPayload:
    return ArrowValidationPayload(
        strict=options.strict,
        coerce=options.coerce,
        max_errors=options.max_errors,
        emit_invalid_rows=options.emit_invalid_rows,
        emit_error_table=options.emit_error_table,
    )


def _validation_from_payload(payload: ArrowValidationPayload) -> ArrowValidationOptions:
    return ArrowValidationOptions(
        strict=payload.strict,
        coerce=payload.coerce,
        max_errors=payload.max_errors,
        emit_invalid_rows=payload.emit_invalid_rows,
        emit_error_table=payload.emit_error_table,
    )


def _view_payload(spec: ViewSpec) -> ViewSpecPayload:
    schema_msgpack = schema_to_msgpack(spec.schema)
    builder_name = getattr(spec.builder, "__name__", None) if spec.builder is not None else None
    return ViewSpecPayload(
        name=spec.name,
        sql=spec.sql,
        schema_msgpack=schema_msgpack,
        builder_name=builder_name,
    )


def _view_from_payload(payload: ViewSpecPayload) -> ViewSpec:
    schema = schema_from_msgpack(payload.schema_msgpack)
    return ViewSpec(name=payload.name, sql=payload.sql, schema=schema, builder=None)


def _projection_payload(spec: IbisProjectionSpec) -> IbisProjectionPayload:
    derived = tuple(
        DerivedFieldPayload(name=name, expr=_expr_payload(expr))
        for name, expr in spec.derived.items()
    )
    return IbisProjectionPayload(base=spec.base, derived=derived)


def _projection_from_payload(payload: IbisProjectionPayload) -> IbisProjectionSpec:
    derived = {item.name: _expr_from_payload(item.expr) for item in payload.derived}
    return IbisProjectionSpec(base=payload.base, derived=derived)


def _query_payload(spec: IbisQuerySpec) -> IbisQueryPayload:
    if spec.macros:
        msg = "IbisQuerySpec macros are not serializable."
        raise ValueError(msg)
    return IbisQueryPayload(
        projection=_projection_payload(spec.projection),
        predicate=_expr_payload(spec.predicate) if spec.predicate is not None else None,
        pushdown_predicate=(
            _expr_payload(spec.pushdown_predicate) if spec.pushdown_predicate is not None else None
        ),
    )


def _query_from_payload(payload: IbisQueryPayload) -> IbisQuerySpec:
    return IbisQuerySpec(
        projection=_projection_from_payload(payload.projection),
        predicate=_expr_from_payload(payload.predicate) if payload.predicate is not None else None,
        pushdown_predicate=(
            _expr_from_payload(payload.pushdown_predicate)
            if payload.pushdown_predicate is not None
            else None
        ),
    )


def _schema_evolution_payload(spec: SchemaEvolutionSpec) -> SchemaEvolutionPayload:
    return SchemaEvolutionPayload(
        promote_options=spec.promote_options,
        rename_map=dict(spec.rename_map),
        allow_missing=spec.allow_missing,
        allow_extra=spec.allow_extra,
        allow_casts=spec.allow_casts,
    )


def _schema_evolution_from_payload(payload: SchemaEvolutionPayload) -> SchemaEvolutionSpec:
    return SchemaEvolutionSpec(
        promote_options=payload.promote_options,
        rename_map=dict(payload.rename_map),
        allow_missing=payload.allow_missing,
        allow_extra=payload.allow_extra,
        allow_casts=payload.allow_casts,
    )


def _encode_bytes_map(metadata: Mapping[bytes, bytes]) -> dict[str, str]:
    return {key.hex(): value.hex() for key, value in metadata.items()}


def _decode_bytes_map(metadata: Mapping[str, str]) -> dict[bytes, bytes]:
    return {bytes.fromhex(key): bytes.fromhex(value) for key, value in metadata.items()}


def _metadata_payload(spec: SchemaMetadataSpec) -> SchemaMetadataPayload:
    schema_meta = _encode_bytes_map(spec.schema_metadata)
    field_meta: dict[str, dict[str, str]] = {
        name: _encode_bytes_map(meta) for name, meta in spec.field_metadata.items()
    }
    return SchemaMetadataPayload(schema_metadata=schema_meta, field_metadata=field_meta)


def _metadata_from_payload(payload: SchemaMetadataPayload) -> SchemaMetadataSpec:
    schema_meta = _decode_bytes_map(payload.schema_metadata)
    field_meta = {name: _decode_bytes_map(meta) for name, meta in payload.field_metadata.items()}
    return SchemaMetadataSpec(schema_metadata=schema_meta, field_metadata=field_meta)


def _parquet_column_payload(
    options: DataFusionScanOptions | None,
) -> ParquetColumnOptionsPayload | None:
    if options is None or options.parquet_column_options is None:
        return None
    spec = options.parquet_column_options
    return ParquetColumnOptionsPayload(
        statistics_enabled=spec.statistics_enabled,
        bloom_filter_enabled=spec.bloom_filter_enabled,
        dictionary_enabled=spec.dictionary_enabled,
    )


def _parquet_column_from_payload(
    payload: ParquetColumnOptionsPayload | None,
) -> ParquetColumnOptions | None:
    if payload is None:
        return None
    return ParquetColumnOptions(
        statistics_enabled=payload.statistics_enabled,
        bloom_filter_enabled=payload.bloom_filter_enabled,
        dictionary_enabled=payload.dictionary_enabled,
    )


def _table_schema_contract_payload(contract: TableSchemaContract) -> TableSchemaContractPayload:
    partition_schema_msgpack = None
    if contract.partition_cols:
        partition_schema = pa.schema(
            [pa.field(name, dtype, nullable=False) for name, dtype in contract.partition_cols]
        )
        partition_schema_msgpack = schema_to_msgpack(partition_schema)
    return TableSchemaContractPayload(
        file_schema_msgpack=schema_to_msgpack(contract.file_schema),
        partition_schema_msgpack=partition_schema_msgpack,
    )


def _table_schema_contract_from_payload(payload: TableSchemaContractPayload) -> TableSchemaContract:
    file_schema = schema_from_msgpack(payload.file_schema_msgpack)
    partition_cols: tuple[tuple[str, pa.DataType], ...] = ()
    if payload.partition_schema_msgpack is not None:
        partition_schema = schema_from_msgpack(payload.partition_schema_msgpack)
        partition_cols = tuple((field.name, field.type) for field in partition_schema)
    return TableSchemaContract(file_schema=file_schema, partition_cols=partition_cols)


def _datafusion_scan_payload(options: DataFusionScanOptions) -> DataFusionScanPayload:
    partition_schema_msgpack = None
    if options.partition_cols:
        schema = pa.schema(
            [pa.field(name, dtype, nullable=False) for name, dtype in options.partition_cols]
        )
        partition_schema_msgpack = schema_to_msgpack(schema)
    expr_adapter_factory_name = None
    if options.expr_adapter_factory is not None:
        expr_adapter_factory_name = getattr(options.expr_adapter_factory, "__name__", None)
        if expr_adapter_factory_name is None:
            expr_adapter_factory_name = repr(options.expr_adapter_factory)
    return DataFusionScanPayload(
        partition_schema_msgpack=partition_schema_msgpack,
        file_sort_order=options.file_sort_order,
        parquet_pruning=options.parquet_pruning,
        skip_metadata=options.skip_metadata,
        skip_arrow_metadata=options.skip_arrow_metadata,
        binary_as_string=options.binary_as_string,
        schema_force_view_types=options.schema_force_view_types,
        listing_table_factory_infer_partitions=options.listing_table_factory_infer_partitions,
        listing_table_ignore_subdirectory=options.listing_table_ignore_subdirectory,
        file_extension=options.file_extension,
        cache=options.cache,
        collect_statistics=options.collect_statistics,
        meta_fetch_concurrency=options.meta_fetch_concurrency,
        list_files_cache_ttl=options.list_files_cache_ttl,
        list_files_cache_limit=options.list_files_cache_limit,
        projection_exprs=options.projection_exprs,
        parquet_column_options=_parquet_column_payload(options),
        listing_mutable=options.listing_mutable,
        unbounded=options.unbounded,
        table_schema_contract=(
            _table_schema_contract_payload(options.table_schema_contract)
            if options.table_schema_contract is not None
            else None
        ),
        expr_adapter_factory_name=expr_adapter_factory_name,
    )


def _datafusion_scan_from_payload(payload: DataFusionScanPayload) -> DataFusionScanOptions:
    partition_cols: tuple[tuple[str, pa.DataType], ...] = ()
    if payload.partition_schema_msgpack is not None:
        schema = schema_from_msgpack(payload.partition_schema_msgpack)
        partition_cols = tuple((field.name, field.type) for field in schema)
    parquet_column_options = _parquet_column_from_payload(payload.parquet_column_options)
    table_schema_contract = (
        _table_schema_contract_from_payload(payload.table_schema_contract)
        if payload.table_schema_contract is not None
        else None
    )
    return DataFusionScanOptions(
        partition_cols=partition_cols,
        file_sort_order=payload.file_sort_order,
        parquet_pruning=payload.parquet_pruning,
        skip_metadata=payload.skip_metadata,
        skip_arrow_metadata=payload.skip_arrow_metadata,
        binary_as_string=payload.binary_as_string,
        schema_force_view_types=payload.schema_force_view_types,
        listing_table_factory_infer_partitions=payload.listing_table_factory_infer_partitions,
        listing_table_ignore_subdirectory=payload.listing_table_ignore_subdirectory,
        file_extension=payload.file_extension,
        cache=payload.cache,
        collect_statistics=payload.collect_statistics,
        meta_fetch_concurrency=payload.meta_fetch_concurrency,
        list_files_cache_ttl=payload.list_files_cache_ttl,
        list_files_cache_limit=payload.list_files_cache_limit,
        projection_exprs=payload.projection_exprs,
        parquet_column_options=parquet_column_options,
        listing_mutable=payload.listing_mutable,
        unbounded=payload.unbounded,
        table_schema_contract=table_schema_contract,
        expr_adapter_factory=None,
    )


def _delta_scan_payload(options: DeltaScanOptions) -> DeltaScanPayload:
    schema_msgpack = schema_to_msgpack(options.schema) if options.schema is not None else None
    return DeltaScanPayload(
        file_column_name=options.file_column_name,
        enable_parquet_pushdown=options.enable_parquet_pushdown,
        schema_force_view_types=options.schema_force_view_types,
        wrap_partition_values=options.wrap_partition_values,
        schema_msgpack=schema_msgpack,
    )


def _delta_scan_from_payload(payload: DeltaScanPayload) -> DeltaScanOptions:
    schema = schema_from_msgpack(payload.schema_msgpack) if payload.schema_msgpack else None
    return DeltaScanOptions(
        file_column_name=payload.file_column_name,
        enable_parquet_pushdown=payload.enable_parquet_pushdown,
        schema_force_view_types=payload.schema_force_view_types,
        wrap_partition_values=payload.wrap_partition_values,
        schema=schema,
    )


def _delta_write_policy_payload(policy: DeltaWritePolicy) -> DeltaWritePolicyPayload:
    return DeltaWritePolicyPayload(
        target_file_size=policy.target_file_size,
        stats_columns=policy.stats_columns,
    )


def _delta_write_policy_from_payload(payload: DeltaWritePolicyPayload) -> DeltaWritePolicy:
    return DeltaWritePolicy(
        target_file_size=payload.target_file_size,
        stats_columns=payload.stats_columns,
    )


def _delta_schema_policy_payload(policy: DeltaSchemaPolicy) -> DeltaSchemaPolicyPayload:
    return DeltaSchemaPolicyPayload(
        schema_mode=policy.schema_mode,
        column_mapping_mode=policy.column_mapping_mode,
    )


def _delta_schema_policy_from_payload(payload: DeltaSchemaPolicyPayload) -> DeltaSchemaPolicy:
    return DeltaSchemaPolicy(
        schema_mode=payload.schema_mode,
        column_mapping_mode=payload.column_mapping_mode,
    )


def _contract_payload(spec: ContractSpec) -> ContractSpecPayload:
    return ContractSpecPayload(
        name=spec.name,
        table_schema=_table_schema_payload(spec.table_schema),
        dedupe=_dedupe_payload(spec.dedupe) if spec.dedupe is not None else None,
        canonical_sort=tuple(_sort_key_payload(item) for item in spec.canonical_sort),
        constraints=spec.constraints,
        version=spec.version,
        virtual_fields=spec.virtual_fields,
        virtual_field_docs=spec.virtual_field_docs,
        validation=_validation_payload(spec.validation) if spec.validation is not None else None,
        view_specs=tuple(_view_payload(item) for item in spec.view_specs),
    )


def _contract_from_payload(payload: ContractSpecPayload) -> ContractSpec:
    return ContractSpec(
        name=payload.name,
        table_schema=_table_schema_from_payload(payload.table_schema),
        dedupe=_dedupe_from_payload(payload.dedupe) if payload.dedupe is not None else None,
        canonical_sort=tuple(_sort_key_from_payload(item) for item in payload.canonical_sort),
        constraints=payload.constraints,
        version=payload.version,
        virtual_fields=payload.virtual_fields,
        virtual_field_docs=(
            dict(payload.virtual_field_docs) if payload.virtual_field_docs is not None else None
        ),
        validation=(
            _validation_from_payload(payload.validation) if payload.validation is not None else None
        ),
        view_specs=tuple(_view_from_payload(item) for item in payload.view_specs),
    )


def dataset_spec_payload(spec: DatasetSpec) -> DatasetSpecPayload:
    """Return a serializable payload for a DatasetSpec.

    Returns
    -------
    DatasetSpecPayload
        Serialized DatasetSpec payload.
    """
    return DatasetSpecPayload(
        table_schema=_table_schema_payload(spec.table_spec),
        contract_spec=_contract_payload(spec.contract_spec) if spec.contract_spec else None,
        query_spec=_query_payload(spec.query_spec) if spec.query_spec is not None else None,
        view_specs=tuple(_view_payload(item) for item in spec.view_specs),
        datafusion_scan=(
            _datafusion_scan_payload(spec.datafusion_scan)
            if spec.datafusion_scan is not None
            else None
        ),
        delta_scan=_delta_scan_payload(spec.delta_scan) if spec.delta_scan is not None else None,
        delta_write_policy=(
            _delta_write_policy_payload(spec.delta_write_policy)
            if spec.delta_write_policy is not None
            else None
        ),
        delta_schema_policy=(
            _delta_schema_policy_payload(spec.delta_schema_policy)
            if spec.delta_schema_policy is not None
            else None
        ),
        delta_constraints=spec.delta_constraints,
        derived_fields=tuple(_derived_payload(item) for item in spec.derived_fields),
        predicate=_expr_payload(spec.predicate) if spec.predicate is not None else None,
        pushdown_predicate=(
            _expr_payload(spec.pushdown_predicate) if spec.pushdown_predicate is not None else None
        ),
        evolution_spec=_schema_evolution_payload(spec.evolution_spec),
        metadata_spec=_metadata_payload(spec.metadata_spec),
        validation=_validation_payload(spec.validation) if spec.validation is not None else None,
    )


def dataset_spec_from_payload(payload: DatasetSpecPayload) -> DatasetSpec:
    """Build a DatasetSpec from a serialized payload.

    Returns
    -------
    DatasetSpec
        Reconstructed DatasetSpec instance.
    """
    table_spec = _table_schema_from_payload(payload.table_schema)
    return DatasetSpec(
        table_spec=table_spec,
        contract_spec=(
            _contract_from_payload(payload.contract_spec)
            if payload.contract_spec is not None
            else None
        ),
        query_spec=(
            _query_from_payload(payload.query_spec) if payload.query_spec is not None else None
        ),
        view_specs=tuple(_view_from_payload(item) for item in payload.view_specs),
        datafusion_scan=(
            _datafusion_scan_from_payload(payload.datafusion_scan)
            if payload.datafusion_scan is not None
            else None
        ),
        delta_scan=(
            _delta_scan_from_payload(payload.delta_scan) if payload.delta_scan is not None else None
        ),
        delta_write_policy=(
            _delta_write_policy_from_payload(payload.delta_write_policy)
            if payload.delta_write_policy is not None
            else None
        ),
        delta_schema_policy=(
            _delta_schema_policy_from_payload(payload.delta_schema_policy)
            if payload.delta_schema_policy is not None
            else None
        ),
        delta_constraints=payload.delta_constraints,
        derived_fields=tuple(_derived_from_payload(item) for item in payload.derived_fields),
        predicate=_expr_from_payload(payload.predicate) if payload.predicate is not None else None,
        pushdown_predicate=(
            _expr_from_payload(payload.pushdown_predicate)
            if payload.pushdown_predicate is not None
            else None
        ),
        evolution_spec=(
            _schema_evolution_from_payload(payload.evolution_spec)
            if payload.evolution_spec is not None
            else SchemaEvolutionSpec()
        ),
        metadata_spec=(
            _metadata_from_payload(payload.metadata_spec)
            if payload.metadata_spec is not None
            else SchemaMetadataSpec()
        ),
        validation=(
            _validation_from_payload(payload.validation) if payload.validation is not None else None
        ),
    )


def dataset_spec_to_msgpack(spec: DatasetSpec) -> bytes:
    """Serialize a DatasetSpec to MessagePack bytes.

    Returns
    -------
    bytes
        MessagePack payload for the dataset spec.
    """
    return dumps_msgpack(dataset_spec_payload(spec))


def dataset_spec_from_msgpack(payload: bytes) -> DatasetSpec:
    """Deserialize a DatasetSpec from MessagePack bytes.

    Returns
    -------
    DatasetSpec
        Reconstructed DatasetSpec instance.
    """
    decoded = loads_msgpack(payload, target_type=DatasetSpecPayload, strict=False)
    return dataset_spec_from_payload(decoded)


def _json_dec_hook(type_hint: Any, obj: object) -> object:
    if type_hint is bytes and isinstance(obj, str):
        try:
            return bytes.fromhex(obj)
        except ValueError:
            return obj
    return obj


def _loads_json_payload[T](buf: bytes | str, *, target_type: type[T]) -> T:
    decoder = msgspec.json.Decoder(type=target_type, dec_hook=_json_dec_hook, strict=False)
    return decoder.decode(buf)


def dataset_spec_to_json(spec: DatasetSpec, *, pretty: bool = False) -> bytes:
    """Serialize a DatasetSpec to JSON bytes.

    Returns
    -------
    bytes
        JSON payload for the dataset spec.
    """
    return dumps_json(dataset_spec_payload(spec), pretty=pretty)


def dataset_spec_from_json(payload: bytes | str) -> DatasetSpec:
    """Deserialize a DatasetSpec from JSON bytes.

    Returns
    -------
    DatasetSpec
        Reconstructed DatasetSpec instance.
    """
    decoded = _loads_json_payload(payload, target_type=DatasetSpecPayload)
    return dataset_spec_from_payload(decoded)


__all__ = [
    "ContractSpecPayload",
    "DatasetSpecPayload",
    "DerivedFieldPayload",
    "IbisProjectionPayload",
    "IbisQueryPayload",
    "SqlExprPayload",
    "TableSchemaPayload",
    "dataset_spec_from_json",
    "dataset_spec_from_msgpack",
    "dataset_spec_payload",
    "dataset_spec_to_json",
    "dataset_spec_to_msgpack",
]
