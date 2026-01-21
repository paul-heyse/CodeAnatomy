"""Dataset registry bridge for DataFusion SessionContext."""

from __future__ import annotations

import importlib
import inspect
from collections.abc import Callable, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as pafs
from datafusion import SessionContext
from datafusion.catalog import Catalog, Schema
from datafusion.dataframe import DataFrame
from deltalake import DeltaTable

from arrowdsl.core.interop import SchemaLike
from arrowdsl.schema.metadata import ordering_from_schema
from arrowdsl.schema.serialization import schema_fingerprint, schema_to_dict
from core_types import ensure_path
from datafusion_engine.listing_table_provider import (
    TableProviderCapsule,
    parquet_listing_table_provider,
)
from datafusion_engine.schema_registry import SCHEMA_REGISTRY, is_nested_dataset
from ibis_engine.registry import (
    DatasetLocation,
    IbisDatasetRegistry,
    resolve_datafusion_scan_options,
    resolve_dataset_schema,
    resolve_delta_scan_options,
)
from schema_spec.specs import ExternalTableConfig, TableSchemaSpec
from schema_spec.system import (
    DataFusionScanOptions,
    DeltaScanOptions,
    ddl_fingerprint_from_schema,
)
from sqlglot_tools.optimizer import (
    SqlGlotSurface,
    register_datafusion_dialect,
    sqlglot_surface_policy,
)
from storage.deltalake import DeltaCdfOptions, read_delta_cdf

if TYPE_CHECKING:
    from datafusion_engine.runtime import DataFusionRuntimeProfile

DEFAULT_CACHE_MAX_COLUMNS = 64
_REGISTERED_OBJECT_STORES: dict[int, set[str]] = {}
_CACHED_DATASETS: dict[int, set[str]] = {}
_REGISTERED_CATALOGS: dict[int, set[str]] = {}
_REGISTERED_SCHEMAS: dict[int, set[tuple[str, str]]] = {}
_INPUT_PLUGIN_PREFIXES = ("artifact://", "dataset://", "repo://")
_CST_EXTERNAL_TABLE_NAME = "libcst_files_v1"
_CST_PARTITION_FIELDS: tuple[str, ...] = ("repo",)
_CST_FILE_SORT_ORDER: tuple[str, ...] = ("path", "file_id")

try:
    from datafusion.input.base import BaseInputSource as _BaseInputSource
except ImportError:  # pragma: no cover - optional dependency
    _BaseInputSource = None
    _INPUT_PLUGIN_AVAILABLE = False
else:
    _INPUT_PLUGIN_AVAILABLE = True


class DatasetInputSource:
    """Resolve dataset handles into registered tables."""

    def __init__(
        self,
        ctx: SessionContext,
        *,
        registry: IbisDatasetRegistry,
        runtime_profile: DataFusionRuntimeProfile | None,
    ) -> None:
        self._ctx = ctx
        self._registry = registry
        self._runtime_profile = runtime_profile

    def is_correct_input(
        self,
        input_item: object,
        table_name: str,
        **kwargs: object,
    ) -> bool:
        """Return True when the input matches a dataset registry handle.

        Returns
        -------
        bool
            ``True`` when the dataset handle is recognized.
        """
        _ = table_name, kwargs
        name = _dataset_name_from_input(input_item)
        return name is not None and self._registry.catalog.has(name)

    def build_table(
        self,
        input_item: object,
        table_name: str,
        **kwargs: object,
    ) -> DataFrame:
        """Build a DataFusion DataFrame for a dataset registry handle.

        Returns
        -------
        datafusion.dataframe.DataFrame
            DataFrame registered for the dataset handle.

        Raises
        ------
        ValueError
            Raised when the dataset handle is not recognized.
        """
        _ = kwargs
        name = _dataset_name_from_input(input_item)
        if name is None:
            msg = f"Unsupported dataset handle: {input_item!r}."
            raise ValueError(msg)
        location = self._registry.catalog.get(name)
        return register_dataset_df(
            self._ctx,
            name=table_name,
            location=location,
            runtime_profile=self._runtime_profile,
        )


def _dataset_name_from_input(value: object) -> str | None:
    handle = str(value)
    for prefix in _INPUT_PLUGIN_PREFIXES:
        if handle.startswith(prefix):
            name = handle.removeprefix(prefix)
            return name if name else None
    return None


def dataset_input_plugin(
    registry: IbisDatasetRegistry,
    *,
    runtime_profile: DataFusionRuntimeProfile | None = None,
) -> Callable[[SessionContext], None]:
    """Return a SessionContext installer for dataset input sources.

    Returns
    -------
    Callable[[SessionContext], None]
        Installer that registers the dataset input source.
    """

    def _install(ctx: SessionContext) -> None:
        if not _INPUT_PLUGIN_AVAILABLE:
            return
        register = getattr(ctx, "register_input_source", None)
        if not callable(register):
            return
        register(
            DatasetInputSource(
                ctx,
                registry=registry,
                runtime_profile=runtime_profile,
            )
        )

    return _install


def input_plugin_prefixes() -> tuple[str, ...]:
    """Return the registered input plugin prefixes.

    Returns
    -------
    tuple[str, ...]
        Supported input plugin prefixes.
    """
    return _INPUT_PLUGIN_PREFIXES


@dataclass(frozen=True)
class DataFusionRegistryOptions:
    """Resolved DataFusion registration options for a dataset."""

    scan: DataFusionScanOptions | None
    delta_scan: DeltaScanOptions | None
    schema: SchemaLike | None
    read_options: Mapping[str, object]
    cache: bool
    provider: Literal["dataset", "listing", "parquet"] | None


@dataclass(frozen=True)
class DeltaCdfRegistrationOptions:
    """Options for registering Delta CDF tables."""

    cdf_options: DeltaCdfOptions | None = None
    storage_options: Mapping[str, str] | None = None
    runtime_profile: DataFusionRuntimeProfile | None = None


@dataclass(frozen=True)
class DataFusionCachePolicy:
    """Cache policy overrides for DataFusion dataset registration."""

    enabled: bool | None = None
    max_columns: int | None = None


@dataclass(frozen=True)
class DataFusionCacheSettings:
    """Resolved cache settings for DataFusion registration."""

    enabled: bool
    max_columns: int | None


@dataclass(frozen=True)
class DataFusionRegistrationContext:
    """Inputs needed to register a dataset with DataFusion."""

    ctx: SessionContext
    name: str
    location: DatasetLocation
    options: DataFusionRegistryOptions
    cache: DataFusionCacheSettings
    external_table_sql: str | None = None
    runtime_profile: DataFusionRuntimeProfile | None = None


def _schema_field_type(dataset: str, field: str) -> pa.DataType | None:
    """Return the Arrow type for a schema field, when available."""
    schema = SCHEMA_REGISTRY.get(dataset)
    if schema is None:
        return None
    if field not in schema.names:
        return None
    return schema.field(field).type


def _default_scan_options_for_dataset(
    name: str,
    location: DatasetLocation,
) -> DataFusionScanOptions | None:
    """Return default DataFusion scan options for supported datasets."""
    if name != _CST_EXTERNAL_TABLE_NAME:
        return None
    schema = SCHEMA_REGISTRY.get(name)
    if schema is None:
        return None
    partition_cols: list[tuple[str, pa.DataType]] = []
    for field_name in _CST_PARTITION_FIELDS:
        dtype = _schema_field_type(name, field_name)
        if dtype is not None:
            partition_cols.append((field_name, dtype))
    file_sort_order = tuple(
        field for field in _CST_FILE_SORT_ORDER if field in schema.names
    )
    return DataFusionScanOptions(
        partition_cols=tuple(partition_cols),
        file_sort_order=file_sort_order,
        file_extension=".parquet",
        parquet_pruning=True,
        skip_metadata=True,
        collect_statistics=False,
        listing_table_factory_infer_partitions=True,
        listing_mutable=True,
        unbounded=location.format != "delta",
    )


def _default_delta_scan_options_for_dataset(name: str) -> DeltaScanOptions | None:
    if name != _CST_EXTERNAL_TABLE_NAME:
        return None
    schema = SCHEMA_REGISTRY.get(name)
    if schema is None:
        return None
    return DeltaScanOptions(schema=schema)


def _apply_scan_defaults(name: str, location: DatasetLocation) -> DatasetLocation:
    """Attach default scan options to a dataset location."""
    updated = location
    if location.datafusion_scan is not None:
        defaults = None
    else:
        defaults = _default_scan_options_for_dataset(name, location)
        if defaults is not None:
            updated = replace(updated, datafusion_scan=defaults)
    if updated.delta_scan is not None:
        return updated
    delta_defaults = _default_delta_scan_options_for_dataset(name)
    if delta_defaults is None:
        return updated
    return replace(updated, delta_scan=delta_defaults)


def resolve_registry_options(location: DatasetLocation) -> DataFusionRegistryOptions:
    """Resolve DataFusion registration hints for a dataset location.

    Returns
    -------
    DataFusionRegistryOptions
        Registration options derived from the dataset location.
    """
    scan = resolve_datafusion_scan_options(location)
    delta_scan = resolve_delta_scan_options(location)
    schema = resolve_dataset_schema(location)
    provider = location.datafusion_provider
    if provider is None and _prefers_listing_table(location, scan=scan):
        provider = "listing"
    return DataFusionRegistryOptions(
        scan=scan,
        delta_scan=delta_scan,
        schema=schema,
        read_options=dict(location.read_options),
        cache=bool(scan.cache) if scan is not None else False,
        provider=provider,
    )


def _prefers_listing_table(
    location: DatasetLocation,
    *,
    scan: DataFusionScanOptions | None,
) -> bool:
    if location.format != "parquet":
        return False
    if location.files:
        return False
    if scan is not None and (scan.partition_cols or scan.file_sort_order):
        return True
    return _path_is_directory(location.path)


def _path_is_directory(path: str | Path) -> bool:
    if isinstance(path, str) and "://" in path:
        parsed = urlparse(path)
        if parsed.path.endswith("/"):
            return True
        return not Path(parsed.path).suffix
    resolved = ensure_path(path)
    if resolved.exists():
        return resolved.is_dir()
    return not resolved.suffix


def datafusion_external_table_sql(
    *,
    name: str,
    location: DatasetLocation,
    dialect: str | None = None,
    runtime_profile: DataFusionRuntimeProfile | None = None,
    options_override: Mapping[str, object] | None = None,
) -> str | None:
    """Return a CREATE EXTERNAL TABLE statement for a dataset location.

    Returns
    -------
    str | None
        External table DDL when a schema is available, otherwise ``None``.
    """
    if location.format == "delta":
        return None
    table_spec = _resolve_table_spec(location)
    schema = SCHEMA_REGISTRY.get(name)
    if table_spec is None and schema is not None:
        table_spec = table_spec_from_schema(name, schema)
    if table_spec is None:
        return None
    scan = resolve_datafusion_scan_options(location)
    resolved_dialect = (
        dialect or sqlglot_surface_policy(SqlGlotSurface.DATAFUSION_EXTERNAL_TABLE).dialect
    )
    if resolved_dialect == "datafusion_ext":
        register_datafusion_dialect()
    merged_options = _merge_external_table_options(
        runtime_profile.external_table_options if runtime_profile is not None else None,
        location.read_options,
        options_override,
    )
    options, compression = _external_table_options(merged_options)
    partitioned_by = _partitioned_by(location)
    file_sort_order = _file_sort_order(location)
    unbounded = scan.unbounded if scan is not None else False
    config = ExternalTableConfig(
        location=str(location.path),
        file_format=location.format,
        table_name=name,
        dialect=resolved_dialect,
        options=options,
        partitioned_by=partitioned_by,
        file_sort_order=file_sort_order,
        compression=compression,
        unbounded=unbounded,
    )
    return table_spec.to_create_external_table_sql(config)


def _resolve_table_spec(location: DatasetLocation) -> TableSchemaSpec | None:
    if location.table_spec is not None:
        return location.table_spec
    if location.dataset_spec is not None:
        return location.dataset_spec.table_spec
    return None


def _partitioned_by(location: DatasetLocation) -> tuple[str, ...] | None:
    scan = resolve_datafusion_scan_options(location)
    if scan is None or not scan.partition_cols:
        return None
    return tuple(col for col, _ in scan.partition_cols)


def _file_sort_order(location: DatasetLocation) -> tuple[str, ...] | None:
    scan = resolve_datafusion_scan_options(location)
    if scan is not None and scan.file_sort_order:
        return tuple(scan.file_sort_order)
    spec = location.dataset_spec
    if spec is not None and spec.contract_spec is not None and spec.contract_spec.canonical_sort:
        return tuple(key.column for key in spec.contract_spec.canonical_sort)
    if spec is not None and spec.table_spec.key_fields:
        return tuple(spec.table_spec.key_fields)
    if location.table_spec is not None and location.table_spec.key_fields:
        return tuple(location.table_spec.key_fields)
    return None


def _merge_external_table_options(
    *options: Mapping[str, object] | None,
) -> dict[str, object]:
    merged: dict[str, object] = {}
    for mapping in options:
        if not mapping:
            continue
        merged.update({key: value for key, value in mapping.items() if value is not None})
    return merged


def _scan_external_table_options(
    scan: DataFusionScanOptions | None,
) -> Mapping[str, object] | None:
    if scan is None:
        return None
    options: dict[str, object] = {"skip_metadata": scan.skip_metadata}
    if scan.schema_force_view_types is not None:
        options["schema_force_view_types"] = scan.schema_force_view_types
    return options


def _external_table_options(
    read_options: Mapping[str, object],
) -> tuple[Mapping[str, object], str | None]:
    options = dict(read_options)
    compression = None
    for key in ("compression", "compression_type"):
        if key in options:
            compression = str(options.pop(key))
            break
    return options, compression


def _expected_schema_fingerprint(*, location: DatasetLocation) -> str | None:
    table_spec = location.table_spec
    if table_spec is None and location.dataset_spec is not None:
        table_spec = location.dataset_spec.table_spec
    if table_spec is None:
        return None
    return table_spec.ddl_fingerprint()


def _enforce_schema_handshake(
    *,
    ctx: SessionContext,
    name: str,
    location: DatasetLocation,
) -> None:
    if is_nested_dataset(name):
        return
    expected = _expected_schema_fingerprint(location=location)
    if expected is None:
        return
    actual_schema = ctx.table(name).schema()
    actual = ddl_fingerprint_from_schema(name, actual_schema)
    if actual == expected:
        return
    msg = (
        f"Dataset schema fingerprint mismatch for {name!r}: expected {expected}, observed {actual}."
    )
    raise ValueError(msg)


def register_dataset_df(
    ctx: SessionContext,
    *,
    name: str,
    location: DatasetLocation,
    cache_policy: DataFusionCachePolicy | None = None,
    runtime_profile: DataFusionRuntimeProfile | None = None,
) -> DataFrame:
    """Register a dataset location with DataFusion and return a DataFrame.

    Returns
    -------
    datafusion.dataframe.DataFrame
        DataFusion DataFrame for the registered dataset.

    Raises
    ------
    ValueError
        Raised when the dataset format is unsupported.
    """
    location = _apply_scan_defaults(name, location)
    _register_object_store(ctx, location)
    if runtime_profile is not None:
        _ensure_catalog_schema(
            ctx,
            catalog=runtime_profile.default_catalog,
            schema=runtime_profile.default_schema,
        )
    options = resolve_registry_options(location)
    cache = _resolve_cache_policy(
        options,
        cache_policy=cache_policy,
        runtime_profile=runtime_profile,
    )
    context = DataFusionRegistrationContext(
        ctx=ctx,
        name=name,
        location=location,
        options=options,
        cache=cache,
        external_table_sql=datafusion_external_table_sql(
            name=name,
            location=location,
            runtime_profile=runtime_profile,
            options_override=_scan_external_table_options(options.scan),
        ),
        runtime_profile=runtime_profile,
    )
    scan = options.scan
    if (scan is not None and scan.unbounded) or _should_register_external_table(context):
        df = _register_external_table(context)
    elif location.format == "delta":
        df = _register_delta(context)
    elif options.provider == "dataset":
        df = _register_dataset_provider(context)
    elif location.format == "parquet":
        df = _register_parquet(context)
    elif location.format == "csv":
        df = _register_simple(context, method="register_csv")
    elif location.format == "json":
        df = _register_simple(context, method="register_json")
    elif location.format == "avro":
        df = _register_simple(context, method="register_avro")
    else:
        msg = f"Unsupported DataFusion dataset format: {location.format!r}."
        raise ValueError(msg)
    _enforce_schema_handshake(ctx=ctx, name=name, location=location)
    return df


def _should_register_external_table(context: DataFusionRegistrationContext) -> bool:
    if context.external_table_sql is None:
        return False
    return context.name in SCHEMA_REGISTRY


def _register_parquet(context: DataFusionRegistrationContext) -> DataFrame:
    scan = context.options.scan
    file_extension = scan.file_extension if scan and scan.file_extension else ".parquet"
    partition_cols = scan.partition_cols if scan and scan.partition_cols else None
    table_partition_cols = (
        [(col, str(dtype)) for col, dtype in partition_cols] if partition_cols else None
    )
    kwargs: dict[str, Any] = {
        "schema": context.options.schema,
        "file_extension": file_extension,
        "table_partition_cols": table_partition_cols,
    }
    skip_metadata = None
    file_sort_order = None
    if scan is not None:
        file_sort_order = scan.file_sort_order or None
        kwargs["file_sort_order"] = file_sort_order
        kwargs["parquet_pruning"] = scan.parquet_pruning
        skip_metadata = _effective_skip_metadata(context.location, scan)
        kwargs["skip_metadata"] = skip_metadata
    kwargs = _merge_kwargs(kwargs, context.options.read_options)
    use_listing = context.options.provider == "listing"
    if use_listing:
        _apply_scan_settings(context.ctx, scan=scan)
        if skip_metadata is not None:
            _set_runtime_setting(
                context.ctx,
                key="datafusion.execution.parquet.skip_metadata",
                value=str(skip_metadata).lower(),
            )

        listing_provider = None
        if file_sort_order is None:
            listing_provider = parquet_listing_table_provider(
                path=str(context.location.path),
                schema=context.options.schema,
                file_extension=file_extension,
                table_partition_cols=partition_cols,
                parquet_pruning=scan.parquet_pruning if scan is not None else None,
                skip_metadata=skip_metadata,
                collect_statistics=scan.collect_statistics if scan is not None else None,
            )

        def _register_listing() -> None:
            if listing_provider is not None:
                context.ctx.register_table(context.name, listing_provider)
                return
            _call_register(
                context.ctx.register_listing_table,
                context.name,
                context.location.path,
                kwargs,
            )

        if scan is not None and scan.listing_mutable:
            _refresh_listing_table(
                context.ctx,
                name=context.name,
                register=_register_listing,
                runtime_profile=context.runtime_profile,
            )
        else:
            _register_listing()
        df = context.ctx.table(context.name)
        _record_listing_table_artifact(
            context,
            scan=scan,
            file_extension=file_extension,
            table_partition_cols=table_partition_cols,
            skip_metadata=skip_metadata,
        )
    else:
        _apply_scan_settings(context.ctx, scan=scan)
        if skip_metadata is not None:
            _set_runtime_setting(
                context.ctx,
                key="datafusion.execution.parquet.skip_metadata",
                value=str(skip_metadata).lower(),
            )
        _call_register(
            context.ctx.register_parquet,
            context.name,
            context.location.path,
            kwargs,
        )
        df = context.ctx.table(context.name)
    return _maybe_cache(context, df)


def _register_external_table(
    context: DataFusionRegistrationContext,
) -> DataFrame:
    scan = context.options.scan
    if context.external_table_sql is None:
        msg = "External table registration requires a schema-backed DDL statement."
        raise ValueError(msg)
    file_extension = scan.file_extension if scan and scan.file_extension else ".parquet"
    table_partition_cols = (
        [(col, str(dtype)) for col, dtype in scan.partition_cols]
        if scan and scan.partition_cols
        else None
    )
    skip_metadata = None
    if scan is not None:
        skip_metadata = _effective_skip_metadata(context.location, scan)
    _apply_scan_settings(context.ctx, scan=scan)
    try:
        context.ctx.sql(context.external_table_sql).collect()
    except (RuntimeError, TypeError, ValueError) as exc:
        msg = f"Failed to register unbounded external table: {exc}"
        raise ValueError(msg) from exc
    df = context.ctx.table(context.name)
    _record_listing_table_artifact(
        context,
        scan=scan,
        file_extension=file_extension,
        table_partition_cols=table_partition_cols,
        skip_metadata=skip_metadata,
    )
    return _maybe_cache(context, df)


def provider_capsule_id(provider: object) -> str:
    """Return a stable identifier for a DataFusion table provider capsule.

    Returns
    -------
    str
        Capsule identifier string.

    Raises
    ------
    AttributeError
        Raised when the provider does not expose a capsule.
    """
    capsule = _table_provider_capsule(provider)
    if capsule is None:
        msg = "Provider does not expose a DataFusion table provider capsule."
        raise AttributeError(msg)
    return repr(capsule)


def _table_provider_capsule(provider: object) -> object | None:
    attr = getattr(provider, "__datafusion_table_provider__", None)
    if callable(attr):
        return attr()
    attr = getattr(provider, "datafusion_table_provider", None)
    if callable(attr):
        return attr()
    return None


def _provider_capsule_id(
    provider: object | None,
    *,
    source: object | None,
) -> str | None:
    if source is not None:
        try:
            return provider_capsule_id(source)
        except (AttributeError, RuntimeError, TypeError, ValueError):
            return None
    if provider is None:
        return None
    return repr(provider)


def _provider_pushdown_value(provider: object, *, names: Sequence[str]) -> str | bool | None:
    for name in names:
        attr = getattr(provider, name, None)
        if isinstance(attr, bool):
            return attr
        if callable(attr):
            try:
                value = attr()
            except TypeError:
                continue
            if isinstance(value, bool):
                return value
            if value is None:
                return None
            return str(value)
    return None


def _provider_pushdown_hints(provider: object) -> dict[str, object]:
    return {
        "projection_pushdown": _provider_pushdown_value(
            provider,
            names=("supports_projection_pushdown",),
        ),
        "predicate_pushdown": _provider_pushdown_value(
            provider,
            names=(
                "supports_filter_pushdown",
                "supports_filters_pushdown",
                "supports_predicate_pushdown",
            ),
        ),
        "limit_pushdown": _provider_pushdown_value(
            provider,
            names=("supports_limit_pushdown",),
        ),
    }


def _record_table_provider_artifact(
    runtime_profile: DataFusionRuntimeProfile | None,
    *,
    name: str,
    provider: object | None,
    provider_kind: str,
    source: object | None = None,
) -> None:
    if runtime_profile is None or runtime_profile.diagnostics_sink is None:
        return
    capsule_id = _provider_capsule_id(provider, source=source)
    payload: dict[str, object] = {
        "name": name,
        "provider": provider_kind,
        "provider_type": type(provider).__name__ if provider is not None else None,
        "capsule_id": capsule_id,
    }
    if provider is not None:
        payload.update(_provider_pushdown_hints(provider))
    runtime_profile.diagnostics_sink.record_artifact("datafusion_table_providers_v1", payload)


def _register_delta(context: DataFusionRegistrationContext) -> DataFrame:
    table = DeltaTable(
        context.location.path,
        storage_options=(
            dict(context.location.storage_options) if context.location.storage_options else None
        ),
        version=context.location.delta_version,
    )
    if context.location.delta_timestamp is not None:
        table.load_as_version(context.location.delta_timestamp)
    delta_features = _delta_feature_payload(table)
    delta_protocol = _delta_protocol_payload(table)
    delta_schema = table.schema().to_arrow()
    provider = "table"
    delta_provider = None
    if not context.location.files:
        delta_provider = _delta_rust_table_provider(context, delta_scan=context.options.delta_scan)
        if delta_provider is None:
            delta_provider = _delta_table_provider(table, delta_scan=context.options.delta_scan)
    if delta_provider is not None:
        context.ctx.register_table(context.name, delta_provider)
        provider = "table_provider"
        _record_table_provider_artifact(
            context.runtime_profile,
            name=context.name,
            provider=delta_provider,
            provider_kind=provider,
            source=table,
        )
    elif context.location.files:
        dataset = _delta_dataset_from_files(context)
        context.ctx.register_table(context.name, dataset)
        provider = "dataset_files"
        _record_table_provider_artifact(
            context.runtime_profile,
            name=context.name,
            provider=dataset,
            provider_kind=provider,
            source=table,
        )
    elif context.options.provider == "dataset":
        dataset = _delta_dataset_from_table(context, table)
        context.ctx.register_table(context.name, dataset)
        provider = "dataset_forced"
        _record_table_provider_artifact(
            context.runtime_profile,
            name=context.name,
            provider=dataset,
            provider_kind=provider,
            source=table,
        )
    else:
        try:
            context.ctx.register_table(context.name, table)
        except TypeError:
            dataset = _delta_dataset_from_table(context, table)
            context.ctx.register_table(context.name, dataset)
            provider = "dataset"
            _record_table_provider_artifact(
                context.runtime_profile,
                name=context.name,
                provider=dataset,
                provider_kind=provider,
                source=table,
            )
        else:
            _record_table_provider_artifact(
                context.runtime_profile,
                name=context.name,
                provider=table,
                provider_kind=provider,
                source=table,
            )
    df = context.ctx.table(context.name)
    provider_schema = df.schema()
    expected_schema = SCHEMA_REGISTRY.get(context.name)
    if context.location.delta_version is not None:
        delta_version = context.location.delta_version
    else:
        delta_version = table.version()
    _record_delta_table_artifact(
        context,
        provider=provider,
        delta_version=delta_version,
        delta_features=delta_features,
        delta_protocol=delta_protocol,
        expected_schema=expected_schema,
        delta_schema=delta_schema,
        provider_schema=provider_schema,
    )
    return _maybe_cache(context, df)


def _record_listing_table_artifact(
    context: DataFusionRegistrationContext,
    *,
    scan: DataFusionScanOptions | None,
    file_extension: str,
    table_partition_cols: Sequence[tuple[str, str]] | None,
    skip_metadata: bool | None,
) -> None:
    profile = context.runtime_profile
    if profile is None or profile.diagnostics_sink is None:
        return
    schema_payload = (
        schema_to_dict(context.options.schema) if context.options.schema is not None else None
    )
    ordering_keys: list[list[str]] | None = None
    ordering_matches_scan: bool | None = None
    if context.options.schema is not None:
        ordering = ordering_from_schema(context.options.schema)
        if ordering.keys:
            ordering_keys = [list(key) for key in ordering.keys]
            if scan is not None and scan.file_sort_order:
                expected = [key[0] for key in ordering.keys]
                ordering_matches_scan = list(scan.file_sort_order) == expected
    payload: dict[str, object] = {
        "name": context.name,
        "path": str(context.location.path),
        "format": context.location.format,
        "provider": "listing",
        "file_extension": file_extension,
        "partition_cols": [
            {"name": col, "dtype": dtype} for col, dtype in (table_partition_cols or ())
        ],
        "file_sort_order": list(scan.file_sort_order) if scan is not None else None,
        "ordering_keys": ordering_keys,
        "ordering_matches_scan": ordering_matches_scan,
        "parquet_pruning": scan.parquet_pruning if scan is not None else None,
        "skip_metadata": skip_metadata,
        "skip_arrow_metadata": scan.skip_arrow_metadata if scan is not None else None,
        "binary_as_string": scan.binary_as_string if scan is not None else None,
        "schema_force_view_types": scan.schema_force_view_types if scan is not None else None,
        "collect_statistics": scan.collect_statistics if scan is not None else None,
        "meta_fetch_concurrency": scan.meta_fetch_concurrency if scan is not None else None,
        "list_files_cache_limit": scan.list_files_cache_limit if scan is not None else None,
        "list_files_cache_ttl": scan.list_files_cache_ttl if scan is not None else None,
        "listing_table_factory_infer_partitions": (
            scan.listing_table_factory_infer_partitions if scan is not None else None
        ),
        "listing_table_ignore_subdirectory": (
            scan.listing_table_ignore_subdirectory if scan is not None else None
        ),
        "listing_mutable": scan.listing_mutable if scan is not None else None,
        "unbounded": scan.unbounded if scan is not None else None,
        "schema": schema_payload,
        "read_options": dict(context.options.read_options),
    }
    profile.diagnostics_sink.record_artifact("datafusion_listing_tables_v1", payload)


def _apply_scan_settings(ctx: SessionContext, *, scan: DataFusionScanOptions | None) -> None:
    """Apply per-table DataFusion scan settings via SET statements."""
    if scan is None:
        return
    settings: Sequence[tuple[str, object | None, bool]] = (
        ("datafusion.execution.collect_statistics", scan.collect_statistics, True),
        ("datafusion.execution.meta_fetch_concurrency", scan.meta_fetch_concurrency, False),
        ("datafusion.runtime.list_files_cache_limit", scan.list_files_cache_limit, False),
        ("datafusion.runtime.list_files_cache_ttl", scan.list_files_cache_ttl, False),
        (
            "datafusion.execution.listing_table_factory_infer_partitions",
            scan.listing_table_factory_infer_partitions,
            True,
        ),
        (
            "datafusion.execution.listing_table_ignore_subdirectory",
            scan.listing_table_ignore_subdirectory,
            True,
        ),
        (
            "datafusion.execution.parquet.skip_arrow_metadata",
            scan.skip_arrow_metadata,
            True,
        ),
        ("datafusion.execution.parquet.binary_as_string", scan.binary_as_string, True),
        (
            "datafusion.execution.parquet.schema_force_view_types",
            scan.schema_force_view_types,
            True,
        ),
        ("datafusion.execution.parquet.skip_metadata", scan.skip_metadata, True),
    )
    for key, value, lower in settings:
        if value is None:
            continue
        text = str(value).lower() if lower else str(value)
        _set_runtime_setting(ctx, key=key, value=text)


def _set_runtime_setting(ctx: SessionContext, *, key: str, value: str) -> None:
    """Apply a DataFusion session setting."""
    ctx.sql(f"SET {key} = '{value}'").collect()


def _refresh_listing_table(
    ctx: SessionContext,
    *,
    name: str,
    register: Callable[[], None],
    runtime_profile: DataFusionRuntimeProfile | None,
) -> None:
    """Refresh a listing table registration by deregistering and re-registering."""
    with suppress(KeyError, ValueError):
        ctx.deregister_table(name)
    register()
    if runtime_profile is None or runtime_profile.diagnostics_sink is None:
        return
    runtime_profile.diagnostics_sink.record_artifact(
        "datafusion_listing_refresh_v1",
        {"name": name},
    )


def _schema_fingerprint(schema: pa.Schema | None) -> str | None:
    if schema is None:
        return None
    return schema_fingerprint(schema)


def _ddl_fingerprint(name: str, schema: pa.Schema | None) -> str | None:
    if schema is None:
        return None
    return ddl_fingerprint_from_schema(name, schema)


def _fingerprints_match(left: str | None, right: str | None) -> bool | None:
    if left is None or right is None:
        return None
    return left == right


def _record_delta_table_artifact(
    context: DataFusionRegistrationContext,
    *,
    provider: str,
    delta_version: int | None,
    delta_features: Mapping[str, str] | None,
    delta_protocol: Mapping[str, object] | None,
    expected_schema: pa.Schema | None,
    delta_schema: pa.Schema | None,
    provider_schema: pa.Schema | None,
) -> None:
    profile = context.runtime_profile
    if profile is None or profile.diagnostics_sink is None:
        return
    expected_fingerprint = _schema_fingerprint(expected_schema)
    delta_fingerprint = _schema_fingerprint(delta_schema)
    provider_fingerprint = _schema_fingerprint(provider_schema)
    expected_ddl = _ddl_fingerprint(context.name, expected_schema)
    delta_ddl = _ddl_fingerprint(context.name, delta_schema)
    provider_ddl = _ddl_fingerprint(context.name, provider_schema)
    schema_payload = (
        schema_to_dict(context.options.schema) if context.options.schema is not None else None
    )
    payload: dict[str, object] = {
        "name": context.name,
        "path": str(context.location.path),
        "format": context.location.format,
        "provider": provider,
        "delta_version": delta_version,
        "delta_timestamp": context.location.delta_timestamp,
        "delta_features": dict(delta_features) if delta_features else None,
        "delta_protocol": dict(delta_protocol) if delta_protocol else None,
        "schema": schema_payload,
        "expected_schema_fingerprint": expected_fingerprint,
        "delta_schema_fingerprint": delta_fingerprint,
        "provider_schema_fingerprint": provider_fingerprint,
        "expected_ddl_fingerprint": expected_ddl,
        "delta_ddl_fingerprint": delta_ddl,
        "provider_ddl_fingerprint": provider_ddl,
        "expected_matches_delta": _fingerprints_match(expected_fingerprint, delta_fingerprint),
        "expected_matches_provider": _fingerprints_match(
            expected_fingerprint,
            provider_fingerprint,
        ),
        "delta_matches_provider": _fingerprints_match(delta_fingerprint, provider_fingerprint),
        "delta_scan": _delta_scan_payload(context.options.delta_scan),
        "read_options": dict(context.options.read_options),
        "storage_options": dict(context.location.storage_options)
        if context.location.storage_options
        else None,
    }
    profile.diagnostics_sink.record_artifact("datafusion_delta_tables_v1", payload)


def _delta_dataset_from_table(
    context: DataFusionRegistrationContext,
    table: DeltaTable,
) -> ds.Dataset:
    delta_scan = context.options.delta_scan
    schema = (
        delta_scan.schema
        if delta_scan is not None and delta_scan.schema is not None
        else context.options.schema
    )
    parquet_read_options = _parquet_read_options(context.options.read_options)
    return table.to_pyarrow_dataset(
        filesystem=_delta_bulk_filesystem(context.location),
        parquet_read_options=parquet_read_options,
        schema=schema,
        as_large_types=bool(delta_scan.schema_force_view_types)
        if delta_scan is not None
        else False,
    )


def _delta_dataset_from_files(context: DataFusionRegistrationContext) -> ds.Dataset:
    delta_scan = context.options.delta_scan
    schema = (
        delta_scan.schema
        if delta_scan is not None and delta_scan.schema is not None
        else context.options.schema
    )
    file_paths = _resolve_delta_file_paths(context.location)
    parquet_format = ds.ParquetFileFormat(
        read_options=_parquet_read_options(context.options.read_options)
    )
    return ds.dataset(
        file_paths,
        format=parquet_format,
        filesystem=_delta_filesystem_override(context.location),
        schema=schema,
    )


def _resolve_delta_file_paths(location: DatasetLocation) -> list[str]:
    files = location.files or ()
    if not files:
        return []
    if location.filesystem is not None:
        return list(files)
    base = str(location.path)
    return [_join_delta_path(base, name) for name in files]


def _join_delta_path(base: str, name: str) -> str:
    if "://" in name:
        return name
    parsed = urlparse(base)
    if parsed.scheme:
        prefix = f"{parsed.scheme}://{parsed.netloc}"
        base_path = parsed.path.rstrip("/")
        return f"{prefix}{base_path}/{name.lstrip('/')}"
    return str(Path(base) / name)


def _delta_scan_payload(options: DeltaScanOptions | None) -> dict[str, object] | None:
    if options is None:
        return None
    return {
        "file_column_name": options.file_column_name,
        "enable_parquet_pushdown": options.enable_parquet_pushdown,
        "schema_force_view_types": options.schema_force_view_types,
        "schema": schema_to_dict(options.schema) if options.schema is not None else None,
    }


def _delta_scan_config(options: DeltaScanOptions | None) -> dict[str, object] | None:
    if options is None:
        return None
    config: dict[str, object] = {
        "enable_parquet_pushdown": bool(options.enable_parquet_pushdown),
        "schema_force_view_types": bool(options.schema_force_view_types),
    }
    if options.file_column_name is not None:
        config["file_column_name"] = options.file_column_name
    if options.schema is not None:
        config["schema"] = options.schema
    return config or None


def _delta_schema_ipc(schema: pa.Schema | None) -> bytes | None:
    if schema is None:
        return None
    buffer = schema.serialize()
    return buffer.to_pybytes()


def _delta_rust_table_provider(
    context: DataFusionRegistrationContext,
    *,
    delta_scan: DeltaScanOptions | None,
) -> object | None:
    try:
        module = importlib.import_module("datafusion_ext")
    except ImportError:  # pragma: no cover - optional dependency
        return None
    factory = getattr(module, "delta_table_provider", None)
    if not callable(factory):
        return None
    storage_options = (
        list(context.location.storage_options.items()) if context.location.storage_options else None
    )
    schema_ipc = None
    if delta_scan is not None and delta_scan.schema is not None:
        schema_ipc = _delta_schema_ipc(delta_scan.schema)
    capsule = factory(
        table_uri=str(context.location.path),
        storage_options=storage_options,
        version=context.location.delta_version,
        timestamp=context.location.delta_timestamp,
        file_column_name=delta_scan.file_column_name if delta_scan is not None else None,
        enable_parquet_pushdown=(
            delta_scan.enable_parquet_pushdown if delta_scan is not None else None
        ),
        schema_ipc=schema_ipc,
    )
    return TableProviderCapsule(capsule)


def _delta_table_provider(
    table: DeltaTable,
    *,
    delta_scan: DeltaScanOptions | None,
) -> object | None:
    provider = getattr(table, "__datafusion_table_provider__", None)
    if not callable(provider):
        return None
    config = _delta_scan_config(delta_scan)
    if config is None:
        try:
            return provider()
        except TypeError:
            return None
    try:
        return provider(config)
    except TypeError:
        try:
            return provider(**config)
        except TypeError:
            return None


def _delta_feature_payload(table: DeltaTable) -> dict[str, str] | None:
    metadata = table.metadata()
    configuration = metadata.configuration or {}
    features = {key: str(value) for key, value in configuration.items() if key.startswith("delta.")}
    protocol = table.protocol()
    if protocol.reader_features:
        features["reader_features"] = ",".join(protocol.reader_features)
    if protocol.writer_features:
        features["writer_features"] = ",".join(protocol.writer_features)
    return features or None


def _delta_protocol_payload(table: DeltaTable) -> dict[str, object] | None:
    protocol = table.protocol()
    return {
        "min_reader_version": protocol.min_reader_version,
        "min_writer_version": protocol.min_writer_version,
        "reader_features": list(protocol.reader_features) if protocol.reader_features else None,
        "writer_features": list(protocol.writer_features) if protocol.writer_features else None,
    }


def _parquet_read_options(read_options: Mapping[str, object]) -> ds.ParquetReadOptions | None:
    option = read_options.get("parquet_read_options")
    if isinstance(option, ds.ParquetReadOptions):
        return option
    return None


def _delta_bulk_filesystem(location: DatasetLocation) -> pafs.FileSystem | None:
    if location.filesystem is not None:
        return _normalize_filesystem(location.filesystem)
    if not isinstance(location.path, str) or "://" not in location.path:
        return None
    raw_fs, normalized_path = pafs.FileSystem.from_uri(location.path)
    return pafs.SubTreeFileSystem(normalized_path, raw_fs)


def _delta_filesystem_override(location: DatasetLocation) -> pafs.FileSystem | None:
    if location.filesystem is None:
        return None
    return _normalize_filesystem(location.filesystem)


def _normalize_filesystem(filesystem: object) -> pafs.FileSystem | None:
    if isinstance(filesystem, pafs.FileSystem):
        return filesystem
    handler = getattr(pafs, "FSSpecHandler", None)
    if handler is not None:
        try:
            return pafs.PyFileSystem(handler(filesystem))
        except (TypeError, ValueError):
            return None
    if isinstance(filesystem, pafs.FileSystemHandler):
        return pafs.PyFileSystem(filesystem)
    return None


def register_delta_cdf_df(
    ctx: SessionContext,
    *,
    name: str,
    path: str,
    options: DeltaCdfRegistrationOptions | None = None,
) -> DataFrame:
    """Register a Delta CDF snapshot as a DataFusion table.

    Returns
    -------
    datafusion.dataframe.DataFrame
        DataFusion DataFrame for the registered CDF dataset.
    """
    resolved = options or DeltaCdfRegistrationOptions()
    cdf_options = resolved.cdf_options
    storage_options = resolved.storage_options
    runtime_profile = resolved.runtime_profile
    table = DeltaTable(path, storage_options=dict(storage_options) if storage_options else None)
    provider = _delta_cdf_provider(table, cdf_options)
    provider_name = "arrow"
    if provider is not None:
        ctx.register_table(name, provider)
        provider_name = "table_provider"
        _record_table_provider_artifact(
            runtime_profile,
            name=name,
            provider=provider,
            provider_kind="cdf_table_provider",
            source=None,
        )
    else:
        cdf = read_delta_cdf(path, options=cdf_options, storage_options=storage_options)
        arrow_table = _ensure_pyarrow_table(cdf)
        ctx.register_record_batches(name, [arrow_table.to_batches()])
    _record_delta_cdf_artifact(
        runtime_profile,
        name=name,
        path=path,
        provider=provider_name,
        options=cdf_options,
    )
    return ctx.table(name)


def _ensure_pyarrow_table(value: object) -> pa.Table:
    if isinstance(value, pa.Table):
        return value
    msg = f"Delta CDF read expected pyarrow.Table, got {type(value)}"
    raise TypeError(msg)


def _call_cdf_provider(
    provider: Callable[..., object],
    *,
    args: object | None = None,
    kwargs: Mapping[str, object] | None = None,
) -> object | None:
    try:
        if kwargs is not None:
            return provider(**kwargs)
        if args is None:
            return provider()
        return provider(args)
    except TypeError:
        return None


def _delta_cdf_provider(
    table: DeltaTable,
    options: DeltaCdfOptions | None,
) -> object | None:
    provider = getattr(table, "cdf_table_provider", None)
    if not callable(provider):
        return None
    result: object | None = None
    if options is None:
        return _call_cdf_provider(provider)
    kwargs = _cdf_provider_kwargs(options)
    if kwargs:
        result = _call_cdf_provider(provider, kwargs=kwargs)
    if result is None:
        result = _call_cdf_provider(provider, args=options)
    if result is None:
        result = _call_cdf_provider(provider)
    return result


def _cdf_provider_kwargs(options: DeltaCdfOptions) -> dict[str, object]:
    payload: dict[str, object] = {
        "starting_version": options.starting_version,
        "ending_version": options.ending_version,
        "starting_timestamp": options.starting_timestamp,
        "ending_timestamp": options.ending_timestamp,
        "columns": options.columns,
        "predicate": options.predicate,
        "allow_out_of_range": options.allow_out_of_range,
    }
    return {key: value for key, value in payload.items() if value is not None}


def _record_delta_cdf_artifact(
    runtime_profile: DataFusionRuntimeProfile | None,
    *,
    name: str,
    path: str,
    provider: str,
    options: DeltaCdfOptions | None,
) -> None:
    if runtime_profile is None or runtime_profile.diagnostics_sink is None:
        return
    payload: dict[str, object] = {
        "name": name,
        "path": path,
        "provider": provider,
        "options": _cdf_options_payload(options),
    }
    runtime_profile.diagnostics_sink.record_artifact("datafusion_delta_cdf_v1", payload)


def _cdf_options_payload(options: DeltaCdfOptions | None) -> dict[str, object] | None:
    if options is None:
        return None
    return {
        "starting_version": options.starting_version,
        "ending_version": options.ending_version,
        "starting_timestamp": options.starting_timestamp,
        "ending_timestamp": options.ending_timestamp,
        "columns": list(options.columns) if options.columns is not None else None,
        "predicate": options.predicate,
        "allow_out_of_range": options.allow_out_of_range,
    }


def _register_simple(context: DataFusionRegistrationContext, *, method: str) -> DataFrame:
    register = getattr(context.ctx, method, None)
    if not callable(register):
        msg = f"DataFusion SessionContext missing {method}."
        raise TypeError(msg)
    kwargs = dict(context.options.read_options)
    if context.options.schema is not None:
        kwargs.setdefault("schema", context.options.schema)
    _call_register(register, context.name, context.location.path, kwargs)
    df = context.ctx.table(context.name)
    return _maybe_cache(context, df)


def _register_object_store(ctx: SessionContext, location: DatasetLocation) -> None:
    register = getattr(ctx, "register_object_store", None)
    if not callable(register):
        return
    if location.filesystem is None:
        return
    scheme = _scheme_prefix(location.path)
    if scheme is None:
        return
    ctx_key = id(ctx)
    registered = _REGISTERED_OBJECT_STORES.setdefault(ctx_key, set())
    if scheme in registered:
        return
    register(scheme, location.filesystem, None)
    registered.add(scheme)


def _scheme_prefix(path: str | Path) -> str | None:
    if not isinstance(path, str):
        return None
    parsed = urlparse(path)
    if not parsed.scheme:
        return None
    return f"{parsed.scheme}://"


def _register_dataset_provider(context: DataFusionRegistrationContext) -> DataFrame:
    dataset = ds.dataset(
        context.location.path,
        format=context.location.format,
        schema=context.options.schema,
        filesystem=context.location.filesystem,
        partitioning=context.location.partitioning or "hive",
    )
    context.ctx.register_table(context.name, dataset)
    df = context.ctx.table(context.name)
    return _maybe_cache(context, df)


def _merge_kwargs(base: Mapping[str, object], extra: Mapping[str, object]) -> dict[str, object]:
    merged = dict(base)
    merged.update(extra)
    return merged


def _effective_skip_metadata(location: DatasetLocation, scan: DataFusionScanOptions) -> bool:
    return scan.skip_metadata and not _has_metadata_sidecars(location.path)


def _has_metadata_sidecars(path: str | Path) -> bool:
    if isinstance(path, str) and "://" in path:
        return False
    base = ensure_path(path)
    if not base.exists():
        return False
    if base.is_dir():
        return (base / "_common_metadata").exists() or (base / "_metadata").exists()
    return False


def _maybe_cache(context: DataFusionRegistrationContext, df: DataFrame) -> DataFrame:
    if not context.cache.enabled or not _should_cache_df(
        df,
        cache_max_columns=context.cache.max_columns,
    ):
        return df
    cached = df.cache()
    context.ctx.deregister_table(context.name)
    context.ctx.register_table(context.name, cached)
    cached_set = _CACHED_DATASETS.setdefault(id(context.ctx), set())
    cached_set.add(context.name)
    return cached


def cached_dataset_names(ctx: SessionContext) -> tuple[str, ...]:
    """Return cached dataset names for a SessionContext.

    Returns
    -------
    tuple[str, ...]
        Cached dataset names sorted in ascending order.
    """
    cached = _CACHED_DATASETS.get(id(ctx), set())
    return tuple(sorted(cached))


def _ensure_catalog_schema(ctx: SessionContext, *, catalog: str, schema: str) -> None:
    ctx_id = id(ctx)
    registered_catalogs = _REGISTERED_CATALOGS.setdefault(ctx_id, set())
    registered_schemas = _REGISTERED_SCHEMAS.setdefault(ctx_id, set())
    cat: Catalog
    if catalog in registered_catalogs:
        cat = ctx.catalog(catalog)
    else:
        try:
            cat = ctx.catalog(catalog)
        except KeyError:
            cat = Catalog.memory_catalog()
            ctx.register_catalog_provider(catalog, cat)
        registered_catalogs.add(catalog)
    if (catalog, schema) in registered_schemas:
        return
    if schema not in cat.schema_names():
        cat.register_schema(schema, Schema.memory_schema())
    registered_schemas.add((catalog, schema))


def _should_cache_df(df: DataFrame, *, cache_max_columns: int | None) -> bool:
    if cache_max_columns is None:
        return True
    column_count = len(df.schema().names)
    return column_count <= cache_max_columns


def _resolve_cache_policy(
    options: DataFusionRegistryOptions,
    *,
    cache_policy: DataFusionCachePolicy | None,
    runtime_profile: DataFusionRuntimeProfile | None,
) -> DataFusionCacheSettings:
    enabled = cache_policy.enabled if cache_policy is not None else None
    max_columns = cache_policy.max_columns if cache_policy is not None else None
    if runtime_profile is not None:
        if enabled is None:
            enabled = runtime_profile.cache_enabled
        if max_columns is None:
            max_columns = runtime_profile.cache_max_columns
    if enabled is None:
        enabled = True
    if max_columns is None:
        max_columns = DEFAULT_CACHE_MAX_COLUMNS
    return DataFusionCacheSettings(enabled=options.cache and enabled, max_columns=max_columns)


def _call_register(
    fn: Callable[..., object],
    name: str,
    path: str | Path,
    kwargs: Mapping[str, object],
) -> None:
    if not callable(fn):
        msg = "DataFusion registration target is not callable."
        raise TypeError(msg)
    filtered = _filter_kwargs(fn, kwargs)
    fn(name, path, **filtered)


def _filter_kwargs(fn: Callable[..., object], kwargs: Mapping[str, object]) -> dict[str, object]:
    try:
        signature = inspect.signature(fn)
    except (TypeError, ValueError):
        return dict(kwargs)
    if any(param.kind == param.VAR_KEYWORD for param in signature.parameters.values()):
        return dict(kwargs)
    return {key: value for key, value in kwargs.items() if key in signature.parameters}


__all__ = [
    "DataFusionCachePolicy",
    "DataFusionCacheSettings",
    "DataFusionRegistryOptions",
    "DatasetInputSource",
    "datafusion_external_table_sql",
    "dataset_input_plugin",
    "input_plugin_prefixes",
    "register_dataset_df",
    "register_delta_cdf_df",
    "resolve_registry_options",
]
