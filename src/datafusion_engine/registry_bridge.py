"""Dataset registry bridge for DataFusion SessionContext."""

from __future__ import annotations

import inspect
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlparse

import pyarrow.dataset as ds
from datafusion import SessionContext
from datafusion.dataframe import DataFrame

from arrowdsl.core.interop import SchemaLike
from core_types import ensure_path
from datafusion_engine.runtime import DataFusionRuntimeProfile
from ibis_engine.registry import (
    DatasetLocation,
    resolve_datafusion_scan_options,
    resolve_dataset_schema,
)
from schema_spec.specs import ExternalTableConfig, TableSchemaSpec
from schema_spec.system import DataFusionScanOptions
from sqlglot_tools.optimizer import register_datafusion_dialect

DEFAULT_CACHE_MAX_COLUMNS = 64


@dataclass(frozen=True)
class DataFusionRegistryOptions:
    """Resolved DataFusion registration options for a dataset."""

    scan: DataFusionScanOptions | None
    schema: SchemaLike | None
    read_options: Mapping[str, object]
    cache: bool
    provider: Literal["dataset", "listing", "parquet"] | None


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


def resolve_registry_options(location: DatasetLocation) -> DataFusionRegistryOptions:
    """Resolve DataFusion registration hints for a dataset location.

    Returns
    -------
    DataFusionRegistryOptions
        Registration options derived from the dataset location.
    """
    scan = resolve_datafusion_scan_options(location)
    schema = resolve_dataset_schema(location)
    provider = location.datafusion_provider
    if (
        provider is None
        and scan is not None
        and (scan.partition_cols or scan.file_sort_order)
    ):
        provider = "listing"
    return DataFusionRegistryOptions(
        scan=scan,
        schema=schema,
        read_options=dict(location.read_options),
        cache=bool(scan.cache) if scan is not None else False,
        provider=provider,
    )


def datafusion_external_table_sql(
    *,
    name: str,
    location: DatasetLocation,
    dialect: str = "datafusion_ext",
) -> str | None:
    """Return a CREATE EXTERNAL TABLE statement for a dataset location.

    Returns
    -------
    str | None
        External table DDL when a schema is available, otherwise ``None``.
    """
    table_spec = _resolve_table_spec(location)
    if table_spec is None:
        return None
    if dialect == "datafusion_ext":
        register_datafusion_dialect()
    options, compression = _external_table_options(location.read_options)
    partitioned_by = _partitioned_by(location)
    config = ExternalTableConfig(
        location=str(location.path),
        file_format=location.format,
        table_name=name,
        dialect=dialect,
        options=options,
        partitioned_by=partitioned_by,
        compression=compression,
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
    _register_object_store(ctx, location)
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
        external_table_sql=datafusion_external_table_sql(name=name, location=location),
    )
    if options.provider == "dataset":
        return _register_dataset_provider(context)
    if location.format == "parquet":
        return _register_parquet(context)
    if location.format == "csv":
        return _register_simple(context, method="register_csv")
    if location.format == "json":
        return _register_simple(context, method="register_json")
    if location.format == "avro":
        return _register_simple(context, method="register_avro")
    msg = f"Unsupported DataFusion dataset format: {location.format!r}."
    raise ValueError(msg)


def _register_parquet(context: DataFusionRegistrationContext) -> DataFrame:
    scan = context.options.scan
    file_extension = scan.file_extension if scan and scan.file_extension else ".parquet"
    table_partition_cols = (
        [(col, str(dtype)) for col, dtype in scan.partition_cols]
        if scan and scan.partition_cols
        else None
    )
    kwargs: dict[str, Any] = {
        "schema": context.options.schema,
        "file_extension": file_extension,
        "table_partition_cols": table_partition_cols,
    }
    if scan is not None:
        kwargs["file_sort_order"] = scan.file_sort_order or None
        kwargs["parquet_pruning"] = scan.parquet_pruning
        kwargs["skip_metadata"] = _effective_skip_metadata(context.location, scan)
    kwargs = _merge_kwargs(kwargs, context.options.read_options)
    if table_partition_cols or context.options.provider == "listing":
        _call_register(
            context.ctx.register_listing_table,
            context.name,
            context.location.path,
            kwargs,
        )
        df = context.ctx.table(context.name)
    else:
        _call_register(
            context.ctx.register_parquet,
            context.name,
            context.location.path,
            kwargs,
        )
        df = context.ctx.table(context.name)
    return _maybe_cache(context, df)


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
    register(scheme, location.filesystem, None)


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
    return cached


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
    "datafusion_external_table_sql",
    "register_dataset_df",
    "resolve_registry_options",
]
