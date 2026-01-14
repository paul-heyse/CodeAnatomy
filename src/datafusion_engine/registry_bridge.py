"""Dataset registry bridge for DataFusion SessionContext."""

from __future__ import annotations

import inspect
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Literal

import pyarrow.dataset as ds
from datafusion import SessionContext
from datafusion.dataframe import DataFrame

from arrowdsl.core.interop import SchemaLike
from core_types import ensure_path
from ibis_engine.registry import (
    DatasetLocation,
    resolve_datafusion_scan_options,
    resolve_dataset_schema,
)
from schema_spec.system import DataFusionScanOptions


@dataclass(frozen=True)
class DataFusionRegistryOptions:
    """Resolved DataFusion registration options for a dataset."""

    scan: DataFusionScanOptions | None
    schema: SchemaLike | None
    read_options: Mapping[str, object]
    cache: bool
    provider: Literal["dataset", "listing", "parquet"] | None


def resolve_registry_options(location: DatasetLocation) -> DataFusionRegistryOptions:
    """Resolve DataFusion registration hints for a dataset location.

    Returns
    -------
    DataFusionRegistryOptions
        Registration options derived from the dataset location.
    """
    scan = resolve_datafusion_scan_options(location)
    schema = resolve_dataset_schema(location)
    return DataFusionRegistryOptions(
        scan=scan,
        schema=schema,
        read_options=dict(location.read_options),
        cache=bool(scan.cache) if scan is not None else False,
        provider=location.datafusion_provider,
    )


def register_dataset_df(
    ctx: SessionContext,
    *,
    name: str,
    location: DatasetLocation,
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
    options = resolve_registry_options(location)
    if options.provider == "dataset":
        return _register_dataset_provider(ctx, name=name, location=location, options=options)
    if location.format == "parquet":
        return _register_parquet(ctx, name=name, location=location, options=options)
    if location.format == "csv":
        return _register_simple(
            ctx,
            name=name,
            location=location,
            options=options,
            method="register_csv",
        )
    if location.format == "json":
        return _register_simple(
            ctx,
            name=name,
            location=location,
            options=options,
            method="register_json",
        )
    if location.format == "avro":
        return _register_simple(
            ctx,
            name=name,
            location=location,
            options=options,
            method="register_avro",
        )
    msg = f"Unsupported DataFusion dataset format: {location.format!r}."
    raise ValueError(msg)


def _register_parquet(
    ctx: SessionContext,
    *,
    name: str,
    location: DatasetLocation,
    options: DataFusionRegistryOptions,
) -> DataFrame:
    scan = options.scan
    file_extension = scan.file_extension if scan and scan.file_extension else ".parquet"
    table_partition_cols = (
        [(col, str(dtype)) for col, dtype in scan.partition_cols]
        if scan and scan.partition_cols
        else None
    )
    kwargs: dict[str, Any] = {
        "schema": options.schema,
        "file_extension": file_extension,
        "table_partition_cols": table_partition_cols,
    }
    if scan is not None:
        kwargs["file_sort_order"] = scan.file_sort_order or None
        kwargs["parquet_pruning"] = scan.parquet_pruning
        kwargs["skip_metadata"] = _effective_skip_metadata(location, scan)
    kwargs = _merge_kwargs(kwargs, options.read_options)
    if table_partition_cols or options.provider == "listing":
        _call_register(ctx.register_listing_table, name, location.path, kwargs)
        df = ctx.table(name)
    else:
        _call_register(ctx.register_parquet, name, location.path, kwargs)
        df = ctx.table(name)
    return _maybe_cache(ctx, df, cache=options.cache, name=name)


def _register_simple(
    ctx: SessionContext,
    *,
    name: str,
    location: DatasetLocation,
    options: DataFusionRegistryOptions,
    method: str,
) -> DataFrame:
    register = getattr(ctx, method, None)
    if not callable(register):
        msg = f"DataFusion SessionContext missing {method}."
        raise TypeError(msg)
    kwargs = dict(options.read_options)
    if options.schema is not None:
        kwargs.setdefault("schema", options.schema)
    _call_register(register, name, location.path, kwargs)
    df = ctx.table(name)
    return _maybe_cache(ctx, df, cache=options.cache, name=name)


def _register_dataset_provider(
    ctx: SessionContext,
    *,
    name: str,
    location: DatasetLocation,
    options: DataFusionRegistryOptions,
) -> DataFrame:
    dataset = ds.dataset(
        location.path,
        format=location.format,
        schema=options.schema,
        filesystem=location.filesystem,
        partitioning=location.partitioning or "hive",
    )
    ctx.register_table(name, dataset)
    df = ctx.table(name)
    return _maybe_cache(ctx, df, cache=options.cache, name=name)


def _merge_kwargs(base: Mapping[str, object], extra: Mapping[str, object]) -> dict[str, object]:
    merged = dict(base)
    merged.update(extra)
    return merged


def _effective_skip_metadata(location: DatasetLocation, scan: DataFusionScanOptions) -> bool:
    return scan.skip_metadata and not _has_metadata_sidecars(location.path)


def _has_metadata_sidecars(path: str | object) -> bool:
    if isinstance(path, str) and "://" in path:
        return False
    base = ensure_path(path)
    if not base.exists():
        return False
    if base.is_dir():
        return (base / "_common_metadata").exists() or (base / "_metadata").exists()
    return False


def _maybe_cache(
    ctx: SessionContext,
    df: DataFrame,
    *,
    cache: bool,
    name: str,
) -> DataFrame:
    if not cache:
        return df
    cached = df.cache()
    ctx.deregister_table(name)
    ctx.register_table(name, cached)
    return cached


def _call_register(
    fn: Callable[..., object],
    name: str,
    path: str | object,
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


__all__ = ["DataFusionRegistryOptions", "register_dataset_df", "resolve_registry_options"]
