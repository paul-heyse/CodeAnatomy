"""Cache-policy and registration utility helpers for dataset registration."""

from __future__ import annotations

import inspect
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa
from datafusion.dataframe import DataFrame

from datafusion_engine.arrow.interop import arrow_schema_from_df
from datafusion_engine.dataset.registration_core import (
    DataFusionCacheSettings as _DataFusionCacheSettings,
)
from datafusion_engine.dataset.registration_scan import DEFAULT_CACHE_MAX_COLUMNS
from datafusion_engine.dataset.registry import DatasetLocation
from datafusion_engine.identity import schema_identity_hash
from datafusion_engine.io.adapter import DataFusionIOAdapter

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.dataset.registration_core import (
        DataFusionCachePolicy,
        DataFusionRegistrationContext,
        DataFusionRegistryOptions,
        DatasetCaches,
    )
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile


def _resolve_dataset_caches(caches: DatasetCaches | None) -> DatasetCaches:
    if caches is not None:
        return caches
    from datafusion_engine.dataset.registration_core import DatasetCaches as _DatasetCaches

    return _DatasetCaches()


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
) -> _DataFusionCacheSettings:
    enabled = cache_policy.enabled if cache_policy is not None else None
    max_columns = cache_policy.max_columns if cache_policy is not None else None
    storage = cache_policy.storage if cache_policy is not None else "memory"
    base_cache_enabled = options.cache if cache_policy is None else True
    if runtime_profile is not None:
        if enabled is None:
            enabled = runtime_profile.features.cache_enabled
        if max_columns is None:
            max_columns = runtime_profile.policies.cache_max_columns
    if enabled is None:
        enabled = True
    if max_columns is None:
        max_columns = DEFAULT_CACHE_MAX_COLUMNS
    return _DataFusionCacheSettings(
        enabled=base_cache_enabled and enabled,
        max_columns=max_columns,
        storage=storage,
    )


def _maybe_cache(context: DataFusionRegistrationContext, df: DataFrame) -> DataFrame:
    if not context.cache.enabled or not _should_cache_df(
        df,
        cache_max_columns=context.cache.max_columns,
    ):
        return df
    if context.cache.storage == "delta_staging":
        return _register_delta_cache_for_dataset(context, df)
    return _register_memory_cache(context, df)


def _register_memory_cache(
    context: DataFusionRegistrationContext,
    df: DataFrame,
) -> DataFrame:
    cached = df.cache()
    adapter = DataFusionIOAdapter(ctx=context.ctx, profile=context.runtime_profile)
    adapter.register_view(
        context.name,
        cached,
        overwrite=True,
        temporary=False,
    )
    cached_set = context.caches.cached_datasets.setdefault(context.ctx, set())
    cached_set.add(context.name)
    return cached


def _register_delta_cache_for_dataset(
    context: DataFusionRegistrationContext,
    df: DataFrame,
) -> DataFrame:
    runtime_profile = context.runtime_profile
    if runtime_profile is None:
        return _register_memory_cache(context, df)
    cache_root = Path(runtime_profile.io_ops.cache_root()) / "dataset_cache"
    cache_root.mkdir(parents=True, exist_ok=True)
    from datafusion_engine.tables.spec import table_spec_from_location

    cache_key = table_spec_from_location(
        context.name,
        context.location,
    ).cache_key()
    safe_name = context.name.replace("/", "_").replace(":", "_")
    cache_path = str(cache_root / f"{safe_name}__{cache_key}")
    cached_df = _strip_delta_file_column_for_cache(df, location=context.location)
    schema = arrow_schema_from_df(cached_df)
    schema_hash = schema_identity_hash(schema)
    partition_by = _dataset_cache_partition_by(schema, location=context.location)
    from datafusion_engine.cache.commit_metadata import (
        CacheCommitMetadataRequest,
        cache_commit_metadata,
    )
    from datafusion_engine.io.write_core import (
        WriteFormat,
        WriteMode,
        WritePipeline,
        WriteRequest,
    )
    from obs.otel import cache_span

    commit_metadata = cache_commit_metadata(
        CacheCommitMetadataRequest(
            operation="cache_write",
            cache_policy="dataset_delta_staging",
            cache_scope="dataset",
            schema_hash=schema_hash,
            cache_key=cache_key,
        )
    )
    pipeline = WritePipeline(context.ctx, runtime_profile=runtime_profile)
    with cache_span(
        "cache.dataset.delta_staging.write",
        cache_policy="dataset_delta_staging",
        cache_scope="dataset",
        operation="write",
        attributes={
            "dataset_name": context.name,
            "cache_key": cache_key,
        },
    ) as (_span, set_result):
        result = pipeline.write(
            WriteRequest(
                source=cached_df,
                destination=cache_path,
                format=WriteFormat.DELTA,
                mode=WriteMode.OVERWRITE,
                partition_by=partition_by,
                format_options={"commit_metadata": commit_metadata},
            )
        )
        set_result("write")
    from datafusion_engine.cache.inventory import CacheInventoryEntry, delta_report_file_count
    from datafusion_engine.cache.registry import (
        record_cache_inventory,
        register_cached_delta_table,
    )
    from datafusion_engine.dataset.registry import DatasetLocation

    location = DatasetLocation(path=cache_path, format="delta")
    register_cached_delta_table(
        context.ctx,
        runtime_profile,
        name=context.name,
        location=location,
        snapshot_version=result.delta_result.version if result.delta_result else None,
    )
    file_count = delta_report_file_count(
        result.delta_result.report if result.delta_result is not None else None
    )
    record_cache_inventory(
        runtime_profile,
        entry=CacheInventoryEntry(
            view_name=context.name,
            cache_policy="dataset_delta_staging",
            cache_path=cache_path,
            result="write",
            plan_fingerprint=None,
            plan_identity_hash=cache_key,
            schema_identity_hash=schema_hash,
            snapshot_version=result.delta_result.version if result.delta_result else None,
            snapshot_timestamp=None,
            row_count=result.rows_written,
            file_count=file_count,
            partition_by=partition_by,
        ),
        ctx=context.ctx,
    )
    return context.ctx.table(context.name)


def _strip_delta_file_column_for_cache(
    df: DataFrame,
    *,
    location: DatasetLocation,
) -> DataFrame:
    scan = location.delta_scan
    if scan is None or not scan.file_column_name:
        return df
    schema = df.schema()
    names = schema.names if hasattr(schema, "names") else tuple(field.name for field in schema)
    file_column = scan.file_column_name
    if file_column not in names:
        return df
    return df.drop(file_column)


def _dataset_cache_partition_by(
    schema: pa.Schema,
    *,
    location: DatasetLocation,
) -> tuple[str, ...]:
    policy_partition_by: tuple[str, ...] = ()
    policy = location.delta_write_policy
    if policy is not None:
        policy_partition_by = tuple(str(name) for name in policy.partition_by)
    available = set(schema.names)
    if policy_partition_by:
        missing = [name for name in policy_partition_by if name not in available]
        if missing:
            msg = f"Delta partition_by columns missing from schema: {sorted(missing)}."
            raise ValueError(msg)
    return tuple(name for name in policy_partition_by if name in available)


def cached_dataset_names(
    ctx: SessionContext,
    *,
    caches: DatasetCaches | None = None,
) -> tuple[str, ...]:
    """Return cached dataset names for a SessionContext."""
    resolved_caches = _resolve_dataset_caches(caches)
    cached = resolved_caches.cached_datasets.get(ctx, set())
    return tuple(sorted(cached))


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
    "_call_register",
    "_dataset_cache_partition_by",
    "_filter_kwargs",
    "_maybe_cache",
    "_register_delta_cache_for_dataset",
    "_register_memory_cache",
    "_resolve_cache_policy",
    "_should_cache_df",
    "_strip_delta_file_column_for_cache",
    "cached_dataset_names",
]
