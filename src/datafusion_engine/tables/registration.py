"""Centralized DataFusion table registration helpers."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, cast

import pyarrow as pa
import pyarrow.dataset as ds
from datafusion import SessionContext
from datafusion.dataframe import DataFrame

from datafusion_engine.catalog.provider_registry import ProviderRegistry
from datafusion_engine.dataset.registry import DatasetLocation
from datafusion_engine.delta.store_policy import apply_delta_store_policy
from datafusion_engine.io.adapter import DataFusionIOAdapter, ListingTableRegistration
from datafusion_engine.session.runtime import (
    DATAFUSION_MAJOR_VERSION,
    DATAFUSION_RUNTIME_SETTINGS_SKIP_VERSION,
    record_runtime_setting_override,
)
from datafusion_engine.sql import options as _sql_options
from datafusion_engine.tables.spec import table_spec_from_location

if TYPE_CHECKING:
    from datafusion import SQLOptions

    from datafusion_engine.dataset.registration import (
        DataFusionCachePolicy,
        DataFusionRegistryOptions,
    )
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from schema_spec.system import DataFusionScanOptions


class ListingRegistrationContext(Protocol):
    """Protocol for listing table registration inputs."""

    @property
    def ctx(self) -> SessionContext: ...

    @property
    def name(self) -> str: ...

    @property
    def location(self) -> DatasetLocation: ...

    @property
    def options(self) -> DataFusionRegistryOptions: ...

    @property
    def runtime_profile(self) -> DataFusionRuntimeProfile | None: ...


@dataclass(frozen=True)
class ListingRegistrationResult:
    """Result for listing table registration."""

    df: DataFrame
    provider: object
    details: Mapping[str, object]


@dataclass(frozen=True)
class TableRegistrationRequest:
    """Inputs for registering a table with DataFusion."""

    name: str
    location: DatasetLocation
    cache_policy: DataFusionCachePolicy | None = None
    runtime_profile: DataFusionRuntimeProfile | None = None


def register_table(
    ctx: SessionContext,
    request: TableRegistrationRequest,
) -> DataFrame:
    """Register a table using the unified registry and return a DataFrame.

    Args:
        ctx: DataFusion session context.
        request: Table registration request payload.

    Returns:
        DataFrame: Result.

    Raises:
        ValueError: If runtime profile is missing from the request.
    """
    if request.runtime_profile is None:
        msg = "Runtime profile is required for table registration."
        raise ValueError(msg)
    if request.location.format != "delta":
        from datafusion_engine.session.runtime import effective_catalog_autoload

        catalog_location, catalog_format = effective_catalog_autoload(request.runtime_profile)
        if catalog_location is not None and catalog_format is not None:
            try:
                return ctx.table(request.name)
            except (KeyError, RuntimeError, TypeError, ValueError) as exc:
                msg = f"Catalog autoload failed for {request.name!r}: {exc}"
                raise ValueError(msg) from exc
    location = apply_delta_store_policy(
        request.location,
        policy=request.runtime_profile.policies.delta_store_policy,
    )
    from datafusion_engine.dataset.registry import resolve_dataset_schema

    schema = resolve_dataset_schema(location)
    if schema is None and location.format != "delta":
        try:
            dataset = ds.dataset(
                list(location.files) if location.files is not None else location.path,
                format=location.format,
                filesystem=location.filesystem,
                partitioning=location.partitioning,
            )
            schema = dataset.schema
        except (TypeError, ValueError, OSError) as exc:
            msg = f"Schema required for dataset registration: {request.name!r}."
            raise ValueError(msg) from exc
    if schema is None:
        msg = f"Schema required for dataset registration: {request.name!r}."
        raise ValueError(msg)
    spec = table_spec_from_location(
        request.name,
        location,
        schema=cast("pa.Schema", schema),
        required_udfs=(),
    )
    registry = ProviderRegistry(ctx=ctx, runtime_profile=request.runtime_profile)
    return registry.register_df(spec, cache_policy=request.cache_policy)


def register_listing_table(
    context: ListingRegistrationContext,
) -> ListingRegistrationResult:
    """Register a listing table using DataFusion-native surfaces when possible.

    Returns:
    -------
    ListingRegistrationResult
        Registered DataFusion DataFrame and provider metadata.
    """
    location = context.location
    scan = context.options.scan
    runtime_profile = context.runtime_profile
    sql_options = _sql_options.sql_options_for_profile(runtime_profile)
    _apply_scan_settings(context.ctx, scan=scan, sql_options=sql_options)
    provider: object
    registration_mode = "listing_table"
    if _can_use_listing_table(location):
        _register_listing_table_native(context, scan=scan)
        provider = context.ctx.table(context.name)
    else:
        registration_mode = "pyarrow_dataset"
        provider = _build_pyarrow_dataset(
            location,
            schema=context.options.schema,
        )
        adapter = DataFusionIOAdapter(ctx=context.ctx, profile=runtime_profile)
        adapter.register_table(context.name, provider)
    df = context.ctx.table(context.name)
    details: dict[str, object] = {
        "path": str(location.path),
        "format": location.format,
        "partitioning": location.partitioning,
        "read_options": dict(context.options.read_options),
        "registration_mode": registration_mode,
    }
    if scan is not None:
        details.update(_scan_details(scan))
    return ListingRegistrationResult(df=df, provider=provider, details=details)


def _apply_scan_settings(
    ctx: SessionContext,
    *,
    scan: DataFusionScanOptions | None,
    sql_options: SQLOptions,
) -> None:
    if scan is None:
        return
    skip_runtime_settings = (
        DATAFUSION_MAJOR_VERSION is not None
        and DATAFUSION_MAJOR_VERSION >= DATAFUSION_RUNTIME_SETTINGS_SKIP_VERSION
    )
    settings: list[tuple[str, object | None, bool]] = [
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
    ]
    for key, value, lower in settings:
        if value is None:
            continue
        text = str(value).lower() if lower else str(value)
        if skip_runtime_settings and key.startswith("datafusion.runtime."):
            record_runtime_setting_override(ctx, key=key, value=text)
            continue
        _set_runtime_setting(ctx, key=key, value=text, sql_options=sql_options)


def _set_runtime_setting(
    ctx: SessionContext,
    *,
    key: str,
    value: str,
    sql_options: SQLOptions,
) -> None:
    sql = f"SET {key} = '{value}'"
    allow_statements_flag = True
    resolved_sql_options = sql_options.with_allow_statements(allow_statements_flag)
    try:
        df = ctx.sql_with_options(sql, resolved_sql_options)
    except (RuntimeError, TypeError, ValueError) as exc:
        msg = "SET execution failed."
        raise ValueError(msg) from exc
    if df is None:
        msg = "SET execution did not return a DataFusion DataFrame."
        raise ValueError(msg)
    df.collect()


def _scan_details(scan: DataFusionScanOptions) -> dict[str, object]:
    return {
        "partition_cols": [(col, str(dtype)) for col, dtype in scan.partition_cols_pyarrow()],
        "file_sort_order": [list(value) for value in scan.file_sort_order],
        "file_extension": scan.file_extension,
        "parquet_pruning": scan.parquet_pruning,
        "skip_metadata": scan.skip_metadata,
        "skip_arrow_metadata": scan.skip_arrow_metadata,
        "binary_as_string": scan.binary_as_string,
        "schema_force_view_types": scan.schema_force_view_types,
        "listing_table_factory_infer_partitions": scan.listing_table_factory_infer_partitions,
        "listing_table_ignore_subdirectory": scan.listing_table_ignore_subdirectory,
        "cache": scan.cache,
        "collect_statistics": scan.collect_statistics,
        "meta_fetch_concurrency": scan.meta_fetch_concurrency,
        "list_files_cache_ttl": scan.list_files_cache_ttl,
        "list_files_cache_limit": scan.list_files_cache_limit,
        "projection_exprs": list(scan.projection_exprs),
        "listing_mutable": scan.listing_mutable,
        "unbounded": scan.unbounded,
    }


def _can_use_listing_table(location: DatasetLocation) -> bool:
    return location.files is None and location.filesystem is None


def _register_listing_table_native(
    context: ListingRegistrationContext,
    *,
    scan: DataFusionScanOptions | None,
) -> None:
    file_extension = scan.file_extension if scan is not None else None
    partition_cols = scan.partition_cols_pyarrow() if scan is not None else ()
    file_sort_order = scan.file_sort_order if scan is not None else ()
    arrow_schema = cast("pa.Schema | None", context.options.schema)
    adapter = DataFusionIOAdapter(ctx=context.ctx, profile=context.runtime_profile)
    adapter.register_listing_table(
        ListingTableRegistration(
            name=context.name,
            path=str(context.location.path),
            file_extension=file_extension,
            table_partition_cols=partition_cols,
            schema=arrow_schema,
            file_sort_order=file_sort_order,
        )
    )


def _build_pyarrow_dataset(
    location: DatasetLocation,
    *,
    schema: object | None,
) -> object:
    arrow_schema = cast("pa.Schema | None", schema)
    if location.files:
        return ds.dataset(
            list(location.files),
            format=location.format,
            filesystem=location.filesystem,
            schema=arrow_schema,
        )
    return ds.dataset(
        str(Path(location.path)),
        format=location.format,
        filesystem=location.filesystem,
        partitioning=location.partitioning,
        schema=arrow_schema,
    )


__all__ = [
    "ListingRegistrationResult",
    "TableRegistrationRequest",
    "register_listing_table",
    "register_table",
]
