"""DataFusion table provider registration for Delta Change Data Feed."""

from __future__ import annotations

import contextlib
import uuid
from dataclasses import dataclass

import pyarrow as pa
from datafusion import SessionContext
from deltalake import DeltaTable

from datafusion_engine.table_provider_metadata import (
    TableProviderMetadata,
    record_table_provider_metadata,
)
from incremental.cdf_filters import CdfFilterPolicy
from storage.deltalake.delta import DeltaCdfOptions, StorageOptions


@dataclass(frozen=True)
class DeltaCdfRegistration:
    """Registration parameters for Delta CDF tables."""

    table_path: str
    table_name: str | None = None
    storage_options: StorageOptions | None = None
    cdf_options: DeltaCdfOptions | None = None
    filter_policy: CdfFilterPolicy | None = None


@dataclass(frozen=True)
class DeltaCdfStreamSpec:
    """Convenience spec for streaming CDF registrations."""

    table_path: str
    starting_version: int
    ending_version: int | None = None
    table_name: str | None = None
    storage_options: StorageOptions | None = None
    filter_policy: CdfFilterPolicy | None = None


def register_delta_cdf_table(ctx: SessionContext, registration: DeltaCdfRegistration) -> str:
    """Register a Delta CDF table as a DataFusion table provider.

    This function reads changes from a Delta table's change data feed
    and registers the resulting data as a queryable table in DataFusion.

    Parameters
    ----------
    ctx : SessionContext
        DataFusion session context for table registration.
    registration : DeltaCdfRegistration
        Registration configuration for the CDF table.

    Returns
    -------
    str
        Name of the registered table.

    Raises
    ------
    ValueError
        If CDF is not enabled on the Delta table or if the version range is invalid.
    """
    table_name = _resolve_table_name(registration.table_name)

    # Prepare storage options
    storage: dict[str, str] = (
        dict(registration.storage_options) if registration.storage_options is not None else {}
    )

    # Load the Delta table
    dt = DeltaTable(registration.table_path, storage_options=storage or None)

    # Check if CDF is enabled
    try:
        _ensure_cdf_enabled(dt=dt, table_path=registration.table_path)
    except ValueError as exc:
        raise ValueError(str(exc)) from exc

    # Prepare CDF options
    resolved_cdf_options = registration.cdf_options or DeltaCdfOptions()

    # Load CDF data
    try:
        cdf_data = _load_cdf_data(
            dt=dt,
            table_path=registration.table_path,
            cdf_options=resolved_cdf_options,
        )
    except ValueError as exc:
        raise ValueError(str(exc)) from exc

    # Convert to Arrow table
    cdf_table = _coerce_cdf_table(cdf_data)

    # Apply filter policy if provided
    cdf_table = _apply_filter_policy(
        ctx=ctx,
        cdf_table=cdf_table,
        filter_policy=registration.filter_policy,
    )

    # Register the CDF table
    ctx.register_record_batches(table_name, [list(cdf_table.to_batches())])

    # Record metadata
    metadata_obj = TableProviderMetadata(
        table_name=table_name,
        storage_location=registration.table_path,
        file_format="delta_cdf",
        metadata={
            "starting_version": str(resolved_cdf_options.starting_version),
            "ending_version": str(resolved_cdf_options.ending_version)
            if resolved_cdf_options.ending_version is not None
            else "latest",
            "filter_policy": (
                str(registration.filter_policy) if registration.filter_policy is not None else "all"
            ),
        },
    )
    record_table_provider_metadata(id(ctx), metadata=metadata_obj)

    return table_name


def register_delta_cdf_stream(ctx: SessionContext, stream: DeltaCdfStreamSpec) -> str:
    """Register a Delta CDF stream for a version range.

    This is a convenience function for the common case of reading CDF
    changes between two versions.

    Parameters
    ----------
    ctx : SessionContext
        DataFusion session context for table registration.
    stream : DeltaCdfStreamSpec
        Stream specification describing the version range and storage details.

    Returns
    -------
    str
        Name of the registered table.
    """
    cdf_options = DeltaCdfOptions(
        starting_version=stream.starting_version,
        ending_version=stream.ending_version,
    )
    registration = DeltaCdfRegistration(
        table_path=stream.table_path,
        table_name=stream.table_name,
        storage_options=stream.storage_options,
        cdf_options=cdf_options,
        filter_policy=stream.filter_policy,
    )
    return register_delta_cdf_table(ctx, registration)


def _resolve_table_name(table_name: str | None) -> str:
    if table_name is None:
        return f"__delta_cdf_{uuid.uuid4().hex}"
    return table_name


def _ensure_cdf_enabled(*, dt: DeltaTable, table_path: str) -> None:
    metadata = dt.metadata()
    configuration = metadata.configuration or {}
    cdf_enabled = configuration.get("delta.enableChangeDataFeed", "false").lower() == "true"
    if not cdf_enabled:
        msg = f"Change data feed is not enabled on Delta table at {table_path}"
        raise ValueError(msg)


def _load_cdf_data(
    *,
    dt: DeltaTable,
    table_path: str,
    cdf_options: DeltaCdfOptions,
) -> object:
    try:
        return dt.load_cdf(
            starting_version=cdf_options.starting_version,
            ending_version=cdf_options.ending_version,
            starting_timestamp=cdf_options.starting_timestamp,
            ending_timestamp=cdf_options.ending_timestamp,
            columns=cdf_options.columns,
        )
    except (OSError, RuntimeError, TypeError, ValueError) as exc:
        msg = f"Failed to load CDF from Delta table at {table_path}: {exc}"
        raise ValueError(msg) from exc


def _coerce_cdf_table(cdf_data: object) -> pa.Table:
    read_all = getattr(cdf_data, "read_all", None)
    if callable(read_all):
        return read_all()
    to_arrow = getattr(cdf_data, "to_arrow", None)
    if callable(to_arrow):
        result = to_arrow()
        if isinstance(result, pa.Table):
            return result
    if isinstance(cdf_data, pa.Table):
        return cdf_data
    msg = "CDF data could not be coerced to a PyArrow table."
    raise TypeError(msg)


def _apply_filter_policy(
    *,
    ctx: SessionContext,
    cdf_table: pa.Table,
    filter_policy: CdfFilterPolicy | None,
) -> pa.Table:
    if filter_policy is None:
        return cdf_table

    predicate = filter_policy.to_sql_predicate()
    if predicate is None:
        return cdf_table

    if predicate == "FALSE":
        return pa.table({}, schema=cdf_table.schema)

    temp_name = f"__temp_cdf_{uuid.uuid4().hex}"
    ctx.register_record_batches(temp_name, [list(cdf_table.to_batches())])
    try:
        filtered_df = ctx.sql(f"SELECT * FROM {temp_name} WHERE {predicate}")
        return filtered_df.to_arrow_table()
    finally:
        deregister = getattr(ctx, "deregister_table", None)
        if callable(deregister):
            with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
                deregister(temp_name)


__all__ = [
    "DeltaCdfRegistration",
    "DeltaCdfStreamSpec",
    "register_delta_cdf_stream",
    "register_delta_cdf_table",
]
