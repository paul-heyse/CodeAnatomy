"""Shared Delta access context helpers for incremental pipelines."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import pyarrow as pa

from ibis_engine.registry import (
    DatasetLocation,
    resolve_delta_log_storage_options,
    resolve_delta_scan_options,
)
from ibis_engine.sources import IbisDeltaReadOptions, read_delta_ibis
from incremental.ibis_exec import ibis_expr_to_table
from incremental.runtime import IncrementalRuntime, TempTableRegistry
from storage.deltalake import StorageOptions


@dataclass(frozen=True)
class DeltaStorageOptions:
    """Storage options for Delta table access."""

    storage_options: StorageOptions | None = None
    log_storage_options: StorageOptions | None = None


@dataclass(frozen=True)
class DeltaAccessContext:
    """Delta access context bundling runtime and storage options."""

    runtime: IncrementalRuntime
    storage: DeltaStorageOptions = field(default_factory=DeltaStorageOptions)

    def storage_kwargs(self) -> dict[str, StorageOptions | None]:
        """Return storage option kwargs for Delta helpers.

        Returns
        -------
        dict[str, StorageOptions | None]
            Storage option kwargs for Delta helpers.
        """
        return {
            "storage_options": self.storage.storage_options,
            "log_storage_options": self.storage.log_storage_options,
        }


def read_delta_table_via_facade(
    context: DeltaAccessContext,
    *,
    path: str | Path,
    name: str,
    version: int | None = None,
    timestamp: str | None = None,
) -> pa.Table:
    """Read a Delta table via the DataFusion execution facade.

    Returns
    -------
    pyarrow.Table
        Materialized table from the Delta provider.
    """
    ctx = context.runtime.session_context()
    profile_location = context.runtime.profile.dataset_location(name)
    resolved_storage = context.storage.storage_options or {}
    resolved_log_storage = context.storage.log_storage_options or {}
    resolved_scan = None
    resolved_version = version
    resolved_timestamp = timestamp
    if profile_location is not None and profile_location.format == "delta":
        if profile_location.storage_options:
            resolved_storage = profile_location.storage_options
        resolved_log_storage = (
            resolve_delta_log_storage_options(profile_location) or resolved_log_storage
        )
        resolved_scan = resolve_delta_scan_options(profile_location)
        if resolved_version is None:
            resolved_version = profile_location.delta_version
        if resolved_timestamp is None:
            resolved_timestamp = profile_location.delta_timestamp
    location = DatasetLocation(
        path=str(path),
        format="delta",
        storage_options=resolved_storage,
        delta_log_storage_options=resolved_log_storage,
        delta_version=resolved_version,
        delta_timestamp=resolved_timestamp,
        delta_scan=resolved_scan,
    )
    with TempTableRegistry(ctx) as registry:
        expr = read_delta_ibis(
            context.runtime.ibis_backend(),
            str(path),
            options=IbisDeltaReadOptions(
                table_name=name,
                storage_options=location.storage_options,
                log_storage_options=location.delta_log_storage_options,
                delta_scan=location.delta_scan,
                version=location.delta_version,
                timestamp=location.delta_timestamp,
            ),
        )
        registry.track(name)
        return ibis_expr_to_table(expr, runtime=context.runtime, name=name)


__all__ = [
    "DeltaAccessContext",
    "DeltaStorageOptions",
    "read_delta_table_via_facade",
]
