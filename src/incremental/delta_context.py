"""Shared Delta access context helpers for incremental pipelines."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import pyarrow as pa
from datafusion.dataframe import DataFrame

from datafusion_engine.dataset_registry import (
    DatasetLocation,
    resolve_delta_log_storage_options,
    resolve_delta_scan_options,
)
from datafusion_engine.delta_store_policy import resolve_delta_store_policy
from datafusion_engine.registry_bridge import register_dataset_df
from incremental.plan_bundle_exec import execute_df_to_table
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

    def resolve_storage(self, *, table_uri: str) -> DeltaStorageOptions:
        """Return effective storage options merged with runtime policy.

        Returns
        -------
        DeltaStorageOptions
            Storage option bundle after applying policy overrides.
        """
        policy = self.runtime.profile.delta_store_policy
        if policy is None:
            return self.storage
        storage, log_storage = resolve_delta_store_policy(
            table_uri=table_uri,
            policy=policy,
            storage_options=self.storage.storage_options,
            log_storage_options=self.storage.log_storage_options,
        )
        return DeltaStorageOptions(
            storage_options=storage or None,
            log_storage_options=log_storage or None,
        )


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
    with TempTableRegistry(context.runtime) as registry:
        df = register_delta_df(
            context,
            path=path,
            name=name,
            version=version,
            timestamp=timestamp,
        )
        registry.track(name)
        return execute_df_to_table(
            context.runtime,
            df,
            view_name=f"incremental_delta_read::{name}",
        )


def register_delta_df(
    context: DeltaAccessContext,
    *,
    path: str | Path,
    name: str,
    version: int | None = None,
    timestamp: str | None = None,
) -> DataFrame:
    """Register a Delta table in DataFusion and return a DataFrame.

    Returns
    -------
    DataFrame
        DataFusion DataFrame for the registered Delta table.
    """
    profile_location = context.runtime.profile.dataset_location(name)
    resolved_store = context.resolve_storage(table_uri=str(path))
    resolved_storage = resolved_store.storage_options or {}
    resolved_log_storage = resolved_store.log_storage_options or {}
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
    return register_dataset_df(
        context.runtime.session_runtime().ctx,
        name=name,
        location=location,
        runtime_profile=context.runtime.profile,
    )


__all__ = [
    "DeltaAccessContext",
    "DeltaStorageOptions",
    "read_delta_table_via_facade",
    "register_delta_df",
]
