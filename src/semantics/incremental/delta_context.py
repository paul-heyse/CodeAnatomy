"""Shared Delta access context helpers for incremental pipelines."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa
from datafusion.dataframe import DataFrame

from datafusion_engine.dataset.registry import (
    DatasetLocation,
    DatasetLocationOverrides,
    resolve_dataset_location,
)
from datafusion_engine.delta.maintenance import (
    DeltaMaintenancePlanInput,
    resolve_delta_maintenance_plan,
    run_delta_maintenance,
)
from datafusion_engine.delta.service import delta_service_for_profile
from datafusion_engine.delta.store_policy import resolve_delta_store_policy
from datafusion_engine.session.facade import DataFusionExecutionFacade
from semantics.incremental.plan_bundle_exec import execute_df_to_table
from semantics.incremental.runtime import IncrementalRuntime, TempTableRegistry
from storage.deltalake import StorageOptions

if TYPE_CHECKING:
    from semantics.program_manifest import ManifestDatasetResolver


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

    @property
    def dataset_resolver(self) -> ManifestDatasetResolver:
        """Return the dataset resolver from the incremental runtime.

        Returns:
        -------
        ManifestDatasetResolver
            Dataset resolver from manifest bindings.
        """
        return self.runtime.dataset_resolver

    def storage_kwargs(self) -> dict[str, StorageOptions | None]:
        """Return storage option kwargs for Delta helpers.

        Returns:
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

        Returns:
        -------
        DeltaStorageOptions
            Storage option bundle after applying policy overrides.
        """
        policy = self.runtime.profile.policies.delta_store_policy
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

    Returns:
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

    Returns:
    -------
    DataFrame
        DataFusion DataFrame for the registered Delta table.
    """
    profile_location = context.dataset_resolver.location(name)
    resolved_store = context.resolve_storage(table_uri=str(path))
    resolved_storage = resolved_store.storage_options or {}
    resolved_log_storage = resolved_store.log_storage_options or {}
    resolved_scan = None
    resolved_version = version
    resolved_timestamp = timestamp
    dataset_spec = None
    table_spec = None
    if profile_location is not None and profile_location.format == "delta":
        resolved_location = resolve_dataset_location(profile_location)
        if profile_location.storage_options:
            resolved_storage = profile_location.storage_options
        resolved_log_storage = resolved_location.delta_log_storage_options or resolved_log_storage
        resolved_scan = resolved_location.delta_scan
        if resolved_version is None:
            resolved_version = profile_location.delta_version
        if resolved_timestamp is None:
            resolved_timestamp = profile_location.delta_timestamp
        dataset_spec = resolved_location.dataset_spec
        table_spec = resolved_location.table_spec
    overrides = None
    if resolved_scan is not None or table_spec is not None:
        from schema_spec.system import DeltaPolicyBundle

        delta_bundle = DeltaPolicyBundle(scan=resolved_scan) if resolved_scan is not None else None
        overrides = DatasetLocationOverrides(delta=delta_bundle, table_spec=table_spec)
    location = DatasetLocation(
        path=str(path),
        format="delta",
        storage_options=resolved_storage,
        delta_log_storage_options=resolved_log_storage,
        delta_version=resolved_version,
        delta_timestamp=resolved_timestamp,
        dataset_spec=dataset_spec,
        overrides=overrides,
    )
    return DataFusionExecutionFacade(
        ctx=context.runtime.session_runtime().ctx,
        runtime_profile=context.runtime.profile,
    ).register_dataset(
        name=name,
        location=location,
        overwrite=True,
    )


def run_delta_maintenance_if_configured(
    context: DeltaAccessContext,
    *,
    table_uri: str,
    dataset_name: str | None,
    storage_options: StorageOptions | None,
    log_storage_options: StorageOptions | None,
) -> None:
    """Run Delta maintenance when policies are configured.

    Parameters
    ----------
    context
        Delta access context with runtime profile.
    table_uri
        Delta table URI for maintenance actions.
    dataset_name
        Optional dataset name for policy resolution.
    storage_options
        Storage options for the Delta table.
    log_storage_options
        Log storage options for the Delta table.
    """
    runtime_profile = context.runtime.profile
    if dataset_name is None:
        dataset_location = None
    else:
        dataset_location = context.dataset_resolver.location(dataset_name)
    plan = resolve_delta_maintenance_plan(
        DeltaMaintenancePlanInput(
            dataset_location=dataset_location,
            table_uri=table_uri,
            dataset_name=dataset_name,
            storage_options=storage_options,
            log_storage_options=log_storage_options,
            delta_version=delta_service_for_profile(runtime_profile).table_version(
                path=table_uri,
                storage_options=storage_options,
                log_storage_options=log_storage_options,
            ),
            delta_timestamp=None,
            feature_gate=dataset_location.resolved.delta_feature_gate
            if dataset_location is not None
            else None,
            policy=None,
        )
    )
    if plan is None:
        return
    run_delta_maintenance(
        context.runtime.session_runtime().ctx,
        plan=plan,
        runtime_profile=runtime_profile,
    )


__all__ = [
    "DeltaAccessContext",
    "DeltaStorageOptions",
    "read_delta_table_via_facade",
    "register_delta_df",
    "run_delta_maintenance_if_configured",
]
