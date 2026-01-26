"""Shared Delta access context helpers for incremental pipelines."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import pyarrow as pa

from datafusion_engine.execution_facade import DataFusionExecutionFacade
from ibis_engine.registry import DatasetLocation
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
    facade = DataFusionExecutionFacade(ctx=ctx, runtime_profile=context.runtime.profile)
    location = DatasetLocation(
        path=str(path),
        format="delta",
        storage_options=context.storage.storage_options or {},
        delta_log_storage_options=context.storage.log_storage_options or {},
        delta_version=version,
        delta_timestamp=timestamp,
    )
    with TempTableRegistry(ctx) as registry:
        facade.register_dataset(name=name, location=location)
        registry.track(name)
        backend = context.runtime.ibis_backend()
        expr = backend.table(name)
        return ibis_expr_to_table(expr, runtime=context.runtime, name=name)


__all__ = [
    "DeltaAccessContext",
    "DeltaStorageOptions",
    "read_delta_table_via_facade",
]
