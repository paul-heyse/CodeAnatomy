"""CDF runtime helpers for incremental pipelines."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, cast

import pyarrow as pa

from datafusion_engine.execution_facade import DataFusionExecutionFacade
from ibis_engine.registry import (
    DatasetLocation,
    resolve_delta_log_storage_options,
    resolve_delta_scan_options,
)
from incremental.cdf_cursors import CdfCursor, CdfCursorStore
from incremental.cdf_filters import CdfFilterPolicy
from incremental.delta_context import DeltaAccessContext
from incremental.ibis_exec import ibis_expr_to_table
from incremental.runtime import TempTableRegistry
from schema_spec.system import DeltaScanOptions
from storage.deltalake import DeltaCdfOptions, StorageOptions, delta_table_version

if TYPE_CHECKING:
    from ibis.expr.types import BooleanValue

    from incremental.runtime import IncrementalRuntime


@dataclass(frozen=True)
class CdfReadResult:
    """Result for a Delta CDF read."""

    table: pa.Table
    updated_version: int


@dataclass(frozen=True)
class CdfReadInputs:
    """Resolved inputs for a Delta CDF read."""

    path: Path
    current_version: int
    storage_options: StorageOptions
    log_storage_options: StorageOptions
    scan_options: DeltaScanOptions | None


def _resolve_cdf_inputs(
    context: DeltaAccessContext,
    *,
    dataset_path: str,
    dataset_name: str,
) -> CdfReadInputs | None:
    path = Path(dataset_path)
    if not path.exists():
        return None
    runtime = context.runtime
    profile_location = runtime.profile.dataset_location(dataset_name)
    resolved_storage = context.storage.storage_options or {}
    resolved_log_storage = context.storage.log_storage_options or {}
    resolved_scan = None
    if profile_location is not None and profile_location.format == "delta":
        if profile_location.storage_options:
            resolved_storage = profile_location.storage_options
        resolved_log_storage = (
            resolve_delta_log_storage_options(profile_location) or resolved_log_storage
        )
        resolved_scan = resolve_delta_scan_options(profile_location)
    current_version = delta_table_version(
        str(path),
        storage_options=resolved_storage,
        log_storage_options=resolved_log_storage,
    )
    if current_version is None:
        return None
    return CdfReadInputs(
        path=path,
        current_version=current_version,
        storage_options=resolved_storage,
        log_storage_options=resolved_log_storage,
        scan_options=resolved_scan,
    )


def _read_cdf_table(
    runtime: IncrementalRuntime,
    *,
    table_name: str,
    filter_policy: CdfFilterPolicy | None,
) -> pa.Table:
    backend = runtime.ibis_backend()
    expr = backend.table(table_name)
    predicate = (filter_policy or CdfFilterPolicy.include_all()).to_ibis_predicate(expr)
    if predicate is not None:
        expr = expr.filter(cast("BooleanValue", predicate))
    return ibis_expr_to_table(expr, runtime=runtime, name="cdf_changes")


def read_cdf_changes(
    context: DeltaAccessContext,
    *,
    dataset_path: str,
    dataset_name: str,
    cursor_store: CdfCursorStore,
    filter_policy: CdfFilterPolicy | None = None,
) -> CdfReadResult | None:
    """Read Delta CDF changes using the shared incremental runtime.

    Returns
    -------
    CdfReadResult | None
        CDF read result when changes are available, otherwise ``None``.
    """
    inputs = _resolve_cdf_inputs(context, dataset_path=dataset_path, dataset_name=dataset_name)
    if inputs is None:
        return None
    runtime = context.runtime
    current_version = inputs.current_version
    cursor = cursor_store.load_cursor(dataset_name)
    if cursor is None:
        cursor_store.save_cursor(CdfCursor(dataset_name=dataset_name, last_version=current_version))
        return None
    if cursor.last_version >= current_version:
        return None
    starting_version = cursor.last_version + 1
    cdf_options = DeltaCdfOptions(
        starting_version=starting_version,
        ending_version=current_version,
    )
    ctx = runtime.session_context()
    with TempTableRegistry(ctx) as registry:
        cdf_name = f"__cdf_{uuid.uuid4().hex}"
        try:
            location = DatasetLocation(
                path=str(inputs.path),
                format="delta",
                storage_options=inputs.storage_options,
                delta_log_storage_options=inputs.log_storage_options,
                delta_cdf_options=cdf_options,
                delta_scan=inputs.scan_options,
                datafusion_provider="delta_cdf",
            )
            facade = DataFusionExecutionFacade(ctx=ctx, runtime_profile=runtime.profile)
            _ = facade.register_dataset(name=cdf_name, location=location)
        except ValueError:
            return None
        registry.track(cdf_name)
        table = _read_cdf_table(runtime, table_name=cdf_name, filter_policy=filter_policy)
    cursor_store.save_cursor(CdfCursor(dataset_name=dataset_name, last_version=current_version))
    return CdfReadResult(table=table, updated_version=current_version)


__all__ = ["CdfReadResult", "read_cdf_changes"]
