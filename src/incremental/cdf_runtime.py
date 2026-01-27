"""CDF runtime helpers for incremental pipelines."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa

from datafusion_engine.dataset_registry import (
    DatasetLocation,
    resolve_delta_cdf_policy,
    resolve_delta_log_storage_options,
    resolve_delta_scan_options,
)
from datafusion_engine.execution_facade import DataFusionExecutionFacade
from incremental.cdf_cursors import CdfCursor, CdfCursorStore
from incremental.cdf_filters import CdfFilterPolicy
from incremental.delta_context import DeltaAccessContext
from incremental.plan_bundle_exec import execute_df_to_table
from incremental.runtime import TempTableRegistry
from schema_spec.system import DeltaScanOptions
from storage.deltalake import DeltaCdfOptions, StorageOptions, delta_table_version

if TYPE_CHECKING:
    from incremental.runtime import IncrementalRuntime
    from schema_spec.system import DeltaCdfPolicy


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
    cdf_policy: DeltaCdfPolicy | None


@dataclass(frozen=True)
class _CdfReadState:
    """Prepared CDF read inputs derived from cursors and versions."""

    runtime: IncrementalRuntime
    inputs: CdfReadInputs
    current_version: int
    cdf_options: DeltaCdfOptions


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
    resolved_store = context.resolve_storage(table_uri=str(path))
    resolved_storage = resolved_store.storage_options or {}
    resolved_log_storage = resolved_store.log_storage_options or {}
    resolved_scan = None
    cdf_policy = None
    if profile_location is not None and profile_location.format == "delta":
        if profile_location.storage_options:
            resolved_storage = profile_location.storage_options
        resolved_log_storage = (
            resolve_delta_log_storage_options(profile_location) or resolved_log_storage
        )
        resolved_scan = resolve_delta_scan_options(profile_location)
        cdf_policy = resolve_delta_cdf_policy(profile_location)
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
        cdf_policy=cdf_policy,
    )


def _read_cdf_table(
    runtime: IncrementalRuntime,
    *,
    table_name: str,
    filter_policy: CdfFilterPolicy | None,
) -> pa.Table:
    ctx = runtime.session_runtime().ctx
    df = ctx.table(table_name)
    predicate = (filter_policy or CdfFilterPolicy.include_all()).to_datafusion_predicate()
    if predicate is not None:
        df = df.filter(predicate)
    return execute_df_to_table(runtime, df, view_name=f"incremental_cdf::{table_name}")


def _prepare_cdf_read_state(
    context: DeltaAccessContext,
    *,
    dataset_name: str,
    inputs: CdfReadInputs,
    cursor_store: CdfCursorStore,
) -> _CdfReadState | None:
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
        allow_out_of_range=inputs.cdf_policy.allow_out_of_range
        if inputs.cdf_policy is not None
        else False,
    )
    return _CdfReadState(
        runtime=runtime,
        inputs=inputs,
        current_version=current_version,
        cdf_options=cdf_options,
    )


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

    Raises
    ------
    ValueError
        Raised when Delta CDF is required but unavailable or registration fails.
    """
    profile_location = context.runtime.profile.dataset_location(dataset_name)
    cdf_policy = resolve_delta_cdf_policy(profile_location) if profile_location else None
    inputs = _resolve_cdf_inputs(context, dataset_path=dataset_path, dataset_name=dataset_name)
    if inputs is None:
        if cdf_policy is not None and cdf_policy.required:
            msg = f"Delta CDF is required for dataset {dataset_name!r} but is unavailable."
            raise ValueError(msg)
        return None
    state = _prepare_cdf_read_state(
        context,
        dataset_name=dataset_name,
        inputs=inputs,
        cursor_store=cursor_store,
    )
    if state is None:
        return None
    with TempTableRegistry(state.runtime) as registry:
        cdf_name = f"__cdf_{uuid.uuid4().hex}"
        try:
            location = DatasetLocation(
                path=str(state.inputs.path),
                format="delta",
                storage_options=state.inputs.storage_options,
                delta_log_storage_options=state.inputs.log_storage_options,
                delta_cdf_options=state.cdf_options,
                delta_scan=state.inputs.scan_options,
                datafusion_provider="delta_cdf",
            )
            runtime_profile = state.runtime.profile
            ctx = runtime_profile.session_runtime().ctx
            facade = DataFusionExecutionFacade(ctx=ctx, runtime_profile=runtime_profile)
            _ = facade.register_dataset(name=cdf_name, location=location)
        except ValueError as exc:
            if state.inputs.cdf_policy is not None and state.inputs.cdf_policy.required:
                msg = (
                    f"Delta CDF provider registration failed for required dataset {dataset_name!r}."
                )
                raise ValueError(msg) from exc
            return None
        registry.track(cdf_name)
        table = _read_cdf_table(state.runtime, table_name=cdf_name, filter_policy=filter_policy)
    cursor_store.save_cursor(
        CdfCursor(dataset_name=dataset_name, last_version=state.current_version)
    )
    return CdfReadResult(table=table, updated_version=state.current_version)


__all__ = ["CdfReadResult", "read_cdf_changes"]
