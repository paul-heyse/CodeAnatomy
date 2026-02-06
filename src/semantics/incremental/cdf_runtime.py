"""CDF runtime helpers for incremental pipelines."""

from __future__ import annotations

import time
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa

from datafusion_engine.arrow.coercion import coerce_table_to_storage, to_arrow_table
from datafusion_engine.dataset.registry import DatasetLocation, DatasetLocationOverrides
from datafusion_engine.delta.scan_config import resolve_delta_scan_options
from datafusion_engine.lineage.diagnostics import record_artifact
from datafusion_engine.session.facade import DataFusionExecutionFacade
from schema_spec.system import DeltaScanOptions
from semantics.incremental.cdf_cursors import CdfCursor, CdfCursorStore
from semantics.incremental.cdf_types import CdfFilterPolicy
from semantics.incremental.delta_context import DeltaAccessContext
from semantics.incremental.plan_bundle_exec import execute_df_to_table
from semantics.incremental.runtime import TempTableRegistry
from storage.deltalake import DeltaCdfOptions, StorageOptions
from utils.uuid_factory import uuid7_hex

if TYPE_CHECKING:
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from schema_spec.system import DeltaCdfPolicy
    from semantics.incremental.runtime import IncrementalRuntime


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


@dataclass(frozen=True)
class _CdfReadRecord:
    dataset_name: str
    dataset_path: str
    status: str
    inputs: CdfReadInputs | None
    state: _CdfReadState | None
    table: pa.Table | None
    error: str | None
    filter_policy: CdfFilterPolicy | None


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
    profile_location = runtime.profile.catalog_ops.dataset_location(dataset_name)
    resolved_store = context.resolve_storage(table_uri=str(path))
    resolved_storage = resolved_store.storage_options or {}
    resolved_log_storage = resolved_store.log_storage_options or {}
    resolved_scan = None
    cdf_policy = None
    if profile_location is not None and profile_location.format == "delta":
        if profile_location.storage_options:
            resolved_storage = profile_location.storage_options
        resolved_log_storage = (
            profile_location.resolved.delta_log_storage_options or resolved_log_storage
        )
        resolved_scan = resolve_delta_scan_options(
            profile_location,
            scan_policy=runtime.profile.policies.scan_policy,
        )
        cdf_policy = profile_location.resolved.delta_cdf_policy
    current_version = runtime.profile.delta_ops.delta_service().table_version(
        path=str(path),
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


def _is_cdf_start_version_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return "no starting version or timestamp provided for cdc" in message


def _read_cdf_table_fallback(
    state: _CdfReadState,
    *,
    filter_policy: CdfFilterPolicy | None,
) -> pa.Table:
    from deltalake import DeltaTable

    storage_options = dict(state.inputs.storage_options) if state.inputs.storage_options else None
    table = DeltaTable(str(state.inputs.path), storage_options=storage_options)
    reader = table.load_cdf(
        starting_version=state.cdf_options.starting_version,
        ending_version=state.cdf_options.ending_version,
        starting_timestamp=state.cdf_options.starting_timestamp,
        ending_timestamp=state.cdf_options.ending_timestamp,
        columns=state.cdf_options.columns,
        predicate=state.cdf_options.predicate,
        allow_out_of_range=state.cdf_options.allow_out_of_range,
    )
    # deltalake may return arro3-backed table/array objects; normalize to
    # pyarrow to keep downstream compute/filter code deterministic.
    resolved = coerce_table_to_storage(to_arrow_table(reader.read_all()))
    policy = filter_policy or CdfFilterPolicy.include_all()
    if "_change_type" not in resolved.column_names:
        return resolved
    allowed_types: list[str] = []
    if policy.include_insert:
        allowed_types.append("insert")
    if policy.include_update_postimage:
        allowed_types.append("update_postimage")
    if policy.include_delete:
        allowed_types.append("delete")
    if not allowed_types:
        return resolved.slice(0, 0)
    change_type_column = resolved["_change_type"]
    try:
        import pyarrow.compute as pc

        normalized_change_type = pc.cast(change_type_column, pa.string())
    except (pa.ArrowInvalid, pa.ArrowTypeError, NotImplementedError):
        normalized_change_type = pa.array(
            [None if value is None else str(value) for value in change_type_column.to_pylist()],
            type=pa.string(),
        )
    allowed_type_set = set(allowed_types)
    mask = pa.array(
        [
            value is not None and str(value) in allowed_type_set
            for value in normalized_change_type.to_pylist()
        ],
        type=pa.bool_(),
    )
    return resolved.filter(mask)


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

    Args:
        context: Description.
            dataset_path: Description.
            dataset_name: Description.
            cursor_store: Description.
            filter_policy: Description.

    Returns:
        CdfReadResult | None: Result.

    Raises:
        ValueError: If the operation cannot be completed.
    """
    profile = context.runtime.profile
    profile_location = profile.catalog_ops.dataset_location(dataset_name)
    cdf_policy = (
        profile_location.resolved.delta_cdf_policy if profile_location is not None else None
    )
    inputs = _resolve_cdf_inputs(context, dataset_path=dataset_path, dataset_name=dataset_name)
    if inputs is None:
        if cdf_policy is not None and cdf_policy.required:
            msg = f"Delta CDF is required for dataset {dataset_name!r} but is unavailable."
            _record_cdf_read(
                profile,
                record=_CdfReadRecord(
                    dataset_name=dataset_name,
                    dataset_path=dataset_path,
                    status="unavailable",
                    inputs=None,
                    state=None,
                    table=None,
                    error=msg,
                    filter_policy=filter_policy,
                ),
            )
            raise ValueError(msg)
        _record_cdf_read(
            profile,
            record=_CdfReadRecord(
                dataset_name=dataset_name,
                dataset_path=dataset_path,
                status="unavailable",
                inputs=None,
                state=None,
                table=None,
                error=None,
                filter_policy=filter_policy,
            ),
        )
        return None
    state = _prepare_cdf_read_state(
        context,
        dataset_name=dataset_name,
        inputs=inputs,
        cursor_store=cursor_store,
    )
    if state is None:
        _record_cdf_read(
            profile,
            record=_CdfReadRecord(
                dataset_name=dataset_name,
                dataset_path=dataset_path,
                status="no_changes",
                inputs=inputs,
                state=None,
                table=None,
                error=None,
                filter_policy=filter_policy,
            ),
        )
        return None
    with TempTableRegistry(state.runtime) as registry:
        cdf_name = f"__cdf_{uuid7_hex()}"
        try:
            overrides = None
            if state.inputs.scan_options is not None:
                from schema_spec.system import DeltaPolicyBundle

                overrides = DatasetLocationOverrides(
                    delta=DeltaPolicyBundle(scan=state.inputs.scan_options)
                )
            location = DatasetLocation(
                path=str(state.inputs.path),
                format="delta",
                storage_options=state.inputs.storage_options,
                delta_log_storage_options=state.inputs.log_storage_options,
                delta_cdf_options=state.cdf_options,
                overrides=overrides,
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
                _record_cdf_read(
                    profile,
                    record=_CdfReadRecord(
                        dataset_name=dataset_name,
                        dataset_path=dataset_path,
                        status="error",
                        inputs=inputs,
                        state=state,
                        table=None,
                        error=msg,
                        filter_policy=filter_policy,
                    ),
                )
                raise ValueError(msg) from exc
            _record_cdf_read(
                profile,
                record=_CdfReadRecord(
                    dataset_name=dataset_name,
                    dataset_path=dataset_path,
                    status="error",
                    inputs=inputs,
                    state=state,
                    table=None,
                    error=str(exc),
                    filter_policy=filter_policy,
                ),
            )
            return None
        registry.track(cdf_name)
        try:
            table = _read_cdf_table(
                state.runtime,
                table_name=cdf_name,
                filter_policy=filter_policy,
            )
            if "_change_type" not in table.column_names:
                # Degraded provider registration can return a base Delta dataset
                # without CDF virtual columns. Fall back to direct load_cdf().
                table = _read_cdf_table_fallback(
                    state,
                    filter_policy=filter_policy,
                )
        except Exception as exc:
            if not _is_cdf_start_version_error(exc):
                raise
            table = _read_cdf_table_fallback(
                state,
                filter_policy=filter_policy,
            )
    cursor_store.save_cursor(
        CdfCursor(dataset_name=dataset_name, last_version=state.current_version)
    )
    _record_cdf_read(
        profile,
        record=_CdfReadRecord(
            dataset_name=dataset_name,
            dataset_path=dataset_path,
            status="read",
            inputs=inputs,
            state=state,
            table=table,
            error=None,
            filter_policy=filter_policy,
        ),
    )
    return CdfReadResult(table=table, updated_version=state.current_version)


def _record_cdf_read(
    profile: DataFusionRuntimeProfile | None,
    *,
    record: _CdfReadRecord,
) -> None:
    change_counts: dict[str, int] | None = None
    if record.table is not None:
        change_counts = _cdf_change_counts(record.table)
    payload = {
        "event_time_unix_ms": int(time.time() * 1000),
        "dataset": record.dataset_name,
        "path": record.dataset_path,
        "status": record.status,
        "required": (
            record.inputs.cdf_policy.required
            if record.inputs is not None and record.inputs.cdf_policy is not None
            else False
        ),
        "current_version": record.inputs.current_version if record.inputs else None,
        "starting_version": (
            record.state.cdf_options.starting_version if record.state is not None else None
        ),
        "ending_version": (
            record.state.cdf_options.ending_version if record.state is not None else None
        ),
        "rows": record.table.num_rows if record.table is not None else 0,
        "change_counts": change_counts,
        "filter_policy": _cdf_filter_payload(record.filter_policy),
        "error": record.error,
    }
    record_artifact(profile, "incremental_cdf_read_v1", payload)


def _cdf_filter_payload(filter_policy: CdfFilterPolicy | None) -> dict[str, bool] | None:
    if filter_policy is None:
        return None
    return {
        "include_insert": filter_policy.include_insert,
        "include_update_postimage": filter_policy.include_update_postimage,
        "include_delete": filter_policy.include_delete,
    }


def _cdf_change_counts(table: pa.Table) -> dict[str, int]:
    if "_change_type" not in table.column_names:
        return {}
    values = table["_change_type"].to_pylist()
    counts = Counter(value for value in values if value is not None)
    return {str(key): int(value) for key, value in counts.items()}


__all__ = ["CdfReadResult", "read_cdf_changes"]
