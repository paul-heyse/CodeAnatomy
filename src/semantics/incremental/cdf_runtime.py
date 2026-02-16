"""CDF runtime helpers for incremental pipelines."""

from __future__ import annotations

import logging
import time
from collections import Counter
from dataclasses import dataclass
from typing import TYPE_CHECKING, NoReturn

import pyarrow as pa

from datafusion_engine.dataset.registry import (
    DatasetLocation,
    DatasetLocationOverrides,
)
from datafusion_engine.delta.scan_config import resolve_delta_scan_options
from datafusion_engine.lineage.diagnostics import record_artifact
from schema_spec.dataset_spec import DeltaScanOptions
from semantics.incremental.cdf_core import CanonicalCdfReadRequest, read_cdf_table
from semantics.incremental.cdf_cursors import CdfCursor, CdfCursorStore
from semantics.incremental.cdf_types import CdfFilterPolicy
from semantics.incremental.delta_context import DeltaAccessContext
from semantics.incremental.delta_port import DeltaServiceCdfPort
from semantics.incremental.plan_bundle_exec import execute_df_to_table
from semantics.incremental.runtime import TempTableRegistry
from storage.deltalake import DeltaCdfOptions, StorageOptions
from utils.uuid_factory import uuid7_hex

if TYPE_CHECKING:
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from schema_spec.dataset_spec import DeltaCdfPolicy
    from semantics.incremental.runtime import IncrementalRuntime


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RuntimeCdfReadResult:
    """Result for a Delta CDF read."""

    table: pa.Table
    updated_version: int


@dataclass(frozen=True)
class CdfReadInputs:
    """Resolved inputs for a Delta CDF read."""

    path: str
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


def _profile_dataset_location(
    context: DeltaAccessContext,
    dataset_name: str,
) -> DatasetLocation | None:
    return context.dataset_resolver.location(dataset_name)


def _resolve_cdf_inputs(
    context: DeltaAccessContext,
    *,
    dataset_path: str,
    dataset_name: str,
) -> CdfReadInputs | None:
    runtime = context.runtime
    profile_location = _profile_dataset_location(context, dataset_name)
    resolved_store = context.resolve_storage(table_uri=dataset_path)
    resolved_storage = resolved_store.storage_options or {}
    resolved_log_storage = resolved_store.log_storage_options or {}
    resolved_scan = None
    cdf_policy = None
    if profile_location is not None and profile_location.format == "delta":
        if profile_location.storage_options:
            resolved_storage = profile_location.storage_options
        resolved_log_storage = (
            profile_location.resolved_delta_log_storage_options or resolved_log_storage
        )
        resolved_scan = resolve_delta_scan_options(
            profile_location,
            scan_policy=runtime.scan_policy(),
        )
        cdf_policy = profile_location.delta_cdf_policy
    current_version = runtime.delta_service().table_version(
        path=dataset_path,
        storage_options=resolved_storage,
        log_storage_options=resolved_log_storage,
    )
    if current_version is None:
        return None
    return CdfReadInputs(
        path=dataset_path,
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
) -> pa.Table:
    ctx = runtime.session_runtime().ctx
    df = ctx.table(table_name)
    return execute_df_to_table(runtime, df, view_name=f"incremental_cdf::{table_name}")


def _is_cdf_start_version_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return "no starting version or timestamp provided for cdc" in message


def _read_cdf_table_fallback(
    state: _CdfReadState,
) -> pa.Table:
    start_version = state.cdf_options.starting_version
    if start_version is None:
        msg = "CDF fallback requires starting_version."
        raise ValueError(msg)
    canonical = read_cdf_table(
        request=CanonicalCdfReadRequest(
            table_path=state.inputs.path,
            start_version=start_version,
            end_version=state.cdf_options.ending_version,
            storage_options=state.inputs.storage_options,
            log_storage_options=state.inputs.log_storage_options,
            columns=state.cdf_options.columns,
            predicate=state.cdf_options.predicate,
            allow_out_of_range=state.cdf_options.allow_out_of_range,
        ),
        port=DeltaServiceCdfPort(state.runtime.delta_service()),
    )
    if canonical is None:
        msg = f"Delta CDF fallback read failed for {state.inputs.path!r}."
        raise ValueError(msg)
    return canonical.table


def _prepare_cdf_read_state(
    context: DeltaAccessContext,
    *,
    dataset_name: str,
    inputs: CdfReadInputs,
    cursor_store: CdfCursorStore,
    filter_policy: CdfFilterPolicy | None,
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
    policy = filter_policy or CdfFilterPolicy.include_all()
    cdf_options = DeltaCdfOptions(
        starting_version=starting_version,
        ending_version=current_version,
        predicate=policy.to_sql_predicate(),
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


def _record_unavailable_cdf_read(
    profile: DataFusionRuntimeProfile | None,
    *,
    dataset_name: str,
    dataset_path: str,
    required: bool,
    filter_policy: CdfFilterPolicy | None,
) -> None:
    error = (
        f"Delta CDF is required for dataset {dataset_name!r} but is unavailable."
        if required
        else None
    )
    _record_cdf_read(
        profile,
        record=_CdfReadRecord(
            dataset_name=dataset_name,
            dataset_path=dataset_path,
            status="unavailable",
            inputs=None,
            state=None,
            table=None,
            error=error,
            filter_policy=filter_policy,
        ),
    )
    if error is not None:
        raise ValueError(error)


def _register_cdf_dataset(
    state: _CdfReadState,
    *,
    registry: TempTableRegistry,
) -> str:
    cdf_name = f"__cdf_{uuid7_hex()}"
    overrides = None
    if state.inputs.scan_options is not None:
        from schema_spec.dataset_spec import DeltaPolicyBundle

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
    result = state.runtime.registry_facade.register_dataset(
        name=cdf_name,
        location=location,
    )
    if not result.success:
        msg = result.error or f"Failed to register CDF dataset {cdf_name!r}."
        raise ValueError(msg)
    registry.track(cdf_name)
    return cdf_name


def _raise_non_cdf_start_error(exc: Exception) -> NoReturn:
    raise exc


def _load_cdf_table(
    state: _CdfReadState,
    *,
    registry: TempTableRegistry,
) -> pa.Table:
    cdf_name = _register_cdf_dataset(state, registry=registry)
    try:
        table = _read_cdf_table(
            state.runtime,
            table_name=cdf_name,
        )
        if "_change_type" not in table.column_names:
            # Degraded provider registration can return a base Delta dataset
            # without CDF virtual columns. Fall back to direct load_cdf().
            logger.debug(
                "DataFusion CDF read returned no _change_type for %s; falling back to DeltaTable.load_cdf.",
                state.inputs.path,
            )
            table = _read_cdf_table_fallback(
                state,
            )
    except (RuntimeError, ValueError) as exc:
        if not _is_cdf_start_version_error(exc):
            _raise_non_cdf_start_error(exc)
        logger.debug(
            "DataFusion CDF read failed for %s (%s); falling back to DeltaTable.load_cdf.",
            state.inputs.path,
            exc,
        )
        table = _read_cdf_table_fallback(
            state,
        )
    return table


def read_cdf_changes(
    context: DeltaAccessContext,
    *,
    dataset_path: str,
    dataset_name: str,
    cursor_store: CdfCursorStore,
    filter_policy: CdfFilterPolicy | None = None,
) -> RuntimeCdfReadResult | None:
    """Read Delta CDF changes using the shared incremental runtime.

    Args:
        context: Delta access context.
        dataset_path: Dataset table path.
        dataset_name: Dataset logical name.
        cursor_store: CDF cursor state store.
        filter_policy: Optional CDF filter policy.

    Returns:
        RuntimeCdfReadResult | None: Result.

    Raises:
        ValueError: If CDF state cannot be initialized.
    """
    profile = context.runtime.profile
    profile_location = _profile_dataset_location(context, dataset_name)
    cdf_policy = profile_location.delta_cdf_policy if profile_location is not None else None
    inputs = _resolve_cdf_inputs(context, dataset_path=dataset_path, dataset_name=dataset_name)
    if inputs is None:
        _record_unavailable_cdf_read(
            profile,
            dataset_name=dataset_name,
            dataset_path=dataset_path,
            required=bool(cdf_policy is not None and cdf_policy.required),
            filter_policy=filter_policy,
        )
        return None
    state = _prepare_cdf_read_state(
        context,
        dataset_name=dataset_name,
        inputs=inputs,
        cursor_store=cursor_store,
        filter_policy=filter_policy,
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
        try:
            table = _load_cdf_table(
                state,
                registry=registry,
            )
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
    return RuntimeCdfReadResult(table=table, updated_version=state.current_version)


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
    from serde_artifact_specs import INCREMENTAL_CDF_READ_SPEC

    record_artifact(profile, INCREMENTAL_CDF_READ_SPEC, payload)


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


__all__ = ["RuntimeCdfReadResult", "read_cdf_changes"]
