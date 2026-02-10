"""Unified helpers for executing DataFusion plan bundles."""

from __future__ import annotations

import importlib
import time
from collections.abc import AsyncIterator, Mapping
from typing import TYPE_CHECKING

import pyarrow as pa
from datafusion import DataFrameWriteOptions, SessionContext, col

from datafusion_engine.dataset.resolution import apply_scan_unit_overrides
from datafusion_engine.plan.bundle_artifact import DataFusionPlanArtifact
from datafusion_engine.plan.result_types import (
    PlanEmissionOptions,
    PlanExecutionOptions,
    PlanExecutionResult,
    PlanScanOverrides,
)

if TYPE_CHECKING:
    from datafusion.dataframe import DataFrame

    from schema_spec.policies import DataFusionWritePolicy


# Type definitions have been moved to result_types.py to break the circular
# dependency between this module and facade.py. They are imported above and
# re-exported via __all__ for backward compatibility.


def execute_plan_artifact(
    ctx: SessionContext,
    plan_bundle: DataFusionPlanArtifact,
    *,
    options: PlanExecutionOptions | None = None,
) -> PlanExecutionResult:
    """Execute a plan bundle with optional scan overrides and telemetry.

    Parameters
    ----------
    ctx
        DataFusion session context used for execution.
    plan_bundle
        Plan bundle to execute.
    options
        Optional execution options controlling overrides, artifacts, and telemetry.

    Returns:
    -------
    PlanExecutionResult
        Execution result payload with artifacts and telemetry.
    """
    resolved_options = options or PlanExecutionOptions()
    scan_options = resolved_options.scan
    resolved_units = tuple(scan_options.scan_units)
    resolved_keys = (
        tuple(scan_options.scan_keys)
        if scan_options.scan_keys is not None
        else tuple(unit.key for unit in resolved_units)
    )
    if (
        scan_options.apply_scan_overrides
        and resolved_units
        and resolved_options.runtime_profile is not None
    ):
        apply_scan_unit_overrides(
            ctx,
            scan_units=resolved_units,
            runtime_profile=resolved_options.runtime_profile,
            dataset_resolver=resolved_options.dataset_resolver,
        )
    # Lazy import to avoid circular dependency with facade.py
    from datafusion_engine.session.facade import DataFusionExecutionFacade

    start = time.perf_counter()
    result = DataFusionExecutionFacade(
        ctx=ctx,
        runtime_profile=resolved_options.runtime_profile,
    ).execute_plan_artifact(
        plan_bundle,
        view_name=resolved_options.view_name,
        scan_units=resolved_units,
        scan_keys=resolved_keys,
    )
    telemetry = _telemetry_payload(
        start,
        emit_telemetry=resolved_options.emit.emit_telemetry,
    )
    return PlanExecutionResult(
        plan_bundle=plan_bundle,
        execution_result=result,
        output=result.dataframe,
        artifacts=plan_bundle.artifacts if resolved_options.emit.emit_artifacts else None,
        telemetry=telemetry,
        scan_units=resolved_units,
        scan_keys=resolved_keys,
    )


def _telemetry_payload(start_time: float, *, emit_telemetry: bool) -> dict[str, float]:
    if not emit_telemetry:
        return {}
    duration_ms = (time.perf_counter() - start_time) * 1000.0
    return {"duration_ms": duration_ms}


def _internal_substrait_logical_plan(
    ctx: SessionContext,
    payload: bytes,
) -> object | None:
    """Build a logical plan via ``datafusion._internal.substrait`` when available.

    Returns:
    -------
    object | None
        Decoded logical plan, or ``None`` when internal APIs are unavailable.

    Raises:
        ValueError: If internal Substrait replay fails at runtime.
    """
    try:
        datafusion_internal = importlib.import_module("datafusion._internal")
    except ImportError:
        return None

    internal_substrait = getattr(datafusion_internal, "substrait", None)
    internal_serde = getattr(internal_substrait, "Serde", None)
    internal_consumer = getattr(internal_substrait, "Consumer", None)
    deserialize = getattr(internal_serde, "deserialize_bytes", None)
    from_substrait = getattr(internal_consumer, "from_substrait_plan", None)
    if not callable(deserialize) or not callable(from_substrait):
        return None
    try:
        decoded_plan = deserialize(payload)
        raw_logical_plan = from_substrait(ctx.ctx, decoded_plan)
        from datafusion.plan import LogicalPlan as DataFusionLogicalPlan

        return DataFusionLogicalPlan(raw_logical_plan)
    except Exception as exc:
        msg = f"Substrait replay failed: {exc}"
        raise ValueError(msg) from exc


def _public_substrait_logical_plan(
    ctx: SessionContext,
    payload: bytes,
) -> object:
    """Build a logical plan via public ``datafusion.substrait`` APIs.

    Returns:
    -------
    object
        Decoded logical plan from public Substrait APIs.

    Raises:
        TypeError: If required public Substrait callables are unavailable.
        ValueError: If public Substrait APIs are unavailable or replay fails.
    """
    try:
        from datafusion.substrait import Consumer as SubstraitConsumer
        from datafusion.substrait import Serde as SubstraitSerde
    except ImportError as exc:
        msg = "Substrait replay requires datafusion.substrait support."
        raise ValueError(msg) from exc

    deserialize = getattr(SubstraitSerde, "deserialize_bytes", None)
    if not callable(deserialize):
        msg = "Substrait replay requires Serde.deserialize_bytes."
        raise TypeError(msg)
    from_substrait = getattr(SubstraitConsumer, "from_substrait_plan", None)
    if not callable(from_substrait):
        msg = "Substrait replay requires Consumer.from_substrait_plan."
        raise TypeError(msg)
    try:
        plan = deserialize(payload)
        return from_substrait(ctx, plan)
    except Exception as exc:
        msg = f"Substrait replay failed: {exc}"
        raise ValueError(msg) from exc


def _dataframe_from_logical_plan(ctx: SessionContext, logical_plan: object) -> DataFrame:
    """Construct a DataFrame from a logical plan with type-contract checks.

    Returns:
    -------
    DataFrame
        DataFusion dataframe created from the logical plan.

    Raises:
        TypeError: If dataframe construction APIs are unavailable or return the wrong type.
        ValueError: If logical-plan execution fails while constructing the dataframe.
    """
    create = getattr(ctx, "create_dataframe_from_logical_plan", None)
    if not callable(create):
        msg = "SessionContext.create_dataframe_from_logical_plan is unavailable."
        raise TypeError(msg)
    from datafusion.dataframe import DataFrame as DataFusionDataFrame

    try:
        candidate = create(logical_plan)
    except (RuntimeError, TypeError, ValueError, AttributeError) as exc:
        msg = f"Substrait DataFrame construction failed: {exc}"
        raise ValueError(msg) from exc
    if not isinstance(candidate, DataFusionDataFrame):
        msg = f"Expected DataFrame from Substrait replay, got {type(candidate).__name__}."
        raise TypeError(msg)
    return candidate


def replay_substrait_bytes(ctx: SessionContext, payload: bytes) -> DataFrame:
    """Replay Substrait bytes into a DataFusion DataFrame.

    Args:
        ctx: DataFusion session context.
        payload: Serialized substrait payload.

    Returns:
        DataFrame: Result.

    """
    logical_plan = _internal_substrait_logical_plan(ctx, payload)
    if logical_plan is None:
        logical_plan = _public_substrait_logical_plan(ctx, payload)
    return _dataframe_from_logical_plan(ctx, logical_plan)


def validate_substrait_plan(
    substrait_bytes: bytes,
    *,
    df: DataFrame | None = None,
    ctx: SessionContext | None = None,
) -> Mapping[str, object]:
    """Validate Substrait bytes against a DataFusion DataFrame.

    Returns:
    -------
    Mapping[str, object]
        Validation payload with status, match, and diagnostics.
    """
    diagnostics: dict[str, object] = {}
    if df is None:
        diagnostics["reason"] = "missing_plan"
        return {"status": "error", "match": False, "diagnostics": diagnostics}
    resolved_ctx = ctx or _resolve_df_context(df)
    if resolved_ctx is None:
        diagnostics["reason"] = "missing_context"
        return {"status": "error", "match": False, "diagnostics": diagnostics}
    try:
        replay_df = replay_substrait_bytes(resolved_ctx, substrait_bytes)
    except (ValueError, TypeError) as exc:
        diagnostics["reason"] = "substrait_replay_failed"
        diagnostics["error"] = str(exc)
        return {"status": "error", "match": False, "diagnostics": diagnostics}
    try:
        left = _dataframe_table(df)
        right = _dataframe_table(replay_df)
    except (RuntimeError, TypeError, ValueError, AttributeError) as exc:
        diagnostics["reason"] = "arrow_collection_failed"
        diagnostics["error"] = str(exc)
        return {"status": "error", "match": False, "diagnostics": diagnostics}
    diagnostics["left_rows"] = left.num_rows
    diagnostics["right_rows"] = right.num_rows
    diagnostics["left_columns"] = list(left.column_names)
    diagnostics["right_columns"] = list(right.column_names)
    return {
        "status": "ok",
        "match": bool(left.equals(right)),
        "diagnostics": diagnostics,
    }


def _dataframe_table(df: DataFrame) -> pa.Table:
    method = getattr(df, "to_arrow_table", None)
    if not callable(method):
        msg = "DataFrame.to_arrow_table is unavailable."
        raise TypeError(msg)
    table = method()
    if not isinstance(table, pa.Table):
        msg = f"Expected pyarrow.Table, got {type(table).__name__}."
        raise TypeError(msg)
    return table


def _resolve_df_context(df: DataFrame) -> SessionContext | None:
    for name in ("session_context", "context", "ctx"):
        value = getattr(df, name, None)
        if isinstance(value, SessionContext):
            return value
        if callable(value):
            try:
                candidate = value()
            except (RuntimeError, TypeError, ValueError, AttributeError):
                continue
            if isinstance(candidate, SessionContext):
                return candidate
    return None


async def datafusion_to_async_batches(df: DataFrame) -> AsyncIterator[pa.RecordBatch]:
    """Yield RecordBatches asynchronously from a DataFusion DataFrame.

    Yields:
    ------
    pa.RecordBatch
        Record batches from the DataFusion result.
    """
    import asyncio

    reader = pa.RecordBatchReader.from_stream(df)

    def _collect_batches() -> list[pa.RecordBatch]:
        return list(reader)

    batches = await asyncio.to_thread(_collect_batches)
    for batch in batches:
        yield batch


def datafusion_write_options(policy: DataFusionWritePolicy) -> DataFrameWriteOptions:
    """Return DataFusion write options derived from a write policy.

    Returns:
    -------
    DataFrameWriteOptions
        DataFusion write options derived from the policy.
    """
    sort_exprs = tuple(col(name) for name in policy.sort_by) if policy.sort_by else None
    return DataFrameWriteOptions(
        partition_by=tuple(policy.partition_by),
        single_file_output=policy.single_file_output,
        sort_by=sort_exprs,
    )


__all__ = [
    "PlanEmissionOptions",
    "PlanExecutionOptions",
    "PlanExecutionResult",
    "PlanScanOverrides",
    "datafusion_to_async_batches",
    "datafusion_write_options",
    "execute_plan_artifact",
    "replay_substrait_bytes",
    "validate_substrait_plan",
]
