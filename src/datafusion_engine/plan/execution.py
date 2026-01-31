"""Unified helpers for executing DataFusion plan bundles."""

from __future__ import annotations

import time
from collections.abc import AsyncIterator, Mapping
from typing import TYPE_CHECKING

import pyarrow as pa
from datafusion import DataFrameWriteOptions, SessionContext, col

from datafusion_engine.dataset.resolution import apply_scan_unit_overrides
from datafusion_engine.plan.bundle import DataFusionPlanBundle
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


def execute_plan_bundle(
    ctx: SessionContext,
    plan_bundle: DataFusionPlanBundle,
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

    Returns
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
        )
    # Lazy import to avoid circular dependency with facade.py
    from datafusion_engine.session.facade import DataFusionExecutionFacade

    start = time.perf_counter()
    result = DataFusionExecutionFacade(
        ctx=ctx,
        runtime_profile=resolved_options.runtime_profile,
    ).execute_plan_bundle(
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


def replay_substrait_bytes(ctx: SessionContext, payload: bytes) -> DataFrame:
    """Replay Substrait bytes into a DataFusion DataFrame.

    Raises
    ------
    ValueError
        Raised when Substrait replay is unavailable.
    """
    _ = (ctx, payload)
    msg = "Substrait replay is unavailable in this build."
    raise ValueError(msg)


def validate_substrait_plan(
    substrait_bytes: bytes,
    *,
    df: DataFrame | None = None,
) -> Mapping[str, object] | None:
    """Validate Substrait bytes against a DataFusion DataFrame.

    Returns
    -------
    Mapping[str, object] | None
        Validation payload or ``None`` when validation is unavailable.
    """
    _ = (substrait_bytes, df)
    return {"status": "unavailable", "reason": "substrait_deserialization_unavailable"}


async def datafusion_to_async_batches(df: DataFrame) -> AsyncIterator[pa.RecordBatch]:
    """Yield RecordBatches asynchronously from a DataFusion DataFrame.

    Yields
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

    Returns
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
    "execute_plan_bundle",
    "replay_substrait_bytes",
    "validate_substrait_plan",
]
