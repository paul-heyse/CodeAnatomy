"""Plan bundle helpers for incremental DataFusion execution."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa

from datafusion_engine.lineage.datafusion import extract_lineage
from datafusion_engine.lineage.scan import ScanUnit, plan_scan_unit
from datafusion_engine.plan.execution import (
    PlanExecutionOptions,
    PlanScanOverrides,
)
from datafusion_engine.plan.execution import (
    execute_plan_bundle as execute_plan_bundle_helper,
)
from datafusion_engine.session.facade import DataFusionExecutionFacade, ExecutionResult

if TYPE_CHECKING:
    from datafusion.dataframe import DataFrame

    from datafusion_engine.plan.bundle import DataFusionPlanBundle
    from semantics.incremental.runtime import IncrementalRuntime


def build_plan_bundle_for_df(
    runtime: IncrementalRuntime,
    df: DataFrame,
    *,
    compute_execution_plan: bool = True,
) -> DataFusionPlanBundle:
    """Build a plan bundle from a DataFrame using the incremental runtime.

    Returns
    -------
    DataFusionPlanBundle
        Plan bundle compiled from the provided DataFrame.
    """
    facade = _facade(runtime)
    return facade.build_plan_bundle(
        df,
        compute_execution_plan=compute_execution_plan,
    )


def execute_plan_bundle(
    runtime: IncrementalRuntime,
    bundle: DataFusionPlanBundle,
    *,
    view_name: str | None = None,
) -> ExecutionResult:
    """Execute a plan bundle with scan unit overrides applied.

    Returns
    -------
    ExecutionResult
        Execution result for the plan bundle.
    """
    scan_units, scan_keys = _plan_scan_units(bundle, runtime)
    execution = execute_plan_bundle_helper(
        runtime.session_runtime().ctx,
        bundle,
        options=PlanExecutionOptions(
            runtime_profile=runtime.profile,
            view_name=view_name,
            scan=PlanScanOverrides(
                scan_units=scan_units,
                scan_keys=scan_keys,
                apply_scan_overrides=True,
            ),
        ),
    )
    return execution.execution_result


def execute_df_to_table(
    runtime: IncrementalRuntime,
    df: DataFrame,
    *,
    view_name: str,
) -> pa.Table:
    """Execute a DataFrame via plan bundle and return a materialized table.

    Returns
    -------
    pyarrow.Table
        Materialized table derived from plan-bundle execution.
    """
    bundle = build_plan_bundle_for_df(runtime, df)
    result = execute_plan_bundle(runtime, bundle, view_name=view_name)
    return result.require_dataframe().to_arrow_table()


def _facade(runtime: IncrementalRuntime) -> DataFusionExecutionFacade:
    return DataFusionExecutionFacade(
        ctx=runtime.session_runtime().ctx,
        runtime_profile=runtime.profile,
    )


def _plan_scan_units(
    bundle: DataFusionPlanBundle,
    runtime: IncrementalRuntime,
) -> tuple[tuple[ScanUnit, ...], tuple[str, ...]]:
    session_runtime = runtime.session_runtime()
    scan_units: dict[str, ScanUnit] = {}
    for scan in extract_lineage(
        bundle.optimized_logical_plan,
        udf_snapshot=bundle.artifacts.udf_snapshot,
    ).scans:
        location = runtime.profile.dataset_location(scan.dataset_name)
        if location is None:
            continue
        unit = plan_scan_unit(
            session_runtime.ctx,
            dataset_name=scan.dataset_name,
            location=location,
            lineage=scan,
        )
        scan_units[unit.key] = unit
    units = tuple(sorted(scan_units.values(), key=lambda unit: unit.key))
    return units, tuple(unit.key for unit in units)


__all__ = [
    "build_plan_bundle_for_df",
    "execute_df_to_table",
    "execute_plan_bundle",
]
