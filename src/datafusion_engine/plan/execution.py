"""Unified helpers for executing DataFusion plan bundles."""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from datafusion import SessionContext

from datafusion_engine.dataset.resolution import apply_scan_unit_overrides
from datafusion_engine.plan.bundle import DataFusionPlanBundle
from datafusion_engine.session.facade import DataFusionExecutionFacade, ExecutionResult

if TYPE_CHECKING:
    from datafusion.dataframe import DataFrame

    from datafusion_engine.lineage.scan import ScanUnit
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from serde_artifacts import PlanArtifacts


@dataclass(frozen=True)
class PlanExecutionResult:
    """Result payload for plan-bundle execution."""

    plan_bundle: DataFusionPlanBundle
    execution_result: ExecutionResult
    output: DataFrame | None
    artifacts: PlanArtifacts | None
    telemetry: Mapping[str, float]
    scan_units: tuple[ScanUnit, ...] = ()
    scan_keys: tuple[str, ...] = ()


@dataclass(frozen=True)
class PlanScanOverrides:
    """Scan override options for plan execution."""

    scan_units: Sequence[ScanUnit] = ()
    scan_keys: Sequence[str] | None = None
    apply_scan_overrides: bool = True


@dataclass(frozen=True)
class PlanEmissionOptions:
    """Emission options for plan execution artifacts and telemetry."""

    emit_artifacts: bool = True
    emit_telemetry: bool = True


@dataclass(frozen=True)
class PlanExecutionOptions:
    """Options that control plan bundle execution."""

    runtime_profile: DataFusionRuntimeProfile | None = None
    view_name: str | None = None
    scan: PlanScanOverrides = field(default_factory=PlanScanOverrides)
    emit: PlanEmissionOptions = field(default_factory=PlanEmissionOptions)


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


__all__ = [
    "PlanEmissionOptions",
    "PlanExecutionOptions",
    "PlanExecutionResult",
    "PlanScanOverrides",
    "execute_plan_bundle",
]
