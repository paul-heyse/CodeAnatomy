"""Execution lane helpers for relspec rule plans."""

from __future__ import annotations

import base64
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from ibis.expr.types import Value as IbisValue

from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.interop import TableLike
from datafusion_engine.bridge import (
    MaterializationPolicy,
    datafusion_to_table,
    ibis_to_datafusion_dual_lane,
)
from datafusion_engine.compile_options import DataFusionCompileOptions
from ibis_engine.plan import IbisPlan
from sqlglot_tools.bridge import IbisCompilerBackend

if TYPE_CHECKING:
    from collections.abc import Mapping

    from datafusion_engine.runtime import (
        AdapterExecutionPolicy,
        DataFusionRuntimeProfile,
        DiagnosticsSink,
        ExecutionLabel,
    )

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DataFusionLaneOptions:
    """Options for executing plans through DataFusion."""

    prefer_substrait: bool = True
    record_substrait_gaps: bool = True
    allow_full_materialization: bool = False
    capture_explain: bool | None = None
    capture_plan_artifacts: bool | None = None


@dataclass(frozen=True)
class DataFusionLaneInputs:
    """Inputs for executing plans through DataFusion."""

    ctx: ExecutionContext
    backend: IbisCompilerBackend
    params: Mapping[IbisValue, object] | None
    execution_policy: AdapterExecutionPolicy | None
    execution_label: ExecutionLabel | None
    runtime_profile: DataFusionRuntimeProfile
    options: DataFusionLaneOptions | None = None


@dataclass(frozen=True)
class ExecutionLaneRecord:
    """Diagnostics payload for execution lane selection."""

    engine: str
    lane: str
    fallback_reason: str | None
    execution_label: ExecutionLabel | None = None
    substrait_bytes: bytes | None = None
    options: DataFusionLaneOptions | None = None
    capture_explain: bool | None = None
    capture_plan_artifacts: bool | None = None


def execute_plan_datafusion(
    plan: IbisPlan,
    *,
    inputs: DataFusionLaneInputs,
) -> TableLike:
    """Execute an Ibis plan via DataFusion with Substrait fallback.

    Returns
    -------
    TableLike
        Materialized Arrow table from DataFusion execution.
    """
    resolved = inputs.options or DataFusionLaneOptions()
    df_ctx = inputs.runtime_profile.session_context()
    capture_explain = resolved.capture_explain
    if capture_explain is None:
        capture_explain = inputs.runtime_profile.diagnostics_sink is not None
    capture_plan_artifacts = resolved.capture_plan_artifacts
    if capture_plan_artifacts is None:
        capture_plan_artifacts = inputs.runtime_profile.diagnostics_sink is not None
    run_id = None
    if inputs.execution_label is not None:
        run_id = f"{inputs.execution_label.rule_name}:{inputs.execution_label.output_dataset}"
    compile_options = inputs.runtime_profile.compile_options(
        options=DataFusionCompileOptions(
            prefer_substrait=resolved.prefer_substrait,
            record_substrait_gaps=resolved.record_substrait_gaps,
            capture_explain=capture_explain,
            capture_plan_artifacts=capture_plan_artifacts,
            run_id=run_id,
        ),
        params=inputs.params,
        execution_policy=inputs.execution_policy,
        execution_label=inputs.execution_label,
    )
    result = ibis_to_datafusion_dual_lane(
        plan.expr,
        backend=inputs.backend,
        ctx=df_ctx,
        options=compile_options,
    )
    record_execution_lane(
        diagnostics_sink=inputs.runtime_profile.diagnostics_sink,
        record=ExecutionLaneRecord(
            engine="datafusion",
            lane=result.lane,
            fallback_reason=result.fallback_reason,
            execution_label=inputs.execution_label,
            substrait_bytes=result.substrait_bytes,
            options=resolved,
            capture_explain=capture_explain,
            capture_plan_artifacts=capture_plan_artifacts,
        ),
    )
    materialization_policy = MaterializationPolicy(
        allow_full_materialization=resolved.allow_full_materialization,
        debug_mode=inputs.ctx.debug,
    )
    return datafusion_to_table(
        result.df,
        ordering=plan.ordering,
        policy=materialization_policy,
    )


def can_execute_datafusion(runtime_profile: DataFusionRuntimeProfile | None) -> bool:
    """Return True when DataFusion runtime profile is available.

    Returns
    -------
    bool
        ``True`` when a runtime profile is provided.
    """
    return runtime_profile is not None


def record_execution_lane(
    *,
    diagnostics_sink: DiagnosticsSink | None,
    record: ExecutionLaneRecord,
) -> None:
    """Record execution lane selection diagnostics when a sink is available."""
    if diagnostics_sink is None:
        return
    payload: dict[str, object] = {
        "engine": record.engine,
        "lane": record.lane,
        "fallback_reason": record.fallback_reason,
    }
    if record.execution_label is not None:
        payload["rule_name"] = record.execution_label.rule_name
        payload["output_dataset"] = record.execution_label.output_dataset
    if record.options is not None:
        payload["prefer_substrait"] = record.options.prefer_substrait
        payload["record_substrait_gaps"] = record.options.record_substrait_gaps
        payload["allow_full_materialization"] = record.options.allow_full_materialization
    if record.capture_explain is not None:
        payload["capture_explain"] = record.capture_explain
    if record.capture_plan_artifacts is not None:
        payload["capture_plan_artifacts"] = record.capture_plan_artifacts
    if record.substrait_bytes is not None:
        payload["substrait_b64"] = base64.b64encode(record.substrait_bytes).decode("ascii")
    diagnostics_sink.record_artifact("relspec_execution_lane_v1", payload)


def safe_backend(backend: object | None) -> IbisCompilerBackend | None:
    """Coerce an Ibis backend into a SQLGlot-capable backend when possible.

    Returns
    -------
    IbisCompilerBackend | None
        SQLGlot-capable backend when available.
    """
    if backend is None:
        return None
    compiler = getattr(backend, "compiler", None)
    if compiler is None:
        return None
    return cast("IbisCompilerBackend", backend)


__all__ = [
    "DataFusionLaneInputs",
    "DataFusionLaneOptions",
    "ExecutionLaneRecord",
    "can_execute_datafusion",
    "execute_plan_datafusion",
    "record_execution_lane",
    "safe_backend",
]
