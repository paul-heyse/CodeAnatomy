"""Plan diagnostics helpers for DataFusion plan bundles."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import msgspec
from opentelemetry import trace

from datafusion_engine.lineage.diagnostics import recorder_for_profile
from datafusion_engine.lineage.reporting import referenced_tables_from_plan
from obs.otel.run_context import get_run_id
from obs.otel.tracing import set_span_attributes
from serde_msgspec import StructBaseHotPath, to_builtins_mapping

if TYPE_CHECKING:
    from datafusion_engine.plan.bundle_artifact import DataFusionPlanArtifact
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile


class PlanBundleDiagnostics(StructBaseHotPath, frozen=True):
    """Serializable diagnostics payload for plan bundle artifacts."""

    plan_hash: str
    plan_identity_hash: str | None
    plan_kind: str
    stage: str
    view_name: str | None = None
    required_udfs: tuple[str, ...] = ()
    required_rewrite_tags: tuple[str, ...] = ()
    referenced_tables: tuple[str, ...] = ()
    run_id: str | None = None


class PlanExecutionDiagnosticsPayload(StructBaseHotPath, frozen=True):
    """Serializable diagnostics payload for plan execution artifacts."""

    plan_hash: str
    plan_identity_hash: str | None
    plan_kind: str
    status: str
    duration_ms: float
    view_name: str | None = None
    run_id: str | None = None
    error: str | None = None
    trace_id: str | None = None
    span_id: str | None = None
    rows_produced: int | None = None
    bytes_scanned: int | None = None
    partition_count: int | None = None
    repartition_count: int | None = None
    explain_analyze_duration_ms: float | None = None
    explain_analyze_output_rows: int | None = None


@dataclass(frozen=True)
class PlanExecutionStats:
    """Typed execution stats extracted from plan details."""

    rows_produced: int | None = None
    bytes_scanned: int | None = None
    partition_count: int | None = None
    repartition_count: int | None = None
    explain_analyze_duration_ms: float | None = None
    explain_analyze_output_rows: int | None = None


class PlanPhaseDiagnosticsPayload(StructBaseHotPath, frozen=True):
    """Serializable diagnostics payload for plan phase artifacts."""

    plan_hash: str
    plan_identity_hash: str | None
    phase: str
    duration_ms: float | None = None
    trace_id: str | None = None
    span_id: str | None = None
    run_id: str | None = None
    diagnostic_severity: str = msgspec.field(
        default="info",
        name="diagnostic.severity",
    )
    diagnostic_category: str = msgspec.field(
        default="datafusion_plan",
        name="diagnostic.category",
    )


def record_plan_bundle_diagnostics(
    *,
    bundle: DataFusionPlanArtifact,
    runtime_profile: DataFusionRuntimeProfile | None,
    plan_kind: str,
    stage: str,
    view_name: str | None = None,
) -> None:
    """Record diagnostics artifacts for a plan bundle and annotate spans."""
    plan_hash = bundle.plan_fingerprint
    payload = PlanBundleDiagnostics(
        plan_hash=plan_hash,
        plan_identity_hash=bundle.plan_identity_hash,
        plan_kind=plan_kind,
        stage=stage,
        view_name=view_name,
        required_udfs=tuple(bundle.required_udfs),
        required_rewrite_tags=tuple(bundle.required_rewrite_tags),
        referenced_tables=tuple(referenced_tables_from_plan(bundle.optimized_logical_plan)),
        run_id=get_run_id(),
    )
    recorder = recorder_for_profile(
        runtime_profile,
        operation_id=f"plan_bundle:{plan_hash}",
    )
    if recorder is not None:
        from serde_artifact_specs import DATAFUSION_PLAN_BUNDLE_SPEC

        recorder.record_artifact(
            DATAFUSION_PLAN_BUNDLE_SPEC,
            to_builtins_mapping(payload, str_keys=True),
        )

    span = trace.get_current_span()
    if span is None or not span.is_recording():
        return
    set_span_attributes(
        span,
        {
            "codeanatomy.plan_hash": plan_hash,
            "codeanatomy.plan_kind": plan_kind,
        },
    )


@dataclass(frozen=True)
class PlanExecutionDiagnostics:
    """Execution diagnostics payload for a plan bundle."""

    bundle: DataFusionPlanArtifact
    runtime_profile: DataFusionRuntimeProfile | None
    view_name: str | None
    plan_kind: str
    status: str
    duration_ms: float
    error: str | None


@dataclass(frozen=True)
class PlanPhaseDiagnostics:
    """Diagnostics payload for a plan phase."""

    runtime_profile: DataFusionRuntimeProfile | None
    plan_hash: str
    plan_identity_hash: str | None
    phase: str
    duration_ms: float | None


def record_plan_execution_diagnostics(
    *,
    request: PlanExecutionDiagnostics,
) -> None:
    """Record execution diagnostics for a plan bundle."""
    plan_hash = request.bundle.plan_fingerprint
    span = trace.get_current_span()
    trace_id = None
    span_id = None
    if span is not None:
        context = span.get_span_context()
        if context is not None and context.is_valid:
            trace_id = f"{context.trace_id:032x}"
            span_id = f"{context.span_id:016x}"
    stats = _execution_stats_payload(request.bundle)
    payload = PlanExecutionDiagnosticsPayload(
        plan_hash=plan_hash,
        plan_identity_hash=request.bundle.plan_identity_hash,
        plan_kind=request.plan_kind,
        status=request.status,
        duration_ms=request.duration_ms,
        view_name=request.view_name,
        run_id=get_run_id(),
        error=request.error,
        trace_id=trace_id,
        span_id=span_id,
        rows_produced=stats.rows_produced,
        bytes_scanned=stats.bytes_scanned,
        partition_count=stats.partition_count,
        repartition_count=stats.repartition_count,
        explain_analyze_duration_ms=stats.explain_analyze_duration_ms,
        explain_analyze_output_rows=stats.explain_analyze_output_rows,
    )
    recorder = recorder_for_profile(
        request.runtime_profile,
        operation_id=f"plan_execution:{plan_hash}",
    )
    if recorder is not None:
        from serde_artifact_specs import DATAFUSION_PLAN_EXECUTION_SPEC

        recorder.record_artifact(
            DATAFUSION_PLAN_EXECUTION_SPEC,
            to_builtins_mapping(payload, str_keys=True),
        )
    if span is None or not span.is_recording():
        return
    set_span_attributes(
        span,
        {
            "codeanatomy.plan_hash": plan_hash,
            "codeanatomy.plan_kind": request.plan_kind,
            "codeanatomy.execution_status": request.status,
            "codeanatomy.execution_duration_ms": request.duration_ms,
        },
    )
    if stats.rows_produced is not None:
        set_span_attributes(span, {"codeanatomy.rows_produced": stats.rows_produced})
    if stats.bytes_scanned is not None:
        set_span_attributes(span, {"codeanatomy.bytes_scanned": stats.bytes_scanned})


def record_plan_phase_diagnostics(*, request: PlanPhaseDiagnostics) -> None:
    """Record diagnostics for a DataFusion plan phase."""
    span = trace.get_current_span()
    trace_id = None
    span_id = None
    if span is not None:
        context = span.get_span_context()
        if context is not None and context.is_valid:
            trace_id = f"{context.trace_id:032x}"
            span_id = f"{context.span_id:016x}"
    payload = PlanPhaseDiagnosticsPayload(
        plan_hash=request.plan_hash,
        plan_identity_hash=request.plan_identity_hash,
        phase=request.phase,
        duration_ms=request.duration_ms,
        trace_id=trace_id,
        span_id=span_id,
        run_id=get_run_id(),
    )
    recorder = recorder_for_profile(
        request.runtime_profile,
        operation_id=f"plan_phase:{request.plan_hash}:{request.phase}",
    )
    if recorder is not None:
        from serde_artifact_specs import DATAFUSION_PLAN_PHASE_SPEC

        recorder.record_artifact(
            DATAFUSION_PLAN_PHASE_SPEC,
            to_builtins_mapping(payload, str_keys=True),
        )


def _execution_stats_payload(bundle: DataFusionPlanArtifact) -> PlanExecutionStats:
    from datafusion_engine.plan.signals import extract_plan_signals

    signals = extract_plan_signals(bundle)
    stats = signals.stats
    return PlanExecutionStats(
        rows_produced=stats.num_rows if stats is not None else None,
        bytes_scanned=stats.total_bytes if stats is not None else None,
        partition_count=stats.partition_count if stats is not None else None,
        repartition_count=signals.repartition_count,
        explain_analyze_duration_ms=signals.explain_analyze_duration_ms,
        explain_analyze_output_rows=signals.explain_analyze_output_rows,
    )


def _coerce_int(value: object) -> int | None:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    return None


def _coerce_float(value: object) -> float | None:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    return None


__all__ = [
    "PlanBundleDiagnostics",
    "PlanExecutionDiagnostics",
    "PlanExecutionDiagnosticsPayload",
    "PlanPhaseDiagnostics",
    "PlanPhaseDiagnosticsPayload",
    "record_plan_bundle_diagnostics",
    "record_plan_execution_diagnostics",
    "record_plan_phase_diagnostics",
]
