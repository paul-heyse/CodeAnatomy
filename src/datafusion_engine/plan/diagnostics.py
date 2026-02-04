"""Plan diagnostics helpers for DataFusion plan bundles."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING

from opentelemetry import trace

from datafusion_engine.lineage.datafusion import referenced_tables_from_plan
from datafusion_engine.lineage.diagnostics import recorder_for_profile
from obs.otel.run_context import get_run_id
from obs.otel.tracing import set_span_attributes

if TYPE_CHECKING:
    from datafusion_engine.plan.bundle import DataFusionPlanBundle
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile


def record_plan_bundle_diagnostics(
    *,
    bundle: DataFusionPlanBundle,
    runtime_profile: DataFusionRuntimeProfile | None,
    plan_kind: str,
    stage: str,
    view_name: str | None = None,
) -> None:
    """Record diagnostics artifacts for a plan bundle and annotate spans."""
    plan_hash = bundle.plan_fingerprint
    payload = {
        "plan_hash": plan_hash,
        "plan_identity_hash": bundle.plan_identity_hash,
        "plan_kind": plan_kind,
        "stage": stage,
        "view_name": view_name,
        "required_udfs": list(bundle.required_udfs),
        "required_rewrite_tags": list(bundle.required_rewrite_tags),
        "referenced_tables": list(referenced_tables_from_plan(bundle.optimized_logical_plan)),
        "run_id": get_run_id(),
    }
    recorder = recorder_for_profile(
        runtime_profile,
        operation_id=f"plan_bundle:{plan_hash}",
    )
    if recorder is not None:
        recorder.record_artifact("datafusion_plan_bundle_v1", payload)

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

    bundle: DataFusionPlanBundle
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
    payload: dict[str, object] = {
        "plan_hash": plan_hash,
        "plan_identity_hash": request.bundle.plan_identity_hash,
        "plan_kind": request.plan_kind,
        "status": request.status,
        "duration_ms": request.duration_ms,
        "view_name": request.view_name,
        "run_id": get_run_id(),
        "error": request.error,
        "trace_id": trace_id,
        "span_id": span_id,
        **stats,
    }
    recorder = recorder_for_profile(
        request.runtime_profile,
        operation_id=f"plan_execution:{plan_hash}",
    )
    if recorder is not None:
        recorder.record_artifact("datafusion_plan_execution_v1", payload)
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
    if stats.get("rows_produced") is not None:
        set_span_attributes(span, {"codeanatomy.rows_produced": stats["rows_produced"]})
    if stats.get("bytes_scanned") is not None:
        set_span_attributes(span, {"codeanatomy.bytes_scanned": stats["bytes_scanned"]})


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
    payload: dict[str, object] = {
        "plan_hash": request.plan_hash,
        "plan_identity_hash": request.plan_identity_hash,
        "phase": request.phase,
        "duration_ms": request.duration_ms,
        "trace_id": trace_id,
        "span_id": span_id,
        "run_id": get_run_id(),
        "diagnostic.severity": "info",
        "diagnostic.category": "datafusion_plan",
    }
    recorder = recorder_for_profile(
        request.runtime_profile,
        operation_id=f"plan_phase:{request.plan_hash}:{request.phase}",
    )
    if recorder is not None:
        recorder.record_artifact("datafusion_plan_phase_v1", payload)


def _execution_stats_payload(bundle: DataFusionPlanBundle) -> dict[str, object]:
    details = bundle.plan_details
    stats = details.get("statistics") if isinstance(details, Mapping) else None
    rows = None
    bytes_scanned = None
    if isinstance(stats, Mapping):
        rows = _coerce_int(stats.get("num_rows") or stats.get("row_count"))
        bytes_scanned = _coerce_int(stats.get("total_byte_size") or stats.get("total_bytes"))
    return {
        "rows_produced": rows,
        "bytes_scanned": bytes_scanned,
        "partition_count": _coerce_int(details.get("partition_count")),
        "repartition_count": _coerce_int(details.get("repartition_count")),
        "explain_analyze_duration_ms": _coerce_float(details.get("explain_analyze_duration_ms")),
        "explain_analyze_output_rows": _coerce_int(details.get("explain_analyze_output_rows")),
    }


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
    "PlanExecutionDiagnostics",
    "PlanPhaseDiagnostics",
    "record_plan_bundle_diagnostics",
    "record_plan_execution_diagnostics",
    "record_plan_phase_diagnostics",
]
