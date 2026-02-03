"""Plan diagnostics helpers for DataFusion plan bundles."""

from __future__ import annotations

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


__all__ = ["record_plan_bundle_diagnostics"]
