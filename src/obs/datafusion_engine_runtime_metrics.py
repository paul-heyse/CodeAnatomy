"""DataFusion engine runtime metrics and tracing instruments."""

from __future__ import annotations

from collections.abc import Mapping

from opentelemetry import metrics
from opentelemetry.trace import Tracer

from obs.otel.scopes import SCOPE_DATAFUSION
from obs.otel.tracing import get_tracer

_METER = metrics.get_meter("obs.datafusion_engine_runtime")
_TRACER = get_tracer(SCOPE_DATAFUSION)

_DELTA_MAINTENANCE_RUNS = _METER.create_counter(
    "datafusion_engine.delta.maintenance.runs",
    description="Count of Delta maintenance operation executions.",
)
_DELTA_MAINTENANCE_DURATION = _METER.create_histogram(
    "datafusion_engine.delta.maintenance.duration.seconds",
    unit="s",
    description="Duration of Delta maintenance operations.",
)


def maintenance_tracer() -> Tracer:
    """Return tracer for DataFusion maintenance spans."""
    return _TRACER


def record_delta_maintenance_run(
    *,
    operation: str,
    status: str,
    duration_s: float,
    extra_attributes: Mapping[str, str] | None = None,
) -> None:
    """Record metric points for a Delta maintenance operation."""
    attrs: dict[str, str] = {
        "operation": operation,
        "status": status,
    }
    if extra_attributes:
        attrs.update(extra_attributes)
    _DELTA_MAINTENANCE_RUNS.add(1, attrs)
    _DELTA_MAINTENANCE_DURATION.record(duration_s, attrs)


__all__ = [
    "maintenance_tracer",
    "record_delta_maintenance_run",
]
