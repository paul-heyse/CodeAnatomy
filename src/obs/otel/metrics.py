"""Metrics catalog and helpers for CodeAnatomy telemetry."""

from __future__ import annotations

import threading
from collections.abc import Iterable, Mapping
from dataclasses import dataclass

from opentelemetry import metrics
from opentelemetry.metrics import CallbackOptions, Observation
from opentelemetry.sdk.metrics.view import ExplicitBucketHistogramAggregation, View
from opentelemetry.util.types import AttributeValue

from obs.otel.attributes import normalize_attributes
from obs.otel.scopes import SCOPE_OBS

_STAGE_DURATION = "codeanatomy.stage.duration_ms"
_TASK_DURATION = "codeanatomy.task.duration_ms"
_DATAFUSION_DURATION = "codeanatomy.datafusion.execute.duration_ms"
_WRITE_DURATION = "codeanatomy.datafusion.write.duration_ms"
_ARTIFACT_COUNT = "codeanatomy.artifact.count"
_ERROR_COUNT = "codeanatomy.error.count"
_DATASET_ROWS = "codeanatomy.dataset.rows"
_DATASET_COLUMNS = "codeanatomy.dataset.columns"
_SCAN_ROW_GROUPS = "codeanatomy.scan.row_groups"
_SCAN_FRAGMENTS = "codeanatomy.scan.fragments"

_DEFAULT_BUCKETS_MS = (
    5,
    10,
    25,
    50,
    100,
    250,
    500,
    1000,
    2000,
    5000,
    10000,
    20000,
)


@dataclass
class GaugeStore:
    """Track the latest gauge values for observable metrics."""

    name: str
    description: str
    unit: str
    _values: dict[tuple[tuple[str, AttributeValue], ...], float]
    _lock: threading.Lock

    @classmethod
    def create(cls, *, name: str, description: str, unit: str) -> GaugeStore:
        return cls(
            name=name,
            description=description,
            unit=unit,
            _values={},
            _lock=threading.Lock(),
        )

    def observe(self, _options: CallbackOptions) -> Iterable[Observation]:
        with self._lock:
            items = list(self._values.items())
        for key, value in items:
            yield Observation(value, dict(key))

    def set_value(self, value: float, attributes: Mapping[str, AttributeValue]) -> None:
        key = tuple(sorted(attributes.items()))
        with self._lock:
            self._values[key] = value


@dataclass
class MetricsRegistry:
    """Registry for CodeAnatomy metric instruments."""

    stage_duration: metrics.Histogram
    task_duration: metrics.Histogram
    datafusion_duration: metrics.Histogram
    write_duration: metrics.Histogram
    artifact_count: metrics.Counter
    error_count: metrics.Counter
    dataset_rows: GaugeStore
    dataset_columns: GaugeStore
    scan_row_groups: GaugeStore
    scan_fragments: GaugeStore


_REGISTRY_CACHE: dict[str, MetricsRegistry | None] = {"value": None}


def _meter() -> metrics.Meter:
    return metrics.get_meter(SCOPE_OBS)


def metric_views() -> list[View]:
    """Return default metric Views for the OTel MeterProvider.

    Returns
    -------
    list[View]
        Configured metric views for CodeAnatomy instruments.
    """
    histogram = ExplicitBucketHistogramAggregation(list(_DEFAULT_BUCKETS_MS))
    return [
        View(
            instrument_name=_STAGE_DURATION,
            aggregation=histogram,
            attribute_keys={"stage", "status"},
        ),
        View(
            instrument_name=_TASK_DURATION,
            aggregation=histogram,
            attribute_keys={"task_kind", "status"},
        ),
        View(
            instrument_name=_DATAFUSION_DURATION,
            aggregation=histogram,
            attribute_keys={"status", "plan_kind"},
        ),
        View(
            instrument_name=_WRITE_DURATION,
            aggregation=histogram,
            attribute_keys={"status", "destination"},
        ),
        View(
            instrument_name=_ARTIFACT_COUNT,
            attribute_keys={"artifact_kind", "status"},
        ),
        View(
            instrument_name=_ERROR_COUNT,
            attribute_keys={"stage", "error_type"},
        ),
    ]


def reset_metrics_registry() -> None:
    """Reset cached metric instruments so they can be re-created."""
    _REGISTRY_CACHE["value"] = None


def _registry() -> MetricsRegistry:
    cached = _REGISTRY_CACHE["value"]
    if cached is not None:
        return cached
    meter = _meter()
    registry = MetricsRegistry(
        stage_duration=meter.create_histogram(
            _STAGE_DURATION,
            unit="ms",
            description="Stage execution duration.",
        ),
        task_duration=meter.create_histogram(
            _TASK_DURATION,
            unit="ms",
            description="Hamilton task execution duration.",
        ),
        datafusion_duration=meter.create_histogram(
            _DATAFUSION_DURATION,
            unit="ms",
            description="DataFusion execution duration.",
        ),
        write_duration=meter.create_histogram(
            _WRITE_DURATION,
            unit="ms",
            description="DataFusion write duration.",
        ),
        artifact_count=meter.create_counter(
            _ARTIFACT_COUNT,
            unit="1",
            description="Artifact counts emitted by the pipeline.",
        ),
        error_count=meter.create_counter(
            _ERROR_COUNT,
            unit="1",
            description="Error counts emitted by the pipeline.",
        ),
        dataset_rows=GaugeStore.create(
            name=_DATASET_ROWS,
            description="Dataset row counts.",
            unit="1",
        ),
        dataset_columns=GaugeStore.create(
            name=_DATASET_COLUMNS,
            description="Dataset column counts.",
            unit="1",
        ),
        scan_row_groups=GaugeStore.create(
            name=_SCAN_ROW_GROUPS,
            description="Row group counts observed during scans.",
            unit="1",
        ),
        scan_fragments=GaugeStore.create(
            name=_SCAN_FRAGMENTS,
            description="Fragment counts observed during scans.",
            unit="1",
        ),
    )
    meter.create_observable_gauge(
        registry.dataset_rows.name,
        callbacks=[registry.dataset_rows.observe],
        description=registry.dataset_rows.description,
        unit=registry.dataset_rows.unit,
    )
    meter.create_observable_gauge(
        registry.dataset_columns.name,
        callbacks=[registry.dataset_columns.observe],
        description=registry.dataset_columns.description,
        unit=registry.dataset_columns.unit,
    )
    meter.create_observable_gauge(
        registry.scan_row_groups.name,
        callbacks=[registry.scan_row_groups.observe],
        description=registry.scan_row_groups.description,
        unit=registry.scan_row_groups.unit,
    )
    meter.create_observable_gauge(
        registry.scan_fragments.name,
        callbacks=[registry.scan_fragments.observe],
        description=registry.scan_fragments.description,
        unit=registry.scan_fragments.unit,
    )
    _REGISTRY_CACHE["value"] = registry
    return registry


def record_stage_duration(
    stage: str,
    duration_ms: float,
    *,
    status: str,
    attributes: Mapping[str, object] | None = None,
) -> None:
    """Record a stage duration histogram value."""
    registry = _registry()
    payload: dict[str, object] = {"stage": stage, "status": status}
    if attributes:
        payload.update(attributes)
    registry.stage_duration.record(duration_ms, normalize_attributes(payload))


def record_task_duration(
    task_kind: str,
    duration_ms: float,
    *,
    status: str,
    attributes: Mapping[str, object] | None = None,
) -> None:
    """Record a task duration histogram value."""
    registry = _registry()
    payload: dict[str, object] = {"task_kind": task_kind, "status": status}
    if attributes:
        payload.update(attributes)
    registry.task_duration.record(duration_ms, normalize_attributes(payload))


def record_datafusion_duration(
    duration_ms: float,
    *,
    status: str,
    plan_kind: str,
    attributes: Mapping[str, object] | None = None,
) -> None:
    """Record a DataFusion execution duration histogram value."""
    registry = _registry()
    payload: dict[str, object] = {"status": status, "plan_kind": plan_kind}
    if attributes:
        payload.update(attributes)
    registry.datafusion_duration.record(duration_ms, normalize_attributes(payload))


def record_write_duration(
    duration_ms: float,
    *,
    status: str,
    destination: str,
    attributes: Mapping[str, object] | None = None,
) -> None:
    """Record a DataFusion write duration histogram value."""
    registry = _registry()
    payload: dict[str, object] = {"status": status, "destination": destination}
    if attributes:
        payload.update(attributes)
    registry.write_duration.record(duration_ms, normalize_attributes(payload))


def record_artifact_count(
    artifact_kind: str,
    *,
    status: str,
    attributes: Mapping[str, object] | None = None,
) -> None:
    """Increment the artifact count metric."""
    registry = _registry()
    payload: dict[str, object] = {"artifact_kind": artifact_kind, "status": status}
    if attributes:
        payload.update(attributes)
    registry.artifact_count.add(1, normalize_attributes(payload))


def record_error(
    stage: str,
    error_type: str,
    *,
    attributes: Mapping[str, object] | None = None,
) -> None:
    """Increment the error count metric."""
    registry = _registry()
    payload: dict[str, object] = {"stage": stage, "error_type": error_type}
    if attributes:
        payload.update(attributes)
    registry.error_count.add(1, normalize_attributes(payload))


def set_dataset_stats(
    dataset: str,
    *,
    rows: int,
    columns: int,
) -> None:
    """Set dataset row and column gauge values."""
    registry = _registry()
    attrs = normalize_attributes({"dataset": dataset})
    registry.dataset_rows.set_value(float(rows), attrs)
    registry.dataset_columns.set_value(float(columns), attrs)


def set_scan_telemetry(
    dataset: str,
    *,
    fragment_count: int,
    row_group_count: int,
) -> None:
    """Set scan telemetry gauge values."""
    registry = _registry()
    attrs = normalize_attributes({"dataset": dataset})
    registry.scan_fragments.set_value(float(fragment_count), attrs)
    registry.scan_row_groups.set_value(float(row_group_count), attrs)


__all__ = [
    "metric_views",
    "record_artifact_count",
    "record_datafusion_duration",
    "record_error",
    "record_stage_duration",
    "record_task_duration",
    "record_write_duration",
    "reset_metrics_registry",
    "set_dataset_stats",
    "set_scan_telemetry",
]
