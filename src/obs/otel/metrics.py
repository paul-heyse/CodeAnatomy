"""Metrics catalog and helpers for CodeAnatomy telemetry."""

from __future__ import annotations

import threading
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass

from opentelemetry import metrics
from opentelemetry.metrics import CallbackOptions, Observation
from opentelemetry.sdk.metrics.view import ExplicitBucketHistogramAggregation, View
from opentelemetry.util.types import AttributeValue

from obs.otel.attributes import normalize_attributes
from obs.otel.constants import AttributeName, MetricName
from obs.otel.run_context import get_run_id
from obs.otel.scope_metadata import instrumentation_schema_url, instrumentation_version
from obs.otel.scopes import SCOPE_OBS

_STAGE_DURATION = MetricName.STAGE_DURATION
_TASK_DURATION = MetricName.TASK_DURATION
_DATAFUSION_DURATION = MetricName.DATAFUSION_DURATION
_WRITE_DURATION = MetricName.WRITE_DURATION
_ARTIFACT_COUNT = MetricName.ARTIFACT_COUNT
_ERROR_COUNT = MetricName.ERROR_COUNT
_DATASET_ROWS = MetricName.DATASET_ROWS
_DATASET_COLUMNS = MetricName.DATASET_COLUMNS
_SCAN_ROW_GROUPS = MetricName.SCAN_ROW_GROUPS
_SCAN_FRAGMENTS = MetricName.SCAN_FRAGMENTS
_CACHE_OPERATION_COUNT = MetricName.CACHE_OPERATION_COUNT
_CACHE_OPERATION_DURATION = MetricName.CACHE_OPERATION_DURATION
_STORAGE_OPERATION_COUNT = MetricName.STORAGE_OPERATION_COUNT
_STORAGE_OPERATION_DURATION = MetricName.STORAGE_OPERATION_DURATION

_DEFAULT_BUCKETS_S = (
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.0,
    5.0,
    10.0,
    20.0,
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

    def snapshot_values(self) -> dict[str, float]:
        """Return the latest gauge values as a mapping.

        Returns:
        -------
        dict[str, float]
            Snapshot of gauge values keyed by attribute string.
        """
        with self._lock:
            items = dict(self._values)
        snapshot: dict[str, float] = {}
        for attributes, value in items.items():
            key = ",".join(f"{name}={attr}" for name, attr in attributes)
            snapshot[key] = value
        return snapshot


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
    cache_operation_count: metrics.Counter
    cache_operation_duration: metrics.Histogram
    storage_operation_count: metrics.Counter
    storage_operation_duration: metrics.Histogram
    dataset_rows_gauge: metrics.ObservableGauge
    dataset_columns_gauge: metrics.ObservableGauge
    scan_row_groups_gauge: metrics.ObservableGauge
    scan_fragments_gauge: metrics.ObservableGauge


_REGISTRY_CACHE: dict[str, MetricsRegistry | None] = {"value": None}


def _meter() -> metrics.Meter:
    version_value = instrumentation_version()
    version = version_value if version_value is not None else "unknown"
    return metrics.get_meter(
        SCOPE_OBS,
        version,
        schema_url=instrumentation_schema_url(),
    )


def _with_run_id(payload: dict[str, object]) -> dict[str, object]:
    run_id = get_run_id()
    if run_id:
        payload[AttributeName.RUN_ID.value] = run_id
    return payload


def metric_views() -> list[View]:
    """Return default metric Views for the OTel MeterProvider.

    Returns:
    -------
    list[View]
        Configured metric views for CodeAnatomy instruments.
    """
    histogram = ExplicitBucketHistogramAggregation(list(_DEFAULT_BUCKETS_S))
    return [
        View(
            instrument_name=_STAGE_DURATION,
            aggregation=histogram,
            attribute_keys={
                AttributeName.RUN_ID.value,
                AttributeName.STAGE.value,
                AttributeName.STATUS.value,
            },
        ),
        View(
            instrument_name=_TASK_DURATION,
            aggregation=histogram,
            attribute_keys={
                AttributeName.RUN_ID.value,
                AttributeName.TASK_KIND.value,
                AttributeName.STATUS.value,
            },
        ),
        View(
            instrument_name=_DATAFUSION_DURATION,
            aggregation=histogram,
            attribute_keys={
                AttributeName.RUN_ID.value,
                AttributeName.STATUS.value,
                AttributeName.PLAN_KIND.value,
            },
        ),
        View(
            instrument_name=_WRITE_DURATION,
            aggregation=histogram,
            attribute_keys={
                AttributeName.RUN_ID.value,
                AttributeName.STATUS.value,
                AttributeName.DESTINATION.value,
            },
        ),
        View(
            instrument_name=_ARTIFACT_COUNT,
            attribute_keys={
                AttributeName.ARTIFACT_KIND.value,
                AttributeName.RUN_ID.value,
                AttributeName.STATUS.value,
            },
        ),
        View(
            instrument_name=_ERROR_COUNT,
            attribute_keys={
                AttributeName.RUN_ID.value,
                AttributeName.ERROR_TYPE.value,
                AttributeName.STAGE.value,
            },
        ),
        View(
            instrument_name=_CACHE_OPERATION_COUNT,
            attribute_keys={
                AttributeName.RUN_ID.value,
                AttributeName.CACHE_POLICY.value,
                AttributeName.CACHE_SCOPE.value,
                AttributeName.CACHE_OPERATION.value,
                AttributeName.CACHE_RESULT.value,
            },
        ),
        View(
            instrument_name=_CACHE_OPERATION_DURATION,
            aggregation=histogram,
            attribute_keys={
                AttributeName.RUN_ID.value,
                AttributeName.CACHE_POLICY.value,
                AttributeName.CACHE_SCOPE.value,
                AttributeName.CACHE_OPERATION.value,
                AttributeName.CACHE_RESULT.value,
            },
        ),
        View(
            instrument_name=_STORAGE_OPERATION_COUNT,
            attribute_keys={
                AttributeName.RUN_ID.value,
                AttributeName.STATUS.value,
                AttributeName.STORAGE_OPERATION.value,
            },
        ),
        View(
            instrument_name=_STORAGE_OPERATION_DURATION,
            aggregation=histogram,
            attribute_keys={
                AttributeName.RUN_ID.value,
                AttributeName.STATUS.value,
                AttributeName.STORAGE_OPERATION.value,
            },
        ),
        View(
            instrument_name=_DATASET_ROWS,
            attribute_keys={
                AttributeName.RUN_ID.value,
                AttributeName.DATASET.value,
            },
        ),
        View(
            instrument_name=_DATASET_COLUMNS,
            attribute_keys={
                AttributeName.RUN_ID.value,
                AttributeName.DATASET.value,
            },
        ),
        View(
            instrument_name=_SCAN_ROW_GROUPS,
            attribute_keys={
                AttributeName.RUN_ID.value,
                AttributeName.DATASET.value,
            },
        ),
        View(
            instrument_name=_SCAN_FRAGMENTS,
            attribute_keys={
                AttributeName.RUN_ID.value,
                AttributeName.DATASET.value,
            },
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
    dataset_rows_store = GaugeStore.create(
        name=_DATASET_ROWS,
        description="Dataset row counts.",
        unit="1",
    )
    dataset_columns_store = GaugeStore.create(
        name=_DATASET_COLUMNS,
        description="Dataset column counts.",
        unit="1",
    )
    scan_row_groups_store = GaugeStore.create(
        name=_SCAN_ROW_GROUPS,
        description="Row group counts observed during scans.",
        unit="1",
    )
    scan_fragments_store = GaugeStore.create(
        name=_SCAN_FRAGMENTS,
        description="Fragment counts observed during scans.",
        unit="1",
    )
    dataset_rows_gauge = meter.create_observable_gauge(
        dataset_rows_store.name,
        callbacks=[dataset_rows_store.observe],
        description=dataset_rows_store.description,
        unit=dataset_rows_store.unit,
    )
    dataset_columns_gauge = meter.create_observable_gauge(
        dataset_columns_store.name,
        callbacks=[dataset_columns_store.observe],
        description=dataset_columns_store.description,
        unit=dataset_columns_store.unit,
    )
    scan_row_groups_gauge = meter.create_observable_gauge(
        scan_row_groups_store.name,
        callbacks=[scan_row_groups_store.observe],
        description=scan_row_groups_store.description,
        unit=scan_row_groups_store.unit,
    )
    scan_fragments_gauge = meter.create_observable_gauge(
        scan_fragments_store.name,
        callbacks=[scan_fragments_store.observe],
        description=scan_fragments_store.description,
        unit=scan_fragments_store.unit,
    )
    registry = MetricsRegistry(
        stage_duration=meter.create_histogram(
            _STAGE_DURATION,
            unit="s",
            description="Stage execution duration (seconds).",
        ),
        task_duration=meter.create_histogram(
            _TASK_DURATION,
            unit="s",
            description="Hamilton task execution duration (seconds).",
        ),
        datafusion_duration=meter.create_histogram(
            _DATAFUSION_DURATION,
            unit="s",
            description="DataFusion execution duration (seconds).",
        ),
        write_duration=meter.create_histogram(
            _WRITE_DURATION,
            unit="s",
            description="DataFusion write duration (seconds).",
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
        dataset_rows=dataset_rows_store,
        dataset_columns=dataset_columns_store,
        scan_row_groups=scan_row_groups_store,
        scan_fragments=scan_fragments_store,
        cache_operation_count=meter.create_counter(
            _CACHE_OPERATION_COUNT,
            unit="1",
            description="Cache operation count by policy/scope/result.",
        ),
        cache_operation_duration=meter.create_histogram(
            _CACHE_OPERATION_DURATION,
            unit="s",
            description="Cache operation duration by policy/scope/result.",
        ),
        storage_operation_count=meter.create_counter(
            _STORAGE_OPERATION_COUNT,
            unit="1",
            description="Storage operation count by operation and status.",
        ),
        storage_operation_duration=meter.create_histogram(
            _STORAGE_OPERATION_DURATION,
            unit="s",
            description="Storage operation duration by operation and status.",
        ),
        dataset_rows_gauge=dataset_rows_gauge,
        dataset_columns_gauge=dataset_columns_gauge,
        scan_row_groups_gauge=scan_row_groups_gauge,
        scan_fragments_gauge=scan_fragments_gauge,
    )
    _REGISTRY_CACHE["value"] = registry
    return registry


def _emit_metric(
    record_fn: Callable[[float, Mapping[str, AttributeValue]], None],
    *,
    value: float,
    base_attributes: Mapping[str, object],
    attributes: Mapping[str, object] | None = None,
) -> None:
    payload = dict(base_attributes)
    if attributes:
        payload.update(attributes)
    record_fn(value, normalize_attributes(_with_run_id(payload)))


def record_stage_duration(
    stage: str,
    duration_s: float,
    *,
    status: str,
    attributes: Mapping[str, object] | None = None,
) -> None:
    """Record a stage duration histogram value."""
    registry = _registry()
    _emit_metric(
        registry.stage_duration.record,
        value=duration_s,
        base_attributes={
            AttributeName.STAGE.value: stage,
            AttributeName.STATUS.value: status,
        },
        attributes=attributes,
    )


def record_task_duration(
    task_kind: str,
    duration_s: float,
    *,
    status: str,
    attributes: Mapping[str, object] | None = None,
) -> None:
    """Record a task duration histogram value."""
    registry = _registry()
    _emit_metric(
        registry.task_duration.record,
        value=duration_s,
        base_attributes={
            AttributeName.TASK_KIND.value: task_kind,
            AttributeName.STATUS.value: status,
        },
        attributes=attributes,
    )


def record_datafusion_duration(
    duration_s: float,
    *,
    status: str,
    plan_kind: str,
    attributes: Mapping[str, object] | None = None,
) -> None:
    """Record a DataFusion execution duration histogram value."""
    registry = _registry()
    _emit_metric(
        registry.datafusion_duration.record,
        value=duration_s,
        base_attributes={
            AttributeName.STATUS.value: status,
            AttributeName.PLAN_KIND.value: plan_kind,
        },
        attributes=attributes,
    )


def record_write_duration(
    duration_s: float,
    *,
    status: str,
    destination: str,
    attributes: Mapping[str, object] | None = None,
) -> None:
    """Record a DataFusion write duration histogram value."""
    registry = _registry()
    _emit_metric(
        registry.write_duration.record,
        value=duration_s,
        base_attributes={
            AttributeName.STATUS.value: status,
            AttributeName.DESTINATION.value: destination,
        },
        attributes=attributes,
    )


def record_artifact_count(
    artifact_kind: str,
    *,
    status: str,
    attributes: Mapping[str, object] | None = None,
) -> None:
    """Increment the artifact count metric."""
    registry = _registry()
    _emit_metric(
        registry.artifact_count.add,
        value=1.0,
        base_attributes={
            AttributeName.ARTIFACT_KIND.value: artifact_kind,
            AttributeName.STATUS.value: status,
        },
        attributes=attributes,
    )


def record_error(
    stage: str,
    error_type: str,
    *,
    attributes: Mapping[str, object] | None = None,
) -> None:
    """Increment the error count metric."""
    registry = _registry()
    _emit_metric(
        registry.error_count.add,
        value=1.0,
        base_attributes={
            AttributeName.STAGE.value: stage,
            AttributeName.ERROR_TYPE.value: error_type,
        },
        attributes=attributes,
    )


def set_dataset_stats(
    dataset: str,
    *,
    rows: int,
    columns: int,
) -> None:
    """Set dataset row and column gauge values."""
    registry = _registry()
    base = {AttributeName.DATASET.value: dataset}
    _emit_metric(
        registry.dataset_rows.set_value,
        value=float(rows),
        base_attributes=base,
    )
    _emit_metric(
        registry.dataset_columns.set_value,
        value=float(columns),
        base_attributes=base,
    )


def set_scan_telemetry(
    dataset: str,
    *,
    fragment_count: int,
    row_group_count: int,
) -> None:
    """Set scan telemetry gauge values."""
    registry = _registry()
    base = {AttributeName.DATASET.value: dataset}
    _emit_metric(
        registry.scan_fragments.set_value,
        value=float(fragment_count),
        base_attributes=base,
    )
    _emit_metric(
        registry.scan_row_groups.set_value,
        value=float(row_group_count),
        base_attributes=base,
    )


def record_cache_event(
    *,
    cache_policy: str,
    cache_scope: str,
    operation: str,
    result: str,
    duration_s: float | None = None,
) -> None:
    """Record a cache operation as metrics."""
    registry = _registry()
    base = {
        AttributeName.CACHE_POLICY.value: cache_policy,
        AttributeName.CACHE_SCOPE.value: cache_scope,
        AttributeName.CACHE_OPERATION.value: operation,
        AttributeName.CACHE_RESULT.value: result,
    }
    if duration_s is not None:
        _emit_metric(
            registry.cache_operation_duration.record,
            value=duration_s,
            base_attributes=base,
        )
    _emit_metric(
        registry.cache_operation_count.add,
        value=1.0,
        base_attributes=base,
    )


def record_storage_operation(
    *,
    operation: str,
    status: str,
    duration_s: float | None = None,
    attributes: Mapping[str, object] | None = None,
) -> None:
    """Record a storage operation as metrics."""
    registry = _registry()
    base = {
        AttributeName.STORAGE_OPERATION.value: operation,
        AttributeName.STATUS.value: status,
    }
    if duration_s is not None:
        _emit_metric(
            registry.storage_operation_duration.record,
            value=duration_s,
            base_attributes=base,
            attributes=attributes,
        )
    _emit_metric(
        registry.storage_operation_count.add,
        value=1.0,
        base_attributes=base,
        attributes=attributes,
    )


def metrics_snapshot() -> dict[str, object]:
    """Return a snapshot of gauge metrics for diagnostics.

    Returns:
    -------
    dict[str, object]
        Mapping of gauge names to their latest values.
    """
    registry = _registry()
    return {
        "dataset_rows": registry.dataset_rows.snapshot_values(),
        "dataset_columns": registry.dataset_columns.snapshot_values(),
        "scan_row_groups": registry.scan_row_groups.snapshot_values(),
        "scan_fragments": registry.scan_fragments.snapshot_values(),
    }


__all__ = [
    "metric_views",
    "metrics_snapshot",
    "record_artifact_count",
    "record_cache_event",
    "record_datafusion_duration",
    "record_error",
    "record_stage_duration",
    "record_storage_operation",
    "record_task_duration",
    "record_write_duration",
    "reset_metrics_registry",
    "set_dataset_stats",
    "set_scan_telemetry",
]
