"""Contract tests for OpenTelemetry metrics."""

from __future__ import annotations

from obs.otel.metrics import (
    record_cache_event,
    record_stage_duration,
    record_task_duration,
    set_dataset_stats,
    set_scan_telemetry,
)
from obs.otel.run_context import reset_run_id, set_run_id
from tests.obs._support.otel_harness import get_otel_harness


def _metric_names(data: object) -> set[str]:
    if data is None:
        return set()
    names: set[str] = set()
    resource_metrics = getattr(data, "resource_metrics", ())
    for resource_metric in resource_metrics:
        for scope_metric in getattr(resource_metric, "scope_metrics", ()):
            for metric in getattr(scope_metric, "metrics", ()):
                name = getattr(metric, "name", None)
                if isinstance(name, str):
                    names.add(name)
    return names


def _metric_points(data: object, name: str) -> list[object]:
    if data is None:
        return []
    points: list[object] = []
    resource_metrics = getattr(data, "resource_metrics", ())
    for resource_metric in resource_metrics:
        for scope_metric in getattr(resource_metric, "scope_metrics", ()):
            for metric in getattr(scope_metric, "metrics", ()):
                if getattr(metric, "name", None) != name:
                    continue
                payload = getattr(metric, "data", None)
                data_points = getattr(payload, "data_points", ())
                points.extend(data_points)
    return points


def test_metrics_catalog_emits() -> None:
    """Ensure the metric catalog emits expected instrument names."""
    harness = get_otel_harness()
    record_stage_duration("extract", 12.5, status="ok")
    record_task_duration("scan", 7.0, status="ok")
    set_dataset_stats("cpg_nodes", rows=100, columns=5)
    set_scan_telemetry("cpg_nodes", fragment_count=2, row_group_count=4)
    record_cache_event(
        cache_policy="dataset_delta_staging",
        cache_scope="dataset",
        operation="write",
        result="write",
        duration_s=0.01,
    )
    data = harness.metric_reader.get_metrics_data()
    names = _metric_names(data)
    assert "codeanatomy.stage.duration" in names
    assert "codeanatomy.task.duration" in names
    assert "codeanatomy.dataset.rows" in names
    assert "codeanatomy.scan.row_groups" in names
    assert "codeanatomy.cache.operation.count" in names
    assert "codeanatomy.cache.operation.duration" in names


def test_cache_metrics_include_run_id() -> None:
    """Ensure cache metrics include run_id when available."""
    harness = get_otel_harness()
    harness.reset()
    token = set_run_id("run-1")
    try:
        record_cache_event(
            cache_policy="dataset_delta_staging",
            cache_scope="dataset",
            operation="read",
            result="hit",
            duration_s=0.01,
        )
    finally:
        reset_run_id(token)
    data = harness.metric_reader.get_metrics_data()
    points = _metric_points(data, "codeanatomy.cache.operation.count")
    assert any(
        (getattr(point, "attributes", {}) or {}).get("codeanatomy.run_id") == "run-1"
        for point in points
    )
