"""Contract tests for OpenTelemetry tracing."""

from __future__ import annotations

from obs.otel.cache import cache_span
from obs.otel.hamilton import OtelNodeHook
from obs.otel.tracing import root_span
from tests.obs._support.otel_harness import get_otel_harness


def test_root_span_contract() -> None:
    """Validate root span creation for the pipeline."""
    harness = get_otel_harness()
    harness.reset()
    with root_span("graph_product.build", attributes={"codeanatomy.product": "cpg"}):
        pass
    spans = harness.span_exporter.get_finished_spans()
    assert any(span.name == "graph_product.build" for span in spans)
    assert any((span.attributes or {}).get("otel.sampling.rule") for span in spans)


def test_hamilton_node_span_contract() -> None:
    """Validate Hamilton node spans include the expected attributes."""
    harness = get_otel_harness()
    harness.reset()
    hook = OtelNodeHook()
    hook.run_before_node_execution(
        node_name="test_node",
        node_tags={"layer": "execution", "kind": "task", "task_kind": "scan"},
        run_id="run-1",
        task_id="task-1",
    )
    hook.run_after_node_execution(
        node_name="test_node",
        node_tags={"layer": "execution", "kind": "task", "task_kind": "scan"},
        run_id="run-1",
        task_id="task-1",
        success=True,
    )
    spans = harness.span_exporter.get_finished_spans()
    assert any(span.name == "hamilton.task" for span in spans)
    assert any((span.attributes or {}).get("hamilton.task_kind") == "scan" for span in spans)


def test_cache_span_contract() -> None:
    """Validate cache span attributes are emitted."""
    harness = get_otel_harness()
    harness.reset()
    with cache_span(
        "cache.test",
        cache_policy="dataset_delta_staging",
        cache_scope="dataset",
        operation="read",
    ) as (_span, set_result):
        set_result("hit")
    spans = harness.span_exporter.get_finished_spans()
    cache_spans = [span for span in spans if span.name == "cache.test"]
    assert cache_spans
    attributes = cache_spans[-1].attributes or {}
    assert attributes.get("cache.policy") == "dataset_delta_staging"
    assert attributes.get("cache.operation") == "read"
    assert attributes.get("cache.result") == "hit"
