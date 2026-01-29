"""Hamilton lifecycle hooks for OpenTelemetry instrumentation."""

from __future__ import annotations

import time
from collections.abc import Mapping
from contextvars import Token
from dataclasses import dataclass, field
from typing import Any

from hamilton.lifecycle import api as lifecycle_api
from opentelemetry import context, trace
from opentelemetry.context import Context
from opentelemetry.trace import Link, Span

from obs.otel.attributes import normalize_attributes
from obs.otel.metrics import record_error, record_stage_duration, record_task_duration
from obs.otel.scopes import scope_for_layer
from obs.otel.tracing import root_span_link, set_span_attributes


@dataclass
class _SpanState:
    span: Span
    token: Token[Context]
    start: float


@dataclass
class OtelNodeHook(lifecycle_api.NodeExecutionHook):
    """Emit OpenTelemetry spans and metrics for Hamilton node execution."""

    _spans: dict[tuple[str, str, str | None], _SpanState] = field(default_factory=dict)

    def run_before_node_execution(
        self,
        *,
        node_name: str,
        node_tags: Mapping[str, Any],
        run_id: str,
        task_id: str | None,
        **kwargs: Any,
    ) -> None:
        """Start an OpenTelemetry span for a Hamilton node."""
        _ = kwargs
        key = (run_id, node_name, task_id)
        layer = _tag_str(node_tags, "layer")
        kind = _tag_str(node_tags, "kind")
        tracer = trace.get_tracer(scope_for_layer(layer))
        span_name = _span_name(kind)
        attributes = _span_attributes(
            node_name=node_name,
            run_id=run_id,
            task_id=task_id,
            node_tags=node_tags,
        )
        links: list[Link] = []
        link = root_span_link()
        if link is not None:
            links.append(link)
        span = tracer.start_span(
            span_name,
            attributes=normalize_attributes(attributes),
            links=links or None,
        )
        token = context.attach(trace.set_span_in_context(span))
        self._spans[key] = _SpanState(span=span, token=token, start=time.monotonic())

    def run_after_node_execution(
        self,
        *,
        node_name: str,
        node_tags: Mapping[str, Any],
        run_id: str,
        task_id: str | None,
        success: bool,
        **kwargs: Any,
    ) -> None:
        """Finalize the OpenTelemetry span and metrics for a Hamilton node."""
        key = (run_id, node_name, task_id)
        state = self._spans.pop(key, None)
        if state is None:
            return
        context.detach(state.token)
        duration_ms = (time.monotonic() - state.start) * 1000.0
        layer = _tag_str(node_tags, "layer")
        kind = _tag_str(node_tags, "kind")
        task_kind = _tag_str(node_tags, "task_kind")
        status = "ok" if success else "error"
        if kind == "task" and task_kind is not None:
            record_task_duration(task_kind, duration_ms, status=status)
        else:
            record_stage_duration(layer or "hamilton", duration_ms, status=status)
        error = kwargs.get("error")
        if error is not None:
            record_error(layer or "hamilton", type(error).__name__)
            if isinstance(error, Exception):
                state.span.record_exception(error)
        set_span_attributes(state.span, {"duration_ms": duration_ms, "status": status})
        if not success:
            state.span.set_status(trace.Status(trace.StatusCode.ERROR))
        state.span.end()


@dataclass
class OtelPlanHook(lifecycle_api.GraphExecutionHook):
    """Emit OpenTelemetry spans for Hamilton graph execution."""

    run_span: Span | None = None
    _token: Token[Context] | None = None

    def run_before_graph_execution(self, *, run_id: str, **kwargs: object) -> None:
        """Start a graph execution span for the Hamilton run."""
        _ = kwargs
        tracer = trace.get_tracer(scope_for_layer("execution"))
        span = tracer.start_span(
            "hamilton.graph",
            attributes=normalize_attributes({"run_id": run_id}),
        )
        token = context.attach(trace.set_span_in_context(span))
        self.run_span = span
        self._token = token

    def run_after_graph_execution(self, *, run_id: str, **kwargs: object) -> None:
        """Finalize the Hamilton graph execution span."""
        _ = kwargs
        span = self.run_span
        token = self._token
        if span is None or token is None:
            return
        context.detach(token)
        set_span_attributes(span, {"run_id": run_id})
        span.end()


def _span_name(kind: str | None) -> str:
    if kind == "task":
        return "hamilton.task"
    if kind == "stage":
        return "hamilton.stage"
    return "hamilton.node"


def _tag_str(tags: Mapping[str, Any], key: str) -> str | None:
    value = tags.get(key)
    return value if isinstance(value, str) else None


def _span_attributes(
    *,
    node_name: str,
    run_id: str,
    task_id: str | None,
    node_tags: Mapping[str, Any],
) -> dict[str, object]:
    attributes: dict[str, object] = {
        "run_id": run_id,
        "node_name": node_name,
    }
    if task_id is not None:
        attributes["task_id"] = task_id
    for key in (
        "layer",
        "kind",
        "artifact",
        "task_name",
        "task_kind",
        "cache_policy",
        "priority",
        "plan_signature",
        "plan_fingerprint",
        "plan_task_signature",
    ):
        value = node_tags.get(key)
        if value is not None:
            attributes[f"hamilton.{key}"] = value
    return attributes


__all__ = ["OtelNodeHook", "OtelPlanHook"]
