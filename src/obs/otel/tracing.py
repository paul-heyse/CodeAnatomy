"""Tracing helpers for CodeAnatomy instrumentation."""

from __future__ import annotations

import time
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar

from opentelemetry import trace
from opentelemetry.trace import Link, Span, SpanContext, Status, StatusCode
from opentelemetry.util.types import AttributeValue

from obs.otel.attributes import normalize_attributes
from obs.otel.heartbeat import pop_stage, push_stage
from obs.otel.metrics import record_stage_duration
from obs.otel.scope_metadata import instrumentation_schema_url, instrumentation_version
from obs.otel.scopes import SCOPE_PIPELINE

_ROOT_SPAN_CONTEXT: ContextVar[SpanContext | None] = ContextVar(
    "codeanatomy_root_span_context",
    default=None,
)
_ROOT_SPAN_CONTEXT_FALLBACK: dict[str, SpanContext | None] = {"value": None}


def get_tracer(scope_name: str) -> trace.Tracer:
    """Return a tracer for the given instrumentation scope.

    Parameters
    ----------
    scope_name
        Instrumentation scope name.

    Returns
    -------
    opentelemetry.trace.Tracer
        Tracer bound to the requested scope.
    """
    version_value = instrumentation_version()
    version = version_value if version_value is not None else "unknown"
    return trace.get_tracer(
        scope_name,
        instrumenting_library_version=version,
        schema_url=instrumentation_schema_url(),
    )


def set_root_span_context(span_context: SpanContext | None) -> None:
    """Set the root span context for linking child spans.

    Parameters
    ----------
    span_context
        Span context to store.
    """
    _ROOT_SPAN_CONTEXT.set(span_context)
    _ROOT_SPAN_CONTEXT_FALLBACK["value"] = span_context


def root_span_context() -> SpanContext | None:
    """Return the stored root span context, if any.

    Returns
    -------
    SpanContext | None
        Stored root span context.
    """
    return _ROOT_SPAN_CONTEXT.get() or _ROOT_SPAN_CONTEXT_FALLBACK["value"]


def root_span_link() -> Link | None:
    """Return a span link to the current root span context when available.

    Returns
    -------
    Link | None
        Link to the root span context when available.
    """
    context = root_span_context()
    if context is None:
        return None
    return Link(context)


def set_span_attributes(span: Span, attrs: Mapping[str, object] | None) -> None:
    """Attach normalized attributes to a span.

    Parameters
    ----------
    span
        Span to update.
    attrs
        Raw attributes to normalize and attach.
    """
    normalized = normalize_attributes(attrs)
    for key, value in normalized.items():
        span.set_attribute(key, value)


def record_exception(span: Span, exc: Exception) -> None:
    """Record an exception on a span and mark it as error.

    Parameters
    ----------
    span
        Span to annotate.
    exc
        Exception to record.
    """
    span.record_exception(exc)
    span.set_status(Status(StatusCode.ERROR))


def span_attributes(*, attrs: Mapping[str, object] | None = None) -> dict[str, AttributeValue]:
    """Return normalized attributes for direct use in span creation.

    Returns
    -------
    dict[str, AttributeValue]
        Normalized attributes for span creation.
    """
    return normalize_attributes(attrs)


@contextmanager
def stage_span(
    name: str,
    *,
    stage: str,
    scope_name: str,
    attributes: Mapping[str, object] | None = None,
) -> Iterator[Span]:
    """Start a stage span and emit stage duration metrics.

    Parameters
    ----------
    name
        Span name.
    stage
        Stage name for metrics and attributes.
    scope_name
        Instrumentation scope name.
    attributes
        Optional span attributes.

    Yields
    ------
    Span
        The started span.
    """
    base_attrs: dict[str, object] = {"codeanatomy.stage": stage}
    if attributes:
        base_attrs.update(attributes)
    tracer = get_tracer(scope_name)
    start = time.monotonic()
    status = "ok"
    token = push_stage(stage)
    with tracer.start_as_current_span(name, attributes=span_attributes(attrs=base_attrs)) as span:
        span.add_event(
            "stage.start",
            attributes=normalize_attributes({"stage": stage}),
        )
        try:
            yield span
        except Exception as exc:
            status = "error"
            record_exception(span, exc)
            raise
        finally:
            duration_s = time.monotonic() - start
            record_stage_duration(stage, duration_s, status=status)
            slow_threshold_s = 5.0
            slow_attrs: dict[str, object] = {}
            if duration_s >= slow_threshold_s:
                slow_attrs = {
                    "codeanatomy.slow": True,
                    "codeanatomy.slow_threshold_s": slow_threshold_s,
                }
            set_span_attributes(
                span,
                {"duration_s": duration_s, "status": status, **slow_attrs},
            )
            span.add_event(
                "stage.end",
                attributes=normalize_attributes(
                    {"stage": stage, "status": status, "duration_s": duration_s}
                ),
            )
            pop_stage(token)


@contextmanager
def root_span(
    name: str,
    *,
    attributes: Mapping[str, object] | None = None,
    scope_name: str = SCOPE_PIPELINE,
) -> Iterator[Span]:
    """Start a root span and store its context for linking.

    Parameters
    ----------
    name
        Span name.
    attributes
        Span attributes.
    scope_name
        Instrumentation scope name.

    Yields
    ------
    Span
        The started span.
    """
    tracer = get_tracer(scope_name)
    with tracer.start_as_current_span(name, attributes=span_attributes(attrs=attributes)) as span:
        token = _ROOT_SPAN_CONTEXT.set(span.get_span_context())
        try:
            yield span
        finally:
            _ROOT_SPAN_CONTEXT.reset(token)


__all__ = [
    "get_tracer",
    "record_exception",
    "root_span",
    "root_span_context",
    "root_span_link",
    "set_root_span_context",
    "set_span_attributes",
    "span_attributes",
    "stage_span",
]
