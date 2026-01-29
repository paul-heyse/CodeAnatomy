"""Test harness for OpenTelemetry contracts."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from opentelemetry import metrics, trace
from opentelemetry._logs import set_logger_provider
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import InMemoryLogRecordExporter, SimpleLogRecordProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from obs.otel.metrics import metric_views, reset_metrics_registry


@dataclass
class OtelTestHarness:
    """Container for in-memory OTel exporters used in tests."""

    span_exporter: InMemorySpanExporter
    metric_reader: InMemoryMetricReader
    log_exporter: InMemoryLogRecordExporter

    def reset(self) -> None:
        """Clear collected telemetry between tests."""
        self.span_exporter.clear()
        self.log_exporter.clear()


def _install_logging_handler(logger_provider: LoggerProvider) -> None:
    root = logging.getLogger()
    for handler in root.handlers:
        if isinstance(handler, LoggingHandler):
            return
    root.addHandler(LoggingHandler(logger_provider=logger_provider))
    if root.level == logging.NOTSET:
        root.setLevel(logging.INFO)


_HARNESS_CACHE: dict[str, OtelTestHarness | None] = {"value": None}


def get_otel_harness() -> OtelTestHarness:
    """Return a singleton OTel test harness with in-memory exporters.

    Returns
    -------
    OtelTestHarness
        Configured in-memory exporters for traces, metrics, and logs.
    """
    cached = _HARNESS_CACHE["value"]
    if cached is not None:
        return cached
    resource = Resource.create({"service.name": "codeanatomy-tests"})

    span_exporter = InMemorySpanExporter()
    tracer_provider = TracerProvider(resource=resource)
    tracer_provider.add_span_processor(SimpleSpanProcessor(span_exporter))
    trace.set_tracer_provider(tracer_provider)

    metric_reader = InMemoryMetricReader()
    meter_provider = MeterProvider(
        resource=resource,
        metric_readers=[metric_reader],
        views=metric_views(),
    )
    metrics.set_meter_provider(meter_provider)
    reset_metrics_registry()

    log_exporter = InMemoryLogRecordExporter()
    logger_provider = LoggerProvider(resource=resource)
    logger_provider.add_log_record_processor(SimpleLogRecordProcessor(log_exporter))
    set_logger_provider(logger_provider)
    _install_logging_handler(logger_provider)

    harness = OtelTestHarness(
        span_exporter=span_exporter,
        metric_reader=metric_reader,
        log_exporter=log_exporter,
    )
    _HARNESS_CACHE["value"] = harness
    return harness


__all__ = ["OtelTestHarness", "get_otel_harness"]
