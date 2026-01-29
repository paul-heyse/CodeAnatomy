"""Bootstrap OpenTelemetry providers for CodeAnatomy."""

from __future__ import annotations

import logging
import os
from collections.abc import Mapping
from dataclasses import dataclass
from importlib.metadata import PackageNotFoundError, version
from typing import TYPE_CHECKING

from opentelemetry import metrics, trace
from opentelemetry._logs import get_logger_provider, set_logger_provider
from opentelemetry.baggage.propagation import W3CBaggagePropagator
from opentelemetry.propagate import set_global_textmap
from opentelemetry.propagators.composite import CompositePropagator
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import (
    BatchLogRecordProcessor,
    InMemoryLogRecordExporter,
    LogRecordExporter,
    SimpleLogRecordProcessor,
)
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    InMemoryMetricReader,
    MetricExporter,
    PeriodicExportingMetricReader,
)
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import SpanLimits, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SimpleSpanProcessor, SpanExporter
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from obs.otel.config import resolve_otel_config
from obs.otel.metrics import metric_views, reset_metrics_registry
from obs.otel.resources import build_resource, resolve_service_name

if TYPE_CHECKING:
    from obs.otel.config import OtelConfig

_LOGGER = logging.getLogger(__name__)


def _resolve_service_version() -> str | None:
    env_version = os.environ.get("CODEANATOMY_SERVICE_VERSION")
    if env_version:
        return env_version.strip()
    for package in ("codeanatomy", "codeanatomy-engine"):
        try:
            return version(package)
        except PackageNotFoundError:
            continue
    return None


def _resolve_service_namespace() -> str | None:
    env_namespace = os.environ.get("CODEANATOMY_SERVICE_NAMESPACE")
    return env_namespace.strip() if env_namespace else None


def _resolve_environment() -> str | None:
    env_value = os.environ.get("CODEANATOMY_ENVIRONMENT")
    if env_value:
        return env_value.strip()
    alt = os.environ.get("DEPLOYMENT_ENVIRONMENT")
    return alt.strip() if alt else None


@dataclass(frozen=True)
class OtelProviders:
    """Container for configured OpenTelemetry providers."""

    resource: Resource
    tracer_provider: TracerProvider | None
    meter_provider: MeterProvider | None
    logger_provider: LoggerProvider | None

    def activate_global(self) -> None:
        """Activate providers as global defaults."""
        if self.tracer_provider is not None:
            trace.set_tracer_provider(self.tracer_provider)
        if self.meter_provider is not None:
            metrics.set_meter_provider(self.meter_provider)
        if self.logger_provider is not None:
            set_logger_provider(self.logger_provider)

    def shutdown(self) -> None:
        """Shutdown all configured providers."""
        if self.tracer_provider is not None:
            self.tracer_provider.shutdown()
        if self.meter_provider is not None:
            self.meter_provider.shutdown()
        if self.logger_provider is not None:
            self.logger_provider.shutdown()


@dataclass(frozen=True)
class OtelBootstrapOptions:
    """Optional overrides for bootstrap configuration."""

    service_version: str | None = None
    service_namespace: str | None = None
    environment: str | None = None
    resource_overrides: Mapping[str, str] | None = None
    enable_traces: bool | None = None
    enable_metrics: bool | None = None
    enable_logs: bool | None = None
    enable_log_correlation: bool | None = None
    enable_auto_instrumentation: bool | None = None
    test_mode: bool | None = None
    enable_system_metrics: bool | None = None


@dataclass(frozen=True)
class _ResolvedBootstrapState:
    config: OtelConfig
    resolved_service_name: str
    resource: Resource
    traces_enabled: bool
    metrics_enabled: bool
    logs_enabled: bool
    log_correlation: bool
    use_test_mode: bool
    enable_auto: bool
    enable_system_metrics: bool


_STATE: dict[str, OtelProviders | None] = {"providers": None}


def _resolve_protocol(signal: str) -> str:
    key = f"OTEL_EXPORTER_OTLP_{signal.upper()}_PROTOCOL"
    protocol = os.environ.get(key) or os.environ.get("OTEL_EXPORTER_OTLP_PROTOCOL")
    return (protocol or "grpc").strip().lower()


def _build_span_exporter() -> SpanExporter:
    protocol = _resolve_protocol("traces")
    if protocol.startswith("http"):
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

        return OTLPSpanExporter()
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

    return OTLPSpanExporter()


def _build_metric_exporter() -> MetricExporter:
    protocol = _resolve_protocol("metrics")
    if protocol.startswith("http"):
        from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter

        return OTLPMetricExporter()
    from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter

    return OTLPMetricExporter()


def _build_log_exporter() -> LogRecordExporter:
    protocol = _resolve_protocol("logs")
    if protocol.startswith("http"):
        from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter

        return OTLPLogExporter()
    from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter

    return OTLPLogExporter()


def _install_logging_handler(logger_provider: LoggerProvider) -> None:
    root = logging.getLogger()
    for handler in root.handlers:
        if isinstance(handler, LoggingHandler):
            return
    root.addHandler(LoggingHandler(logger_provider=logger_provider))
    if root.level == logging.NOTSET:
        root.setLevel(logging.INFO)


def _set_propagators() -> None:
    if os.environ.get("OTEL_PROPAGATORS"):
        return
    set_global_textmap(
        CompositePropagator([TraceContextTextMapPropagator(), W3CBaggagePropagator()])
    )


def _providers_from_globals(resource: Resource) -> OtelProviders | None:
    tracer_provider = trace.get_tracer_provider()
    meter_provider = metrics.get_meter_provider()
    logger_provider = get_logger_provider()
    tracer = None if tracer_provider.__class__.__name__ == "ProxyTracerProvider" else tracer_provider
    meter = None if meter_provider.__class__.__name__ == "_ProxyMeterProvider" else meter_provider
    logger = None if logger_provider.__class__.__name__ == "ProxyLoggerProvider" else logger_provider
    if tracer is None and meter is None and logger is None:
        return None
    return OtelProviders(
        resource=resource,
        tracer_provider=tracer,
        meter_provider=meter,
        logger_provider=logger,
    )


def _resolve_bootstrap_state(
    service_name: str | None,
    options: OtelBootstrapOptions | None,
) -> _ResolvedBootstrapState:
    config = resolve_otel_config()
    resolved_service_name = service_name or resolve_service_name()
    overrides = options or OtelBootstrapOptions()
    resource = build_resource(
        service_name=resolved_service_name,
        service_version=overrides.service_version or _resolve_service_version(),
        service_namespace=overrides.service_namespace or _resolve_service_namespace(),
        environment=overrides.environment or _resolve_environment(),
        attributes=overrides.resource_overrides,
    )
    traces_enabled = (
        config.enable_traces if overrides.enable_traces is None else overrides.enable_traces
    )
    metrics_enabled = (
        config.enable_metrics if overrides.enable_metrics is None else overrides.enable_metrics
    )
    logs_enabled = config.enable_logs if overrides.enable_logs is None else overrides.enable_logs
    log_correlation = (
        config.enable_log_correlation
        if overrides.enable_log_correlation is None
        else overrides.enable_log_correlation
    )
    enable_system_metrics = (
        config.enable_system_metrics
        if overrides.enable_system_metrics is None
        else overrides.enable_system_metrics
    )
    use_test_mode = config.test_mode if overrides.test_mode is None else overrides.test_mode
    enable_auto = (
        config.auto_instrumentation
        if overrides.enable_auto_instrumentation is None
        else overrides.enable_auto_instrumentation
    )
    return _ResolvedBootstrapState(
        config=config,
        resolved_service_name=resolved_service_name,
        resource=resource,
        traces_enabled=traces_enabled,
        metrics_enabled=metrics_enabled,
        logs_enabled=logs_enabled,
        log_correlation=log_correlation,
        use_test_mode=use_test_mode,
        enable_auto=enable_auto,
        enable_system_metrics=enable_system_metrics,
    )


def _build_tracer_provider(
    config: OtelConfig,
    resource: Resource,
    *,
    use_test_mode: bool,
) -> TracerProvider:
    span_limits = SpanLimits(
        max_attributes=config.attribute_count_limit,
        max_attribute_length=config.attribute_value_length_limit,
        max_span_attributes=config.span_attribute_count_limit,
        max_span_attribute_length=config.span_attribute_length_limit,
        max_events=config.span_event_count_limit,
        max_links=config.span_link_count_limit,
        max_event_attributes=config.span_event_attribute_count_limit,
        max_link_attributes=config.span_link_attribute_count_limit,
    )
    tracer_provider = TracerProvider(
        resource=resource,
        sampler=config.sampler,
        span_limits=span_limits,
    )
    if use_test_mode:
        tracer_provider.add_span_processor(SimpleSpanProcessor(InMemorySpanExporter()))
    else:
        tracer_provider.add_span_processor(
            BatchSpanProcessor(
                _build_span_exporter(),
                max_queue_size=config.bsp_max_queue_size,
                schedule_delay_millis=config.bsp_schedule_delay_ms,
                max_export_batch_size=config.bsp_max_export_batch_size,
                export_timeout_millis=config.bsp_export_timeout_ms,
            )
        )
    return tracer_provider


def _build_meter_provider(
    config: OtelConfig,
    resource: Resource,
    *,
    use_test_mode: bool,
) -> MeterProvider:
    if use_test_mode:
        reader = InMemoryMetricReader()
    else:
        reader = PeriodicExportingMetricReader(
            _build_metric_exporter(),
            export_interval_millis=config.metric_export_interval_ms,
            export_timeout_millis=config.metric_export_timeout_ms,
        )
    meter_provider = MeterProvider(
        resource=resource,
        metric_readers=[reader],
        views=metric_views(),
    )
    reset_metrics_registry()
    return meter_provider


def _build_logger_provider(
    config: OtelConfig,
    resource: Resource,
    *,
    use_test_mode: bool,
    log_correlation: bool,
) -> LoggerProvider:
    logger_provider = LoggerProvider(resource=resource)
    if use_test_mode:
        logger_provider.add_log_record_processor(
            SimpleLogRecordProcessor(InMemoryLogRecordExporter())
        )
    else:
        logger_provider.add_log_record_processor(
            BatchLogRecordProcessor(
                _build_log_exporter(),
                max_queue_size=config.blrp_max_queue_size,
                schedule_delay_millis=config.blrp_schedule_delay_ms,
                max_export_batch_size=config.blrp_max_export_batch_size,
                export_timeout_millis=config.blrp_export_timeout_ms,
            )
        )
    _install_logging_handler(logger_provider)
    if log_correlation:
        _enable_log_correlation()
    return logger_provider


def _enable_auto_instrumentation() -> None:
    try:
        from opentelemetry.instrumentation.auto_instrumentation import initialize
    except ImportError:
        _LOGGER.warning("OpenTelemetry auto-instrumentation is unavailable.")
    else:
        initialize()


def _enable_log_correlation() -> None:
    try:
        from opentelemetry.instrumentation.logging import LoggingInstrumentor
    except ImportError:
        _LOGGER.warning("OpenTelemetry logging instrumentation is unavailable.")
    else:
        LoggingInstrumentor().instrument(set_logging_format=False)
        _LOGGER.info("OpenTelemetry log correlation enabled")


def _enable_system_metrics() -> None:
    try:
        from opentelemetry.instrumentation.system_metrics import SystemMetricsInstrumentor
    except ImportError:
        _LOGGER.warning("OpenTelemetry system metrics instrumentation is unavailable.")
    else:
        SystemMetricsInstrumentor().instrument()


def configure_otel(
    *,
    service_name: str | None = None,
    options: OtelBootstrapOptions | None = None,
) -> OtelProviders:
    """Configure OpenTelemetry providers for the current process.

    Returns
    -------
    OtelProviders
        Configured providers for traces, metrics, and logs.
    """
    if _STATE["providers"] is not None:
        return _STATE["providers"]
    _set_propagators()
    state = _resolve_bootstrap_state(service_name, options)
    if state.config.config_file:
        existing = _providers_from_globals(state.resource)
        if existing is not None:
            _STATE["providers"] = existing
            _LOGGER.info(
                "OpenTelemetry providers already configured; honoring OTEL_EXPERIMENTAL_CONFIG_FILE."
            )
            return existing
        _LOGGER.warning(
            "OTEL_EXPERIMENTAL_CONFIG_FILE is set but no providers were configured; continuing with CodeAnatomy bootstrap."
        )
    tracer_provider = (
        _build_tracer_provider(state.config, state.resource, use_test_mode=state.use_test_mode)
        if state.traces_enabled
        else None
    )
    meter_provider = (
        _build_meter_provider(state.config, state.resource, use_test_mode=state.use_test_mode)
        if state.metrics_enabled
        else None
    )
    logger_provider = (
        _build_logger_provider(
            state.config,
            state.resource,
            use_test_mode=state.use_test_mode,
            log_correlation=state.log_correlation,
        )
        if state.logs_enabled
        else None
    )
    if state.enable_auto:
        if os.environ.get("OTEL_SEMCONV_STABILITY_OPT_IN") is None:
            os.environ["OTEL_SEMCONV_STABILITY_OPT_IN"] = "http"
        _enable_auto_instrumentation()

    providers = OtelProviders(
        resource=state.resource,
        tracer_provider=tracer_provider,
        meter_provider=meter_provider,
        logger_provider=logger_provider,
    )
    providers.activate_global()
    _STATE["providers"] = providers
    if state.enable_system_metrics and providers.meter_provider is not None:
        _enable_system_metrics()
    _LOGGER.info("OpenTelemetry configured for service %s", state.resolved_service_name)
    return providers


__all__ = ["OtelBootstrapOptions", "OtelProviders", "configure_otel"]
