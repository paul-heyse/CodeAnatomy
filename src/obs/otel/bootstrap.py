"""Bootstrap OpenTelemetry providers for CodeAnatomy."""

from __future__ import annotations

import contextlib
import importlib
import logging
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from importlib.metadata import PackageNotFoundError, version
from typing import TYPE_CHECKING, Protocol, cast

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
from opentelemetry.sdk.metrics._internal.aggregation import Aggregation, AggregationTemporality
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

from obs.otel.config import OtelConfigOverrides, resolve_otel_config
from obs.otel.metrics import metric_views, reset_metrics_registry
from obs.otel.resource_detectors import (
    build_detected_resource,
    merge_resource_overrides,
    resolve_service_instance_id,
)
from obs.otel.resources import ResourceOptions, build_resource, resolve_service_name
from utils.env_utils import env_bool_strict, env_text, env_value

if TYPE_CHECKING:
    from opentelemetry.sdk.metrics._internal.exemplar.exemplar_filter import ExemplarFilter
    from opentelemetry.sdk.metrics.export import MetricReader
    from opentelemetry.sdk.metrics.view import View

    from obs.otel.config import OtelConfig


class _Instrumentor(Protocol):
    def instrument(self, *args: object, **kwargs: object) -> None: ...


_LOGGER = logging.getLogger(__name__)


class ConfiguredMeterProvider(MeterProvider):
    """MeterProvider subclass that exposes the configured exemplar filter."""

    exemplar_filter: ExemplarFilter | None

    def __init__(
        self,
        metric_readers: Sequence[MetricReader] = (),
        resource: Resource | None = None,
        *,
        exemplar_filter: ExemplarFilter | None = None,
        shutdown_on_exit: bool = True,
        views: Sequence[View] = (),
    ) -> None:
        super().__init__(
            metric_readers=metric_readers,
            resource=resource,
            exemplar_filter=exemplar_filter,
            shutdown_on_exit=shutdown_on_exit,
            views=views,
        )
        self.exemplar_filter = exemplar_filter


def _resolve_service_version() -> str | None:
    env_version = env_value("CODEANATOMY_SERVICE_VERSION")
    if env_version:
        return env_version
    for package in ("codeanatomy", "codeanatomy-engine"):
        try:
            return version(package)
        except PackageNotFoundError:
            continue
    return None


def _resolve_service_namespace() -> str | None:
    return env_value("CODEANATOMY_SERVICE_NAMESPACE")


def _resolve_environment() -> str | None:
    env_current = env_value("CODEANATOMY_ENVIRONMENT")
    if env_current:
        return env_current
    return env_value("DEPLOYMENT_ENVIRONMENT")


def _resolve_sampling_rule() -> str:
    return env_value("CODEANATOMY_OTEL_SAMPLING_RULE") or "codeanatomy.default"


def _apply_otlp_env_overrides(options: OtelBootstrapOptions | None) -> None:
    if options is None:
        return
    endpoint = options.otlp_endpoint
    if isinstance(endpoint, str) and endpoint.strip():
        value = endpoint.strip()
        os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"] = value
        os.environ["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"] = value
        os.environ["OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"] = value
        os.environ["OTEL_EXPORTER_OTLP_LOGS_ENDPOINT"] = value
    protocol = options.otlp_protocol
    if isinstance(protocol, str) and protocol.strip():
        value = protocol.strip()
        os.environ["OTEL_EXPORTER_OTLP_PROTOCOL"] = value
        os.environ["OTEL_EXPORTER_OTLP_TRACES_PROTOCOL"] = value
        os.environ["OTEL_EXPORTER_OTLP_METRICS_PROTOCOL"] = value
        os.environ["OTEL_EXPORTER_OTLP_LOGS_PROTOCOL"] = value


def _config_overrides_from_options(
    options: OtelBootstrapOptions | None,
) -> OtelConfigOverrides | None:
    if options is None:
        return None
    if not any(
        value is not None
        for value in (
            options.sampler,
            options.sampler_arg,
            options.metric_export_interval_ms,
            options.metric_export_timeout_ms,
            options.bsp_schedule_delay_ms,
            options.bsp_export_timeout_ms,
            options.bsp_max_queue_size,
            options.bsp_max_export_batch_size,
            options.blrp_schedule_delay_ms,
            options.blrp_export_timeout_ms,
            options.blrp_max_queue_size,
            options.blrp_max_export_batch_size,
        )
    ):
        return None
    return OtelConfigOverrides(
        sampler=options.sampler,
        sampler_arg=options.sampler_arg,
        metric_export_interval_ms=options.metric_export_interval_ms,
        metric_export_timeout_ms=options.metric_export_timeout_ms,
        bsp_schedule_delay_ms=options.bsp_schedule_delay_ms,
        bsp_export_timeout_ms=options.bsp_export_timeout_ms,
        bsp_max_queue_size=options.bsp_max_queue_size,
        bsp_max_export_batch_size=options.bsp_max_export_batch_size,
        blrp_schedule_delay_ms=options.blrp_schedule_delay_ms,
        blrp_export_timeout_ms=options.blrp_export_timeout_ms,
        blrp_max_queue_size=options.blrp_max_queue_size,
        blrp_max_export_batch_size=options.blrp_max_export_batch_size,
    )


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
    otlp_endpoint: str | None = None
    otlp_protocol: str | None = None
    sampler: str | None = None
    sampler_arg: float | None = None
    metric_export_interval_ms: int | None = None
    metric_export_timeout_ms: int | None = None
    bsp_schedule_delay_ms: int | None = None
    bsp_export_timeout_ms: int | None = None
    bsp_max_queue_size: int | None = None
    bsp_max_export_batch_size: int | None = None
    blrp_schedule_delay_ms: int | None = None
    blrp_export_timeout_ms: int | None = None
    blrp_max_queue_size: int | None = None
    blrp_max_export_batch_size: int | None = None
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
    protocol = env_text(key, allow_empty=True, strip=False) or env_text(
        "OTEL_EXPORTER_OTLP_PROTOCOL",
        allow_empty=True,
        strip=False,
    )
    return (protocol or "grpc").strip().lower()


def _build_span_exporter() -> SpanExporter:
    protocol = _resolve_protocol("traces")
    if protocol.startswith("http"):
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

        return OTLPSpanExporter()
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

    return OTLPSpanExporter()


def _metric_export_preferences(
    config: OtelConfig,
) -> tuple[
    dict[type, AggregationTemporality] | None,
    dict[type, Aggregation] | None,
]:
    preferred_temporality: dict[type, AggregationTemporality] | None = None
    preferred_aggregation: dict[type, Aggregation] | None = None
    if config.metrics_temporality_preference is not None:
        from opentelemetry.sdk.metrics._internal import instrument

        preferred_temporality = {
            instrument.Counter: config.metrics_temporality_preference,
            instrument.UpDownCounter: config.metrics_temporality_preference,
            instrument.Histogram: config.metrics_temporality_preference,
            instrument.ObservableCounter: config.metrics_temporality_preference,
            instrument.ObservableUpDownCounter: config.metrics_temporality_preference,
            instrument.ObservableGauge: config.metrics_temporality_preference,
            instrument.Gauge: config.metrics_temporality_preference,
        }
    if config.metrics_histogram_aggregation is not None:
        from opentelemetry.sdk.metrics._internal import instrument

        preferred_aggregation = {
            instrument.Histogram: config.metrics_histogram_aggregation,
        }
    return preferred_temporality, preferred_aggregation


def _build_metric_exporter(config: OtelConfig) -> MetricExporter:
    protocol = _resolve_protocol("metrics")
    preferred_temporality, preferred_aggregation = _metric_export_preferences(config)
    if protocol.startswith("http"):
        from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter

        return OTLPMetricExporter(
            preferred_temporality=preferred_temporality,
            preferred_aggregation=preferred_aggregation,
        )
    from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter

    return OTLPMetricExporter(
        preferred_temporality=preferred_temporality,
        preferred_aggregation=preferred_aggregation,
    )


def _build_log_exporter() -> LogRecordExporter:
    protocol = _resolve_protocol("logs")
    if protocol.startswith("http"):
        from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter

        return OTLPLogExporter()
    from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter

    return OTLPLogExporter()


def _configure_otel_log_level(level: int | None) -> None:
    if level is None:
        return
    logging.getLogger("opentelemetry").setLevel(level)
    logging.getLogger("opentelemetry.sdk").setLevel(level)


def _logging_auto_instrumentation_enabled() -> bool:
    return env_bool_strict(
        "OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED",
        default=False,
    )


def _install_logging_handler(logger_provider: LoggerProvider) -> None:
    if _logging_auto_instrumentation_enabled():
        _LOGGER.info("Skipping LoggingHandler; auto-instrumentation logging is enabled.")
        return
    root = logging.getLogger()
    for handler in root.handlers:
        if isinstance(handler, LoggingHandler):
            return
    root.addHandler(LoggingHandler(logger_provider=logger_provider))
    if root.level == logging.NOTSET:
        root.setLevel(logging.INFO)


def _set_propagators() -> None:
    raw = env_text("OTEL_PROPAGATORS", allow_empty=True, strip=False)
    if raw:
        return
    set_global_textmap(
        CompositePropagator([TraceContextTextMapPropagator(), W3CBaggagePropagator()])
    )


def _providers_from_globals(resource: Resource) -> OtelProviders | None:
    tracer_provider = trace.get_tracer_provider()
    meter_provider = metrics.get_meter_provider()
    logger_provider = get_logger_provider()
    tracer = tracer_provider if isinstance(tracer_provider, TracerProvider) else None
    meter = meter_provider if isinstance(meter_provider, MeterProvider) else None
    logger = logger_provider if isinstance(logger_provider, LoggerProvider) else None
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
    resolved_service_name = service_name or resolve_service_name()
    overrides = options or OtelBootstrapOptions()
    config = resolve_otel_config(_config_overrides_from_options(overrides))
    base_resource = build_resource(
        resolved_service_name,
        ResourceOptions(
            service_version=overrides.service_version or _resolve_service_version(),
            service_namespace=overrides.service_namespace or _resolve_service_namespace(),
            environment=overrides.environment or _resolve_environment(),
            instance_id=resolve_service_instance_id(),
            attributes=None,
        ),
    )
    resource = merge_resource_overrides(
        build_detected_resource(base_resource),
        overrides.resource_overrides,
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
    from obs.otel.sampling import wrap_sampler

    sampler = wrap_sampler(config.sampler, rule=_resolve_sampling_rule())
    tracer_provider = TracerProvider(
        resource=resource,
        sampler=sampler,
        id_generator=config.id_generator,
        span_limits=span_limits,
    )
    from obs.otel.processors import RunIdSpanProcessor

    tracer_provider.add_span_processor(RunIdSpanProcessor())
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
            _build_metric_exporter(config),
            export_interval_millis=config.metric_export_interval_ms,
            export_timeout_millis=config.metric_export_timeout_ms,
        )
    meter_provider = ConfiguredMeterProvider(
        resource=resource,
        metric_readers=[reader],
        exemplar_filter=config.metrics_exemplar_filter,
        views=metric_views(),
    )
    reset_metrics_registry()
    return meter_provider


def build_meter_provider(
    config: OtelConfig,
    resource: Resource,
    *,
    use_test_mode: bool,
) -> MeterProvider:
    """Build a ``MeterProvider`` from the resolved OpenTelemetry configuration.

    Parameters
    ----------
    config
        Resolved OpenTelemetry configuration.
    resource
        Resource describing the service instance.
    use_test_mode
        Whether to use test-mode exporters/processors.

    Returns
    -------
    MeterProvider
        Configured meter provider.
    """
    return _build_meter_provider(config, resource, use_test_mode=use_test_mode)


def _build_logger_provider(
    config: OtelConfig,
    resource: Resource,
    *,
    use_test_mode: bool,
    log_correlation: bool,
) -> LoggerProvider:
    logger_provider = LoggerProvider(resource=resource)
    from obs.otel.processors import RunIdLogRecordProcessor

    logger_provider.add_log_record_processor(RunIdLogRecordProcessor())
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
        module = importlib.import_module("opentelemetry.instrumentation.logging")
    except ImportError:
        _LOGGER.warning("OpenTelemetry logging instrumentation is unavailable.")
        return
    instrumentor = getattr(module, "LoggingInstrumentor", None)
    if instrumentor is None:
        _LOGGER.warning("OpenTelemetry logging instrumentation is unavailable.")
        return
    instrumentor_type = cast("type[_Instrumentor]", instrumentor)
    instrumentor_type().instrument(set_logging_format=False)
    _LOGGER.info("OpenTelemetry log correlation enabled")


def _enable_system_metrics() -> None:
    try:
        module = importlib.import_module("opentelemetry.instrumentation.system_metrics")
    except ImportError:
        _LOGGER.warning("OpenTelemetry system metrics instrumentation is unavailable.")
        return
    instrumentor = getattr(module, "SystemMetricsInstrumentor", None)
    if instrumentor is None:
        _LOGGER.warning("OpenTelemetry system metrics instrumentation is unavailable.")
        return
    instrumentor_type = cast("type[_Instrumentor]", instrumentor)
    instrumentor_type().instrument()


def configure_otel(
    *,
    service_name: str | None = None,
    options: OtelBootstrapOptions | None = None,
) -> OtelProviders:
    """Configure OpenTelemetry providers for the current process.

    Raises
    ------
    RuntimeError
        Raised when OTEL_EXPERIMENTAL_CONFIG_FILE is set but does not configure providers.

    Returns
    -------
    OtelProviders
        Configured providers for traces, metrics, and logs.
    """
    if options is not None and options.test_mode and _STATE["providers"] is not None:
        _STATE["providers"].shutdown()
        _STATE["providers"] = None
    if _STATE["providers"] is not None:
        return _STATE["providers"]
    _apply_otlp_env_overrides(options)
    _set_propagators()
    state = _resolve_bootstrap_state(service_name, options)
    _configure_otel_log_level(state.config.otel_log_level)
    if state.config.python_context:
        _LOGGER.info("OTEL_PYTHON_CONTEXT=%s", state.config.python_context)
    if state.config.config_file:
        existing = _providers_from_globals(state.resource)
        if existing is not None:
            _STATE["providers"] = existing
            _LOGGER.info(
                "OpenTelemetry providers already configured; honoring OTEL_EXPERIMENTAL_CONFIG_FILE."
            )
            return existing
        msg = (
            "OTEL_EXPERIMENTAL_CONFIG_FILE is set but no providers were configured. "
            "Configure providers via the OpenTelemetry configuration file or unset the variable."
        )
        raise RuntimeError(msg)
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
        os.environ.setdefault(
            "OTEL_PYTHON_DISABLED_INSTRUMENTATIONS",
            "urllib,urllib3",
        )
        os.environ.setdefault("OTEL_PYTHON_EXCLUDED_URLS", ".*health.*")
        if env_text("OTEL_SEMCONV_STABILITY_OPT_IN", allow_empty=True) is None:
            os.environ["OTEL_SEMCONV_STABILITY_OPT_IN"] = "http,db"
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


def reset_providers_for_tests() -> None:
    """Reset global providers for test isolation."""
    providers = _STATE["providers"]
    if providers is not None:
        providers.shutdown()
    _STATE["providers"] = None
    from opentelemetry._logs import _internal as logs_internal
    from opentelemetry.metrics import _internal as metrics_internal

    def _reset_once(holder: object | None) -> None:
        if holder is None:
            return
        with contextlib.suppress(AttributeError):
            holder._done = False

    def _reset_proxy_meter(proxy: object | None) -> None:
        if proxy is None:
            return
        with contextlib.suppress(AttributeError):
            proxy._real_meter_provider = None
        meters = getattr(proxy, "_meters", None)
        if hasattr(meters, "clear"):
            meters.clear()

    _reset_once(getattr(trace, "_TRACER_PROVIDER_SET_ONCE", None))
    _reset_once(getattr(metrics_internal, "_METER_PROVIDER_SET_ONCE", None))
    _reset_proxy_meter(getattr(metrics_internal, "_PROXY_METER_PROVIDER", None))
    _reset_once(getattr(logs_internal, "_LOGGER_PROVIDER_SET_ONCE", None))
    trace._TRACER_PROVIDER = None
    metrics_internal._METER_PROVIDER = None
    logs_internal._LOGGER_PROVIDER = None
    reset_metrics_registry()


__all__ = [
    "ConfiguredMeterProvider",
    "OtelBootstrapOptions",
    "OtelProviders",
    "build_meter_provider",
    "configure_otel",
    "reset_providers_for_tests",
]
