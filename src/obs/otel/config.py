"""Configuration helpers for OpenTelemetry bootstrap."""

from __future__ import annotations

import importlib
import importlib.util
import logging
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from importlib.metadata import EntryPoint, entry_points
from typing import cast

from opentelemetry.sdk.metrics._internal.aggregation import (
    Aggregation,
    AggregationTemporality,
    ExplicitBucketHistogramAggregation,
    ExponentialBucketHistogramAggregation,
)
from opentelemetry.sdk.metrics._internal.exemplar.exemplar_filter import (
    AlwaysOffExemplarFilter,
    AlwaysOnExemplarFilter,
    ExemplarFilter,
    TraceBasedExemplarFilter,
)
from opentelemetry.sdk.trace.id_generator import IdGenerator, RandomIdGenerator
from opentelemetry.sdk.trace.sampling import (
    ALWAYS_OFF,
    ALWAYS_ON,
    ParentBased,
    Sampler,
    TraceIdRatioBased,
)

from core.config_base import config_fingerprint
from utils.env_utils import (
    env_bool,
    env_bool_strict,
    env_enum,
    env_float,
    env_int,
    env_text,
    env_value,
)

_LOGGER = logging.getLogger(__name__)


def _type_name(obj: object | None) -> str | None:
    if obj is None:
        return None
    return f"{obj.__class__.__module__}.{obj.__class__.__qualname__}"


@dataclass(frozen=True)
class OtelConfig:
    """Resolved OpenTelemetry configuration."""

    enable_traces: bool
    enable_metrics: bool
    enable_logs: bool
    enable_log_correlation: bool
    enable_system_metrics: bool
    sampler: Sampler
    metric_export_interval_ms: int
    metric_export_timeout_ms: int
    bsp_schedule_delay_ms: int
    bsp_export_timeout_ms: int
    bsp_max_queue_size: int
    bsp_max_export_batch_size: int
    blrp_schedule_delay_ms: int
    blrp_export_timeout_ms: int
    blrp_max_queue_size: int
    blrp_max_export_batch_size: int
    attribute_count_limit: int | None
    attribute_value_length_limit: int | None
    log_record_attribute_count_limit: int | None
    log_record_attribute_value_length_limit: int | None
    span_attribute_count_limit: int | None
    span_attribute_length_limit: int | None
    span_event_count_limit: int | None
    span_link_count_limit: int | None
    span_event_attribute_count_limit: int | None
    span_link_attribute_count_limit: int | None
    metrics_exemplar_filter: ExemplarFilter | None
    metrics_temporality_preference: AggregationTemporality | None
    metrics_histogram_aggregation: Aggregation | None
    otel_log_level: int | None
    python_context: str | None
    id_generator: IdGenerator | None
    test_mode: bool
    auto_instrumentation: bool
    config_file: str | None

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return canonical payload for fingerprinting.

        Returns
        -------
        Mapping[str, object]
            Payload used for configuration fingerprinting.
        """
        return {
            "enable_traces": self.enable_traces,
            "enable_metrics": self.enable_metrics,
            "enable_logs": self.enable_logs,
            "enable_log_correlation": self.enable_log_correlation,
            "enable_system_metrics": self.enable_system_metrics,
            "sampler": _type_name(self.sampler),
            "metric_export_interval_ms": self.metric_export_interval_ms,
            "metric_export_timeout_ms": self.metric_export_timeout_ms,
            "bsp_schedule_delay_ms": self.bsp_schedule_delay_ms,
            "bsp_export_timeout_ms": self.bsp_export_timeout_ms,
            "bsp_max_queue_size": self.bsp_max_queue_size,
            "bsp_max_export_batch_size": self.bsp_max_export_batch_size,
            "blrp_schedule_delay_ms": self.blrp_schedule_delay_ms,
            "blrp_export_timeout_ms": self.blrp_export_timeout_ms,
            "blrp_max_queue_size": self.blrp_max_queue_size,
            "blrp_max_export_batch_size": self.blrp_max_export_batch_size,
            "attribute_count_limit": self.attribute_count_limit,
            "attribute_value_length_limit": self.attribute_value_length_limit,
            "log_record_attribute_count_limit": self.log_record_attribute_count_limit,
            "log_record_attribute_value_length_limit": self.log_record_attribute_value_length_limit,
            "span_attribute_count_limit": self.span_attribute_count_limit,
            "span_attribute_length_limit": self.span_attribute_length_limit,
            "span_event_count_limit": self.span_event_count_limit,
            "span_link_count_limit": self.span_link_count_limit,
            "span_event_attribute_count_limit": self.span_event_attribute_count_limit,
            "span_link_attribute_count_limit": self.span_link_attribute_count_limit,
            "metrics_exemplar_filter": _type_name(self.metrics_exemplar_filter),
            "metrics_temporality_preference": (
                str(self.metrics_temporality_preference)
                if self.metrics_temporality_preference is not None
                else None
            ),
            "metrics_histogram_aggregation": _type_name(self.metrics_histogram_aggregation),
            "otel_log_level": self.otel_log_level,
            "python_context": self.python_context,
            "id_generator": _type_name(self.id_generator),
            "test_mode": self.test_mode,
            "auto_instrumentation": self.auto_instrumentation,
            "config_file": self.config_file,
        }

    def fingerprint(self) -> str:
        """Return a stable fingerprint for the config.

        Returns
        -------
        str
            Fingerprint string for the configuration.
        """
        return config_fingerprint(self.fingerprint_payload())


@dataclass(frozen=True)
class _BatchProcessorSettings:
    schedule_delay_ms: int
    export_timeout_ms: int
    max_queue_size: int
    max_export_batch_size: int


def _parse_log_level(raw: str | None) -> int | None:
    if raw is None:
        return None
    value = raw.strip().upper()
    if not value:
        return None
    level = _LOG_LEVEL_MAP.get(value)
    if level is not None:
        return level
    _LOGGER.warning("Invalid OTEL_LOG_LEVEL: %r", raw)
    return None


_LOG_LEVEL_MAP = {
    "CRITICAL": logging.CRITICAL,
    "FATAL": logging.FATAL,
    "ERROR": logging.ERROR,
    "WARNING": logging.WARNING,
    "WARN": logging.WARNING,
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG,
    "NOTSET": logging.NOTSET,
}


def _resolve_exemplar_filter() -> ExemplarFilter | None:
    raw = env_value("OTEL_METRICS_EXEMPLAR_FILTER")
    if raw is None:
        return None
    name = raw.lower()
    if name == "always_on":
        return AlwaysOnExemplarFilter()
    if name == "always_off":
        return AlwaysOffExemplarFilter()
    if name in {"trace_based", "tracebased"}:
        return TraceBasedExemplarFilter()
    _LOGGER.warning("Invalid OTEL_METRICS_EXEMPLAR_FILTER: %r", raw)
    return None


def _resolve_metrics_temporality() -> AggregationTemporality | None:
    raw = env_value("OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE")
    if raw is None:
        return None
    name = raw.lower()
    if name == "lowmemory":
        return AggregationTemporality.DELTA
    value = env_enum(
        "OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE",
        AggregationTemporality,
    )
    if value is None:
        _LOGGER.warning(
            "Invalid OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE: %r",
            raw,
        )
        return None
    return value


def _resolve_metrics_histogram_aggregation() -> Aggregation | None:
    raw = env_value("OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION")
    if raw is None:
        return None
    name = raw.lower()
    if name == "explicit_bucket_histogram":
        return ExplicitBucketHistogramAggregation()
    if name in {"base2_exponential_bucket_histogram", "exponential_bucket_histogram"}:
        return ExponentialBucketHistogramAggregation()
    _LOGGER.warning(
        "Invalid OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION: %r",
        raw,
    )
    return None


def _resolve_id_generator() -> IdGenerator | None:
    raw = env_value("OTEL_PYTHON_ID_GENERATOR")
    name = raw.lower() if raw is not None else ""
    if not name:
        return None
    if name in {"random", "default"}:
        return RandomIdGenerator()
    if name == "xray":
        return _resolve_xray_id_generator()
    _LOGGER.warning("Invalid OTEL_PYTHON_ID_GENERATOR: %r", raw)
    return None


def _resolve_xray_id_generator() -> IdGenerator | None:
    module = importlib.util.find_spec("opentelemetry.sdk.extension.aws.trace")
    if module is None:
        _LOGGER.warning(
            "OTEL_PYTHON_ID_GENERATOR=xray requested but opentelemetry-sdk-extension-aws is unavailable."
        )
        return None
    trace_mod = importlib.import_module("opentelemetry.sdk.extension.aws.trace")
    generator_type = getattr(trace_mod, "XRayIdGenerator", None)
    if generator_type is None:
        _LOGGER.warning(
            "OTEL_PYTHON_ID_GENERATOR=xray requested but XRayIdGenerator was not found."
        )
        return None
    return cast("IdGenerator", generator_type())


def _resolve_batch_sizes(
    *,
    max_queue_size: int,
    max_export_batch_size: int,
    label: str,
) -> tuple[int, int]:
    if max_queue_size <= 0:
        _LOGGER.warning(
            "%s max queue size must be positive; defaulting to 2048.",
            label,
        )
        max_queue_size = 2048
    if max_export_batch_size <= 0:
        _LOGGER.warning(
            "%s max export batch size must be positive; defaulting to 512.",
            label,
        )
        max_export_batch_size = 512
    if max_export_batch_size > max_queue_size:
        _LOGGER.warning(
            "%s max export batch size (%s) exceeds max queue size (%s); using queue size.",
            label,
            max_export_batch_size,
            max_queue_size,
        )
        max_export_batch_size = max_queue_size
    return max_queue_size, max_export_batch_size


def _exporter_enabled(env_name: str, *, default: bool = True) -> bool:
    value = env_value(env_name)
    if value is None:
        return default
    return value.lower() != "none"


def _resolve_sampler() -> Sampler:
    sampler_name = env_text(
        "OTEL_TRACES_SAMPLER",
        default="parentbased_traceidratio",
        allow_empty=True,
    )
    name = sampler_name.strip().lower() if sampler_name is not None else ""
    ratio = env_float("OTEL_TRACES_SAMPLER_ARG", default=0.1)
    sampler = _resolve_builtin_sampler(name, ratio)
    if sampler is None:
        sampler = _resolve_entrypoint_sampler(name, env_value("OTEL_TRACES_SAMPLER_ARG"))
    if sampler is None:
        _LOGGER.warning(
            "Unsupported sampler %s; defaulting to parentbased_traceidratio.",
            name,
        )
        sampler = ParentBased(TraceIdRatioBased(ratio))
    return sampler


def _resolve_builtin_sampler(name: str, ratio: float) -> Sampler | None:
    if name in {"always_on", "parentbased_always_on"}:
        sampler: Sampler = ALWAYS_ON
    elif name in {"always_off", "parentbased_always_off"}:
        sampler = ALWAYS_OFF
    elif "traceidratio" in name:
        sampler = TraceIdRatioBased(ratio)
    else:
        return None
    if name.startswith("parentbased"):
        return ParentBased(sampler)
    return sampler


def _resolve_entrypoint_sampler(name: str, arg: str | None) -> Sampler | None:
    eps = _entry_points("opentelemetry_traces_sampler")
    sampler_arg = arg or ""
    for entry in eps:
        if entry.name.lower() != name:
            continue
        try:
            factory = entry.load()
        except (ImportError, AttributeError) as exc:
            _LOGGER.warning("Failed to load sampler entrypoint %s: %s", entry.name, exc)
            return None
        try:
            sampler = factory(sampler_arg)
        except (RuntimeError, TypeError, ValueError) as exc:
            _LOGGER.warning("Sampler factory %s failed: %s", entry.name, exc)
            return None
        if isinstance(sampler, Sampler):
            return sampler
        _LOGGER.warning("Sampler entrypoint %s returned invalid type.", entry.name)
        return None
    return None


def _entry_points(group: str) -> list[EntryPoint]:
    eps = entry_points()
    if hasattr(eps, "select"):
        return list(eps.select(group=group))
    getter = getattr(eps, "get", None)
    if callable(getter):
        return list(cast("Iterable[EntryPoint]", getter(group, ())))
    return []


def _resolve_batch_settings(
    *,
    schedule_delay_ms: int,
    export_timeout_ms: int,
    max_queue_size: int,
    max_export_batch_size: int,
    label: str,
) -> _BatchProcessorSettings:
    queue_size, batch_size = _resolve_batch_sizes(
        max_queue_size=max_queue_size,
        max_export_batch_size=max_export_batch_size,
        label=label,
    )
    return _BatchProcessorSettings(
        schedule_delay_ms=schedule_delay_ms,
        export_timeout_ms=export_timeout_ms,
        max_queue_size=queue_size,
        max_export_batch_size=batch_size,
    )


def _resolve_disabled_config() -> OtelConfig:
    return OtelConfig(
        enable_traces=False,
        enable_metrics=False,
        enable_logs=False,
        enable_log_correlation=False,
        enable_system_metrics=False,
        sampler=ALWAYS_OFF,
        metric_export_interval_ms=60000,
        metric_export_timeout_ms=30000,
        bsp_schedule_delay_ms=5000,
        bsp_export_timeout_ms=30000,
        bsp_max_queue_size=2048,
        bsp_max_export_batch_size=512,
        blrp_schedule_delay_ms=5000,
        blrp_export_timeout_ms=30000,
        blrp_max_queue_size=2048,
        blrp_max_export_batch_size=512,
        attribute_count_limit=None,
        attribute_value_length_limit=None,
        log_record_attribute_count_limit=None,
        log_record_attribute_value_length_limit=None,
        span_attribute_count_limit=None,
        span_attribute_length_limit=None,
        span_event_count_limit=None,
        span_link_count_limit=None,
        span_event_attribute_count_limit=None,
        span_link_attribute_count_limit=None,
        metrics_exemplar_filter=None,
        metrics_temporality_preference=None,
        metrics_histogram_aggregation=None,
        otel_log_level=_parse_log_level(env_value("OTEL_LOG_LEVEL")),
        python_context=env_value("OTEL_PYTHON_CONTEXT"),
        id_generator=_resolve_id_generator(),
        test_mode=env_bool("CODEANATOMY_OTEL_TEST_MODE", default=False, on_invalid="false"),
        auto_instrumentation=env_bool(
            "CODEANATOMY_OTEL_AUTO_INSTRUMENTATION",
            default=False,
            on_invalid="false",
        ),
        config_file=env_value("OTEL_EXPERIMENTAL_CONFIG_FILE"),
    )


def _resolve_enabled_config() -> OtelConfig:
    enable_traces = _exporter_enabled("OTEL_TRACES_EXPORTER", default=True)
    enable_metrics = _exporter_enabled("OTEL_METRICS_EXPORTER", default=True)
    enable_logs = _exporter_enabled("OTEL_LOGS_EXPORTER", default=True)
    enable_log_correlation = env_bool_strict("OTEL_PYTHON_LOG_CORRELATION", default=True)
    enable_system_metrics = env_bool(
        "CODEANATOMY_OTEL_SYSTEM_METRICS",
        default=False,
        on_invalid="false",
    )
    metric_export_interval_ms = env_int("OTEL_METRIC_EXPORT_INTERVAL", default=60000)
    metric_export_timeout_ms = env_int("OTEL_METRIC_EXPORT_TIMEOUT", default=30000)
    bsp_settings = _resolve_batch_settings(
        schedule_delay_ms=env_int("OTEL_BSP_SCHEDULE_DELAY", default=5000),
        export_timeout_ms=env_int("OTEL_BSP_EXPORT_TIMEOUT", default=30000),
        max_queue_size=env_int("OTEL_BSP_MAX_QUEUE_SIZE", default=2048),
        max_export_batch_size=env_int("OTEL_BSP_MAX_EXPORT_BATCH_SIZE", default=512),
        label="BatchSpanProcessor",
    )
    blrp_settings = _resolve_batch_settings(
        schedule_delay_ms=env_int("OTEL_BLRP_SCHEDULE_DELAY", default=5000),
        export_timeout_ms=env_int("OTEL_BLRP_EXPORT_TIMEOUT", default=30000),
        max_queue_size=env_int("OTEL_BLRP_MAX_QUEUE_SIZE", default=2048),
        max_export_batch_size=env_int("OTEL_BLRP_MAX_EXPORT_BATCH_SIZE", default=512),
        label="BatchLogRecordProcessor",
    )
    sampler = _resolve_sampler()
    return OtelConfig(
        enable_traces=enable_traces,
        enable_metrics=enable_metrics,
        enable_logs=enable_logs,
        enable_log_correlation=enable_log_correlation,
        enable_system_metrics=enable_system_metrics,
        sampler=sampler,
        metric_export_interval_ms=metric_export_interval_ms,
        metric_export_timeout_ms=metric_export_timeout_ms,
        bsp_schedule_delay_ms=bsp_settings.schedule_delay_ms,
        bsp_export_timeout_ms=bsp_settings.export_timeout_ms,
        bsp_max_queue_size=bsp_settings.max_queue_size,
        bsp_max_export_batch_size=bsp_settings.max_export_batch_size,
        blrp_schedule_delay_ms=blrp_settings.schedule_delay_ms,
        blrp_export_timeout_ms=blrp_settings.export_timeout_ms,
        blrp_max_queue_size=blrp_settings.max_queue_size,
        blrp_max_export_batch_size=blrp_settings.max_export_batch_size,
        attribute_count_limit=env_int("OTEL_ATTRIBUTE_COUNT_LIMIT", default=None),
        attribute_value_length_limit=env_int("OTEL_ATTRIBUTE_VALUE_LENGTH_LIMIT", default=None),
        log_record_attribute_count_limit=env_int(
            "OTEL_LOGRECORD_ATTRIBUTE_COUNT_LIMIT",
            default=None,
        ),
        log_record_attribute_value_length_limit=env_int(
            "OTEL_LOGRECORD_ATTRIBUTE_VALUE_LENGTH_LIMIT",
            default=None,
        ),
        span_attribute_count_limit=env_int("OTEL_SPAN_ATTRIBUTE_COUNT_LIMIT", default=None),
        span_attribute_length_limit=env_int("OTEL_SPAN_ATTRIBUTE_VALUE_LENGTH_LIMIT", default=None),
        span_event_count_limit=env_int("OTEL_SPAN_EVENT_COUNT_LIMIT", default=None),
        span_link_count_limit=env_int("OTEL_SPAN_LINK_COUNT_LIMIT", default=None),
        span_event_attribute_count_limit=env_int("OTEL_EVENT_ATTRIBUTE_COUNT_LIMIT", default=None),
        span_link_attribute_count_limit=env_int("OTEL_LINK_ATTRIBUTE_COUNT_LIMIT", default=None),
        metrics_exemplar_filter=_resolve_exemplar_filter(),
        metrics_temporality_preference=_resolve_metrics_temporality(),
        metrics_histogram_aggregation=_resolve_metrics_histogram_aggregation(),
        otel_log_level=_parse_log_level(env_value("OTEL_LOG_LEVEL")),
        python_context=env_value("OTEL_PYTHON_CONTEXT"),
        id_generator=_resolve_id_generator(),
        test_mode=env_bool("CODEANATOMY_OTEL_TEST_MODE", default=False, on_invalid="false"),
        auto_instrumentation=env_bool(
            "CODEANATOMY_OTEL_AUTO_INSTRUMENTATION",
            default=False,
            on_invalid="false",
        ),
        config_file=env_value("OTEL_EXPERIMENTAL_CONFIG_FILE"),
    )


def resolve_otel_config() -> OtelConfig:
    """Resolve configuration from environment variables.

    Returns
    -------
    OtelConfig
        Resolved configuration values.
    """
    if env_bool_strict("OTEL_SDK_DISABLED", default=False):
        return _resolve_disabled_config()
    return _resolve_enabled_config()


__all__ = ["OtelConfig", "resolve_otel_config"]
