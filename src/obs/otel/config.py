"""Configuration helpers for OpenTelemetry bootstrap."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from importlib.metadata import entry_points

from opentelemetry.sdk.trace.sampling import (
    ALWAYS_OFF,
    ALWAYS_ON,
    ParentBased,
    Sampler,
    TraceIdRatioBased,
)

_LOGGER = logging.getLogger(__name__)


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
    span_attribute_count_limit: int | None
    span_attribute_length_limit: int | None
    span_event_count_limit: int | None
    span_link_count_limit: int | None
    span_event_attribute_count_limit: int | None
    span_link_attribute_count_limit: int | None
    test_mode: bool
    auto_instrumentation: bool
    config_file: str | None


def _env_truthy(value: str | None) -> bool:
    return value is not None and value.strip().lower() == "true"


def _env_bool(name: str, *, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    value = raw.strip().lower()
    return value in {"1", "true", "yes", "y"}


def _env_int(name: str, *, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return int(raw.strip())
    except ValueError:
        _LOGGER.warning("Invalid integer for %s: %s", name, raw)
        return default


def _env_float(name: str, *, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return float(raw.strip())
    except ValueError:
        _LOGGER.warning("Invalid float for %s: %s", name, raw)
        return default


def _env_optional_int(name: str) -> int | None:
    raw = os.environ.get(name)
    if raw is None:
        return None
    try:
        return int(raw.strip())
    except ValueError:
        _LOGGER.warning("Invalid integer for %s: %s", name, raw)
        return None


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
    value = os.environ.get(env_name)
    if value is None:
        return default
    return value.strip().lower() != "none"


def _resolve_sampler() -> Sampler:
    sampler_name = os.environ.get("OTEL_TRACES_SAMPLER", "parentbased_traceidratio")
    name = sampler_name.strip().lower()
    ratio = _env_float("OTEL_TRACES_SAMPLER_ARG", default=0.1)
    sampler = _resolve_builtin_sampler(name, ratio)
    if sampler is None:
        sampler = _resolve_entrypoint_sampler(name, os.environ.get("OTEL_TRACES_SAMPLER_ARG"))
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
    try:
        eps = entry_points(group="opentelemetry_traces_sampler")
    except TypeError:
        eps = entry_points().get("opentelemetry_traces_sampler", ())
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
        except Exception as exc:
            _LOGGER.warning("Sampler factory %s failed: %s", entry.name, exc)
            return None
        if isinstance(sampler, Sampler):
            return sampler
        _LOGGER.warning("Sampler entrypoint %s returned invalid type.", entry.name)
        return None
    return None


def resolve_otel_config() -> OtelConfig:
    """Resolve configuration from environment variables.

    Returns
    -------
    OtelConfig
        Resolved configuration values.
    """
    if _env_truthy(os.environ.get("OTEL_SDK_DISABLED")):
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
            span_attribute_count_limit=None,
            span_attribute_length_limit=None,
            span_event_count_limit=None,
            span_link_count_limit=None,
            span_event_attribute_count_limit=None,
            span_link_attribute_count_limit=None,
            test_mode=_env_bool("CODEANATOMY_OTEL_TEST_MODE", default=False),
            auto_instrumentation=_env_bool(
                "CODEANATOMY_OTEL_AUTO_INSTRUMENTATION",
                default=False,
            ),
            config_file=os.environ.get("OTEL_EXPERIMENTAL_CONFIG_FILE"),
        )
    enable_traces = _exporter_enabled("OTEL_TRACES_EXPORTER", default=True)
    enable_metrics = _exporter_enabled("OTEL_METRICS_EXPORTER", default=True)
    enable_logs = _exporter_enabled("OTEL_LOGS_EXPORTER", default=True)
    enable_log_correlation = _env_bool("OTEL_PYTHON_LOG_CORRELATION", default=True)
    enable_system_metrics = _env_bool("CODEANATOMY_OTEL_SYSTEM_METRICS", default=False)
    metric_export_interval_ms = _env_int("OTEL_METRIC_EXPORT_INTERVAL", default=60000)
    metric_export_timeout_ms = _env_int("OTEL_METRIC_EXPORT_TIMEOUT", default=30000)
    bsp_schedule_delay_ms = _env_int("OTEL_BSP_SCHEDULE_DELAY", default=5000)
    bsp_export_timeout_ms = _env_int("OTEL_BSP_EXPORT_TIMEOUT", default=30000)
    bsp_max_queue_size = _env_int("OTEL_BSP_MAX_QUEUE_SIZE", default=2048)
    bsp_max_export_batch_size = _env_int("OTEL_BSP_MAX_EXPORT_BATCH_SIZE", default=512)
    bsp_max_queue_size, bsp_max_export_batch_size = _resolve_batch_sizes(
        max_queue_size=bsp_max_queue_size,
        max_export_batch_size=bsp_max_export_batch_size,
        label="BatchSpanProcessor",
    )
    blrp_schedule_delay_ms = _env_int("OTEL_BLRP_SCHEDULE_DELAY", default=5000)
    blrp_export_timeout_ms = _env_int("OTEL_BLRP_EXPORT_TIMEOUT", default=30000)
    blrp_max_queue_size = _env_int("OTEL_BLRP_MAX_QUEUE_SIZE", default=2048)
    blrp_max_export_batch_size = _env_int("OTEL_BLRP_MAX_EXPORT_BATCH_SIZE", default=512)
    blrp_max_queue_size, blrp_max_export_batch_size = _resolve_batch_sizes(
        max_queue_size=blrp_max_queue_size,
        max_export_batch_size=blrp_max_export_batch_size,
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
        bsp_schedule_delay_ms=bsp_schedule_delay_ms,
        bsp_export_timeout_ms=bsp_export_timeout_ms,
        bsp_max_queue_size=bsp_max_queue_size,
        bsp_max_export_batch_size=bsp_max_export_batch_size,
        blrp_schedule_delay_ms=blrp_schedule_delay_ms,
        blrp_export_timeout_ms=blrp_export_timeout_ms,
        blrp_max_queue_size=blrp_max_queue_size,
        blrp_max_export_batch_size=blrp_max_export_batch_size,
        attribute_count_limit=_env_optional_int("OTEL_ATTRIBUTE_COUNT_LIMIT"),
        attribute_value_length_limit=_env_optional_int("OTEL_ATTRIBUTE_VALUE_LENGTH_LIMIT"),
        span_attribute_count_limit=_env_optional_int("OTEL_SPAN_ATTRIBUTE_COUNT_LIMIT"),
        span_attribute_length_limit=_env_optional_int("OTEL_SPAN_ATTRIBUTE_VALUE_LENGTH_LIMIT"),
        span_event_count_limit=_env_optional_int("OTEL_SPAN_EVENT_COUNT_LIMIT"),
        span_link_count_limit=_env_optional_int("OTEL_SPAN_LINK_COUNT_LIMIT"),
        span_event_attribute_count_limit=_env_optional_int("OTEL_EVENT_ATTRIBUTE_COUNT_LIMIT"),
        span_link_attribute_count_limit=_env_optional_int("OTEL_LINK_ATTRIBUTE_COUNT_LIMIT"),
        test_mode=_env_bool("CODEANATOMY_OTEL_TEST_MODE", default=False),
        auto_instrumentation=_env_bool(
            "CODEANATOMY_OTEL_AUTO_INSTRUMENTATION",
            default=False,
        ),
        config_file=os.environ.get("OTEL_EXPERIMENTAL_CONFIG_FILE"),
    )


__all__ = ["OtelConfig", "resolve_otel_config"]
