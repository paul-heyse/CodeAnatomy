"""OpenTelemetry configuration type definitions."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from opentelemetry.sdk.metrics._internal.aggregation import Aggregation, AggregationTemporality
from opentelemetry.sdk.metrics._internal.exemplar.exemplar_filter import ExemplarFilter
from opentelemetry.sdk.trace.id_generator import IdGenerator
from opentelemetry.sdk.trace.sampling import Sampler

from core.config_base import config_fingerprint
from core_types import NonNegativeInt
from serde_msgspec import StructBaseStrict


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
        """Return canonical payload for fingerprinting."""
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
        """Return a stable fingerprint for the config."""
        return config_fingerprint(self.fingerprint_payload())


class OtelConfigSpec(StructBaseStrict, frozen=True):
    """Serializable OpenTelemetry configuration spec."""

    enable_traces: bool | None = None
    enable_metrics: bool | None = None
    enable_logs: bool | None = None
    enable_log_correlation: bool | None = None
    enable_system_metrics: bool | None = None
    sampler: str | None = None
    sampler_arg: float | None = None
    metric_export_interval_ms: NonNegativeInt | None = None
    metric_export_timeout_ms: NonNegativeInt | None = None
    bsp_schedule_delay_ms: NonNegativeInt | None = None
    bsp_export_timeout_ms: NonNegativeInt | None = None
    bsp_max_queue_size: NonNegativeInt | None = None
    bsp_max_export_batch_size: NonNegativeInt | None = None
    blrp_schedule_delay_ms: NonNegativeInt | None = None
    blrp_export_timeout_ms: NonNegativeInt | None = None
    blrp_max_queue_size: NonNegativeInt | None = None
    blrp_max_export_batch_size: NonNegativeInt | None = None
    attribute_count_limit: NonNegativeInt | None = None
    attribute_value_length_limit: NonNegativeInt | None = None
    log_record_attribute_count_limit: NonNegativeInt | None = None
    log_record_attribute_value_length_limit: NonNegativeInt | None = None
    span_attribute_count_limit: NonNegativeInt | None = None
    span_attribute_length_limit: NonNegativeInt | None = None
    span_event_count_limit: NonNegativeInt | None = None
    span_link_count_limit: NonNegativeInt | None = None
    span_event_attribute_count_limit: NonNegativeInt | None = None
    span_link_attribute_count_limit: NonNegativeInt | None = None
    metrics_exemplar_filter: str | None = None
    metrics_temporality_preference: str | None = None
    metrics_histogram_aggregation: str | None = None
    otel_log_level: str | int | None = None
    python_context: str | None = None
    id_generator: str | None = None
    test_mode: bool | None = None
    auto_instrumentation: bool | None = None
    config_file: str | None = None


@dataclass(frozen=True)
class OtelConfigOverrides:
    """Optional overrides for OpenTelemetry configuration resolution."""

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


@dataclass(frozen=True)
class BatchProcessorSettings:
    """Resolved queue and batching settings for batch processors."""

    schedule_delay_ms: int
    export_timeout_ms: int
    max_queue_size: int
    max_export_batch_size: int


__all__ = [
    "BatchProcessorSettings",
    "OtelConfig",
    "OtelConfigOverrides",
    "OtelConfigSpec",
]
