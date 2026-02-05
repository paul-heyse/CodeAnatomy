"""Runtime validation models for OpenTelemetry config specs."""

from __future__ import annotations

from pydantic import Field, model_validator

from runtime_models.base import RuntimeBase


class OtelConfigRuntime(RuntimeBase):
    """Validated OpenTelemetry configuration payload."""

    enable_traces: bool = True
    enable_metrics: bool = True
    enable_logs: bool = True
    enable_log_correlation: bool = True
    enable_system_metrics: bool = False
    sampler: str | None = None
    sampler_arg: float | None = None
    metric_export_interval_ms: int = Field(default=60000, ge=0)
    metric_export_timeout_ms: int = Field(default=30000, ge=0)
    bsp_schedule_delay_ms: int = Field(default=5000, ge=0)
    bsp_export_timeout_ms: int = Field(default=30000, ge=0)
    bsp_max_queue_size: int = Field(default=2048, ge=0)
    bsp_max_export_batch_size: int = Field(default=512, ge=0)
    blrp_schedule_delay_ms: int = Field(default=5000, ge=0)
    blrp_export_timeout_ms: int = Field(default=30000, ge=0)
    blrp_max_queue_size: int = Field(default=2048, ge=0)
    blrp_max_export_batch_size: int = Field(default=512, ge=0)
    attribute_count_limit: int | None = Field(default=None, ge=0)
    attribute_value_length_limit: int | None = Field(default=None, ge=0)
    log_record_attribute_count_limit: int | None = Field(default=None, ge=0)
    log_record_attribute_value_length_limit: int | None = Field(default=None, ge=0)
    span_attribute_count_limit: int | None = Field(default=None, ge=0)
    span_attribute_length_limit: int | None = Field(default=None, ge=0)
    span_event_count_limit: int | None = Field(default=None, ge=0)
    span_link_count_limit: int | None = Field(default=None, ge=0)
    span_event_attribute_count_limit: int | None = Field(default=None, ge=0)
    span_link_attribute_count_limit: int | None = Field(default=None, ge=0)
    metrics_exemplar_filter: str | None = None
    metrics_temporality_preference: str | None = None
    metrics_histogram_aggregation: str | None = None
    otel_log_level: str | int | None = None
    python_context: str | None = None
    id_generator: str | None = None
    test_mode: bool = False
    auto_instrumentation: bool = False
    config_file: str | None = None

    @model_validator(mode="after")
    def _validate_sampler(self) -> OtelConfigRuntime:
        if self.sampler_arg is None:
            return self
        if self.sampler is None:
            msg = "sampler_arg requires sampler to be set"
            raise ValueError(msg)
        if not (0.0 <= self.sampler_arg <= 1.0):
            msg = "sampler_arg must be within [0.0, 1.0]"
            raise ValueError(msg)
        return self


__all__ = ["OtelConfigRuntime"]
