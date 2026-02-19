"""Contract tests for OpenTelemetry configuration parsing."""

from __future__ import annotations

import importlib
import logging

import pytest
from opentelemetry.sdk.metrics._internal.exemplar.exemplar_filter import AlwaysOnExemplarFilter
from opentelemetry.sdk.resources import Resource

from obs.otel import bootstrap as otel_bootstrap
from obs.otel import config as otel_config


def test_exemplar_filter_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure OTEL_METRICS_EXEMPLAR_FILTER resolves to an ExemplarFilter."""
    monkeypatch.setenv("OTEL_METRICS_EXEMPLAR_FILTER", "always_on")
    config = otel_config.resolve_otel_config()
    assert isinstance(config.metrics_exemplar_filter, AlwaysOnExemplarFilter)


def test_exemplar_filter_wiring(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure exemplar filters are wired into the MeterProvider."""
    monkeypatch.setenv("OTEL_METRICS_EXEMPLAR_FILTER", "always_on")
    config = otel_config.resolve_otel_config()
    resource = Resource.create({"service.name": "codeanatomy-tests"})
    meter_provider = otel_bootstrap.build_meter_provider(
        config,
        resource,
        use_test_mode=True,
    )
    assert isinstance(meter_provider, otel_bootstrap.ConfiguredMeterProvider)
    exemplar_filter = meter_provider.exemplar_filter
    assert isinstance(exemplar_filter, AlwaysOnExemplarFilter)


def test_metrics_exporter_preferences(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure OTLP exporter preferences are parsed from env vars."""
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE", "delta")
    monkeypatch.setenv(
        "OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION",
        "explicit_bucket_histogram",
    )
    config = otel_config.resolve_otel_config()
    assert config.metrics_temporality_preference is not None
    assert config.metrics_histogram_aggregation is not None


def test_log_level_parsing(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure OTEL_LOG_LEVEL is parsed to a logging level."""
    monkeypatch.setenv("OTEL_LOG_LEVEL", "DEBUG")
    config = otel_config.resolve_otel_config()
    assert config.otel_log_level == logging.DEBUG


def test_sdk_disabled_strict(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure OTEL_SDK_DISABLED only accepts strict true/false values."""
    monkeypatch.setenv("OTEL_SDK_DISABLED", "1")
    config = otel_config.resolve_otel_config()
    assert config.enable_log_correlation


def test_bootstrap_option_precedence(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure bootstrap options override env export toggles."""
    monkeypatch.setenv("OTEL_TRACES_EXPORTER", "none")
    monkeypatch.setenv("CODEANATOMY_OTEL_TEST_MODE", "true")
    reloaded = importlib.reload(otel_bootstrap)
    providers = reloaded.configure_otel(
        service_name="codeanatomy-tests",
        options=reloaded.OtelBootstrapOptions(enable_traces=True, test_mode=True),
    )
    assert providers.tracer_provider is not None


def test_sampler_arg_requires_sampler() -> None:
    """sampler_arg without sampler should fail config validation."""
    with pytest.raises(ValueError, match="sampler_arg requires sampler to be set"):
        otel_config.resolve_otel_config(
            spec=otel_config.OtelConfigSpec(sampler_arg=0.5),
        )


def test_sampler_arg_bounds_validation() -> None:
    """sampler_arg must be within [0.0, 1.0]."""
    with pytest.raises(ValueError, match=r"sampler_arg must be within \[0.0, 1.0\]"):
        otel_config.resolve_otel_config(
            spec=otel_config.OtelConfigSpec(sampler="traceidratio", sampler_arg=1.1),
        )
