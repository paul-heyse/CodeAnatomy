"""Main application setup for the CodeAnatomy CLI."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Literal, cast

from cyclopts import App, Parameter
from cyclopts.config import Toml

from cli.commands.version import get_version
from cli.config_provider import build_cyclopts_config, resolve_config
from cli.config_source import ConfigSource, ConfigValue, ConfigWithSources
from cli.context import RunContext
from cli.groups import admin_group, observability_group, session_group
from cli.result_action import cli_result_action
from cli.telemetry import apply_telemetry_config, invoke_with_telemetry
from cli.validation import validate_config_mutual_exclusion
from core_types import JsonValue
from obs.otel import OtelBootstrapOptions
from utils.uuid_factory import uuid7_str

LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR")

_HELP_EPILOGUE = """
Examples:
  codeanatomy build .                    Build CPG for current directory
  codeanatomy build ./repo -o ./out      Build with custom output directory
  codeanatomy plan ./repo --show-graph   Show execution plan with graph
  codeanatomy config show                Show effective configuration
  codeanatomy delta vacuum --path ./data Vacuum Delta table

Environment Variables:
  CODEANATOMY_LOG_LEVEL      Default log level (DEBUG, INFO, WARNING, ERROR)
  CODEANATOMY_RUNTIME_PROFILE Runtime profile name
  CODEANATOMY_OUTPUT_DIR     Default output directory
  CODEANATOMY_ENABLE_TRACES  Enable OpenTelemetry traces

Tips:
  Use `codeanatomy config show --with-sources` to see config precedence.

Documentation: https://github.com/codeanatomy/codeanatomy
"""


def _telemetry_config_hook(*_args: object, **_kwargs: object) -> None:
    apply_telemetry_config()


app = App(
    name="codeanatomy",
    help="CodeAnatomy CPG Builder - Inference-driven Code Property Graph generation.",
    help_format="rich",
    help_epilogue=_HELP_EPILOGUE,
    version=get_version(),
    version_flags=["--version", "-V"],
    default_parameter=Parameter(
        show_default=True,
        show_env_var=True,
    ),
    result_action=cli_result_action,
    config=[
        Toml("codeanatomy.toml", must_exist=False, search_parents=True),
        Toml(
            "pyproject.toml",
            root_keys=("tool", "codeanatomy"),
            must_exist=False,
            search_parents=True,
        ),
        _telemetry_config_hook,
    ],
    exit_on_error=True,
    print_error=True,
    help_on_error=False,
)

app.meta.group_parameters = session_group


@dataclass(frozen=True)
class SessionOptions:
    """Session-level configuration parameters."""

    config_file: Annotated[
        str | None,
        Parameter(
            name="--config",
            help="Path to configuration file (overrides default search).",
            group=session_group,
        ),
    ] = None
    run_id: Annotated[
        str | None,
        Parameter(
            name="--run-id",
            help="Explicit run identifier (UUID7 generated if not provided).",
            group=session_group,
        ),
    ] = None
    log_level: Annotated[
        Literal["DEBUG", "INFO", "WARNING", "ERROR"],
        Parameter(
            name="--log-level",
            help="Logging verbosity level.",
            env_var="CODEANATOMY_LOG_LEVEL",
            group=session_group,
        ),
    ] = "INFO"


@dataclass(frozen=True)
class ObservabilityOptions:
    """OpenTelemetry configuration parameters."""

    enable_traces: Annotated[
        bool | None,
        Parameter(
            name="--enable-traces",
            help="Enable OpenTelemetry traces.",
            env_var="CODEANATOMY_ENABLE_TRACES",
            group=observability_group,
        ),
    ] = None
    otel_endpoint: Annotated[
        str | None,
        Parameter(
            name="--otel-endpoint",
            help="Override OTLP endpoint for traces/metrics/logs.",
            group=observability_group,
        ),
    ] = None
    otel_protocol: Annotated[
        Literal["grpc", "http/protobuf"] | None,
        Parameter(
            name="--otel-protocol",
            help="Override OTLP protocol for traces/metrics/logs.",
            group=observability_group,
        ),
    ] = None
    otel_sampler: Annotated[
        str | None,
        Parameter(
            name="--otel-sampler",
            help="Override OTEL_TRACES_SAMPLER.",
            group=observability_group,
        ),
    ] = None
    otel_sampler_arg: Annotated[
        float | None,
        Parameter(
            name="--otel-sampler-arg",
            help="Override OTEL_TRACES_SAMPLER_ARG.",
            group=observability_group,
        ),
    ] = None
    otel_log_correlation: Annotated[
        bool | None,
        Parameter(
            name="--otel-log-correlation",
            help="Enable OpenTelemetry log correlation.",
            group=observability_group,
        ),
    ] = None
    otel_metric_export_interval_ms: Annotated[
        int | None,
        Parameter(
            name="--otel-metric-export-interval-ms",
            help="Override OTEL_METRIC_EXPORT_INTERVAL (ms).",
            group=observability_group,
        ),
    ] = None
    otel_metric_export_timeout_ms: Annotated[
        int | None,
        Parameter(
            name="--otel-metric-export-timeout-ms",
            help="Override OTEL_METRIC_EXPORT_TIMEOUT (ms).",
            group=observability_group,
        ),
    ] = None
    otel_bsp_schedule_delay_ms: Annotated[
        int | None,
        Parameter(
            name="--otel-bsp-schedule-delay-ms",
            help="Override OTEL_BSP_SCHEDULE_DELAY (ms).",
            group=observability_group,
        ),
    ] = None
    otel_bsp_export_timeout_ms: Annotated[
        int | None,
        Parameter(
            name="--otel-bsp-export-timeout-ms",
            help="Override OTEL_BSP_EXPORT_TIMEOUT (ms).",
            group=observability_group,
        ),
    ] = None
    otel_bsp_max_queue_size: Annotated[
        int | None,
        Parameter(
            name="--otel-bsp-max-queue-size",
            help="Override OTEL_BSP_MAX_QUEUE_SIZE.",
            group=observability_group,
        ),
    ] = None
    otel_bsp_max_export_batch_size: Annotated[
        int | None,
        Parameter(
            name="--otel-bsp-max-export-batch-size",
            help="Override OTEL_BSP_MAX_EXPORT_BATCH_SIZE.",
            group=observability_group,
        ),
    ] = None
    otel_blrp_schedule_delay_ms: Annotated[
        int | None,
        Parameter(
            name="--otel-blrp-schedule-delay-ms",
            help="Override OTEL_BLRP_SCHEDULE_DELAY (ms).",
            group=observability_group,
        ),
    ] = None
    otel_blrp_export_timeout_ms: Annotated[
        int | None,
        Parameter(
            name="--otel-blrp-export-timeout-ms",
            help="Override OTEL_BLRP_EXPORT_TIMEOUT (ms).",
            group=observability_group,
        ),
    ] = None
    otel_blrp_max_queue_size: Annotated[
        int | None,
        Parameter(
            name="--otel-blrp-max-queue-size",
            help="Override OTEL_BLRP_MAX_QUEUE_SIZE.",
            group=observability_group,
        ),
    ] = None
    otel_blrp_max_export_batch_size: Annotated[
        int | None,
        Parameter(
            name="--otel-blrp-max-export-batch-size",
            help="Override OTEL_BLRP_MAX_EXPORT_BATCH_SIZE.",
            group=observability_group,
        ),
    ] = None
    enable_metrics: Annotated[
        bool | None,
        Parameter(
            name="--enable-metrics",
            help="Enable OpenTelemetry metrics.",
            env_var="CODEANATOMY_ENABLE_METRICS",
            group=observability_group,
        ),
    ] = None
    enable_logs: Annotated[
        bool | None,
        Parameter(
            name="--enable-logs",
            help="Enable OpenTelemetry logs.",
            env_var="CODEANATOMY_ENABLE_LOGS",
            group=observability_group,
        ),
    ] = None
    otel_test_mode: Annotated[
        bool | None,
        Parameter(
            name="--otel-test-mode",
            help="Use in-memory exporters for OpenTelemetry (for testing).",
            env_var="CODEANATOMY_OTEL_TEST_MODE",
            group=observability_group,
        ),
    ] = None


_DEFAULT_SESSION_OPTIONS = SessionOptions()
_DEFAULT_OBSERVABILITY_OPTIONS = ObservabilityOptions()


def _config_str(config: Mapping[str, JsonValue], key: str) -> str | None:
    value = config.get(key)
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _config_section(
    config: Mapping[str, JsonValue],
    key: str,
) -> dict[str, JsonValue]:
    value = config.get(key)
    if isinstance(value, Mapping):
        return dict(cast("Mapping[str, JsonValue]", value))
    return {}


def _config_bool(config: Mapping[str, JsonValue], key: str) -> bool | None:
    value = config.get(key)
    return value if isinstance(value, bool) else None


def _config_int(config: Mapping[str, JsonValue], key: str) -> int | None:
    value = config.get(key)
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    return None


def _config_float(config: Mapping[str, JsonValue], key: str) -> float | None:
    value = config.get(key)
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _apply_cli_overrides(
    config_contents: dict[str, JsonValue],
    sources: ConfigWithSources | None,
    overrides: Mapping[str, tuple[JsonValue, str]],
) -> ConfigWithSources | None:
    if not overrides:
        return sources
    for key, (value, _location) in overrides.items():
        existing = config_contents.get(key)
        if isinstance(value, Mapping) and isinstance(existing, Mapping):
            merged = dict(cast("Mapping[str, JsonValue]", existing))
            merged.update(cast("Mapping[str, JsonValue]", value))
            config_contents[key] = merged
        else:
            config_contents[key] = value
    if sources is None:
        return None
    values = dict(sources.values)
    for key, (value, location) in overrides.items():
        existing = config_contents.get(key)
        if isinstance(value, Mapping) and isinstance(existing, Mapping):
            resolved_value = cast("Mapping[str, JsonValue]", existing)
        else:
            resolved_value = value
        values[key] = ConfigValue(
            key=key,
            value=resolved_value,
            source=ConfigSource.CLI,
            location=location,
        )
    return ConfigWithSources(values=values)


def _validate_session_options(session: SessionOptions) -> None:
    if session.log_level not in LOG_LEVELS:
        msg = f"Unsupported log level {session.log_level!r}."
        raise ValueError(msg)
    if session.config_file is None:
        return
    config_path = Path(session.config_file)
    if not config_path.exists():
        msg = f"Config file not found: {session.config_file!r}."
        raise ValueError(msg)


def _append_cli_override(
    overrides: dict[str, tuple[JsonValue, str]],
    *,
    key: str,
    value: JsonValue | None,
    flag: str,
    require_non_empty: bool = False,
) -> None:
    if value is None:
        return
    if require_non_empty and isinstance(value, str) and not value.strip():
        return
    overrides[key] = (value, flag)


def _build_otel_cli_overrides(
    observability: ObservabilityOptions,
) -> dict[str, tuple[JsonValue, str]]:
    overrides: dict[str, tuple[JsonValue, str]] = {}
    otel_payload: dict[str, JsonValue] = {}
    location: str | None = None
    specs: tuple[tuple[str, JsonValue | None, str, bool], ...] = (
        ("endpoint", observability.otel_endpoint, "--otel-endpoint", True),
        ("protocol", observability.otel_protocol, "--otel-protocol", False),
        ("sampler", observability.otel_sampler, "--otel-sampler", True),
        ("sampler_arg", observability.otel_sampler_arg, "--otel-sampler-arg", False),
        (
            "log_correlation",
            observability.otel_log_correlation,
            "--otel-log-correlation",
            False,
        ),
        (
            "metric_export_interval_ms",
            observability.otel_metric_export_interval_ms,
            "--otel-metric-export-interval-ms",
            False,
        ),
        (
            "metric_export_timeout_ms",
            observability.otel_metric_export_timeout_ms,
            "--otel-metric-export-timeout-ms",
            False,
        ),
        (
            "bsp_schedule_delay_ms",
            observability.otel_bsp_schedule_delay_ms,
            "--otel-bsp-schedule-delay-ms",
            False,
        ),
        (
            "bsp_export_timeout_ms",
            observability.otel_bsp_export_timeout_ms,
            "--otel-bsp-export-timeout-ms",
            False,
        ),
        (
            "bsp_max_queue_size",
            observability.otel_bsp_max_queue_size,
            "--otel-bsp-max-queue-size",
            False,
        ),
        (
            "bsp_max_export_batch_size",
            observability.otel_bsp_max_export_batch_size,
            "--otel-bsp-max-export-batch-size",
            False,
        ),
        (
            "blrp_schedule_delay_ms",
            observability.otel_blrp_schedule_delay_ms,
            "--otel-blrp-schedule-delay-ms",
            False,
        ),
        (
            "blrp_export_timeout_ms",
            observability.otel_blrp_export_timeout_ms,
            "--otel-blrp-export-timeout-ms",
            False,
        ),
        (
            "blrp_max_queue_size",
            observability.otel_blrp_max_queue_size,
            "--otel-blrp-max-queue-size",
            False,
        ),
        (
            "blrp_max_export_batch_size",
            observability.otel_blrp_max_export_batch_size,
            "--otel-blrp-max-export-batch-size",
            False,
        ),
    )
    for key, value, flag, require_non_empty in specs:
        if value is None:
            continue
        if require_non_empty and isinstance(value, str) and not value.strip():
            continue
        otel_payload[key] = value
        location = location or flag
    if otel_payload:
        overrides["otel"] = (otel_payload, location or "--otel")
    return overrides


def _build_otel_options(
    config_contents: Mapping[str, JsonValue],
    observability: ObservabilityOptions,
) -> OtelBootstrapOptions | None:
    otel_config = _config_section(config_contents, "otel")
    otel_values = {
        "otlp_endpoint": _config_str(otel_config, "endpoint"),
        "otlp_protocol": _config_str(otel_config, "protocol"),
        "sampler": _config_str(otel_config, "sampler"),
        "sampler_arg": _config_float(otel_config, "sampler_arg"),
        "log_correlation": _config_bool(otel_config, "log_correlation"),
        "metric_export_interval_ms": _config_int(otel_config, "metric_export_interval_ms"),
        "metric_export_timeout_ms": _config_int(otel_config, "metric_export_timeout_ms"),
        "bsp_schedule_delay_ms": _config_int(otel_config, "bsp_schedule_delay_ms"),
        "bsp_export_timeout_ms": _config_int(otel_config, "bsp_export_timeout_ms"),
        "bsp_max_queue_size": _config_int(otel_config, "bsp_max_queue_size"),
        "bsp_max_export_batch_size": _config_int(otel_config, "bsp_max_export_batch_size"),
        "blrp_schedule_delay_ms": _config_int(otel_config, "blrp_schedule_delay_ms"),
        "blrp_export_timeout_ms": _config_int(otel_config, "blrp_export_timeout_ms"),
        "blrp_max_queue_size": _config_int(otel_config, "blrp_max_queue_size"),
        "blrp_max_export_batch_size": _config_int(otel_config, "blrp_max_export_batch_size"),
    }

    if not any(
        value is not None
        for value in (
            observability.enable_traces,
            observability.enable_metrics,
            observability.enable_logs,
            observability.otel_test_mode,
            observability.otel_log_correlation,
            *otel_values.values(),
        )
    ):
        return None
    return OtelBootstrapOptions(
        enable_traces=observability.enable_traces,
        enable_metrics=observability.enable_metrics,
        enable_logs=observability.enable_logs,
        enable_log_correlation=observability.otel_log_correlation
        if observability.otel_log_correlation is not None
        else otel_values["log_correlation"],
        test_mode=observability.otel_test_mode,
        otlp_endpoint=otel_values["otlp_endpoint"],
        otlp_protocol=otel_values["otlp_protocol"],
        sampler=otel_values["sampler"],
        sampler_arg=otel_values["sampler_arg"],
        metric_export_interval_ms=otel_values["metric_export_interval_ms"],
        metric_export_timeout_ms=otel_values["metric_export_timeout_ms"],
        bsp_schedule_delay_ms=otel_values["bsp_schedule_delay_ms"],
        bsp_export_timeout_ms=otel_values["bsp_export_timeout_ms"],
        bsp_max_queue_size=otel_values["bsp_max_queue_size"],
        bsp_max_export_batch_size=otel_values["bsp_max_export_batch_size"],
        blrp_schedule_delay_ms=otel_values["blrp_schedule_delay_ms"],
        blrp_export_timeout_ms=otel_values["blrp_export_timeout_ms"],
        blrp_max_queue_size=otel_values["blrp_max_queue_size"],
        blrp_max_export_batch_size=otel_values["blrp_max_export_batch_size"],
    )


@app.meta.default
def meta_launcher(
    *tokens: Annotated[str, Parameter(show=False, allow_leading_hyphen=True)],
    session: Annotated[SessionOptions, Parameter(name="*")] = _DEFAULT_SESSION_OPTIONS,
    observability: Annotated[ObservabilityOptions, Parameter(name="*")] = (
        _DEFAULT_OBSERVABILITY_OPTIONS
    ),
) -> int:
    """Meta launcher for config selection and context injection.

    Returns:
    -------
    int
        Exit status code from command execution.
    """
    logging.basicConfig(level=session.log_level.upper())
    _validate_session_options(session)

    config_resolution = resolve_config(session.config_file)
    config_contents = dict(config_resolution.contents)
    validate_config_mutual_exclusion(config_contents)
    app.config = [*build_cyclopts_config(config_contents), _telemetry_config_hook]

    effective_config_contents = dict(config_contents)
    otel_cli_overrides = _build_otel_cli_overrides(observability)

    config_sources = _apply_cli_overrides(
        effective_config_contents,
        config_resolution.sources,
        otel_cli_overrides,
    )

    otel_options = _build_otel_options(effective_config_contents, observability)

    effective_run_id = session.run_id or uuid7_str()
    run_context = RunContext(
        run_id=effective_run_id,
        log_level=session.log_level,
        config_contents=effective_config_contents,
        config_sources=config_sources or config_resolution.sources,
        otel_options=otel_options,
    )

    exit_code, _event = invoke_with_telemetry(
        app,
        list(tokens),
        run_context=run_context,
    )
    return exit_code


# Lazy-loaded commands with aliases
app.command("cli.commands.build:build_command", name="build", alias="b")
app.command("cli.commands.plan:plan_command", name="plan", alias="p")
app.command("cli.commands.diag:diag_command", name="diag", alias="d")

# Delta subapp
_delta_app = App(name="delta", help="Delta Lake maintenance operations.")
_delta_app.command("cli.commands.delta:vacuum_command", name="vacuum")
_delta_app.command("cli.commands.delta:checkpoint_command", name="checkpoint")
_delta_app.command("cli.commands.delta:cleanup_log_command", name="cleanup-log")
_delta_app.command("cli.commands.delta:export_command", name="export")
_delta_app.command("cli.commands.delta:restore_command", name="restore")
app.command(_delta_app)

# Config subapp with alias
_config_app = App(name="config", help="Configuration management.")
_config_app.command("cli.commands.config:show_config", name="show")
_config_app.command("cli.commands.config:validate_config", name="validate")
_config_app.command("cli.commands.config:init_config", name="init")
app.command(_config_app, alias="cfg")
app.command("cli.commands.version:version_command", name="version", alias="v")

# Completion install command
app.register_install_completion_command(
    name="--install-completion",
    add_to_startup=False,
    group=admin_group,
    help="Install shell completion scripts.",
)


def main() -> None:
    """Run the CodeAnatomy CLI."""
    app.meta()


__all__ = ["app", "main"]
