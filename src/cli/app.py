"""Main application setup for the CodeAnatomy CLI."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Literal

from cyclopts import App, Parameter
from cyclopts.config import Toml

from cli.commands.version import get_version
from cli.config_provider import build_cyclopts_config, resolve_config
from cli.context import RunContext
from cli.groups import admin_group, observability_group, session_group
from cli.result_action import cli_result_action
from cli.telemetry import invoke_with_telemetry
from cli.validation import validate_config_mutual_exclusion
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


@app.meta.default
def meta_launcher(
    *tokens: Annotated[str, Parameter(show=False, allow_leading_hyphen=True)],
    session: Annotated[SessionOptions, Parameter(name="*")] = _DEFAULT_SESSION_OPTIONS,
    observability: Annotated[ObservabilityOptions, Parameter(name="*")] = (
        _DEFAULT_OBSERVABILITY_OPTIONS
    ),
) -> int:
    """Meta launcher for config selection and context injection.

    Returns
    -------
    int
        Exit status code from command execution.

    Raises
    ------
    ValueError
        Raised when the log level or config path is invalid.
    """
    from obs.otel import OtelBootstrapOptions

    logging.basicConfig(level=session.log_level.upper())
    if session.log_level not in LOG_LEVELS:
        msg = f"Unsupported log level {session.log_level!r}."
        raise ValueError(msg)

    if session.config_file is not None:
        config_path = Path(session.config_file)
        if not config_path.exists():
            msg = f"Config file not found: {session.config_file!r}."
            raise ValueError(msg)

    config_resolution = resolve_config(session.config_file)
    config_contents = dict(config_resolution.contents)
    validate_config_mutual_exclusion(config_contents)
    app.config = build_cyclopts_config(config_contents)

    # Build OTel options from CLI parameters
    otel_options: OtelBootstrapOptions | None = None
    if any(
        opt is not None
        for opt in (
            observability.enable_traces,
            observability.enable_metrics,
            observability.enable_logs,
            observability.otel_test_mode,
        )
    ):
        otel_options = OtelBootstrapOptions(
            enable_traces=observability.enable_traces,
            enable_metrics=observability.enable_metrics,
            enable_logs=observability.enable_logs,
            test_mode=observability.otel_test_mode,
        )

    effective_run_id = session.run_id or uuid7_str()
    run_context = RunContext(
        run_id=effective_run_id,
        log_level=session.log_level,
        config_contents=config_contents,
        config_sources=config_resolution.sources,
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
