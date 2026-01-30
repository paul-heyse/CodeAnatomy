"""Main application setup for the CodeAnatomy CLI."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Annotated, Literal

from cyclopts import App, Parameter
from cyclopts.config import Toml

from cli.config_loader import load_effective_config, normalize_config_contents
from cli.context import RunContext
from cli.groups import admin_group, session_group
from cli.telemetry import invoke_with_telemetry
from cli.validation import validate_config_mutual_exclusion
from utils.uuid_factory import uuid7_str

LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR")

app = App(
    name="codeanatomy",
    help="CodeAnatomy CPG Builder - Inference-driven Code Property Graph generation.",
    help_format="markdown",
    version=None,
    version_flags=[],
    default_parameter=Parameter(
        negative="no-",
    ),
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


@app.command(group=admin_group)
def version() -> None:
    """Show version and engine information."""
    from cli.commands.version import print_version_info

    print_version_info()


@app.meta.default
def meta_launcher(
    *tokens: Annotated[str, Parameter(show=False, allow_leading_hyphen=True)],
    config_file: Annotated[
        str | None,
        Parameter(
            name="--config",
            help="Path to configuration file (overrides default search).",
            group=session_group,
        ),
    ] = None,
    run_id: Annotated[
        str | None,
        Parameter(
            name="--run-id",
            help="Explicit run identifier (UUID7 generated if not provided).",
            group=session_group,
        ),
    ] = None,
    log_level: Annotated[
        Literal["DEBUG", "INFO", "WARNING", "ERROR"],
        Parameter(
            name="--log-level",
            help="Logging verbosity level.",
            env_var="CODEANATOMY_LOG_LEVEL",
            group=session_group,
        ),
    ] = "INFO",
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
    logging.basicConfig(level=log_level.upper())
    if log_level not in LOG_LEVELS:
        msg = f"Unsupported log level {log_level!r}."
        raise ValueError(msg)

    if config_file is not None:
        config_path = Path(config_file)
        if not config_path.exists():
            msg = f"Config file not found: {config_file!r}."
            raise ValueError(msg)
        app.config = [Toml(config_file, must_exist=True)]

    config_contents = load_effective_config(config_file)
    config_contents = normalize_config_contents(config_contents)
    validate_config_mutual_exclusion(config_contents)

    effective_run_id = run_id or uuid7_str()
    run_context = RunContext(
        run_id=effective_run_id,
        log_level=log_level,
        config_contents=config_contents,
    )

    exit_code, _event = invoke_with_telemetry(
        app,
        list(tokens),
        run_context=run_context,
    )
    return exit_code


# Lazy-loaded commands
app.command("cli.commands.build:build_command", name="build")
app.command("cli.commands.plan:plan_command", name="plan")
app.command("cli.commands.diag:diag_command", name="diag")

# Delta subapp
_delta_app = App(name="delta", help="Delta Lake maintenance operations.")
_delta_app.command("cli.commands.delta:vacuum_command", name="vacuum")
_delta_app.command("cli.commands.delta:checkpoint_command", name="checkpoint")
_delta_app.command("cli.commands.delta:cleanup_log_command", name="cleanup-log")
_delta_app.command("cli.commands.delta:export_command", name="export")
_delta_app.command("cli.commands.delta:restore_command", name="restore")
app.command(_delta_app)

# Config subapp
_config_app = App(name="config", help="Configuration management.")
_config_app.command("cli.commands.config:show_config", name="show")
_config_app.command("cli.commands.config:validate_config", name="validate")
_config_app.command("cli.commands.config:init_config", name="init")
app.command(_config_app)

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
