"""CQ CLI application using cyclopts."""

from __future__ import annotations

import os
import sys
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated

from cyclopts import App, Parameter
from rich.console import Console

from tools.cq.cli_app.config import build_config_chain, load_typed_config, load_typed_env_config
from tools.cq.cli_app.config_types import CqConfig
from tools.cq.cli_app.context import CliContext, CliContextOptions
from tools.cq.cli_app.groups import (
    admin_group,
    analysis_group,
    global_group,
    setup_group,
)
from tools.cq.cli_app.result_action import cq_result_action
from tools.cq.cli_app.telemetry import invoke_with_telemetry
from tools.cq.cli_app.types import OutputFormat
from tools.cq.core.structs import CqStruct

VERSION = "0.4.0"

_HELP_EPILOGUE = """
Examples:
  cq search build_graph --lang python
  cq q "entity=function name=~^build"
  cq run --steps '[{"type":"q","query":"entity=function"}]'
  cq neighborhood tools/cq/cli_app/app.py:1 --lang python

Environment Variables:
  CQ_ROOT           Repository root path
  CQ_FORMAT         Output format (md, json, both, summary, mermaid, dot, ldmd)
  CQ_VERBOSE        Verbosity level
  CQ_ARTIFACT_DIR   Artifact output directory
  CQ_SAVE_ARTIFACT  Enable/disable artifact persistence
"""


class GlobalOptions(CqStruct, frozen=True):
    """Resolved global options for launcher configuration."""

    root: Path | None = None
    verbose: int = 0
    output_format: OutputFormat = OutputFormat.md
    artifact_dir: Path | None = None
    save_artifact: bool = True


@dataclass(frozen=True, slots=True)
class ConfigOptionArgs:
    """Parsed CLI config options."""

    config: Annotated[
        str | None,
        Parameter(
            name="--config",
            env_var="CQ_CONFIG",
            group=global_group,
            help="Config file path",
        ),
    ] = None
    use_config: Annotated[
        bool,
        Parameter(
            name="--use-config",
            negative="--no-config",
            negative_bool=(),
            env_var="CQ_USE_CONFIG",
            group=global_group,
            help="Enable config file loading",
        ),
    ] = True


@dataclass(frozen=True, slots=True)
class GlobalOptionArgs:
    """Parsed CLI global options."""

    root: Annotated[
        Path | None,
        Parameter(
            name="--root",
            env_var="CQ_ROOT",
            group=global_group,
            help="Repository root",
        ),
    ] = None
    verbose: Annotated[
        int,
        Parameter(
            name=["--verbose", "-v"],
            env_var="CQ_VERBOSE",
            group=global_group,
            help="Verbosity level",
        ),
    ] = 0
    output_format: Annotated[
        OutputFormat,
        Parameter(
            name="--format",
            env_var="CQ_FORMAT",
            group=global_group,
            help="Output format",
        ),
    ] = OutputFormat.md
    artifact_dir: Annotated[
        Path | None,
        Parameter(
            name="--artifact-dir",
            env_var="CQ_ARTIFACT_DIR",
            group=global_group,
            help="Artifact directory",
        ),
    ] = None
    save_artifact: Annotated[
        bool,
        Parameter(
            name="--save-artifact",
            negative="--no-save-artifact",
            negative_bool=(),
            env_var="CQ_SAVE_ARTIFACT",
            group=global_group,
            help="Persist output artifacts",
        ),
    ] = True


class LaunchContext(CqStruct, frozen=True):
    """Resolved launch context for CLI execution."""

    argv: list[str]
    root: Path | None
    verbose: int
    output_format: OutputFormat
    artifact_dir: Path | None
    save_artifact: bool


def _make_console(*, stderr: bool = False) -> Console:
    """Create a deterministic console for output.

    Returns:
        Console: A configured rich console instance.
    """
    force_color = os.environ.get("CQ_FORCE_COLOR", "").lower() in {"1", "true"}
    return Console(
        stderr=stderr,
        width=100,
        force_terminal=force_color,
        color_system="auto" if force_color else None,
        highlight=False,
    )


console = _make_console()
error_console = _make_console(stderr=True)

app = App(
    name="cq",
    help="Code Query - High-signal code analysis macros",
    version=VERSION,
    help_format="rich",
    help_epilogue=_HELP_EPILOGUE,
    name_transform=lambda s: s.replace("_", "-"),
    default_parameter=Parameter(show_default=True, show_env_var=True),
    result_action=cq_result_action,
    config=build_config_chain(),
    console=console,
    error_console=error_console,
    exit_on_error=False,
    print_error=True,
    help_on_error=True,
)
app.meta.group_parameters = global_group


def _apply_config_overrides(opts: GlobalOptions, config: CqConfig | None) -> GlobalOptions:
    if config is None:
        return opts

    root = opts.root
    if root is None and config.root:
        root = Path(config.root)

    verbose = opts.verbose
    if verbose == 0 and config.verbose is not None:
        verbose = config.verbose

    output_format = opts.output_format
    if output_format == OutputFormat.md and config.output_format:
        with suppress(ValueError):
            output_format = OutputFormat(config.output_format)

    artifact_dir = opts.artifact_dir
    if artifact_dir is None and config.artifact_dir:
        artifact_dir = Path(config.artifact_dir)

    save_artifact = opts.save_artifact
    if config.save_artifact is not None and opts.save_artifact:
        save_artifact = config.save_artifact

    return GlobalOptions(
        root=root,
        verbose=verbose,
        output_format=output_format,
        artifact_dir=artifact_dir,
        save_artifact=save_artifact,
    )


def _resolve_global_options(
    cli_opts: GlobalOptions,
    config: CqConfig | None,
    env: CqConfig | None,
) -> GlobalOptions:
    opts = _apply_config_overrides(cli_opts, config)
    return _apply_config_overrides(opts, env)


def _build_launch_context(
    argv: list[str],
    config_opts: ConfigOptionArgs,
    global_opts: GlobalOptionArgs,
) -> LaunchContext:
    no_config = not config_opts.use_config
    if config_opts.config or no_config:
        app.config = build_config_chain(
            config_file=config_opts.config,
            no_config=no_config,
        )

    typed_config = load_typed_config(
        config_file=config_opts.config,
        no_config=no_config,
    )
    typed_env = None if no_config else load_typed_env_config()

    cli_opts = GlobalOptions(
        root=global_opts.root,
        verbose=global_opts.verbose,
        output_format=global_opts.output_format,
        artifact_dir=global_opts.artifact_dir,
        save_artifact=global_opts.save_artifact,
    )
    resolved = _resolve_global_options(cli_opts, typed_config, typed_env)
    return LaunchContext(
        argv=argv,
        root=resolved.root,
        verbose=resolved.verbose,
        output_format=resolved.output_format,
        artifact_dir=resolved.artifact_dir,
        save_artifact=resolved.save_artifact,
    )


def _build_cli_context(launch: LaunchContext) -> CliContext:
    options = CliContextOptions(
        root=launch.root,
        verbose=launch.verbose,
        output_format=launch.output_format,
        artifact_dir=launch.artifact_dir,
        save_artifact=launch.save_artifact,
    )
    return CliContext.build(argv=launch.argv, options=options)


@app.meta.default
def launcher(
    *tokens: Annotated[str, Parameter(show=False, allow_leading_hyphen=True)],
    global_opts: GlobalOptionArgs | None = None,
    config_opts: ConfigOptionArgs | None = None,
) -> int:
    """Handle global options and dispatch the selected command.

    Returns:
        int: Process exit code from command execution.
    """
    resolved_global_opts = global_opts if global_opts is not None else GlobalOptionArgs()
    resolved_config_opts = config_opts if config_opts is not None else ConfigOptionArgs()
    launch = _build_launch_context(
        argv=sys.argv[1:],
        config_opts=resolved_config_opts,
        global_opts=resolved_global_opts,
    )
    ctx = _build_cli_context(launch)
    exit_code, _event = invoke_with_telemetry(app, list(tokens), ctx=ctx)
    return exit_code


# Analysis commands
app.command("tools.cq.cli_app.commands.analysis:impact", group=analysis_group)
app.command("tools.cq.cli_app.commands.analysis:calls", group=analysis_group)
app.command("tools.cq.cli_app.commands.analysis:imports", group=analysis_group)
app.command("tools.cq.cli_app.commands.analysis:exceptions", group=analysis_group)
app.command(
    "tools.cq.cli_app.commands.analysis:sig_impact",
    name="sig-impact",
    group=analysis_group,
)
app.command(
    "tools.cq.cli_app.commands.analysis:side_effects",
    name="side-effects",
    group=analysis_group,
)
app.command("tools.cq.cli_app.commands.analysis:scopes", group=analysis_group)
app.command(
    "tools.cq.cli_app.commands.analysis:bytecode_surface",
    name="bytecode-surface",
    group=analysis_group,
)
app.command("tools.cq.cli_app.commands.query:q", name="q", group=analysis_group)
app.command("tools.cq.cli_app.commands.search:search", group=analysis_group)
app.command("tools.cq.cli_app.commands.report:report", group=analysis_group)
app.command("tools.cq.cli_app.commands.run:run", group=analysis_group)
app.command("tools.cq.cli_app.commands.chain:chain", group=analysis_group)
app.command(
    "tools.cq.cli_app.commands.neighborhood:neighborhood",
    name="neighborhood",
    alias="nb",
    group=analysis_group,
)

# Admin commands
app.command("tools.cq.cli_app.commands.admin:index", group=admin_group)
app.command("tools.cq.cli_app.commands.admin:cache", group=admin_group)
app.command("tools.cq.cli_app.commands.admin:schema", group=admin_group)

# Protocol command sub-apps (lazy-loaded)
app.command("tools.cq.cli_app.commands.ldmd:ldmd_app", name="ldmd")
app.command(
    "tools.cq.cli_app.commands.artifact:artifact_app",
    name="artifact",
)

# Setup commands
app.command("tools.cq.cli_app.commands.repl:repl", group=setup_group)
app.register_install_completion_command(
    name="--install-completion",
    add_to_startup=False,
    group=setup_group,
    help="Install CQ shell completion scripts.",
)
