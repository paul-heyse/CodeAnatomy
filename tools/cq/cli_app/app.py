"""CQ CLI application using cyclopts.

This module defines the root cyclopts App with meta-app launcher pattern
for unified global option handling and context injection.
"""

from __future__ import annotations

import os
import sys
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any

from cyclopts import App, Group, Parameter
from rich.console import Console

from tools.cq.cli_app.config import build_config_chain, load_typed_config, load_typed_env_config
from tools.cq.cli_app.config_types import CqConfig
from tools.cq.cli_app.context import CliContext, CliResult, FilterConfig
from tools.cq.cli_app.result import handle_result
from tools.cq.cli_app.types import OutputFormat
from tools.cq.core.structs import CqStruct

# Version string
VERSION = "0.3.0"

# Command groups - global options appear first
global_group = Group("Global Options", sort_key=0)
analysis_group = Group("Analysis", sort_key=1)
admin_group = Group("Administration", sort_key=2)


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
        str | None, Parameter(name="--config", group=global_group, help="Config file path")
    ] = None
    no_config: Annotated[
        bool, Parameter(name="--no-config", group=global_group, help="Skip config file loading")
    ] = False


@dataclass(frozen=True, slots=True)
class GlobalOptionArgs:
    """Parsed CLI global options."""

    root: Annotated[
        Path | None, Parameter(name="--root", group=global_group, help="Repository root")
    ] = None
    verbose: Annotated[
        int, Parameter(name=["--verbose", "-v"], group=global_group, help="Verbosity level")
    ] = 0
    output_format: Annotated[
        OutputFormat, Parameter(name="--format", group=global_group, help="Output format")
    ] = OutputFormat.md
    artifact_dir: Annotated[
        Path | None, Parameter(name="--artifact-dir", group=global_group, help="Artifact directory")
    ] = None
    no_save_artifact: Annotated[
        bool, Parameter(name="--no-save-artifact", group=global_group, help="Don't save artifact")
    ] = False


class LaunchContext(CqStruct, frozen=True):
    """Resolved launch context for CLI execution."""

    argv: list[str]
    root: Path | None
    verbose: int
    output_format: OutputFormat
    artifact_dir: Path | None
    save_artifact: bool


def _make_console() -> Console:
    """Create a deterministic console for output.

    Returns
    -------
    Console
        Configured Rich console.
    """
    force_color = os.environ.get("CQ_FORCE_COLOR", "").lower() in {"1", "true"}
    return Console(
        width=100,
        force_terminal=force_color,
        color_system="auto" if force_color else None,
        highlight=False,
    )


# Create the root app with config chain
app = App(
    name="cq",
    help="Code Query - High-signal code analysis macros",
    version=VERSION,
    help_format="markdown",
    name_transform=lambda s: s.replace("_", "-"),
    config=build_config_chain(),
)

# Set group parameters for meta app
app.meta.group_parameters = global_group

console = _make_console()


# ============================================================================
# Meta App Launcher
# ============================================================================


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
    if config_opts.config or config_opts.no_config:
        app.config = build_config_chain(
            config_file=config_opts.config,
            no_config=config_opts.no_config,
        )

    typed_config = load_typed_config(
        config_file=config_opts.config,
        no_config=config_opts.no_config,
    )
    typed_env = None if config_opts.no_config else load_typed_env_config()

    cli_opts = GlobalOptions(
        root=global_opts.root,
        verbose=global_opts.verbose,
        output_format=global_opts.output_format,
        artifact_dir=global_opts.artifact_dir,
        save_artifact=not global_opts.no_save_artifact,
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
    return CliContext.build(
        argv=launch.argv,
        root=launch.root,
        verbose=launch.verbose,
        output_format=launch.output_format,
        artifact_dir=launch.artifact_dir,
        save_artifact=launch.save_artifact,
    )


def _execute_command(tokens: tuple[str, ...], ctx: CliContext) -> CliResult | int | object:
    command, bound, ignored = app.parse_args(
        tokens,
        exit_on_error=True,
        print_error=True,
    )

    extra: dict[str, Any] = {}
    if "ctx" in ignored:
        extra["ctx"] = ctx

    return command(*bound.args, **bound.kwargs, **extra)


def _finalize_result(result: CliResult | int | object) -> int:
    if isinstance(result, CliResult):
        filters = result.filters if result.filters else FilterConfig()
        return handle_result(result, filters)
    if isinstance(result, int):
        return result
    return 0


@app.meta.default
def launcher(
    *tokens: Annotated[str, Parameter(show=False, allow_leading_hyphen=True)],
    global_opts: GlobalOptionArgs | None = None,
    config_opts: ConfigOptionArgs | None = None,
) -> int:
    """Handle global options and dispatch the selected command.

    Returns
    -------
    int
        Process exit code.
    """
    if global_opts is None:
        global_opts = GlobalOptionArgs()
    if config_opts is None:
        config_opts = ConfigOptionArgs()
    launch = _build_launch_context(
        argv=sys.argv[1:],
        config_opts=config_opts,
        global_opts=global_opts,
    )
    ctx = _build_cli_context(launch)
    result = _execute_command(tokens, ctx)
    return _finalize_result(result)


# ============================================================================
# Register Analysis Commands
# ============================================================================

app.command("tools.cq.cli_app.commands.analysis:impact", group=analysis_group)
app.command("tools.cq.cli_app.commands.analysis:calls", group=analysis_group)
app.command("tools.cq.cli_app.commands.analysis:imports", group=analysis_group)
app.command("tools.cq.cli_app.commands.analysis:exceptions", group=analysis_group)
app.command(
    "tools.cq.cli_app.commands.analysis:sig_impact", name="sig-impact", group=analysis_group
)
app.command(
    "tools.cq.cli_app.commands.analysis:side_effects", name="side-effects", group=analysis_group
)
app.command("tools.cq.cli_app.commands.analysis:scopes", group=analysis_group)
app.command(
    "tools.cq.cli_app.commands.analysis:bytecode_surface",
    name="bytecode-surface",
    group=analysis_group,
)


# ============================================================================
# Register Query Command
# ============================================================================

app.command("tools.cq.cli_app.commands.query:q", group=analysis_group)


# ============================================================================
# Register Search Command
# ============================================================================

app.command("tools.cq.cli_app.commands.search:search", group=analysis_group)


# ============================================================================
# Register Report Command
# ============================================================================

app.command("tools.cq.cli_app.commands.report:report", group=analysis_group)


# ============================================================================
# Register Admin Commands
# ============================================================================

app.command("tools.cq.cli_app.commands.admin:index", group=admin_group)
app.command("tools.cq.cli_app.commands.admin:cache", group=admin_group)
app.command("tools.cq.cli_app.commands.admin:schema", group=admin_group)

# Run/chain commands
app.command("tools.cq.cli_app.commands.run:run", group=analysis_group)
app.command("tools.cq.cli_app.commands.chain:chain", group=analysis_group)
