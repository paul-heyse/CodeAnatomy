"""CQ CLI application using cyclopts.

This module defines the root cyclopts App with meta-app launcher pattern
for unified global option handling and context injection.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any

from cyclopts import App, Group, Parameter
from rich.console import Console

from tools.cq.cli_app.config import build_config_chain
from tools.cq.cli_app.context import CliContext, CliResult, FilterConfig
from tools.cq.cli_app.result import handle_result
from tools.cq.cli_app.types import OutputFormat

if TYPE_CHECKING:
    pass

# Version string
VERSION = "0.3.0"

# Command groups - global options appear first
global_group = Group("Global Options", sort_key=0)
analysis_group = Group("Analysis", sort_key=1)
admin_group = Group("Administration", sort_key=2)


def _make_console() -> Console:
    """Create a deterministic console for output.

    Returns
    -------
    Console
        Configured Rich console.
    """
    force_color = os.environ.get("CQ_FORCE_COLOR", "").lower() in ("1", "true")
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


@app.meta.default
def launcher(
    *tokens: Annotated[str, Parameter(show=False, allow_leading_hyphen=True)],
    root: Annotated[Path | None, Parameter(name="--root", group=global_group, help="Repository root")] = None,
    config: Annotated[str | None, Parameter(name="--config", group=global_group, help="Config file path")] = None,
    no_config: Annotated[bool, Parameter(name="--no-config", group=global_group, help="Skip config file loading")] = False,
    verbose: Annotated[int, Parameter(name=["--verbose", "-v"], group=global_group, help="Verbosity level")] = 0,
    output_format: Annotated[OutputFormat, Parameter(name="--format", group=global_group, help="Output format")] = OutputFormat.md,
    artifact_dir: Annotated[Path | None, Parameter(name="--artifact-dir", group=global_group, help="Artifact directory")] = None,
    no_save_artifact: Annotated[bool, Parameter(name="--no-save-artifact", group=global_group, help="Don't save artifact")] = False,
) -> int:
    """Global option handler and command dispatcher."""
    # Rebuild config if --config or --no-config specified
    if config or no_config:
        app.config = build_config_chain(config_file=config, no_config=no_config)

    # Build context
    ctx = CliContext.build(
        argv=sys.argv[1:],
        root=root,
        verbose=verbose,
        output_format=output_format,
        artifact_dir=artifact_dir,
        save_artifact=not no_save_artifact,
    )

    # Parse and dispatch to command with context injection
    command, bound, ignored = app.parse_args(
        tokens,
        exit_on_error=True,
        print_error=True,
    )

    # Inject context for commands that need it
    extra: dict[str, Any] = {}
    if "ctx" in ignored:
        extra["ctx"] = ctx

    # Execute command
    result = command(*bound.args, **bound.kwargs, **extra)

    # Handle result
    if isinstance(result, CliResult):
        filters = result.filters if result.filters else FilterConfig()
        return handle_result(result, filters)
    if isinstance(result, int):
        return result
    return 0


# ============================================================================
# Register Analysis Commands
# ============================================================================

from tools.cq.cli_app.commands.analysis import (
    async_hazards,
    bytecode_surface,
    calls,
    exceptions,
    impact,
    imports,
    scopes,
    side_effects,
    sig_impact,
)

app.command(impact, group=analysis_group)
app.command(calls, group=analysis_group)
app.command(imports, group=analysis_group)
app.command(exceptions, group=analysis_group)
app.command(sig_impact, name="sig-impact", group=analysis_group)
app.command(side_effects, name="side-effects", group=analysis_group)
app.command(scopes, group=analysis_group)
app.command(async_hazards, name="async-hazards", group=analysis_group)
app.command(bytecode_surface, name="bytecode-surface", group=analysis_group)


# ============================================================================
# Register Query Command
# ============================================================================

from tools.cq.cli_app.commands.query import q

app.command(q, group=analysis_group)


# ============================================================================
# Register Report Command
# ============================================================================

from tools.cq.cli_app.commands.report import report

app.command(report, group=analysis_group)


# ============================================================================
# Register Admin Commands
# ============================================================================

from tools.cq.cli_app.commands.admin import cache, index

app.command(index, group=admin_group)
app.command(cache, group=admin_group)
