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

from tools.cq.cli_app.config import build_config_chain, load_typed_config, load_typed_env_config
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
    cache_dir: Annotated[
        Path | None,
        Parameter(name="--cache-dir", group=global_group, help="DiskCache root directory"),
    ] = None,
    cache_query_ttl: Annotated[
        float | None,
        Parameter(name="--cache-query-ttl", group=global_group, help="Query cache TTL seconds"),
    ] = None,
    cache_query_size: Annotated[
        int | None,
        Parameter(name="--cache-query-size", group=global_group, help="Query cache size limit (bytes)"),
    ] = None,
    cache_index_size: Annotated[
        int | None,
        Parameter(name="--cache-index-size", group=global_group, help="Index cache size limit (bytes)"),
    ] = None,
    cache_query_shards: Annotated[
        int | None,
        Parameter(name="--cache-query-shards", group=global_group, help="Query cache shard count"),
    ] = None,
) -> int:
    """Global option handler and command dispatcher."""
    cli_root = root
    cli_verbose = verbose
    cli_output_format = output_format
    cli_artifact_dir = artifact_dir
    cli_no_save_artifact = no_save_artifact
    cli_cache_dir = cache_dir
    cli_cache_query_ttl = cache_query_ttl
    cli_cache_query_size = cache_query_size
    cli_cache_index_size = cache_index_size
    cli_cache_query_shards = cache_query_shards

    # Rebuild config if --config or --no-config specified
    if config or no_config:
        app.config = build_config_chain(config_file=config, no_config=no_config)

    typed_config = load_typed_config(config_file=config, no_config=no_config)
    typed_env = None if no_config else load_typed_env_config()
    if typed_config is not None:
        if cli_root is None and typed_config.root:
            root = Path(typed_config.root)
        if cli_verbose == 0 and typed_config.verbose is not None:
            verbose = typed_config.verbose
        if typed_config.output_format and cli_output_format == OutputFormat.md:
            try:
                output_format = OutputFormat(typed_config.output_format)
            except ValueError:
                pass
        if cli_artifact_dir is None and typed_config.artifact_dir:
            artifact_dir = Path(typed_config.artifact_dir)
        if typed_config.save_artifact is not None and not cli_no_save_artifact:
            no_save_artifact = not typed_config.save_artifact
        if cli_cache_dir is None and typed_config.cache_dir:
            cache_dir = Path(typed_config.cache_dir)
        if cli_cache_query_ttl is None and typed_config.cache_query_ttl is not None:
            cache_query_ttl = typed_config.cache_query_ttl
        if cli_cache_query_size is None and typed_config.cache_query_size is not None:
            cache_query_size = typed_config.cache_query_size
        if cli_cache_index_size is None and typed_config.cache_index_size is not None:
            cache_index_size = typed_config.cache_index_size
        if cli_cache_query_shards is None and typed_config.cache_query_shards is not None:
            cache_query_shards = typed_config.cache_query_shards
    if typed_env is not None:
        if cli_root is None and typed_env.root:
            root = Path(typed_env.root)
        if cli_verbose == 0 and typed_env.verbose is not None:
            verbose = typed_env.verbose
        if typed_env.output_format and cli_output_format == OutputFormat.md:
            try:
                output_format = OutputFormat(typed_env.output_format)
            except ValueError:
                pass
        if cli_artifact_dir is None and typed_env.artifact_dir:
            artifact_dir = Path(typed_env.artifact_dir)
        if typed_env.save_artifact is not None and not cli_no_save_artifact:
            no_save_artifact = not typed_env.save_artifact
        if cli_cache_dir is None and typed_env.cache_dir:
            cache_dir = Path(typed_env.cache_dir)
        if cli_cache_query_ttl is None and typed_env.cache_query_ttl is not None:
            cache_query_ttl = typed_env.cache_query_ttl
        if cli_cache_query_size is None and typed_env.cache_query_size is not None:
            cache_query_size = typed_env.cache_query_size
        if cli_cache_index_size is None and typed_env.cache_index_size is not None:
            cache_index_size = typed_env.cache_index_size
        if cli_cache_query_shards is None and typed_env.cache_query_shards is not None:
            cache_query_shards = typed_env.cache_query_shards

    from tools.cq.cli_app.params import DiskCacheOptions

    DiskCacheOptions(
        cache_dir=cache_dir,
        cache_query_ttl=cache_query_ttl,
        cache_query_size=cache_query_size,
        cache_index_size=cache_index_size,
        cache_query_shards=cache_query_shards,
    ).apply_env()

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

from tools.cq.cli_app.commands.admin import cache, index, schema

app.command(index, group=admin_group)
app.command(cache, group=admin_group)
app.command(schema, group=admin_group)
